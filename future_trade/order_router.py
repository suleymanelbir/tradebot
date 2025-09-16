"""Emir gönderme ve koruma (SL/TP/trailing) yönetimi
- place_entry_with_protection(plan): entry + SL/TP
- update_trailing_for_open_positions(...): açık pozisyonlar için SL güncelle
"""
# /opt/tradebot/future_trade/order_router.py
from __future__ import annotations  # Tip ipuçları için (Python 3.7+ sonrası)

# Standart kütüphaneler
import time
import math
import logging

# Tip tanımları
from typing import Dict, Any

# Proje içi modüller
from .exchange_utils import quantize, price_quantize, symbol_filters


class OrderRouter:
    def __init__(self, client, cfg: Dict[str, Any], notifier, persistence):
        self.client = client
        self.cfg = cfg
        self.notifier = notifier
        self.persistence = persistence
        self._exi_cache = None
        
    async def _exi(self):
        if self._exi_cache is None:
            try:
                self._exi_cache = await self.client.get_exchange_info()
                logging.info("exchangeInfo cached")
            except Exception as e:
                logging.warning(f"exchangeInfo fetch failed: {e}")
                self._exi_cache = {"symbols": []}
        return self._exi_cache



    async def place_entry_with_protection(self, plan):
        """
        plan: .symbol, .side ("LONG"/"SHORT"), .qty, .entry, .sl, .tp
        Gerçek emir akışı: MARKET entry + STOP_MARKET SL + TAKE_PROFIT(MARKET/LIMIT)
        """
        sym = plan.symbol
        side = "BUY" if plan.side == "LONG" else "SELL"
        opp  = "SELL" if plan.side == "LONG" else "BUY"

        # 0) Bildirim (erken)
        await self.notifier.trade({"event": "entry", "symbol": sym, "side": plan.side, "qty": plan.qty})

        # 1) Filtreler ve kuantizasyon
        exi = await self._exi()
        tick, step, min_notional = symbol_filters(exi, sym)

        # qty kuantizasyon + minNotional kontrolü
        qty = max(quantize(float(plan.qty), step), step)
        entry_ref = float(plan.entry or 0.0)
        if entry_ref <= 0.0:
            # market fiyatını referans almak istersen burada ticker çağırıp doldurabilirsin.
            # şimdilik qty*entry_ref kontrolünü atlıyoruz
            pass
        else:
            notional = qty * entry_ref
            if min_notional and notional < min_notional:
                need_qty = min_notional / max(entry_ref, 1e-12)
                # step'e yuvarla, en az 1 step
                qty = max(quantize(need_qty, step), step)

        def qprice(p):
            return price_quantize(float(p), tick) if p else None

        sl_price = qprice(getattr(plan, "sl", None))
        tp_price = qprice(getattr(plan, "tp", None))

        # 2) Eski koruma emirlerini iptal et (çakışma olmasın)
        await self._cancel_existing_protections(sym)

        # 3) MARKET entry
        entry_params = {
            "symbol": sym,
            "side": side,
            "type": "MARKET",
            "quantity": qty,
            "newClientOrderId": f"entry_{int(time.time()*1000)}",
        }
        try:
            od_entry = await self.client.place_order(**entry_params)
            await self.notifier.debug_trades({"event": "entry_ack", "symbol": sym, "orderId": od_entry.get("orderId"), "qty": qty})
        except Exception as e:
            await self.notifier.alert({"event": "order_error", "symbol": sym, "stage": "entry", "error": str(e)})
            raise

        # 4) STOP (SL) — reduceOnly + closePosition
        if sl_price:
            sl_params = {
                "symbol": sym,
                "side": opp,
                "type": "STOP_MARKET",
                "stopPrice": sl_price,
                "closePosition": "true",                # string → binance 'true' bekler
                
                "workingType": self.cfg.get("sl_working_type", "MARK_PRICE"),
                "newClientOrderId": f"sl_{int(time.time()*1000)}",
            }
            try:
                od_sl = await self.client.place_order(**sl_params)
                await self.notifier.debug_trades({"event": "sl_ack", "symbol": sym, "orderId": od_sl.get("orderId"), "stop": sl_price})
            except Exception as e:
                await self.notifier.alert({"event": "order_error", "symbol": sym, "stage": "stop", "error": str(e)})

        # 5) TAKE PROFIT
        tp_mode = str(self.cfg.get("tp_mode", "MARKET")).upper()
        if tp_price:
            if tp_mode == "MARKET":
                tp_params = {
                    "symbol": sym,
                    "side": opp,
                    "type": "TAKE_PROFIT_MARKET",
                    "stopPrice": tp_price,
                    "closePosition": "true",            # Close-All
                    "workingType": self.cfg.get("sl_working_type", "MARK_PRICE"),
                    "newClientOrderId": f"tp_{int(time.time()*1000)}",
                }
                try:
                    od_tp = await self.client.place_order(**tp_params)
                    await self.notifier.debug_trades({"event": "tp_ack", "symbol": sym, "orderId": od_tp.get("orderId"), "tp": tp_price})
                except Exception as e:
                    await self.notifier.alert({"event": "order_error", "symbol": sym, "stage": "take_profit", "error": str(e)})
            
                
            try:
                od_tp = await self.client.place_order(**tp_params)
                await self.notifier.debug_trades({"event": "tp_ack", "symbol": sym, "orderId": od_tp.get("orderId"), "tp": tp_price})
            except Exception as e:
                await self.notifier.alert({"event": "order_error", "symbol": sym, "stage": "take_profit", "error": str(e)})

        # 6) (DB tarafı) — Reconciler borsadan okuyacağı için burada ek işlem zorunlu değil.



    async def _cancel_existing_protections(self, symbol: str):
        """Sembole ait açık reduceOnly SL/TP emirlerini iptal et (çakışmayı önler)."""
        try:
            opens = await self.client.open_orders(symbol)
        except Exception as e:
            logging.debug(f"open_orders fail {symbol}: {e}")
            return
        prot_types = {"STOP", "STOP_MARKET", "TAKE_PROFIT", "TAKE_PROFIT_MARKET"}
        for od in opens:
            if od.get("reduceOnly") and od.get("type") in prot_types:
                try:
                    await self.client.cancel_order(symbol, orderId=od.get("orderId"))
                    logging.info(f"cancel protect {symbol} #{od.get('orderId')} type={od.get('type')}")
                except Exception as ce:
                    logging.debug(f"cancel protect fail {symbol}: {ce}")


    async def update_trailing_for_open_positions(self, stream, trailing_cfg: Dict[str, Any]):
        return  # placeholder; 401 çözülünce gerçek trailing SL'yi ekleyeceğiz
