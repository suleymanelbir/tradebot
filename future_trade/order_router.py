"""Emir gönderme ve koruma (SL/TP/trailing) yönetimi
- place_entry_with_protection(plan): entry + SL/TP
- update_trailing_for_open_positions(...): açık pozisyonlar için SL güncelle
"""
# /opt/tradebot/future_trade/order_router.py
from __future__ import annotations  # Tip ipuçları için (Python 3.7+ sonrası)

# 📦 Standart Kütüphaneler
import time
import math
import logging
import uuid
import httpx

# 🧠 Tip Tanımları
from typing import Dict, Any, List, Optional

# 🛠️ Proje İçi Modüller
from .exchange_utils import quantize, price_quantize, symbol_filters
from .strategy.indicators import atr  # ATR hesaplamak için




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
                    # İSİM BİRLİĞİ: binance_client.exchange_info()
                    self._exi_cache = await self.client.exchange_info()
                    logging.info("exchangeInfo cached")
                except Exception as e:
                    logging.warning(f"exchangeInfo fetch failed: {e}")
                    self._exi_cache = {"symbols": []}
            return self._exi_cache


    def _cid(self, prefix: str) -> str:
        # 24h içinde benzersiz: ms timestamp + kısa uuid
        return f"{prefix}_{int(time.time()*1000)}_{uuid.uuid4().hex[:6]}"

    async def _place_idempotent(self, params: dict, stage: str) -> dict:
        try:
            return await self.client.place_order(**params)
        except Exception as e:
            resp = getattr(e, "response", None)
            body = (resp.text or "") if resp is not None else ""
            if "ClientOrderId is duplicated" in body or "-4116" in body:
                # zaten aynı CID ile bir emir var → onu çek
                try:
                    od = await self.client.get_order(
                        symbol=params["symbol"],
                        origClientOrderId=params.get("newClientOrderId")
                    )
                    logging.info(f"idempotent-ok {stage} {params['symbol']} cid={params.get('newClientOrderId')} -> #{od.get('orderId')}")
                    return od
                except Exception as e2:
                    logging.warning(f"idempotent-lookup-failed {stage}: {e2}; proceeding as placed")
                    # En kötü ihtimalle devam (Router üst katmanda bir sonraki sync’te yakalar)
                    return {"symbol": params["symbol"], "clientOrderId": params.get("newClientOrderId")}
            raise



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

        qty = max(quantize(float(plan.qty), step), step)
        entry_ref = float(plan.entry or 0.0)

        # Marj uygunluğuna göre qty küçült
        qty_pref = await self._cap_qty_by_margin(sym, qty, entry_ref, step)
        if qty_pref < step:
            await self.notifier.debug_trades({
                "event": "sizing_rejected",
                "symbol": sym,
                "reason": "insufficient_margin",
                "wanted_qty": qty, "capped_qty": qty_pref
            })
            raise Exception(f"Insufficient margin for {sym}: wanted {qty}, capped {qty_pref}")
        qty = qty_pref

        if entry_ref > 0.0:
            notional = qty * entry_ref
            if min_notional and notional < min_notional:
                need_qty = min_notional / max(entry_ref, 1e-12)
                qty = max(quantize(need_qty, step), step)

        def qprice(p):
            return price_quantize(float(p), tick) if p else None

        sl_price = qprice(getattr(plan, "sl", None))
        tp_price = qprice(getattr(plan, "tp", None))

        # 2) Eski koruma emirlerini iptal et
        await self._cancel_existing_protections(sym)

        # 3) MARKET entry (idempotent)
        entry_params = {
            "symbol": sym,
            "side": side,
            "type": "MARKET",
            "quantity": qty,
            "newClientOrderId": self._cid("entry"),
        }
        try:
            od_entry = await self._place_idempotent(entry_params, "entry")
            await self.notifier.debug_trades({
                "event": "entry_ack",
                "symbol": sym,
                "orderId": od_entry.get("orderId"),
                "qty": qty
            })
        except Exception as e:
            await self.notifier.alert({
                "event": "order_error",
                "symbol": sym,
                "stage": "entry",
                "error": str(e)
            })
            raise

        # 4) STOP (SL) — reduceOnly + closePosition
        if sl_price:
            sl_params = {
                "symbol": sym,
                "side": opp,
                "type": "STOP_MARKET",
                "stopPrice": sl_price,
                "closePosition": "true",
                "workingType": self.cfg.get("sl_working_type", "MARK_PRICE"),
                "newClientOrderId": self._cid("sl"),
            }
            try:
                od_sl = await self._place_idempotent(sl_params, "stop")
                await self.notifier.debug_trades({
                    "event": "sl_ack",
                    "symbol": sym,
                    "orderId": od_sl.get("orderId"),
                    "stop": sl_price
                })
            except Exception as e:
                await self.notifier.alert({
                    "event": "order_error",
                    "symbol": sym,
                    "stage": "stop",
                    "error": str(e)
                })

        # 5) TAKE PROFIT
        tp_mode = str(self.cfg.get("tp_mode", "MARKET")).upper()
        if tp_price:
            if tp_mode == "MARKET":
                tp_params = {
                    "symbol": sym,
                    "side": opp,
                    "type": "TAKE_PROFIT_MARKET",
                    "stopPrice": tp_price,
                    "closePosition": "true",
                    "workingType": self.cfg.get("sl_working_type", "MARK_PRICE"),
                    "newClientOrderId": self._cid("tp"),
                }
                try:
                    od_tp = await self._place_idempotent(tp_params, "take_profit")
                    await self.notifier.debug_trades({
                        "event": "tp_ack",
                        "symbol": sym,
                        "orderId": od_tp.get("orderId"),
                        "tp": tp_price
                    })
                except Exception as e:
                    await self.notifier.alert({
                        "event": "order_error",
                        "symbol": sym,
                        "stage": "take_profit",
                        "error": str(e)
                    })

    # 6) (DB tarafı) — Reconciler borsadan okuyacağı için burada ek işlem zorunlu değil.

    async def _effective_leverage(self, symbol: str) -> int:
        # config: leverage: { "default": 3, "ETHUSDT": 5, ... }
        lev_cfg = self.cfg.get("leverage", {})
        return int(lev_cfg.get(symbol, lev_cfg.get("default", 3)))

    async def _cap_qty_by_margin(self, symbol: str, qty: float, entry_ref: float, step: float) -> float:
        """
        available USDT ve kaldıraç ile maks. alım gücünü hesapla,
        adedi step’e yuvarlayıp geri döndür.
        """
        lev = await self._effective_leverage(symbol)
        px  = entry_ref or await self.client.get_price(symbol)
        avail = await self.client.get_available_usdt()
        # güvenlik payı
        safety = float(self.cfg.get("margin_safety", 0.95))
        max_notional = avail * lev * safety
        if px <= 0:
            return qty
        max_qty = max_notional / px
        # step’e oturt
        from .exchange_utils import quantize
        capped = quantize(max_qty, step)
        return min(qty, max(capped, step))


    def _cid(self, prefix: str) -> str:
        return f"{prefix}_{int(time.time()*1000)}_{uuid.uuid4().hex[:6]}"

    async def _exi(self):
        if self._exi_cache is None:
            try:
                self._exi_cache = await self.client.get_exchange_info()
                logging.info("exchangeInfo cached")
            except Exception as e:
                logging.warning(f"exchangeInfo fetch failed: {e}")
                self._exi_cache = {"symbols": []}
        return self._exi_cache

    async def _current_sl_order(self, symbol: str) -> Optional[dict]:
        """Açık STOP_MARKET close-all (closePosition=true) SL emrini bul."""
        try:
            opens = await self.client.open_orders(symbol)
        except Exception as e:
            logging.debug(f"open_orders fail {symbol}: {e}")
            return None
        for od in opens:
            t = od.get("type")
            cp = str(od.get("closePosition", "")).lower() == "true"
            if t in ("STOP_MARKET", "STOP") and cp:
                return od
        return None

    async def _cancel_order_silent(self, symbol: str, order_id: Optional[int]=None, client_id: Optional[str]=None):
        try:
            p = {"symbol": symbol}
            if order_id: p["orderId"] = order_id
            if client_id: p["origClientOrderId"] = client_id
            await self.client.cancel_order(**p)
        except Exception as e:
            logging.debug(f"cancel_order_silent {symbol}: {e}")



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
        """
        ATR tabanlı trailing:
        - LONG: new_stop = close - atr_mult * ATR
        - SHORT: new_stop = close + atr_mult * ATR
        - Yalnızca 'önemli iyileşme' varsa günceller (step_pct eşiği)
        - STOP_MARKET closePosition=true kullanır (quantity/reduceOnly gönderilmez)
        """
        tf = stream.tf_entry
        period = int(trailing_cfg.get("atr_period", 14))
        mult = float(trailing_cfg.get("atr_mult", 2.0))
        step_pct = float(trailing_cfg.get("step_pct", 0.1)) / 100.0

        conn = self.persistence._conn()
        try:
            cur = conn.cursor()
            cur.execute("""
                SELECT symbol,
                    qty,
                    CASE WHEN qty >= 0 THEN 'LONG' ELSE 'SHORT' END AS side
                FROM futures_positions
                WHERE ABS(qty) > 0
            """)
            rows = cur.fetchall()
        finally:
            conn.close()

        if not rows:
            return

        exi = await self._exi()

        for sym, qty, side in rows:
            try:
                if (side == "LONG" and qty < 0) or (side == "SHORT" and qty > 0):
                    await self.notifier.alert({
                        "event": "trailing_mismatch",
                        "symbol": sym,
                        "qty": qty,
                        "side": side,
                        "error": "Position direction mismatch"
                    })
                    continue

                tick, step, _ = symbol_filters(exi, sym)

                limit = max(period + 3, 25)
                kl = await self.client.get_klines(sym, interval=tf, limit=limit)
                if not kl or len(kl) < period + 1:
                    continue

                closes = [float(k[4]) for k in kl]
                highs  = [float(k[2]) for k in kl]
                lows   = [float(k[3]) for k in kl]

                atr_val = atr(highs, lows, closes, period=period)
                if not atr_val or math.isnan(atr_val):
                    continue

                last_close = closes[-1]

                if side == "LONG":
                    target = last_close - mult * atr_val
                    opp = "SELL"
                else:
                    target = last_close + mult * atr_val
                    opp = "BUY"

                target_q = price_quantize(target, tick)

                cur_sl = await self._current_sl_order(sym)
                cur_stop = float(cur_sl["stopPrice"]) if cur_sl and cur_sl.get("stopPrice") else None

                should_update = False
                if cur_stop is None:
                    should_update = True
                else:
                    if side == "LONG":
                        should_update = target_q > cur_stop * (1.0 + step_pct)
                    else:
                        should_update = target_q < cur_stop * (1.0 - step_pct)

                if not should_update:
                    continue

                if cur_sl:
                    await self._cancel_order_silent(sym, order_id=cur_sl.get("orderId"))

                sl_params = {
                    "symbol": sym,
                    "side": opp,
                    "type": "STOP_MARKET",
                    "stopPrice": target_q,
                    "closePosition": "true",
                    "workingType": self.cfg.get("sl_working_type", "MARK_PRICE"),
                    "newClientOrderId": self._cid("trail_sl"),
                }

                try:
                    od = await self.client.place_order(**sl_params)
                    await self.notifier.debug_trades({
                        "event": "trailing_updated",
                        "symbol": sym,
                        "side": side,
                        "stop": target_q,
                        "orderId": od.get("orderId"),
                    })
                except httpx.HTTPStatusError as e:
                    body = e.response.text if e.response else ""
                    await self.notifier.trade({
                        "event": "trailing_error",
                        "symbol": sym,
                        "status": getattr(e.response, "status_code", None),
                        "binance": body
                    })
                    raise
                except Exception as e:
                    await self.notifier.trade({
                        "event": "trailing_error",
                        "symbol": sym,
                        "error": str(e)
                    })
                    raise

            except Exception as e:
                await self.notifier.alert({
                    "event": "trailing_error",
                    "symbol": sym,
                    "error": str(e)
                })