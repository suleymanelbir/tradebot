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
        PAPER modda:
        - futures_positions'a upsert (qty sign: LONG=+, SHORT=-)
        - symbol_state'e trail_stop/peak/trough başlangıçları
        - trades_bot'a entry/sl/tp 'ack' mesajları
        Gerçekte:
        - Binance order gönderilecek (ileride live moda geçince)
        """
        sym = plan.symbol
        side = plan.side

        # 0) Emir yönleri
        entry_side = "BUY" if side == "LONG" else "SELL"
        opp = "SELL" if side == "LONG" else "BUY"

        # 1) Sembol filtreleri → tick/step/min_notional
        exi = await self.client.exchange_info()
        tick, step, min_notional = symbol_filters(exi, sym)

        # 2) Miktar ve fiyatları kademeye oturt
        qty = max(quantize(float(plan.qty), step), step)

        def qprice(p):
            return price_quantize(float(p), tick) if p is not None else None

        entry = qprice(plan.entry)
        sl_price = qprice(getattr(plan, "sl", None))
        tp_price = qprice(getattr(plan, "tp", None))

        # 3) Eski koruma emirlerini iptal et
        await self._cancel_existing_protections(sym)

        # 4) MARKET entry (idempotent)
        entry_params = {
            "symbol": sym,
            "side": entry_side,
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

        # 5) STOP (SL) — reduceOnly + closePosition
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

        # 6) TAKE PROFIT
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
        Gerçek modda:
        - ATR tabanlı trailing SL güncellemesi
        - STOP_MARKET closePosition=true ile emir gönderimi
        PAPER modda:
        - Basit % trailing hesaplanır
        - symbol_state.trail_stop güncellenir
        - trades_bot'a trailing_updated mesajı gönderilir
        """
        now = int(time.time())
        step_pct = float(trailing_cfg.get("step_pct", 0.1)) / 100.0
        atr_period = int(trailing_cfg.get("atr_period", 14))
        atr_mult = float(trailing_cfg.get("atr_mult", 2.0))

        # Açık pozisyonları çek
        with self.persistence._conn() as conn:
            cur = conn.cursor()
            cur.execute("SELECT symbol, side, qty FROM futures_positions WHERE ABS(qty)>0")
            rows = cur.fetchall()

        if not rows:
            return

        # PAPER mod kontrolü
        paper_mode = self.cfg.get("mode", "paper") == "paper"

        if paper_mode:
            # PAPER mod: basit % trailing
            for sym, side, qty in rows:
                if sym not in stream.whitelist:
                    continue
                last = stream._mock_price(sym, now)
                with self.persistence._conn() as conn:
                    cur = conn.cursor()
                    cur.execute("SELECT trail_stop FROM symbol_state WHERE symbol=?", (sym,))
                    r = cur.fetchone()
                    prev = None if r is None else r[0]

                if side == "LONG":
                    new_ts = max(prev or 0.0, last * (1 - step_pct))
                    improved = (prev is None) or (new_ts > prev)
                else:
                    new_ts = min(prev or 1e18, last * (1 + step_pct))
                    improved = (prev is None) or (new_ts < prev)

                if improved:
                    with self.persistence._conn() as conn:
                        cur = conn.cursor()
                        cur.execute(
                            "UPDATE symbol_state SET trail_stop=?, updated_at=? WHERE symbol=?",
                            (new_ts, now, sym)
                        )
                        conn.commit()
                    await self.notifier.debug_trades({
                        "event": "trailing_updated",
                        "symbol": sym,
                        "side": side,
                        "stop": round(new_ts, 4)
                    })
            return

        # GERÇEK mod: ATR tabanlı trailing SL
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
                limit = max(atr_period + 3, 25)
                kl = await self.client.get_klines(sym, interval=stream.tf_entry, limit=limit)
                if not kl or len(kl) < atr_period + 1:
                    continue

                closes = [float(k[4]) for k in kl]
                highs  = [float(k[2]) for k in kl]
                lows   = [float(k[3]) for k in kl]

                atr_val = atr(highs, lows, closes, period=atr_period)
                if not atr_val or math.isnan(atr_val):
                    continue

                last_close = closes[-1]
                if side == "LONG":
                    target = last_close - atr_mult * atr_val
                    opp = "SELL"
                else:
                    target = last_close + atr_mult * atr_val
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