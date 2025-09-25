# /opt/tradebot/future_trade/order_router.py
# -*- coding: utf-8 -*-
from __future__ import annotations

import time, logging, uuid, asyncio
from typing import Optional, Dict, Any, List


class OrderRouter:
    """
    Emirler için tek kapı:
      - Tick/step/minNotional normalizasyonu (ExchangeNormalizer ile)
      - Güvenli payload hazırla
      - Binance çağrısı (tek seferlik güvenli düzeltme + retry politikası)
      - Kapanış (reduceOnly MARKET)
      - Trailing SL upsert (ATR ya da step_pct )
    """

    def __init__(self, config: dict, binance_client, normalizer=None, logger=None, paper_engine=None):
        self.cfg = config or {}
        self.client = binance_client
        self.normalizer = normalizer
        self.logger = logger or logging.getLogger("order_router")
        self.paper_engine = paper_engine
        self._trail_ctx: Dict[str, Any] = {}

        def _current_position_qty(self, symbol: str, close_side: str) -> float:
            """
            close_side: BUY → SHORT kapama, SELL → LONG kapama
            persistence/list_open_positions bağlamı üzerinden mevcut qty'yi döndürür.
            """
            list_pos = self._trail_ctx.get("list_open_positions") if hasattr(self, "_trail_ctx") else None
            if not callable(list_pos):
                return 0.0
            try:
                positions = list_pos() or []
            except Exception:
                return 0.0

            # BUY ile kapatılacaksa pozisyon SHORT; SELL ile kapatılacaksa LONG
            want_side = "SHORT" if close_side.upper() == "BUY" else "LONG"
            for p in positions:
                if p.get("symbol") == symbol and (p.get("side") or "").upper() == want_side:
                    try:
                        return abs(float(p.get("qty", 0) or 0))
                    except Exception:
                        return 0.0
            return 0.0


    # -------------------------------------------------------------------------
    # Trailing context (fiyat/pozisyon/ATR/SL-upsert sağlayıcıları)
    # -------------------------------------------------------------------------
    def attach_trailing_context(
        self,
        get_last_price=None,
        list_open_positions=None,
        upsert_stop=None,
        get_atr=None,
        upsert_tp=None,  
    ):
        self._trail_ctx = {
            "get_last_price": get_last_price,
            "list_open_positions": list_open_positions,
            "upsert_stop": upsert_stop,
            "get_atr": get_atr,
            "upsert_tp": upsert_tp,
        }
        self.logger.info("[TRAIL] context attached")

    # -------------------------------------------------------------------------
    # Yardımcılar
    # -------------------------------------------------------------------------
    @staticmethod
    def _coid(tag: str) -> str:
        """Güvenli clientOrderId üret (aynı anda çoklu çağrılara dayanıklı)."""
        base = (tag or "ORD").upper()[:8]
        return f"{base}-{int(time.time()*1000)%10_000_000:07d}-{uuid.uuid4().hex[:6].upper()}"

    def _last_price(self, symbol: str) -> Optional[float]:
        fn = self._trail_ctx.get("get_last_price") if hasattr(self, "_trail_ctx") else None
        if callable(fn):
            try:
                px = fn(symbol)
                return float(px) if px is not None else None
            except Exception:
                return None
        return None

    @staticmethod
    def _parse_err(exc: Exception) -> Dict[str, Any]:
        """
        Binance hata iletilerinde geçen olası kodları ayıklar.
        Sık görülür: -1013(precision), -4003(minNotional), -2010(immediately trigger),
                     -2022(reduceOnly reject), -4164(reduceOnly), -2011(cancel nosuch), -2021(reject)
        """
        s = str(exc)
        code = None
        for tok in ("-1013", "-4003", "-2010", "-2022", "-4164", "-2011", "-2021"):
            if tok in s:
                try:
                    code = int(tok)
                    break
                except Exception:
                    pass
        return {"code": code, "msg": s}

    # -------------------------------------------------------------------------
    # Hata bazlı tek-seferlik güvenli düzeltme
    # -------------------------------------------------------------------------
    def _adjust_on_error(
    self,
    symbol: str,
    side: str,
    qty: float,
    price: Optional[float],
    order_type: str,
    reduce_only: bool,
    err: Dict[str, Any],
) -> Dict[str, Any]:
        """
        Sadece güvenli senaryolarda ayar yapar, aksi halde no_retry=True döner.

        - -1013 precision/lot/tick: normalizer ile tekrar düzelt
        - -4003 MIN_NOTIONAL: (reduceOnly=False ise) qty'yi minNotional için yükselt
        - -2010 would immediately trigger: STOP* emirlerde stopPrice'ı 1 tick uzağa kaydır
        - -2022 / -4164 reduceOnly reddi: pozisyon miktarına göre kırp
        """
        msg = (err.get("msg") or "").lower()
        code = err.get("code")
        n_price, n_qty = price, qty
        no_retry = False

        # 1) precision/step/tick
        if code == -1013 or "precision" in msg or "lot size" in msg or "tick size" in msg:
            if self.normalizer:
                self.logger.info(f"[RETRY] normalize by -1013 for {symbol}")
                norm = self.normalizer.normalize_order(symbol, side, qty, price, order_type, reduce_only)
                n_price = norm["price"] if price is not None else None
                n_qty = norm["qty"]
            else:
                no_retry = True

        # 2) minNotional
        elif code == -4003 or "min notional" in msg or "min_notional" in msg:
            if not reduce_only and self.normalizer:
                px_for_notional = n_price if n_price is not None else (self._last_price(symbol) or 0.0)
                if px_for_notional > 0:
                    self.logger.info(f"[RETRY] ensure minNotional for {symbol}")
                    n_qty = self.normalizer.ensure_min_notional(symbol, px_for_notional, n_qty)
                else:
                    no_retry = True
            else:
                no_retry = True

        # 3) would immediately trigger (STOP/TP)
        elif code == -2010 or "would immediately trigger" in msg:
            if order_type and "STOP" in order_type.upper():
                if self.normalizer and n_price is not None:
                    tick = self.normalizer.steps(symbol)["tick"]
                    if tick > 0:
                        delta = tick
                        n_price = (n_price + delta) if side.upper() == "BUY" else max(0.0, n_price - delta)
                        self.logger.info(f"[RETRY] move stopPrice by +1 tick for {symbol}")
                    else:
                        no_retry = True
                else:
                    no_retry = True
            else:
                no_retry = True

        # 4) reduceOnly reject: -2022 / -4164
        elif code in (-2022, -4164) or "reduceonly" in msg:
            if reduce_only:
                avail = self._current_position_qty(symbol, side)
                if avail > 0:
                    n_qty = min(float(avail), float(qty))
                    if self.normalizer:
                        n_qty = self.normalizer.normalize_qty(symbol, n_qty)
                    if n_qty <= 0:
                        no_retry = True
                else:
                    no_retry = True
            else:
                no_retry = True

        else:
            no_retry = True

        return {"price": n_price, "qty": n_qty, "no_retry": no_retry}

    # -------------------------------------------------------------------------
    # Ana: Emir Aç (tek retry politikası ile)
    # -------------------------------------------------------------------------
    def place_order(
        self,
        symbol: str,
        side: str,
        qty: float,
        price: float = None,
        order_type: str = "MARKET",
        reduce_only: bool = False,
        tag: str = "entry",
        time_in_force: str = None,
        stop_price: float = None,
    ) -> Dict[str, Any]:
        """
        Son kapı:
          - normalizasyon (tick/step/minNotional)
          - payload hazırlama
          - Binance çağrısı
          - hata halinde güvenli düzeltme + tek retry
        """
        side = side.upper()
        order_type = (order_type or "MARKET").upper()
        tif = time_in_force or (self.cfg.get("order", {}) or {}).get("time_in_force", "GTC")

        # 1) Ön normalizasyon
        price_n = price
        qty_n = qty
        last_px = self._last_price(symbol) if (price is None and order_type == "MARKET") else None

        if self.normalizer:
            norm = self.normalizer.normalize_order(symbol, side, qty_n, price_n, order_type, reduce_only)
            price_n = norm["price"] if price is not None else None
            qty_n = norm["qty"]

            # MARKET minNotional kontrolü (entry tarafında)
            if not reduce_only:
                px_for_notional = price_n if price_n is not None else (last_px or 0.0)
                if px_for_notional > 0:
                    qty_n = self.normalizer.ensure_min_notional(symbol, px_for_notional, qty_n)

        if qty_n is None or qty_n <= 0:
            raise ValueError("Normalized quantity is zero or invalid")

        # 2) Payload
        client_oid = self._coid(tag)
        payload = {
            "symbol": symbol,
            "side": side,
            "type": order_type,
            "quantity": float(f"{qty_n:.10f}"),
            "reduceOnly": bool(reduce_only),
            "timeInForce": tif,
            "newClientOrderId": client_oid,
        }
        if price is not None:
            if price_n is None:
                raise ValueError("Non-market order requires normalized price")
            payload["price"] = float(f"{price_n:.10f}")

        if order_type in ("STOP", "STOP_MARKET", "STOP_LIMIT", "TAKE_PROFIT", "TAKE_PROFIT_MARKET", "TAKE_PROFIT_LIMIT"):
            sp = stop_price if stop_price is not None else price_n
            if sp is None:
                sp = last_px  # MARKET + stop_price gelmediyse son fiyat
            if sp is None:
                raise ValueError("stop order requires stopPrice")
            payload["stopPrice"] = float(f"{sp:.10f}")
            working_type = (self.cfg.get("order", {}) or {}).get("sl_working_type", "MARK_PRICE")
            payload["workingType"] = working_type

        if order_type in ("LIMIT", "STOP_LIMIT", "TAKE_PROFIT_LIMIT"):
            payload["timeInForce"] = tif

        # 3) Çağrı (tek retry’lı)
        def _maybe_await(res):
            # client.new_order async olabilir; sync wrapper
            if asyncio.iscoroutine(res):
                # zaten event loop içindeyiz; task'a bırak ve bekle
                return asyncio.get_running_loop().run_until_complete(res)  # dıştan sync çağrı gelebilir
            return res

        def _call(p):
            try:
                ret = self.client.new_order(**p)
                return _maybe_await(ret)
            except RuntimeError:
                # Eğer run_until_complete loop içinde yasaksa, client zaten sync'tir
                return self.client.new_order(**p)

        try:
            res = _call(payload)
            return res
        except Exception as e:
            err = self._parse_err(e)
            self.logger.warning(
                f"[ORDER-ERR-1] {symbol} {side} {order_type} qty={qty_n} price={price_n} reduceOnly={reduce_only} : {err}"
            )

            adj = self._adjust_on_error(symbol, side, qty_n, price_n, order_type, reduce_only, err)
            if adj.get("no_retry", False):
                raise

            price2 = adj["price"]
            qty2 = adj["qty"]
            if qty2 is None or qty2 <= 0:
                raise

            payload2 = dict(payload)
            payload2["quantity"] = float(f"{qty2:.10f}")
            if price is not None:
                if price2 is None:
                    raise
                payload2["price"] = float(f"{price2:.10f}")
            # stopPrice ayarlaması gerekebilir
            if "stopPrice" in payload2 and order_type in (
                "STOP", "STOP_MARKET", "STOP_LIMIT", "TAKE_PROFIT", "TAKE_PROFIT_MARKET", "TAKE_PROFIT_LIMIT"
            ):
                if price is not None and price2 is not None:
                    payload2["stopPrice"] = float(f"{price2:.10f}")

            payload2["newClientOrderId"] = self._coid(f"{tag}-R")

            try:
                self.logger.info(f"[ORDER-RETRY] {symbol} {side} {order_type} qty={qty2} price={price2}")
                res2 = _call(payload2)
                return res2
            except Exception as e2:
                err2 = self._parse_err(e2)
                self.logger.error(
                    f"[ORDER-ERR-2] {symbol} {side} {order_type} qty={qty2} price={price2} : {err2}"
                )
                raise

    # -------------------------------------------------------------------------
    # Kapanış: reduceOnly MARKET
    # -------------------------------------------------------------------------
    def close_position_market(self, symbol: str, side: str, qty: float, tag: str = "close") -> Dict[str, Any]:
        """
        Pozisyonu piyasa fiyatından kapatmak için ters yönde reduceOnly MARKET.
        side: "BUY" → long kapamak için "SELL" kullan; "SELL" → short kapamak için "BUY".
        """
        side = side.upper()
        if side not in ("BUY", "SELL"):
            raise ValueError("close_position_market: side must be BUY or SELL")

        # qty normalizasyonu — reduceOnly olduğu için minNotional yükseltmesi yapmayız
        qty_n = qty
        if self.normalizer:
            qty_n = self.normalizer.normalize_qty(symbol, qty_n)

        # ✅ Opsiyonel: close öncesi mevcut qty'ye kırp
        avail = self._current_position_qty(symbol, side)
        if avail > 0:
            qty_n = min(qty_n, avail)

        if qty_n is None or qty_n <= 0:
            raise ValueError("close_position_market: normalized qty <= 0")

        payload = {
            "symbol": symbol,
            "side": side,
            "type": "MARKET",
            "quantity": float(f"{qty_n:.10f}"),
            "reduceOnly": True,
            "newClientOrderId": self._coid(tag),
        }

        def _maybe_await(res):
            if asyncio.iscoroutine(res):
                return asyncio.get_running_loop().run_until_complete(res)
            return res

        try:
            ret = self.client.new_order(**payload)
            return _maybe_await(ret)
        except Exception as e:
            err = self._parse_err(e)
            self.logger.error(f"[CLOSE-ERR] {symbol} {side} qty={qty_n} : {err}")
            raise


    # -------------------------------------------------------------------------
    # Emir iptali (varsa async client ile uyumlu)
    # -------------------------------------------------------------------------
    def cancel_order(self, symbol: str, order_id: int = None, client_order_id: str = None) -> Dict[str, Any]:
        """
        Var olan bir emri iptal et. Client'ın async olması ihtimaline karşı uyumlu çağırır.
        """
        kwargs = {"symbol": symbol}
        if order_id is not None:
            kwargs["orderId"] = int(order_id)
        if client_order_id:
            kwargs["origClientOrderId"] = str(client_order_id)

        async def _call_async():
            return await self.client.cancel_order(**kwargs)

        try:
            ret = self.client.cancel_order(**kwargs)
            if asyncio.iscoroutine(ret):
                return asyncio.get_running_loop().run_until_complete(ret)
            return ret
        except TypeError:
            # bazı client sürümleri sadece async
            return asyncio.get_running_loop().run_until_complete(_call_async())
        except Exception as e:
            err = self._parse_err(e)
            self.logger.error(f"[CANCEL-ERR] {symbol} {kwargs} : {err}")
            raise

    # -------------------------------------------------------------------------
    # Trailing SL güncelleme (ATR varsa ATR, yoksa step_pct)
    # -------------------------------------------------------------------------
    def update_trailing_for_open_positions(self) -> None:
        """
        Açık pozisyonlar için yeni stop seviyesini hesaplar ve upsert fonksiyonunu çağırır.
        - LONG: stop = last - atr_mult*ATR   (fallback: last*(1 - step_pct/100))
        - SHORT: stop = last + atr_mult*ATR  (fallback: last*(1 + step_pct/100))
        """
        ctx = self._trail_ctx or {}
        get_px = ctx.get("get_last_price")
        list_pos = ctx.get("list_open_positions")
        upsert = ctx.get("upsert_stop")
        get_atr = ctx.get("get_atr")

        if not callable(list_pos) or not callable(get_px) or not callable(upsert):
            self.logger.debug("[TRAIL] context incomplete; skip")
            return

        tr_cfg = (self.cfg.get("trailing") or {})
        tr_type = (tr_cfg.get("type") or "step_pct").lower()
        atr_period = int(tr_cfg.get("atr_period", 14))
        atr_mult = float(tr_cfg.get("atr_mult", 2.5))
        step_pct = float(tr_cfg.get("step_pct", 0.1))

        positions: List[Dict[str, Any]] = []
        try:
            positions = list_pos() or []
        except Exception as e:
            self.logger.debug(f"[TRAIL] list positions failed: {e}")
            return

        for p in positions:
            try:
                sym = p.get("symbol")
                side_pos = (p.get("side") or "").upper()
                if side_pos not in ("LONG", "SHORT"):
                    continue

                last = get_px(sym)
                last = float(last) if last is not None else None
                if last is None or last <= 0:
                    continue

                new_stop = None
                if tr_type == "atr" and callable(get_atr):
                    try:
                        atr_val = get_atr(sym, atr_period)
                        if atr_val and atr_val > 0:
                            if side_pos == "LONG":
                                new_stop = last - atr_mult * float(atr_val)
                            else:
                                new_stop = last + atr_mult * float(atr_val)
                    except Exception:
                        new_stop = None

                # Fallback: step_pct
                if new_stop is None:
                    if side_pos == "LONG":
                        new_stop = last * (1.0 - step_pct / 100.0)
                    else:
                        new_stop = last * (1.0 + step_pct / 100.0)

                # Stop upsert çağrısı (reduceOnly STOP_MARKET)
                stop_side = "SELL" if side_pos == "LONG" else "BUY"
                upsert(sym, stop_side, float(new_stop))
            except Exception as e:
                self.logger.debug(f"[TRAIL] update error for {p}: {e}")
