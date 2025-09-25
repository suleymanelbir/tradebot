# /opt/tradebot/future_trade/take_profit_manager.py
# -*- coding: utf-8 -*-
from __future__ import annotations

import time, logging, asyncio
from typing import Optional, Dict, Any, List, Tuple



class TakeProfitManager:
    """
    TP (take profit) yönetimi (replace destekli):
      - Mevcut reduceOnly TAKE_PROFIT* emirlerini listeler
      - Hedef anlamlı değişmişse (min_move_pct) ve "debounce" süresi geçmişse:
          * mevcut TP iptal edilir
          * yeni TAKE_PROFIT_MARKET reduceOnly gönderilir
    Modlar:
      - percent: entry'ye göre % hedef
      - rr: risk/ödül (entry, SL ve RR katsayısı ile)
        * İsteğe bağlı: orderbook likiditesine "snap" (asks/bids)
    """

    def __init__(
        self,
        router,
        persistence,
        cfg: Dict[str, Any],
        logger=None,
        orderbook_provider=None,  # callable(symbol:str, depth:int) -> dict{"bids":[(p,q)..], "asks":[(p,q)..]}
    ):
        self.router = router
        self.persistence = persistence
        self.cfg = cfg or {}
        self.logger = logger or logging.getLogger("tp_manager")
        self._last_upsert_ts: Dict[str, float] = {}
        self._orderbook = orderbook_provider  # async/sync toleranslı

    # -------------------- yardımcılar --------------------
    def _last_price(self, symbol: str) -> Optional[float]:
        fn = getattr(self.router, "_last_price", None)
        if callable(fn):
            try:
                return float(fn(symbol))
            except Exception:
                return None
        return None

    def _list_open_orders_sync(self, symbol: str) -> List[Dict[str, Any]]:
        client = getattr(self.router, "client", None)
        if client is None:
            return []
        try:
            coro = client.list_open_orders(symbol=symbol)
            if asyncio.iscoroutine(coro):
                return asyncio.get_running_loop().run_until_complete(coro)
            return coro
        except RuntimeError:
            try:
                return client.list_open_orders(symbol=symbol)
            except Exception as e:
                self.logger.debug(f"list_open_orders failed: {e}")
                return []
        except Exception as e:
            self.logger.debug(f"list_open_orders error: {e}")
            return []

    def _find_existing_tp(self, symbol: str, side_close: str) -> Optional[Dict[str, Any]]:
        open_orders = self._list_open_orders_sync(symbol)
        if not open_orders:
            return None
        side_close = side_close.upper()
        for o in open_orders:
            try:
                typ = (o.get("type") or "").upper()
                if "TAKE_PROFIT" not in typ:
                    continue
                if not bool(o.get("reduceOnly", False)):
                    continue
                if (o.get("side") or "").upper() != side_close:
                    continue
                return o
            except Exception:
                continue
        return None

    def _should_replace(self, symbol: str, new_tp: float, old_tp: float, min_move_pct: float, debounce_sec: int) -> bool:
        now = time.time()
        last_t = self._last_upsert_ts.get(symbol, 0.0)
        if now - last_t < max(0, debounce_sec):
            return False
        last_px = self._last_price(symbol) or 0.0
        if last_px <= 0:
            return True
        pct = abs(new_tp - old_tp) / last_px * 100.0
        return pct >= max(0.0, min_move_pct)

    def _position_side_entry_sl(self, symbol: str) -> Tuple[Optional[str], Optional[float], Optional[float]]:
        """
        Açık pozisyondan: (side, entry_price, sl) döner. SL yoksa None.
        list_open_positions() içinde p["sl"] / p["stop"] / p["stop_price"] alanlarından birini kabul eder.
        """
        list_pos = getattr(self.router, "_trail_ctx", {}).get("list_open_positions")
        if not callable(list_pos):
            return None, None, None
        try:
            for p in (list_pos() or []):
                if p.get("symbol") != symbol:
                    continue
                side = (p.get("side") or "").upper()
                entry = p.get("entry_price") or p.get("entry") or p.get("avg_entry_price")
                sl = p.get("sl") or p.get("stop") or p.get("stop_price")
                entry = float(entry) if entry else None
                sl = float(sl) if sl else None
                return side, entry, sl
        except Exception:
            pass
        return None, None, None

    def _position_qty_for_close(self, symbol: str, side_close: str) -> float:
        fn = getattr(self.router, "_current_position_qty", None)
        if callable(fn):
            try:
                return float(fn(symbol, side_close))
            except Exception:
                return 0.0
        return 0.0

    # -------------------- orderbook helpers --------------------
    async def _orderbook_async(self, symbol: str, depth: int) -> Optional[Dict[str, List[Tuple[float, float]]]]:
        if not callable(self._orderbook):
            return None
        try:
            res = self._orderbook(symbol, depth)
            if asyncio.iscoroutine(res):
                res = await res
            # normalize
            asks = [(float(p), float(q)) for p, q in (res.get("asks") or []) if float(q) > 0]
            bids = [(float(p), float(q)) for p, q in (res.get("bids") or []) if float(q) > 0]
            # sıralama garantisi: asks artan, bids azalan (Binance zaten böyle döndürür)
            asks.sort(key=lambda x: x[0])
            bids.sort(key=lambda x: x[0], reverse=True)
            return {"asks": asks, "bids": bids}
        except Exception as e:
            self.logger.debug(f"orderbook fetch failed: {e}")
            return None

    @staticmethod
    def _find_nearest_liquidity(target: float, levels: List[Tuple[float, float]], side_pos: str, snap: str) -> Optional[float]:
        """
        levels: [(price, qty), ...]
        snap: 'nearest' → |p-target| en küçük
              'favorable' → hedef yönünde en az hedef kadar “kârlı” seviye
                 LONG (SELL close): price >= target (asks)
                 SHORT (BUY  close): price <= target (bids)
              uygun seviye yoksa 'nearest'a düşer
        """
        if not levels:
            return None
        if snap == "nearest":
            # mutlak fark en küçük
            best = min(levels, key=lambda x: abs(x[0] - target))
            return best[0]
        snap = snap.lower()
        if side_pos == "LONG":
            # SELL ile kapanacak → asks tarafı, hedefin ÜSTÜ "favorable"
            cands = [p for p, q in levels if p >= target]
            if cands:
                return cands[0]  # en yakın üst seviye
        else:
            # SHORT → BUY ile kapanacak → bids tarafı, hedefin ALTI "favorable"
            cands = [p for p, q in levels if p <= target]
            if cands:
                return cands[0]  # en yakın alt seviye
        # fallback
        best = min(levels, key=lambda x: abs(x[0] - target))
        return best[0]

    # -------------------- hedef hesaplayıcıları --------------------
    def _compute_target_percent(self, symbol: str, side_pos: str, entry: float) -> Optional[float]:
        tp_cfg = (self.cfg.get("take_profit") or {})
        pct = float(tp_cfg.get("pct", 0.5))
        if side_pos == "LONG":
            return entry * (1.0 + pct / 100.0)
        elif side_pos == "SHORT":
            return entry * (1.0 - pct / 100.0)
        return None

    async def _compute_target_rr(self, symbol: str, side_pos: str, entry: float, sl: float) -> Optional[float]:
        """
        RR hedef:
          LONG : target = entry + rr * |entry - sl|
          SHORT: target = entry - rr * |entry - sl|
        Orderbook snapping:
          - LONG: asks,  snap policy 'nearest'/'favorable'
          - SHORT: bids, snap policy 'nearest'/'favorable'
        """
        tp_cfg = (self.cfg.get("take_profit") or {})
        rr = float(tp_cfg.get("rr", 1.5))
        book_depth = int(tp_cfg.get("book_depth", 50))
        snap = (tp_cfg.get("snap", "nearest") or "nearest").lower()

        raw_tp = None
        try:
            dist = abs(float(entry) - float(sl))
            if dist <= 0:
                return None
            if side_pos == "LONG":
                raw_tp = float(entry) + rr * dist
            elif side_pos == "SHORT":
                raw_tp = float(entry) - rr * dist
            else:
                return None
        except Exception:
            return None

        # Orderbook ile snap
        ob = await self._orderbook_async(symbol, book_depth)
        if not ob:
            return raw_tp  # fallback: doğrudan raw_tp

        levels = ob["asks"] if side_pos == "LONG" else ob["bids"]
        snapped = self._find_nearest_liquidity(raw_tp, levels, side_pos, snap)
        return snapped or raw_tp

    # -------------------- ana hesaplayıcı --------------------
    async def _compute_target(self, symbol: str) -> Optional[Tuple[str, float]]:
        """
        Dönüş: (side_close, tp_target)
          side_close: LONG için SELL, SHORT için BUY
        """
        side_pos, entry, sl = self._position_side_entry_sl(symbol)
        if side_pos not in ("LONG", "SHORT") or not entry:
            return None

        side_close = "SELL" if side_pos == "LONG" else "BUY"
        tp_cfg = (self.cfg.get("take_profit") or {})
        mode = (tp_cfg.get("mode") or "percent").lower()

        target = None
        if mode == "percent":
            target = self._compute_target_percent(symbol, side_pos, entry)
        elif mode == "rr":
            # SL yoksa RR hesaplanamaz → fallback: None (TP upsert atlanır)
            if sl is None or sl <= 0:
                return None
            target = await self._compute_target_rr(symbol, side_pos, entry, sl)
        else:
            # bilinmeyen mod → hiç bir şey yapma
            return None

        if target and target > 0:
            return side_close, float(target)
        return None

    # -------------------- ana API --------------------
    def upsert_take_profit(self, symbol: str, side_close: str, tp_price: float) -> Optional[Dict[str, Any]]:
        """
        Yeni TP hedefi verildiğinde:
        1) mevcut reduceOnly TAKE_PROFIT* emri bulunur
        2) replace kriterleri (min_move_pct, debounce_sec) sağlanıyorsa: cancel + yeni TAKE_PROFIT_MARKET
        3) yoksa yeni TAKE_PROFIT_MARKET direkt gönderilir
        4) başarılı upsert sonrası TP seviyesi ve orderId cache'e yazılır
        """
        # 0) TP yapılandırma parametreleri
        tp_cfg = (self.cfg.get("take_profit") or {})
        min_move_pct = float(tp_cfg.get("min_move_pct", 0.05))   # %0.05 default
        debounce_sec = int(tp_cfg.get("debounce_sec", 15))       # 15s default

        # 1) Giriş doğrulama
        side_close = side_close.upper()
        if side_close not in ("BUY", "SELL"):
            self.logger.error(f"[TP] invalid side_close: {side_close}")
            return None

        if tp_price is None or tp_price <= 0:
            self.logger.error(f"[TP] invalid target for {symbol}: {tp_price}")
            return None

        # 2) Mevcut TP emri var mı?
        existed = self._find_existing_tp(symbol, side_close)
        old_tp = 0.0
        if existed:
            try:
                old_tp = float(existed.get("stopPrice") or existed.get("price") or 0.0)
            except Exception:
                old_tp = 0.0

        # 3) Kapatılacak miktar (aktif pozisyon)
        qty = self._position_qty_for_close(symbol, side_close)
        if qty <= 0:
            self.logger.debug(f"[TP] no position to protect for {symbol}")
            return None

        # 4) Replace mi, yeni mi?
        do_replace = False
        if existed and old_tp > 0:
            do_replace = self._should_replace(symbol, float(tp_price), old_tp, min_move_pct, debounce_sec)

        # 5) Replace gerekiyorsa eski TP emrini iptal et
        if existed and do_replace:
            try:
                oid = existed.get("orderId")
                coid = existed.get("clientOrderId")
                if oid or coid:
                    self.router.cancel_order(symbol=symbol, order_id=oid, client_order_id=coid)
                    self.logger.info(f"[TP] cancel old TP for {symbol} ({old_tp})")
            except Exception as e:
                self.logger.warning(f"[TP] cancel failed for {symbol}: {e}")

        # 6) Yeni TAKE_PROFIT_MARKET emrini gönder
        try:
            res = self.router.place_order(
                symbol=symbol,
                side=side_close,
                qty=qty,
                order_type="TAKE_PROFIT_MARKET",
                stop_price=float(tp_price),
                reduce_only=True,
                tag="TP"
            )

            # 6.1) orderId bilgisini al
            order_id = None
            try:
                order_id = str(res.get("orderId") or res.get("order_id") or "")
            except Exception:
                order_id = None

            # 6.2) zaman damgası ve log
            self._last_upsert_ts[symbol] = time.time()
            self.logger.info(f"[TP] upsert {symbol} {side_close} tp={tp_price} qty={qty} replace={bool(existed and do_replace)}")

            # 6.3) cache: TP seviyesi ve orderId
            if hasattr(self.persistence, "cache_update_tp"):
                try:
                    self.persistence.cache_update_tp(symbol, float(tp_price))
                except Exception:
                    pass

            if hasattr(self.persistence, "cache_update_tp_order_id") and order_id:
                try:
                    self.persistence.cache_update_tp_order_id(symbol, order_id)
                except Exception:
                    pass

            return res
        except Exception as e:
            # 7) Emir gönderimi başarısızsa logla
            self.logger.error(f"[TP] upsert failed for {symbol}: {e}")
            return None

    # -------------------- periyodik (otomatik) --------------------
    async def run(self, stop_event: asyncio.Event = None, poll_sec: int = None):
        """
        Mod:
          - 'percent': entry'ye göre % hedef
          - 'rr'     : entry/SL ve RR ile hedef (orderbook snap opsiyonlu)
        """
        tp_cfg = (self.cfg.get("take_profit") or {})
        if poll_sec is None:
            poll_sec = int(tp_cfg.get("update_interval_sec", 30))

        while not (stop_event and stop_event.is_set()):
            try:
                # aktif pozisyonlu semboller
                list_pos = getattr(self.router, "_trail_ctx", {}).get("list_open_positions")
                syms = []
                if callable(list_pos):
                    try:
                        for p in (list_pos() or []):
                            if (p.get("side") or "").upper() in ("LONG", "SHORT"):
                                syms.append(p.get("symbol"))
                    except Exception:
                        pass
                syms = sorted(set([s for s in syms if s]))

                for sym in syms:
                    comp = await self._compute_target(sym)
                    if not comp:
                        continue
                    side_close, target = comp
                    self.upsert_take_profit(sym, side_close, target)
            except Exception as e:
                self.logger.error(f"TP loop error: {e}")
            await asyncio.sleep(max(5, poll_sec))
