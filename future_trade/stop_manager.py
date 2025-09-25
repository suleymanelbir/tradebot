# /opt/tradebot/future_trade/stop_manager.py
# -*- coding: utf-8 -*-
from __future__ import annotations

import time, logging, asyncio
from typing import Optional, Dict, Any, List


class StopManager:
    """
    Trailing SL yönetimi (replace destekli):
      - Mevcut reduceOnly STOP* emirlerini listeler
      - Yeni stop mesafesi anlamlı hareket ettiyse (min_move_pct) ve "debounce" süresi geçtiyse:
          * mevcut STOP iptal edilir
          * yeni STOP_MARKET reduceOnly gönderilir
      - Amaç: borsa tarafında her zaman tek ve güncel bir SL tutmak
    """

    def __init__(self, router, persistence, cfg: Dict[str, Any], logger=None):
        self.router = router
        self.persistence = persistence
        self.cfg = cfg or {}
        self.logger = logger or logging.getLogger("stop_manager")

        # Debounce hafızası: {symbol: last_epoch_sec}
        self._last_upsert_ts: Dict[str, float] = {}

    # ---------- yardımcılar ----------
    def _last_price(self, symbol: str) -> Optional[float]:
        fn = getattr(self.router, "_last_price", None)
        if callable(fn):
            try:
                return float(fn(symbol))
            except Exception:
                return None
        return None

    def _list_open_orders_sync(self, symbol: str) -> List[Dict[str, Any]]:
        """
        client.list_open_orders async olabilir; burada sync ortamda güvenli şekilde çağır.
        """
        client = getattr(self.router, "client", None)
        if client is None:
            return []
        try:
            coro = client.list_open_orders(symbol=symbol)
            if asyncio.iscoroutine(coro):
                loop = asyncio.get_running_loop()
                return loop.run_until_complete(coro)
            return coro
        except RuntimeError:
            # run_until_complete mevcut loop içinde yasaksa, client sync kabul edilip tekrar denenir
            try:
                return client.list_open_orders(symbol=symbol)
            except Exception as e:
                self.logger.debug(f"list_open_orders failed: {e}")
                return []
        except Exception as e:
            self.logger.debug(f"list_open_orders error: {e}")
            return []

    def _find_existing_stop(self, symbol: str, side_close: str) -> Optional[Dict[str, Any]]:
        """
        Aynı sembol ve kapanış yönünde reduceOnly STOP* emrini bulur.
        """
        open_orders = self._list_open_orders_sync(symbol)
        if not open_orders:
            return None
        side_close = side_close.upper()
        for o in open_orders:
            try:
                typ = (o.get("type") or "").upper()
                if "STOP" not in typ and "TAKE_PROFIT" not in typ:
                    continue
                if not bool(o.get("reduceOnly", False)):
                    continue
                if (o.get("side") or "").upper() != side_close:
                    continue
                return o
            except Exception:
                continue
        return None

    def _should_replace(self, symbol: str, new_stop: float, old_stop: float, min_move_pct: float, debounce_sec: int) -> bool:
        """
        Hem "anlamlı hareket" hem de "debounce" koşulu sağlanıyorsa True.
        """
        now = time.time()
        last_t = self._last_upsert_ts.get(symbol, 0.0)
        if now - last_t < max(0, debounce_sec):
            return False

        last_px = self._last_price(symbol) or 0.0
        if last_px <= 0:
            return True  # fiyatı alamadıysak mesafe karşılaştırması yapamıyoruz → temkinli replace

        # fiyat bazlı yüzde fark (daha stabil ölçüt)
        pct = abs(new_stop - old_stop) / last_px * 100.0
        return pct >= max(0.0, min_move_pct)

    def _position_qty_for_close(self, symbol: str, side_close: str) -> float:
        """
        Router'ın mevcut qty bulucu yardımcı fonksiyonu üzerinden miktarı al.
        """
        fn = getattr(self.router, "_current_position_qty", None)
        if callable(fn):
            try:
                return float(fn(symbol, side_close))
            except Exception:
                return 0.0
        return 0.0

    # ---------- ana API ----------
    def upsert_stop_loss(self, symbol: str, side_close: str, stop_price: float) -> Optional[Dict[str, Any]]:
        """
        Yeni SL hedefi verildiğinde:
          - mevcut reduceOnly STOP* emri bulunur
          - replace kriterleri (min_move_pct, debounce_sec) sağlanıyorsa: cancel + yeni STOP_MARKET
          - yoksa yeni STOP_MARKET direkt gönderilir
        """
        tr_cfg = (self.cfg.get("trailing") or {})
        min_move_pct = float(tr_cfg.get("min_move_pct", 0.05))   # %0.05 default
        debounce_sec = int(tr_cfg.get("debounce_sec", 15))       # 15s default

        side_close = side_close.upper()
        if side_close not in ("BUY", "SELL"):
            self.logger.error(f"[SL] invalid side_close: {side_close}")
            return None

        if stop_price is None or stop_price <= 0:
            self.logger.error(f"[SL] invalid stop_price for {symbol}: {stop_price}")
            return None

        # 1) Mevcut STOP* var mı?
        existed = self._find_existing_stop(symbol, side_close)
        if existed:
            try:
                old_sp = float(existed.get("stopPrice") or existed.get("price") or 0.0)
            except Exception:
                old_sp = 0.0
        else:
            old_sp = 0.0

        # 2) Kapatma miktarı (reduceOnly için aktif pozisyon kadar)
        qty = self._position_qty_for_close(symbol, side_close)
        if qty <= 0:
            self.logger.debug(f"[SL] no position to protect for {symbol}")
            return None

        # 3) Replace mi, yeni mi?
        do_replace = False
        if existed and old_sp > 0:
            do_replace = self._should_replace(symbol, float(stop_price), old_sp, min_move_pct, debounce_sec)

        # 4) Replace akışı
        if existed and do_replace:
            try:
                oid = existed.get("orderId")
                coid = existed.get("clientOrderId")
                if oid or coid:
                    self.router.cancel_order(symbol=symbol, order_id=oid, client_order_id=coid)
                    self.logger.info(f"[SL] cancel old STOP for {symbol} ({old_sp})")
            except Exception as e:
                self.logger.warning(f"[SL] cancel failed for {symbol}: {e}")

        # 5) Yeni SL emri (ya ilk defa ya da replace sonrası)
        try:
            res = self.router.place_order(
                symbol=symbol,
                side=side_close,
                qty=qty,
                order_type="STOP_MARKET",
                stop_price=float(stop_price),
                reduce_only=True,
                tag="SL"
            )
            self._last_upsert_ts[symbol] = time.time()
            self.logger.info(f"[SL] upsert {symbol} {side_close} stop={stop_price} qty={qty} replace={bool(existed and do_replace)}")
            return res
        except Exception as e:
            self.logger.error(f"[SL] upsert failed for {symbol}: {e}")
            return None
