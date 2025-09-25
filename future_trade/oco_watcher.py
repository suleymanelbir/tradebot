# /opt/tradebot/future_trade/oco_watcher.py
# -*- coding: utf-8 -*-
from __future__ import annotations
import asyncio, logging
from typing import Dict, Any, List


class OCOWatcher:
    """
    OCO davranışı:
      - Bir sembolde SL veya TP ortadan kaybolduysa (fill/iptal),
        diğeri otomatik iptal edilir.
      - Pozisyon yoksa ProtectiveSweeper zaten temizlik yapıyor; burada
        açık pozisyon varken tetiklenmeyi ele alıyoruz.
    """

    def __init__(self, router, persistence, logger=None, interval_sec: int = 5):
        self.router = router
        self.persistence = persistence
        self.logger = logger or logging.getLogger("oco_watcher")
        self.interval = max(3, int(interval_sec))

    async def _list_open_orders(self, symbol: str) -> List[Dict[str, Any]]:
        client = getattr(self.router, "client", None)
        if client is None:
            return []
        try:
            res = client.list_open_orders(symbol=symbol)
            if asyncio.iscoroutine(res):
                return await res
            return res
        except Exception as e:
            self.logger.debug(f"open_orders error for {symbol}: {e}")
            return []

    def _has_order_id(self, orders: List[Dict[str, Any]], order_id: str) -> bool:
        if not order_id:
            return False
        for o in orders or []:
            try:
                if str(o.get("orderId") or o.get("order_id") or "") == str(order_id):
                    return True
            except Exception:
                continue
        return False

    async def _process_symbol(self, p: Dict[str, Any]) -> None:
        symbol = p.get("symbol")
        side = (p.get("side") or "").upper()
        qty = abs(float(p.get("qty", 0) or 0))
        if not symbol or side not in ("LONG", "SHORT") or qty <= 0:
            return

        orders = await self._list_open_orders(symbol)
        sl_oid = p.get("sl_order_id")
        tp_oid = p.get("tp_order_id")

        sl_alive = self._has_order_id(orders, sl_oid)
        tp_alive = self._has_order_id(orders, tp_oid)

        # 1) SL yok olduysa, TP'yi iptal et
        if sl_oid and not sl_alive and tp_alive and tp_oid:
            try:
                self.router.cancel_order(symbol=symbol, order_id=None, client_order_id=None)  # fallback
                # hedef TP order'ını nokta atışı iptal
                self.router.cancel_order(symbol=symbol, order_id=int(tp_oid))
            except Exception as e:
                self.logger.debug(f"[OCO] cancel TP failed {symbol}: {e}")
            # cache temizliği
            if hasattr(self.persistence, "cache_update_tp_order_id"):
                try:
                    self.persistence.cache_update_tp_order_id(symbol, None)
                except Exception:
                    pass
            if hasattr(self.persistence, "cache_update_tp"):
                try:
                    self.persistence.cache_update_tp(symbol, None)
                except Exception:
                    pass
            self.logger.info(f"[OCO] SL filled/removed → TP cancelled for {symbol}")

        # 2) TP yok olduysa, SL'yi iptal et
        if tp_oid and not tp_alive and sl_alive and sl_oid:
            try:
                self.router.cancel_order(symbol=symbol, order_id=int(sl_oid))
            except Exception as e:
                self.logger.debug(f"[OCO] cancel SL failed {symbol}: {e}")
            if hasattr(self.persistence, "cache_update_sl_order_id"):
                try:
                    self.persistence.cache_update_sl_order_id(symbol, None)
                except Exception:
                    pass
            if hasattr(self.persistence, "cache_update_sl"):
                try:
                    self.persistence.cache_update_sl(symbol, None)
                except Exception:
                    pass
            self.logger.info(f"[OCO] TP filled/removed → SL cancelled for {symbol}")

    async def run(self, stop_event: asyncio.Event = None):
        while not (stop_event and stop_event.is_set()):
            try:
                # sadece açık pozisyonları hedefle
                positions = []
                try:
                    positions = self.persistence.list_open_positions() or []
                except Exception:
                    pass
                for p in positions:
                    await self._process_symbol(p)
            except Exception as e:
                self.logger.error(f"OCO loop error: {e}")
            await asyncio.sleep(self.interval)
