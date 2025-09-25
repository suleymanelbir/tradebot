# /opt/tradebot/future_trade/protective_sweeper.py
# -*- coding: utf-8 -*-
from __future__ import annotations

import asyncio, logging
from typing import Dict, Any, List, Optional


class ProtectiveSweeper:
    """
    Pozisyonu kapanmış sembollerde borsada kalan reduceOnly STOP/TP emirlerini iptal eder.
    - list_open_positions(): açık pozisyonları verir
    - client.list_open_orders(symbol=...) ile açık emirleri çeker
    - reduceOnly ve STOP*/TAKE_PROFIT* olanları iptal eder
    """

    def __init__(self, router, persistence, logger=None, interval_sec: int = 20):
        self.router = router
        self.persistence = persistence
        self.logger = logger or logging.getLogger("protective_sweeper")
        self.interval = max(5, int(interval_sec))

    def _symbols_with_positions(self) -> List[str]:
        list_fn = getattr(self.router, "_trail_ctx", {}).get("list_open_positions")
        if not callable(list_fn):
            return []
        try:
            syms = set()
            for p in (list_fn() or []):
                side = (p.get("side") or "").upper()
                if side in ("LONG", "SHORT") and (p.get("qty") or 0) not in (0, 0.0):
                    syms.add(p.get("symbol"))
            return sorted(syms)
        except Exception as e:
            self.logger.debug(f"list_open_positions failed: {e}")
            return []

    async def _list_open_orders(self, symbol: Optional[str] = None) -> List[Dict[str, Any]]:
        client = getattr(self.router, "client", None)
        if client is None:
            return []
        try:
            res = client.list_open_orders(symbol=symbol) if symbol else client.list_open_orders()
            if asyncio.iscoroutine(res):
                return await res
            return res
        except Exception as e:
            self.logger.debug(f"list_open_orders error: {e}")
            return []

    def _is_protective(self, o: Dict[str, Any]) -> bool:
        try:
            typ = (o.get("type") or "").upper()
            if "STOP" in typ or "TAKE_PROFIT" in typ:
                return bool(o.get("reduceOnly", False))
            return False
        except Exception:
            return False

    async def _sweep_symbol(self, symbol: str) -> int:
        """
        Pozisyonu olmayan semboldeki koruyucu emirleri iptal eder.
        Geri dönüş: iptal edilen emir sayısı.
        """
        # Pozisyon var mı? Varsa süpürme, çık.
        if symbol in self._symbols_with_positions():
            return 0

        cancelled = 0
        orders = await self._list_open_orders(symbol=symbol)
        for o in orders or []:
            if not self._is_protective(o):
                continue
            try:
                oid = o.get("orderId")
                coid = o.get("clientOrderId")
                self.router.cancel_order(symbol=symbol, order_id=oid, client_order_id=coid)
                cancelled += 1
            except Exception as e:
                self.logger.debug(f"cancel protective failed for {symbol}: {e}")
        if cancelled:
            self.logger.info(f"[SWEEP] {symbol}: cancelled {cancelled} protective order(s)")
        return cancelled

    async def run(self, stop_event: asyncio.Event = None):
        """
        Periyodik tarama: açık emirleri sembol-bazlı süpürür.
        """
        while not (stop_event and stop_event.is_set()):
            try:
                # Hedef semboller: whitelist + açık emirleri olanlar
                wh = list(self.router.cfg.get("symbols_whitelist", []))
                # Open orders içinden de topla (geniş kapsama)
                all_orders = await self._list_open_orders()
                syms = set(wh)
                for o in all_orders or []:
                    s = o.get("symbol")
                    if s:
                        syms.add(s)

                for sym in sorted(syms):
                    await self._sweep_symbol(sym)
            except Exception as e:
                self.logger.error(f"ProtectiveSweeper loop error: {e}")
            await asyncio.sleep(self.interval)
