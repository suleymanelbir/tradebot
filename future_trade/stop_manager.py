# /opt/tradebot/future_trade/stop_manager.py
from __future__ import annotations
import logging

class StopManager:
    def __init__(self, router, persistence, logger=None):
        self.router = router
        self.persistence = persistence
        self.logger = logger or logging.getLogger("stop_manager")

    def upsert_stop_loss(self, symbol: str, side_for_stop: str, stop_price: float) -> dict:
        """
        Çok basit sürüm: mevcut SL'yi arayıp iptal etmeden direkt STOP_MARKET reduceOnly atar.
        (Gelişmiş sürüm: mevcut SL bulunur → fiyat yeterince uzaksa replace yapılır.)
        """
        # qty: mevcut pozisyonun tamamı kadar SL
        qty = 0.0
        if hasattr(self.persistence, "list_open_positions"):
            for p in self.persistence.list_open_positions():
                if p.get("symbol") == symbol:
                    qty = abs(float(p.get("qty", 0) or 0))
                    break
        if qty <= 0:
            self.logger.info(f"[SL-UPsert] no open qty for {symbol}, skip")
            return {"status":"SKIP"}

        # STOP_MARKET reduceOnly
        res = self.router.place_order(
            symbol=symbol,
            side=side_for_stop.upper(),
            qty=qty,
            order_type="STOP_MARKET",
            stop_price=float(stop_price),
            reduce_only=True,
            tag="trail_upsert"
        )
        self.logger.info(f"[SL-UPsert] {symbol} {side_for_stop} qty={qty} stop={stop_price} -> {res.get('status','OK')}")
        return res
