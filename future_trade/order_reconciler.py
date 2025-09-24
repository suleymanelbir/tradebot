# /opt/tradebot/future_trade/order_reconciler.py
from __future__ import annotations
from typing import Callable, Optional, Dict, Any
import logging

from .order_router import OrderRouter, Mode   


class _ClientAdapter:
    """
    Eski imzayla gelen client'ı (binance wrapper) router benzeri arayüze çevirir.
    Sadece close_position_market için gereken new_order'ı kullanır.
    """
    def __init__(self, client, logger: logging.Logger):
        self.client = client
        self.logger = logger

    def close_position_market(self, symbol: str, side: str, qty: float, tag: str = "reconcile_close") -> Dict[str, Any]:
        # reduceOnly MARKET kapatma (client new_order varsa)
        call = getattr(self.client, "new_order", None)
        if call is None:
            raise AttributeError("ClientAdapter: client.new_order bulunamadı")
        payload = {
            "symbol": symbol,
            "side": side,
            "type": "MARKET",
            "quantity": qty,
            "reduceOnly": True,
            "newClientOrderId": f"RC|{symbol}|{tag}"
        }
        res = call(**payload)
        self.logger.info(f"[RECONCILE-LIVE] {res}")
        return res


class OrderReconciler:
    """
    Geriye dönük uyumlu reconciler.

    Desteklenen kurulumlar:
      - Yeni mimari: OrderReconciler(router=router, persistence=..., logger=..., price_provider=...)
      - Eski mimari: OrderReconciler(client=client, persistence=..., notifier=..., price_provider=..., logger=...)

    Temel iş: pozisyon kapatma (reduceOnly MARKET) + PnL hesap/kayıt.
    """
    def __init__(
        self,
        router: Optional[OrderRouter] = None,
        *,
        client: Optional[object] = None,
        persistence: Optional[object] = None,
        notifier: Optional[object] = None,
        logger: Optional[logging.Logger] = None,
        price_provider: Optional[Callable[[str], float]] = None
    ):
        self.logger = logger or logging.getLogger("reconciler")
        self.db = persistence
        self.notifier = notifier
        self._price_provider = price_provider

        if router is not None:
            self.router = router
        elif client is not None:
            # Eski kullanım: client'ı adapter ile router benzeri arayüze çevir
            self.router = _ClientAdapter(client, self.logger)  # type: ignore
        else:
            raise ValueError("OrderReconciler: router veya client parametresinden en az biri zorunlu")

    # Dışarıdan çağrılan yardımcı
    def close_all_for_symbol(self, symbol: str, position_side: str, qty: float, entry_price: float, exit_price: Optional[float] = None) -> float:
        """
        Verilen semboldeki pozisyonu tamamen kapatır, PnL döndürür.
        position_side: 'LONG' veya 'SHORT'
        """
        side_for_order = "SELL" if position_side.upper() == "LONG" else "BUY"

        if exit_price is None:
            exit_price = float(self._price_provider(symbol)) if self._price_provider else entry_price

        return self._close_position(symbol, side_for_order, entry_price, abs(float(qty)), float(exit_price))

    # İç iş: kapat + PnL + kayıt
    def _close_position(self, symbol: str, side: str, entry_price: float, qty: float, exit_price: float) -> float:
        try:
            # 1) Borsada kapat (tek kapı / adapter)
            self.router.close_position_market(symbol=symbol, side=side, qty=qty, tag="reconcile_close")

            # 2) PnL hesap
            pnl = (exit_price - entry_price) * qty if side.upper() == "SELL" else (entry_price - exit_price) * qty

            # 3) Kayıt (varsa)
            if self.db and hasattr(self.db, "record_close"):
                try:
                    self.db.record_close(symbol, side, entry_price, exit_price, qty, pnl)
                except Exception as e:
                    self.logger.warning(f"record_close hata: {e}")

            # 4) Cache'ten temizle (varsa)
            if self.db and hasattr(self.db, "cache_close_position"):
                try:
                    self.db.cache_close_position(symbol)
                except Exception as e:
                    self.logger.warning(f"cache_close_position hata: {e}")

            self.logger.info(f"[RECONCILE] {symbol} kapatıldı side={side} qty={qty} entry={entry_price} exit={exit_price} pnl={pnl:.6f}")
            return pnl
        except Exception as e:
            self.logger.error(f"[RECONCILE] Close error: {symbol} {e}")
            # Bildirim
            if self.notifier and hasattr(self.notifier, "alert"):
                try:
                    # async olup olmamasını önemsemiyoruz; fire-and-forget
                    self.notifier.alert({"event": "reconcile_error", "symbol": symbol, "error": str(e)})
                except Exception:
                    pass
            raise
