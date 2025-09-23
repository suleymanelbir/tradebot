# /opt/tradebot/future_trade/order_manager.py
from __future__ import annotations
from typing import Dict, Any, Optional

from .order_router import OrderRouter, Mode

class OrderManager:
    """
    Router üstü ince yönetim katmanı:
    - Entry aç → persistence.cache_add_open_position(...)
    - Kısmi kapat → persistence.cache_update_position(...)
    - Tam kapat (kapanış fonksiyonları zaten Reconciler'da) → persistence.cache_close_position(...)
    Not: Tam kapanış için OrderReconciler zaten çağrılıyor; oraya ayrıca dokunacağız.
    """

    def __init__(self, router: OrderRouter, persistence, market_stream, logger):
        self.router = router
        self.persistence = persistence
        self.stream = market_stream
        self.logger = logger

    @staticmethod
    def _extract_entry_fill_price(resp: Dict[str, Any], fallback_price: Optional[float]) -> float:
        """
        PAPER/LIVE yanıtlarından "fill price" çıkar. Bulamazsa last_price'a düşer.
        """
        # PAPER sim yanıt
        for k in ("price", "avgPrice", "fillsPrice", "fill_price"):
            v = resp.get(k)
            if v:
                try:
                    return float(v)
                except Exception:
                    pass
        return float(fallback_price or 0.0)

    @staticmethod
    def _extract_executed_qty(resp: Dict[str, Any], default_qty: float) -> float:
        for k in ("executedQty", "executed_qty", "filledQty", "filled_qty", "cumQty"):
            v = resp.get(k)
            if v:
                try:
                    return float(v)
                except Exception:
                    pass
        return float(default_qty)

    # ---------------- ENTRY ----------------
    def open_entry(self, symbol: str, side: str, qty: float, price: float = None, order_type: str = None, tag: str = "entry") -> Dict[str, Any]:
        """
        Entry emri atar ve başarıyla atıldıysa cache'e pozisyon ekler/günceller.
        """
        side = side.upper()
        # Router'a emir
        res = self.router.place_order(
            symbol=symbol,
            side=side,
            qty=qty,
            price=price,
            order_type=order_type or ("MARKET" if price is None else "LIMIT"),
            time_in_force=None,
            reduce_only=False,
            tag=tag
        )

        # Fiyat fallback: stream.get_last_price
        last = self.stream.get_last_price(symbol) if hasattr(self.stream, "get_last_price") else None
        fill_price = self._extract_entry_fill_price(res, last)
        filled_qty = self._extract_executed_qty(res, qty)

        # LONG/SHORT belirle
        pos_side = "LONG" if side == "BUY" else "SHORT"

        # Cache kaydı (pozisyon açıldı)
        try:
            if hasattr(self.persistence, "cache_add_open_position"):
                # Eğer aynı sembolde zaten pozisyon varsa 'update' de gerekebilir; basit tutuyoruz:
                self.persistence.cache_add_open_position(
                    symbol=symbol,
                    side=pos_side,
                    qty=filled_qty,
                    entry_price=fill_price
                )
            else:
                self.logger.debug("persistence.cache_add_open_position bulunamadı")
        except Exception as e:
            self.logger.warning(f"cache_add_open_position hata: {e}")

        self.logger.info(f"[ENTRY] {symbol} {pos_side} qty={filled_qty} entry={fill_price}")
        return res

    # ---------------- PARTIAL CLOSE ----------------
    def partial_close(self, symbol: str, current_side: str, close_qty: float, tag: str = "partial_close") -> Dict[str, Any]:
        """
        Açık pozisyondan kısmi çıkış. reduceOnly MARKET ile kapatır ve cache'i günceller.
        current_side: mevcut pozisyon yönü ("LONG" | "SHORT")
        """
        current_side = current_side.upper()
        if current_side not in ("LONG", "SHORT"):
            raise ValueError("current_side LONG veya SHORT olmalı")

        # Kapatma yönü emir side:
        side_for_order = "SELL" if current_side == "LONG" else "BUY"

        res = self.router.close_position_market(
            symbol=symbol,
            side=side_for_order,
            qty=close_qty,
            tag=tag
        )

        # Cache güncelle: yeni qty = eski qty - close_qty
        try:
            if hasattr(self.persistence, "list_open_positions"):
                positions = self.persistence.list_open_positions() or []
                old_qty = None
                for p in positions:
                    if p.get("symbol") == symbol:
                        old_qty = float(p.get("qty", 0))
                        break
                if old_qty is not None:
                    new_qty = max(0.0, old_qty - float(close_qty))
                    if new_qty > 0:
                        if hasattr(self.persistence, "cache_update_position"):
                            self.persistence.cache_update_position(symbol, qty=new_qty)
                    else:
                        # Tamamen kapanmış say → cache'ten sil
                        if hasattr(self.persistence, "cache_close_position"):
                            self.persistence.cache_close_position(symbol)
        except Exception as e:
            self.logger.warning(f"partial_close cache update hata: {e}")

        self.logger.info(f"[PARTIAL CLOSE] {symbol} {current_side} -{close_qty}")
        return res
