# /opt/tradebot/future_trade/order_manager.py
from __future__ import annotations
from typing import Dict, Any, Optional
from .order_router import OrderRouter

class OrderManager:
    """
    Router üstü ince yönetim katmanı:
    - Entry aç → persistence.cache_add_open_position(...)
    - Kısmi kapat → persistence.cache_update_position(...)
    - Tam kapat (kapanış fonksiyonları zaten Reconciler'da) → persistence.cache_close_position(...)
    Not: Tam kapanış için OrderReconciler zaten çağrılıyor; oraya ayrıca dokunacağız.
    """

    def __init__(self, router: OrderRouter, persistence, market_stream, risk_manager, logger):
        self.router = router
        self.persistence = persistence
        self.stream = market_stream
        self.risk = risk_manager
        self.logger = logger

    def _last_price(self, symbol: str) -> Optional[float]:
        return self.stream.get_last_price(symbol) if hasattr(self.stream, "get_last_price") else None

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

    
    def open_entry_from_intent(self, intent: Dict[str, Any]) -> Dict[str, Any]:
        """
        intent şeması (esnek):
          { "action":"entry", "symbol":"SOLUSDT", "side":"BUY"|"SELL",
            "qty": optional float, "risk_pct": optional float,
            "order_type": "MARKET"|"LIMIT"|"STOP_MARKET" (varsayılan: MARKET),
            "price": optional float (LIMIT için),
          }
        """
        symbol = intent["symbol"]
        side = intent["side"].upper()
        order_type = intent.get("order_type") or ("MARKET" if "price" not in intent else "LIMIT")
        price = intent.get("price")

        # qty belirleme: önce intent.qty, yoksa risk manager
        qty = float(intent.get("qty") or 0)
        if qty <= 0 and hasattr(self.risk, "position_size"):
            last = self._last_price(symbol)
            qty = float(self.risk.position_size(symbol, side, last_price=last, risk_pct=intent.get("risk_pct")))
        if qty <= 0:
            raise ValueError("qty hesaplanamadı")

        res = self.router.place_order(
            symbol=symbol,
            side=side,
            qty=qty,
            price=price,
            order_type=order_type,
            reduce_only=False,
            tag="entry"
        )

        # fill/entry cache
        pos_side = "LONG" if side == "BUY" else "SHORT"
        fill_price = res.get("avgPrice") or res.get("price") or self._last_price(symbol) or price or 0.0
        if hasattr(self.persistence, "cache_add_open_position"):
            self.persistence.cache_add_open_position(symbol, pos_side, qty, float(fill_price))

        self.logger.info(f"[ENTRY] {symbol} {pos_side} qty={qty} entry={fill_price}")
        return res

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
    def partial_close(self, symbol: str, position_side: str, qty: float) -> Dict[str, Any]:
        position_side = position_side.upper()
        if position_side not in ("LONG", "SHORT"):
            raise ValueError("position_side must be LONG or SHORT")

        side_for_order = "SELL" if position_side == "LONG" else "BUY"
        res = self.router.close_position_market(symbol=symbol, side=side_for_order, qty=qty, tag="partial_close")

        try:
            if hasattr(self.persistence, "list_open_positions"):
                for p in self.persistence.list_open_positions():
                    if p.get("symbol") == symbol:
                        new_qty = max(0.0, float(p.get("qty", 0)) - float(qty))
                        if new_qty > 0 and hasattr(self.persistence, "cache_update_position"):
                            self.persistence.cache_update_position(symbol, qty=new_qty)
                        elif new_qty == 0 and hasattr(self.persistence, "cache_close_position"):
                            self.persistence.cache_close_position(symbol)
                        break
        except Exception as e:
            self.logger.warning(f"[PARTIAL CLOSE] cache update error: {e}")

        self.logger.info(f"[PARTIAL CLOSE] {symbol} {position_side} -{qty}")
        return res

