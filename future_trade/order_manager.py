# /opt/tradebot/future_trade/order_manager.py
from __future__ import annotations
from typing import Dict, Any, Optional
import logging

from .order_router import OrderRouter

class OrderManager:
    def __init__(
        self,
        router: OrderRouter,
        persistence,
        market_stream,
        risk_manager,
        logger=None,
        kill_switch=None,          # <<< eklendi
        limits_cfg: Dict[str, Any] = None  # <<< eklendi
    ):
        self.router = router
        self.persistence = persistence
        self.stream = market_stream
        self.risk = risk_manager
        self.logger = logger or logging.getLogger("order_manager")
        self.kill_switch = kill_switch
        self.limits = (limits_cfg or {})

    # ---- yardımcılar ----
    def _count_open(self):
        total = 0; by_sym = {}; long_n = 0; short_n = 0
        if hasattr(self.persistence, "list_open_positions"):
            for p in self.persistence.list_open_positions():
                total += 1
                sym = p.get("symbol")
                side = (p.get("side") or "").upper()
                by_sym[sym] = by_sym.get(sym, 0) + 1
                if side == "LONG": long_n += 1
                elif side == "SHORT": short_n += 1
        return total, by_sym, long_n, short_n

    def _read_limit(self, *names, default=None):
        for n in names:
            if n in self.limits:
                return self.limits[n]
        return default

    def _last_price(self, symbol: str) -> Optional[float]:
        return self.stream.get_last_price(symbol) if hasattr(self.stream, "get_last_price") else None

    # ---- asıl iş ----
    def open_entry_from_intent(self, intent: Dict[str, Any]) -> Dict[str, Any]:
        """
        intent örneği:
        { "action":"entry", "symbol":"SOLUSDT", "side":"BUY"|"SELL",
            "qty": optional float, "risk_pct": optional float,
            "order_type": "MARKET"|"LIMIT"|"STOP_MARKET",
            "price": optional float }
        """

        # 1) Kill-Switch kontrolü
        if self.kill_switch and hasattr(self.kill_switch, "is_trading_allowed") and not self.kill_switch.is_trading_allowed():
            raise RuntimeError("Kill-Switch: trading disabled")

        # 2) Limit kontrolleri
        total, by_sym, long_n, short_n = self._count_open()
        max_open_positions = int(self._read_limit("max_open_positions", "max_open_trades_global", default=0) or 0)
        max_per_symbol    = int(self._read_limit("max_positions_per_symbol", "max_trades_per_symbol", default=0) or 0)
        max_longs         = int(self._read_limit("max_longs", "max_long_trades", default=0) or 0)
        max_shorts        = int(self._read_limit("max_shorts", "max_short_trades", default=0) or 0)

        if max_open_positions and total >= max_open_positions:
            raise RuntimeError("Limit: max_open_positions reached")

        symbol = intent["symbol"]
        side = (intent.get("side") or "").upper()
        if max_per_symbol and by_sym.get(symbol, 0) >= max_per_symbol:
            raise RuntimeError(f"Limit: max_positions_per_symbol reached for {symbol}")
        if side == "BUY" and max_longs and long_n >= max_longs:
            raise RuntimeError("Limit: max_longs reached")
        if side == "SELL" and max_shorts and short_n >= max_shorts:
            raise RuntimeError("Limit: max_shorts reached")

        # 3) Emir parametreleri
        order_type = intent.get("order_type") or ("MARKET" if "price" not in intent else "LIMIT")
        price = intent.get("price")

        # 4) Miktar hesaplama
        qty = float(intent.get("qty") or 0)
        if qty <= 0 and hasattr(self.risk, "position_size"):
            last = self._last_price(symbol)
            qty = float(self.risk.position_size(symbol, side, last_price=last, risk_pct=intent.get("risk_pct")))
        if qty <= 0:
            # Fallback hesaplama: equity ve stop mesafesi
            r_cfg = getattr(self.risk, "cfg", {}) if hasattr(self.risk, "cfg") else {}
            per_risk_pct = float(r_cfg.get("per_trade_risk_pct", 0.2))
            equity = 0.0
            if hasattr(self.persistence, "estimate_account_equity"):
                equity = float(self.persistence.estimate_account_equity(self._last_price, start_equity_fallback=r_cfg.get("start_equity_usdt", 1000)))
            risk_amount = max(0.0, equity * per_risk_pct / 100.0)
            last = self._last_price(symbol) or float(intent.get("price") or 0)
            if last <= 0 or risk_amount <= 0:
                raise ValueError("qty hesaplanamadı (price/equity yok)")
            tr_glob = (self.router.cfg.get("trailing") if hasattr(self.router, "cfg") else None) or {}
            tr_type = (tr_glob.get("type") or "step_pct").lower()
            atr_period = int(tr_glob.get("atr_period", 14))
            atr_mult = float(tr_glob.get("atr_mult", 2.5))
            step_pct = float(tr_glob.get("step_pct", 0.1))
            atr_val = None
            get_atr = getattr(self.router, "_trail_ctx", {}).get("get_atr") if hasattr(self.router, "_trail_ctx") else None
            if callable(get_atr):
                atr_val = get_atr(symbol, atr_period)
            if tr_type == "atr" and atr_val and atr_val > 0:
                stop_dist = atr_mult * atr_val
            else:
                stop_dist = last * (step_pct / 100.0)
            if stop_dist <= 0:
                raise ValueError("invalid stop distance")
            qty = risk_amount / stop_dist

        # 5) Normalize işlemi
        if hasattr(self.router, "normalizer") and self.router.normalizer:
            n = self.router.normalizer.normalize_order(symbol, side, qty, price, order_type, reduce_only=False)
            price = n["price"] if price is not None else None
            qty = n["qty"]

        # 6) Emir gönderimi
        res = self.router.place_order(
            symbol=symbol,
            side=side,
            qty=qty,
            price=price,
            order_type=order_type,
            reduce_only=False,
            tag="entry"
        )

        # 7) ENTRY sonrası pozisyonu cache'e yaz
        pos_side = "LONG" if side == "BUY" else "SHORT"
        fill_price = res.get("avgPrice") or res.get("price") or self._last_price(symbol) or price or 0.0
        if hasattr(self.persistence, "cache_add_open_position"):
            self.persistence.cache_add_open_position(symbol, pos_side, qty, float(fill_price))

        # 8) ENTRY log kaydı
        self.logger.info(f"[ENTRY] {symbol} {pos_side} qty={qty} entry={fill_price}")

        # 9) KISMİ KAPANIŞ kontrolü (örnek: pozisyon zaten vardı ve miktar azaldıysa)
        if hasattr(self.persistence, "cache_update_position"):
            try:
                old_qty = self._get_cached_qty(symbol)
                closed_qty = max(0.0, old_qty - qty)
                if closed_qty > 0 and qty < old_qty:
                    new_qty = max(0.0, old_qty - closed_qty)
                    self.persistence.cache_update_position(symbol, qty=new_qty)
            except Exception:
                pass

        # 10) TAM KAPANIŞ kontrolü (örnek: pozisyon sıfırlandıysa)
        if hasattr(self.persistence, "cache_close_position"):
            try:
                if qty <= 0:
                    self.persistence.cache_close_position(symbol)
            except Exception:
                pass

        return res
