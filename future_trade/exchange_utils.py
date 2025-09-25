from __future__ import annotations
from typing import Dict, Any, Optional
import math, logging

class ExchangeNormalizer:
    def __init__(self, binance_client, logger=None):
        self.client = binance_client
        self.logger = logger or logging.getLogger("normalizer")
        self._exchange = None
        self._filters: Dict[str, Dict[str, Any]] = {}
        self._ex_info: Dict[str, Any] = {}

    def _load(self):
        if self._exchange is None:
            self._exchange = self.client.exchange_info()
            for s in self._exchange.get("symbols", []):
                sym = s["symbol"]
                flt = {}
                for f in s.get("filters", []):
                    flt[f["filterType"]] = f
                self._filters[sym] = {
                    "pricePrecision": s.get("pricePrecision"),
                    "quantityPrecision": s.get("quantityPrecision"),
                    "minNotional": float(flt.get("MIN_NOTIONAL", {}).get("notional", 0)) if "MIN_NOTIONAL" in flt else 0.0,
                    "tickSize": float(flt.get("PRICE_FILTER", {}).get("tickSize", 0)) if "PRICE_FILTER" in flt else 0.0,
                    "stepSize": float(flt.get("LOT_SIZE", {}).get("stepSize", 0)) if "LOT_SIZE" in flt else 0.0,
                    "minQty": float(flt.get("LOT_SIZE", {}).get("minQty", 0)) if "LOT_SIZE" in flt else 0.0,
                    "maxQty": float(flt.get("LOT_SIZE", {}).get("maxQty", 0)) if "LOT_SIZE" in flt else 0.0
                }

    async def warmup(self):
        try:
            self._ex_info = await self.client.exchange_info()
        except Exception as e:
            self.logger.warning(f"exchange_info warmup failed: {e}")
            self._ex_info = {}

    def _symbol_obj(self, symbol: str) -> Optional[Dict[str, Any]]:
        if self._ex_info and "symbols" in self._ex_info:
            for s in self._ex_info["symbols"]:
                if s.get("symbol") == symbol:
                    return s
        get_cache = getattr(self.client, "get_cached_exchange_info", None)
        if callable(get_cache):
            ex = get_cache()
            if ex and "symbols" in ex:
                for s in ex["symbols"]:
                    if s.get("symbol") == symbol:
                        return s
        return None

    def _flt(self, symbol: str, ftype: str) -> Optional[Dict[str, Any]]:
        s = self._symbol_obj(symbol)
        if not s:
            return None
        for f in s.get("filters", []):
            if f.get("filterType") == ftype:
                return f
        return None

    def steps(self, symbol: str) -> Dict[str, float]:
        self._load()
        f = self._filters.get(symbol, {})
        return {
            "tick": f.get("tickSize", 0.0),
            "qty_step": f.get("stepSize", 0.0),
            "min_qty": f.get("minQty", 0.0),
            "max_qty": f.get("maxQty", 0.0),
            "min_notional": f.get("minNotional", 0.0),
        }

    @staticmethod
    def _round_step(x: float, step: float) -> float:
        if step is None or step <= 0:
            return float(x)
        return math.floor(x / step) * step

    @staticmethod
    def _round_precision(value: float, precision: Optional[int]) -> float:
        if precision is None:
            return value
        fmt = "{:0." + str(precision) + "f}"
        return float(fmt.format(value))

    def normalize_price(self, symbol: str, price: float) -> float:
        st = self.steps(symbol)
        p = self._round_step(price, st["tick"])
        return float(f"{p:.10f}")

    def normalize_qty(self, symbol: str, qty: float) -> float:
        st = self.steps(symbol)
        q = self._round_step(qty, st["qty_step"])
        q = float(f"{q:.10f}")
        return q if q >= st["min_qty"] else 0.0

    def notional(self, price: float, qty: float) -> float:
        return float(price) * float(qty)

    def ensure_min_notional(self, symbol: str, price: float, qty: float) -> float:
        st = self.steps(symbol)
        mn = st["min_notional"]
        if mn <= 0 or price <= 0:
            return qty
        need = mn / price
        qstep = st["qty_step"]
        needed_rounded = math.ceil(need / qstep) * qstep if qstep > 0 else need
        q = max(needed_rounded, qty)
        return self.normalize_qty(symbol, q)

    def normalize_order(self, symbol: str, side: str, qty: float, price: float | None, order_type: str, reduce_only: bool) -> Dict[str, float]:
        self._load()
        p = float(price) if price is not None else None
        if p is not None:
            p = self.normalize_price(symbol, p)
        q = self.normalize_qty(symbol, qty)

        if not reduce_only and p is not None and p > 0:
            q = self.ensure_min_notional(symbol, p, q)

        return {"price": p if price is not None else None, "qty": q}

    def normalize(self, symbol: str, qty: float, price: float | None) -> Dict[str, Any]:
        self._load()
        f = self._filters.get(symbol)
        if not f:
            return {"qty": qty, "price": price}

        qty_n = self._round_step(qty, f["stepSize"])
        if f.get("quantityPrecision") is not None:
            qty_n = self._round_precision(qty_n, f["quantityPrecision"])

        price_n = None
        if price is not None:
            price_n = self._round_step(price, f["tickSize"])
            if f.get("pricePrecision") is not None:
                price_n = self._round_precision(price_n, f["pricePrecision"])

        notional_ok = True
        if price_n is not None and f["minNotional"] > 0:
            notional_ok = (qty_n * price_n) >= f["minNotional"]
            if not notional_ok:
                self.logger.warning(
                    f"[Normalize] {symbol} minNotional koşulu sağlanmadı: qty*price={qty_n*price_n:.4f} < {f['minNotional']}"
                )

        return {"qty": qty_n, "price": price_n, "minNotional_ok": notional_ok}
