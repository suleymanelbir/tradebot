# /opt/tradebot/future_trade/exchange_utils.py
from __future__ import annotations
import math
from typing import Dict, Any

class ExchangeNormalizer:
    def __init__(self, binance_client, logger):
        self.binance = binance_client
        self.logger = logger
        self._exchange = None
        self._filters = {}  # symbol -> filters

    def _load(self):
        if self._exchange is None:
            self._exchange = self.binance.exchange_info()
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
                    "stepSize": float(flt.get("LOT_SIZE", {}).get("stepSize", 0)) if "LOT_SIZE" in flt else 0.0
                }

    @staticmethod
    def _round_step(value: float, step: float) -> float:
        if step <= 0:
            return value
        return math.floor(value / step) * step

    @staticmethod
    def _round_precision(value: float, precision: int) -> float:
        if precision is None:
            return value
        fmt = "{:0." + str(precision) + "f}"
        return float(fmt.format(value))

    def normalize(self, symbol: str, qty: float, price: float | None) -> Dict[str, Any]:
        self._load()
        f = self._filters.get(symbol)
        if not f:
            # Bilinmiyorsa ham değerleri döndür (en azından order fail verirse loglarız)
            return {"qty": qty, "price": price}

        step = f["stepSize"] or 0
        tick = f["tickSize"] or 0
        qprec = f["quantityPrecision"]
        pprec = f["pricePrecision"]

        # qty normalize (LOT_SIZE -> stepSize)
        qty_n = self._round_step(qty, step)
        if qprec is not None:
            qty_n = self._round_precision(qty_n, qprec)

        price_n = None
        if price is not None:
            price_n = self._round_step(price, tick) if tick > 0 else price
            if pprec is not None:
                price_n = self._round_precision(price_n, pprec)

        # minNotional kontrol (LIMIT için price varsa kontrol et, MARKET’te borsa kontrol eder)
        notional_ok = True
        if price_n is not None and f["minNotional"] > 0:
            notional_ok = (qty_n * price_n) >= f["minNotional"]
            if not notional_ok:
                self.logger.warning(
                    f"[Normalize] {symbol} minNotional koşulu sağlanmadı: qty*price={qty_n*price_n:.4f} < {f['minNotional']}"
                )

        return {"qty": qty_n, "price": price_n, "minNotional_ok": notional_ok}
