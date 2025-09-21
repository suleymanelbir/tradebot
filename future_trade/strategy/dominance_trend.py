"""Dominance Trend stratejisi (kurallarınıza göre)
LONG: TOTAL3(4H)>EMA20(4H) & TOTAL3(1H)>EMA20(1H) & Sym>EMA20(1H) & RSI(1H)<60 & USDT.D(1H)<EMA20 & BTC.D(1H)<EMA20 & ADX>=20
SHORT: tersi; ADX>=20
Not: Bu iskelette göstergesel değerler ctx['indices'] ve bar history'den gelecektir (TODO: gerçek hesap).
"""


# /opt/tradebot/future_trade/strategy/dominance_trend.py

# future_trade/strategy/dominance_trend.py

# strategy/dominance_trend.py
from typing import Dict, Any, List, Optional
from .base import StrategyBase, Signal
from .indicators import ema, rsi, adx_placeholder


def _gt(x: Optional[float], y: Optional[float]) -> bool:
    return (x is not None and y is not None and x > y)

def _lt(x: Optional[float], y: Optional[float]) -> bool:
    return (x is not None and y is not None and x < y)


class DominanceTrend(StrategyBase):
    def __init__(self, cfg):
        super().__init__(cfg)
        self._closes: Dict[str, List[float]] = {}
        self.ema_p = int(cfg.get("params", {}).get("ema_period", 20))
        self.rsi_p = int(cfg.get("params", {}).get("rsi_period", 14))
        self.adx_p = int(cfg.get("params", {}).get("adx_period", 14))
        self.adx_min = float(cfg.get("params", {}).get("adx_min", 20))

    def on_bar(self, bar_event: Dict[str, Any], ctx: Dict[str, Any]) -> Signal:
        sym = bar_event["symbol"]
        indices = ctx.get("indices", {})
        # TOTAL3, USDT.D, BTC.D snapshot
        tot = indices.get("TOTAL3", {})
        usdt = indices.get("USDT.D", {})
        btc  = indices.get("BTC.D", {})

        tot_1h, tot_4h = tot.get("tf1h", {}), tot.get("tf4h", {})
        usdt_1h = usdt.get("tf1h", {})
        btc_1h  = btc.get("tf1h", {})

        # Bar’dan EMA20 proxy’si (paper’da market_stream verdi)
        sym_close = float(bar_event.get("close", 0.0))
        sym_ema20 = bar_event.get("ema20", None)

        # TODO: gerçek RSI/ADX bağlamak (şimdilik ADX=ok, RSI<60 kabulü kısmi)
        adx_ok = True
        rsi_ok = True  # ileride hesaplanacak

        long_ok = (
            _gt(tot_4h.get("close"), tot_4h.get("ema20")) and
            _gt(tot_1h.get("close"), tot_1h.get("ema20")) and
            (sym_ema20 is None or sym_close > sym_ema20) and
            rsi_ok and
            _lt(usdt_1h.get("close"), usdt_1h.get("ema20")) and
            _lt(btc_1h.get("close"), btc_1h.get("ema20")) and
            adx_ok
        )

        short_ok = (
            _lt(tot_4h.get("close"), tot_4h.get("ema20")) and
            _lt(tot_1h.get("close"), tot_1h.get("ema20")) and
            (sym_ema20 is None or sym_close < sym_ema20) and
            _gt(usdt_1h.get("close"), usdt_1h.get("ema20")) and
            _gt(btc_1h.get("close"), btc_1h.get("ema20")) and
            adx_ok
        )

        if long_ok and not short_ok:
            return Signal(side="LONG", strength=0.6)
        if short_ok and not long_ok:
            return Signal(side="SHORT", strength=0.6)
        return Signal(side="FLAT", strength=0.0)