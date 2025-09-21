"""Dominance Trend stratejisi (kurallarınıza göre)
LONG: TOTAL3(4H)>EMA20(4H) & TOTAL3(1H)>EMA20(1H) & Sym>EMA20(1H) & RSI(1H)<60 & USDT.D(1H)<EMA20 & BTC.D(1H)<EMA20 & ADX>=20
SHORT: tersi; ADX>=20
Not: Bu iskelette göstergesel değerler ctx['indices'] ve bar history'den gelecektir (TODO: gerçek hesap).
"""


# /opt/tradebot/future_trade/strategy/dominance_trend.py

# future_trade/strategy/dominance_trend.py

# strategy/dominance_trend.py
from typing import Dict, Any, List
from .base import StrategyBase, Signal
from .indicators import ema, rsi, adx_placeholder

class DominanceTrend(StrategyBase):
    def __init__(self, cfg):
        super().__init__(cfg)
        self._closes: Dict[str, List[float]] = {}
        self.ema_p = int(cfg.get("params", {}).get("ema_period", 20))
        self.rsi_p = int(cfg.get("params", {}).get("rsi_period", 14))
        self.adx_p = int(cfg.get("params", {}).get("adx_period", 14))
        self.adx_min = float(cfg.get("params", {}).get("adx_min", 20))

    def on_bar(self, bar_event: Dict[str, Any], ctx: Dict[str, Any]) -> Signal:
        close = bar_event.get("close")
        ema20 = bar_event.get("ema20")
        if close is None or ema20 is None:
            return Signal(side="FLAT", strength=0.0)

        if close > ema20:
            return Signal(side="LONG", strength=0.6)
        elif close < ema20:
            return Signal(side="SHORT", strength=0.6)
        else:
            return Signal(side="FLAT", strength=0.0)
