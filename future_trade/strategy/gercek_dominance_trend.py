"""Dominance Trend stratejisi (kurallarınıza göre)
LONG: TOTAL3(4H)>EMA20(4H) & TOTAL3(1H)>EMA20(1H) & Sym>EMA20(1H) & RSI(1H)<60 & USDT.D(1H)<EMA20 & BTC.D(1H)<EMA20 & ADX>=20
SHORT: tersi; ADX>=20
Not: Bu iskelette göstergesel değerler ctx['indices'] ve bar history'den gelecektir (TODO: gerçek hesap).
"""


# /opt/tradebot/future_trade/strategy/dominance_trend.py

# future_trade/strategy/dominance_trend.py

from typing import Dict, Any
from .base import StrategyBase, Signal
from .indicators import ema, rsi, atr, adx
import time


class DominanceTrend(StrategyBase):
    def __init__(self, cfg: Dict[str, Any]):
        super().__init__(cfg)
        self.smoke = bool(cfg.get("smoke", False))  # config’den alınır
        self._flip = 0  # test için yön değiştirici sayaç

    def on_bar(self, event: dict, ctx: dict) -> Signal:
        # --- TEST AMAÇLI: sadece ilk bar için LONG, sonra FLAT ---
        test_force = self.cfg.get("force_paper_entries", False)
        if test_force:
            self._flip += 1
            if self._flip == 1:
                return Signal(side="LONG", strength=0.6)
            else:
                return Signal(side="FLAT", strength=0.0)
        # --- /TEST ---

        if self.smoke:
            return Signal(side="LONG", strength=0.6)

        bars = event["bars"]
        closes = bars["closes"]; highs = bars["highs"]; lows = bars["lows"]
        if len(closes) < 50: 
            return Signal(side="FLAT")

        # İndeks snapshot
        idx = ctx.get("indices", {})
        t3_1h = idx.get("TOTAL3", {}).get("tf1h", {})
        t3_4h = idx.get("TOTAL3", {}).get("tf4h", {})
        usdt_1h = idx.get("USDT.D", {}).get("tf1h", {})
        btc_1h = idx.get("BTC.D", {}).get("tf1h", {})

        # Parametreler
        p = self.cfg.get("params", {})
        ema_p = int(p.get("ema_period", 20))
        rsi_p = int(p.get("rsi_period", 14))
        adx_p = int(p.get("adx_period", 14))
        adx_min = float(p.get("adx_min", 20))
        atr_p = int(p.get("atr_period", 14))
        atr_mult_sl = float(p.get("atr_mult_sl", 2.0))
        atr_mult_tp = float(p.get("atr_mult_tp", 3.0))

        # Sembol 1H EMA
        ema1h = ema(closes[-60:], ema_p)
        last_close = closes[-2] if len(closes) >= 2 else closes[-1]

        # RSI/ATR/ADX
        rsi1h = rsi(closes, rsi_p)
        atr1h = atr(highs, lows, closes, atr_p)
        adx1h = adx(highs, lows, closes, adx_p)

        long_ok = (
            t3_4h.get("close", 0) > t3_4h.get("ema20", 1e18) and
            t3_1h.get("close", 0) > t3_1h.get("ema20", 1e18) and
            last_close > ema1h and
            rsi1h < 60.0 and
            usdt_1h.get("close", 1e18) < usdt_1h.get("ema20", 0) and
            btc_1h.get("close", 1e18) < btc_1h.get("ema20", 0) and
            adx1h >= adx_min
        )

        short_ok = (
            t3_4h.get("close", 0) < t3_4h.get("ema20", -1e18) and
            t3_1h.get("close", 0) < t3_1h.get("ema20", -1e18) and
            last_close < ema1h and
            usdt_1h.get("close", -1e18) > usdt_1h.get("ema20", 0) and
            btc_1h.get("close", -1e18) > btc_1h.get("ema20", 0) and
            adx1h >= adx_min
        )

        if long_ok:
            sl = last_close - atr_mult_sl * atr1h
            tp = last_close + atr_mult_tp * atr1h
            return Signal(side="LONG", entry=last_close, sl=sl, tp=tp)

        if short_ok:
            sl = last_close + atr_mult_sl * atr1h
            tp = last_close - atr_mult_tp * atr1h
            return Signal(side="SHORT", entry=last_close, sl=sl, tp=tp)

        return Signal(side="FLAT")

# registry (eğer yoksa)
STRATEGY_REGISTRY = {"dominance_trend": DominanceTrend}
