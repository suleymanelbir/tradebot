"""Dominance Trend stratejisi (kurallarınıza göre)
LONG: TOTAL3(4H)>EMA20(4H) & TOTAL3(1H)>EMA20(1H) & Sym>EMA20(1H) & RSI(1H)<60 & USDT.D(1H)<EMA20 & BTC.D(1H)<EMA20 & ADX>=20
SHORT: tersi; ADX>=20
Not: Bu iskelette göstergesel değerler ctx['indices'] ve bar history'den gelecektir (TODO: gerçek hesap).
"""
from typing import Dict, Any
from .base import StrategyBase, Signal


class DominanceTrend(StrategyBase):
    def on_bar(self, bar_event: Dict[str, Any], ctx: Dict[str, Any]) -> Signal:
        symbol = bar_event["symbol"]
        indices = ctx.get("indices", {})
        # TODO: ctx'den gerekli 1H/4H EMA/RSI/ADX ve dominance verilerini oku/hesapla
        # Placeholder karar mantığı:
        ok_long = False
        ok_short = False
        adx_ok = True # TODO: hesapla
        if adx_ok:
        # Koşullar (placeholder)
            ok_long = True # gerçek kuralları burada kontrol et
        ok_short = False
        if ok_long:
            return Signal(side="LONG", strength=0.6)
        if ok_short:
            return Signal(side="SHORT", strength=0.6)
        return Signal(side="FLAT", strength=0.0)