# /opt/tradebot/future_trade/strategy/base.py
from __future__ import annotations

from dataclasses import dataclass
from typing import Optional, Dict, Any


@dataclass
class Signal:
    """Stratejinin ürettiği sinyal."""
    side: str                   # "LONG" | "SHORT" | "FLAT"
    strength: Optional[float] = None                      
    entry: Optional[float] = None  # referans giriş fiyatı (son kapalı mum)
    sl: Optional[float] = None     # stop-loss fiyatı
    tp: Optional[float] = None     # take-profit fiyatı


@dataclass
class Plan:
    """Emir planı (Risk sizing sonrası)."""
    symbol: str
    side: str                      # "LONG" | "SHORT"
    qty: float
    entry: Optional[float] = None
    sl: Optional[float] = None
    tp: Optional[float] = None


class StrategyBase:
    def __init__(self, cfg: Dict[str, Any]) -> None:
        self.cfg = cfg or {}

    def on_bar(self, event: Dict[str, Any], ctx: Dict[str, Any]) -> Signal:
        """Her kapalı mumda çağrılır; Signal döndürür."""
        raise NotImplementedError
