# /opt/tradebot/future_trade/strategies.py
from typing import Dict, Type

# 1) Gerçek taban ve stratejiyi dene
StrategyBase = None
DominanceTrend = None

try:
    from .strategy.base import StrategyBase as _SB  # varsa gerçek base
    StrategyBase = _SB
except Exception:
    pass

try:
    from .strategy.dominance_trend import DominanceTrend as _DT  # varsa gerçek strateji
    DominanceTrend = _DT
except Exception:
    pass

# 2) Fallback (projende yoksa minimal sınıflar tanımla)
if StrategyBase is None:
    class StrategyBase:  # type: ignore
        def __init__(self, cfg: dict): self.cfg = cfg

if DominanceTrend is None:
    class DominanceTrend(StrategyBase):  # type: ignore
        pass

# 3) Registry
STRATEGY_REGISTRY: Dict[str, Type[StrategyBase]] = {
    "dominance_trend": DominanceTrend,
}
