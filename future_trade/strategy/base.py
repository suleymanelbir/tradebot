from dataclasses import dataclass
from typing import Optional, Dict, Any, Literal


Side = Literal["LONG", "SHORT", "FLAT"]


@dataclass
class Signal:
    side: Side
    strength: float = 0.0
    sl: Optional[float] = None
    tp: Optional[float] = None


class StrategyBase:
    
    def __init__(self, cfg: Dict[str, Any]):
        self.cfg = cfg
        
    def on_bar(self, bar_event: Dict[str, Any], ctx: Dict[str, Any]) -> Signal:
        raise NotImplementedError