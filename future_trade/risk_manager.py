"""Risk & boyutlandırma: per-trade risk, kaldıraç, SL/TP planı
- qty hesabı: risk / (stop_distance)
- Filtreler: min_notional, stepSize, tickSize (exchangeInfo'dan) → TODO
"""
from dataclasses import dataclass
from typing import Dict, Any, Optional


@dataclass
class TradePlan:
    ok: bool
    reason: Optional[str] = None
    symbol: Optional[str] = None
    side: Optional[str] = None
    entry: Optional[float] = None
    qty: Optional[float] = None
    sl: Optional[float] = None
    tp: Optional[float] = None


class RiskManager:
    def __init__(self, risk_cfg: Dict[str, Any], leverage_map: Dict[str, int], portfolio):
        self.cfg = risk_cfg; self.lev = leverage_map; self.portfolio = portfolio


    def plan_trade(self, symbol: str, signal) -> TradePlan:
        # Placeholder: entry=bar close; stop=ATR veya strateji SL; qty: equity * pct / distance
        equity = self.portfolio.equity()
        per_pct = float(self.cfg.get("per_trade_risk_pct", 0.5)) / 100.0
        entry = 100.0; sl = 98.0; tp = 103.0 # TODO: gerçek değerler
        dist = abs(entry - sl)
        if dist <= 0:
            return TradePlan(ok=False, reason="invalid stop distance")
        risk_cap = equity * per_pct
        qty = risk_cap / dist
        return TradePlan(ok=True, symbol=symbol, side=signal.side, entry=entry, qty=qty, sl=sl, tp=tp)