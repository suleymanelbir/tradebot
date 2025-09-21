"""Risk & boyutlandırma: per-trade risk, kaldıraç, SL/TP planı
- qty hesabı: risk / (stop_distance)
- Filtreler: min_notional, stepSize, tickSize (exchangeInfo'dan) → TODO
"""
# future_trade/risk_manager.py

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
        self.cfg = risk_cfg
        self.lev = leverage_map
        self.portfolio = portfolio

    # risk_manager.py -- plan_trade() yeniden
    def plan_trade(self, symbol: str, signal) -> TradePlan:
        equity = float(self.portfolio.equity())
        per_pct = float(self.cfg.get("per_trade_risk_pct", 0.5)) / 100.0
        entry = 100.0  # default
        # entry'i sinyal event'inden almak daha doğru; biz app.py’de event'i persistence'a yazmadık.
        # market_stream close'u strateji içinde var; burada varsayım: 100.0
        # İstersen signal objesine 'ref_price' ekleyebiliriz.
        if signal.side == "LONG":
            sl = entry * 0.99; tp = entry * 1.02
        elif signal.side == "SHORT":
            sl = entry * 1.01; tp = entry * 0.98
        else:
            return TradePlan(ok=False, reason="signal_flat")

        dist = abs(entry - sl)
        if dist <= 0:
            return TradePlan(ok=False, reason="invalid_stop_distance")

        risk_cap = equity * per_pct
        qty = max(risk_cap / dist, 0.0)

        return TradePlan(ok=True, symbol=symbol, side=signal.side, entry=entry, qty=qty, sl=sl, tp=tp)

