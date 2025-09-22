"""Risk & boyutlandırma: per-trade risk, kaldıraç, SL/TP planı
- qty hesabı: risk / (stop_distance)
- Filtreler: min_notional, stepSize, tickSize (exchangeInfo'dan) → TODO
"""
# future_trade/risk_manager.py

from dataclasses import dataclass
from typing import Dict, Any, Optional
from .exchange_utils import symbol_filters, quantize, price_quantize


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
        entry = 100.0
        sl    = 98.0 if signal.side == "LONG" else 102.0
        tp    = 103.0 if signal.side == "LONG" else 97.0

        dist = abs(entry - sl)
        if dist <= 0:
            return TradePlan(ok=False, reason="invalid stop distance")

        equity  = self.portfolio.equity()
        per_pct = float(self.cfg.get("per_trade_risk_pct", 0.5)) / 100.0
        risk_cap = equity * per_pct
        raw_qty = risk_cap / dist

        # EXCHANGE FILTERS (cache’ten)
        ex_info = getattr(self.portfolio, "exchange_info_cache", {}) or {}
        tick, step, min_notional = symbol_filters(ex_info, symbol)

        qty = max(quantize(raw_qty, step), step)
        q_entry = price_quantize(entry, tick)
        q_sl    = price_quantize(sl, tick)
        q_tp    = price_quantize(tp, tick)

        notional = q_entry * qty
        buf = float(self.cfg.get("min_notional_buffer", 1.1))
        if notional < (min_notional * buf):
            need_qty = (min_notional * buf) / max(q_entry, 1e-9)
            qty = max(quantize(need_qty, step), step)
            notional = q_entry * qty
            if notional < min_notional:
                return TradePlan(ok=False, reason="below_min_notional")

        return TradePlan(ok=True, symbol=symbol, side=signal.side,
                        entry=q_entry, qty=qty, sl=q_sl, tp=q_tp)
