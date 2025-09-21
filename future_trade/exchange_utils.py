# /opt/tradebot/future_trade/exchange_utils.py
from math import floor
from typing import Dict, Tuple


def _find(sym_info: Dict, ftype: str) -> Dict:
    for f in sym_info.get("filters", []):
        if f.get("filterType") == ftype:
            return f
    return {}

def quantize(qty: float, step: float) -> float:
    if step <= 0: return qty
    return float((int(qty / step)) * step)

def price_quantize(px: float, tick: float) -> float:
    if tick <= 0: return px
    return float((int(px / tick)) * tick)

def symbol_filters(ex_info: Dict, symbol: str) -> Tuple[float, float, float]:
    """
    return: tickSize, stepSize, minNotional
    """
    for s in ex_info.get("symbols", []):
        if s.get("symbol") == symbol:
            pf = _find(s, "PRICE_FILTER")
            lf = _find(s, "LOT_SIZE")
            nf = _find(s, "MIN_NOTIONAL") or {}
            tick = float(pf.get("tickSize", "0.01") or 0.01)
            step = float(lf.get("stepSize", "0.001") or 0.001)
            min_notional = float(nf.get("notional", "5") or 5.0)
            return tick, step, min_notional
    # güvenli varsayılanlar
    return 0.01, 0.001, 5.0
