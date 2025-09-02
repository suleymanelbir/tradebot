
import logging
from typing import Tuple, Optional

logger = logging.getLogger(__name__)

def clean_price_text(raw: str) -> Optional[float]:
    if raw is None:
        return None
    s = raw.replace("\u202f", "").replace(",", "").strip()
    if not s:
        return None
    mul = 1.0
    if s.endswith("T"):
        mul, s = 1e12, s[:-1]
    elif s.endswith("B"):
        mul, s = 1e9, s[:-1]
    elif s.endswith("M"):
        mul, s = 1e6, s[:-1]
    elif s.endswith("K"):
        mul, s = 1e3, s[:-1]
    try:
        return float(s) * mul
    except ValueError:
        logger.warning("Could not parse price text", extra={"raw": raw})
        return None

def within_limits(symbol: str, price: float, limits: dict) -> Tuple[bool, bool]:
    lim = limits.get(symbol)
    if not lim:
        return True, False
    lo, hi = lim.get("lower"), lim.get("upper")
    if price is None:
        return False, True
    if lo is not None and price < lo:
        return False, True
    if hi is not None and price > hi:
        return False, True
    return True, False
