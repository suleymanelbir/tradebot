
from typing import Dict, Optional
from .db import Database

def compute_changes(db: Database, symbol: str, live_price: float) -> Dict[str, Optional[float]]:
    out = {}
    for tf in ("15m","1h","4h","1d"):
        close = db.latest_close(symbol, tf)
        if close and close > 0:
            out[tf] = round((live_price - close) / close * 100.0, 4)
        else:
            out[tf] = None
    return out
