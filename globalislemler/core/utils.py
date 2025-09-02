
from datetime import datetime, timezone, timedelta
from typing import Tuple

TF_SECONDS = {"15m": 900, "1h": 3600, "4h": 14400, "1d": 86400}

def floor_time(dt: datetime, timeframe: str) -> datetime:
    sec = TF_SECONDS[timeframe]
    epoch = int(dt.replace(tzinfo=timezone.utc).timestamp())
    floored = epoch - (epoch % sec)
    return datetime.fromtimestamp(floored, tz=timezone.utc)

def iso(dt) -> str:
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
