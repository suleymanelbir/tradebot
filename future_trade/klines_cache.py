# /opt/tradebot/future_trade/klines_cache.py
from __future__ import annotations
import asyncio, logging
from typing import Dict, List, Optional

def _atr_from_ohlc(h: List[float], l: List[float], c: List[float], period: int) -> Optional[float]:
    n = len(c)
    if n < period + 1:  # prev close da lazım
        return None
    tr_vals = []
    for i in range(-period, 0):  # son 'period' bar
        hi = h[i]; lo = l[i]; cl_prev = c[i-1]
        tr = max(hi - lo, abs(hi - cl_prev), abs(lo - cl_prev))
        tr_vals.append(tr)
    # RMA/EMA farkına girmeden basit EMA
    k = 2.0 / (period + 1.0)
    e = tr_vals[0]
    for v in tr_vals[1:]:
        e = v * k + e * (1.0 - k)
    return float(e)

class KlinesCache:
    """
    Whitelist semboller için periyodik klines çeker ve ATR sağlar.
    """
    def __init__(self, client, symbols: List[str], interval: str = "1h", limit: int = 200, logger=None):
        self.client = client
        self.symbols = symbols
        self.interval = interval
        self.limit = int(limit)
        self.logger = logger or logging.getLogger("klines_cache")
        # hafıza: {sym: {"o":[], "h":[], "l":[], "c":[], "t":[]}}
        self._buf: Dict[str, Dict[str, List[float]]] = {}

    async def run(self, stop_event: asyncio.Event = None, poll_sec: int = 30):
        while not (stop_event and stop_event.is_set()):
            try:
                for sym in self.symbols:
                    try:
                        rows = await self.client.futures_klines(sym, self.interval, self.limit)
                        if not rows:
                            continue
                        o,h,l,c,t = [],[],[],[],[]
                        for r in rows:
                            # Binance: [openTime, open, high, low, close, volume, closeTime, ...]
                            o.append(float(r[1])); h.append(float(r[2])); l.append(float(r[3])); c.append(float(r[4])); t.append(int(r[0]))
                        self._buf[sym] = {"o":o, "h":h, "l":l, "c":c, "t":t}
                    except Exception as e:
                        self.logger.debug(f"klines fetch failed for {sym}: {e}")
            except Exception as e:
                self.logger.error(f"klines loop error: {e}")
            await asyncio.sleep(max(10, poll_sec))

    def get_atr(self, symbol: str, period: int) -> Optional[float]:
        buf = self._buf.get(symbol)
        if not buf:
            return None
        return _atr_from_ohlc(buf["h"], buf["l"], buf["c"], int(period))
