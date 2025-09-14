"""Piyasa akışı ve kline üretimi (1H/4H):
- Binance klines (whitelist semboller) + endeksler (TOTAL3/USDT.D/BTC.D) için kaynak
- Şimdilik placeholder: REST poll ile kapanan mumları periyodik çek (WS TODO)
- events(): bar_closed event'lerini async generator ile yayımlar
"""
import asyncio, logging, time
from typing import Dict, Any, List, AsyncGenerator
from .persistence import Persistence


class MarketStream:
    def __init__(self, cfg: Dict[str, Any], whitelist: List[str], indices: List[str], tf_entry: str, tf_confirm: str, persistence: Persistence):
        self.cfg = cfg; self.whitelist = whitelist; self.indices = indices
        self.tf_entry = tf_entry; self.tf_confirm = tf_confirm
        self.persistence = persistence
        self._q = asyncio.Queue(maxsize=1000)
        self._indices_cache: Dict[str, Dict[str, Any]] = {}


    async def run(self):
        # TODO: WS abonelik; şimdilik taklit: her 60 sn poll et ve kapanan bar üret
        while True:
            try:
                now = int(time.time())
                # Placeholder: sembol başına sahte bar_closed üret
                for sym in self.whitelist:
                    event = {"type": "bar_closed", "symbol": sym, "tf": self.tf_entry, "close": 100.0, "time": now}
                await self._q.put(event)
                # İndeks snapshot güncelle (TOTAL3/USDT.D/BTC.D)
                self._indices_cache = {
                "TOTAL3": {"tf1h": {"ema20": 0, "close": 0}, "tf4h": {"ema20": 0, "close": 0}},
                "USDT.D": {"tf1h": {"ema20": 0, "close": 0}},
                "BTC.D": {"tf1h": {"ema20": 0, "close": 0}},
                }
            except Exception as e:
                logging.error(f"stream error: {e}")
            await asyncio.sleep(60)


    async def events(self) -> AsyncGenerator[Dict[str, Any], None]:
        while True:
            ev = await self._q.get(); yield ev


    def indices_snapshot(self) -> Dict[str, Any]:
        return self._indices_cache