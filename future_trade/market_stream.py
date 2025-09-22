# /opt/tradebot/future_trade/market_stream.py
"""
Piyasa akışı (paper/dummy) ve endeks snapshot:
- Her 60 sn: whitelist semboller için kapanmış bar (bar_closed) üretir (close + ema20)
- TOTAL3 / USDT.D / BTC.D snapshot'ını (mümkünse) global_data.db'den çeker
- events(): bar_closed event'lerini async generator ile yayımlar
"""
from __future__ import annotations

import asyncio
import logging
import random
import sqlite3
import time
import math

from collections import deque
from typing import Any, AsyncGenerator, Dict, List, Optional


# Global DB'deki endeks sembolleri
INDEX_MAP = {
    "TOTAL3": "CRYPTOCAP:TOTAL3",
    "USDT.D": "CRYPTOCAP:USDT.D",
    "BTC.D":  "CRYPTOCAP:BTC.D",
}


class MarketStream:
    def __init__(
        self,
        cfg: Dict[str, Any],
        whitelist: List[str],
        indices: List[str],
        tf_entry: str,
        tf_confirm: str,
        persistence,  # şimdilik kullanılmıyor; gelecekte klines cache için kullanılabilir
    ):
        self.cfg = cfg
        self.whitelist = whitelist
        self.indices = indices
        self.tf_entry = tf_entry
        self.tf_confirm = tf_confirm
        self.persistence = persistence

        self._q: asyncio.Queue = asyncio.Queue(maxsize=1000)
        self._indices_cache: Dict[str, Dict[str, Any]] = {}

        # Paper fiyat simülasyonu için kısa history (EMA20 hesabı)
        self._series: Dict[str, deque] = {sym: deque(maxlen=200) for sym in whitelist}
        self._last_prices: Dict[str, float] = {}

        # Global endeks DB yolu (varsa)
        self.global_db: str = self.cfg.get("global_db_path", "/opt/tradebot/veritabani/global_data.db")

        # Başlangıç fiyatlarını sabitle (paper)
        base = float(self.cfg.get("paper_base_price", 100.0))
        for s in self.whitelist:
            # Her sembole hafif farklı başlangıç
            self._series[s].append(base + random.uniform(-2.0, 2.0))
            self._last_prices[s] = self._series[s][-1]

    # ------- Basit yardımcılar -------

    @staticmethod
    def _ema(values: List[float], period: int = 20) -> Optional[float]:
        if not values or len(values) < period:
            return None
        k = 2.0 / (period + 1.0)
        e = values[0]
        for v in values[1:]:
            e = v * k + e * (1.0 - k)
        return e

    @staticmethod
    def _bucketize_last_close(rows: List[tuple], bucket_sec: int) -> List[float]:
        """
        rows: [(ts:int seconds), price:float]
        Zamanı bucket_sec (1H=3600, 4H=14400) aralıklarına kova'layıp, her kovadaki **son** close'u alır.
        """
        if not rows:
            return []
        seen: Dict[int, float] = {}
        for ts, price in rows:
            b = int(ts) // bucket_sec
            seen[b] = float(price)  # aynı kovaya gelen daha geç kayıt 'son' kabul edilir
        buckets = sorted(seen.keys())
        return [seen[b] for b in buckets]

    def _refresh_indices(self) -> None:
        """
        global_data.db varsa, TOTAL3/USDT.D/BTC.D için 1H ve 4H close + EMA20 hesapla.
        Yoksa dummy snapshot bırak.
        """
        path = self.global_db
        try:
            conn = sqlite3.connect(path, timeout=2)
            cur = conn.cursor()
            limit = 2000
            out: Dict[str, Dict[str, Any]] = {}
            for k, db_sym in INDEX_MAP.items():
                # yoksa atla
                try:
                    cur.execute(
                        """
                        SELECT CAST(strftime('%s', timestamp) AS INTEGER) AS ts, live_price
                        FROM global_live_data
                        WHERE symbol = ?
                        ORDER BY ts DESC
                        LIMIT ?
                        """,
                        (db_sym, limit),
                    )
                    rows = cur.fetchall()
                except Exception:
                    rows = []

                if not rows:
                    out[k] = {"tf1h": {"close": 0.0, "ema20": None}, "tf4h": {"close": 0.0, "ema20": None}}
                    continue

                closes_1h = self._bucketize_last_close(rows, 3600)
                closes_4h = self._bucketize_last_close(rows, 14400)

                last_1h = closes_1h[-1] if closes_1h else 0.0
                last_4h = closes_4h[-1] if closes_4h else 0.0
                ema20_1h = self._ema(closes_1h[-60:], 20) if len(closes_1h) >= 20 else None
                ema20_4h = self._ema(closes_4h[-60:], 20) if len(closes_4h) >= 20 else None

                out[k] = {"tf1h": {"close": last_1h, "ema20": ema20_1h},
                          "tf4h": {"close": last_4h, "ema20": ema20_4h}}

            self._indices_cache = out

        except Exception as e:
            # Dosya yoksa veya tablo yoksa uyar, dummy snapshot'a düş
            logging.warning(f"indices refresh failed: {e}")
            self._indices_cache = {
                "TOTAL3": {"tf1h": {"close": 0.0, "ema20": None}, "tf4h": {"close": 0.0, "ema20": None}},
                "USDT.D": {"tf1h": {"close": 0.0, "ema20": None}},
                "BTC.D":  {"tf1h": {"close": 0.0, "ema20": None}},
            }
        finally:
            try:
                conn.close()
            except Exception:
                pass

    def _mock_step(self, symbol: str, now: int) -> float:
        """
        Paper fiyat üretimi: küçük random yürüme + sinüs dalgası.
        """
        import math
        last = self._last_prices.get(symbol, 100.0)
        drift = random.uniform(-0.3, 0.3)
        wave = 0.8 * math.sin(now / 90.0)  # ~1.5 dakikalık dalga
        nxt = max(1e-6, last + drift + wave * 0.1)
        return float(nxt)

    # ------- Dış API -------

    async def run(self):
        poll_sec = int(self.cfg.get("paper_poll_seconds", 60))
        ema_period = int(self.cfg.get("strategy", {}).get("params", {}).get("ema_period", 20))

        while True:
            try:
                now = int(time.time())

                # 1) Her sembol için bir kapanış oluştur → EMA20 hesapla → event sıraya at
                for sym in self.whitelist:
                    close = self._mock_step(sym, now)
                    self._series[sym].append(close)
                    self._last_prices[sym] = close

                    ema20 = self._ema(list(self._series[sym]), ema_period)

                    # en üste: import math, random
                    # sınıfta: self._base = {s: 100.0 for s in self.whitelist}
                    # run() içinde, her sembol için:
                    b = self._base[sym]
                    b += random.uniform(-0.6, 0.6) + 0.3*math.sin(now/90.0)
                    self._base[sym] = b
                    event = {"type": "bar_closed", "symbol": sym, "tf": self.tf_entry, "close": b, "time": now}

                    await self._q.put(event)

                # 2) Endeks snapshot güncelle (opsiyonel global DB)
                self._refresh_indices()

            except Exception as e:
                logging.error(f"stream error: {e}")

            await asyncio.sleep(poll_sec)

    async def events(self) -> AsyncGenerator[Dict[str, Any], None]:
        while True:
            ev = await self._q.get()
            yield ev

    def indices_snapshot(self) -> Dict[str, Any]:
        # Strateji tarafına kopya verelim (yan etkisiz)
        return dict(self._indices_cache)

    def get_last_price(self, symbol: str) -> Optional[float]:
        return self._last_prices.get(symbol)
