"""Piyasa akışı ve kline üretimi (1H/4H):
- Binance klines (whitelist semboller) + endeksler (TOTAL3/USDT.D/BTC.D) için kaynak
- Şimdilik placeholder: REST poll ile kapanan mumları periyodik çek (WS TODO)
- events(): bar_closed event'lerini async generator ile yayımlar
"""
from __future__ import annotations  # Gelecekteki tip ipuçları için (Python 3.7+)

# Standart kütüphaneler
import asyncio
import logging
import sqlite3
import time

from collections import deque
import random


# Tip tanımları
from typing import Dict, Any, List, AsyncGenerator, TYPE_CHECKING

# Sadece type-check sırasında import edilir (döngüsel importları önler)
if TYPE_CHECKING:
    from .binance_client import BinanceClient

# Dahili modüller
from .strategy.indicators import ema

# Global DB'deki sembol isimleri
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
        persistence,
        client: "BinanceClient",   # type hint (forward)
    ):
        self.cfg = cfg
        self.whitelist = whitelist
        self.indices = indices
        self.tf_entry = tf_entry
        self.tf_confirm = tf_confirm
        self.persistence = persistence
        self.client = client                     # <<< ÖNEMLİ: sınıf alanı
        self._q: asyncio.Queue = asyncio.Queue() # event kuyruğu
        self._indices_cache: Dict[str, Any] = {} # TOTAL3/USDT.D/BTC.D snapshot
        self._last_prices: Dict[str, float] = {}
        self.global_db = self.cfg.get("global_db_path", "/opt/tradebot/veritabani/global_data.db")
        self._series: Dict[str, deque] = {sym: deque(maxlen=100) for sym in whitelist}


    async def _fetch_1h_bars(self, symbol: str, limit: int = 150):
        kl = await self.client.get_klines(symbol, interval="1h", limit=limit)  # <<< self.client
        highs = [float(x[2]) for x in kl]
        lows  = [float(x[3]) for x in kl]
        closes= [float(x[4]) for x in kl]
        close_time = int(kl[-2][6]) // 1000 if len(kl) >= 2 else int(kl[-1][6]) // 1000
        last_closed_close = float(kl[-2][4]) if len(kl) >= 2 else float(kl[-1][4])
        return {"highs": highs, "lows": lows, "closes": closes, "t": close_time, "last_close": last_closed_close}

    @staticmethod
    def _bucketize_last_close(rows: List[tuple], bucket_sec: int) -> List[float]:
        """
        rows: [(ts:int seconds), price:float] — zamana göre kovalayıp her kovadaki son close'u alır.
        bucket_sec: 3600 (1H) ya da 14400 (4H)
        """
        if not rows:
            return []
        seen = {}
        for ts, price in rows:
            b = int(ts) // bucket_sec
            # Aynı kovaya daha geç gelen değer, 'son kapanış' sayılır
            seen[b] = float(price)
        buckets = sorted(seen.keys())
        return [seen[b] for b in buckets]

    def _refresh_indices(self) -> None:
        """
        Global DB'den (global_live_data) son verileri okuyup
        TOTAL3 / USDT.D / BTC.D için 1H ve 4H 'close' ve EMA20 hesaplar.
        Sonuçları self._indices_cache'e yazar.
        """
        path = self.global_db
        try:
            conn = sqlite3.connect(path, timeout=3)
            cur = conn.cursor()
            # Her indeks için son ~2000 satır al (fazlası varsa yeter)
            limit = 2000
            out = {}
            for k, db_sym in INDEX_MAP.items():
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
                if not rows:
                    out[k] = {"tf1h": {}, "tf4h": {}}
                    continue

                # Saatlik ve 4 saatlik son 'close' serileri
                closes_1h = self._bucketize_last_close(rows, 3600)
                closes_4h = self._bucketize_last_close(rows, 14400)

                # En az 20 örnek varsa EMA20 hesapla
                ema20_1h = ema(closes_1h[-60:], 20) if len(closes_1h) >= 20 else None
                ema20_4h = ema(closes_4h[-60:], 20) if len(closes_4h) >= 20 else None

                last_1h = closes_1h[-1] if closes_1h else None
                last_4h = closes_4h[-1] if closes_4h else None

                out[k] = {
                    "tf1h": {"close": last_1h, "ema20": ema20_1h},
                    "tf4h": {"close": last_4h, "ema20": ema20_4h},
                }
            self._indices_cache = out
        except Exception as e:
            logging.warning(f"indices refresh failed: {e}")
        finally:
            try:
                conn.close()
            except Exception:
                pass

    def get_last_price(self, symbol: str):
        return self._last_prices.get(symbol)

    def _mock_price(self, sym: str, now: int) -> float:
        # çok basit bir salınım: 100 etrafında +/- 5
        import math
        base = 100.0
        return base + 5.0 * math.sin(now / 60.0)  # ~1 dakikada bir salınsın

    def _ema(self, values, period=20):
        if not values or len(values) < period:
            return 0.0  # Yetersiz veri varsa None yerine güvenli varsayılan
        k = 2 / (period + 1)
        ema = values[0]
        for v in values[1:]:
            ema = v * k + ema * (1 - k)
        return ema



    async def run(self):
        tick = 0
        poll_sec = 60
        while True:
            try:
                now = int(time.time())
                base = getattr(self, "_base_price", 100.0)

                for sym in self.whitelist:
                    # küçük gürültü ile fiyat salla (paper için yeterli)
                    base += random.uniform(-0.5, 0.5)
                    self._series[sym].append(base)

                    # EMA hesapla (son 20 bar üzerinden)
                    ema20 = self._ema(list(self._series[sym]), 20)

                    # bar_closed event oluştur
                    event = {
                        "type": "bar_closed",
                        "symbol": sym,
                        "tf": self.tf_entry,
                        "close": base,
                        "ema20": ema20,
                        "time": now
                    }
                    self._last_prices[sym] = base
                    await self._q.put(event)

                # Dummy indeks snapshot (ileride global veritabanından alınabilir)
                self._indices_cache = {
                    "TOTAL3": {
                        "tf1h": {"ema20": 0.0, "close": 0.0},
                        "tf4h": {"ema20": 0.0, "close": 0.0}
                    },
                    "USDT.D": {
                        "tf1h": {"ema20": 0.0, "close": 0.0}
                    },
                    "BTC.D": {
                        "tf1h": {"ema20": 0.0, "close": 0.0}
                    }
                }

                tick += 1
            except Exception as e:
                logging.error(f"stream error: {e}")
            await asyncio.sleep(poll_sec)
            
            
    async def events(self) -> AsyncGenerator[Dict[str, Any], None]:
        while True:
            ev = await self._q.get(); yield ev


    def indices_snapshot(self) -> Dict[str, Any]:
        # Strateji tarafına kopya (mutasyon olmaz)
        return dict(self._indices_cache)
