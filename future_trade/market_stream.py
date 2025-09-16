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
        self.global_db = self.cfg.get("global_db_path", "/opt/tradebot/veritabani/global_data.db")


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




    async def run(self):
        poll_sec = 60
        while True:
            try:
                self._refresh_indices()  # <<< artık var
                for sym in self.whitelist:
                    try:
                        bars = await self._fetch_1h_bars(sym, limit=150)
                        ev = {"type": "bar_closed", "symbol": sym, "tf": "1h", "bars": bars}
                        await self._q.put(ev)
                    except Exception as se:
                        logging.warning(f"kline fetch failed for {sym}: {se}")
            except Exception as e:
                logging.error(f"stream error: {e}")
            await asyncio.sleep(poll_sec)





    async def events(self) -> AsyncGenerator[Dict[str, Any], None]:
        while True:
            ev = await self._q.get(); yield ev


    def indices_snapshot(self) -> Dict[str, Any]:
        # Strateji tarafına kopya (mutasyon olmaz)
        return dict(self._indices_cache)
