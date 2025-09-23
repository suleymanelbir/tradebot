# /opt/tradebot/future_trade/market_stream.py
# -*- coding: utf-8 -*-
"""
MarketStream (paper/dummy) — Binance Futures için basit piyasa akışı simülatörü.

NE SAĞLAR?
- Whitelist semboller için periyodik "bar_closed" event'leri üretir (async queue).
- Son fiyat (last_price) takibini yapar.
- İsteğe bağlı olarak global endeks (TOTAL3, USDT.D, BTC.D) snapshot'larını
  /opt/tradebot/veritabani/global_data.db içindeki global_live_data tablosundan çeker.

KULLANIM ÖZETİ:
    stream = MarketStream(
        cfg=config_dict,
        logger=logger,
        whitelist=["BTCUSDT", "SOLUSDT"],
        tf_entry="1h",
        global_db="/opt/tradebot/veritabani/global_data.db"
    )
    asyncio.create_task(stream.run())  # olay üreticisini başlat
    async for ev in stream.events():
        # {'type':'bar_closed','symbol':'SOLUSDT','tf':'1h','close':101.23,'time':...}
        handle(ev)

Notlar:
- Bu sınıf varsayılan olarak PAPER/DUMMY akış üretir; websocket gerekmez.
- Websocket entegrasyonu istenirse ayrı bir WS istemcisi bu sınıfa fiyat besleyebilir.
"""

from __future__ import annotations

import asyncio
import time
import math
import random
import sqlite3
import logging
from collections import deque
from typing import Any, AsyncGenerator, Dict, List, Optional

# Global endeks sembol eşlemesi (TV sembolleri)
INDEX_MAP = {
    "TOTAL3": "CRYPTOCAP:TOTAL3",
    "USDT.D": "CRYPTOCAP:USDT.D",
    "BTC.D":  "CRYPTOCAP:BTC.D",
}


class MarketStream:
    """
    PAPER akışı üretir ve isteğe bağlı endeks snapshot'ı sağlar.
    Websocket ZORUNLU DEĞİLDİR. (Bu sınıf WS açmaz; istenirse dışarıdan beslenebilir.)
    """
    def __init__(
        self,
        cfg: Dict[str, Any],
        logger: logging.Logger,
        whitelist: List[str],
        tf_entry: str = "1h",
        tf_confirm: Optional[str] = None,
        global_db: str = "/opt/tradebot/veritabani/global_data.db",
        persistence: Optional[bool] = False  # ✅ yeni parametre
    ):
        if not isinstance(cfg, dict):
            raise TypeError("cfg bir dict olmalı")
        if not whitelist:
            raise ValueError("whitelist boş olamaz")

        self.cfg = cfg
        self.logger = logger
        self.whitelist = list(whitelist)
        self.tf_entry = tf_entry
        self.tf_confirm = tf_confirm or "4h"
        self.global_db = global_db
        self.persistence = persistence

        # Mode bilgisi
        self.mode = cfg.get("app", {}).get("mode", "paper").lower()
        self.paper = (self.mode == "paper")

        # Strateji parametreleri
        strategy_cfg = cfg.get("strategy", {}).get("params", {})
        self.ema_period = int(strategy_cfg.get("ema_period", 20))
        self.rsi_period = int(strategy_cfg.get("rsi_period", 14))
        self.adx_period = int(strategy_cfg.get("adx_period", 14))

        # Log prefix
        self.log = lambda msg: self.logger.info(f"[MarketStream] {msg}")

        # İç durum
        self._q: asyncio.Queue[Dict[str, Any]] = asyncio.Queue()
        self._indices_cache = {
            "TOTAL3": {"tf1h": {"close": 0.0, "ema20": None}, "tf4h": {"close": 0.0, "ema20": None}},
            "USDT.D": {"tf1h": {"close": 0.0, "ema20": None}, "tf4h": {"close": 0.0, "ema20": None}},
            "BTC.D":  {"tf1h": {"close": 0.0, "ema20": None}, "tf4h": {"close": 0.0, "ema20": None}},
        }
        self._series = {s: deque(maxlen=2000) for s in self.whitelist}
        self._last_prices = {s: 100.0 for s in self.whitelist}
        self._base = {s: 100.0 for s in self.whitelist}

    # -------------------- Yardımcı hesaplamalar --------------------

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
        rows: [(ts:int seconds, price:float)]
        Zamanı bucket_sec (1H=3600, 4H=14400) aralıklarına kovalayıp, her kovadaki SON close'u alır.
        """
        if not rows:
            return []
        seen: Dict[int, float] = {}
        for ts, price in rows:
            b = int(ts) // bucket_sec
            seen[b] = float(price)  # daha geç gelen aynı kovayı yazar → son kabul edilir
        buckets = sorted(seen.keys())
        return [seen[b] for b in buckets]

    def _refresh_indices(self) -> None:
        """
        global_live_data tablosundan TOTAL3/USDT.D/BTC.D için 1H ve 4H close + EMA20 hesaplar.
        DB yoksa/detaylar eksikse dummy snapshot set eder.
        """
        path = self.global_db
        conn = None
        try:
            conn = sqlite3.connect(path, timeout=2)
            cur = conn.cursor()
            limit = 2000
            out: Dict[str, Dict[str, Any]] = {}

            for k, db_sym in INDEX_MAP.items():
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
                    out[k] = {
                        "tf1h": {"close": 0.0, "ema20": None},
                        "tf4h": {"close": 0.0, "ema20": None}
                    }
                    continue

                closes_1h = self._bucketize_last_close(rows, 3600)
                closes_4h = self._bucketize_last_close(rows, 14400)

                last_1h = closes_1h[-1] if closes_1h else 0.0
                last_4h = closes_4h[-1] if closes_4h else 0.0
                ema20_1h = self._ema(closes_1h[-60:], 20) if len(closes_1h) >= 20 else None
                ema20_4h = self._ema(closes_4h[-60:], 20) if len(closes_4h) >= 20 else None

                out[k] = {
                    "tf1h": {"close": last_1h, "ema20": ema20_1h},
                    "tf4h": {"close": last_4h, "ema20": ema20_4h}
                }

            self._indices_cache = out

        except Exception as e:
            self.logger.warning(f"indices refresh failed: {e}")
            self._indices_cache = {
                "TOTAL3": {"tf1h": {"close": 0.0, "ema20": None}, "tf4h": {"close": 0.0, "ema20": None}},
                "USDT.D": {"tf1h": {"close": 0.0, "ema20": None}, "tf4h": {"close": 0.0, "ema20": None}},
                "BTC.D":  {"tf1h": {"close": 0.0, "ema20": None}, "tf4h": {"close": 0.0, "ema20": None}},
            }
        finally:
            try:
                if conn is not None:
                    conn.close()
            except Exception:
                pass

    def _mock_step(self, symbol: str, now: int) -> float:
        """
        Basit rastgele yürüme + düşük genlikli sinüs dalgası ile fiyat güncelle.
        """
        last = self._last_prices.get(symbol, 100.0)
        drift = random.uniform(-0.3, 0.3)
        wave = 0.8 * math.sin(now / 90.0)  # ~1.5 dakikalık dalga
        nxt = max(1e-6, last + drift + wave * 0.1)
        return float(nxt)

    # -------------------- Dış API --------------------

    async def run(self) -> None:
        """
        PAPER üretici: her poll_sec saniyede bir whitelist semboller için bar_closed event'i kuyruğa atar.
        Ayrıca global endeks snapshot'ını tazeler.
        """
        poll_sec = int(self.cfg.get("paper_poll_seconds", 60))
        ema_period = int(self.cfg.get("strategy", {}).get("params", {}).get("ema_period", 20))

        while True:
            try:
                now = int(time.time())

                for sym in self.whitelist:
                    # 1) Mock adım → kapanış serisine ekle
                    close = self._mock_step(sym, now)
                    self._series[sym].append(close)
                    self._last_prices[sym] = close

                    # EMA hesapla (şimdilik kullanılmıyor ama ileride gerekebilir)
                    _ = self._ema(list(self._series[sym]), ema_period)

                    # 2) Basit salınım ekle → fiyatı yumuşak şekilde dalgalandır
                    b = self._base[sym]
                    b += random.uniform(-0.6, 0.6) + 0.3 * math.sin(now / 90.0)
                    self._base[sym] = b

                    # 3) Gerçek kapanış ile baz salınımı harmanla (test için daha gerçekçi)
                    blended = 0.8 * close + 0.2 * b

                    # 4) Event oluştur ve sıraya at
                    event = {
                        "type": "bar_closed",
                        "symbol": sym,
                        "tf": self.tf_entry,
                        "close": float(blended),
                        "time": now
                    }
                    await self._q.put(event)

                # 5) Endeks snapshot güncelle
                self._refresh_indices()

            except Exception as e:
                self.logger.error(f"stream error: {e}")

            await asyncio.sleep(poll_sec)


    async def events(self) -> AsyncGenerator[Dict[str, Any], None]:
        """
        Üretilen event'leri (FIFO) async olarak teslim eder.
        """
        while True:
            ev = await self._q.get()
            yield ev

    # -------------------- Yardımcı/okuyucu metotlar --------------------

    def indices_snapshot(self) -> Dict[str, Any]:
        """
        Global endekslerin son snapshot'ını yan etkisiz (kopya) döndürür.
        """
        return dict(self._indices_cache)

    def get_last_price(self, symbol: str) -> Optional[float]:
        """
        Simülatörde tutulan son fiyatı döndürür (yoksa None).
        """
        return self._last_prices.get(symbol)
