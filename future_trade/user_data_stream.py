# /opt/tradebot/future_trade/user_data_stream.py
# -*- coding: utf-8 -*-
from __future__ import annotations

import asyncio, json, logging, time, contextlib
from typing import Any, Dict, Optional, Callable

import aiohttp


class UserDataStream:
    """
    Binance Futures User-Data Stream yöneticisi:
      - listenKey oluşturur, WS'e bağlanır, mesajları işle
      - 30dk'da bir keepalive (Binance 60dk ister; güvenli marjla)
      - listenKeyExpired/bağlantı hatasında otomatik yenile & reconnect
      - Olaylarda OCO/Sweeper/persistence/alerter tetikler
    """

    def __init__(
        self,
        client,                      # BinanceClient
        notifier,                    # Notifier
        persistence,                 # Persistence
        router=None,                 # OrderRouter (iptaller için)
        oco_watcher=None,            # OCOWatcher (tetik sonrası iptal)
        sweeper=None,                # ProtectiveSweeper (temizlik)
        logger: Optional[logging.Logger] = None,
        ws_base_url: Optional[str] = None,
    ):
        self.client = client
        self.notifier = notifier
        self.persistence = persistence
        self.router = router
        self.oco = oco_watcher
        self.sweeper = sweeper
        self.logger = logger or logging.getLogger("uds")
        self._lk_lock = asyncio.Lock()  # <<< listenKey erişimi için kilit
        # WS URL
        self.ws_base_url = ws_base_url or getattr(self.client, "ws_url", "wss://fstream.binance.com")
        # Durum
        self.listen_key: Optional[str] = None
        self._stop = asyncio.Event()
        self._keepalive_task: Optional[asyncio.Task] = None

    # ------------- lifecycle -------------
    async def _create_or_refresh_key(self) -> str:
        # Tek seferlik yaratım/yenileme için kilit
        async with self._lk_lock:
            if not self.listen_key:
                res = await self.client.create_listen_key()
                self.listen_key = res.get("listenKey")
                self.logger.info(f"[UDS] listenKey created")
                # alerts
                import contextlib
                with contextlib.suppress(Exception):
                    await self.notifier.alert({"event": "uds_listenkey", "msg": "listenKey created"})
            else:
                await self.client.keepalive_listen_key(self.listen_key)
                self.logger.debug("[UDS] keepalive OK")
            return self.listen_key
        
    async def _close_key(self):
        if self.listen_key:
            with contextlib.suppress(Exception):
                await self.client.close_listen_key(self.listen_key)
            self.listen_key = None

    def _ws_url(self) -> str:
        return f"{self.ws_base_url}/ws/{self.listen_key}"

    # ------------- keepalive loop -------------
    async def keepalive_loop(self, stop_event: asyncio.Event = None, interval_sec: int = 1800):
        """
        30 dakikada bir keepalive (Binance: en geç 60dk).
        """
        # İlk WS bağlansın diye küçük gecikme
        await asyncio.sleep(10)
        while not (stop_event and stop_event.is_set()):
            try:
                if self.listen_key:
                    # yalnızca mevcut anahtarı canlı tut
                    await self._create_or_refresh_key()
                else:
                    # run() anahtarı yaratacak; burada yalnızca bekle
                    self.logger.debug("[UDS] keepalive: listenKey missing, waiting run()")
            except Exception as e:
                self.logger.warning(f"[UDS] keepalive error: {e}")
                try:
                    await self.notifier.alert({"event":"uds_keepalive_error", "msg": str(e)})
                except Exception:
                    pass
            await asyncio.sleep(max(300, interval_sec))

    # ------------- main ws loop -------------
    async def run(self, stop_event: asyncio.Event = None, reconnect_backoff: int = 5):
        """
        WS'e bağlanır ve mesajları dinler; koparsa dinamik backoff ile tekrar dener.
        """
        while not (stop_event and stop_event.is_set()):
            try:
                # ensure key
                await self._create_or_refresh_key()
                url = self._ws_url()
                self.logger.info(f"[UDS] connecting {url}")
                async with aiohttp.ClientSession() as session:
                    async with session.ws_connect(url, heartbeat=30) as ws:
                        # bağlandı
                        with contextlib.suppress(Exception):
                            await self.notifier.info_trades({"event":"uds_connect", "msg":"user-data stream connected"})
                        # iç döngü
                        async for msg in ws:
                            if msg.type == aiohttp.WSMsgType.TEXT:
                                self._handle_message(json.loads(msg.data))
                            elif msg.type == aiohttp.WSMsgType.ERROR:
                                raise RuntimeError(f"WS error: {ws.exception()}")
                            elif msg.type == aiohttp.WSMsgType.CLOSED:
                                raise RuntimeError("WS closed")
            except Exception as e:
                self.logger.warning(f"[UDS] reconnect: {e}")
                with contextlib.suppress(Exception):
                    await self.notifier.alert({"event":"uds_reconnect", "msg": str(e)})
                await asyncio.sleep(min(60, reconnect_backoff))
                reconnect_backoff = min(60, reconnect_backoff * 2)  # exponential up to 60
                # listenKeyExpired ise anahtar sıfırla
                if "listenKeyExpired" in str(e):
                    with contextlib.suppress(Exception):
                        await self._close_key()
                    reconnect_backoff = 5  # reset

    # ------------- event router -------------
    def _handle_message(self, data: Dict[str, Any]):
        """
        Örnek event tipleri:
          - ACCOUNT_UPDATE (e=ACCOUNT_UPDATE)
          - ORDER_TRADE_UPDATE (e=ORDER_TRADE_UPDATE)
          - listenKeyExpired (e=listenKeyExpired)
          - MARGIN_CALL
        """
        try:
            et = data.get("e")
            if et == "ACCOUNT_UPDATE":
                self._on_account_update(data)
            elif et == "ORDER_TRADE_UPDATE":
                self._on_order_trade_update(data)
            elif et == "listenKeyExpired":
                self._on_listenkey_expired()
            elif et == "MARGIN_CALL":
                self._on_margin_call(data)
            else:
                self.logger.debug(f"[UDS] event {et}")
        except Exception as e:
            self.logger.error(f"[UDS] handle error: {e}")

    # ------------- handlers -------------
    def _on_account_update(self, data: Dict[str, Any]):
        """
        İstersen availableBalance vs. cacheleyebilir, margin_guard için kullanabilirsin.
        Şimdilik log+istenirse notifier.
        """
        self.logger.debug(f"[UDS] ACCOUNT_UPDATE")
        # Örnek: with contextlib.suppress(Exception):
        #   await self.notifier.info_trades({"event":"account_update"})

    def _on_order_trade_update(self, data: Dict[str, Any]):
        """
        ReduceOnly STOP/TP fill/iptal olayını yakala:
          - SL/TP filled/removed → OCO mantığıyla karşıt koruyucu iptal
          - Cache'teki sl_order_id/tp_order_id temizle
        """
        o = data.get("o") or {}
        symbol = o.get("s")
        side = o.get("S")                # BUY/SELL
        typ = (o.get("ot") or "").upper()
        status = (o.get("X") or "").upper()   # NEW, PARTIALLY_FILLED, FILLED, CANCELED, REJECTED, EXPIRED
        reduce_only = bool(o.get("R") or o.get("reduceOnly") or False)
        order_id = str(o.get("i") or "")

        # Sadece koruyucu emirlerle ilgilen
        is_protective = reduce_only and ("STOP" in typ or "TAKE_PROFIT" in typ)
        if not (symbol and is_protective and order_id):
            return

        self.logger.info(f"[UDS] {symbol} {typ} {status} ro={reduce_only} oid={order_id}")

        # cache alanlarını oku
        try:
            pos = None
            for p in self.persistence.list_open_positions() or []:
                if p.get("symbol") == symbol:
                    pos = p
                    break
        except Exception:
            pos = None

        if not pos:
            return

        sl_oid = str(pos.get("sl_order_id") or "")
        tp_oid = str(pos.get("tp_order_id") or "")

        # Hangi tür tetiklendi?
        fired_sl = ("STOP" in typ) and (order_id == sl_oid)
        fired_tp = ("TAKE_PROFIT" in typ) and (order_id == tp_oid)

        if status in ("FILLED", "CANCELED", "EXPIRED"):
            # Tetiklenenin karşıtını iptal et (OCO)
            try:
                if fired_sl and tp_oid:
                    self.router.cancel_order(symbol=symbol, order_id=int(tp_oid))
                elif fired_tp and sl_oid:
                    self.router.cancel_order(symbol=symbol, order_id=int(sl_oid))
            except Exception:
                pass

            # Cache temizliği
            try:
                if fired_sl:
                    if hasattr(self.persistence, "cache_update_sl_order_id"):
                        self.persistence.cache_update_sl_order_id(symbol, None)
                    if hasattr(self.persistence, "cache_update_sl"):
                        self.persistence.cache_update_sl(symbol, None)
                if fired_tp:
                    if hasattr(self.persistence, "cache_update_tp_order_id"):
                        self.persistence.cache_update_tp_order_id(symbol, None)
                    if hasattr(self.persistence, "cache_update_tp"):
                        self.persistence.cache_update_tp(symbol, None)
            except Exception:
                pass

            # Sweeper'a bırakmak için log
            self.logger.info(f"[UDS/OCO] {symbol} {typ} {status} → counter cancelled & cache cleared")

    def _on_listenkey_expired(self):
        # WS döngüsü reconnect ederken yeni key yaratabilsin diye force hata yaratmak yerine logla:
        self.logger.warning("[UDS] listenKeyExpired event")
        # Akış run() içinde hata ile reconnect ediyor; burada sadece uyarı:
        with contextlib.suppress(Exception):
            asyncio.create_task(self.notifier.alert({"event":"uds_listenkey_expired", "msg":"listenKey expired; reconnecting"}))

    def _on_margin_call(self, data: Dict[str, Any]):
        self.logger.warning("[UDS] MARGIN_CALL")
        with contextlib.suppress(Exception):
            asyncio.create_task(self.notifier.alert({"event":"margin_call", "msg":"Futures margin call received"}))
