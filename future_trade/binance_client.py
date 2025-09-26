# /opt/tradebot/future_trade/binance_client.py
"""
Binance USDT-M Futures HTTP/WS istemcisi
- testnet/live: ağ açık
- paper: ağ KAPALI (stub/dummy yanıtlar); botu hızlıca ayağa kaldırmak içindir
"""
import time, hmac, hashlib, logging
from typing import Dict, Any, Optional
import httpx
from urllib.parse import urlencode
import asyncio
from future_trade.exchange_normalizer import ExchangeNormalizer  # dosyanın başına

def _paper_filters_for(symbol: str) -> dict:
    # Basit ve güvenli varsayılanlar; istersen sembol bazında özel değerler koyabilirsin
    return {
        "symbol": symbol,
        "status": "TRADING",
        "filters": [
            {"filterType": "PRICE_FILTER", "tickSize": "0.01"},
            {"filterType": "LOT_SIZE",    "stepSize": "0.001"},
            {"filterType": "MIN_NOTIONAL","notional": "5"}
        ]
    }

class BinanceClient:
    def __init__(self, cfg: Dict[str, Any], mode: str):
        self.mode = mode.lower()
        self.paper = (self.mode == "paper")

        # API key/secret
        self.key = cfg.get("key", "")
        self.secret = (cfg.get("secret", "") or "").encode()

        self.normalizer = ExchangeNormalizer(cfg)  

        # recvWindow (ms)
        self.recv = int(cfg.get("recv_window_ms", 5000))

        # URL seçimleri (mode’a göre)
        self.base = cfg.get("base_url", "https://fapi.binance.com")
        self.ws   = cfg.get("ws_url",   "wss://fstream.binance.com")

        if self.mode in ("paper", "testnet"):
            self.base = cfg.get("testnet_base_url", self.base)
            self.ws   = cfg.get("testnet_ws_url",   self.ws)

        # HTTP client (yalnızca ağ açıkken)
        self._client: Optional[httpx.AsyncClient] = None
        if not self.paper:
            timeout = httpx.Timeout(connect=10.0, read=15.0, write=15.0, pool=15.0)
            self._client = httpx.AsyncClient(base_url=self.base, timeout=timeout)

        # Loglama
        masked = f"{self.key[:4]}...{self.key[-4:]}" if self.key else "<empty>"
        logging.info("[BINANCE] mode=%s base=%s key=%s recvWindow=%s",
                     self.mode, self.base, masked, self.recv)

        if self.paper:
            self._paper_network_disabled = True
            logging.info("[BINANCE] PAPER MODE: network disabled; all requests are stubbed")

    # -------------------- yardımcılar --------------------
    def _paper_stub(self, endpoint: str, extra: Dict[str, Any] | None = None) -> Dict[str, Any]:
        """Paper modunda güvenli sahte cevap üretir."""
        payload = {"paper": True, "endpoint": endpoint, "ts": int(time.time() * 1000)}
        if extra:
            payload.update(extra)
        return payload

    # binance_client.py -- sınıfa ekle
    async def klines(self, symbol: str, interval: str = "1h", limit: int = 150):
        """
        USDT-M futures klines. Dönüş: [ [open_time, open, high, low, close, volume, close_time, ...], ... ]
        """
        r = await self._client.get("/fapi/v1/klines", params={"symbol": symbol, "interval": interval, "limit": limit})
        r.raise_for_status()
        return r.json()



    async def _signed(self, method: str, path: str, params: Dict[str, Any] | None) -> Dict[str, Any]:
        if self.paper:
            # ağ yok; sadece izleme amaçlı stub dön
            return self._paper_stub(path, {"method": method, "params": dict(params or {})})

        ts = int(time.time() * 1000)
        p = dict(params or {})
        p.update({"timestamp": ts, "recvWindow": self.recv})
        qs  = urlencode(p, doseq=True)
        sig = hmac.new(self.secret, qs.encode(), hashlib.sha256).hexdigest()
        url = f"{path}?{qs}&signature={sig}"
        headers = {"X-MBX-APIKEY": self.key}
        r = await self._client.request(method, url, headers=headers)
        if r.status_code >= 400:
            logging.error("Binance response body: %s", r.text)
        r.raise_for_status()
        return r.json()

    async def _public(self, method: str, path: str, params: Dict[str, Any] | None = None) -> Dict[str, Any]:
        if self.paper:
            return self._paper_stub(path, {"method": method, "params": dict(params or {})})
        r = await self._client.request(method, path, params=params or {})
        if r.status_code >= 400:
            logging.error("Binance PUBLIC response body: %s", r.text)
        r.raise_for_status()
        return r.json()

    # -------------------- public uçlar (stub destekli) --------------------
    async def get_price(self, symbol: str) -> float:
        if self.paper:
            # Basit sabit fiyat (strateji/risk akışını çalıştırmak için yeterli)
            return 100.0
        data = await self._public("GET", "/fapi/v1/ticker/price", {"symbol": symbol})
        return float(data["price"])

    async def get_klines(self, symbol: str, interval: str = "1h", limit: int = 2):
        if self.paper:
            now = int(time.time() * 1000)
            # Açıkça basit yapay kline: [open_time, o, h, l, c, v, close_time, ...]
            return [
                [now - 2*3600_000, 100, 101, 99, 100.5, 10, now - 3600_000, 0, 0, 0, 0, 0],
                [now - 1*3600_000, 100.5, 101.2, 100.2, 100.8, 11, now,          0, 0, 0, 0, 0],
            ][:limit]
        return await self._public("GET", "/fapi/v1/klines", {"symbol": symbol, "interval": interval, "limit": limit})

    async def exchange_info(self) -> Dict[str, Any]:
        # PAPER modda ağ kapalı; whitelist için sentetik exchangeInfo dön
        if self.base and "binancefuture.com" not in self.base and "binance.com" not in self.base:
            # Eğer paper modda base_url’i boş ya da farklı tutuyorsan; ama en garantisi flag kontrolü:
            pass

        # PAPER kip kontrolü (senin init’te ‘mode’ bilgisi yoksa bir bayrak ekleyebilirsin)
        # Kolay çözüm: “PAPER MODE: network disabled...” log’unu basıyorsun; onu bayrak olarak sakladıysan kontrol et.
        try:
            # “network disabled” ise sentetik dön
            # self._network_disabled gibi bir bayrak kullanıyorsan:
            if getattr(self, "_paper_network_disabled", False):
                
                # whitelist’i config’ten app içinde biliyoruz; burada bilmiyorsak generic örnek:
                syms = ["BTCUSDT", "ETHUSDT", "SOLUSDT"]
                return {"symbols": [_paper_filters_for(s) for s in syms]}
        except Exception:
            pass

        # Normal modda gerçek endpoint
        r = await self._client.get("/fapi/v1/exchangeInfo")
        r.raise_for_status()
        return r.json()


    # -------------------- private/signed uçlar (stub destekli) --------------------
    async def get_account(self) -> Dict[str, Any]:
        """
        /fapi/v2/account – serbest bakiye ve marj bilgileri
        PAPER modda stub veri döner, gerçek modda imzalı GET çağrısı yapılır.
        """
        if getattr(self, "mode", "paper").lower() == "paper" and getattr(self, "_paper_network_disabled", True):
            # PAPER: basit stub
            return {
                "totalWalletBalance": "1000",
                "availableBalance": "1000"
            }

        get = getattr(self, "_get_signed", None) or getattr(self, "http_get_signed", None)
        if not callable(get):
            raise RuntimeError("signed GET helper not found")

        return await get("/fapi/v2/account", params={})
    
    
    async def get_position_risk(self, symbol: str = None):
        """
        /fapi/v2/positionRisk – pozisyonların risk metrikleri
        """
        if getattr(self, "mode", "paper").lower() == "paper" and getattr(self, "_paper_network_disabled", True):
            return []
        get = getattr(self, "_get_signed", None) or getattr(self, "http_get_signed", None)
        if not callable(get):
            raise RuntimeError("signed GET helper not found")
        params = {}
        if symbol:
            params["symbol"] = symbol
        return await get("/fapi/v2/positionRisk", params=params)


    async def get_leverage_brackets(self, symbol: str):
        """
        /fapi/v1/leverageBracket – bakım marjı basamakları
        Normalize edilmiş bracket listesi döner: floor, cap, mmr
        """
        # 1) PAPER mode için varsayılan tek basamak
        if getattr(self, "mode", "paper").lower() == "paper" and getattr(self, "_paper_network_disabled", True):
            return [{"symbol": symbol, "brackets": [
                {"floor": 0.0, "cap": 1e12, "mmr": 0.004}
            ]}]

        # 2) GET helper fallback (signed veya unsigned)
        get = getattr(self, "_get_signed", None) or getattr(self, "http_get_signed", None) or getattr(self, "_get", None)
        if not callable(get):
            raise RuntimeError("GET helper not found for leverageBracket")

        # 3) API çağrısı
        res = await get("/fapi/v1/leverageBracket", params={"symbol": symbol})

        # 4) Normalize: float dönüşümü ve sıralama
        brackets = []
        try:
            raw = res[0]["brackets"] if isinstance(res, list) and res and "brackets" in res[0] else res.get("brackets", [])
            for r in raw:
                try:
                    floor = float(r.get("notionalFloor", 0))
                    cap = float(r.get("notionalCap", float("inf")))
                    mmr = float(r.get("maintMarginRatio", 0.004))
                    brackets.append({"floor": floor, "cap": cap, "mmr": mmr})
                except Exception:
                    continue
            brackets.sort(key=lambda x: x["floor"])
        except Exception:
            pass

        return [{"symbol": symbol, "brackets": brackets}]


    async def get_available_usdt(self) -> float:
        acc = await self.get_account()
        try:
            if "availableBalance" in acc:
                return float(acc["availableBalance"])
        except Exception:
            pass
        for a in acc.get("assets", []):
            if a.get("asset") == "USDT":
                return float(a.get("availableBalance", 0))
        return 0.0

    async def get_position_side(self):
        if self.paper:
            return {"dualSidePosition": False}
        return await self._signed("GET", "/fapi/v1/positionSide/dual", {})

    async def set_position_side_oneway(self):
        if self.paper:
            logging.info("paper: positionSide ONE_WAY (stub)")
            return {"paper": True, "msg": "No need to change"}
        # zaten ONE_WAY ise -4059 dönebilir; sorun değil
        try:
            cur = await self.get_position_side()
            if not bool(cur.get("dualSidePosition", True)):
                logging.info("positionSide already ONE_WAY")
                return {"code": -4059, "msg": "No need to change"}
        except Exception:
            pass
        return await self._signed("POST", "/fapi/v1/positionSide/dual", {"dualSidePosition": "false"})

    async def set_margin_type(self, symbol: str, margin_type: str = "ISOLATED"):
        if self.paper:
            logging.info("paper: marginType %s for %s (stub)", margin_type, symbol)
            return {"paper": True}
        return await self._signed("POST", "/fapi/v1/marginType", {"symbol": symbol, "marginType": margin_type})

    async def set_leverage(self, symbol: str, leverage: int):
        if self.paper:
            logging.info("paper: leverage %s for %s (stub)", leverage, symbol)
            return {"paper": True}
        return await self._signed("POST", "/fapi/v1/leverage", {"symbol": symbol, "leverage": int(leverage)})

    async def place_order(self, **params) -> Dict[str, Any]:
        if self.paper:
            # Emir vermez; yalnızca log + stub döner (OrderRouter DB'ye yazar)
            logging.info("paper: place_order stub %s", params)
            return {"paper": True, "status": "ACK", "params": params}
        return await self._signed("POST", "/fapi/v1/order", params)

    async def order_test(self, **kwargs):
        if self.paper:
            return {"paper": True}
        return await self._signed("POST", "/fapi/v1/order/test", kwargs)

    async def cancel_order(self, symbol: str, orderId: int = None, origClientOrderId: str = None):
        if self.paper:
            logging.info("paper: cancel_order stub %s %s %s", symbol, orderId, origClientOrderId)
            return {"paper": True, "status": "CANCELED"}
        p = {"symbol": symbol}
        if orderId: p["orderId"] = orderId
        if origClientOrderId: p["origClientOrderId"] = origClientOrderId
        return await self._signed("DELETE", "/fapi/v1/order", p)

    async def get_order(self, symbol: str, **kwargs) -> dict:
        if self.paper:
            return {"paper": True, "symbol": symbol, **kwargs}
        p = {"symbol": symbol}; p.update(kwargs)
        return await self._signed("GET", "/fapi/v1/order", p)
    
    def new_order(self, **kwargs):
        """
        Sync wrapper: async new_order varsa onu çağırır.
        """
        async_impl = getattr(self, "async_new_order", None)
        if async_impl is None:
            # Bazı client'larda method adı doğrudan async cancel_order/new_order olabilir.
            async_impl = getattr(self, "new_order_async", None)
        if async_impl is None:
            # Son çare: new_order zaten sync ise doğrudan çağır.
            meth = getattr(self, "_new_order_sync_impl", None)
            if meth:
                return meth(**kwargs)
            # Eğer gerçekten async tanımın 'new_order' ise ve biz buraya geldiysek:
            raise AttributeError("async_new_order / new_order_async bulunamadı, sync impl da yok")

        loop = None
        try:
            loop = asyncio.get_event_loop()
        except RuntimeError:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
        if loop.is_running():
            raise RuntimeError("Event loop zaten çalışıyor. Router'ı async bağlamda çağır ya da sync wrapper'ı dışarıdan kullanma.")
        return loop.run_until_complete(async_impl(**kwargs))


    async def open_orders(self, symbol: str):
        if self.paper:
            return []
        return await self._signed("GET", "/fapi/v1/openOrders", {"symbol": symbol})

    async def all_orders(self, symbol: str, **kwargs) -> Dict[str, Any]:
        if self.paper:
            return {"paper": True, "orders": []}
        return await self._signed("GET", "/fapi/v1/allOrders", {"symbol": symbol, **kwargs})

    async def positions(self):
        if self.paper:
            return []
        return await self._signed("GET", "/fapi/v2/positionRisk", {})

    async def position_risk(self, symbol: str = None):
        if self.paper:
            return []
        p = {}
        if symbol: p["symbol"] = symbol
        return await self._signed("GET", "/fapi/v2/positionRisk", p)

    async def bootstrap_exchange(self, exch_cfg: Dict[str, Any]) -> None:
        """Başlangıç ONE_WAY ve (opsiyonel) margin/leverage ayarı."""
        if self.paper:
            logging.info("paper: bootstrap_exchange skipped")
            return

        # Pozisyon modu: ONE_WAY
        try:
            await self._signed("POST", "/fapi/v1/positionSide/dual", {"dualSidePosition": "false"})
        except Exception as e:
            logging.warning(f"positionSide setup warning: {e}")

        # Sembol listesi ve margin/kaldıraç ayarları
        wl = exch_cfg.get("symbols_whitelist", [])
        margin_mode = exch_cfg.get("margin_mode", "ISOLATED")
        lev_cfg = exch_cfg.get("leverage", {})

        for sym in wl:
            try:
                # Margin tipi: ISOLATED veya CROSS
                margin_type = "ISOLATED" if margin_mode == "ISOLATED" else "CROSS"
                await self._signed("POST", "/fapi/v1/marginType", {"symbol": sym, "marginType": margin_type})

                # Kaldıraç ayarı
                lev = int(lev_cfg.get(sym, lev_cfg.get("default", 3)))
                await self._signed("POST", "/fapi/v1/leverage", {"symbol": sym, "leverage": lev})

            except Exception as e:
                logging.warning(f"bootstrap warn {sym}: {e}")


    async def close(self):
        if self._client is not None:
            try:
                await self._client.aclose()
            except Exception:
                pass

    # Sınıf içine ekle
    async def futures_klines(self, symbol: str, interval: str = "1h", limit: int = 200):
        """
        Binance Futures kline verisi (open, high, low, close, volume).
        Dönüş: [ [openTime, open, high, low, close, volume, closeTime, ...], ... ]
        """
        # PAPER modda da gerçek endpoint'e istek atmak istemiyorsan burada stub döndürebilirsin.
        if getattr(self, "mode", "paper").lower() == "paper" and getattr(self, "_paper_network_disabled", True):
            # Basit stub: market_stream sadece close ürettiği için ATR'yi step_pct'e düşüreceğiz.
            return []  # boş dönersek ATR sağlayıcı fallback'e düşer

        params = {
            "symbol": symbol,
            "interval": interval,
            "limit": int(limit)
        }
        # Projendeki HTTP get yardımcı fonksiyonun adı farklı olabilir → kendi helper’ına göre ayarla
        # Örn: await self._get("/fapi/v1/klines", params)
        get = getattr(self, "_get", None) or getattr(self, "http_get", None)
        if not callable(get):
            raise RuntimeError("binance_client: _get/http_get helper not found")
        return await get("/fapi/v1/klines", params=params)


    # Sınıf içine ekle
    async def list_open_orders(self, symbol: str = None):
        """
        Açık emirleri döndürür. Futures için /fapi/v1/openOrders
        Dönüş: [ {...order...}, ... ]
        """
        # PAPER modda ağ kapalıysa boş liste döndür
        if getattr(self, "mode", "paper").lower() == "paper" and getattr(self, "_paper_network_disabled", True):
            return []

        params = {}
        if symbol:
            params["symbol"] = symbol

        get = getattr(self, "_get", None) or getattr(self, "http_get", None)
        if not callable(get):
            raise RuntimeError("binance_client: _get/http_get helper not found")

        # Binance, openOrders için imzalı GET ister (timestamp/recvWindow)
        # Projede imzalı GET helper'ınız varsa onu kullanın:
        signed_get = getattr(self, "_get_signed", None) or getattr(self, "http_get_signed", None)
        if callable(signed_get):
            return await signed_get("/fapi/v1/openOrders", params=params)
        # Yoksa _get zaten imzalı sürümü kapsıyorsa onu kullanın
        return await get("/fapi/v1/openOrders", params=params)

    # --- User-Data Stream (listenKey) ---  (FUTURES)
    async def create_listen_key(self):
        """
        POST /fapi/v1/listenKey → {"listenKey": "..."}  (API-KEY header, signature GEREKMEZ)
        """
        if getattr(self, "mode", "paper").lower() == "paper" and getattr(self, "_paper_network_disabled", True):
            return {"listenKey": "paper_listen_key"}
        return await self._api_key_post("/fapi/v1/listenKey", data={})

    async def keepalive_listen_key(self, listen_key: str):
        """
        PUT /fapi/v1/listenKey (API-KEY header) — 30~60 dk'da bir yenile
        """
        if getattr(self, "mode", "paper").lower() == "paper" and getattr(self, "_paper_network_disabled", True):
            return {"code": 200, "msg": "paper keepalive ok"}
        return await self._api_key_put("/fapi/v1/listenKey", data={"listenKey": listen_key})

    async def close_listen_key(self, listen_key: str):
        """
        DELETE /fapi/v1/listenKey (API-KEY header)
        """
        if getattr(self, "mode", "paper").lower() == "paper" and getattr(self, "_paper_network_disabled", True):
            return {"code": 200, "msg": "paper close ok"}
        return await self._api_key_delete("/fapi/v1/listenKey", data={"listenKey": listen_key})


    # ---- API-KEY (unsigned) helpers for listenKey ----
    async def _api_key_post(self, path: str, data: dict | None = None):
        import aiohttp, asyncio
        base = getattr(self, "base_url", "https://fapi.binance.com")
        api_key = getattr(self, "api_key", None) or getattr(self, "key", None)
        url = f"{base}{path}"
        headers = {"X-MBX-APIKEY": api_key} if api_key else {}
        # varsa mevcut helper'ları dene
        for name in ("_post", "http_post"):
            fn = getattr(self, name, None)
            if callable(fn):
                res = fn(path, data=data or {}, headers=headers)
                return await res if asyncio.iscoroutine(res) else res
        # yoksa doğrudan aiohttp ile çağır
        sess = getattr(self, "_session", None)
        if sess is None:
            self._session = aiohttp.ClientSession()
            sess = self._session
        async with sess.post(url, data=data or {}, headers=headers, timeout=15) as r:
            r.raise_for_status()
            return await r.json()

    async def _api_key_put(self, path: str, data: dict | None = None):
        import aiohttp, asyncio
        base = getattr(self, "base_url", "https://fapi.binance.com")
        api_key = getattr(self, "api_key", None) or getattr(self, "key", None)
        url = f"{base}{path}"
        headers = {"X-MBX-APIKEY": api_key} if api_key else {}
        for name in ("_put", "http_put"):
            fn = getattr(self, name, None)
            if callable(fn):
                res = fn(path, data=data or {}, headers=headers)
                return await res if asyncio.iscoroutine(res) else res
        sess = getattr(self, "_session", None)
        if sess is None:
            import aiohttp as _aiohttp
            self._session = _aiohttp.ClientSession()
            sess = self._session
        async with sess.put(url, data=data or {}, headers=headers, timeout=15) as r:
            r.raise_for_status()
            return await r.json()

    async def _api_key_delete(self, path: str, data: dict | None = None):
        import aiohttp, asyncio
        base = getattr(self, "base_url", "https://fapi.binance.com")
        api_key = getattr(self, "api_key", None) or getattr(self, "key", None)
        url = f"{base}{path}"
        headers = {"X-MBX-APIKEY": api_key} if api_key else {}
        for name in ("_delete", "http_delete"):
            fn = getattr(self, name, None)
            if callable(fn):
                res = fn(path, data=data or {}, headers=headers)
                return await res if asyncio.iscoroutine(res) else res
        sess = getattr(self, "_session", None)
        if sess is None:
            import aiohttp as _aiohttp
            self._session = _aiohttp.ClientSession()
            sess = self._session
        async with sess.delete(url, data=data or {}, headers=headers, timeout=15) as r:
            r.raise_for_status()
            # DELETE bazen body dönmez; uyum için boş dict döndür
            try:
                return await r.json()
            except Exception:
                return {}
