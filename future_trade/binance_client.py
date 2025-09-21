# /opt/tradebot/future_trade/binance_client.py
"""
Binance USDT-M Futures HTTP/WS istemcisi
- testnet/live: ağ açık
- paper: ağ KAPALI (stub/dummy yanıtlar); botu hızlıca ayağa kaldırmak içindir
"""
import time, hmac, hashlib, logging
from typing import Dict, Any, Optional, List
import httpx
from urllib.parse import urlencode

class BinanceClient:
    def __init__(self, cfg: dict, mode: str = "testnet"):
        self.mode  = (mode or "testnet").lower()
        self.paper = (self.mode == "paper")

        # API key/secret
        self.key    = cfg.get("key", "")
        self.secret = (cfg.get("secret", "") or "").encode()

        # recvWindow (ms)
        self.recv = int(cfg.get("recv_window_ms", 5000))

        # Baz URL’ler (kayıt için tutuyoruz; paper’da kullanılmayacak)
        if self.mode == "testnet":
            self.base_url = cfg.get("testnet_base_url", "https://testnet.binancefuture.com")
            self.ws_url   = cfg.get("testnet_ws_url",   "wss://stream.binancefuture.com")
        else:
            self.base_url = cfg.get("base_url", "https://fapi.binance.com")
            self.ws_url   = cfg.get("ws_url",   "wss://fstream.binance.com")

        # HTTP client (yalnızca ağ açıkken)
        self._client: Optional[httpx.AsyncClient] = None
        if not self.paper:
            # httpx.Timeout: ya default ver ya dört alanı birlikte ver → dört alanı veriyoruz
            timeout = httpx.Timeout(connect=10.0, read=15.0, write=15.0, pool=15.0)
            self._client = httpx.AsyncClient(base_url=self.base_url, timeout=timeout)

        masked = f"{self.key[:4]}...{self.key[-4:]}" if self.key else "<empty>"
        logging.info("[BINANCE] mode=%s base_url=%s key=%s recvWindow=%s",
                     self.mode, self.base_url, masked, self.recv)
        if self.paper:
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

    async def get_exchange_info(self):
        if self.paper:
            # Minimal sembol filtresi (gerekirse genişletilir)
            return {"symbols": []}
        return await self._public("GET", "/fapi/v1/exchangeInfo")

    # -------------------- private/signed uçlar (stub destekli) --------------------
    async def get_account(self) -> dict:
        if self.paper:
            return {"availableBalance": "10000"}
        return await self._signed("GET", "/fapi/v2/account", {})

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

    async def bootstrap_exchange(self, cfg: dict):
        """Başlangıç ONE_WAY ve (opsiyon) margin/leverage ayarı."""
        if self.paper:
            logging.info("paper: bootstrap_exchange skipped")
            return
        try:
            await self.set_position_side_oneway()
        except Exception as e:
            logging.warning(f"positionSide setup warning: {e}")
        wl = cfg.get("symbols_whitelist", [])
        margin_mode = cfg.get("margin_mode", "ISOLATED")
        lev_cfg = cfg.get("leverage", {})
        for sym in wl:
            try:
                await self.set_margin_type(sym, "ISOLATED" if margin_mode == "ISOLATED" else "CROSS")
                lev = int(lev_cfg.get(sym, lev_cfg.get("default", 3)))
                await self.set_leverage(sym, lev)
            except Exception as e:
                logging.warning(f"bootstrap warn {sym}: {e}")

    async def close(self):
        if self._client is not None:
            try:
                await self._client.aclose()
            except Exception:
                pass
