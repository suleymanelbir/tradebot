"""Binance USDT-M Futures HTTP/WS istemcisi (hafif iskelet)
- margin_mode (ISOLATED|CROSS) ve position_mode (ONE_WAY) set-up
- Emir gönderme (MARKET/LIMIT), koruma emirleri (reduceOnly)
- Basit REST call helper + imza
- NOT: prod için rate-limit & retry/backoff eklenmeli (TODO)
"""
import time, hmac, hashlib, logging
from typing import Dict, Any, Optional
import httpx
from urllib.parse import urlencode

class BinanceClient:
    
    def __init__(self, cfg: dict, mode: str = "testnet"):
        self.mode = (mode or "testnet").lower()

        # API key/secret
        self.key = cfg.get("key", "")
        self.secret = (cfg.get("secret", "") or "").encode()

        # recvWindow (ms) -> self.recv
        self.recv = int(cfg.get("recv_window_ms", 5000))   # <-- eksikti

        # Base URL seçimleri (testnet/mainnet)
        if self.mode == "testnet":
            self.base_url = cfg.get("testnet_base_url", "https://testnet.binancefuture.com")
            self.ws_url   = cfg.get("testnet_ws_url",   "wss://stream.binancefuture.com")
        else:
            self.base_url = cfg.get("base_url", "https://fapi.binance.com")
            self.ws_url   = cfg.get("ws_url",   "wss://fstream.binance.com")

        # HTTP client -> self._client  (ve backward-compat için self.client)
        timeout = httpx.Timeout(connect=10.0, read=15.0, write=15.0, pool=15.0)
        self._client = httpx.AsyncClient(base_url=self.base_url, timeout=timeout)
        self.client  = self._client  # bazı yerlerde client._client erişimi varsa yine de var

        # Başlangıç log'u (hızlı teşhis için)
        masked = f"{self.key[:4]}...{self.key[-4:]}" if self.key else "<empty>"
        logging.info("[BINANCE] mode=%s base_url=%s key=%s recvWindow=%s",
                    self.mode, self.base_url, masked, self.recv)


    # Public price
    async def get_price(self, symbol: str) -> float:
        r = await self._client.get("/fapi/v1/ticker/price", params={"symbol": symbol})
        r.raise_for_status()
        data = r.json()
        return float(data["price"])

    # Account (futures)
    async def get_account(self) -> dict:
        return await self._signed("GET", "/fapi/v2/account", {})

    async def get_available_usdt(self) -> float:
        acc = await self.get_account()
        # Bazı SDK'larda top-level "availableBalance" var; yoksa assets listesine bak
        if "availableBalance" in acc:
            try:
                return float(acc["availableBalance"])
            except Exception:
                pass
        for a in acc.get("assets", []):
            if a.get("asset") == "USDT":
                return float(a.get("availableBalance", 0))
        return 0.0



    async def get_position_side(self):
        # true = HEDGE (dual), false = ONE_WAY
        return await self._signed("GET", "/fapi/v1/positionSide/dual", {})


    async def get_exchange_info(self):
        r = await self._client.get("/fapi/v1/exchangeInfo")
        r.raise_for_status()
        return r.json()
    
    async def set_margin_type(self, symbol: str, margin_type: str = "ISOLATED"):
        # margin_type: "ISOLATED" | "CROSS"
        
        try:
            return await self._signed("POST", "/fapi/v1/marginType", {"symbol": symbol, "marginType": margin_type})
        except Exception as e:
            # -4046: No need to change margin type.
            resp = getattr(e, "response", None)
            if resp is not None and "-4046" in (resp.text or ""):
                logging.info(f"marginType already {margin_type} for {symbol}")
                return {"code": -4046, "msg": "No need to change"}
            raise

    # ---------- Public ----------
    async def get_klines(self, symbol: str, interval: str = "1h", limit: int = 2):
        r = await self._client.get("/fapi/v1/klines", params={"symbol": symbol, "interval": interval, "limit": limit})
        r.raise_for_status()
        return r.json()

    async def _public(self, method: str, path: str, params: dict | None = None) -> dict:
        r = await self._client.request(method, path, params=params or {})
        if r.status_code >= 400:
            logging.error("Binance PUBLIC response body: %s", r.text)
        r.raise_for_status()
        return r.json()


    async def set_leverage(self, symbol: str, leverage: int):
        return await self._signed("POST", "/fapi/v1/leverage",
                                  {"symbol": symbol, "leverage": int(leverage)})
        
    async def new_order(self, **kwargs):
        # kwargs -> symbol, side, type, quantity/stopPrice/closePosition/... vb.
        return await self._signed("POST", "/fapi/v1/order", kwargs)    
    
    async def order_test(self, **kwargs):
        return await self._signed("POST", "/fapi/v1/order/test", kwargs)
    
    
    async def place_order(self, **params):
        # USDT-M Futures order endp.
        return await self._signed("POST", "/fapi/v1/order", params)

    # ---------- Private (signed) ----------
    async def set_position_side_oneway(self):
        # Pre-check: zaten ONE_WAY ise POST atmayalım
        try:
            cur = await self.get_position_side()
            # Binance: {"dualSidePosition": true/false}
            if not bool(cur.get("dualSidePosition", True)):
                logging.info("positionSide already ONE_WAY")
                return {"code": -4059, "msg": "No need to change"}
        except Exception as e:
            # GET başarısız olursa, yine de POST deneyebiliriz
            logging.debug(f"positionSide GET failed, will POST: {e}")

        # Gerekliyse ONE_WAY'e çek
        try:
            return await self._signed("POST", "/fapi/v1/positionSide/dual", {"dualSidePosition": "false"})
        except Exception as e:
            resp = getattr(e, "response", None)
            body = (resp.text or "") if resp is not None else ""
            if "-4059" in body:
                logging.info("positionSide already ONE_WAY")
                return {"code": -4059, "msg": "No need to change"}
            raise


    async def _signed(self, method: str, path: str, params: dict | None) -> dict:
        ts = int(time.time() * 1000)
        params = dict(params or {})
        params.update({"timestamp": ts, "recvWindow": self.recv})

        # Query string ve signature
        qs  = urlencode(params, doseq=True)
        sig = hmac.new(self.secret, qs.encode(), hashlib.sha256).hexdigest()

        url = f"{path}?{qs}&signature={sig}"
        headers = {"X-MBX-APIKEY": self.key}

        r = await self._client.request(method, url, headers=headers)
        if r.status_code >= 400:
            logging.error("Binance response body: %s", r.text)
        r.raise_for_status()
        return r.json()




    async def bootstrap_exchange(self, cfg: dict):
        """Başlangıçta ONE_WAY ve (isteğe bağlı) whitelist semboller için margin/leverage ayarı."""
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

    async def _public(self, method: str, path: str, params: dict | None = None) -> dict:
        r = await self._client.request(method, path, params=params or {})
        if r.status_code >= 400:
            logging.error("Binance PUBLIC response body: %s", r.text)
        r.raise_for_status()
        return r.json()

    async def exchange_info(self) -> Dict[str, Any]:
        r = await self._client.get("/fapi/v1/exchangeInfo"); r.raise_for_status(); return r.json()

    async def klines(self, symbol: str, interval: str, limit: int = 500):
        return await self._public("GET", "/fapi/v1/klines",
                                  {"symbol": symbol, "interval": interval, "limit": limit})


    async def account_balance(self) -> Dict[str, Any]:
        return await self._signed("GET", "/fapi/v2/balance", {})


    async def positions(self) -> Dict[str, Any]:
        return await self._signed("GET", "/fapi/v2/positionRisk", {})

    async def position_risk(self, symbol: str = None):
        p = {}
        if symbol: p["symbol"] = symbol
        # v2 daha yaygın kullanılıyor
        return await self._signed("GET", "/fapi/v2/positionRisk", p)

    async def set_position_side(self, dual_side: bool):
        # dual_side True -> HEDGE; False -> ONE_WAY
        return await self._signed("POST", "/fapi/v1/positionSide/dual",
                                  {"dualSidePosition": str(dual_side).lower()})


    async def open_orders(self, symbol: str):
        return await self._signed("GET", "/fapi/v1/openOrders", {"symbol": symbol})

    async def place_order(self, **params) -> Dict[str, Any]:
        # USDT-M Futures: POST /fapi/v1/order
        return await self._signed("POST", "/fapi/v1/order", params)


    async def cancel_order(self, symbol: str, orderId: int = None, origClientOrderId: str = None):
        p = {"symbol": symbol}
        if orderId: p["orderId"] = orderId
        if origClientOrderId: p["origClientOrderId"] = origClientOrderId
        return await self._signed("DELETE", "/fapi/v1/order", p)

    async def get_order(self, symbol: str, **kwargs) -> dict:
        # kwargs: orderId=... veya origClientOrderId=...
        p = {"symbol": symbol}
        p.update(kwargs)
        return await self._signed("GET", "/fapi/v1/order", p)

    
    async def all_orders(self, symbol: str, **kwargs) -> Dict[str, Any]:
        return await self._signed("GET", "/fapi/v1/allOrders", {"symbol": symbol, **kwargs})


    async def close(self):
        try:
            await self._client.aclose()
        except Exception:
            pass