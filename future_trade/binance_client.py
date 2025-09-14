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
    def __init__(self, cfg: dict, mode: str):
        self.mode = (mode or "testnet").lower()
        self.key = cfg.get("key", "")
        self.secret = cfg.get("secret", "")
        self.recv = int(cfg.get("recv_window_ms", 5000))

        if self.mode == "testnet":
            base_url = cfg["testnet_base_url"]
        else:
            base_url = cfg["base_url"]

        # Tek bir AsyncClient yeterli
        self._client = httpx.AsyncClient(base_url=base_url, timeout=20.0)

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

    async def set_leverage(self, symbol: str, leverage: int):
        return await self._signed("POST", "/fapi/v1/leverage", {"symbol": symbol, "leverage": leverage})

    async def place_order(self, **params):
        # USDT-M Futures order endp.
        return await self._signed("POST", "/fapi/v1/order", params)

    # ---------- Private (signed) ----------
    async def set_position_side_oneway(self):
        try:
            return await self._signed("POST", "/fapi/v1/positionSide/dual", {"dualSidePosition": "false"})
        except Exception as e:
            resp = getattr(e, "response", None)
            body = (resp.text or "") if resp is not None else ""
            if "-4059" in body:  # No need to change position side
                logging.info("positionSide already ONE_WAY")
                return {"code": -4059, "msg": "No need to change"}
            raise


    async def _signed(self, method: str, path: str, params: dict):
        """Binance USDT-M Futures imzalı istek (HMAC-SHA256 over urlencode(params))."""
        ts = int(time.time() * 1000)
        p = dict(params or {})
        p.update({"timestamp": ts, "recvWindow": self.recv})

        query = urlencode(p, doseq=True)
        signature = hmac.new(self.secret.encode(), query.encode(), hashlib.sha256).hexdigest()
        headers = {"X-MBX-APIKEY": self.key}
        url = f"{path}?{query}&signature={signature}"

        r = await self._client.request(method.upper(), url, headers=headers)
        try:
            r.raise_for_status()
        except Exception:
            logging.error("Binance response body: %s", r.text)
            raise
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


    async def exchange_info(self) -> Dict[str, Any]:
        r = await self._client.get("/fapi/v1/exchangeInfo"); r.raise_for_status(); return r.json()


    async def account_balance(self) -> Dict[str, Any]:
        return await self._signed("GET", "/fapi/v2/balance", {})


    async def positions(self) -> Dict[str, Any]:
        return await self._signed("GET", "/fapi/v2/positionRisk", {})

    async def position_risk(self, symbol: str = None):
        p = {}
        if symbol: p["symbol"] = symbol
        # v2 daha yaygın kullanılıyor
        return await self._signed("GET", "/fapi/v2/positionRisk", p)


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

    
    
    async def all_orders(self, symbol: str, **kwargs) -> Dict[str, Any]:
        return await self._signed("GET", "/fapi/v1/allOrders", {"symbol": symbol, **kwargs})


    async def close(self):
        await self._client.aclose()