# /opt/tradebot/future_trade/paper_engine.py
from __future__ import annotations
import time
from typing import Dict, Any

class PaperEngine:
    def __init__(self, logger, slippage_bps: float = 3.0, fee_bps: float = 5.0):
        self.logger = logger
        self.slippage_bps = slippage_bps
        self.fee_bps = fee_bps

    def _apply_slippage(self, side: str, ref_price: float) -> float:
        bps = self.slippage_bps / 10000.0
        if side.upper() == "BUY":
            return ref_price * (1 + bps)
        return ref_price * (1 - bps)

    def sim_place(self, **kwargs) -> Dict[str, Any]:
        """
        kwargs: symbol, side, type, quantity, price?, newClientOrderId, reduceOnly?
        """
        symbol = kwargs["symbol"]
        side = kwargs["side"].upper()
        qty = float(kwargs["quantity"])
        client_id = kwargs.get("newClientOrderId") or f"paper_{int(time.time()*1000)}"
        order_type = kwargs.get("type", "MARKET").upper()
        price = kwargs.get("price")

        # Fiyat belirleme: LIMIT varsa limit, yoksa referans price parametre olarak gelmeli (senin akışında mid/best ask/bid çekebilirsin)
        ref_price = float(price) if price else float(price or 0)
        if ref_price <= 0:
            # canlı fiyatı dışarıdan geçmen daha doğru olur; şimdilik örnek amaçlı
            ref_price = 100.0

        fill_price = self._apply_slippage(side, ref_price)
        fee = (self.fee_bps / 10000.0) * (fill_price * qty)

        result = {
            "status": "FILLED",
            "symbol": symbol,
            "side": side,
            "type": order_type,
            "executedQty": qty,
            "price": fill_price,
            "cumQuote": fill_price * qty,
            "feeQuote": fee,
            "clientOrderId": client_id,
            "transactTime": int(time.time() * 1000),
        }
        return result

    def sim_cancel(self, **kwargs) -> Dict[str, Any]:
        return {"status": "CANCELED", **kwargs}
