# /opt/tradebot/future_trade/order_router.py
from __future__ import annotations
import time
import hashlib
import asyncio
import inspect
from enum import Enum
from typing import Optional, Dict, Any

from .exchange_utils import ExchangeNormalizer


class Mode(Enum):
    DRY = "dry"
    PAPER = "paper"
    LIVE = "live"


class OrderRouter:
    """
    Tek kapı emir yönlendirici.
    - config["app"]["mode"] -> dry|paper|live
    - Binance precision & minNotional normalize
    - clientOrderId ile idempotent
    - async/sync client uyumlu (new_order/cancel_order either sync or async)
    """
    def __init__(
        self,
        config: Dict[str, Any],
        binance_client,
        normalizer: ExchangeNormalizer,
        logger,
        paper_engine=None
    ):
        self.cfg = config
        self.binance = binance_client
        self.logger = logger
        self.paper = paper_engine
        mode_str = (config.get("app", {}).get("mode") or config.get("mode", "paper")).lower()
        if mode_str not in ("dry", "paper", "live"):
            raise ValueError(f"Geçersiz mode: {mode_str}")
        self.mode = Mode(mode_str)
        self.norm = normalizer
        self._seen_ids = set()  # idempotency cache (process içi)

    # ---------- async/sync yardımcı ----------
    @staticmethod
    def _maybe_await(res):
        if inspect.isawaitable(res):
            try:
                loop = asyncio.get_event_loop()
            except RuntimeError:
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
            if loop.is_running():
                # Router sync çalışırken içerde already-running event loop varsa,
                # run_until_complete kullanamayız; bu durumda nest_asyncio kullanmak zorunda kalmamak için
                # basit bir "create_task ve kısa bekleme" paterni uyguluyoruz.
                # Ancak çoğu prod senaryoda loop run etmeyecektir.
                # fut = asyncio.ensure_future(res, loop=loop)   # ileride kullanılabilir
                # Not: blocking bekleyemeyiz; burada sadeleştiriyoruz:
                raise RuntimeError("Binance client async çalışıyor ve event loop zaten aktif. "
                                   "Router'ı async bağlamda çağır ya da binance client için sync wrapper kullan.")
            return loop.run_until_complete(res)
        return res

    # ---------- yardımcılar ----------
    def _mk_client_order_id(self, symbol: str, tag: str = "") -> str:
        seed = f"TB|{symbol}|{tag}|{int(time.time() * 1000)}"
        return hashlib.md5(seed.encode()).hexdigest()[:32]

    def _idempotent_guard(self, client_order_id: str):
        if client_order_id in self._seen_ids:
            raise RuntimeError(f"Tekrarlanan clientOrderId: {client_order_id}")
        self._seen_ids.add(client_order_id)

    # ---------- dış arayüz ----------
    def place_order(
        self,
        symbol: str,
        side: str,                    # "BUY" | "SELL"
        qty: float,
        price: Optional[float] = None,
        order_type: Optional[str] = None,        # "MARKET" | "LIMIT" | "STOP" | ...
        time_in_force: Optional[str] = None,     # "GTC" | "IOC" | "FOK"
        reduce_only: bool = False,
        tag: str = ""
    ) -> Dict[str, Any]:
        """
        Emir açma tek kapısı.
        - DRY: sadece log
        - PAPER: paper_engine.sim_place(...)
        - LIVE: binance.new_order(...), sync/async uyumlu
        """
        order_type = order_type or ("LIMIT" if price is not None else "MARKET")
        time_in_force = time_in_force or self.cfg.get("order", {}).get("time_in_force", "GTC")
        client_order_id = self._mk_client_order_id(symbol, tag)
        self._idempotent_guard(client_order_id)

        # Normalize: qty/price precision, minNotional
        norm = self.norm.normalize(symbol, qty=qty, price=price)
        qty_n, price_n = norm["qty"], norm["price"]

        payload = {
            "symbol": symbol,
            "side": side,
            "type": order_type,
            "quantity": qty_n,
            "reduceOnly": reduce_only,
            "newClientOrderId": client_order_id
        }
        if price_n is not None and order_type in ("LIMIT", "STOP", "STOP_MARKET", "TAKE_PROFIT", "TAKE_PROFIT_MARKET"):
            payload["price"] = price_n
            payload["timeInForce"] = time_in_force

        if self.mode == Mode.DRY:
            self.logger.info(f"[DRY] place_order {payload}")
            return {"status": "DRY_LOGGED", "clientOrderId": client_order_id, "payload": payload}

        if self.mode == Mode.PAPER:
            if not self.paper:
                raise RuntimeError("PAPER mode aktif ama paper_engine atanmamış")
            res = self.paper.sim_place(**payload)
            self.logger.info(f"[PAPER] {res}")
            return res

        # LIVE (sync/async fark etmeyecek)
        call = getattr(self.binance, "new_order", None)
        if call is None:
            raise AttributeError("binance_client.new_order bulunamadı")
        res = self._maybe_await(call(**payload))
        self.logger.info(f"[LIVE] {res}")
        return res

    def cancel(self, symbol: str, client_order_id: Optional[str] = None, order_id: Optional[int] = None) -> Dict[str, Any]:
        if self.mode == Mode.DRY:
            self.logger.info(f"[DRY] cancel symbol={symbol} coid={client_order_id} oid={order_id}")
            return {"status": "DRY_LOGGED"}

        if self.mode == Mode.PAPER:
            if not self.paper:
                raise RuntimeError("PAPER mode aktif ama paper_engine atanmamış")
            res = self.paper.sim_cancel(symbol=symbol, clientOrderId=client_order_id, orderId=order_id)
            self.logger.info(f"[PAPER] {res}")
            return res

        # LIVE (sync/async uyumlu)
        call = getattr(self.binance, "cancel_order", None)
        if call is None:
            raise AttributeError("binance_client.cancel_order bulunamadı")
        # Binance Futures: cancel param isimleri (origClientOrderId veya orderId)
        res = self._maybe_await(call(symbol=symbol, origClientOrderId=client_order_id, orderId=order_id))
        self.logger.info(f"[LIVE] {res}")
        return res

    def close_position_market(self, symbol: str, side: str, qty: float, tag: str = "close"):
        """
        Pozisyon kapama için MARKET reduceOnly emir.
        """
        client_order_id = self._mk_client_order_id(symbol, tag)
        self._idempotent_guard(client_order_id)

        # normalize qty
        qty_n = self.norm.normalize(symbol, qty=qty, price=None)["qty"]

        if self.mode == Mode.DRY:
            self.logger.info(f"[DRY] close_position_market {symbol} {side} {qty_n}")
            return {"status": "DRY_LOGGED"}

        if self.mode == Mode.PAPER:
            if not self.paper:
                raise RuntimeError("PAPER mode aktif ama paper_engine atanmamış")
            res = self.paper.sim_place(
                symbol=symbol,
                side=side,
                type="MARKET",
                quantity=qty_n,
                reduceOnly=True,
                newClientOrderId=client_order_id
            )
            self.logger.info(f"[PAPER] {res}")
            return res

        # LIVE (sync/async uyumlu)
        call = getattr(self.binance, "new_order", None)
        if call is None:
            raise AttributeError("binance_client.new_order bulunamadı")
        res = self._maybe_await(call(
            symbol=symbol,
            side=side,
            type="MARKET",
            quantity=qty_n,
            reduceOnly=True,
            newClientOrderId=client_order_id
        ))
        self.logger.info(f"[LIVE] {res}")
        return res
