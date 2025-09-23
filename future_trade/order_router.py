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

        # ---------- Trailing bağlamı & güncelleme ----------
    def attach_trailing_context(
        self,
        get_last_price=None,              # callable: symbol -> float | None
        list_open_positions=None,         # callable: () -> List[dict]  (symbol, side, qty, entry_price, ... )
        upsert_stop=None                  # callable: (symbol, side, new_stop) -> Any
    ):
        """
        Trailing'in ihtiyaç duyduğu dış bağımlılıkları tak.
        - get_last_price: MarketStream.get_last_price gibi bir fonksiyon
        - list_open_positions: persistence.list_open_positions gibi bir fonksiyon
        - upsert_stop: mevcut/planlanan SL emrini güncelleyen callback (yoksa DRY log yapılır)
        """
        self._trail_ctx = {
            "get_last_price": get_last_price,
            "list_open_positions": list_open_positions,
            "upsert_stop": upsert_stop
        }
        self.logger.info("[TRAIL] context attached")

    def update_trailing_for_open_positions(self) -> int:
        """
        Açık pozisyonlar için basit bir trailing SL güncelleme yürütür.
        Şimdilik minimal/korumacı: step_pct kullanır (ATR ileride eklenecek).
        Dönüş: güncellenen pozisyon sayısı
        """
        ctx = getattr(self, "_trail_ctx", None)
        if not ctx:
            self.logger.warning("[TRAIL] context not attached; skipping")
            return 0

        get_px = ctx.get("get_last_price")
        list_pos = ctx.get("list_open_positions")
        upsert = ctx.get("upsert_stop")

        if not callable(get_px) or not callable(list_pos):
            self.logger.warning("[TRAIL] missing get_last_price or list_open_positions; skipping")
            return 0

        # config'ten trailing parametreleri
        tr_cfg = (self.cfg.get("trailing") or {})
        step_pct = float(tr_cfg.get("step_pct", 0.1))  # %0.1 default
        # ATR tabanlı gelişmiş sürüm ileride bağlanacak
        # atr_period = int(tr_cfg.get("atr_period", 14))
        # atr_mult = float(tr_cfg.get("atr_mult", 2.5))

        updated = 0
        try:
            positions = list_pos() or []
        except Exception as e:
            self.logger.error(f"[TRAIL] list_open_positions error: {e}")
            return 0

        for pos in positions:
            try:
                sym = pos.get("symbol")
                side_pos = (pos.get("side") or "").upper()   # "LONG" | "SHORT"
                qty = float(pos.get("qty", 0) or 0)
                if not sym or qty <= 0:
                    continue

                last = get_px(sym)
                if last is None or last <= 0:
                    self.logger.debug(f"[TRAIL] no last price for {sym}, skip")
                    continue

                # Basit trailing mantığı:
                # LONG: stop = last * (1 - step_pct/100)
                # SHORT: stop = last * (1 + step_pct/100)  (üstten trailing)
                if side_pos == "LONG":
                    new_stop = last * (1.0 - step_pct / 100.0)
                    stop_side = "SELL"   # SL tetikte satış
                elif side_pos == "SHORT":
                    new_stop = last * (1.0 + step_pct / 100.0)
                    stop_side = "BUY"    # SL tetikte alış
                else:
                    continue

                if self.mode == Mode.DRY:
                    self.logger.info(f"[TRAIL-DRY] {sym} {side_pos} last={last:.6f} -> stop={new_stop:.6f}")
                    updated += 1
                    continue

                if upsert and callable(upsert):
                    # Canlı/paper modda gerçek SL güncellemesi bu callback'te yapılır.
                    upsert(sym, stop_side, float(new_stop))
                else:
                    # PAPER/LIVE ama callback yoksa şimdilik loglayıp geç
                    self.logger.info(f"[TRAIL] (no upsert_stop) {sym} {side_pos} last={last:.6f} -> stop={new_stop:.6f}")

                updated += 1

            except Exception as e:
                self.logger.error(f"[TRAIL] update error for {pos}: {e}")

        if updated:
            self.logger.info(f"[TRAIL] updated {updated} position(s)")
        return updated
