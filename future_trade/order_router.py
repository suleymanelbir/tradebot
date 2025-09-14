"""Emir gönderme ve koruma (SL/TP/trailing) yönetimi
- place_entry_with_protection(plan): entry + SL/TP
- update_trailing_for_open_positions(...): açık pozisyonlar için SL güncelle
"""
# /opt/tradebot/future_trade/order_router.py
from typing import Dict, Any
import time
from .exchange_utils import quantize, price_quantize, symbol_filters
import logging

class OrderRouter:
    def __init__(self, client, cfg: Dict[str, Any], notifier, persistence):
        self.client = client
        self.cfg = cfg
        self.notifier = notifier
        self.persistence = persistence
        self._exi_cache = None
        
    async def _exi(self):
        if self._exi_cache is None:
            try:
                self._exi_cache = await self.client.get_exchange_info()
                logging.info("exchangeInfo cached")
            except Exception as e:
                logging.warning(f"exchangeInfo fetch failed: {e}")
                self._exi_cache = {"symbols": []}
        return self._exi_cache


    async def place_entry_with_protection(self, plan):
        # 0) trades kanalına erken bildirim
        await self.notifier.trade({"event": "entry", "symbol": plan.symbol, "side": plan.side, "qty": plan.qty})

        # 1) Sembol filtreleri → tick/step/minNotional
        exi = await self._exi()
        tick, step, min_notional = symbol_filters(exi, plan.symbol)

        # 2) Adet ve fiyat kuantizasyonu
        qty = max(quantize(float(plan.qty), step), step)

        def qprice(p):
            return price_quantize(float(p), tick) if p else None

        sl_price = qprice(getattr(plan, "sl", None))
        tp_price = qprice(getattr(plan, "tp", None))

        # --- NOT: 401 çözülene kadar gerçek emir göndermiyoruz ---
        # Burada normalde:
        #   - MARKET entry (BUY/SELL)
        #   - STOP_MARKET SL (reduceOnly, closePosition=True)
        #   - TAKE_PROFIT(MARKET/LIMIT) TP
        # göndereceğiz. 401 bitince bu bloğu ekleyeceğiz.

        # 3) PAPER: pozisyonu DB'ye yaz (tekrar girişleri engellemek için)
        conn = self.persistence._conn()
        try:
            cur = conn.cursor()
            qty_signed = qty if plan.side == "LONG" else -qty
            cur.execute(
                """INSERT OR REPLACE INTO futures_positions
                (symbol, side, qty, entry_price, leverage, unrealized_pnl, updated_at)
                VALUES (?, ?, ?, ?, ?, 0, strftime('%s','now'))""",
                (plan.symbol, plan.side, qty_signed, float(getattr(plan, "entry", 0.0) or 0.0), 1)
            )
            conn.commit()
            logging.info(f"PAPER position recorded: {plan.symbol} {plan.side} qty={qty} sl={sl_price} tp={tp_price}")
        finally:
            conn.close()


    async def update_trailing_for_open_positions(self, stream, trailing_cfg: Dict[str, Any]):
        return  # placeholder; 401 çözülünce gerçek trailing SL'yi ekleyeceğiz
