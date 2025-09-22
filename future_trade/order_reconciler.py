# /opt/tradebot/future_trade/order_reconciler.py
"""
Paper reconcile döngüsü:
- Açık pozisyonları periyodik kontrol eder.
- price_provider(symbol) ile son fiyatı alır.
- SL/TP veya trail_stop tetiklenmişse pozisyonu kapatır:
  - futures_trades'e realized pnl yazar
  - futures_positions.qty=0 yapar
  - symbol_state.c
  ooldown_until = now + cfg.cooldown_bars_after_exit * bar_saniyesi (basitçe 1h kabul)
  - Telegram'a "exit" bildirimi gönderir.
"""
from __future__ import annotations
import asyncio, time, sqlite3, logging
from typing import Callable, Optional, Dict, Any

class OrderReconciler:
    def __init__(self, client, persistence, notifier, price_provider: Callable[[str], Optional[float]]):
        self.client = client
        self.persistence = persistence
        self.notifier = notifier
        self.price_provider = price_provider

    def _conn(self): return self.persistence._conn()

    def _now(self) -> int: return int(time.time())

    def _get_symbol_state(self, symbol: str) -> Dict[str, Any]:
        with self._conn() as c:
            c.row_factory = sqlite3.Row
            cur = c.cursor()
            cur.execute("SELECT * FROM symbol_state WHERE symbol=?", (symbol,))
            row = cur.fetchone()
            return dict(row) if row else {}

    def _set_symbol_state(self, symbol: str, **fields):
        ts = self._now()
        cols = []
        vals = []
        for k, v in fields.items():
            cols.append(f"{k}=?")
            vals.append(v)
        vals.append(ts)
        set_clause = ", ".join(cols + ["updated_at=?"])
        with self._conn() as c:
            cur = c.cursor()
            cur.execute(
                f"INSERT INTO symbol_state(symbol, {', '.join(k for k in fields.keys())}, updated_at) "
                f"VALUES(?, {', '.join('?' for _ in fields)}, ?) "
                f"ON CONFLICT(symbol) DO UPDATE SET {set_clause};",
                (symbol, *vals),
            )
            c.commit()

    def _first_virtual_order_price(self, symbol: str, otype: str) -> Optional[float]:
        # STOP_MARKET = SL, LIMIT = TP (paper varsayımı)
        with self._conn() as c:
            c.row_factory = sqlite3.Row
            cur = c.cursor()
            cur.execute(
                "SELECT price FROM futures_orders "
                "WHERE symbol=? AND type=? AND reduce_only=1 "
                "ORDER BY id ASC LIMIT 1",
                (symbol, otype),
            )
            r = cur.fetchone()
            return float(r["price"]) if r and r["price"] is not None else None

    def _close_position(self, symbol: str, side: str, entry_price: float, qty: float, exit_price: float):
        ts = self._now()
        # realize PnL (basit): (exit-entry)*qty; qty işaretine dikkat
        # futures_positions’da qty LONG için +, SHORT için - tutuluyor
        signed = qty
        pnl = (exit_price - entry_price) * signed * (-1 if side == "SHORT" else 1)
        with self._conn() as c:
            cur = c.cursor()
            # trade kaydı
            cur.execute(
                "INSERT INTO futures_trades(order_id, symbol, side, price, qty, fee, realized_pnl, ts) "
                "VALUES(NULL,?,?,?,?,?, ?,?)",
                (symbol, side, float(exit_price), float(abs(qty)), 0.0, float(pnl), ts)
            )
            # pozisyonu sıfırla
            cur.execute(
                "UPDATE futures_positions SET qty=0, updated_at=? WHERE symbol=?",
                (ts, symbol)
            )
            c.commit()

        # cooldown: sabit 3 bar * 3600s = 10800s (paper basit yaklaşım)
        cooldown_sec = 3 * 3600
        self._set_symbol_state(symbol, cooldown_until=self._now() + cooldown_sec)

        return pnl

    async def run(self):
        while True:
            try:
                # açık pozisyonları al
                with self._conn() as c:
                    c.row_factory = sqlite3.Row
                    cur = c.cursor()
                    cur.execute("SELECT * FROM futures_positions WHERE ABS(qty) > 0")
                    pos = cur.fetchall()

                for p in pos:
                    sym = p["symbol"]; side = p["side"]
                    qty = float(p["qty"]); entry = float(p["entry_price"] or 0.0)
                    last = self.price_provider(sym)
                    if last is None:
                        continue

                    # SL/TP/Trailing eşikleri
                    tp = self._first_virtual_order_price(sym, "LIMIT")
                    sl = self._first_virtual_order_price(sym, "STOP_MARKET")
                    st = self._get_symbol_state(sym)
                    trail = st.get("trail_stop", None)

                    # LONG kapanış koşulları
                    if side == "LONG":
                        hit_sl = (sl is not None and last <= float(sl)) or (trail is not None and last <= float(trail))
                        hit_tp = (tp is not None and last >= float(tp))
                        if hit_sl or hit_tp:
                            pnl = self._close_position(sym, side, entry, abs(qty), last)
                            await self.notifier.trade({
                                "event": "exit",
                                "symbol": sym,
                                "reason": "SL" if hit_sl else "TP",
                                "exit_price": last,
                                "pnl": pnl
                            })
                    # SHORT kapanış koşulları
                    else:
                        hit_sl = (sl is not None and last >= float(sl)) or (trail is not None and last >= float(trail))
                        hit_tp = (tp is not None and last <= float(tp))
                        if hit_sl or hit_tp:
                            pnl = self._close_position(sym, side, entry, abs(qty), last)
                            await self.notifier.trade({
                                "event": "exit",
                                "symbol": sym,
                                "reason": "SL" if hit_sl else "TP",
                                "exit_price": last,
                                "pnl": pnl
                            })

            except Exception as e:
                logging.warning(f"reconciler loop error: {e}")

            await asyncio.sleep(5)
