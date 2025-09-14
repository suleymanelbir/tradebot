"""Emir ve pozisyon durum uzlaştırma
- Periyodik olarak borsa ile DB durumunu hizalar
- Kaybolan/çakışan emirleri temizler
"""
import asyncio
import asyncio, logging, time

class OrderReconciler:
    def __init__(self, client, persistence, notifier):
        self.client = client
        self.persistence = persistence
        self.notifier = notifier

    async def run(self):
        while True:
            try:
                await self._tick()
            except Exception as e:
                logging.warning(f"reconciler warn: {e}")
            await asyncio.sleep(5)

    async def _tick(self):
        pr = await self.client.position_risk()  # v2
        conn = self.persistence._conn()
        try:
            cur = conn.cursor()
            seen = set()
            for p in pr:
                sym = p["symbol"]
                seen.add(sym)
                pos_amt = float(p.get("positionAmt") or 0.0)
                entry = float(p.get("entryPrice") or 0.0)
                if abs(pos_amt) < 1e-12:
                    # borsada pozisyon yok → DB’den sil
                    cur.execute("DELETE FROM futures_positions WHERE symbol=?", (sym,))
                    logging.debug(f"reconciler: cleared {sym} (no position on exchange)")
                else:
                    side = "LONG" if pos_amt > 0 else "SHORT"
                    cur.execute("""INSERT OR REPLACE INTO futures_positions
                                   (symbol, side, qty, entry_price, leverage, unrealized_pnl, updated_at)
                                   VALUES (?, ?, ?, ?, ?, 0, ?)""",
                                (sym, side, pos_amt, entry, 1, int(time.time())))
                    logging.debug(f"reconciler: upsert {sym} side={side} qty={pos_amt}")
            conn.commit()
        finally:
            conn.close()
