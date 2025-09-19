"""Emir ve pozisyon durum uzlaştırma
- Periyodik olarak borsa ile DB durumunu hizalar
- Kaybolan/çakışan emirleri temizler
"""
from __future__ import annotations
from typing import Any, Dict, Optional
import asyncio, logging, time
from .cooldown import compute_dynamic_cooldown_sec, TF_SECONDS
import time

class OrderReconciler:
    def __init__(self, client, persistence, notifier, cfg: Optional[Dict[str, Any]] = None):
        self.client = client
        self.persistence = persistence
        self.notifier = notifier
        self.cfg = cfg or {}
        
    async def run(self):
        poll_sec = int(self.cfg.get("reconciler", {}).get("poll_interval_sec", 5))
        entry_tf = str(self.cfg.get("strategy", {}).get("timeframe_entry", "1h")).lower()

        while True:
            try:
                # 1) EXCHANGE pozisyonları çek
                ex = await self.client.position_risk()  # /fapi/v2/positionRisk
                # ex_pos: {'SOLUSDT': -15.0, 'BTCUSDT': 0.0, ...}
                ex_pos = {}
                for p in ex:
                    sym = p.get("symbol"); amt = float(p.get("positionAmt", 0) or 0)
                    ex_pos[sym] = amt

                # 2) DB pozisyonlarını çek
                conn = self.persistence._conn()
                cur = conn.cursor()
                cur.execute("SELECT symbol, qty FROM futures_positions")
                db_rows = cur.fetchall()
                conn.close()
                db_pos = {r[0]: float(r[1]) for r in db_rows}

                # 3) Birlikte ele al
                symbols = set(ex_pos.keys()) | set(db_pos.keys())
                for sym in symbols:
                    db_qty = float(db_pos.get(sym, 0.0))
                    ex_qty = float(ex_pos.get(sym, 0.0))

                    # --- (A) POZİSYON KAPANDI: DB'de açık görünüyor ama borsada 0
                    if abs(db_qty) > 0 and abs(ex_qty) == 0:
                        # DB kapat (senin mevcut kapatma kodun burada olmalı)
                        conn = self.persistence._conn()
                        try:
                            c2 = conn.cursor()
                            # ör: qty=0'a çekiyoruz (veya satırı siliyorsan delete)
                            c2.execute("UPDATE futures_positions SET qty=0, unrealized_pnl=0 WHERE symbol=?", (sym,))
                            conn.commit()
                        finally:
                            conn.close()

                        # ==> TAM OLARAK BURADA Dinamik Cooldown Hesapla & Yaz
                        try:
                            sec = await compute_dynamic_cooldown_sec(self.cfg, self.client, self.persistence, sym, entry_tf)
                        except Exception:
                            # fallback: static bar-bazlı cooldown (opsiyonel)
                            bars = int(self.cfg.get("cooldown_bars_after_exit", 0))
                            tf_sec = TF_SECONDS.get(entry_tf, 3600)
                            sec = max(0, bars * tf_sec)

                        exit_ts = int(time.time())
                        until_ts = exit_ts + int(sec)

                        # DB state güncelle
                        self.persistence.mark_exit_ts(sym, exit_ts)
                        self.persistence.set_cooldown(sym, until_ts)

                        # Bilgilendirme (Telegram debug)
                        try:
                            await self.notifier.debug_trades({
                                "event": "cooldown_set",
                                "symbol": sym,
                                "seconds": int(sec),
                                "until_ts": until_ts
                            })
                        except Exception:
                            pass

                    # --- (B) POZİSYON AÇILDI: DB 0 ama borsada != 0 (opsiyonel senkron)
                    elif abs(db_qty) == 0 and abs(ex_qty) > 0:
                        # DB'ye yaz (örnek)
                        side = "LONG" if ex_qty > 0 else "SHORT"
                        conn = self.persistence._conn()
                        try:
                            c2 = conn.cursor()
                            c2.execute("""
                                INSERT OR REPLACE INTO futures_positions(symbol, side, qty, entry_price, leverage, unrealized_pnl, updated_at)
                                VALUES(?, ?, ?, COALESCE(entry_price, 0), COALESCE(leverage, 1), 0, ?)
                            """, (sym, side, ex_qty, int(time.time())))
                            conn.commit()
                        finally:
                            conn.close()

                    # --- (C) İKİ TARAF TA 0 (flat) → yapılacak yok
                    else:
                        pass

            except Exception as e:
                logging.warning(f"reconciler loop error: {e}")

            await asyncio.sleep(poll_sec)


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
