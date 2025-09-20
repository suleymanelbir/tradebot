# /opt/tradebot/future_trade/order_reconciler.py
"""Emir ve pozisyon durum uzlaştırma
- Periyodik olarak borsa ile DB durumunu hizalar
- Kaybolan/çakışan emirleri temizler, cooldown set eder
"""
from __future__ import annotations
from typing import Any, Dict, Optional, List, Tuple
import asyncio, logging, time
import sqlite3

# Dinamik cooldown için güvenli import + fallback
try:
    from .cooldown import compute_dynamic_cooldown_sec, TF_SECONDS
except Exception:
    TF_SECONDS = {"1m":60, "5m":300, "15m":900, "1h":3600, "4h":14400, "1d":86400}
    async def compute_dynamic_cooldown_sec(cfg, client, persistence, symbol, entry_tf: str) -> int:
        bars = int(cfg.get("cooldown_bars_after_exit", 0))
        tf_sec = TF_SECONDS.get(str(entry_tf).lower(), 3600)
        return max(0, bars * tf_sec)

class OrderReconciler:
    def __init__(self, client, persistence, notifier, cfg: Optional[Dict[str, Any]] = None):
        self.client = client
        self.persistence = persistence
        self.notifier = notifier
        self.cfg = cfg or {}

    # ---------- low-level helpers (DB erişimi) ----------
    def _conn(self):
        return self.persistence._conn()

    def _db_get_position(self, symbol: str) -> Optional[Tuple[str, float, float, int]]:
        """futures_positions tablosundan (symbol, qty, entry_price, leverage) döner."""
        with self._conn() as conn:
            cur = conn.cursor()
            cur.execute("SELECT symbol, qty, entry_price, leverage FROM futures_positions WHERE symbol=?", (symbol,))
            row = cur.fetchone()
        return row

    def _db_upsert_position(self, symbol: str, side: str, qty: float, entry: float, lev: int):
        now = int(time.time())
        with self._conn() as conn:
            cur = conn.cursor()
            cur.execute("""
                INSERT INTO futures_positions(symbol, side, qty, entry_price, leverage, unrealized_pnl, updated_at)
                VALUES(?,?,?,?,?,0,?)
                ON CONFLICT(symbol) DO UPDATE SET
                  side=excluded.side, qty=excluded.qty, entry_price=excluded.entry_price,
                  leverage=excluded.leverage, updated_at=excluded.updated_at
            """, (symbol, side, qty, entry, lev, now))
            conn.commit()

    def _db_clear_position(self, symbol: str):
        now = int(time.time())
        with self._conn() as conn:
            cur = conn.cursor()
            cur.execute("""
                UPDATE futures_positions
                SET qty=0, unrealized_pnl=0, updated_at=?
                WHERE symbol=?
            """, (now, symbol))
            conn.commit()

    def _set_cooldown(self, symbol: str, until_ts: int):
        now = int(time.time())
        with self._conn() as conn:
            cur = conn.cursor()
            cur.execute("""
                INSERT INTO symbol_state(symbol, state, cooldown_until, updated_at)
                VALUES(?, 'cooldown', ?, ?)
                ON CONFLICT(symbol) DO UPDATE SET
                  state='cooldown', cooldown_until=excluded.cooldown_until, updated_at=excluded.updated_at
            """, (symbol, until_ts, now))
            conn.commit()

    def _mark_exit_ts(self, symbol: str, ts: Optional[int] = None):
        ts = ts or int(time.time())
        with self._conn() as conn:
            cur = conn.cursor()
            cur.execute("""
                INSERT INTO symbol_state(symbol, last_signal_ts, updated_at)
                VALUES(?, ?, ?)
                ON CONFLICT(symbol) DO UPDATE SET
                  last_signal_ts=excluded.last_signal_ts, updated_at=excluded.updated_at
            """, (symbol, ts, ts))
            conn.commit()

    # ---------- borsa okuma ----------
    async def _fetch_position_risk(self) -> List[Dict[str, Any]]:
        """client.position_risk() / client.positions() uyumlu çağrı sargısı."""
        if hasattr(self.client, "position_risk"):
            return await self.client.position_risk()
        if hasattr(self.client, "positions"):
            return await self.client.positions()
        return []

    # ---------- tek geçiş ----------
    async def _one_pass(self, entry_tf: str) -> None:
        # 1) Borsadan pozisyonlar
        ex = await self._fetch_position_risk()
        # ex_pos: {'SOLUSDT': {'qty': -15.0, 'entry': 232.66, 'lev': 3}, ...}
        ex_pos: Dict[str, Dict[str, float]] = {}
        for p in ex or []:
            sym = p.get("symbol")
            if not sym:
                continue
            amt = float(p.get("positionAmt") or 0.0)
            entry = float(p.get("entryPrice") or 0.0)
            lev = int(float(p.get("leverage") or 0) or 1)
            ex_pos[sym] = {"qty": amt, "entry": entry, "lev": lev}

        # 2) DB pozisyonları
        with self._conn() as conn:
            cur = conn.cursor()
            cur.execute("SELECT symbol, qty FROM futures_positions")
            rows = cur.fetchall()
        db_pos = {r[0]: float(r[1]) for r in rows}

        symbols = set(ex_pos.keys()) | set(db_pos.keys())
        now = int(time.time())

        for sym in symbols:
            db_qty = float(db_pos.get(sym, 0.0))
            ex_qty = float(ex_pos.get(sym, {}).get("qty", 0.0))

            # --- A) Borsa 0, DB ≠ 0 ⇒ Pozisyon kapanmış kabul et
            if abs(ex_qty) < 1e-12 and abs(db_qty) > 1e-12:
                try:
                    self._db_clear_position(sym)
                    self._mark_exit_ts(sym, now)
                    # Dinamik cooldown hesapla
                    cool = await compute_dynamic_cooldown_sec(self.cfg, self.client, self.persistence, sym, entry_tf)
                    self._set_cooldown(sym, now + int(cool))
                    await self.notifier.debug_trades({
                        "event": "reconcile_close_sync",
                        "symbol": sym, "db_qty": db_qty, "ex_qty": ex_qty,
                        "cooldown_sec": int(cool)
                    })
                except Exception as e:
                    await self.notifier.alert({"event": "reconcile_error", "symbol": sym, "stage": "close_sync", "error": str(e)})

            # --- B) Borsa ≠ 0, DB 0 ⇒ Dışarıda açılmış / restore et
            elif abs(ex_qty) > 1e-12 and abs(db_qty) < 1e-12:
                side = "LONG" if ex_qty > 0 else "SHORT"
                try:
                    self._db_upsert_position(sym, side, ex_qty, ex_pos[sym]["entry"], int(ex_pos[sym]["lev"]))
                    await self.notifier.debug_trades({
                        "event": "reconcile_restore",
                        "symbol": sym, "qty": ex_qty, "entry": ex_pos[sym]["entry"], "lev": ex_pos[sym]["lev"]
                    })
                except Exception as e:
                    await self.notifier.alert({"event": "reconcile_error", "symbol": sym, "stage": "restore_sync", "error": str(e)})

            # --- C) İkisi de ≠ 0 ama farklı ⇒ DB’yi borsaya hizala
            elif abs(ex_qty - db_qty) > 1e-12:
                side = "LONG" if ex_qty > 0 else "SHORT"
                try:
                    self._db_upsert_position(sym, side, ex_qty, ex_pos[sym]["entry"], int(ex_pos[sym]["lev"]))
                    await self.notifier.debug_trades({
                        "event": "reconcile_adjust",
                        "symbol": sym, "from": db_qty, "to": ex_qty
                    })
                except Exception as e:
                    await self.notifier.alert({"event": "reconcile_error", "symbol": sym, "stage": "adjust_sync", "error": str(e)})

            # else: tamamen uyumlu → no-op

    # ---------- dış API ----------
    async def run(self):
        entry_tf = str(self.cfg.get("timeframe_entry") or self.cfg.get("strategy", {}).get("timeframe_entry") or "1h")
        poll_sec = int(self.cfg.get("reconcile_interval_sec", 5))
        backoff = 1
        while True:
            try:
                await self._one_pass(entry_tf)
                backoff = 1
            except Exception as e:
                logging.warning(f"reconciler loop error: {e}")
                await self.notifier.alert({"event": "reconcile_loop_error", "error": str(e)})
                # basit backoff
                backoff = min(backoff * 2, 60)
            await asyncio.sleep(max(poll_sec, backoff))
