# /opt/tradebot/future_trade/persistence.py
# -*- coding: utf-8 -*-
"""
SQLite kalıcılık (futures_data.db)
- Şema oluşturma + hafif migration
- Yardımcı operasyonlar: pozisyon/sipariş/trade/state/sinyal
"""
from __future__ import annotations

import json
import logging
import sqlite3
import time

from typing import Any, Dict, List, Optional, TYPE_CHECKING

if TYPE_CHECKING:
    # Sadece type checker için import; çalışma zamanında import edilmez
    from .order_router import OrderRouter

# ---------- Yardımcı ----------
def now_ts() -> int:
    return int(time.time())


PRAGMAS = [
    "PRAGMA journal_mode=WAL;",
    "PRAGMA synchronous=NORMAL;",
    "PRAGMA foreign_keys=ON;",
]


# ---------- Kanonik Şema ----------
SCHEMA = [
    # Klines
    """
    CREATE TABLE IF NOT EXISTS futures_klines (
        symbol TEXT, tf TEXT,
        open_time INTEGER, open REAL, high REAL, low REAL, close REAL, volume REAL,
        close_time INTEGER,
        PRIMARY KEY(symbol, tf, close_time)
    );
    """,
    # Orders
    """
    CREATE TABLE IF NOT EXISTS futures_orders (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        client_id TEXT, symbol TEXT, side TEXT, type TEXT, status TEXT,
        price REAL, qty REAL, reduce_only INTEGER,
        created_at INTEGER, updated_at INTEGER,
        extra_json TEXT
    );
    """,
    # Positions (ONE-WAY per symbol)
    """
    CREATE TABLE IF NOT EXISTS futures_positions (
        symbol TEXT PRIMARY KEY,
        side TEXT, qty REAL, entry_price REAL, leverage INTEGER,
        unrealized_pnl REAL, updated_at INTEGER
    );
    """,
    # Trades
    """
    CREATE TABLE IF NOT EXISTS futures_trades (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        order_id INTEGER, symbol TEXT, side TEXT,
        price REAL, qty REAL, fee REAL, realized_pnl REAL, ts INTEGER
    );
    """,
    # State (KANONİK) — cooldown_until_ts kullan
    """
    CREATE TABLE IF NOT EXISTS symbol_state (
        symbol TEXT PRIMARY KEY,
        state TEXT,
        cooldown_until_ts INTEGER DEFAULT 0,
        last_signal_ts INTEGER DEFAULT 0,
        last_exit_ts INTEGER DEFAULT 0,
        trail_stop REAL,
        peak REAL,
        trough REAL,
        updated_at INTEGER
    );
    """,
    # Risk günlüğü
    """
    CREATE TABLE IF NOT EXISTS risk_journal (
        ts INTEGER, metric TEXT, value REAL
    );
    """,
    # Sinyal karar günlüğü
    """
    CREATE TABLE IF NOT EXISTS signal_audit (
        ts INTEGER, symbol TEXT, side TEXT, decision INTEGER, reasons TEXT
    );
    """,
    # İndeksler
    """
    CREATE INDEX IF NOT EXISTS idx_orders_symbol_time
        ON futures_orders(symbol, created_at DESC);
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_trades_symbol_ts
        ON futures_trades(symbol, ts);
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_signal_audit_ts_symbol
        ON signal_audit(ts, symbol);
    """,
]


class Persistence:
    def __init__(self, path: str, router: Optional["OrderRouter"], logger: logging.Logger):
        self.path = path
        self.router = router
        self.logger = logger or logging.getLogger("db")
        # RAM cache (opsiyonel kullanım için hazır dursun)
        self._open_positions_cache: List[Dict[str, Any]] = []

    # --------------------------- Connection helper ---------------------------
    def _conn(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.path)
        conn.row_factory = sqlite3.Row
        for p in PRAGMAS:
            try:
                conn.execute(p)
            except Exception:
                pass
        return conn

    # --------------------------- Schema & Migration --------------------------
    def init_schema(self) -> None:
        """
        Veritabanı şemasını başlatır:
        - SCHEMA içeriğini uygular
        - cooldown_until (eski) → cooldown_until_ts (yeni) değerini taşır (varsa)
        - positions_cache tablosunu oluşturur
        - updated_at kolonlarını makul değerlere çeker
        - positions_cache tablosuna sl_order_id / tp_order_id kolonlarını ekler (varsa)
        """
        with self._conn() as c:
            cur = c.cursor()

            # 1) Ana şemalar
            for stmt in SCHEMA:
                cur.executescript(stmt)

            # 2) Migration: eski `cooldown_until` değerlerini `cooldown_until_ts`'e kopyala (kolon varsa)
            try:
                cur.execute("PRAGMA table_info(symbol_state)")
                cols = {row[1] for row in cur.fetchall()}
                if "cooldown_until" in cols and "cooldown_until_ts" in cols:
                    cur.execute("""
                        UPDATE symbol_state
                        SET cooldown_until_ts =
                            CASE
                                WHEN COALESCE(cooldown_until_ts,0)=0
                                THEN COALESCE(cooldown_until,0)
                                ELSE cooldown_until_ts
                            END
                    """)
            except Exception as e:
                self.logger.debug(f"cooldown migration skipped: {e}")

            # 3) updated_at kolonlarını normalize et
            try:
                cur.execute("""
                    UPDATE symbol_state
                    SET updated_at = COALESCE(updated_at, strftime('%s','now'))
                    WHERE updated_at IS NULL
                """)
            except Exception:
                pass

            # 4) Yeni tablo: positions_cache (KANONİK)
            try:
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS positions_cache (
                        symbol TEXT PRIMARY KEY,
                        side TEXT,
                        qty REAL,
                        entry_price REAL,
                        sl REAL,
                        tp REAL,
                        updated_at INTEGER
                    )
                """)
            except Exception as e:
                self.logger.warning(f"positions_cache creation failed: {e}")

            # 4.1) positions_cache: sl_order_id / tp_order_id kolonlarını ekle (yoksa)
            try:
                cur.execute("PRAGMA table_info(positions_cache)")
                cols = {row[1] for row in cur.fetchall()}
                if "sl_order_id" not in cols:
                    cur.execute("ALTER TABLE positions_cache ADD COLUMN sl_order_id TEXT")
                if "tp_order_id" not in cols:
                    cur.execute("ALTER TABLE positions_cache ADD COLUMN tp_order_id TEXT")
            except Exception as e:
                self.logger.debug(f"positions_cache add columns skipped: {e}")

            # 5) notifications_log: Telegram vb. bildirimlerin kalıcı kaydı
            cur.execute("""
            CREATE TABLE IF NOT EXISTS notifications_log (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ts INTEGER NOT NULL,
                channel TEXT,         -- 'alerts_bot' | 'trades_bot' | 'system' ...
                topic TEXT,           -- 'pnl_daily', 'position_risk', 'kill_switch', ...
                level TEXT,           -- 'INFO', 'WARN', 'ERROR'
                payload TEXT          -- JSON metni (gönderilen mesaj/saha verileri)
            );
            """)
            cur.execute("CREATE INDEX IF NOT EXISTS idx_notif_ts ON notifications_log(ts)")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_notif_topic ON notifications_log(topic)")

            # 6) Commit işlemi
            c.commit()

    # --------------------------- Helpers -------------------------------------
    @staticmethod
    def _utc() -> int:
        return int(time.time())

    # --------------------------- Signal Audit ---------------------------------
    def record_signal_audit(self, event: Dict[str, Any], signal, decision: bool, reason: Optional[str] = None) -> None:
        with self._conn() as c:
            c.execute(
                "INSERT INTO signal_audit VALUES (?,?,?,?,?)",
                (
                    int(time.time()),
                    event.get("symbol"),
                    getattr(signal, "side", None),
                    int(bool(decision)),
                    json.dumps({"reason": reason}, ensure_ascii=False),
                ),
            )
            c.commit()

    # --------------------------- Symbol State (cooldown_ts std) ---------------
    def set_cooldown(self, symbol: str, until_ts: int) -> None:
        """
        Artık yalnızca cooldown_until_ts kolonunu kullanıyoruz (KANONİK).
        """
        with self._conn() as c:
            c.execute(
                """
                INSERT INTO symbol_state(symbol, cooldown_until_ts, updated_at)
                VALUES(?, ?, ?)
                ON CONFLICT(symbol) DO UPDATE SET
                    cooldown_until_ts=excluded.cooldown_until_ts,
                    updated_at=excluded.updated_at
                """,
                (symbol, int(until_ts), now_ts()),
            )
            c.commit()

    def get_cooldown_ts(self, symbol: str) -> int:
        """
        Kalan süre hesaplarında kullanılacak epoch (yoksa 0).
        Eski kolon (cooldown_until) varsa GERİYE UYUMLU olarak onu da dener.
        """
        with self._conn() as c:
            cur = c.execute("SELECT cooldown_until_ts FROM symbol_state WHERE symbol=?", (symbol,))
            row = cur.fetchone()
            if row and row[0]:
                return int(row[0])
            # backward-compat: eski kolon
            try:
                cur = c.execute("SELECT cooldown_until FROM symbol_state WHERE symbol=?", (symbol,))
                row = cur.fetchone()
                if row and row[0]:
                    return int(row[0])
            except sqlite3.OperationalError:
                pass
            return 0

    def get_cooldown_remaining(self, symbol: str, now_epoch: Optional[int] = None) -> int:
        if now_epoch is None:
            now_epoch = now_ts()
        remain = self.get_cooldown_ts(symbol) - now_epoch
        return int(remain) if remain > 0 else 0

    # Deprecated (geriye uyum)
    def get_cooldown(self, symbol: str) -> int:
        return self.get_cooldown_ts(symbol)

    def set_last_signal_ts(self, symbol: str, ts: int) -> None:
        with self._conn() as c:
            c.execute(
                """
                INSERT INTO symbol_state(symbol, last_signal_ts, updated_at)
                VALUES(?, ?, strftime('%s','now'))
                ON CONFLICT(symbol) DO UPDATE SET
                    last_signal_ts=excluded.last_signal_ts,
                    updated_at=excluded.updated_at
                """,
                (symbol, int(ts)),
            )
            c.commit()

    def set_trail_stop(self, symbol: str, value: Optional[float]) -> None:
        with self._conn() as c:
            c.execute(
                """
                INSERT INTO symbol_state(symbol, trail_stop, updated_at)
                VALUES(?, ?, strftime('%s','now'))
                ON CONFLICT(symbol) DO UPDATE SET
                    trail_stop=excluded.trail_stop,
                    updated_at=excluded.updated_at
                """,
                (symbol, None if value is None else float(value)),
            )
            c.commit()

    def get_trail_stop(self, symbol: str) -> Optional[float]:
        with self._conn() as c:
            cur = c.execute("SELECT trail_stop FROM symbol_state WHERE symbol=?", (symbol,))
            row = cur.fetchone()
            return float(row[0]) if row and row[0] is not None else None

    def get_symbol_state(self, symbol: str) -> Dict[str, Any]:
        with self._conn() as c:
            cur = c.execute("SELECT * FROM symbol_state WHERE symbol=?", (symbol,))
            row = cur.fetchone()
            return dict(row) if row else {}

    # --------------------------- Positions (futures_positions) ----------------
    def upsert_position(self, symbol: str, side: str, qty: float, entry_price: float, leverage: int = 1) -> None:
        with self._conn() as c:
            c.execute(
                """
                INSERT INTO futures_positions(symbol, side, qty, entry_price, leverage, unrealized_pnl, updated_at)
                VALUES(?,?,?,?,?,0,?)
                ON CONFLICT(symbol) DO UPDATE SET
                    side=excluded.side,
                    qty=excluded.qty,
                    entry_price=excluded.entry_price,
                    leverage=excluded.leverage,
                    updated_at=excluded.updated_at
                """,
                (symbol, side, float(qty), float(entry_price), int(leverage), now_ts()),
            )
            c.commit()

    def delete_position(self, symbol: str) -> None:
        with self._conn() as c:
            c.execute("DELETE FROM futures_positions WHERE symbol=?", (symbol,))
            c.commit()

    def open_positions(self) -> List[Dict[str, Any]]:
        with self._conn() as c:
            cur = c.execute("SELECT * FROM futures_positions WHERE ABS(qty) > 0 ORDER BY updated_at DESC")
            return [dict(r) for r in cur.fetchall()]

    def get_open_position(self, symbol: str) -> Optional[Dict[str, Any]]:
        with self._conn() as c:
            cur = c.execute(
                "SELECT side, qty, entry_price FROM futures_positions WHERE symbol=? AND ABS(qty) > 0",
                (symbol,),
            )
            row = cur.fetchone()
            return dict(row) if row else None

    def close_position(self, symbol: str) -> None:
        """
        Pozisyonu router üzerinden kapat, ardından defter ve cache'i güncelle.
        """
        pos = self.get_open_position(symbol)
        if not pos:
            self.logger.info(f"[PERSISTENCE] {symbol} için açık pozisyon yok")
            return

        qty = abs(float(pos["qty"]))
        side_close = "SELL" if str(pos["side"]).upper() == "LONG" else "BUY"

        try:
            if self.router:
                self.router.close_position_market(symbol=symbol, side=side_close, qty=qty, tag="persist_close")
                self.logger.info(f"[PERSISTENCE] Router ile pozisyon kapatıldı: {symbol} {side_close} {qty}")
        except Exception as e:
            self.logger.error(f"[PERSISTENCE] Router close error: {symbol} {e}")

        # DB: futures_positions → qty=0 ; cache → sil
        with self._conn() as c:
            c.execute("UPDATE futures_positions SET qty=0, updated_at=? WHERE symbol=?", (now_ts(), symbol))
            c.commit()
        self.cache_close_position(symbol)

    # --------------------------- Positions Cache (KANONİK) --------------------
    def cache_add_open_position(self, symbol: str, side: str, qty: float, entry_price: float) -> None:
        side = (side or "").upper()
        with self._conn() as c:
            c.execute(
                """
                INSERT INTO positions_cache(symbol, side, qty, entry_price, sl, tp, updated_at)
                VALUES(?,?,?,?,NULL,NULL,?)
                ON CONFLICT(symbol) DO UPDATE SET
                    side=excluded.side,
                    qty=excluded.qty,
                    entry_price=excluded.entry_price,
                    updated_at=excluded.updated_at
                """,
                (symbol, side, float(qty), float(entry_price), self._utc()),
            )
            c.commit()

    def cache_update_position(
        self,
        symbol: str,
        qty: float | None = None,
        entry_price: float | None = None,
        sl: float | None = None,
        tp: float | None = None,
    ) -> None:
        """
        Pozisyonu veritabanında günceller. Sadece verilen alanlar değiştirilir.
        """
        with self._conn() as c:
            cur = c.cursor()
            cur.execute(
                "SELECT symbol, side, qty, entry_price, sl, tp FROM positions_cache WHERE symbol=?",
                (symbol,),
            )
            row = cur.fetchone()
            if not row:
                return
            _, side0, qty0, entry0, sl0, tp0 = row
            new_qty = float(qty) if qty is not None else qty0
            new_entry = float(entry_price) if entry_price is not None else entry0
            new_sl = float(sl) if sl is not None else sl0
            new_tp = float(tp) if tp is not None else tp0
            c.execute(
                """
                UPDATE positions_cache
                   SET qty=?,
                       entry_price=?,
                       sl=?,
                       tp=?,
                       updated_at=?
                 WHERE symbol=?
                """,
                (new_qty, new_entry, new_sl, new_tp, self._utc(), symbol),
            )
            c.commit()

    def cache_update_sl(self, symbol: str, sl: float | None) -> None:
        with self._conn() as c:
            c.execute(
                "UPDATE positions_cache SET sl=?, updated_at=? WHERE symbol=?",
                (float(sl) if sl is not None else None, self._utc(), symbol),
            )
            c.commit()

    def cache_update_tp(self, symbol: str, tp: float | None) -> None:
        with self._conn() as c:
            c.execute(
                "UPDATE positions_cache SET tp=?, updated_at=? WHERE symbol=?",
                (float(tp) if tp is not None else None, self._utc(), symbol),
            )
            c.commit()

    def cache_close_position(self, symbol: str) -> None:
        """
        Pozisyon tamamen kapandığında cache'ten sil.
        """
        with self._conn() as c:
            c.execute("DELETE FROM positions_cache WHERE symbol=?", (symbol,))
            c.commit()
        # RAM cache’te de varsa temizle
        self._open_positions_cache = [p for p in self._open_positions_cache if p.get("symbol") != symbol]

    def list_open_positions(self) -> List[Dict[str, Any]]:
        """
        Açık pozisyonları DB cache’ten döndürür.
        Format: [{symbol, side, qty, entry_price, sl, tp, updated_at, sl_order_id, tp_order_id}, ...]
        """
        out: List[Dict[str, Any]] = []
        with self._conn() as c:
            cur = c.execute(
                """
                SELECT symbol, side, qty, entry_price, sl, tp, updated_at, sl_order_id, tp_order_id
                FROM positions_cache
                WHERE qty IS NOT NULL AND ABS(qty) > 0
                """
            )
            for r in cur.fetchall():
                out.append(
                    {
                        "symbol": r["symbol"],
                        "side": (r["side"] or "").upper(),
                        "qty": float(r["qty"] or 0.0),
                        "entry_price": float(r["entry_price"] or 0.0),
                        "sl": float(r["sl"]) if r["sl"] is not None else None,
                        "tp": float(r["tp"]) if r["tp"] is not None else None,
                        "updated_at": int(r["updated_at"] or 0),
                        "sl_order_id": r["sl_order_id"],
                        "tp_order_id": r["tp_order_id"],
                    }
                )
        return out

    def cache_update_sl_order_id(self, symbol: str, order_id: str | None) -> None:
        with self._conn() as c:
            c.execute("UPDATE positions_cache SET sl_order_id=?, updated_at=? WHERE symbol=?",
                    (order_id, self._utc(), symbol))
            c.commit()

    def cache_update_tp_order_id(self, symbol: str, order_id: str | None) -> None:
        with self._conn() as c:
            c.execute("UPDATE positions_cache SET tp_order_id=?, updated_at=? WHERE symbol=?",
                    (order_id, self._utc(), symbol))
            c.commit()

    # ---- (opsiyonel) yalnız RAM içi mini güncelleme: adı net olsun
    def cache_update_position_mem(self, symbol: str, qty: float | None = None, entry_price: float | None = None, **extras) -> None:
        for p in self._open_positions_cache:
            if p.get("symbol") == symbol:
                if qty is not None:
                    p["qty"] = float(qty)
                if entry_price is not None:
                    p["entry_price"] = float(entry_price)
                if extras:
                    p.update(extras)
                break

    # --------------------------- Orders & Trades ------------------------------
    def record_order(
        self,
        client_id: str,
        symbol: str,
        side: str,
        typ: str,
        status: str,
        price: float,
        qty: float,
        reduce_only: bool,
        extra_json: Optional[Dict[str, Any]] = None,
    ) -> None:
        with self._conn() as c:
            c.execute(
                """
                INSERT INTO futures_orders(client_id, symbol, side, type, status, price, qty, reduce_only, created_at, updated_at, extra_json)
                VALUES(?,?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    client_id,
                    symbol,
                    side,
                    typ,
                    status,
                    float(price),
                    float(qty),
                    1 if reduce_only else 0,
                    now_ts(),
                    now_ts(),
                    json.dumps(extra_json or {}, ensure_ascii=False),
                ),
            )
            c.commit()

    def update_order_status(
        self,
        client_id: str,
        status: str,
        price: Optional[float] = None,
        qty: Optional[float] = None,
        extra: Optional[Dict[str, Any]] = None,
    ) -> None:
        sets, vals = ["status=?", "updated_at=?"], [status, now_ts()]
        if price is not None:
            sets.append("price=?")
            vals.append(float(price))
        if qty is not None:
            sets.append("qty=?")
            vals.append(float(qty))
        if extra is not None:
            sets.append("extra_json=?")
            vals.append(json.dumps(extra, ensure_ascii=False))
        vals.append(client_id)
        sql = f"UPDATE futures_orders SET {', '.join(sets)} WHERE client_id=?"
        with self._conn() as c:
            c.execute(sql, vals)
            c.commit()

    def record_trade(
        self,
        order_id: int,
        symbol: str,
        side: str,
        price: float,
        qty: float,
        fee: float = 0.0,
        realized_pnl: float = 0.0,
        ts: Optional[int] = None,
    ) -> int:
        ts = int(ts or now_ts())
        with self._conn() as c:
            cur = c.execute(
                """
                INSERT INTO futures_trades(order_id, symbol, side, price, qty, fee, realized_pnl, ts)
                VALUES(?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (int(order_id), symbol, side, float(price), float(qty), float(fee), float(realized_pnl), ts),
            )
            c.commit()
            return int(cur.lastrowid)

    # --------------------------- Klines ---------------------------------------
    def record_kline(
        self,
        symbol: str,
        tf: str,
        open_time: int,
        o: float,
        h: float,
        l: float,
        c_: float,
        v: float,
        close_time: int,
    ) -> None:
        with self._conn() as c:
            c.execute(
                """
                INSERT OR REPLACE INTO futures_klines(symbol, tf, open_time, open, high, low, close, volume, close_time)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (symbol, tf, int(open_time), float(o), float(h), float(l), float(c_), float(v), int(close_time)),
            )
            c.commit()

    # --------------------------- Misc helpers ---------------------------------
    def clear_expired_cooldowns(self, now_epoch: Optional[int] = None) -> int:
        if now_epoch is None:
            now_epoch = now_ts()
        with self._conn() as c:
            cur = c.cursor()
            cur.execute(
                """
                UPDATE symbol_state
                   SET cooldown_until_ts=0, updated_at=strftime('%s','now')
                 WHERE COALESCE(cooldown_until_ts,0) > 0 AND cooldown_until_ts < ?
                """,
                (int(now_epoch),),
            )
            c.commit()
            return cur.rowcount

    def recent_orders(self, symbol: str, limit: int = 20) -> List[Dict[str, Any]]:
        with self._conn() as c:
            cur = c.execute(
                """
                SELECT * FROM futures_orders
                 WHERE symbol=?
                 ORDER BY created_at DESC
                 LIMIT ?
                """,
                (symbol, int(limit)),
            )
            return [dict(r) for r in cur.fetchall()]

    def open_positions_by_side(self) -> Dict[str, int]:
        out = {"LONG": 0, "SHORT": 0}
        for p in self.open_positions():
            side = str(p.get("side") or "").upper()
            if side in out:
                out[side] += 1
        return out

    # ---- PnL tahmin/özet (hafif) --------------------------------------------
    def record_close(
        self,
        symbol: str,
        side: str,
        entry_price: float,
        exit_price: float,
        qty: float,
        pnl: float,
    ) -> None:
        """
        Günlük realized PnL için hafif bellek sayacı (istendiğinde DB'ye genişletilebilir).
        """
        if not hasattr(self, "_realized_pnl_today"):
            self._realized_pnl_today = 0.0
            self._realized_pnl_day = None
        import datetime as _dt
        today = _dt.date.today()
        if self._realized_pnl_day != today:
            self._realized_pnl_day = today
            self._realized_pnl_today = 0.0
        self._realized_pnl_today += float(pnl)

    def get_today_realized_pnl(self) -> float:
        if not hasattr(self, "_realized_pnl_today"):
            return 0.0
        import datetime as _dt
        today = _dt.date.today()
        if getattr(self, "_realized_pnl_day", today) != today:
            return 0.0
        return float(self._realized_pnl_today or 0.0)

    def estimate_unrealized_pnl(self, price_provider) -> float:
        """
        list_open_positions() → [{symbol, side, qty, entry_price}, ...] üzerinden anlık PnL tahmini.
        """
        try:
            positions = self.list_open_positions() or []
        except Exception:
            positions = []
        total = 0.0
        for p in positions:
            sym = p.get("symbol")
            side = (p.get("side") or "").upper()
            qty = abs(float(p.get("qty", 0) or 0))
            entry = float(p.get("entry_price", 0) or 0)
            if not sym or qty <= 0 or entry <= 0:
                continue
            last = price_provider(sym) if callable(price_provider) else None
            if last is None:
                continue
            if side == "LONG":
                total += (last - entry) * qty
            elif side == "SHORT":
                total += (entry - last) * qty
        return float(total)

    def estimate_account_equity(self, price_provider, start_equity_fallback: float = 1000.0) -> float:
        """
        Basit equity tahmini: start_equity + realized_today + unrealized.
        Gerçek cüzdan bakiyen varsa burada onu kullanacak şekilde güncelleyebiliriz.
        """
        realized = self.get_today_realized_pnl()
        unrealized = self.estimate_unrealized_pnl(price_provider)
        start_eq = float(start_equity_fallback or 0.0)
        return start_eq + realized + unrealized

    def log_notification(self, channel: str, topic: str, level: str, payload: dict | str) -> None:
        try:
            if not isinstance(payload, str):
                payload = json.dumps(payload, ensure_ascii=False)
            with self._conn() as c:
                c.execute(
                    "INSERT INTO notifications_log (ts, channel, topic, level, payload) VALUES (?, ?, ?, ?, ?)",
                    (self._utc(), channel, topic, level.upper(), payload),
                )
                c.commit()
        except Exception as e:
            self.logger.debug(f"notifications_log insert skipped: {e}")