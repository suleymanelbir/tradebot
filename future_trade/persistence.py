"""SQLite kalıcılık (futures_data.db)
- Şema oluşturma + hafif migration
- Yardımcı operasyonlar: pozisyon/sipariş/trade/state/sinyal
"""
import sqlite3, json, time, logging
from typing import Any, Dict, Optional, Iterable, List

def now_ts() -> int:
    return int(time.time())

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
    # Positions (ONE-WAY per symbol mantığına uygun)
    """
    CREATE TABLE IF NOT EXISTS futures_positions (
        symbol TEXT PRIMARY KEY,
        side TEXT, qty REAL, entry_price REAL, leverage INTEGER,
        unrealized_pnl REAL, updated_at INTEGER
    );
    """,
    # Trades (taker/maker, realized pnl vs.)
    """
    CREATE TABLE IF NOT EXISTS futures_trades (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        order_id INTEGER, symbol TEXT, side TEXT,
        price REAL, qty REAL, fee REAL, realized_pnl REAL, ts INTEGER
    );
    """,
    # State (KANONİK): cooldown_until_ts + last_exit_ts dahil
    # Not: eski tabloda cooldown_until kolonu olabilir. Aşağıda migration ile taşınır.
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
    """
]

PRAGMAS = [
    "PRAGMA journal_mode=WAL;",
    "PRAGMA synchronous=NORMAL;",
    "PRAGMA foreign_keys=ON;"
]

class Persistence:
    def __init__(self, path: str):
        self.path = path

    def _conn(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.path)
        conn.row_factory = sqlite3.Row
        for p in PRAGMAS:
            try:
                conn.execute(p)
            except Exception:
                pass
        return conn

    # --------------------------- Schema & Migration ---------------------------

    def init_schema(self):
        with self._conn() as c:
            cur = c.cursor()
            for stmt in SCHEMA:
                cur.executescript(stmt)

            # --- MIGRATION: eski 'cooldown_until' → yeni 'cooldown_until_ts' ---
            cur.execute("PRAGMA table_info(symbol_state)")
            cols = {row[1] for row in cur.fetchall()}

            # Eski kolon varsa ve yenisi de varsa, taşımayı yap
            if "cooldown_until" in cols and "cooldown_until_ts" in cols:
                try:
                    cur.execute("""
                        UPDATE symbol_state
                        SET cooldown_until_ts =
                            CASE
                                WHEN (cooldown_until_ts IS NULL OR cooldown_until_ts = 0)
                                THEN COALESCE(cooldown_until, 0)
                                ELSE cooldown_until_ts
                            END
                    """)
                except Exception as e:
                    logging.debug(f"cooldown migration skip: {e}")

            # Güvenlik: son olarak updated_at kolonunu güncel şemaya uygun tut
            try:
                cur.execute("""
                    UPDATE symbol_state
                    SET updated_at = COALESCE(updated_at, strftime('%s','now'))
                    WHERE updated_at IS NULL
                """)
            except Exception:
                pass

            c.commit()

    # --------------------------- Signal Audit ---------------------------

    def record_signal_audit(self, event: Dict[str, Any], signal, decision: bool, reason: Optional[str] = None):
        with self._conn() as c:
            c.execute(
                "INSERT INTO signal_audit VALUES (?,?,?,?,?)",
                (int(time.time()), event.get("symbol"), getattr(signal, "side", None),
                 int(bool(decision)), json.dumps({"reason": reason}, ensure_ascii=False))
            )
            c.commit()

    # --------------------------- Symbol State ---------------------------

    def set_cooldown(self, symbol: str, until_ts: int):
        with self._conn() as c:
            c.execute("""
            INSERT INTO symbol_state(symbol, cooldown_until, updated_at)
            VALUES(?, ?, strftime('%s','now'))
            ON CONFLICT(symbol) DO UPDATE SET cooldown_until=excluded.cooldown_until, updated_at=excluded.updated_at
            """, (symbol, until_ts))
            c.commit()

    def get_cooldown(self, symbol: str) -> int:
        with self._conn() as c:
            cur = c.cursor()
            cur.execute("SELECT COALESCE(cooldown_until,0) FROM symbol_state WHERE symbol=?", (symbol,))
            row = cur.fetchone()
            return int(row[0]) if row else 0

    def mark_exit_ts(self, symbol: str, ts: int):
        with self._conn() as c:
            c.execute("""
            INSERT INTO symbol_state(symbol, state, updated_at, last_signal_ts)
            VALUES(?, 'flat', ?, ?)
            ON CONFLICT(symbol) DO UPDATE SET state='flat', updated_at=?, last_signal_ts=?
            """, (symbol, ts, ts, ts, ts))
            c.commit()


    def set_trail_stop(self, symbol: str, value: Optional[float]) -> None:
        with self._conn() as c:
            c.execute("""
                INSERT INTO symbol_state(symbol, trail_stop, updated_at)
                VALUES(?, ?, strftime('%s','now'))
                ON CONFLICT(symbol)
                DO UPDATE SET trail_stop=excluded.trail_stop,
                              updated_at=excluded.updated_at
            """, (symbol, None if value is None else float(value)))
            c.commit()

    def get_trail_stop(self, symbol: str) -> Optional[float]:
        with self._conn() as c:
            cur = c.execute("SELECT trail_stop FROM symbol_state WHERE symbol=?", (symbol,))
            row = cur.fetchone()
            return float(row[0]) if row and row[0] is not None else None

    def set_last_signal_ts(self, symbol: str, ts: int) -> None:
        with self._conn() as c:
            c.execute("""
                INSERT INTO symbol_state(symbol, last_signal_ts, updated_at)
                VALUES(?, ?, strftime('%s','now'))
                ON CONFLICT(symbol)
                DO UPDATE SET last_signal_ts=excluded.last_signal_ts,
                              updated_at=excluded.updated_at
            """, (symbol, int(ts)))
            c.commit()

    def get_symbol_state(self, symbol: str) -> Dict[str, Any]:
        with self._conn() as c:
            cur = c.execute("SELECT * FROM symbol_state WHERE symbol=?", (symbol,))
            row = cur.fetchone()
            return dict(row) if row else {}

    # --------------------------- Positions ---------------------------

    def upsert_position(self, symbol: str, side: str, qty: float, entry_price: float, leverage: int = 1) -> None:
        with self._conn() as c:
            c.execute("""
                INSERT INTO futures_positions(symbol, side, qty, entry_price, leverage, unrealized_pnl, updated_at)
                VALUES(?,?,?,?,?,0,?)
                ON CONFLICT(symbol) DO UPDATE SET
                  side=excluded.side,
                  qty=excluded.qty,
                  entry_price=excluded.entry_price,
                  leverage=excluded.leverage,
                  updated_at=excluded.updated_at
            """, (symbol, side, float(qty), float(entry_price), int(leverage), now_ts()))
            c.commit()

    def delete_position(self, symbol: str) -> None:
        with self._conn() as c:
            c.execute("DELETE FROM futures_positions WHERE symbol=?", (symbol,))
            c.commit()

    def open_positions(self) -> List[Dict[str, Any]]:
        with self._conn() as c:
            cur = c.execute("SELECT * FROM futures_positions WHERE ABS(qty) > 0 ORDER BY updated_at DESC")
            return [dict(r) for r in cur.fetchall()]

    def close_position(self, symbol: str) -> None:
        with self._conn() as c:
            c.execute("UPDATE futures_positions SET qty=0, updated_at=? WHERE symbol=?", (now_ts(), symbol))
            # state dokun: en azından updated_at güncellensin
            c.execute("""
              INSERT INTO symbol_state(symbol, state, cooldown_until, last_signal_ts, trail_stop, peak, trough, updated_at)
              VALUES(?, COALESCE((SELECT state FROM symbol_state WHERE symbol=?), ''), 0, COALESCE((SELECT last_signal_ts FROM symbol_state WHERE symbol=?), 0), NULL, NULL, NULL, ?)
              ON CONFLICT(symbol) DO UPDATE SET updated_at=excluded.updated_at
            """, (symbol, symbol, symbol, now_ts()))
            c.commit()

    def list_open_positions(self) -> List[Dict[str, Any]]:
        with self._conn() as c:
            cur = c.execute("SELECT symbol, side, qty, entry_price, leverage, updated_at FROM futures_positions WHERE ABS(qty) > 0")
            rows = cur.fetchall()
            return [
                {"symbol": r[0], "side": r[1], "qty": float(r[2]), "entry_price": float(r[3]), "leverage": int(r[4]), "updated_at": int(r[5])}
                for r in rows
            ]

    def position_by_symbol(self, symbol: str) -> Optional[Dict[str, Any]]:
        with self._conn() as c:
            cur = c.execute("SELECT * FROM futures_positions WHERE symbol=?", (symbol,))
            row = cur.fetchone()
            return dict(row) if row else None

    # --------------------------- Orders & Trades ---------------------------

    def record_order(self, client_id: str, symbol: str, side: str, typ: str, status: str,
                     price: float, qty: float, reduce_only: bool, extra_json: Optional[Dict[str, Any]] = None) -> None:
        with self._conn() as c:
            c.execute("""
                INSERT INTO futures_orders(client_id, symbol, side, type, status, price, qty, reduce_only, created_at, updated_at, extra_json)
                VALUES(?,?,?,?,?,?,?,?,?,?,?)
            """, (client_id, symbol, side, typ, status, float(price), float(qty), 1 if reduce_only else 0, now_ts(), now_ts(),
                  json.dumps(extra_json or {}, ensure_ascii=False)))
            c.commit()

    def update_order_status(self, client_id: str, status: str,
                            price: Optional[float] = None,
                            qty: Optional[float] = None,
                            extra: Optional[Dict[str, Any]] = None) -> None:
        now = int(time.time())
        sets, vals = ["status=?","updated_at=?"], [status, now]
        if price is not None:
            sets.append("price=?"); vals.append(float(price))
        if qty is not None:
            sets.append("qty=?"); vals.append(float(qty))
        if extra is not None:
            sets.append("extra_json=?"); vals.append(json.dumps(extra, ensure_ascii=False))
        vals.append(client_id)
        sql = f"UPDATE futures_orders SET {', '.join(sets)} WHERE client_id=?"
        with self._conn() as c:
            c.execute(sql, vals)
            c.commit()

    def record_trade(self, order_id: int, symbol: str, side: str, price: float,
                     qty: float, fee: float = 0.0, realized_pnl: float = 0.0, ts: Optional[int] = None) -> int:
        ts = int(ts or time.time())
        with self._conn() as c:
            cur = c.execute("""
                INSERT INTO futures_trades(order_id, symbol, side, price, qty, fee, realized_pnl, ts)
                VALUES(?, ?, ?, ?, ?, ?, ?, ?)
            """, (int(order_id), symbol, side, float(price), float(qty), float(fee), float(realized_pnl), ts))
            c.commit()
            return int(cur.lastrowid)

    # --------------------------- Klines ---------------------------

    def record_kline(self, symbol: str, tf: str, open_time: int, o: float, h: float, l: float, c_: float,
                     v: float, close_time: int) -> None:
        with self._conn() as c:
            c.execute("""
                INSERT OR REPLACE INTO futures_klines(symbol, tf, open_time, open, high, low, close, volume, close_time)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (symbol, tf, int(open_time), float(o), float(h), float(l), float(c_), float(v), int(close_time)))
            c.commit()

    # --- ekleme: net isimli getter'lar ---
    def get_cooldown_ts(self, symbol: str) -> int:
        """Cooldown epoch (yoksa 0)."""
        with self._conn() as c:
            cur = c.execute("SELECT cooldown_until_ts FROM symbol_state WHERE symbol=?", (symbol,))
            row = cur.fetchone()
            if row and row[0]:
                return int(row[0])
            # backward-compat: eski kolon varsa
            try:
                cur = c.execute("SELECT cooldown_until FROM symbol_state WHERE symbol=?", (symbol,))
                row = cur.fetchone()
                if row and row[0]:
                    return int(row[0])
            except sqlite3.OperationalError:
                pass
            return 0

    def get_cooldown_remaining(self, symbol: str, now_ts: Optional[int] = None) -> int:
        """Kalan cooldown saniyesi; yoksa 0."""
        if now_ts is None:
            now_ts = int(time.time())
        until = self.get_cooldown_ts(symbol)
        remain = until - now_ts
        return remain if remain > 0 else 0

    # --- mevcut get_cooldown'u koruyalım ama yönlendirelim ---
    def get_cooldown(self, symbol: str) -> int:
        """DEPRECATED: epoch döndürür. Yeni kullanım için get_cooldown_ts/get_cooldown_remaining."""
        return self.get_cooldown_ts(symbol)

    # --- temizlik: süresi dolmuş cooldown'ları sıfırla ---
    def clear_expired_cooldowns(self, now_ts: Optional[int] = None) -> int:
        if now_ts is None:
            now_ts = int(time.time())
        with self._conn() as c:
            cur = c.cursor()
            cur.execute("""
                UPDATE symbol_state
                SET cooldown_until_ts=0, updated_at=strftime('%s','now')
                WHERE COALESCE(cooldown_until_ts,0) > 0 AND cooldown_until_ts < ?
            """, (int(now_ts),))
            c.commit()
            return cur.rowcount

    def recent_orders(self, symbol: str, limit: int = 20) -> list[dict]:
        with self._conn() as c:
            cur = c.execute("""
                SELECT * FROM futures_orders
                WHERE symbol=?
                ORDER BY created_at DESC
                LIMIT ?
            """, (symbol, int(limit)))
            return [dict(r) for r in cur.fetchall()]

    def open_positions_by_side(self) -> dict:
        """{'LONG': n, 'SHORT': m}"""
        out = {'LONG': 0, 'SHORT': 0}
        for p in self.open_positions():
            side = str(p.get('side') or '').upper()
            if side in out:
                out[side] += 1
        return out
