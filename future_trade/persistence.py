"""SQLite kalıcılık (futures_data.db)
"""
import sqlite3, json, time
from typing import Any, Dict, Optional


SCHEMA = [
"""
CREATE TABLE IF NOT EXISTS futures_klines (
symbol TEXT, tf TEXT, open_time INTEGER, open REAL, high REAL, low REAL, close REAL, volume REAL,
close_time INTEGER, PRIMARY KEY(symbol, tf, close_time)
);
""",
"""
CREATE TABLE IF NOT EXISTS futures_orders (
id INTEGER PRIMARY KEY AUTOINCREMENT,
client_id TEXT, symbol TEXT, side TEXT, type TEXT, status TEXT,
price REAL, qty REAL, reduce_only INTEGER, created_at INTEGER, updated_at INTEGER, extra_json TEXT
);
""",
"""
CREATE TABLE IF NOT EXISTS futures_positions (
symbol TEXT PRIMARY KEY, side TEXT, qty REAL, entry_price REAL, leverage INTEGER,
unrealized_pnl REAL, updated_at INTEGER
);
""",
"""
CREATE TABLE IF NOT EXISTS futures_trades (
id INTEGER PRIMARY KEY AUTOINCREMENT,
order_id INTEGER, symbol TEXT, side TEXT, price REAL, qty REAL, fee REAL, realized_pnl REAL, ts INTEGER
);
""",
"""
CREATE TABLE IF NOT EXISTS symbol_state (
symbol TEXT PRIMARY KEY, state TEXT, cooldown_until INTEGER, last_signal_ts INTEGER,
trail_stop REAL, peak REAL, trough REAL, updated_at INTEGER
);
""",
"""
CREATE TABLE IF NOT EXISTS risk_journal (
ts INTEGER, metric TEXT, value REAL
);
""",
"""
CREATE TABLE IF NOT EXISTS signal_audit (
ts INTEGER, symbol TEXT, side TEXT, decision INTEGER, reasons TEXT
);
""",
]


class Persistence:
    def __init__(self, path: str):
        self.path = path
    
    def _conn(self):
        return sqlite3.connect(self.path)

    def init_schema(self):
        with self._conn() as c:
            cur = c.cursor()

            # Mevcut şemayı kur (senin SCHEMA listenden)
            for stmt in SCHEMA:
                cur.executescript(stmt)

            # --- symbol_state tablosunu güvence altına al ---
            # 1) tablo yoksa oluştur
            cur.execute("""
                CREATE TABLE IF NOT EXISTS symbol_state (
                    symbol TEXT PRIMARY KEY,
                    cooldown_until_ts INTEGER DEFAULT 0,
                    last_exit_ts INTEGER DEFAULT 0
                );
            """)
            # 2) varsa eksik kolonları ekle
            cur.execute("PRAGMA table_info(symbol_state)")
            cols = {row[1] for row in cur.fetchall()}
            if "cooldown_until_ts" not in cols:
                cur.execute("ALTER TABLE symbol_state ADD COLUMN cooldown_until_ts INTEGER DEFAULT 0;")
            if "last_exit_ts" not in cols:
                cur.execute("ALTER TABLE symbol_state ADD COLUMN last_exit_ts INTEGER DEFAULT 0;")

            c.commit()

    def set_cooldown(self, symbol: str, until_ts: int) -> None:
        """Sembole cooldown sonunu (epoch) yazar/yeniler."""
        with self._conn() as c:
            c.execute("""
                INSERT INTO symbol_state(symbol, cooldown_until_ts)
                VALUES(?, ?)
                ON CONFLICT(symbol) DO UPDATE SET cooldown_until_ts=excluded.cooldown_until_ts
            """, (symbol, int(until_ts)))
            c.commit()

    def get_cooldown(self, symbol: str) -> int:
        """Sembol için cooldown sonu (epoch). Yoksa 0 döner."""
        with self._conn() as c:
            cur = c.execute("SELECT cooldown_until_ts FROM symbol_state WHERE symbol=?", (symbol,))
            row = cur.fetchone()
            return int(row[0]) if row and row[0] is not None else 0

    def mark_exit_ts(self, symbol: str, ts: int) -> None:
        """Son çıkış zamanını (epoch) kaydeder."""
        with self._conn() as c:
            c.execute("""
                INSERT INTO symbol_state(symbol, last_exit_ts)
                VALUES(?, ?)
                ON CONFLICT(symbol) DO UPDATE SET last_exit_ts=excluded.last_exit_ts
            """, (symbol, int(ts)))
            c.commit()

        
        
        
    
    def record_signal_audit(self, event: Dict[str, Any], signal, decision: bool, reason: Optional[str] = None):
        with self._conn() as c:
            c.execute("INSERT INTO signal_audit VALUES (?,?,?, ?,?)",
                (int(time.time()), event.get("symbol"), signal.side, int(decision), json.dumps({"reason": reason})))
            c.commit()