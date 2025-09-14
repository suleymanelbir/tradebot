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
            for stmt in SCHEMA:
                cur.executescript(stmt)
            c.commit()
    
    def record_signal_audit(self, event: Dict[str, Any], signal, decision: bool, reason: Optional[str] = None):
        with self._conn() as c:
            c.execute("INSERT INTO signal_audit VALUES (?,?,?, ?,?)",
                (int(time.time()), event.get("symbol"), signal.side, int(decision), json.dumps({"reason": reason})))
            c.commit()