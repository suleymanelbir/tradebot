
import sqlite3
from pathlib import Path
import json
import logging
from typing import Dict, Optional, Tuple

logger = logging.getLogger(__name__)

SCHEMA = {
    "global_closing_data": """
        CREATE TABLE IF NOT EXISTS global_closing_data (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp DATETIME NOT NULL,
            symbol TEXT NOT NULL,
            price REAL NOT NULL,
            interval TEXT NOT NULL,
            suspect INTEGER DEFAULT 0,
            UNIQUE(symbol, interval, timestamp)
        );
    """,
    "global_live_data": """
        CREATE TABLE IF NOT EXISTS global_live_data (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp DATETIME NOT NULL,
            symbol TEXT NOT NULL,
            live_price REAL NOT NULL,
            change_15m REAL,
            change_1h REAL,
            change_4h REAL,
            change_1d REAL,
            suspect INTEGER DEFAULT 0
        );
    """,
    "ingestion_runs": """
        CREATE TABLE IF NOT EXISTS ingestion_runs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            started_at DATETIME NOT NULL,
            finished_at DATETIME,
            status TEXT NOT NULL,
            stats_json TEXT,
            message TEXT
        );
    """
}

PRAGMAS = [
    "PRAGMA journal_mode=WAL;",
    "PRAGMA synchronous=NORMAL;",
    "PRAGMA temp_store=MEMORY;",
    "PRAGMA cache_size=-16000;",
    "PRAGMA busy_timeout=8000;"
]

class Database:
    def __init__(self, db_path: Path):
        self.db_path = Path(db_path)
        self.conn = None

    def connect(self) -> None:
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self.conn = sqlite3.connect(str(self.db_path), check_same_thread=False, timeout=30)
        self.conn.row_factory = sqlite3.Row
        cur = self.conn.cursor()
        for p in PRAGMAS:
            cur.execute(p)
        for _, ddl in SCHEMA.items():
            cur.execute(ddl)
        self.conn.commit()
        logger.info("DB connected and schema ensured", extra={"db_path": str(self.db_path)})

    def close(self) -> None:
        if self.conn:
            self.conn.close()
            self.conn = None

    def _execute(self, sql: str, params=()) -> sqlite3.Cursor:
        cur = self.conn.cursor()
        cur.execute(sql, params)
        return cur

    def upsert_close(self, symbol: str, interval: str, close_ts_iso: str, price: float, suspect: int=0) -> None:
        sql = """
            INSERT INTO global_closing_data (timestamp, symbol, price, interval, suspect)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(symbol, interval, timestamp)
            DO UPDATE SET price=excluded.price, suspect=excluded.suspect;
        """
        self._execute(sql, (close_ts_iso, symbol, price, interval, suspect))
        self.conn.commit()

    def insert_live(self, ts_iso: str, symbol: str, price: float, changes: Dict[str, Optional[float]], suspect: int=0) -> None:
        sql = """
            INSERT INTO global_live_data (timestamp, symbol, live_price, change_15m, change_1h, change_4h, change_1d, suspect)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?);
        """
        self._execute(sql, (
            ts_iso, symbol, price,
            changes.get("15m"), changes.get("1h"), changes.get("4h"), changes.get("1d"),
            suspect
        ))
        self.conn.commit()

    def prune(self, limits: Dict[str, int]) -> None:
        for table, limit in limits.items():
            sql = f"""
                DELETE FROM {table}
                WHERE id NOT IN (
                    SELECT id FROM {table} ORDER BY timestamp DESC LIMIT ?
                );
            """
            cur = self._execute(sql, (limit,))
            if cur.rowcount and cur.rowcount > 0:
                logger.info("Pruned rows", extra={"table": table, "deleted": cur.rowcount})
        self.conn.commit()

    def record_run_start(self) -> int:
        sql = "INSERT INTO ingestion_runs (started_at, status) VALUES (datetime('now'), 'running');"
        cur = self._execute(sql)
        self.conn.commit()
        return cur.lastrowid

    def record_run_end(self, run_id: int, status: str, stats: Dict, message: str="") -> None:
        sql = "UPDATE ingestion_runs SET finished_at=datetime('now'), status=?, stats_json=?, message=? WHERE id=?;"
        self._execute(sql, (status, json.dumps(stats, ensure_ascii=False), message, run_id))
        self.conn.commit()

    def latest_close(self, symbol: str, interval: str) -> Optional[float]:
        sql = """
            SELECT price FROM global_closing_data
            WHERE symbol=? AND interval=?
            ORDER BY timestamp DESC LIMIT 1;
        """
        cur = self._execute(sql, (symbol, interval))
        row = cur.fetchone()
        return float(row["price"]) if row else None
