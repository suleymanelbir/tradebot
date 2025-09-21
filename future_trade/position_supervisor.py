"""Giriş kabul/ret; kapasite ve sembol kotaları; cooldown
"""
import time
from typing import Tuple, Set
from typing import Optional

# position_supervisor.py

class PositionSupervisor:
    def __init__(self, cfg, portfolio, notifier, persistence):
        self.cfg = cfg
        self.portfolio = portfolio
        self.notifier = notifier
        
    def evaluate_entry(self, symbol, signal) -> Tuple[bool, str]:
        # 1) FLAT ise asla girme
        if signal.side == "FLAT":
            return False, "flat"

        # 2) Whitelist kontrolü
        wl: Set[str] = set(self.cfg.get("symbols_whitelist", []))
        if symbol not in wl:
            return False, "not_in_whitelist"

        # 3) Sembol başı limit
        max_per_symbol = int(self.cfg.get("max_trades_per_symbol", 1))
        positions = self.portfolio.open_positions()
        symbol_open = sum(1 for p in positions if p.symbol == symbol)
        if symbol_open >= max_per_symbol:
            return False, "max_symbol_limit_reached"

        # 4) Global ve yön bazlı limitler
        total_open = len(positions)
        long_count  = sum(1 for p in positions if p.side == "LONG")
        short_count = sum(1 for p in positions if p.side == "SHORT")

        if total_open >= int(self.cfg.get("max_open_trades_global", 4)):
            return False, "max_global_limit_reached"
        if signal.side == "LONG" and long_count >= int(self.cfg.get("max_long_trades", 3)):
            return False, "max_long_limit_reached"
        if signal.side == "SHORT" and short_count >= int(self.cfg.get("max_short_trades", 3)):
            return False, "max_short_limit_reached"

        # ✅ Tüm kontroller geçti
        return True, "ok"

    def set_cooldown(self, symbol: str, until_ts: int) -> None:
        """Sinyal sonrası sembolü until_ts (epoch) zamanına kadar kilitle."""
        now = int(time.time())
        with self._conn() as c:
            cur = c.cursor()
            cur.execute("""
                INSERT INTO symbol_state(symbol, state, cooldown_until, last_signal_ts, updated_at)
                VALUES (?, 'cooldown', ?, ?, ?)
                ON CONFLICT(symbol) DO UPDATE SET
                    state='cooldown',
                    cooldown_until=excluded.cooldown_until,
                    last_signal_ts=excluded.last_signal_ts,
                    updated_at=excluded.updated_at
            """, (symbol, int(until_ts), now, now))
            c.commit()

    def get_cooldown(self, symbol: str, now_ts: Optional[int] = None) -> int:
        """Kalan cooldown saniyesi (yoksa 0)."""
        if now_ts is None:
            now_ts = int(time.time())
        with self._conn() as c:
            cur = c.cursor()
            cur.execute("SELECT cooldown_until FROM symbol_state WHERE symbol=?", (symbol,))
            row = cur.fetchone()
            if not row or row[0] is None:
                return 0
            remain = int(row[0]) - now_ts
            return max(0, remain)

    def mark_exit_ts(self, symbol: str, ts: Optional[int] = None) -> None:
        """Pozisyon kapandıktan sonra son çıkış zamanını işaretle (rapor/analiz için)."""
        if ts is None:
            ts = int(time.time())
        with self._conn() as c:
            cur = c.cursor()
            cur.execute("""
                INSERT INTO symbol_state(symbol, last_signal_ts, updated_at)
                VALUES (?, ?, ?)
                ON CONFLICT(symbol) DO UPDATE SET
                    last_signal_ts=excluded.last_signal_ts,
                    updated_at=excluded.updated_at
            """, (symbol, ts, ts))
            c.commit()