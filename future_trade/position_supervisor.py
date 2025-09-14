"""Giriş kabul/ret; kapasite ve sembol kotaları; cooldown
"""
from typing import Tuple


# position_supervisor.py
class PositionSupervisor:
    def __init__(self, cfg, portfolio, notifier):
        self.cfg = cfg
        self.portfolio = portfolio
        self.notifier = notifier

    def evaluate_entry(self, symbol, signal) -> Tuple[bool, str]:
        if signal.side == "FLAT":
            return False, "flat"

        whitelist = set(self.cfg.get("symbols_whitelist", []))
        if symbol not in whitelist:
            return False, "not_in_whitelist"

        max_per_symbol = int(self.cfg.get("max_trades_per_symbol", 1))

        # Mevcut pozisyonlar
        positions = self.portfolio.open_positions()
        total_open = len(positions)
        long_count  = sum(1 for p in positions if p.side == "LONG")
        short_count = sum(1 for p in positions if p.side == "SHORT")

        # Sembol başı kota (>=1 desteklenir)
        symbol_open = sum(1 for p in positions if p.symbol == symbol)
        if symbol_open >= max_per_symbol:
            return False, "max_symbol_limit_reached"

        # Global ve yön bazlı limitler
        max_global = int(self.cfg.get("max_open_trades_global", 4))
        max_longs  = int(self.cfg.get("max_long_trades", 3))
        max_shorts = int(self.cfg.get("max_short_trades", 3))

        if total_open >= max_global:
            return False, "max_global_limit_reached"
        if signal.side == "LONG" and long_count >= max_longs:
            return False, "max_long_limit_reached"
        if signal.side == "SHORT" and short_count >= max_shorts:
            return False, "max_short_limit_reached"

        return True, "ok"