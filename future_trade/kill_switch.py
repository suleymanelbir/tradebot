# /opt/tradebot/future_trade/kill_switch.py
from __future__ import annotations
import logging
from typing import Callable, Optional

class KillSwitch:
    """
    Günlük zarar limiti ve/veya gün içi DD tetikleyicisi.
    Koşul sağlanınca:
      - Tüm açık pozisyonlar kapatılır
      - Yeni emir açma kilitlenir (trading_enabled=False)
      - Telegram'a alarm atılır
    """

    def __init__(
        self,
        cfg: dict,
        persistence,
        reconciler,          # close_all_for_symbol(...) için kullanacağız
        notifier,
        price_provider: Callable[[str], Optional[float]],
        logger: logging.Logger = None
    ):
        self.cfg = cfg or {}
        self.logger = logger or logging.getLogger("kill_switch")
        self.db = persistence
        self.rec = reconciler
        self.notifier = notifier
        self.price_provider = price_provider

        r = self.cfg.get("risk", {}) or {}
        self.daily_loss_limit_pct = float(r.get("daily_loss_limit_pct", 0) or 0)
        self.global_dd_limit_pct = float(r.get("global_kill_switch_drawdown_pct", 0) or 0)
        self.start_equity = float(r.get("start_equity_usdt", 0) or 0)

        self.trading_enabled = True
        self._peak_equity = None  # gün içi zirve equity (DD için)

    # Dışarıya “yeni emir açılabilir mi?” sorusu için:
    def is_trading_allowed(self) -> bool:
        return bool(self.trading_enabled)

    async def _alert(self, payload: dict):
        if not self.notifier or not hasattr(self.notifier, "alert"):
            return
        try:
            await self.notifier.alert(payload)
        except Exception:
            # sync notifier bile olabilir; sessiz düş
            try:
                self.notifier.alert(payload)  # type: ignore
            except Exception:
                pass

    def _equity_now(self) -> float:
        # Persistence üzerinden basit tahmin
        return float(self.db.estimate_account_equity(self.price_provider, start_equity_fallback=self.start_equity))

    async def check_and_maybe_trigger(self) -> dict:
        """
        Eşikler aşıldıysa Kill-Switch tetikler. Aksi halde sadece ölçümleri döndürür.
        Dönüş: {"equity": float, "pnl_pct": float, "dd_pct": float, "triggered": bool, "reason": str|None}
        """
        eq = self._equity_now()
        reason = None
        triggered = False

        # Başlangıç equity referansı
        start_eq = self.start_equity or eq  # ilk çalışmada start set edilmemişse mevcut equity'i baz al
        if self.start_equity == 0:
            self.start_equity = eq

        # Gün içi zirveyi takip et (DD için)
        if self._peak_equity is None or eq > self._peak_equity:
            self._peak_equity = eq

        pnl_pct = 0.0 if start_eq <= 0 else (eq - start_eq) / start_eq * 100.0
        dd_pct = 0.0 if not self._peak_equity or self._peak_equity <= 0 else (self._peak_equity - eq) / self._peak_equity * 100.0

        # Eşik kontrol
        if self.daily_loss_limit_pct > 0 and pnl_pct <= -abs(self.daily_loss_limit_pct):
            reason = f"daily_loss_limit_pct reached ({pnl_pct:.2f}%)"
            triggered = True
        elif self.global_dd_limit_pct > 0 and dd_pct >= abs(self.global_dd_limit_pct):
            reason = f"drawdown_limit reached ({dd_pct:.2f}%)"
            triggered = True

        if triggered and self.trading_enabled:
            await self._trigger_liquidation(reason)

        return {"equity": eq, "pnl_pct": pnl_pct, "dd_pct": dd_pct, "triggered": triggered, "reason": reason}

    async def _trigger_liquidation(self, reason: str):
        """
        Tüm açık pozisyonları kapat ve alarmları gönder; yeni emirleri kilitle.
        """
        self.logger.error(f"[KILL-SWITCH] Triggering due to: {reason}")
        await self._alert({"event": "kill_switch_trigger", "reason": reason})

        # Açık pozisyonları kapat
        try:
            positions = self.db.list_open_positions() or []
        except Exception as e:
            self.logger.error(f"[KILL-SWITCH] list_open_positions error: {e}")
            positions = []

        closed = 0
        for p in positions:
            try:
                sym = p.get("symbol")
                side = (p.get("side") or "").upper()  # LONG/SHORT
                qty = float(p.get("qty", 0) or 0)
                entry = float(p.get("entry_price", 0) or 0)
                last = self.price_provider(sym)
                if not sym or qty <= 0 or last is None:
                    continue
                # Reconciler üzerinden TAM kapat
                self.rec.close_all_for_symbol(sym, side, qty, entry_price=entry, exit_price=last)
                closed += 1
            except Exception as e:
                self.logger.error(f"[KILL-SWITCH] close error for {p}: {e}")

        await self._alert({"event": "kill_switch_positions_closed", "count": closed})

        # Yeni emirleri kilitle
        self.trading_enabled = False
        self.logger.error("[KILL-SWITCH] Trading disabled")

    def reset_for_new_day(self, new_start_equity: float = None):
        """
        Gün başında çağır: limitleyicileri sıfırla ve yeni bazları ata.
        """
        if isinstance(new_start_equity, (int, float)) and new_start_equity > 0:
            self.start_equity = float(new_start_equity)
        else:
            self.start_equity = self._equity_now()
        self._peak_equity = None
        self.trading_enabled = True
