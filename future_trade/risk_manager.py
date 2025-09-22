# /opt/tradebot/future_trade/risk_manager.py
"""
Risk & boyutlandırma:
- Qty = (equity * per_trade_risk_pct) / stop_distance
- Stop/TP yoksa basit yüzdesel SL/TP kuralı uygula (paper için yeterli)
- Leverage haritası (cfg["leverage"]) dikkate alınır
- Min notional buffer uygulanır (cfg["risk"].min_notional_buffer)
NOT: Gerçek modda tick/step/minNotional filtreleri OrderRouter/exchange_utils ile birlikte uygulanacak.
"""

from dataclasses import dataclass
from typing import Dict, Any, Optional
import math


@dataclass
class TradePlan:
    ok: bool
    reason: Optional[str] = None

    symbol: Optional[str] = None
    side: Optional[str] = None

    entry: Optional[float] = None
    qty: Optional[float] = None
    leverage: Optional[int] = None

    sl: Optional[float] = None
    tp: Optional[float] = None

    # Router için yardımcı alanlar:
    entry_type: str = "MARKET"   # config: order.entry
    tp_mode: str = "limit"       # config: order.tp_mode
    sl_working_type: str = "MARK_PRICE"  # config: order.sl_working_type
    time_in_force: str = "GTC"   # config: order.time_in_force
    reduce_only_protection: bool = True  # config: order.reduce_only_protection


class RiskManager:
    def __init__(self, risk_cfg: Dict[str, Any], leverage_map: Dict[str, int], portfolio):
        self.risk_cfg = risk_cfg or {}
        self.lev_map = leverage_map or {}
        self.portfolio = portfolio

        # Varsayılan parametreler
        self.per_trade_risk_pct = float(self.risk_cfg.get("per_trade_risk_pct", 0.5)) / 100.0
        self.daily_max_loss_pct = float(self.risk_cfg.get("daily_max_loss_pct", 3.0)) / 100.0
        self.max_concurrent = int(self.risk_cfg.get("max_concurrent_positions", 4))
        self.kill_switch_after = int(self.risk_cfg.get("kill_switch_after_consecutive_losses", 4))
        self.min_notional_buffer = float(self.risk_cfg.get("min_notional_buffer", 1.1))

    def _symbol_leverage(self, symbol: str) -> int:
        return int(self.lev_map.get(symbol, self.lev_map.get("default", 3)))

    def _fallback_entry_sl_tp(self, side: str, last_close: float, sl_pct: float, tp_rr: float):
        """
        Stop yüzdesi ve risk/ödül oranına göre SL/TP hazırla.
        sl_pct: ör. 1.0 => %1 stop
        tp_rr : ör. 1.5 => risk * 1.5 (RR=1.5)
        """
        entry = float(last_close)
        if side == "LONG":
            sl = entry * (1.0 - sl_pct / 100.0)
            tp = entry + (entry - sl) * tp_rr
        else:
            sl = entry * (1.0 + sl_pct / 100.0)
            tp = entry - (sl - entry) * tp_rr
        return entry, sl, tp

    def plan_trade(self, symbol: str, signal) -> TradePlan:
        """
        Strategy.on_bar() sonucu gelen sinyal doğrultusunda boyutlandırma yapar.
        Paper akışta entry=bar close kabul edilir (router gerçek fill'i kaydedecek).
        """
        if signal.side not in ("LONG", "SHORT"):
            return TradePlan(ok=False, reason="flat_signal")

        # Portföy özeti
        equity = float(self.portfolio.equity())
        if equity <= 0:
            return TradePlan(ok=False, reason="no_equity")

        # Risk parametreleri
        risk_cap = equity * self.per_trade_risk_pct
        if risk_cap <= 0:
            return TradePlan(ok=False, reason="zero_risk_cap")

        # Leverage
        lev = self._symbol_leverage(symbol)

        # Paper akış: son fiyatı portföy/stream tarafı verebiliyor; burada son close'u portfolio'dan almıyoruz.
        # Router, plan.entry yoksa "stream.get_last_price()" kullanıyor olabilir; güvenli taraf için entry'yi burada hesaplayalım.
        # (app/strategy tarafı event.close taşıyorsa, plan.entry'yi onu kullanan yere geçirmen de mümkün)
        # Basit yaklaşım: entry/SL/TP fallback parametreleri
        order_cfg: Dict[str, Any] = getattr(self, "order_cfg", {}) or {}
        sl_pct = float(order_cfg.get("sl_pct", 1.0))    # %1 varsayılan
        tp_rr  = float(order_cfg.get("tp_rr", 1.5))     # RR=1.5 varsayılan

        # Strategy sinyalinde opsiyonel SL/TP gelmiş olabilir; yoksa fallback üret
        last_close = getattr(signal, "entry_price", None) or getattr(signal, "close", None) or 100.0
        entry, sl, tp = self._fallback_entry_sl_tp(signal.side, float(last_close), sl_pct, tp_rr)

        stop_distance = abs(entry - sl)
        if stop_distance <= 0:
            return TradePlan(ok=False, reason="invalid_stop_distance")

        # Boyut (qty)
        # NOT: USDT margined sözleşmelerde notional = price * qty
        qty = risk_cap / stop_distance
        if qty <= 0:
            return TradePlan(ok=False, reason="qty_zero")

        # Min notional buffer (ör. 1.1x)
        notional = entry * qty
        min_notional = 5.0  # paper için muhafazakar default; gerçek filtre ile değişecek
        if notional < min_notional * self.min_notional_buffer:
            # qty'yi yükseltmek yerine güvenli tarafta reddedelim (paper)
            return TradePlan(ok=False, reason=f"min_notional_violation wanted={notional:.2f} min~{min_notional*self.min_notional_buffer:.2f}")

        # Paper için kaba quantize (gerçekte symbol_filters ile step/tick uygulanacak)
        qty = max(0.001, float(f"{qty:.3f}"))

        # Emir türleri & flags (OrderRouter ile uyumlu)
        plan = TradePlan(
            ok=True,
            symbol=symbol,
            side=signal.side,
            entry=entry,
            qty=qty,
            leverage=lev,
            sl=sl,
            tp=tp,
            entry_type=order_cfg.get("entry", "MARKET"),
            tp_mode=order_cfg.get("tp_mode", "limit"),
            sl_working_type=order_cfg.get("sl_working_type", "MARK_PRICE"),
            time_in_force=order_cfg.get("time_in_force", "GTC"),
            reduce_only_protection=bool(order_cfg.get("reduce_only_protection", True)),
        )
        return plan

    # app.py içinde RiskManager yaratılırken order ayarlarını enjekte edebilmen için:
    def bind_order_cfg(self, order_cfg: Dict[str, Any]):
        self.order_cfg = order_cfg or {}
