# /opt/tradebot/future_trade/risk_manager.py
"""
Risk & boyutlandırma:
- Qty = (equity * per_trade_risk_pct) / stop_distance
- Stop/TP yoksa basit yüzdesel SL/TP kuralı uygula (paper için yeterli)
- Leverage haritası (cfg["leverage"]) dikkate alınır
- Min notional buffer uygulanır (cfg["risk"].min_notional_buffer)
- NORMALİZASYON: price/qty (tick/step) ve minNotional kontrolleri için
  normalizer kancaları desteklenir (bind_normalizer / bind_trailing_cfg / bind_atr_provider).
"""

from dataclasses import dataclass
from typing import Dict, Any, Optional



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

        # Opsiyonel kancalar (app tarafından bağlanabilir)
        self.order_cfg: Dict[str, Any] = {}
        self.trailing_cfg: Dict[str, Any] = {}
        self.normalizer = None           # exchange_utils.ExchangeNormalizer
        self._get_atr = None             # callable(symbol, period) -> float|None

    # ---------- KANCALAR ----------
    def bind_order_cfg(self, order_cfg: Dict[str, Any]):
        self.order_cfg = order_cfg or {}

    def bind_trailing_cfg(self, trailing_cfg: Dict[str, Any]):
        self.trailing_cfg = trailing_cfg or {}

    def bind_normalizer(self, normalizer):
        self.normalizer = normalizer

    def bind_atr_provider(self, get_atr_callable):
        """get_atr(symbol:str, period:int) -> float|None"""
        self._get_atr = get_atr_callable

    # ---------- YARDIMCILAR ----------
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

    def _stop_distance_from_trailing(self, symbol: str, last_price: float) -> Optional[float]:
        """
        Trailing cfg'ye göre tahmini bir stop mesafesi üretir.
        ATR mevcutsa atr_mult*ATR; yoksa step_pct * last_price.
        """
        if not last_price or last_price <= 0:
            return None
        tr = self.trailing_cfg or {}
        tr_type = (tr.get("type") or "step_pct").lower()
        atr_period = int(tr.get("atr_period", 14))
        atr_mult = float(tr.get("atr_mult", 2.5))
        step_pct = float(tr.get("step_pct", 0.1))
        if tr_type == "atr" and callable(self._get_atr):
            try:
                atr_val = self._get_atr(symbol, atr_period)
                if atr_val and atr_val > 0:
                    return atr_mult * float(atr_val)
            except Exception:
                pass
        # fallback: step_pct
        return float(last_price) * (step_pct / 100.0)

    # ---------- BOYUTLANDIRMA (OrderManager çağırabilir) ----------
    def position_size(
        self,
        symbol: str,
        side: str,
        *,
        last_price: Optional[float] = None,
        risk_pct: Optional[float] = None
    ) -> float:
        """
        Qty hesaplar. Adımlar:
          1) Equity * risk_pct
          2) Stop mesafesi (ATR/step) ile böl
          3) Normalizer ile qty yuvarla ve minNotional'i sağlamaya çalış
        """
        # 1) equity ve risk
        equity = float(self.portfolio.equity())
        if equity <= 0:
            return 0.0
        use_risk = float(risk_pct/100.0) if risk_pct is not None else self.per_trade_risk_pct
        risk_cap = equity * use_risk
        if risk_cap <= 0:
            return 0.0

        # 2) stop distance
        if not last_price or last_price <= 0:
            return 0.0
        stop_dist = self._stop_distance_from_trailing(symbol, float(last_price))
        if not stop_dist or stop_dist <= 0:
            return 0.0

        qty = risk_cap / float(stop_dist)
        if qty <= 0:
            return 0.0

        # 3) normalizer ile adım/minNotional
        if self.normalizer:
            # qty adımına yuvarla
            qty = self.normalizer.normalize_qty(symbol, qty)
            if qty <= 0:
                return 0.0
            # MARKET için minNotional — last_price ile kontrol et
            qty = self.normalizer.ensure_min_notional(symbol, float(last_price), qty)

        return float(qty)

    # ---------- STRATEJİDEN GELEN SİNYAL İLE PLAN ----------
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

        # Emir türleri fallback parametreleri
        order_cfg: Dict[str, Any] = self.order_cfg or {}
        sl_pct = float(order_cfg.get("sl_pct", 1.0))    # %1 varsayılan
        tp_rr  = float(order_cfg.get("tp_rr", 1.5))     # RR=1.5 varsayılan

        # Strategy sinyalinde opsiyonel SL/TP gelmiş olabilir; yoksa fallback üret
        last_close = getattr(signal, "entry_price", None) or getattr(signal, "close", None) or 100.0
        entry, sl, tp = self._fallback_entry_sl_tp(signal.side, float(last_close), sl_pct, tp_rr)

        stop_distance = abs(entry - sl)
        if stop_distance <= 0:
            return TradePlan(ok=False, reason="invalid_stop_distance")

        # Boyut (qty)
        qty = risk_cap / stop_distance
        if qty <= 0:
            return TradePlan(ok=False, reason="qty_zero")

        # Min notional buffer (paper koruması)
        notional = entry * qty
        min_notional = 5.0  # paper için muhafazakar default; gerçek filtre ile değişecek
        if notional < min_notional * self.min_notional_buffer:
            # qty'yi yükseltmek yerine güvenli tarafta reddedelim (paper)
            return TradePlan(ok=False, reason=f"min_notional_violation wanted={notional:.2f} min~{min_notional*self.min_notional_buffer:.2f}")

        # --- NORMALİZASYON: qty/price adımlarına oturt ---
        if self.normalizer:
            try:
                # entry normalizasyonu (tick)
                entry_n = self.normalizer.normalize_price(symbol, entry)
                # qty normalizasyonu (step)
                qty_n = self.normalizer.normalize_qty(symbol, qty)
                # minNotional (LIMIT için price; MARKET'te last_price gerek; plan aşamasında entry kabul)
                qty_n = self.normalizer.ensure_min_notional(symbol, entry_n, qty_n)
                # uygunsa kullan
                if qty_n > 0:
                    qty = qty_n
                    entry = entry_n
            except Exception:
                # normalizer sorun çıkarırsa plan yine de dönsün; router son kapı kontrol yapar
                pass

        # Paper için kaba quantize (ek koruma; normalizer yoksa devrede)
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
