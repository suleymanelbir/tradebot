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
        self._init_balance_cache()      # ACCOUNT_UPDATE balance cache
        # bracket cache: {SYMBOL: {"ts": int, "brackets": list}}
        self._br_cache = {} 

    # ----- ACCOUNT_UPDATE balance cache -----
    def _init_balance_cache(self):
        self._balance_cache = {"available": None, "ts": 0}

    def update_balance_cache(self, available: float, ts: int | None = None):
        """
        UDS ACCOUNT_UPDATE'tan gelen available (approx) bakiyeyi cache'le.
        """
        if not hasattr(self, "_balance_cache"):
            self._init_balance_cache()
        import time as _t
        self._balance_cache["available"] = float(available)
        self._balance_cache["ts"] = int(ts or _t.time())


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


        # ------------- Margin Guard Helpers -----------------
    def bind_client(self, client):
        self._client = client

    def bind_margin_cfg(self, mg_cfg: dict | None):
        self._mg = mg_cfg or {}
        self._mg.setdefault("enabled", True)
        self._mg.setdefault("reserve_pct", 0.10)
        self._mg.setdefault("min_free_usdt", 15.0)
        self._mg.setdefault("fee_rate", 0.0005)
        self._mg.setdefault("slippage_pct", 0.001)
        self._mg.setdefault("mm_buffer_pct", 0.05)
        self._mg.setdefault("allow_reduce_only_override", True)
        self._mg.setdefault("use_account_ws", True)
        self._mg.setdefault("stale_timeout_sec", 60)
        self._mg.setdefault("bracket_cache_ttl_sec", 3600) 
        

    def _notional(self, price: float, qty: float) -> float:
        return max(0.0, float(price) * float(qty))

    def _est_fees(self, notional: float) -> float:
        fee = float(self._mg.get("fee_rate", 0.0005))
        return notional * fee

#    async def _maint_margin_ratio(self, symbol: str, notional: float, lev: int) -> float:
#        """
#        Leverage bracket'tan approx MMR bul. Basit yaklaşım: ilk bracket'ın oranını kullan,
#        ya da notional'a en yakın bracket'ı seç.
#        """
#        client = getattr(self, "_client", None)
#        if not client:
#            return 0.004  # güvenli varsayım (~0.4%)
#        try:
#            data = await client.get_leverage_brackets(symbol)
#            # response bazen list of obj olabilir
#            if isinstance(data, list) and data and "brackets" in data[0]:
#                brackets = data[0]["brackets"]
#            elif isinstance(data, dict) and "brackets" in data:
#                brackets = data["brackets"]
#            else:
#                return 0.004
#            # en basit seçim: ilk bracket
#            mmr = float(brackets[0].get("maintMarginRatio", 0.004))
#            return max(0.0, mmr)
#        except Exception:
#            return 0.004

    async def suggest_affordable_qty(
        self,
        symbol: str,
        side: str,
        desired_qty: float,
        price: float,
        leverage: int,
        reduce_only: bool = False,
    ) -> tuple[float, str]:
        """
        Emir öncesi teminat kontrolü:
        - availableBalance, reserve_pct, min_free_usdt
        - initial margin ≈ notional/leverage
        - maint margin ≈ notional * mmr (bracket-aware)
        - fees + slippage + buffer
        Döner: (uygun_qty, reason)
        """

        # 1) ReduceOnly emirler için override kontrolü
        if reduce_only and self._mg.get("allow_reduce_only_override", True):
            return float(desired_qty), "reduce_only_override"

        # 2) Binance client erişimi kontrolü
        client = getattr(self, "_client", None)
        if not client:
            return float(desired_qty), "no_client"

        # 3) availableBalance hesaplama: önce WS cache, sonra REST fallback
        use_ws = bool(self._mg.get("use_account_ws", True))
        stale_sec = int(self._mg.get("stale_timeout_sec", 60))
        avail = None

        try:
            # 3.1) WS cache varsa ve tazeyse kullan
            bc = getattr(self, "_balance_cache", None) or {}
            if use_ws and bc.get("available") is not None and bc.get("ts"):
                import time as _t
                age = _t.time() - float(bc["ts"])
                if age <= stale_sec:
                    avail = float(bc["available"])
                    self._log and self._log.debug(f"[MG] using WS cache avail={avail:.4f} age={age:.1f}s")
        except Exception:
            pass

        if avail is None:
            # 3.2) REST fallback: get_account ile güncel veriyi al
            try:
                acct = await client.get_account()
                avail = float(acct.get("availableBalance") or acct.get("totalWalletBalance") or 0.0)
                self._log and self._log.debug(f"[MG] using REST avail={avail:.4f}")
                try:
                    self.update_balance_cache(avail)  # cache güncelle
                except Exception:
                    pass
            except Exception:
                avail = 0.0

        # 4) Risk parametrelerini al
        reserve = avail * float(self._mg.get("reserve_pct", 0.10))
        min_free = float(self._mg.get("min_free_usdt", 15.0))
        fee_rate = float(self._mg.get("fee_rate", 0.0005))
        slip = float(self._mg.get("slippage_pct", 0.001))
        mm_buf = float(self._mg.get("mm_buffer_pct", 0.05))

        # 5) Slippage ile fiyatı kötüleştir
        px = float(price) * (1.0 + slip if side.upper() == "BUY" else 1.0 - slip)
        lev = max(1, int(leverage))

        # 6) Binary search için başlangıç değerleri
        lo, hi = 0.0, float(desired_qty)
        ok_qty = 0.0
        reason = "ok"
        self.logger.debug(f"[MG] fee_rate={fee_rate}, initial_reason={reason}")

        # 7) Bracket'ları bir kez çek (her qty için yeniden hesaplanacak)
        brackets = await self._get_brackets(symbol)

        # 8) Margin ihtiyacını hesaplayan yardımcı fonksiyon (bracket-aware MMR ile)
        def need_margin(qty: float) -> float:
            notional = self._notional(px, qty)
            mmr = self._mmr_from_brackets(brackets, notional) if brackets else 0.004
            im = notional / lev
            mm = notional * mmr * (1.0 + mm_buf)
            fees = self._est_fees(notional)
            return im + mm + fees

        # 9) Kullanılabilir bütçeyi hesapla
        budget = max(0.0, avail - reserve)
        if budget < min_free:
            return 0.0, f"insufficient_free_balance: free<{min_free}usdt"

        # 10) Desired qty zaten bütçeye sığıyorsa direkt döndür
        if need_margin(hi) <= budget:
            return hi, "within_budget"

        # 11) Binary search ile en yüksek uygun qty’yi bul
        for _ in range(24):
            mid = (lo + hi) / 2.0
            if mid <= 0:
                break
            req = need_margin(mid)
            if req <= budget:
                ok_qty = mid
                lo = mid
            else:
                hi = mid

        # 12) Sonuç döndür
        if ok_qty <= 0:
            return 0.0, "no_affordable_qty"

        return ok_qty, "shrunk_to_fit"

    # ---- Leverage Bracket Tabanlı MMR Hesaplama (Binance uyumlu) ----
    async def _get_brackets(self, symbol: str) -> list[dict]:
        """
        Bracket listesini TTL ile önbellekten getirir. Yapı örneği (her eleman):
          {"notionalCap": "5000", "notionalFloor": "0", "maintMarginRatio": "0.004", ...}
        """
        import time as _t
        ttl = int(self._mg.get("bracket_cache_ttl_sec", 3600))
        now = int(_t.time())
        # cache hit?
        b = self._br_cache.get(symbol)
        if b and (now - int(b.get("ts", 0)) < ttl) and b.get("brackets"):
            return b["brackets"]

        client = getattr(self, "_client", None)
        if not client:
            return []

        try:
            raw = await client.get_leverage_brackets(symbol)
            # Binance bazen list döner; normalize et
            if isinstance(raw, list):
                # Futures genelde: [{"symbol": "...", "brackets":[...]}]
                if raw and isinstance(raw[0], dict) and "brackets" in raw[0]:
                    brackets = raw[0]["brackets"]
                else:
                    brackets = []
            elif isinstance(raw, dict) and "brackets" in raw:
                brackets = raw["brackets"]
            else:
                brackets = []
            # sayısallaştır ve sırala
            norm = []
            for r in brackets:
                try:
                    floor_ = float(r.get("notionalFloor", 0))
                    cap_   = float(r.get("notionalCap", float("inf")))
                    mmr_   = float(r.get("maintMarginRatio", 0.004))
                    norm.append({"floor": floor_, "cap": cap_, "mmr": max(0.0, mmr_)})
                except Exception:
                    continue
            norm.sort(key=lambda x: x["floor"])
            # cache set
            self._br_cache[symbol] = {"ts": now, "brackets": norm}
            return norm
        except Exception:
            return []

    @staticmethod
    def _mmr_from_brackets(brackets: list[dict], notional: float) -> float:
        """
        Verilen notional için uygun bracket'ı bul ve mmr döndür.
        Kapsama: floor <= notional <= cap; bulunamazsa son bracket'ın mmr'ı.
        """
        if not brackets:
            return 0.004
        for b in brackets:
            if notional >= b["floor"] and notional <= b["cap"]:
                return float(b["mmr"])
        # eğer cap'leri aştıysa sonuncuyu kullan
        return float(brackets[-1]["mmr"])
