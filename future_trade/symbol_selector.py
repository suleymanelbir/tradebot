# /opt/tradebot/future_trade/symbol_selector.py
# -*- coding: utf-8 -*-
from __future__ import annotations
import math, time, asyncio, logging, statistics
from typing import Dict, List, Tuple, Any, Optional

class SymbolSelector:
    """
    Dinamik, yön-bazlı sembol seçici:
      - Trend/Momentum/Vol fit/Liq/Clean skorları
      - Funding bias: 'dynamic' (carry vs contrarian seçimi rejime göre), 'carry', 'contrarian'
      - Korelasyon penaltısı + hard exclude eşiği
      - Dinamik ağırlıklar (trend/chop rejimine göre)
      - Dinamik TopK (piyasa oynaklığına göre)
      - Cooldown ve annotated rapor üretimi
    Gereken yardımcılar (varsa kullanır, yoksa güvenli düşer):
      * KlinesCache: get_atr, get_ema, get_adx, get_roc, get_klines(symbol, tf, limit=..)
      * MarketStream: get_last_price(symbol)
      * BinanceClient: exchange_info(), depth(), ticker_24hr(), funding_rate() (opsiyonel)
    """

    def __init__(self, cfg: dict, client, klines_cache, market_stream, logger: Optional[logging.Logger] = None):
        self.root_cfg = cfg or {}
        self.cfg = (cfg or {}).get("dynamic_universe", {}) or {}
        self.client = client
        self.kl = klines_cache
        self.stream = market_stream
        self.log = logger or logging.getLogger("symbol_selector")

        self.active_long: List[str] = []
        self.active_short: List[str] = []

        self._cooldown_until: Dict[str, float] = {}
        self._last_scan_ts: float = 0.0

        self._prev_long: List[str] = []
        self._prev_short: List[str] = []

        # Dyn weights smoothing
        self._last_weights: Dict[str, float] = {}

    # ---------- Public API ----------
    def get_active(self, side: str) -> List[str]:
        side = side.upper()
        return list(self.active_long if side == "LONG" else self.active_short)

    def ban_symbol(self, symbol: str, sec: int) -> None:
        self._cooldown_until[symbol] = time.time() + max(0, sec)

    # ---------- Universe & data ----------
    async def _load_universe(self) -> List[str]:
        uni = self.cfg.get("universe", "auto_usdt_perp")
        if isinstance(uni, list):
            return [s.upper() for s in uni]
        # auto
        try:
            ex = await self.client.exchange_info()
            out = []
            for s in ex.get("symbols", []):
                if s.get("contractType") == "PERPETUAL" and s.get("quoteAsset") == "USDT" and s.get("status") == "TRADING":
                    out.append(str(s["symbol"]).upper())
            return out
        except Exception as e:
            self.log.warning(f"universe load failed: {e}")
            return []

    async def _load_liquidity(self, symbols: List[str]) -> Dict[str, Dict[str, float]]:
        res: Dict[str, Dict[str, float]] = {}
        # 24h quote volume
        get24 = getattr(self.client, "ticker_24hr", None)
        if callable(get24):
            try:
                data = await get24()
                m = {d["symbol"]: d for d in data} if isinstance(data, list) else {}
                for s in symbols:
                    d = m.get(s) or {}
                    res.setdefault(s, {})["quoteVolume"] = float(d.get("quoteVolume") or 0.0)
            except Exception:
                for s in symbols: res.setdefault(s, {})["quoteVolume"] = 0.0
        # spread
        for s in symbols:
            try:
                ob = await self.client.depth(symbol=s, limit=5)
                bids = ob.get("bids") or []; asks = ob.get("asks") or []
                if bids and asks:
                    bid = float(bids[0][0]); ask = float(asks[0][0]); mid = 0.5*(bid+ask)
                    sp_bps = ((ask - bid)/mid)*1e4 if mid>0 else 9999.0
                else:
                    sp_bps = 9999.0
            except Exception:
                sp_bps = 9999.0
            res.setdefault(s, {})["spread_bps"] = sp_bps
        return res

    def _natr(self, symbol: str, tf: str = "1d", period: int = 14) -> float:
        try:
            atr = self.kl.get_atr(symbol, tf, period)
            last = self.stream.get_last_price(symbol)
            return float(atr/last) if (atr and last) else 0.0
        except Exception:
            return 0.0

    def _trend_strength(self, symbol: str, tf: str) -> Tuple[float, float]:
        try:
            ema20 = self.kl.get_ema(symbol, tf, 20)
            ema50 = self.kl.get_ema(symbol, tf, 50)
            adx = self.kl.get_adx(symbol, tf, 14)
            slope = (ema20 - ema50)/ema50 if (ema20 and ema50 and ema50!=0) else 0.0
            adx_n = min(1.0, max(0.0, (adx or 0)/50.0))
            long_sc = min(1.0, max(0.0, slope*5.0))*(0.5+0.5*adx_n)
            short_sc= min(1.0, max(0.0, -slope*5.0))*(0.5+0.5*adx_n)
            return (long_sc, short_sc)
        except Exception:
            return (0.0, 0.0)

    def _momentum(self, symbol: str, tf: str) -> Tuple[float, float]:
        try:
            roc = self.kl.get_roc(symbol, tf, 10)  # ~% değişim
            if roc is None: return (0.0, 0.0)
            long_sc = min(1.0, max(0.0, roc/0.05))
            short_sc= min(1.0, max(0.0, -roc/0.05))
            return (long_sc, short_sc)
        except Exception:
            return (0.0, 0.0)

    # ---------- Funding ----------
    async def _funding_bias(self, symbol: str) -> float:
        """
        + → long lehine bias, - → short lehine bias gibi kullanılabilir.
        Burada sadece funding değerini çekip küçük bir normalize döneriz.
        """
        fr = 0.0
        get = getattr(self.client, "funding_rate", None)
        try:
            if callable(get):
                # beklenen: funding_rate(symbol) → {'lastFundingRate': '0.0001', ...} veya float
                x = await get(symbol)
                if isinstance(x, dict):
                    fr = float(x.get("lastFundingRate") or 0.0)
                else:
                    fr = float(x or 0.0)
        except Exception:
            fr = 0.0
        # nazik normalize
        # funding ~ %0.01 = 0.0001 → 0.1 ölçeğinde tanh
        return math.tanh(fr * 100.0)

    # ---------- Correlation ----------
    def _corr(self, a: List[float], b: List[float]) -> float:
        # basit pearson
        if not a or not b: return 0.0
        n = min(len(a), len(b))
        a = a[-n:]; b = b[-n:]
        ma = statistics.fmean(a); mb = statistics.fmean(b)
        da = [x-ma for x in a]; db = [y-mb for y in b]
        sa = math.sqrt(sum(x*x for x in da)); sb = math.sqrt(sum(y*y for y in db))
        if sa==0 or sb==0: return 0.0
        return sum(x*y for x,y in zip(da,db))/(sa*sb)

    def _series(self, symbol: str, tf: str, lb: int) -> Optional[List[float]]:
        try:
            kl = self.kl.get_klines(symbol, tf, lb+1)
            if not kl or len(kl)<2: return None
            closes = [float(o["close"]) if isinstance(o, dict) else float(o[4]) for o in kl]
            # log returns
            ret = []
            for i in range(1, len(closes)):
                if closes[i-1]>0:
                    ret.append(math.log(closes[i]/closes[i-1]))
            return ret[-lb:]
        except Exception:
            return None

    # ---------- Regime & dynamic weights ----------
    def _regime_weights(self) -> Tuple[str, Dict[str,float]]:
        base = self.cfg.get("scoring",{}).get("weights_base", {})
        w_tr = self.cfg.get("scoring",{}).get("weights_trend_regime", {})
        w_ch = self.cfg.get("scoring",{}).get("weights_chop_regime", {})

        # BTC/ETH ile basit rejim: 4h ADX ve 1h/4h NATR
        try:
            adx_b = self.kl.get_adx("BTCUSDT", "4h", 14) or 0
            adx_e = self.kl.get_adx("ETHUSDT", "4h", 14) or 0
            adx_avg = (adx_b + adx_e)/2.0
            natr_b = self._safe_natr("BTCUSDT"); natr_e = self._safe_natr("ETHUSDT")
            vol_avg = (natr_b + natr_e)/2.0
        except Exception:
            adx_avg, vol_avg = 15.0, 0.015

        # basit kural: ADX > 20 trend; yoksa chop ağırlıkları
        regime = "trend" if adx_avg >= 20 else "chop"
        target = w_tr if regime=="trend" else w_ch
        # smooth: EMA(0.6)
        out = {}
        for k in ("trend","momentum","vol_fit","liquidity","clean"):
            new = float(target.get(k, base.get(k, 0.2)))
            old = float(self._last_weights.get(k, new))
            sm = 0.6*new + 0.4*old
            out[k] = sm
        self._last_weights = dict(out)
        return regime, out

    def _safe_natr(self, sym: str) -> float:
        try: return float(self._natr(sym, "1d", 14) or 0.0)
        except: return 0.0

    # ---------- Dynamic TopK ----------
    def _dynamic_topk(self, base_k: int) -> int:
        dcfg = (self.cfg.get("scoring") or {}).get("dynamic_topk") or {}
        if not dcfg.get("enabled", True): return base_k
        min_k = int(dcfg.get("min_k", 1)); max_k = int(dcfg.get("max_k", 5))

        # piyasa vol metriği: BTC/ETH 1d NATR ortalaması
        v = (self._safe_natr("BTCUSDT") + self._safe_natr("ETHUSDT"))/2.0
        # 0.8% → min_k, 6% → max_k arası lineer
        lo = float((self.cfg.get("filters") or {}).get("min_natr", 0.008))
        hi = float((self.cfg.get("filters") or {}).get("max_natr", 0.06))
        if v <= lo: return min_k
        if v >= hi: return max_k
        r = (v - lo) / (hi - lo)
        k = int(round(min_k + r*(max_k - min_k)))
        return max(min_k, min(max_k, k))

    # ---------- MAIN SCAN ----------
    async def scan_once(self) -> Dict[str, Any]:
        if not self.cfg.get("enabled", True):
            return {"long": [], "short": [], "topk": {"long": 0, "short": 0}, "change": False}

        entry_tf   = self.cfg.get("scoring",{}).get("entry_tf","1h")
        confirm_tf = self.cfg.get("scoring",{}).get("confirm_tf","4h")
        filt = self.cfg.get("filters",{}) or {}
        min_qv = float(filt.get("min_quote_volume_24h", 5_000_000))
        max_sp = float(filt.get("max_spread_bps", 8))
        natr_lo = float(filt.get("min_natr", 0.008))
        natr_hi = float(filt.get("max_natr", 0.06))
        min_score = float(self.cfg.get("scoring",{}).get("min_score", 0.55))

        base_topk_long = int(self.cfg.get("scoring",{}).get("topk_long", 2))
        base_topk_short= int(self.cfg.get("scoring",{}).get("topk_short", 2))
        topkL = self._dynamic_topk(base_topk_long)
        topkS = self._dynamic_topk(base_topk_short)

        universe = await self._load_universe()
        liq = await self._load_liquidity(universe)
        regime, W = self._regime_weights()

        funding_cfg = self.cfg.get("funding",{}) or {}
        f_policy = str(funding_cfg.get("policy","dynamic")).lower()
        alpha = float(funding_cfg.get("alpha",0.15))

        corr_cfg = self.cfg.get("correlation",{}) or {}
        corr_on = bool(corr_cfg.get("enabled", True))
        corr_tf = str(corr_cfg.get("tf","1h"))
        corr_lb = int(corr_cfg.get("lookback_bars", 48))
        rho_thr = float(corr_cfg.get("rho_threshold", 0.75))
        rho_hard= float(corr_cfg.get("hard_exclude_threshold", 0.90))
        beta    = float(corr_cfg.get("beta",0.35))

        now = time.time()

        cand_long: List[Tuple[str, float, Dict[str,float]]] = []
        cand_short: List[Tuple[str, float, Dict[str,float]]] = []

        for s in universe:
            if self._cooldown_until.get(s, 0) > now:
                continue

            qv = float(liq.get(s,{}).get("quoteVolume") or 0.0)
            sp = float(liq.get(s,{}).get("spread_bps") or 9999)
            if qv < min_qv or sp > max_sp:
                continue

            natr = self._natr(s, "1d", 14)
            if natr <= 0: continue
            if natr < natr_lo or natr > natr_hi:
                # “tamamıyla” eleme yerine hafif kırpma da yapılabilir.
                pass

            tL_e, tS_e = self._trend_strength(s, entry_tf)
            tL_c, tS_c = self._trend_strength(s, confirm_tf)
            tL = 0.6*tL_e + 0.4*tL_c
            tS = 0.6*tS_e + 0.4*tS_c

            mL, mS = self._momentum(s, entry_tf)

            # vol fit
            if natr <= 0: vol_fit = 0.0
            elif natr < natr_lo: vol_fit = (natr/natr_lo)*0.6
            elif natr > natr_hi: vol_fit = (natr_hi/natr)*0.6
            else: vol_fit = 1.0

            # liq score (qv & spread)
            liq_sc = min(1.0, max(0.0, (math.log10(qv+1)-5)/2.0))
            sp_pen = min(1.0, max(0.0, 1.0 - sp/max_sp))
            LQ = 0.7*liq_sc + 0.3*sp_pen

            clean = 1.0  # ileride wick/gap bayraklarıyla düşürülebilir

            # funding düzeltmesi
            f_adj = 0.0
            try:
                fr = await self._funding_bias(s)  # ~[-1..+1] küçük
            except Exception:
                fr = 0.0

            if f_policy == "carry":
                # funding ile aynı yönü destekle
                f_long = max(0.0, fr); f_short = max(0.0, -fr)
                adjL = 1.0 + alpha * f_long
                adjS = 1.0 + alpha * f_short
            elif f_policy == "contrarian":
                # funding kalabalık yönünü kırp
                f_long = max(0.0, fr); f_short = max(0.0, -fr)
                adjL = 1.0 - alpha * f_long
                adjS = 1.0 - alpha * f_short
            else:
                # 'dynamic': trend güçlü & momentum yüksekse carry, yoksa contrarian
                # basit karar: (tL/tS ve mL/mS)
                trend_strength = max(tL, tS)
                mom_strength   = max(mL, mS)
                strong = (trend_strength >= 0.6) and (mom_strength >= 0.5)
                if strong:
                    # carry
                    f_long = max(0.0, fr); f_short = max(0.0, -fr)
                    adjL = 1.0 + alpha * f_long
                    adjS = 1.0 + alpha * f_short
                else:
                    # contrarian
                    f_long = max(0.0, fr); f_short = max(0.0, -fr)
                    adjL = 1.0 - alpha * f_long
                    adjS = 1.0 - alpha * f_short

            # ağırlıklı skor
            scL_raw = W["trend"]*tL + W["momentum"]*mL + W["vol_fit"]*vol_fit + W["liquidity"]*LQ + W["clean"]*clean
            scS_raw = W["trend"]*tS + W["momentum"]*mS + W["vol_fit"]*vol_fit + W["liquidity"]*LQ + W["clean"]*clean

            scL = scL_raw * adjL
            scS = scS_raw * adjS

            comp = {"trend":tL, "momentum":mL, "vol_fit":vol_fit, "liquidity":LQ, "clean":clean,
                    "funding": fr, "adjL":adjL, "adjS":adjS, "rawL":scL_raw, "rawS":scS_raw}

            if scL >= min_score: cand_long.append((s, scL, comp))
            if scS >= min_score: cand_short.append((s, scS, comp))

        # skor sıralaması
        cand_long.sort(key=lambda x: x[1], reverse=True)
        cand_short.sort(key=lambda x: x[1], reverse=True)

        # --- Korelasyon penaltısı / hard exclude ile greedy seçim ---
        def build_pool(cands: List[Tuple[str,float,dict]], K: int) -> List[Tuple[str,float,dict]]:
            if not corr_on or K<=0: return cands[:K]
            out: List[Tuple[str,float,dict]] = []
            series_cache: Dict[str, List[float] | None] = {}
            for (sym, sc, comp) in cands:
                ok_sc = sc
                hard_block = False
                # mevcut seçilenlerle korelasyon
                for (s2,_,_) in out:
                    # serileri getir
                    series_cache.setdefault(sym, self._series(sym, corr_tf, corr_lb))
                    series_cache.setdefault(s2,  self._series(s2,  corr_tf, corr_lb))
                    a = series_cache[sym]; b = series_cache[s2]
                    if not a or not b: continue
                    rho = abs(self._corr(a,b))
                    if rho >= rho_hard:
                        hard_block = True; break
                    if rho >= rho_thr:
                        ok_sc *= max(0.0, 1.0 - beta*(rho - rho_thr)/(1.0 - rho_thr))
                if hard_block: 
                    continue
                out.append((sym, ok_sc, comp))
                out.sort(key=lambda x: x[1], reverse=True)
                if len(out)>=K:
                    break
            return out

        poolL = build_pool(cand_long, topkL)
        poolS = build_pool(cand_short, topkS)

        new_long  = [s for s,_,_ in poolL]
        new_short = [s for s,_,_ in poolS]

        added_long = [s for s in new_long if s not in self.active_long]
        removed_long = [s for s in self.active_long if s not in new_long]
        added_short = [s for s in new_short if s not in self.active_short]
        removed_short = [s for s in self.active_short if s not in new_short]

        self._prev_long, self._prev_short = self.active_long, self.active_short
        self.active_long, self.active_short = new_long, new_short
        self._last_scan_ts = now

        # rapor objesi
        rep = {
            "event": "selector_update",
            "regime": regime,
            "weights": W,
            "long": [(s, round(sc,4)) for s,sc,_ in poolL],
            "short": [(s, round(sc,4)) for s,sc,_ in poolS],
            "topk": {"long": topkL, "short": topkS},
            "added_long": added_long,
            "removed_long": removed_long,
            "added_short": added_short,
            "removed_short": removed_short,
            "change": bool(added_long or removed_long or added_short or removed_short),
        }

        # debug log topN
        try:
            topn = int((self.cfg.get("reporting") or {}).get("debug_log_topn", 5))
            self.log.info(f"[SEL] LONG top: {rep['long'][:topn]}")
            self.log.info(f"[SEL] SHORT top: {rep['short'][:topn]}")
            if rep["change"]:
                self.log.info(f"[SEL] Δ added L:{added_long} S:{added_short} | removed L:{removed_long} S:{removed_short}")
        except Exception:
            pass

        return rep
