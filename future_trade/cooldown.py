# /opt/tradebot/future_trade/cooldown.py
from __future__ import annotations
from typing import Dict, Any, Tuple
import math
import time

from .strategy.indicators import atr, adx  # mevcut dosyandan

TF_SECONDS = {"15m": 900, "1h": 3600, "4h": 14400, "1d": 86400}

def _clamp(x: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, x))

def _lerp(value: float, v_lo: float, v_hi: float, f_lo: float, f_hi: float) -> float:
    """value∈[v_lo,v_hi] aralığını faktör∈[f_lo,f_hi] aralığına doğrusal eşle."""
    if v_hi <= v_lo:
        return f_lo
    t = (value - v_lo) / (v_hi - v_lo)
    t = _clamp(t, 0.0, 1.0)
    return f_lo + t * (f_hi - f_lo)

async def _klines(client, symbol: str, interval: str, limit: int):
    try:
        return await client.get_klines(symbol, interval=interval, limit=limit)
    except Exception:
        return []

def _series_from_kl(kl):
    closes = [float(k[4]) for k in kl]
    highs  = [float(k[2]) for k in kl]
    lows   = [float(k[3]) for k in kl]
    return highs, lows, closes

async def volatility_factor(client, symbol: str, tf: str, vol_cfg: Dict[str, Any]) -> float:
    lb = int(vol_cfg.get("lookback_bars", 14))
    v_lo = float(vol_cfg.get("natr_low", 0.01))
    v_hi = float(vol_cfg.get("natr_high", 0.05))
    f_lo = float(vol_cfg.get("factor_low", 1.3))
    f_hi = float(vol_cfg.get("factor_high", 0.7))
    kl = await _klines(client, symbol, tf, max(lb + 2, 20))
    if len(kl) < lb + 1:
        return 1.0
    h, l, c = _series_from_kl(kl)
    a = atr(h, l, c, period=lb)
    if not a or math.isnan(a):
        return 1.0
    last_close = c[-1]
    natr = a / last_close  # ~ or a / mean(c)
    return _lerp(natr, v_lo, v_hi, f_lo, f_hi)

def _count_recent_entries_fallback(persistence, symbol: str, since_ts: int) -> int:
    """Çeşitli tablo şemalarına uyumlu, en azından 0 döner."""
    try:
        # 1) signal_audit (decision=1 başarıyla alınmış giriş kabul edelim)
        conn = persistence._conn()
        cur = conn.cursor()
        cur.execute("""
            SELECT COUNT(1) FROM signal_audit
             WHERE symbol=? AND decision=1 AND ts>=?
        """, (symbol, since_ts))
        row = cur.fetchone()
        conn.close()
        if row and row[0] is not None:
            return int(row[0])
    except Exception:
        pass
    try:
        # 2) futures_orders (entry clientOrderId prefix'iyle saymayı deneyelim)
        conn = persistence._conn()
        cur = conn.cursor()
        cur.execute("""
            SELECT COUNT(1) FROM futures_orders
             WHERE symbol=? AND created_at>=?
        """, (symbol, since_ts))
        row = cur.fetchone()
        conn.close()
        if row and row[0] is not None:
            return int(row[0])
    except Exception:
        pass
    return 0

async def frequency_factor(persistence, symbol: str, tf: str, freq_cfg: Dict[str, Any]) -> float:
    window_bars = int(freq_cfg.get("window_bars", 50))
    max_trades  = int(freq_cfg.get("max_trades", 5))
    f_lo = float(freq_cfg.get("factor_low", 0.9))
    f_hi = float(freq_cfg.get("factor_high", 1.5))
    tf_sec = TF_SECONDS.get(tf.lower(), 3600)
    since_ts = int(time.time()) - window_bars * tf_sec
    cnt = _count_recent_entries_fallback(persistence, symbol, since_ts)
    # 0 → f_lo, >=max_trades → f_hi
    return _lerp(float(cnt), 0.0, float(max_trades), f_lo, f_hi)

async def signal_factor(client, symbol: str, tf: str, sig_cfg: Dict[str, Any]) -> float:
    # ADX: düşük→uzun, yüksek→kısa cooldown
    metric = (sig_cfg.get("metric") or "adx").lower()
    period = int(sig_cfg.get("period", 14))
    lo = float(sig_cfg.get("low", 18.0))
    hi = float(sig_cfg.get("high", 30.0))
    f_lo = float(sig_cfg.get("factor_low", 1.2))
    f_hi = float(sig_cfg.get("factor_high", 0.85))
    if metric != "adx":
        return 1.0
    kl = await _klines(client, symbol, tf, max(period + 20, 60))
    if len(kl) < period + 10:
        return 1.0
    h, l, c = _series_from_kl(kl)
    val = adx(h, l, c, period=period)
    if not val or math.isnan(val):
        return 1.0
    return _lerp(val, lo, hi, f_lo, f_hi)

def tf_factor(tf: str, scale_map: Dict[str, float]) -> float:
    return float(scale_map.get(tf.lower(), 1.0))

async def compute_dynamic_cooldown_sec(
    cfg: Dict[str, Any],
    client,
    persistence,
    symbol: str,
    tf_entry: str
) -> int:
    cd = cfg.get("cooldown", {}) or {}
    if cd.get("mode", "dynamic").lower() != "dynamic":
        # Fallback: static base or default 1800
        base = int(cd.get("base_sec", 1800))
        mn = int(cd.get("min_sec", 300))
        mx = int(cd.get("max_sec", 7200))
        return int(_clamp(base, mn, mx))

    base = float(cd.get("base_sec", 1800))
    mn = float(cd.get("min_sec", 300))
    mx = float(cd.get("max_sec", 7200))

    tf_sc   = tf_factor(tf_entry, cd.get("tf_scale", {}))
    vol_f   = await volatility_factor(client, symbol, tf_entry, cd.get("volatility", {}))
    freq_f  = await frequency_factor(persistence, symbol, tf_entry, cd.get("frequency", {}))
    sig_f   = await signal_factor(client, symbol, tf_entry, cd.get("signal", {}))

    sec = base * tf_sc * vol_f * freq_f * sig_f
    return int(_clamp(sec, mn, mx))
