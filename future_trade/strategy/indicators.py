# /opt/tradebot/future_trade/strategy/indicators.py
from typing import List
from math import isnan

def ema(values: List[float], period: int) -> float:
    if not values: return 0.0
    k = 2.0 / (period + 1.0)
    e = values[0]
    for v in values[1:]:
        e = v * k + e * (1.0 - k)
    return e

def rsi(closes: List[float], period: int = 14) -> float:
    if len(closes) < period + 1: return 50.0
    gains = []; losses = []
    for i in range(1, period + 1):
        diff = closes[i] - closes[i-1]
        gains.append(max(diff, 0.0)); losses.append(max(-diff, 0.0))
    avg_gain = sum(gains) / period
    avg_loss = sum(losses) / period
    for i in range(period + 1, len(closes)):
        diff = closes[i] - closes[i-1]
        gain = max(diff, 0.0)
        loss = max(-diff, 0.0)
        avg_gain = (avg_gain * (period - 1) + gain) / period
        avg_loss = (avg_loss * (period - 1) + loss) / period
    if avg_loss == 0: return 100.0
    rs = avg_gain / avg_loss
    return 100.0 - (100.0 / (1.0 + rs))

def true_range(highs: List[float], lows: List[float], closes: List[float]) -> List[float]:
    trs = []
    for i in range(len(closes)):
        if i == 0:
            trs.append(highs[i] - lows[i])
        else:
            tr = max(highs[i] - lows[i], abs(highs[i] - closes[i-1]), abs(lows[i] - closes[i-1]))
            trs.append(tr)
    return trs

def atr(highs: List[float], lows: List[float], closes: List[float], period: int = 14) -> float:
    trs = true_range(highs, lows, closes)
    if len(trs) < period: return 0.0
    a = sum(trs[:period]) / period
    for t in trs[period:]:
        a = (a * (period - 1) + t) / period
    return a

def adx(highs: List[float], lows: List[float], closes: List[float], period: int = 14) -> float:
    if len(closes) < period + 2: return 0.0
    pdm = [0.0]; ndm = [0.0]
    for i in range(1, len(closes)):
        up = highs[i] - highs[i-1]
        dn = lows[i-1] - lows[i]
        pdm.append(up if up > dn and up > 0 else 0.0)
        ndm.append(dn if dn > up and dn > 0 else 0.0)
    tr = true_range(highs, lows, closes)
    # Wilder smoothing
    def wilder(series, p):
        s = [0.0]*len(series)
        s[p-1] = sum(series[:p])
        for i in range(p, len(series)):
            s[i] = s[i-1] - (s[i-1]/p) + series[i]
        return s
    ps = wilder(pdm, period)
    ns = wilder(ndm, period)
    trs = wilder(tr, period)
    dxs = []
    for i in range(period-1, len(closes)):
        if trs[i] == 0: 
            dxs.append(0.0); continue
        pdi = 100.0 * (ps[i] / trs[i])
        ndi = 100.0 * (ns[i] / trs[i])
        denom = pdi + ndi
        dxs.append(0.0 if denom == 0 else 100.0 * abs(pdi - ndi) / denom)
    if len(dxs) < period: return 0.0
    adx_val = sum(dxs[:period]) / period
    for d in dxs[period:]:
        adx_val = (adx_val * (period - 1) + d) / period
    return adx_val

    def adx_placeholder(series: List[float], period: int = 14) -> float:
        # gerçek ADX değil; şimdilik sabit/orta değer dönelim
        return 25.0