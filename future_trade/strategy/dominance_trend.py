# /opt/tradebot/future_trade/strategy/dominance_trend.py
"""
Dominance Trend stratejisi (paper akış ile uyumlu basit/sağlam sürüm)

Kurallar (özet):
LONG:
  TOTAL3(4H).close > EMA20(4H)
  AND TOTAL3(1H).close > EMA20(1H)
  AND Symbol(1H).close > EMA20(1H)
  AND RSI(1H) < 60
  AND USDT.D(1H).close < EMA20(1H)
  AND BTC.D(1H).close < EMA20(1H)
  AND ADX >= adx_min

SHORT:
  TOTAL3(4H).close < EMA20(4H)
  AND TOTAL3(1H).close < EMA20(1H)
  AND Symbol(1H).close < EMA20(1H)
  AND USDT.D(1H).close > EMA20(1H)
  AND BTC.D(1H).close > EMA20(1H)
  AND ADX >= adx_min

Notlar:
- MarketStream şu anda paper modda sadece 'close' ve 'ema20' yayımlıyor.
- RSI/ADX bu sınıf içinde, gelen close serisinden yaklaşık hesaplanıyor.
- ADX, OHLC olmadan "trend gücü" proxy'si ile yaklaşık alınır (paper için yeterli).
"""

from collections import deque
from typing import Any, Dict, Deque, List, Optional
import math

from .base import StrategyBase, Signal


def _safe_gt(a: Optional[float], b: Optional[float]) -> bool:
    return (a is not None) and (b is not None) and (a > b)

def _safe_lt(a: Optional[float], b: Optional[float]) -> bool:
    return (a is not None) and (b is not None) and (a < b)

def _rsi(closes: List[float], period: int = 14) -> Optional[float]:
    if len(closes) < period + 1:
        return None
    gains = 0.0
    losses = 0.0
    for i in range(-period, 0):
        diff = closes[i] - closes[i - 1]
        if diff >= 0:
            gains += diff
        else:
            losses -= diff  # diff negatifse azalmanın pozitif büyüklüğü
    avg_gain = gains / period
    avg_loss = losses / period
    if avg_loss == 0:
        return 100.0
    rs = avg_gain / avg_loss
    return 100.0 - (100.0 / (1.0 + rs))

def _adx_proxy(closes: List[float], period: int = 14) -> Optional[float]:
    """
    ADX yerine trend gücü/volatilite temelli yaklaşık bir metrik (0-100 ölçek).
    Paper akışta OHLC olmadığı için close serisinden türetiriz.
    Fikir: normalize edilmiş hareketliliği ölçeklendir.
    """
    n = len(closes)
    if n < period + 2:
        return None
    # Ortalama mutlak değişim ve toplam aralık oranı
    diffs = [abs(closes[i] - closes[i-1]) for i in range(1, n)]
    avg_abs_diff = sum(diffs[-period:]) / period
    window = closes[-period:]
    rng = max(window) - min(window)
    if rng <= 0:
        return 0.0
    strength = (avg_abs_diff / rng)  # 0..1 civarı
    return max(0.0, min(100.0, 100.0 * strength))


class DominanceTrend(StrategyBase):
    def __init__(self, cfg: Dict[str, Any]):
        super().__init__(cfg)
        p = cfg.get("params", {}) if isinstance(cfg, dict) else {}
        self.ema_period: int = int(p.get("ema_period", 20))
        self.rsi_period: int = int(p.get("rsi_period", 14))
        self.adx_period: int = int(p.get("adx_period", 14))
        self.adx_min: float = float(p.get("adx_min", 20))

        # Her sembol için kapanış history (RSI/ADX proxy için)
        self._closes: Dict[str, Deque[float]] = {}

    def _push_close(self, symbol: str, close: float) -> List[float]:
        dq = self._closes.get(symbol)
        if dq is None:
            dq = deque(maxlen=200)
            self._closes[symbol] = dq
        dq.append(float(close))
        return list(dq)

    def on_bar(self, bar_event: Dict[str, Any], ctx: Dict[str, Any]) -> Signal:
        """
        bar_event: {"type":"bar_closed","symbol":SYM,"tf":"1h","close":X,"ema20":Y,"time":T}
        ctx["indices"] snapshot: {
          "TOTAL3":{"tf1h":{"close":c1,"ema20":e1},"tf4h":{"close":c4,"ema20":e4}},
          "USDT.D":{"tf1h":{"close":u1,"ema20":ue1}},
          "BTC.D":{"tf1h":{"close":b1,"ema20":be1}}
        }
        """
        if bar_event.get("type") != "bar_closed":
            return Signal(side="FLAT")

        symbol = bar_event["symbol"]
        close_sym = float(bar_event.get("close", 0.0))
        ema20_sym = bar_event.get("ema20", None)

        # Kendi kapanış serimizi güncelle
        closes = self._push_close(symbol, close_sym)

        # RSI ve ADX-proxy
        rsi_val = _rsi(closes, self.rsi_period)
        adx_val = _adx_proxy(closes, self.adx_period)

        # Endeks snapshot
        indices = ctx.get("indices", {}) or {}
        T = indices.get("TOTAL3", {})
        U = indices.get("USDT.D", {})
        B = indices.get("BTC.D", {})

        T1 = T.get("tf1h", {}); T4 = T.get("tf4h", {})
        U1 = U.get("tf1h", {}); B1 = B.get("tf1h", {})

        # Koşulları güvenli şekilde oku
        T4_c, T4_e = T4.get("close"), T4.get("ema20")
        T1_c, T1_e = T1.get("close"), T1.get("ema20")
        U1_c, U1_e = U1.get("close"), U1.get("ema20")
        B1_c, B1_e = B1.get("close"), B1.get("ema20")

        # ADX min şartı
        adx_ok = (adx_val is None) or (adx_val >= self.adx_min)  # yeter veri yoksa engellemek istemiyorsan (None → geç)

        # LONG şartları
        long_ok = (
            _safe_gt(T4_c, T4_e) and
            _safe_gt(T1_c, T1_e) and
            _safe_gt(close_sym, ema20_sym) and
            (rsi_val is None or rsi_val < 60.0) and
            _safe_lt(U1_c, U1_e) and
            _safe_lt(B1_c, B1_e) and
            adx_ok
        )

        # SHORT şartları
        short_ok = (
            _safe_lt(T4_c, T4_e) and
            _safe_lt(T1_c, T1_e) and
            _safe_lt(close_sym, ema20_sym) and
            _safe_gt(U1_c, U1_e) and
            _safe_gt(B1_c, B1_e) and
            adx_ok
        )

        if long_ok and not short_ok:
            # strength basitçe rsi ve adx'ten türetilebilir; şimdi 0.6 sabitleyelim
            return Signal(side="LONG", strength=0.6)
        if short_ok and not long_ok:
            return Signal(side="SHORT", strength=0.6)

        return Signal(side="FLAT", strength=0.0)
