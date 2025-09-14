# /opt/tradebot/future_trade/exchange_utils.py
from math import floor

def quantize(val: float, step: float) -> float:
    """Adedi (LOT_SIZE.stepSize) gridine yuvarla."""
    if step <= 0:
        return float(val)
    return floor(float(val) / step) * step

def price_quantize(val: float, tick: float) -> float:
    """Fiyatı (PRICE_FILTER.tickSize) gridine yuvarla."""
    if tick <= 0:
        return float(val)
    return floor(float(val) / tick) * tick

def symbol_filters(exchange_info: dict, symbol: str):
    """exchangeInfo içinden tickSize, stepSize, minNotional döndür."""
    for s in exchange_info.get("symbols", []):
        if s["symbol"] == symbol:
            tick = step = min_notional = 0.0
            for f in s.get("filters", []):
                t = f.get("filterType")
                if t == "PRICE_FILTER":
                    tick = float(f["tickSize"])
                elif t == "LOT_SIZE":
                    step = float(f["stepSize"])
                elif t == "MIN_NOTIONAL":
                    # USDT-M futures'ta 'notional' anahtarı kullanılıyor
                    # bazı hesaplarda 'minNotional' olabilir, ikisini de dene
                    min_notional = float(f.get("notional") or f.get("minNotional") or 0.0)
            return tick, step, min_notional
    raise KeyError(f"symbol {symbol} not found in exchangeInfo")
