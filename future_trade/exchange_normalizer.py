from typing import Dict, Any

class ExchangeNormalizer:
    def __init__(self, cfg: Dict[str, Any]):
        self.cfg = cfg
        self.precision_map = {}  # sembol bazlı hassasiyet bilgisi

    def set_precision(self, symbol: str, price_precision: int, qty_precision: int):
        self.precision_map[symbol] = {
            "price": price_precision,
            "qty": qty_precision
        }

    def normalize_price(self, symbol: str, price: float) -> float:
        p = self.precision_map.get(symbol, {}).get("price", 2)
        return round(price, p)

    def normalize_qty(self, symbol: str, qty: float) -> float:
        q = self.precision_map.get(symbol, {}).get("qty", 3)
        return round(qty, q)
