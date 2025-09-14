# /opt/tradebot/future_trade/portfolio.py
from dataclasses import dataclass

@dataclass
class OpenPosition:
    symbol: str
    side: str    # "LONG" / "SHORT"
    qty: float

class Portfolio:
    def __init__(self, persistence):
        self.persistence = persistence

    def equity(self) -> float:
        return 10000.0  # TODO: Binance bakiyesi ile bağla

    def snapshot(self):
        return {"equity": self.equity()}

    def open_positions(self) -> list[OpenPosition]:
        conn = self.persistence._conn()
        try:
            cur = conn.cursor()
            cur.execute("""SELECT symbol, side, qty
                           FROM futures_positions
                           WHERE qty IS NOT NULL AND ABS(qty) > 0""")
            rows = cur.fetchall()
            out = []
            for sym, side, qty in rows:
                # side boş/yanlışsa qty işaretine göre belirle
                if not side or side not in ("LONG", "SHORT"):
                    side = "LONG" if float(qty) > 0 else "SHORT"
                out.append(OpenPosition(symbol=sym, side=side, qty=float(qty)))
            return out
        finally:
            conn.close()

    def open_symbols(self) -> set[str]:
        return {p.symbol for p in self.open_positions()}
