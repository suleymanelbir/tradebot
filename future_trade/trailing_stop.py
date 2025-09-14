"""İz-süren stop algoritmaları (percent/atr/chandelier)
- Sadece kâr yönüne sıkılaştırma
- step_pct ile güncelleme frekansı
"""
from typing import Optional

def percent_trailing(side: str, last_price: float, prev_trail: Optional[float], pct: float) -> float:
    # İlk kurulum: girişe göre belirlenir; burada basitçe pct ile başlat
    if prev_trail is None:
        if side == "LONG":
            return last_price * (1 - pct / 100.0)
        else:
            return last_price * (1 + pct / 100.0)
    
    if side == "LONG":
        return max(prev_trail, last_price * (1 - pct/100.0))
    else:
        return min(prev_trail, last_price * (1 + pct/100.0))