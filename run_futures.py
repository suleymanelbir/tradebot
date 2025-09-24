"""
ps aux | grep run_futures.py dosayının çalışıp çalışmadını kontrol etemk için
pkill -f "run_futures.py" || true   # varsa durdur
    python /opt/tradebot/run_futures.py          varsa arka planda başlat

chmod +x run_futures.py
./run_futures.py
Bu komutla dosya doğrudan çalıştırılır, çünkü shebang satırı Python 3 yorumlayıcısını çağırır.
"""


#!/usr/bin/env python3
import asyncio
import sys
ROOT = "/opt/tradebot"
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)
from future_trade.app import main as app_main  # noqa: E402


if __name__ == "__main__":
    try:
        asyncio.run(app_main())
    except KeyboardInterrupt:
        print("Bot durduruldu (Ctrl+C)")
    except Exception as e:
        import traceback
        print("Başlatma hatası:", e)
        traceback.print_exc()
