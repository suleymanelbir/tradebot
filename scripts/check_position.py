# çalıştırmak için: 
# /opt/tradebot/trade_env/bin/python /opt/tradebot/scripts/check_position.py


# /opt/tradebot/scripts/check_position.py

import sys
from pathlib import Path

# Proje kök dizinini PYTHONPATH'e ekle
ROOT_DIR = Path(__file__).resolve().parent.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

import asyncio, json, os
from future_trade.binance_client import BinanceClient

CFG = Path(os.environ.get("FUTURE_TRADE_CONFIG", "/opt/tradebot/future_trade/config.json"))
cfg = json.loads(CFG.read_text(encoding="utf-8"))

async def main():
    c = BinanceClient(cfg["binance"], cfg.get("mode", "testnet"))
    acc = await c.position_risk()
    for p in acc:
        if p.get("symbol") == "SOLUSDT":
            print("EXCHANGE POSITION:", p)
    await c.close()

if __name__ == "__main__":
    asyncio.run(main())
