#!/usr/bin/env python3
import asyncio, json, os
from pathlib import Path

from future_trade.binance_client import BinanceClient
from future_trade.telegram_notifier import Notifier
from future_trade.persistence import Persistence
from future_trade.order_router import OrderRouter
from future_trade.strategy.base import Plan  # dataclass

CONFIG_PATH = Path(os.environ.get("FUTURE_TRADE_CONFIG", "/opt/tradebot/future_trade/config.json"))

async def main():
    cfg = json.loads(CONFIG_PATH.read_text(encoding="utf-8"))
    notifier = Notifier(cfg.get("telegram", {}))
    persistence = Persistence(cfg["database"]["path"]); persistence.init_schema()

    client = BinanceClient(cfg["binance"], cfg["mode"])
    await client.bootstrap_exchange(cfg)

    router = OrderRouter(
        client=client,
        cfg={**cfg.get("order", {}), "leverage": cfg.get("leverage", {})},  # leverage görünür olsun
        notifier=notifier,
        persistence=persistence
    )

    sym = "SOLUSDT"
    px = await client.get_price(sym)
    # Basit SL/TP: %2 aşağı SL, %2 yukarı TP (MARKET close-all)
    sl = round(px * 0.98, 4)
    tp = round(px * 1.02, 4)

    # Küçük qty; Router minNotional ve marjı ayrıca kontrol edip kısabilir
    plan = Plan(symbol=sym, side="LONG", qty=5.0, entry=px, sl=sl, tp=tp)

    await router.place_entry_with_protection(plan)
    await client.close()

if __name__ == "__main__":
    asyncio.run(main())
