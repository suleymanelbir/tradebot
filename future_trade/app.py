# /opt/tradebot/future_trade/app.py
from __future__ import annotations

import asyncio
import logging
import os
import signal
import contextlib
import json
import aiohttp
from pathlib import Path
from typing import Any, Dict, Optional

from .binance_client import BinanceClient
from .market_stream import MarketStream
from .strategy.base import StrategyBase, Signal
from .strategy.dominance_trend import DominanceTrend
from .risk_manager import RiskManager
from .order_router import OrderRouter
from .order_reconciler import OrderReconciler
from .position_supervisor import PositionSupervisor
from .portfolio import Portfolio
from .persistence import Persistence
from .telegram_notifier import Notifier


# Varsayılan config yolu: future_trade/config.json
CONFIG_PATH = Path(__file__).with_name("config.json")

# Strateji kayıt defteri
STRATEGY_REGISTRY: Dict[str, type[StrategyBase]] = {
    "dominance_trend": DominanceTrend,
}


async def load_config(path: Path) -> Dict[str, Any]:
    """Config dosyasını yükler (utf-8)."""
    text = path.read_text(encoding="utf-8")
    return json.loads(text)


async def strat_loop(
    stream: MarketStream,
    strategy: StrategyBase,
    portfolio: Portfolio,
    supervisor: PositionSupervisor,
    risk: RiskManager,
    router: OrderRouter,
    persistence: Persistence,
    notifier: Notifier,
    cfg: Dict[str, Any],
) -> None:
    """
    Kapanan barlarda sinyal üret → süpervizör & risk kontrol → emir akışı.
    """
    signal: Optional[Signal] = None

    async for event in stream.events():
        if event.get("type") != "bar_closed":
            continue

        symbol = event.get("symbol")
        if not symbol or symbol not in cfg.get("symbols_whitelist", []):
            continue

        # Sinyal üretimi
        signal = strategy.on_bar(
            event,
            ctx={
                "indices": stream.indices_snapshot(),
                "portfolio": portfolio.snapshot(),
            },
        )

        # Giriş uygun mu?
        ok, reason = supervisor.evaluate_entry(symbol, signal)
        if not ok:
            await notifier.debug_trades(
                {"event": "entry_rejected", "symbol": symbol, "reason": reason}
            )
            # İstersen şurayı açıp reddedilen sinyali de audit’e yazabilirsin:
            # persistence.record_signal_audit(event, signal, decision=False, reason=reason)
            continue

        # Boyutlandırma
        plan = risk.plan_trade(symbol, signal)
        if not plan.ok:
            await notifier.debug_trades(
                {"event": "sizing_rejected", "symbol": symbol, "reason": plan.reason}
            )
            # persistence.record_signal_audit(event, signal, decision=False, reason=plan.reason)
            continue

        # Emir akışı
        try:
            await router.place_entry_with_protection(plan)
            persistence.record_signal_audit(event, signal, decision=True)
        except Exception as e:
            await notifier.alert({"event": "order_error", "symbol": symbol, "error": str(e)})
            persistence.record_signal_audit(event, signal, decision=False, reason=str(e))


async def trailing_loop(
    router: OrderRouter,
    stream: MarketStream,
    notifier: Notifier,
    cfg: Dict[str, Any],
    stop_event: asyncio.Event,
) -> None:
    """
    ATR tabanlı trailing SL güncelleme döngüsü (OrderRouter içindeki fonksiyonu çağırır).
    """
    trailing_cfg = cfg.get("trailing", {}) or {}
    try:
        interval = int(trailing_cfg.get("update_interval_sec", 30))
    except Exception:
        interval = 30

    while not stop_event.is_set():
        try:
            await router.update_trailing_for_open_positions(stream, trailing_cfg)
        except Exception as e:
            await notifier.alert({"event": "trailing_error", "error": str(e)})
        await asyncio.sleep(interval)

async def send_alert(msg: str):
    import aiohttp
    token = "7179161717:AAEGcM8LCvvBtp5JJ6uToSM9dUSKDZvujM0"
    chat_id = "-1002248511924"
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    async with aiohttp.ClientSession() as session:
        try:
            await session.post(url, data={"chat_id": chat_id, "text": msg})
        except Exception as e:
            logging.error(f"Telegram alert failed: {e}")



async def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    print("Futures bot starting...")

    cfg_path_env = os.environ.get("FUTURE_TRADE_CONFIG")
    cfg_file = Path(cfg_path_env) if cfg_path_env else CONFIG_PATH
    logging.info(f"[CONFIG] using: {cfg_file}")

    try:
        cfg = await load_config(cfg_file)
        logging.info("Config loaded OK")
    except Exception as e:
        logging.error(f"Config load failed: {e}")
        await send_alert(f"❌ Config load failed: {e}")
        return

    # Notifier
    try:
        notifier = Notifier(cfg.get("telegram", {}))
        await notifier.info_trades({"event": "startup", "msg": "✅ Notifier initialized"})
        logging.info("Notifier initialized")
    except Exception as e:
        logging.error(f"Notifier init failed: {e}")
        await send_alert(f"❌ Notifier init failed: {e}")
        return

    # DB
    try:
        persistence = Persistence(cfg["database"]["path"])
        persistence.init_schema()
        await notifier.info_trades({"event": "startup", "msg": "✅ Database schema OK"})
        logging.info("Database schema initialized")
    except Exception as e:
        logging.error(f"Database init failed: {e}")
        await notifier.alert({"event": "startup_error", "msg": f"❌ DB init failed: {e}"})
        return

    # Binance client
    try:
        client = BinanceClient(cfg["binance"], cfg.get("mode", "testnet"))
        await client.bootstrap_exchange(cfg)
        await notifier.info_trades({"event": "startup", "msg": "✅ Binance client OK"})
        logging.info("Binance client bootstrapped")
    except Exception as e:
        logging.error(f"Binance client failed: {e}")
        await notifier.alert({"event": "startup_error", "msg": f"❌ Binance client failed: {e}"})
        return

    # Portföy ve Risk
    try:
        portfolio = Portfolio(persistence)
        risk = RiskManager(cfg.get("risk", {}), cfg.get("leverage", {}), portfolio)
        logging.info("Portfolio and RiskManager ready")
    except Exception as e:
        logging.error(f"Portfolio/RiskManager init failed: {e}")
        await notifier.alert({"event": "startup_error", "msg": f"❌ Portfolio/RiskManager failed: {e}"})
        return

    # Market stream
    try:
        stream = MarketStream(
            cfg=cfg,
            whitelist=cfg.get("symbols_whitelist", []),
            indices=["TOTAL3", "USDT.D", "BTC.D"],
            tf_entry=cfg.get("strategy", {}).get("timeframe_entry", "1h"),
            tf_confirm=cfg.get("strategy", {}).get("confirm_tf", "4h"),
            persistence=persistence,
            client=client,
        )
        logging.info("MarketStream initialized")
    except Exception as e:
        logging.error(f"MarketStream init failed: {e}")
        await notifier.alert({"event": "startup_error", "msg": f"❌ MarketStream failed: {e}"})
        return

    # Strateji
    try:
        strat_name = cfg.get("strategy", {}).get("name", "dominance_trend")
        strat_cls = STRATEGY_REGISTRY.get(strat_name, DominanceTrend)
        strategy: StrategyBase = strat_cls(cfg.get("strategy", {}))
        logging.info(f"Strategy selected: {strat_name}")
    except Exception as e:
        logging.error(f"Strategy init failed: {e}")
        await notifier.alert({"event": "startup_error", "msg": f"❌ Strategy failed: {e}"})
        return

    # Router, Reconciler, Supervisor
    try:
        router_cfg = dict(cfg.get("order", {}) or {})
        router_cfg.setdefault("leverage", cfg.get("leverage", {}))

        router = OrderRouter(client=client, cfg=router_cfg, notifier=notifier, persistence=persistence)
        reconciler = OrderReconciler(client=client, persistence=persistence, notifier=notifier)
        supervisor = PositionSupervisor(cfg, portfolio, notifier, persistence)
        logging.info("OrderRouter, Reconciler, Supervisor ready")
    except Exception as e:
        logging.error(f"Router/Reconciler/Supervisor init failed: {e}")
        await notifier.alert({"event": "startup_error", "msg": f"❌ Router/Reconciler/Supervisor failed: {e}"})
        return

    # Graceful shutdown
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, stop.set)
        except NotImplementedError:
            pass

    # Görevler
    tasks = [
        asyncio.create_task(stream.run(), name="stream"),
        asyncio.create_task(
            strat_loop(stream, strategy, portfolio, supervisor, risk, router, persistence, notifier, cfg),
            name="strat_loop",
        ),
        asyncio.create_task(reconciler.run(), name="reconciler"),
        asyncio.create_task(trailing_loop(router, stream, notifier, cfg, stop), name="trailing"),
    ]

    await notifier.info_trades({"event": "startup", "msg": "✅ All modules initialized. Bot is running."})
    logging.info("All tasks scheduled. Bot is running.")

    try:
        await stop.wait()
    finally:
        for t in tasks:
            t.cancel()
        with contextlib.suppress(Exception):
            await asyncio.gather(*tasks, return_exceptions=True)
        with contextlib.suppress(Exception):
            await client.close()
        aclose = getattr(notifier, "aclose", None)
        if callable(aclose):
            with contextlib.suppress(Exception):
                await aclose()

# Modül doğrudan çalıştırılırsa:
if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
