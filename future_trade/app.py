"""
Ana orkestrasyon: config yükleme, client/akış/strateji/risk/emir/reconcile görevlerini başlatır.
"""

import asyncio, json, signal, logging, os, contextlib
from pathlib import Path
from typing import Any, Dict

from .binance_client import BinanceClient
from .market_stream import MarketStream
from .strategy.dominance_trend import DominanceTrend
from .risk_manager import RiskManager
from .order_router import OrderRouter
from .order_reconciler import OrderReconciler
from .position_supervisor import PositionSupervisor
from .portfolio import Portfolio
from .persistence import Persistence
from .telegram_notifier import Notifier
# from .n8n_bridge import N8NBridge   # <- Şimdilik kullanılmıyor; yoruma al




CONFIG_PATH = Path(__file__).with_name("config.json")


STRATEGY_REGISTRY = {
"dominance_trend": DominanceTrend,
}


async def load_config(path: Path) -> Dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


async def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    print("Futures bot starting...")

    cfg_path = os.environ.get("FUTURE_TRADE_CONFIG")
    cfg_file = Path(cfg_path) if cfg_path else CONFIG_PATH
    logging.info(f"[CONFIG] using: {cfg_file}")
    cfg = await load_config(cfg_file)
    logging.info("Config loaded OK")

    notifier = Notifier(cfg.get("telegram", {}))
    await notifier.info_trades({"event": "startup", "msg": "Futures bot starting"})

    persistence = Persistence(cfg["database"]["path"])
    persistence.init_schema()

    client = BinanceClient(cfg["binance"], cfg["mode"])
    await client.bootstrap_exchange(cfg)

    portfolio = Portfolio(persistence)
    risk = RiskManager(cfg["risk"], cfg.get("leverage", {}), portfolio)

    stream = MarketStream(
        cfg=cfg,
        whitelist=cfg["symbols_whitelist"],
        indices=["TOTAL3", "USDT.D", "BTC.D"],
        tf_entry=cfg["strategy"]["timeframe_entry"],
        tf_confirm=cfg["strategy"]["confirm_tf"],
        persistence=persistence,
        client=client,
    )

    strat_name = cfg.get("strategy", {}).get("name", "dominance_trend")
    strat_cls = STRATEGY_REGISTRY.get(strat_name, DominanceTrend)
   

    router = OrderRouter(client=client, cfg=cfg["order"], notifier=notifier, persistence=persistence)
    reconciler = OrderReconciler(client=client, persistence=persistence, notifier=notifier)
    supervisor = PositionSupervisor(cfg, portfolio, notifier)

    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for s in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(s, stop.set)
        except NotImplementedError:
            pass

    async def strat_loop():
        async for event in stream.events():
            if event.get("type") != "bar_closed":
                continue
            symbol = event["symbol"]
            if symbol not in cfg["symbols_whitelist"]:
                continue

            
              

            ok, reason = supervisor.evaluate_entry(symbol, sig)
            if not ok:
                await notifier.debug_trades({"event": "entry_rejected", "symbol": symbol, "reason": reason})
                continue

            plan = risk.plan_trade(symbol, sig)
            if not plan.ok:
                await notifier.debug_trades({"event": "sizing_rejected", "symbol": symbol, "reason": plan.reason})
                continue

            try:
                await router.place_entry_with_protection(plan)
                persistence.record_signal_audit(event, sig, decision=True)
            except Exception as e:
                await notifier.alert({"event": "order_error", "symbol": symbol, "error": str(e)})
                persistence.record_signal_audit(event, sig, decision=False, reason=str(e))

    async def trailing_loop():
        trailing_cfg = cfg.get("trailing", {})
        interval = int(trailing_cfg.get("update_interval_sec", 30))
        while not stop.is_set():
            try:
                await router.update_trailing_for_open_positions(stream, trailing_cfg)
            except Exception as e:
                await notifier.alert({"event": "trailing_error", "error": str(e)})
            await asyncio.sleep(interval)

    tasks = [
        asyncio.create_task(stream.run()),
        asyncio.create_task(strat_loop()),
        asyncio.create_task(reconciler.run()),
        asyncio.create_task(trailing_loop()),
    ]

   