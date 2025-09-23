# /opt/tradebot/future_trade/loops.py
import asyncio, logging

async def strat_loop(stream, strategy, portfolio, supervisor, risk, router, persistence, notifier, cfg):
    log = logging.getLogger("strat_loop")
    # TODO: buraya Strategy.on_bar() tetikleyip plan_trade → order_manager vb. bağlanacak
    while True:
        await asyncio.sleep(1)

async def trailing_loop(router, stream, notifier, cfg, stop_event):
    log = logging.getLogger("trailing_loop")
    interval = int(cfg.get("trailing", {}).get("update_interval_sec", 30))
    while not stop_event.is_set():
        try:
            router.update_trailing_for_open_positions()
        except Exception as e:
            await notifier.alert({"event": "trailing_error", "error": str(e)})
        await asyncio.sleep(max(5, interval))
