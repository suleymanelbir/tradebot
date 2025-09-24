# /opt/tradebot/future_trade/loops.py
import asyncio
import logging
from typing import Any, Dict
import inspect
from future_trade.strategy.base import Signal


def _build_ctx(
    *,
    stream, strategy, portfolio, supervisor, risk, router, persistence, notifier,
    cfg: Dict[str, Any], kill_switch, symbol: str, close: float, timestamp: int
) -> Dict[str, Any]:
    """
    Stratejilere verilecek ortak bağlam.
    - İleride state, cache, bars vb. genişletebilirsin.
    """
    return {
        "symbol": symbol,
        "close": close,
        "timestamp": timestamp,
        "get_last_price": getattr(stream, "get_last_price", None),
        "stream": stream,
        "strategy": strategy,
        "portfolio": portfolio,
        "supervisor": supervisor,
        "risk": risk,
        "router": router,
        "persistence": persistence,
        "notifier": notifier,
        "cfg": cfg,
        "kill_switch": kill_switch,
    }

def _call_strategy_on_bar_dynamic(strategy, event: dict, ctx: dict):
    import logging
    log = logging.getLogger("strat_loop")

    func = getattr(strategy, "on_bar", None)
    if not callable(func):
        return None

    argmap = {
        "event": event,
        "bar_event": event,
        "symbol": event.get("symbol"),
        "close": event.get("close"),
        "price": event.get("close"),
        "timestamp": event.get("time"),
        "ts": event.get("time"),
        "tf": event.get("tf"),
        "ctx": ctx,
    }
    # print("BAR:", argmap) # sembole ait tüm verileri termiande gösterir 
    sig = inspect.signature(func)
    params = [
        p for p in sig.parameters.values()
        if p.kind in (inspect.Parameter.POSITIONAL_ONLY,
                      inspect.Parameter.POSITIONAL_OR_KEYWORD,
                      inspect.Parameter.KEYWORD_ONLY)
    ]
    names = [p.name for p in params]

    # Positional arg list
    posargs = [argmap[name] for name in names if name in argmap]
    log.debug(f"[STRAT-CALL] posargs: {posargs}")

    try:
        return func(*posargs)  # ← önce positional dene
    except TypeError as e:
        log.debug(f"[STRAT-CALL] positional failed: {e}")
        kwargs = {name: argmap[name] for name in names if name in argmap}
        log.debug(f"[STRAT-CALL] fallback kwargs: {kwargs}")
        return func(**kwargs)

async def strat_loop(
    stream,
    strategy,
    portfolio,
    supervisor,
    risk,
    router,
    persistence,
    notifier,
    cfg: Dict[str, Any],
    kill_switch=None,
    order_manager=None  # ← yeni parametre eklendi
):
    log = logging.getLogger("strat_loop")
    tf_entry = (cfg.get("strategy", {}) or {}).get("timeframe_entry", "1h")

    async for ev in stream.events():
        try:
            if ev.get("type") != "bar_closed":
                continue
            if ev.get("tf") != tf_entry:
                log.debug(f"[FILTER] skipped tf={ev.get('tf')} ≠ expected={tf_entry}")
                continue

            symbol = ev.get("symbol")
            close = float(ev.get("close", 0) or 0)
            ts = int(ev.get("time", 0) or 0)
            ctx = _build_ctx(
                stream=stream, strategy=strategy, portfolio=portfolio, supervisor=supervisor,
                risk=risk, router=router, persistence=persistence, notifier=notifier,
                cfg=cfg, kill_switch=kill_switch, symbol=symbol, close=close, timestamp=ts
            )
            log.debug(f"[CTX] built for {symbol} @ {ts}: {list(ctx.keys())}")

            if kill_switch and hasattr(kill_switch, "is_trading_allowed") and not kill_switch.is_trading_allowed():
                log.info(f"[KS-BLOCK] trading disabled; skip signals @ {symbol}")
                continue

            event_dict = {
                "type": "bar_closed",
                "symbol": symbol,
                "tf": tf_entry,
                "close": close,
                "time": ts,
                "ema20": ev.get("ema20")
            }
            log.debug(f"[STRAT-CALL] event_dict: {event_dict}")

            try:
                trade_intent = _call_strategy_on_bar_dynamic(strategy, event_dict, ctx)
            except Exception as e:
                log.error(f"[STRAT] on_bar error: {e}")
                continue

            if isinstance(trade_intent, Signal):
                log.info(f"[SIGNAL] {symbol} → {trade_intent.side} strength={trade_intent.strength} entry={trade_intent.entry}")

            # ✅ Intent işleme – artık gerçek emir gönderimi var
            if isinstance(trade_intent, dict) and trade_intent.get("action") == "entry":
                side = (trade_intent.get("side") or "").upper()
                if side in ("BUY", "SELL"):
                    if kill_switch and hasattr(kill_switch, "is_trading_allowed") and not kill_switch.is_trading_allowed():
                        log.info(f"[KS-BLOCK] trading disabled; drop intent {trade_intent}")
                        continue
                    if order_manager:
                        try:
                            res = order_manager.open_entry_from_intent(trade_intent)
                            try:
                                await notifier.info_trades({
                                    "event": "entry",
                                    "symbol": trade_intent.get("symbol"),
                                    "side": side,
                                    "qty": trade_intent.get("qty")
                                })
                            except Exception:
                                pass
                            log.info(f"[ENTRY-OK] {res}")
                        except Exception as e:
                            log.error(f"[ENTRY-ERR] {e}")
                            try:
                                await notifier.alert({
                                    "event": "entry_error",
                                    "error": str(e),
                                    "intent": trade_intent
                                })
                            except Exception:
                                pass
                    else:
                        log.info(f"[INTENT] {trade_intent} (no order_manager bound)")

        except Exception as e:
            log.error(f"strat_loop error: {e}")
            await asyncio.sleep(0.5)


async def trailing_loop(router, stream, notifier, cfg, stop_event=None):
    log = logging.getLogger("trailing_loop")
    interval = int((cfg.get("trailing", {}) or {}).get("update_interval_sec", 30))
    while not (stop_event and stop_event.is_set()):
        try:
            router.update_trailing_for_open_positions()
        except Exception as e:
            try:
                await notifier.alert({"event": "trailing_error", "error": str(e)})
            except Exception:
                pass
            log.error(f"trailing_loop error: {e}")
        await asyncio.sleep(max(5, interval))


async def kill_switch_loop(kill_switch, notifier, interval_sec: int = 10, stop_event=None):
    log = logging.getLogger("kill_switch_loop")
    while not (stop_event and stop_event.is_set()):
        try:
            snap = await kill_switch.check_and_maybe_trigger()
            log.info(f"[KS] eq={snap['equity']:.2f} pnl%={snap['pnl_pct']:.2f} dd%={snap['dd_pct']:.2f} triggered={snap['triggered']}")
        except Exception as e:
            log.error(f"kill_switch_loop error: {e}")
        await asyncio.sleep(max(5, int(interval_sec)))

async def daily_reset_loop(kill_switch, notifier, stop_event=None, tz_offset_hours: int = 0):
    """
    Her gün UTC 00:00 (+ offset) 'de kill-switch'i resetler.
    tz_offset_hours: yerel saat için ofset (örn. Türkiye UTC+3 -> 3)
    """
    import asyncio, datetime as dt, logging
    log = logging.getLogger("daily_reset_loop")
    while not (stop_event and stop_event.is_set()):
        now = dt.datetime.utcnow() + dt.timedelta(hours=tz_offset_hours)
        tomorrow = (now + dt.timedelta(days=1)).date()
        target = dt.datetime.combine(tomorrow, dt.time(0,0))  # yarın 00:00 (local offset ile)
        wait_sec = max(5, (target - now).total_seconds())
        await asyncio.sleep(wait_sec)
        try:
            # yeni gün başlangıç equity ile reset
            kill_switch.reset_for_new_day()
            try:
                await notifier.info_trades({"event":"kill_switch_reset", "msg":"new trading day started"})
            except Exception:
                pass
            log.info("[KS-RESET] new day reset complete")
        except Exception as e:
            log.error(f"[KS-RESET] error: {e}")
