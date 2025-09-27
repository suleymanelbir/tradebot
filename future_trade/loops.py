# /opt/tradebot/future_trade/loops.py
import asyncio
import inspect
import logging
from datetime import datetime, timedelta, timezone
from typing import Any, Dict

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
    order_manager=None,  # ← yeni parametre eklendi
    selector=None        # sembol_selector  özelliği için eklendi
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

async def daily_pnl_summary_loop(
    persistence,
    notifier,
    stop_event: asyncio.Event,
    *,
    tz_offset_hours: int = 3,
    run_at: str = "23:59",
    price_provider = None,          # async/sync: price_provider(symbol)
    include_unrealized: bool = True # açık pozisyonları rapora ekle
):
    logger = logging.getLogger("pnl_daily")
    hh, mm = map(int, run_at.split(":"))
    tz = timezone(timedelta(hours=tz_offset_hours))

    def _next_run() -> float:
        now = datetime.now(tz)
        target = now.replace(hour=hh, minute=mm, second=0, microsecond=0)
        if target <= now:
            target = target + timedelta(days=1)
        return (target - now).total_seconds()

    while not (stop_event and stop_event.is_set()):
        await asyncio.sleep(_next_run())
        if stop_event and stop_event.is_set():
            break
        try:
            date_str = datetime.now(tz).strftime("%Y-%m-%d")

            # Unrealized PnL’i persistence içinde hesaplamak zor → price_provider async/sync olabilir
            # Bu yüzden burada basit bir sarmalayıcı yapıyoruz:
            def _pp(sym: str):
                res = price_provider(sym) if callable(price_provider) else None
                return res

            summary = persistence.compute_daily_pnl_summary(
                date_str,
                tz_offset_hours=tz_offset_hours,
                include_unrealized=include_unrealized,
                price_provider=_pp,
            )

            # Telegram (ve Notifier mirror ile DB)
            await notifier.notify_pnl_daily(summary)
            logger.info(f"[PNL] daily summary sent: {summary}")

        except Exception as e:
            logger.error(f"daily pnl loop error: {e}")
            
async def position_risk_guard_loop(
    client,
    notifier,
    persistence,
    symbols: list[str],
    stop_event: asyncio.Event,
    interval_sec: int = 60,
    warn_pct: float = 3.0,
    crit_pct: float = 1.5,
):
    """
    Her interval'da semboller için /fapi/v2/positionRisk kontrol eder.
    Likidasyon mesafesi (|mark-liq|/mark) eşiğin altına inerse Telegram + DB log.
    """
    log = logging.getLogger("pos_risk_guard")
    warn = warn_pct / 100.0
    crit = crit_pct / 100.0

    async def _check_symbol(sym: str):
        try:
            data = await client.get_position_risk(symbol=sym)
        except Exception as e:
            log.debug(f"fetch risk err {sym}: {e}")
            return

        for r in data or []:
            try:
                qty = float(r.get("positionAmt") or 0)
                if abs(qty) <= 0:
                    continue  # pozisyon yok
                liq = float(r.get("liquidationPrice") or 0)
                mark = float(r.get("markPrice") or 0)
                if liq <= 0 or mark <= 0:
                    continue
                dist = abs(mark - liq) / mark
                payload = {
                    "event": "position_risk",
                    "symbol": sym,
                    "qty": qty,
                    "mark": mark,
                    "liq": liq,
                    "dist_pct": dist,
                }
                if dist <= crit:
                    await notifier.notify_position_risk(payload | {"level": "CRITICAL"})
                    # DB aynası notifier içinde
                    log.warning(f"[PRG] CRIT {sym} dist={dist:.2%}")
                elif dist <= warn:
                    await notifier.notify_position_risk(payload | {"level": "WARN"})
                    log.info(f"[PRG] WARN {sym} dist={dist:.2%}")
            except Exception as ie:
                log.debug(f"parse risk err {sym}: {ie}")

    while not (stop_event and stop_event.is_set()):
        try:
            if not symbols:
                # boş ise sessizce geç
                await asyncio.sleep(max(20, interval_sec))
                continue
            await asyncio.gather(*[_check_symbol(s) for s in symbols])
        except Exception as e:
            log.error(f"pos risk loop err: {e}")
        await asyncio.sleep(max(20, interval_sec))

# Gün içi günlük Pnl özeti


async def performance_guard_loop(
    persistence,
    notifier,
    stop_event: asyncio.Event,
    *,
    interval_sec: int = 120,
    profit_factor_floor: float = 1.0,
    loss_streak_threshold: int = 3,
    min_trades: int = 5,
    cooldown_sec: int = 900,
    tz_offset_hours: int = 3,
):
    """
    Gün içinde belirli aralıklarla günlük PnL özetini (o ana kadar) hesaplar,
    aşağıdaki koşullarda erken uyarı gönderir:
      - profit_factor < floor
      - current loss streak >= threshold
    Uyarı tipi başına cooldown uygulanır.
    """
    log = logging.getLogger("perf_guard")
    last_alert_ts = {"PF_BELOW_FLOOR": 0, "LOSS_STREAK": 0}

    def _now_ts():
        import time
        return int(time.time())

    def _today_str():
        tz = timezone(timedelta(hours=tz_offset_hours))
        return datetime.now(tz).strftime("%Y-%m-%d")

    while not (stop_event and stop_event.is_set()):
        try:
            date_str = _today_str()

            # Gün içi o ana kadar olan özet (unrealized gereksiz → False)
            summary = persistence.compute_daily_pnl_summary(
                date_str,
                tz_offset_hours=tz_offset_hours,
                include_unrealized=False,
                price_provider=None,
            )
            trades = int(summary.get("trades") or 0)
            if trades >= min_trades:
                # PF sayıya çevrilir (inf/string olabilir)
                pf_raw = summary.get("profit_factor")
                try:
                    pf = float(pf_raw)
                except Exception:
                    pf = float("inf") if str(pf_raw).lower() == "inf" else None

                # 1) Profit Factor zeminin altı
                if pf is not None and pf < float(profit_factor_floor):
                    now = _now_ts()
                    if now - last_alert_ts["PF_BELOW_FLOOR"] >= int(cooldown_sec):
                        payload = {
                            "event": "perf_guard",
                            "date": date_str,
                            "trades": trades,
                            "winrate": summary.get("winrate", 0.0),
                            "profit_factor": pf,
                            "floor": profit_factor_floor,
                            "loss_streak": summary.get("current_streak_len", 0)
                                if summary.get("current_streak_type") == "LOSS" else 0,
                            "threshold": loss_streak_threshold,
                            "reason": "PF_BELOW_FLOOR",
                        }
                        await notifier.notify_performance_alert(payload)
                        last_alert_ts["PF_BELOW_FLOOR"] = now
                        log.warning(f"[PERF] PF below floor: pf={pf} floor={profit_factor_floor}")

                # 2) Loss streak eşiği
                if str(summary.get("current_streak_type")).upper() == "LOSS":
                    ls = int(summary.get("current_streak_len") or 0)
                    if ls >= int(loss_streak_threshold):
                        now = _now_ts()
                        if now - last_alert_ts["LOSS_STREAK"] >= int(cooldown_sec):
                            payload = {
                                "event": "perf_guard",
                                "date": date_str,
                                "trades": trades,
                                "winrate": summary.get("winrate", 0.0),
                                "profit_factor": summary.get("profit_factor"),
                                "floor": profit_factor_floor,
                                "loss_streak": ls,
                                "threshold": loss_streak_threshold,
                                "reason": "LOSS_STREAK",
                            }
                            await notifier.notify_performance_alert(payload)
                            last_alert_ts["LOSS_STREAK"] = now
                            log.warning(f"[PERF] Loss streak >= threshold: ls={ls} thr={loss_streak_threshold}")

        except Exception as e:
            log.error(f"performance guard loop error: {e}")

        await asyncio.sleep(max(30, int(interval_sec)))
        
# **********symbol_selector için eklendi************"

async def selector_loop(selector, stop_event: asyncio.Event, notifier=None, cfg=None, interval_sec: int = 300):
    import asyncio, logging
    log = logging.getLogger("selector_loop")
    rep_cfg = ((cfg or {}).get("dynamic_universe") or {}).get("reporting") or {}
    annotated = bool(rep_cfg.get("annotated", True))
    only_on_change = bool(rep_cfg.get("only_on_change", True))

    while not (stop_event and stop_event.is_set()):
        try:
            rep = await selector.scan_once()
            should_send = True
            if only_on_change and not rep.get("change"):
                should_send = False
            if annotated and should_send and notifier and hasattr(notifier, "info_selection"):
                await notifier.info_selection(rep)
        except Exception as e:
            log.error(f"selector scan error: {e}")
        await asyncio.sleep(max(60, int(interval_sec)))

    