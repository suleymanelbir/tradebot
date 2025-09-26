# /opt/tradebot/future_trade/app.py
# -*- coding: utf-8 -*-
from __future__ import annotations

# =========================
# 0) Standart Kütüphaneler
# =========================
import os, signal, asyncio, contextlib, logging
from pathlib import Path
from typing import Any, Dict

# =========================
# 1) Yapılandırma / Yardımcılar
# =========================
from future_trade.config_loader import load_config
from future_trade.exchange_utils import ExchangeNormalizer

# =========================
# 2) Temel Bileşenler
# =========================
from future_trade.persistence import Persistence
from future_trade.telegram_notifier import Notifier
from future_trade.binance_client import BinanceClient
from future_trade.portfolio import Portfolio
from future_trade.risk_manager import RiskManager

# =========================
# 3) Emir & Pozisyon Yönetimi
# =========================
from future_trade.order_router import OrderRouter
from future_trade.order_reconciler import OrderReconciler
from future_trade.order_manager import OrderManager
from future_trade.position_supervisor import PositionSupervisor
from future_trade.stop_manager import StopManager
from future_trade.take_profit_manager import TakeProfitManager
from future_trade.protective_sweeper import ProtectiveSweeper
from future_trade.oco_watcher import OCOWatcher
from future_trade.klines_cache import KlinesCache
from future_trade.loops import position_risk_guard_loop
from future_trade.loops import performance_guard_loop
# =========================
# 4) Strateji Sistemi
# =========================
from future_trade.strategies import STRATEGY_REGISTRY

# =========================
# 5) Veri Akışları
# =========================
from future_trade.market_stream import MarketStream
from future_trade.user_data_stream import UserDataStream

# =========================
# 6) Döngüler
# =========================
from future_trade.kill_switch import KillSwitch
from future_trade.loops import (
    strat_loop,
    trailing_loop,
    kill_switch_loop,
    daily_reset_loop,
    daily_pnl_summary_loop,
)

# 0.1) Varsayılan config yolu
CONFIG_PATH = Path("/opt/tradebot/future_trade/config.json")


async def main() -> None:
    # ================
    # 1) LOG & BOOT
    # ================
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    logger = logging.getLogger("TradeBot")
    print("Futures bot starting...")

    # =======================
    # 2) CONFIG YÜKLEME
    # =======================
    cfg_path_env = os.environ.get("FUTURE_TRADE_CONFIG")
    cfg_file = Path(cfg_path_env) if cfg_path_env else CONFIG_PATH
    logging.info(f"[CONFIG] using: {cfg_file}")
    try:
        cfg: Dict[str, Any] = await load_config(cfg_file)
        logging.info("Config loaded OK")
    except Exception as e:
        logging.error(f"Config load failed: {e}")
        return

    # =======================
    # 3) NOTIFIER (persistence henüz yok → None ver)
    # =======================
    try:
        notifier = Notifier(cfg.get("telegram", {}), persistence=None)
        await notifier.info_trades({"event": "startup", "msg": "✅ Notifier initialized"})
        logging.info("Notifier initialized")
    except Exception as e:
        logging.error(f"Notifier init failed: {e}")
        return

    # =======================
    # 4) DATABASE (Persistence)
    # =======================
    try:
        persistence = Persistence(
            path=cfg["database"]["path"],
            router=None,
            logger=logging.getLogger("db"),
        )
        persistence.init_schema()
        # Notifier’a persistence bağla (DB aynalama için)
        notifier.persistence = persistence
        await notifier.info_trades({"event": "startup", "msg": "✅ Database schema OK"})
        logging.info("Database schema initialized")
    except Exception as e:
        logging.error(f"Database init failed: {e}")
        await notifier.alert({"event": "startup_error", "msg": f"❌ DB init failed: {e}"})
        return

    # =======================
    # 5) BINANCE CLIENT
    # =======================
    try:
        app_section = cfg.get("app", {})
        mode = app_section.get("mode", cfg.get("mode", "paper")).lower()

        client = BinanceClient(cfg["binance"], mode)
        try:
            await client.bootstrap_exchange({
                "margin_mode": app_section.get("margin_mode", cfg.get("margin_mode", "ISOLATED")),
                "position_mode": app_section.get("position_mode", cfg.get("position_mode", "ONE_WAY")),
            })
        except Exception as e:
            msg = str(e)
            if "4059" in msg or "No need to change position side" in msg:
                logging.info("positionSide already correct (4059). Continuing.")
            else:
                raise
        await notifier.info_trades({"event": "startup", "msg": "✅ Binance client OK"})
        logging.info("Binance client bootstrapped")
    except Exception as e:
        logging.error(f"Binance client failed: {e}")
        await notifier.alert({"event": "startup_error", "msg": f"❌ Binance client failed: {e}"})
        return

    # =======================
    # 6) PORTFÖY & RISK
    # =======================
    try:
        portfolio = Portfolio(persistence)

        # 6.1) exchangeInfo cache
        try:
            ex_info = await client.exchange_info()
            portfolio.exchange_info_cache = ex_info
            logging.info("ExchangeInfo cached (%d symbols)", len(ex_info.get("symbols", [])))
        except Exception as e:
            logging.warning(f"exchangeInfo fetch skipped: {e}")
            portfolio.exchange_info_cache = {"symbols": []}

        # 6.2) RiskManager
        risk = RiskManager(cfg.get("risk", {}), cfg.get("leverage", {}), portfolio)
        risk.bind_order_cfg(cfg.get("order", {}))
        risk.bind_trailing_cfg(cfg.get("trailing", {}))
        risk.bind_client(client)
        risk.bind_margin_cfg(cfg.get("margin_guard", {}))
        # ATR provider, klines kurulduktan sonra bağlanacak
        logging.info("Portfolio and RiskManager ready")
    except Exception as e:
        logging.error(f"Portfolio/RiskManager init failed: {e}")
        await notifier.alert({"event": "startup_error", "msg": f"❌ Portfolio/RiskManager failed: {e}"})
        return

    # =======================
    # 7) MARKET STREAM
    # =======================
    try:
        stream = MarketStream(
            cfg=cfg,
            logger=logger,
            whitelist=cfg["symbols_whitelist"],
            tf_entry=cfg["strategy"]["timeframe_entry"],
            global_db="/opt/tradebot/veritabani/global_data.db",
        )
        logging.info("MarketStream initialized")
    except Exception as e:
        logging.error(f"MarketStream init failed: {e}")
        await notifier.alert({"event": "startup_error", "msg": f"❌ MarketStream failed: {e}"})
        return

    # =======================
    # 8) STRATEJİ
    # =======================
    try:
        strat_cfg = cfg.get("strategy", {}) or {}
        strat_name = strat_cfg.get("name", "dominance_trend")
        strat_cls = (
            STRATEGY_REGISTRY.get(strat_name)
            or STRATEGY_REGISTRY.get("dominance_trend")
            or (next(iter(STRATEGY_REGISTRY.values())) if STRATEGY_REGISTRY else None)
        )
        if strat_cls is None:
            raise RuntimeError(f"Strategy registry boş; '{strat_name}' yüklenemedi")
        strategy = strat_cls(strat_cfg)
        logging.info(f"✅ Strategy selected: {strat_name} → {strategy.__class__.__name__}")
    except Exception as e:
        logging.error(f"❌ Strategy init failed: {e}")
        await notifier.alert({"event": "startup_error", "msg": f"❌ Strategy failed: {e}"})
        return

    # =======================
    # 9) KLINES CACHE (ATR)
    # =======================
    klines = KlinesCache(
        client=client,
        symbols=cfg["symbols_whitelist"],
        interval=cfg["strategy"]["timeframe_entry"],
        limit=200,
        logger=logging.getLogger("klines_cache"),
    )
    if hasattr(risk, "bind_atr_provider"):
        risk.bind_atr_provider(klines.get_atr)

    # =======================
    # 10) NORMALIZER & ROUTER & YÖNETİCİLER
    # =======================
    try:
        normalizer = ExchangeNormalizer(binance_client=client, logger=logging.getLogger("normalizer"))

        router = OrderRouter(
            config=cfg,
            binance_client=client,
            normalizer=normalizer,
            logger=logger,
            paper_engine=None,
        )

        stop_manager = StopManager(
            router=router,
            persistence=persistence,
            cfg=cfg.get("stop", {}),
            logger=logging.getLogger("stop_manager"),
        )

        router.attach_trailing_context(
            get_last_price=stream.get_last_price,
            list_open_positions=persistence.list_open_positions,
            upsert_stop=stop_manager.upsert_stop_loss,
            get_atr=klines.get_atr,
        )

        reconciler = OrderReconciler(
            router=router,
            logger=logging.getLogger("reconciler"),
            persistence=persistence,
        )
        supervisor = PositionSupervisor(cfg, portfolio, notifier, persistence)

        ks_logger = logging.getLogger("kill_switch")
        kill_switch = KillSwitch(
            cfg=cfg,
            persistence=persistence,
            reconciler=reconciler,
            notifier=notifier,
            price_provider=lambda s: stream.get_last_price(s),
            logger=ks_logger,
        )

        order_manager = OrderManager(
            router=router,
            persistence=persistence,
            market_stream=stream,
            risk_manager=risk,
            logger=logging.getLogger("order_manager"),
        )

        if getattr(persistence, "router", None) is None:
            persistence.router = router

        logging.info("OrderRouter, Reconciler, Supervisor ready")
    except Exception as e:
        logging.error(f"Router/Reconciler/Supervisor init failed: {e}")
        await notifier.alert({"event": "startup_error", "msg": f"❌ Router/Reconciler/Supervisor failed: {e}"})
        return

    # =======================
    # 11) TAKE PROFIT / SWEEPER / OCO
    # =======================
    async def _orderbook_provider(symbol: str, depth: int = 50):
        depth_fn = getattr(client, "depth", None)
        if callable(depth_fn):
            res = depth_fn(symbol=symbol, limit=depth)
            if asyncio.iscoroutine(res):
                res = await res
        else:
            get_fn = getattr(client, "_get", None) or getattr(client, "http_get", None)
            if not callable(get_fn):
                return None
            res = get_fn("/fapi/v1/depth", params={"symbol": symbol, "limit": depth})
            if asyncio.iscoroutine(res):
                res = await res
        bids = [(float(p), float(q)) for p, q in (res.get("bids") or []) if float(q) > 0]
        asks = [(float(p), float(q)) for p, q in (res.get("asks") or []) if float(q) > 0]
        return {"bids": bids, "asks": asks}

    tp_manager = TakeProfitManager(
        router=router,
        persistence=persistence,
        cfg=cfg,
        logger=logging.getLogger("tp_manager"),
        orderbook_provider=_orderbook_provider,
    )

    sweeper = ProtectiveSweeper(
        router=router,
        persistence=persistence,
        logger=logging.getLogger("protective_sweeper"),
        interval_sec=int((cfg.get("take_profit") or {}).get("update_interval_sec", 30)),
    )
    oco = OCOWatcher(
        router=router,
        persistence=persistence,
        logger=logging.getLogger("oco_watcher"),
        interval_sec=5,
    )

    # =======================
    # 12) USER-DATA STREAM (WS + keepalive)
    # =======================
    uds = UserDataStream(
        client=client,
        notifier=notifier,
        persistence=persistence,
        router=router,
        oco_watcher=oco,
        sweeper=sweeper,
        logger=logging.getLogger("uds"),
        ws_base_url=cfg["binance"].get("ws_url", "wss://fstream.binance.com"),
        risk=risk,
    )

    # =======================
    # 13) GRACEFUL SHUTDOWN — stop EVENT ÖNCE!
    # =======================
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, stop.set)
        except NotImplementedError:
            pass

    # ======================================
    # 14) GÖREVLER (SCHEDULED TASKS)
    # ======================================
    tasks = []  # ← önce tanımla, sonra ekle

    # 14.1 – MarketStream
    tasks.append(asyncio.create_task(stream.run(), name="stream"))

    # 14.2 – Klines/ATR
    tasks.append(asyncio.create_task(klines.run(stop), name="klines"))

    # 14.3 – Strateji
    tasks.append(asyncio.create_task(
        strat_loop(
            stream, strategy, portfolio, supervisor, risk, router,
            persistence, notifier, cfg, kill_switch=kill_switch, order_manager=order_manager
        ),
        name="strat_loop",
    ))

    # 14.4 – Trailing
    tasks.append(asyncio.create_task(
        trailing_loop(router, stream, notifier, cfg, stop),
        name="trailing",
    ))

    # 14.5 – Kill-Switch loop
    tasks.append(asyncio.create_task(
        kill_switch_loop(kill_switch, notifier, interval_sec=10, stop_event=stop),
        name="kill_switch",
    ))

    # 14.6 – Kill-Switch günlük reset
    tasks.append(asyncio.create_task(
        daily_reset_loop(kill_switch, notifier, stop_event=stop, tz_offset_hours=3),
        name="ks_daily_reset",
    ))

    # 14.7 – TP Manager
    tasks.append(asyncio.create_task(
        tp_manager.run(stop),
        name="take_profit",
    ))

    # 14.8 – Protective Sweeper
    tasks.append(asyncio.create_task(
        sweeper.run(stop),
        name="protective_sweeper",
    ))

    # 14.9 – OCO Watcher
    tasks.append(asyncio.create_task(
        oco.run(stop),
        name="oco_watcher",
    ))

    # 14.10 – User-Data Stream (WS)
    tasks.append(asyncio.create_task(
        uds.run(stop),
        name="user_data_stream",
    ))

    # 14.11 – User-Data Keepalive
    tasks.append(asyncio.create_task(
        uds.keepalive_loop(stop, interval_sec=1800),
        name="user_data_keepalive",
    ))

    # 14.12 – Gün sonu PnL özeti (STOP tan sonra, TASKS içine!)
    tasks.append(asyncio.create_task(
        daily_pnl_summary_loop(
        persistence, notifier, stop,
        tz_offset_hours=3, run_at="23:59",
        price_provider=lambda s: stream.get_last_price(s),
        include_unrealized=True,
    ),
    name="pnl_daily_summary",
    ))

    await notifier.info_trades({"event": "startup", "msg": "✅ All modules initialized. Bot is running."})
    logging.info("All tasks scheduled. Bot is running.")

     # 14.13 – Position Risk Guard (liq proximity)
    pr_cfg = cfg.get("position_risk_guard", {}) or {}
    if pr_cfg.get("enabled", True):
        pr_symbols = pr_cfg.get("symbols") or cfg.get("symbols_whitelist") or []
        tasks.append(asyncio.create_task(
            position_risk_guard_loop(
                client=client,
                notifier=notifier,
                persistence=persistence,
                symbols=pr_symbols,
                stop_event=stop,
                interval_sec=int(pr_cfg.get("interval_sec", 60)),
                warn_pct=float(pr_cfg.get("distance_warn_pct", 3.0)),
                crit_pct=float(pr_cfg.get("distance_crit_pct", 1.5)),
            ),
            name="position_risk_guard",
        ))
        
        # 14.14 Performance Guard (intra-day PF & loss streak)
    pg = cfg.get("performance_guard", {}) or {}
    if pg.get("enabled", True):
        tasks.append(asyncio.create_task(
            performance_guard_loop(
                persistence=persistence,
                notifier=notifier,
                stop_event=stop,
                interval_sec=int(pg.get("interval_sec", 120)),
                profit_factor_floor=float(pg.get("profit_factor_floor", 1.0)),
                loss_streak_threshold=int(pg.get("loss_streak_threshold", 3)),
                min_trades=int(pg.get("min_trades", 5)),
                cooldown_sec=int(pg.get("cooldown_sec", 900)),
                tz_offset_hours=int(pg.get("tz_offset_hours", 3)),
            ),
            name="performance_guard",
        ))


    # =======================
    # 15) ÇALIŞTIR & KAPAT
    # =======================
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
