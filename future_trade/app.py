# /opt/tradebot/future_trade/app.py
# -*- coding: utf-8 -*-
from __future__ import annotations

# =========================
# TradeBot Uygulama Girişi
# =========================
# 0) Standart kütüphaneler
import os, signal, asyncio, contextlib, logging
from pathlib import Path
from typing import Any, Dict

# 1) Proje içi modüller
from future_trade.config_loader import load_config
from future_trade.telegram_notifier import Notifier
from future_trade.persistence import Persistence
from future_trade.binance_client import BinanceClient
from future_trade.portfolio import Portfolio
from future_trade.risk_manager import RiskManager
from future_trade.market_stream import MarketStream
from future_trade.exchange_utils import ExchangeNormalizer
from future_trade.order_router import OrderRouter
from future_trade.order_reconciler import OrderReconciler
from future_trade.position_supervisor import PositionSupervisor
from future_trade.strategies import STRATEGY_REGISTRY
from future_trade.kill_switch import KillSwitch
from future_trade.stop_manager import StopManager
from future_trade.order_manager import OrderManager
from future_trade.klines_cache import KlinesCache
from future_trade.take_profit_manager import TakeProfitManager
from future_trade.protective_sweeper import ProtectiveSweeper
from future_trade.oco_watcher import OCOWatcher
from future_trade.user_data_stream import UserDataStream
from future_trade.loops import (
    strat_loop,
    trailing_loop,
    kill_switch_loop,
    daily_reset_loop,
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
    # 3) NOTIFIER
    # =======================
    try:
        notifier = Notifier(cfg.get("telegram", {}))
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
            router=None,  # router birazdan set edilecek
            logger=logging.getLogger("db"),
        )
        persistence.init_schema()
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
            # -4059: No need to change position side → benign
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

        # 6.2) RiskManager (tek örnek)
        risk = RiskManager(cfg.get("risk", {}), cfg.get("leverage", {}), portfolio)
        risk.bind_order_cfg(cfg.get("order", {}))
        risk.bind_trailing_cfg(cfg.get("trailing", {}))
        risk.bind_client(client)                 # Margin Guard için client
        risk.bind_margin_cfg(cfg.get("margin_guard", {}))
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
    # ATR sağlayıcısını risk'e bağla
    if hasattr(risk, "bind_atr_provider"):
        risk.bind_atr_provider(klines.get_atr)

    # =======================
    # 10) NORMALIZER & ROUTER
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

        # 10.1) StopManager
        stop_manager = StopManager(
            router=router,
            persistence=persistence,
            cfg=cfg.get("stop", {}),
            logger=logging.getLogger("stop_manager"),
        )

        # 10.2) Trailing bağlamı (tek ve doğru)
        router.attach_trailing_context(
            get_last_price=stream.get_last_price,
            list_open_positions=persistence.list_open_positions,
            upsert_stop=stop_manager.upsert_stop_loss,
            get_atr=klines.get_atr,
        )

        # 10.3) Reconciler & Supervisor
        reconciler = OrderReconciler(
            router=router,
            logger=logging.getLogger("reconciler"),
            persistence=persistence,
        )
        supervisor = PositionSupervisor(cfg, portfolio, notifier, persistence)

        # 10.4) Kill-Switch
        ks_logger = logging.getLogger("kill_switch")
        kill_switch = KillSwitch(
            cfg=cfg,
            persistence=persistence,
            reconciler=reconciler,
            notifier=notifier,
            price_provider=lambda s: stream.get_last_price(s),
            logger=ks_logger,
        )

        # 10.5) OrderManager (tek örnek, risk/stream/router ile)
        order_manager = OrderManager(
            router=router,
            persistence=persistence,
            market_stream=stream,
            risk_manager=risk,
            logger=logging.getLogger("order_manager"),
        )

        # Persistence’a router referansı ver
        if getattr(persistence, "router", None) is None:
            persistence.router = router

        logging.info("OrderRouter, Reconciler, Supervisor ready")
    except Exception as e:
        logging.error(f"Router/Reconciler/Supervisor init failed: {e}")
        await notifier.alert({"event": "startup_error", "msg": f"❌ Router/Reconciler/Supervisor failed: {e}"})
        return

    # =======================
    # 11) TAKE PROFIT MANAGER (RR + Orderbook Snap)
    # =======================
    async def _orderbook_provider(symbol: str, depth: int = 50):
        # Binance depth → {"bids":[(p,q)..], "asks":[(p,q)..]}
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

    # =======================
    # 12) PROTECTIVE SWEEPER & OCO WATCHER
    # =======================
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
    # 13) USER-DATA STREAM (WS + keepalive)
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
    )

    # =======================
    # 14) GRACEFUL SHUTDOWN
    # =======================
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, stop.set)
        except NotImplementedError:
            pass

    # ======================================
    # 15) GÖREVLER (SCHEDULED TASKS)
    # ======================================
    # 17.1 – Market verisi (tick/aggTrade)
    stream_task = asyncio.create_task(stream.run(), name="stream")

    # 17.2 – Klines/ATR cache
    klines_task = asyncio.create_task(klines.run(stop), name="klines")

    # 17.3 – Strateji döngüsü (OrderManager & KillSwitch ile)
    strat_task = asyncio.create_task(
        strat_loop(
            stream, strategy, portfolio, supervisor, risk, router,
            persistence, notifier, cfg, kill_switch=kill_switch, order_manager=order_manager
        ),
        name="strat_loop",
    )

    # 17.4 – Trailing döngüsü (StopManager.upsert_stop_loss)
    trailing_task = asyncio.create_task(
        trailing_loop(router, stream, notifier, cfg, stop),
        name="trailing",
    )

    # 17.5 – Kill-Switch sürekli kontrol
    ks_task = asyncio.create_task(
        kill_switch_loop(kill_switch, notifier, interval_sec=10, stop_event=stop),
        name="kill_switch",
    )

    # 17.6 – Kill-Switch günlük reset (UTC+3)
    ks_reset_task = asyncio.create_task(
        daily_reset_loop(kill_switch, notifier, stop_event=stop, tz_offset_hours=3),
        name="ks_daily_reset",
    )

    # 17.7 – Take-Profit (RR + Orderbook snap) periyodik güncelle
    tp_task = asyncio.create_task(
        tp_manager.run(stop),
        name="take_profit",
    )

    # 17.8 – Protective Sweeper (pozisyon yoksa koruyucu emirleri süpür)
    sweeper_task = asyncio.create_task(
        sweeper.run(stop),
        name="protective_sweeper",
    )

    # 17.9 – OCO Watcher (SL/TP biri tetiklenirse karşıtını iptal)
    oco_task = asyncio.create_task(
        oco.run(stop),
        name="oco_watcher",
    )

    # 17.10 – User-Data Stream (WS push: ORDER_TRADE_UPDATE, ACCOUNT_UPDATE)
    uds_task = asyncio.create_task(
        uds.run(stop),
        name="user_data_stream",
    )

    # 17.11 – User-Data Keepalive (30 dakikada bir listenKey yenile ve alert)
    uds_keepalive_task = asyncio.create_task(
        uds.keepalive_loop(stop, interval_sec=1800),
        name="user_data_keepalive",
    )

    tasks = [
        stream_task,
        klines_task,
        strat_task,
        trailing_task,
        ks_task,
        ks_reset_task,
        tp_task,
        sweeper_task,
        oco_task,
        uds_task,
        uds_keepalive_task,
    ]

    await notifier.info_trades({"event": "startup", "msg": "✅ All modules initialized. Bot is running."})
    logging.info("All tasks scheduled. Bot is running.")

    # =======================
    # 16) ÇALIŞTIR & KAPAT
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
