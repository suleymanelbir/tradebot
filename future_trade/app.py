# /opt/tradebot/future_trade/app.py
# -*- coding: utf-8 -*-
from __future__ import annotations

# (0) STDLIB
import os, signal, asyncio, contextlib, logging
from pathlib import Path
from typing import Any, Dict

# (1) PROJECT IMPORTS
from future_trade.config_loader import load_config
from future_trade.telegram_notifier import Notifier
from future_trade.persistence import Persistence
from future_trade.binance_client import BinanceClient
from future_trade.portfolio import Portfolio
from future_trade.risk_manager import RiskManager
from future_trade.market_stream import MarketStream
from future_trade.order_router import OrderRouter
from future_trade.exchange_utils import ExchangeNormalizer
from future_trade.order_reconciler import OrderReconciler
from future_trade.position_supervisor import PositionSupervisor
from future_trade.strategies import STRATEGY_REGISTRY
from future_trade.kill_switch import KillSwitch
from future_trade.order_manager import OrderManager
from future_trade.stop_manager import StopManager
from future_trade.klines_cache import KlinesCache
from future_trade.loops import strat_loop, trailing_loop, kill_switch_loop, daily_reset_loop
from future_trade.take_profit_manager import TakeProfitManager
from future_trade.protective_sweeper import ProtectiveSweeper
from future_trade.oco_watcher import OCOWatcher 


# (0.1) DEFAULT CONFIG PATH
CONFIG_PATH = Path("/opt/tradebot/future_trade/config.json")


async def main() -> None:
    # 1) BOOT & LOGGING
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    logger = logging.getLogger("TradeBot")
    print("Futures bot starting...")

    # 2) LOAD CONFIG
    cfg_path_env = os.environ.get("FUTURE_TRADE_CONFIG")
    cfg_file = Path(cfg_path_env) if cfg_path_env else CONFIG_PATH
    logging.info(f"[CONFIG] using: {cfg_file}")
    try:
        cfg: Dict[str, Any] = await load_config(cfg_file)
        logging.info("Config loaded OK")
    except Exception as e:
        logging.error(f"Config load failed: {e}")
        return

    # 3) NOTIFIER
    try:
        notifier = Notifier(cfg.get("telegram", {}))
        await notifier.info_trades({"event": "startup", "msg": "✅ Notifier initialized"})
        logging.info("Notifier initialized")
    except Exception as e:
        logging.error(f"Notifier init failed: {e}")
        return

    # 4) DB / PERSISTENCE
    try:
        persistence = Persistence(
            path=cfg["database"]["path"],
            router=None,
            logger=logging.getLogger("db")
        )
        persistence.init_schema()
        await notifier.info_trades({"event": "startup", "msg": "✅ Database schema OK"})
        logging.info("Database schema initialized")
    except Exception as e:
        logging.error(f"Database init failed: {e}")
        await notifier.alert({"event": "startup_error", "msg": f"❌ DB init failed: {e}"})
        return

    # 5) BINANCE CLIENT (bootstrap margin/position mode)
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
            # -4059: "No need to change position side." → zaten ONE_WAY; INFO seviyesine indir.
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

    # 6) NORMALIZER (erken kur)
    normalizer = ExchangeNormalizer(binance_client=client, logger=logging.getLogger("normalizer"))
    # (opsiyonel) exchangeInfo ön ısıtma:
    try:
        await normalizer.warmup()
    except Exception:
        pass

    # 7) PORTFOLIO & RISK
    try:
        portfolio = Portfolio(persistence)
        try:
            ex_info = await client.exchange_info()
            portfolio.exchange_info_cache = ex_info
            logging.info("ExchangeInfo cached (%d symbols)", len(ex_info.get("symbols", [])))
        except Exception as e:
            logging.warning(f"exchangeInfo fetch skipped: {e}")
            portfolio.exchange_info_cache = {"symbols": []}

        risk = RiskManager(cfg.get("risk", {}), cfg.get("leverage", {}), portfolio)
        # risk kancaları — ATR sağlayıcısını klines kurulunca bağlayacağız
        risk.bind_order_cfg(cfg.get("order", {}))
        risk.bind_trailing_cfg(cfg.get("trailing", {}))
        risk.bind_normalizer(normalizer)
        logging.info("Portfolio and RiskManager ready")
    except Exception as e:
        logging.error(f"Portfolio/RiskManager init failed: {e}")
        await notifier.alert({"event": "startup_error", "msg": f"❌ Portfolio/RiskManager failed: {e}"})
        return

    # 8) MARKET STREAM
    try:
        stream = MarketStream(
            cfg=cfg,
            logger=logger,
            whitelist=cfg["symbols_whitelist"],
            tf_entry=cfg["strategy"]["timeframe_entry"],
            global_db="/opt/tradebot/veritabani/global_data.db"
        )
        logging.info("MarketStream initialized")
    except Exception as e:
        logging.error(f"MarketStream init failed: {e}")
        await notifier.alert({"event": "startup_error", "msg": f"❌ MarketStream failed: {e}"})
        return

    # 9) STRATEGY
    try:
        strat_cfg = cfg.get("strategy", {}) or {}
        strat_name = strat_cfg.get("name", "dominance_trend")
        strat_cls = (STRATEGY_REGISTRY.get(strat_name)
                     or STRATEGY_REGISTRY.get("dominance_trend")
                     or (next(iter(STRATEGY_REGISTRY.values())) if STRATEGY_REGISTRY else None))
        if strat_cls is None:
            raise RuntimeError(f"Strategy registry boş; '{strat_name}' yüklenemedi")
        strategy = strat_cls(strat_cfg)
        logging.info(f"✅ Strategy selected: {strat_name} → {strategy.__class__.__name__}")
    except Exception as e:
        logging.error(f"❌ Strategy init failed: {e}")
        await notifier.alert({"event": "startup_error", "msg": f"❌ Strategy failed: {e}"})
        return

    # 10) KLINES CACHE (ATR için OHLC besleme)
    klines = KlinesCache(
        client=client,
        symbols=cfg["symbols_whitelist"],
        interval=cfg["strategy"]["timeframe_entry"],
        limit=200,
        logger=logging.getLogger("klines_cache")
    )
    # risk’e ATR sağlayıcıyı şimdi bağla (klines artık var)
    try:
        risk.bind_atr_provider(klines.get_atr)
    except Exception:
        pass

    # 11) ROUTER (normalizer ile)
    try:
        router = OrderRouter(
            config=cfg,
            binance_client=client,
            normalizer=normalizer,
            logger=logger,
            paper_engine=None
        )
    except Exception as e:
        logging.error(f"Router init failed: {e}")
        await notifier.alert({"event": "startup_error", "msg": f"❌ Router init failed: {e}"})
        return

    # 12) STOP MANAGER → TRAILING CONTEXT
    stop_manager = StopManager(
        router=router,
        persistence=persistence,
        cfg=cfg,
        logger=logging.getLogger("stop_manager")
    )
    router.attach_trailing_context(
        get_last_price=stream.get_last_price,
        list_open_positions=persistence.list_open_positions,
        upsert_stop=stop_manager.upsert_stop_loss,
        get_atr=klines.get_atr
    )

    #12_1) OTOMATİK ZOMBİ EMİR TEMİZLEME 
    sweeper = ProtectiveSweeper(
        router=router,
        persistence=persistence,
        logger=logging.getLogger("protective_sweeper"),
        interval_sec=int((cfg.get("take_profit") or {}).get("update_interval_sec", 30))
    )
   # ----------------
    # 13) TAKE_PROFİT MANAGER → TRAILING CONTEXT → → ORDER_BOOK
    # app.py içinde, tp_manager oluşturma kısmı:
    async def _orderbook_provider(symbol: str, depth: int = 50):
        """
        Binance depth endpoint'inden kitap döndürür.
        Sync/async client farkını TP manager zaten tolere ediyor ama
        burada da olabildiğince async kullanıyoruz.
        Dönüş: {"bids":[(p,q),...], "asks":[(p,q),...]}
        """
        client = router.client  # same instance
        # Eğer client.depth mevcutsa onu tercih et
        depth_fn = getattr(client, "depth", None)
        if callable(depth_fn):
            res = depth_fn(symbol=symbol, limit=depth)
            if asyncio.iscoroutine(res):
                res = await res
        else:
            # imzasız GET ile /fapi/v1/depth
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
        orderbook_provider=_orderbook_provider,   # <<< eklendi
    )
    # 13_1) OCO Watcher’ı başlat
    oco = OCOWatcher(
        router=router,
        persistence=persistence,
        logger=logging.getLogger("oco_watcher"),
        interval_sec=5
    )

#--------------------
    # (opsiyonel) bağlama da ekleyelim
    router.attach_trailing_context(
        get_last_price=stream.get_last_price,
        list_open_positions=persistence.list_open_positions,
        upsert_stop=stop_manager.upsert_stop_loss,
        get_atr=klines.get_atr,
        upsert_tp=tp_manager.upsert_take_profit,   # <<< eklendi (kullanmasan da zararı yok)
    )

    # 14) RECONCILER & SUPERVISOR
    try:
        reconciler = OrderReconciler(
            router=router,
            logger=logging.getLogger("reconciler"),
            persistence=persistence
        )
        supervisor = PositionSupervisor(cfg, portfolio, notifier, persistence)
        logging.info("OrderRouter, Reconciler, Supervisor ready")
        if hasattr(persistence, "router") and persistence.router is None:
            persistence.router = router
    except Exception as e:
        logging.error(f"Router/Reconciler/Supervisor init failed: {e}")
        await notifier.alert({"event": "startup_error", "msg": f"❌ Router/Reconciler/Supervisor failed: {e}"})
        return

    # 15) KILL-SWITCH & ORDER MANAGER
    ks_logger = logging.getLogger("kill_switch")
    kill_switch = KillSwitch(
        cfg=cfg,
        persistence=persistence,
        reconciler=reconciler,
        notifier=notifier,
        price_provider=lambda s: stream.get_last_price(s),
        logger=ks_logger
    )
    order_manager = OrderManager(
        router=router,
        persistence=persistence,
        market_stream=stream,
        risk_manager=risk,
        logger=logging.getLogger("order_manager"),
        kill_switch=kill_switch,
        limits_cfg=cfg.get("limits", {})
    )

    # 16) GRACEFUL SHUTDOWN
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, stop.set)
        except NotImplementedError:
            pass

    # 17) TASK SCHEDULER (tekil ve sıralı)
    # 17.1) Market verisi (ticker/aggTrade vb.) — strateji ve trailing için ana besleme
    stream_task = asyncio.create_task(stream.run(), name="stream")

    # 17.2) Klines/ATR beslemesi — ATR tabanlı trailing ve risk için OHLC cache
    klines_task = asyncio.create_task(klines.run(stop), name="klines")

    # 17.3) Strateji döngüsü — sinyal üretimi, OrderManager ile giriş/çıkış
    strat_task = asyncio.create_task(
        strat_loop(
            stream, strategy, portfolio, supervisor, risk, router,
            persistence, notifier, cfg, kill_switch=kill_switch, order_manager=order_manager
        ),
        name="strat_loop",
    )

    # 17.4) Trailing döngüsü — açık pozisyonlarda SL seviyesini güncelle (StopManager → upsert_stop_loss)
    trailing_task = asyncio.create_task(
        trailing_loop(router, stream, notifier, cfg, stop),
        name="trailing"
    )

    # 17.5) Kill-Switch döngüsü — DD/PnL limitlerini periyodik kontrol, tetiklenirse tüm pozisyonları kapat
    ks_task = asyncio.create_task(
        kill_switch_loop(kill_switch, notifier, interval_sec=10, stop_event=stop),
        name="kill_switch"
    )

    # 17.6) Kill-Switch günlük reset — gün başında sayaçları ve günlük zarar limitini sıfırla
    ks_reset_task = asyncio.create_task(
        daily_reset_loop(kill_switch, notifier, stop_event=stop, tz_offset_hours=3),
        name="ks_daily_reset"
    )

    # 17.7) Take-Profit döngüsü — TP hedefini (örn. %pct) periyodik hesapla, anlamlı değişimde cancel+new yap
    tp_task = asyncio.create_task(
        tp_manager.run(stop),
        name="take_profit"
    )

    # 17.8) Protective Sweeper — pozisyon yoksa reduceOnly STOP/TP emirlerini iptal eder
    sweeper_task = asyncio.create_task(
        sweeper.run(stop),
        name="protective_sweeper"
    )

    tasks = [
        stream_task,
        klines_task,
        strat_task,
        trailing_task,
        ks_task,
        ks_reset_task,
        tp_task,
        sweeper_task,   # <<< eklendi
    ]

    # 17.9) OCO Watcher — SL ya da TP kaybolduysa diğerini iptal eder (biri tetiklenince diğeri söner)
    oco_task = asyncio.create_task(
        oco.run(stop),
        name="oco_watcher"
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
        oco_task,         # <<< eklendi
    ]


    # 17.x) Görev seti (iptal/temizlik için referans)
    tasks = [
        stream_task,
        klines_task,
        strat_task,
        trailing_task,
        ks_task,
        ks_reset_task,
        tp_task,
    ]

    # 17.y) Başlatma bildirimi
    await notifier.info_trades({"event": "startup", "msg": "✅ All modules initialized. Bot is running."})
    logging.info("All tasks scheduled. Bot is running.")


    # 18) RUN & CLEANUP
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
