# /opt/tradebot/future_trade/app.py
# -*- coding: utf-8 -*-
from __future__ import annotations

import os
import signal
import asyncio
import contextlib
import logging
from pathlib import Path
from typing import Any, Dict

# ---- Proje içi importlar (modül kökünden) ----
from future_trade.config_loader import load_config                       # async config loader
from future_trade.telegram_notifier import Notifier                      # Telegram bildirimleri
from future_trade.persistence import Persistence                         # DB erişimi ve cache
from future_trade.binance_client import BinanceClient                    # Binance wrapper (async bootstrap)
from future_trade.portfolio import Portfolio                             # Portföy nesnesi
from future_trade.risk_manager import RiskManager                        # Risk yönetimi
from future_trade.market_stream import MarketStream                      # PAPER/dummy stream
from future_trade.order_router import OrderRouter                        # Emir yönlendirici
from future_trade.exchange_utils import ExchangeNormalizer               # Precision/minNotional normalizer
from future_trade.order_reconciler import OrderReconciler                # Emir mutabakatı (close + PnL)
from future_trade.position_supervisor import PositionSupervisor          # Pozisyon gözetmen
from future_trade.strategies import STRATEGY_REGISTRY                    # Strateji kayıt sistemi
from future_trade.loops import strat_loop, trailing_loop                 # Ana döngüler


# Varsayılan config yolu (ENV yoksa)
CONFIG_PATH = Path("/opt/tradebot/future_trade/config.json")


async def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    logger = logging.getLogger("TradeBot")
    print("Futures bot starting...")

    # ---------- CONFIG ----------
    cfg_path_env = os.environ.get("FUTURE_TRADE_CONFIG")
    cfg_file = Path(cfg_path_env) if cfg_path_env else CONFIG_PATH
    logging.info(f"[CONFIG] using: {cfg_file}")

    try:
        cfg: Dict[str, Any] = await load_config(cfg_file)
        logging.info("Config loaded OK")
    except Exception as e:
        logging.error(f"Config load failed: {e}")
        # notifier henüz yok; hızlı çık
        return

    # ---------- NOTIFIER ----------
    try:
        notifier = Notifier(cfg.get("telegram", {}))
        await notifier.info_trades({"event": "startup", "msg": "✅ Notifier initialized"})
        logging.info("Notifier initialized")
    except Exception as e:
        logging.error(f"Notifier init failed: {e}")
        return

    # ---------- DATABASE / PERSISTENCE ----------
    try:
        persistence = Persistence(
            path=cfg["database"]["path"],
            router=None,                       # router birazdan atanacak
            logger=logging.getLogger("db")
        )
        persistence.init_schema()
        await notifier.info_trades({"event": "startup", "msg": "✅ Database schema OK"})
        logging.info("Database schema initialized")
    except Exception as e:
        logging.error(f"Database init failed: {e}")
        await notifier.alert({"event": "startup_error", "msg": f"❌ DB init failed: {e}"})
        return

    # ---------- BINANCE CLIENT ----------
    try:
        app_section = cfg.get("app", {})
        mode = app_section.get("mode", cfg.get("mode", "paper")).lower()

        client = BinanceClient(cfg["binance"], mode)
        await client.bootstrap_exchange({
            "margin_mode": app_section.get("margin_mode", cfg.get("margin_mode", "ISOLATED")),
            "position_mode": app_section.get("position_mode", cfg.get("position_mode", "ONE_WAY")),
        })
        await notifier.info_trades({"event": "startup", "msg": "✅ Binance client OK"})
        logging.info("Binance client bootstrapped")
    except Exception as e:
        logging.error(f"Binance client failed: {e}")
        await notifier.alert({"event": "startup_error", "msg": f"❌ Binance client failed: {e}"})
        return

    # ---------- PORTFÖY & RISK ----------
    try:
        portfolio = Portfolio(persistence)

        # exchangeInfo önbelleği
        try:
            ex_info = await client.exchange_info()
            portfolio.exchange_info_cache = ex_info
            logging.info("ExchangeInfo cached (%d symbols)", len(ex_info.get("symbols", [])))
        except Exception as e:
            logging.warning(f"exchangeInfo fetch skipped: {e}")
            portfolio.exchange_info_cache = {"symbols": []}

        risk = RiskManager(cfg.get("risk", {}), cfg.get("leverage", {}), portfolio)
        risk.bind_order_cfg(cfg.get("order", {}))  # order paramlarını risk katmanına ver
        logging.info("Portfolio and RiskManager ready")
    except Exception as e:
        logging.error(f"Portfolio/RiskManager init failed: {e}")
        await notifier.alert({"event": "startup_error", "msg": f"❌ Portfolio/RiskManager failed: {e}"})
        return

    # ---------- MARKET STREAM ----------
    try:
        # Bizim MarketStream imzası: (cfg, logger, whitelist, tf_entry, global_db?)
        stream = MarketStream(
            cfg=cfg,
            logger=logger,
            whitelist=cfg["symbols_whitelist"],
            tf_entry=cfg["strategy"]["timeframe_entry"],
            global_db="/opt/tradebot/veritabani/global_data.db"  # global_endeksleri buradan okuyor
        )
        logging.info("MarketStream initialized")
    except Exception as e:
        logging.error(f"MarketStream init failed: {e}")
        await notifier.alert({"event": "startup_error", "msg": f"❌ MarketStream failed: {e}"})
        return
    
    # ---------- STRATEJİ ----------
    try:
        strat_cfg = cfg.get("strategy", {}) or {}
        strat_name = strat_cfg.get("name", "dominance_trend")

        # Sınıfı registry'den al; yoksa "dominance_trend"e, o da yoksa registry'nin ilk sınıfına düş
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
        await notifier.alert({
            "event": "startup_error",
            "msg": f"❌ Strategy failed: {e}"
        })
        return



    # ---------- ROUTER, RECONCILER, SUPERVISOR ----------
    try:
        # Normalizer: precision/minNotional düzeltmeleri için
        normalizer = ExchangeNormalizer(binance_client=client, logger=logging.getLogger("normalizer"))

        # Router: tüm config'i veriyoruz; mode'u 'app.mode'dan okur
        router = OrderRouter(
            config=cfg,
            binance_client=client,
            normalizer=normalizer,
            logger=logger,
            paper_engine=None  # istersen client.paper_engine bağlayabilirsin
        )

        # Trailing bağlamı
        router.attach_trailing_context(
            get_last_price=stream.get_last_price,
            list_open_positions=persistence.list_open_positions if hasattr(persistence, "list_open_positions") else lambda: [],
            upsert_stop=None  # LIVE'da upsert_stop bağlayacağız
        )

        # Reconciler: yeni imzaya göre (router + persistence + logger)
        reconciler = OrderReconciler(
            router=router,
            logger=logging.getLogger("reconciler"),
            persistence=persistence
        )

        supervisor = PositionSupervisor(cfg, portfolio, notifier, persistence)
        logging.info("OrderRouter, Reconciler, Supervisor ready")

        # Persistence'a router referansı ver (bazı işlemler için kullanmak isteyebilir)
        if hasattr(persistence, "router") and persistence.router is None:
            persistence.router = router

    except Exception as e:
        logging.error(f"Router/Reconciler/Supervisor init failed: {e}")
        await notifier.alert({"event": "startup_error", "msg": f"❌ Router/Reconciler/Supervisor failed: {e}"})
        return

    # ---------- Graceful shutdown ----------
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, stop.set)
        except NotImplementedError:
            pass

    # ---------- Görevler ----------
    tasks = [
        asyncio.create_task(stream.run(), name="stream"),
        asyncio.create_task(
            strat_loop(stream, strategy, portfolio, supervisor, risk, router, persistence, notifier, cfg),
            name="strat_loop",
        ),
        # reconciler.run() senin proje akışına bağlı; loop varsa aktif et
        # asyncio.create_task(reconciler.run(), name="reconciler"),
        asyncio.create_task(trailing_loop(router, stream, notifier, cfg, stop), name="trailing"),
    ]

    await notifier.info_trades({"event": "startup", "msg": "✅ All modules initialized. Bot is running."})
    logging.info("All tasks scheduled. Bot is running.")

    # ---------- Çalıştır & Kapat ----------
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
