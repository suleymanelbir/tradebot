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
from future_trade.kill_switch import KillSwitch                          # Kill-Switch kontrolü
from future_trade.loops import strat_loop, trailing_loop, kill_switch_loop, daily_reset_loop  # Ana döngüler
from future_trade.stop_manager import StopManager                        # Stop yönetimi
from future_trade.order_manager import OrderManager                      # Emir yönetimi (geçici kullanım için hazırlandı)


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
        risk.bind_order_cfg(cfg.get("order", {}))              # order paramlarını risk katmanına ver
        risk.bind_client(client)                               # client referansını risk katmanına ver
        risk.bind_trailing_cfg(cfg.get("trailing", {}))        # trailing yapılandırmasını bağla
        risk.bind_margin_cfg(cfg.get("margin_guard", {}))      # margin guard yapılandırmasını bağla
        logging.info("Portfolio and RiskManager ready")
    except Exception as e:
        logging.error(f"Portfolio/RiskManager init failed: {e}")
        await notifier.alert({"event": "startup_error", "msg": f"❌ Portfolio/RiskManager failed: {e}"})
        return

    # ---------- ORDER MANAGER (geçici kullanım) ----------
    try:
        order_manager = OrderManager()  # ileride strateji entegrasyonu için hazırlandı
        _ = order_manager  # Pyflakes uyarısını bastırmak için geçici kullanım
    except Exception as e:
        logging.warning(f"OrderManager init skipped: {e}")


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
        # ✅ StopManager: router hazır olduğunda oluşturulmalı
        stop_manager = StopManager(
            router=router,
            persistence=persistence,
             cfg=cfg.get("stop", {}),
            logger=logging.getLogger("stop_manager")
        )

        # ✅ Trailing context: artık stop_manager üzerinden stop güncellemesi yapılabilir
        router.attach_trailing_context(
            get_last_price=stream.get_last_price,
            list_open_positions=persistence.list_open_positions if hasattr(persistence, "list_open_positions") else lambda: [],
            upsert_stop=stop_manager.upsert_stop_loss  # <<< aktif hale geldi

        )
        # RiskManager: config ve portfolio ile oluştur
        risk_cfg = cfg.get("risk", {})
        leverage_map = cfg.get("leverage_map", {})
        risk_manager = RiskManager(
            risk_cfg=risk_cfg,
            leverage_map=leverage_map,
            portfolio=portfolio  # daha önce tanımlanmış olmalı
        )
        logger.info(f"[RISK INIT] per_trade={risk_manager.per_trade_risk_pct:.4f} daily_max_loss={risk_manager.daily_max_loss_pct:.4f} "
                    f"max_concurrent={risk_manager.max_concurrent} kill_switch_after={risk_manager.kill_switch_after} buffer={risk_manager.min_notional_buffer}")


        # 2️⃣ OrderManager: router hazır olduğunda oluşturulabilir
        order_manager = OrderManager(
            router=router,
            persistence=persistence,
            market_stream=stream,
            risk_manager=risk_manager,  # ← eksiksiz parametre adı!
            logger=logger
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

        # Kill-Switch
        ks_logger = logging.getLogger("kill_switch")
        kill_switch = KillSwitch(
            cfg=cfg,
            persistence=persistence,
            reconciler=reconciler,
            notifier=notifier,
            price_provider=lambda s: stream.get_last_price(s),
            logger=ks_logger
        )


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
    asyncio.create_task(daily_reset_loop(kill_switch, notifier, stop_event=stop, tz_offset_hours=3), name="ks_daily_reset"), # İstanbul saati (UTC+3)

    tasks = [
        asyncio.create_task(stream.run(), name="stream"),
        asyncio.create_task(
            strat_loop(stream, strategy, portfolio, supervisor, risk, router, persistence, notifier, cfg, kill_switch=kill_switch),
            name="strat_loop",
        ),
        asyncio.create_task(trailing_loop(router, stream, notifier, cfg, stop), name="trailing"),
        asyncio.create_task(kill_switch_loop(kill_switch, notifier, interval_sec=10, stop_event=stop), name="kill_switch, order_manager=order_manager"),
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
