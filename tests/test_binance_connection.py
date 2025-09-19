# /opt/tradebot/tests/test_binance_connection.py
# Bu dosyanın amacı Binance bağlantısını ve yapılandırmayı test etmektir

import pytest
import asyncio
import json
import os
import pathlib
import logging
from future_trade.binance_client import BinanceClient

logging.basicConfig(level=logging.INFO)

@pytest.mark.asyncio
async def test_binance_connection():
    config_path = os.environ.get("FUTURE_TRADE_CONFIG", "/opt/tradebot/future_trade/config.json")
    cfg = json.loads(pathlib.Path(config_path).read_text())
    client = BinanceClient(cfg["binance"], cfg.get("mode", "testnet"))

    try:
        account = await client._signed("GET", "/fapi/v2/account", {})
        logging.info("✅ ACCOUNT OK; canTrade = %s", account.get("canTrade", "?"))
        assert account.get("canTrade") is True or account.get("canTrade") is False  # canTrade değeri beklenmeli

        test_order = await client._signed("POST", "/fapi/v1/order/test", {
            "symbol": "SOLUSDT",
            "side": "BUY",
            "type": "STOP_MARKET",
            "stopPrice": 275.88,
            "closePosition": "true",
            "workingType": "MARK_PRICE"
        })
        logging.info("✅ ORDER TEST OK: %s", test_order)
        assert test_order == {}  # Binance test endpoint boş dict döner
    finally:
        await client.close()
