# /opt/tradebot/tests/test_binance_balance.py
# Bu dosyanın amacı Binance Futures bakiyesini test etmektir

import pytest
import asyncio
import json
import os
import pathlib
import logging
from future_trade.binance_client import BinanceClient

logging.basicConfig(level=logging.INFO)

def resolve_binance_credentials(cfg: dict) -> dict:
    # 1. Config içinden doğrudan al
    key = cfg.get("key")
    secret = cfg.get("secret")

    # 2. Eğer yoksa, env üzerinden al
    if not key:
        env_key_name = cfg.get("api_key_env", "BINANCE_API_KEY")
        key = os.environ.get(env_key_name)

    if not secret:
        env_secret_name = cfg.get("api_secret_env", "BINANCE_API_SECRET")
        secret = os.environ.get(env_secret_name)

    # 3. Hâlâ yoksa uyarı ver
    if not key or not secret:
        logging.warning("⚠️ Binance API key veya secret bulunamadı. Config ve env kontrol edilmeli.")
        return None

    return {
        "key": key,
        "secret": secret,
        "base_url": cfg.get("testnet_base_url", "https://testnet.binancefuture.com"),
        "recv_window_ms": cfg.get("recv_window_ms", 5000)
    }

@pytest.mark.asyncio
async def test_binance_balance_fetch():
    config_path = os.environ.get("FUTURE_TRADE_CONFIG", "/opt/tradebot/future_trade/config.json")
    cfg = json.loads(pathlib.Path(config_path).read_text())
    binance_cfg = cfg.get("binance", {})
    creds = resolve_binance_credentials(binance_cfg)

    if creds is None:
        pytest.skip("❌ API key/secret bulunamadığı için test atlandı.")

    # BinanceClient config'i güncellenmiş key/secret ile oluşturulmalı
    binance_cfg["key"] = creds["key"]
    binance_cfg["secret"] = creds["secret"]
    client = BinanceClient(binance_cfg, cfg.get("mode", "testnet"))

    try:
        account = await client._signed("GET", "/fapi/v2/account", {})
        assets = account.get("assets", [])
        logging.info("✅ BALANCE FETCH OK; asset count = %d", len(assets))

        assert isinstance(assets, list)
        assert len(assets) > 0

        for asset in assets:
            assert "asset" in asset
            assert "walletBalance" in asset
            logging.info("🔹 %s: %s USDT", asset["asset"], asset["walletBalance"])
    finally:
        await client.close()
