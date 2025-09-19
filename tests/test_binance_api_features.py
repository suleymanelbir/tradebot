# Binance Futures API Özellik Testi
# Amaç: API'nin kapsamını gözlemlemek, stratejik kullanım alanlarını belirlemek

import pytest
import asyncio
import json
import os
import pathlib
import logging
from future_trade.binance_client import BinanceClient

logging.basicConfig(level=logging.INFO)

@pytest.fixture(scope="module")
def client():
    config_path = os.environ.get("FUTURE_TRADE_CONFIG", "/opt/tradebot/future_trade/config.json")
    cfg = json.loads(pathlib.Path(config_path).read_text())
    client = BinanceClient(cfg["binance"], cfg.get("mode", "testnet"))
    yield client
    asyncio.get_event_loop().run_until_complete(client.close())

# 1️⃣ Hesap bilgisi ve işlem yetkisi
@pytest.mark.asyncio
async def test_account_info(client):
    account = await client._signed("GET", "/fapi/v2/account", {})
    logging.info("✅ ACCOUNT: canTrade = %s, totalMarginBalance = %s", account.get("canTrade"), account.get("totalMarginBalance"))
    assert "canTrade" in account

# 2️⃣ Pozisyon riski analizi
@pytest.mark.asyncio
async def test_position_risk(client):
    positions = await client._signed("GET", "/fapi/v2/positionRisk", {})
    logging.info("✅ POSITION RISK: %d aktif pozisyon", len(positions))
    assert isinstance(positions, list)

# 3️⃣ Emir defteri (depth)
@pytest.mark.asyncio
async def test_order_book(client):
    depth = await client._public("GET", "/fapi/v1/depth", {"symbol": "BTCUSDT", "limit": 5})
    logging.info("✅ ORDER BOOK: bid = %s, ask = %s", depth["bids"][0], depth["asks"][0])
    assert "bids" in depth and "asks" in depth

# 4️⃣ Fonlama oranı bilgisi
@pytest.mark.asyncio
async def test_funding_rate(client):
    rate = await client._public("GET", "/fapi/v1/fundingRate", {"symbol": "BTCUSDT", "limit": 1})
    logging.info("✅ FUNDING RATE: %s", rate[0])
    assert isinstance(rate, list)

# 5️⃣ Kaldıraç ayarı (sadece testnette çalışır)
@pytest.mark.asyncio
async def test_leverage_setting(client):
    result = await client._signed("POST", "/fapi/v1/leverage", {"symbol": "BTCUSDT", "leverage": 10})
    logging.info("✅ LEVERAGE SET: %s", result)
    assert result.get("leverage") == 10

# 6️⃣ Test emri gönderme
@pytest.mark.asyncio
async def test_order_test(client):
    test_order = await client._signed("POST", "/fapi/v1/order/test", {
        "symbol": "BTCUSDT",
        "side": "BUY",
        "type": "LIMIT",
        "price": 20000,
        "quantity": 0.01,
        "timeInForce": "GTC"
    })
    logging.info("✅ ORDER TEST OK")
    assert test_order == {}

# 7️⃣ Gerçek emir gönderme (isteğe bağlı, testnette aktif pozisyon açar)
# @pytest.mark.asyncio
# async def test_real_order(client):
#     order = await client._signed("POST", "/fapi/v1/order", {
#         "symbol": "BTCUSDT",
#         "side": "BUY",
#         "type": "MARKET",
#         "quantity": 0.01
#     })
#     logging.info("✅ REAL ORDER: %s", order)
#     assert "orderId" in order

