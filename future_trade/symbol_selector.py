import asyncio
import aiohttp
import json
import logging
from typing import List, Dict, Any

# 🔧 Ayarlanabilir filtre eşikleri
SETUP = {
    "volume_threshold": 5_000_000,
    "price_rise_threshold": 0.1,
    "price_drop_threshold": -0.1,
    "vwap_bias_threshold": 0.1,
    "strong_bias_threshold": 0.2,
}

class BinanceClient:
    def __init__(self, api_key: str, api_secret: str):
        self.api_key = api_key
        self.api_secret = api_secret
        self.session = None

    async def initialize(self):
        self.session = aiohttp.ClientSession()

    async def close(self):
        await self.session.close()

    async def get_exchange_info(self):
        async with self.session.get("https://fapi.binance.com/fapi/v1/exchangeInfo") as resp:
            return (await resp.json())["symbols"]

    async def get_24hr_ticker(self, symbol):
        async with self.session.get(f"https://fapi.binance.com/fapi/v1/ticker/24hr?symbol={symbol}") as resp:
            return await resp.json()

    async def get_klines(self, symbol, interval="5m", limit=3):
        async with self.session.get(f"https://fapi.binance.com/fapi/v1/klines?symbol={symbol}&interval={interval}&limit={limit}") as resp:
            return await resp.json()

class FilterTester:
    def __init__(self, client: BinanceClient):
        self.client = client
        self.logger = logging.getLogger("filter_tester")

    async def get_perpetual_symbols(self) -> List[str]:
        info = await self.client.get_exchange_info()
        return [s["symbol"] for s in info if s.get("contractType") == "PERPETUAL"]

    async def test_volume(self) -> List[str]:
        symbols = await self.get_perpetual_symbols()
        passed = []
        for sym in symbols:
            try:
                ticker = await self.client.get_24hr_ticker(sym)
                volume = float(ticker.get("quoteVolume", 0))
                if volume >= SETUP["volume_threshold"]:
                    passed.append(sym)
            except Exception as e:
                self.logger.warning(f"[volume_test ERROR] {sym}: {str(e)}")
        return passed

    async def test_price_rise(self) -> List[str]:
        symbols = await self.get_perpetual_symbols()
        passed = []
        for sym in symbols:
            try:
                klines = await self.client.get_klines(sym)
                if len(klines) < 2:
                    continue
                p_now = float(klines[-1][4])
                p_prev = float(klines[-2][4])
                price_change = (p_now - p_prev) / p_prev * 100
                if price_change >= SETUP["price_rise_threshold"]:
                    passed.append(sym)
            except Exception as e:
                self.logger.warning(f"[price_rise_test ERROR] {sym}: {str(e)}")
        return passed

    async def test_price_drop(self) -> List[str]:
        symbols = await self.get_perpetual_symbols()
        passed = []
        for sym in symbols:
            try:
                klines = await self.client.get_klines(sym)
                if len(klines) < 2:
                    continue
                p_now = float(klines[-1][4])
                p_prev = float(klines[-2][4])
                price_change = (p_now - p_prev) / p_prev * 100
                if price_change <= SETUP["price_drop_threshold"]:
                    passed.append(sym)
            except Exception as e:
                self.logger.warning(f"[price_drop_test ERROR] {sym}: {str(e)}")
        return passed

    async def test_vwap_bias(self) -> List[str]:
        symbols = await self.get_perpetual_symbols()
        passed = []
        for sym in symbols:
            try:
                klines = await self.client.get_klines(sym)
                if len(klines) < 2:
                    continue
                p_now = float(klines[-1][4])
                vwap = self._vwap(klines)
                if vwap == 0:
                    continue
                bias = abs((p_now - vwap) / vwap * 100)
                if bias >= SETUP["vwap_bias_threshold"]:
                    passed.append(sym)
            except Exception as e:
                self.logger.warning(f"[vwap_test ERROR] {sym}: {str(e)}")
        return passed

    async def find_strong_candidates(self) -> Dict[str, List[str]]:
        symbols = await self.get_perpetual_symbols()
        long_candidates = []
        short_candidates = []

        for sym in symbols:
            try:
                klines = await self.client.get_klines(sym)
                if len(klines) < 2:
                    continue
                p_now = float(klines[-1][4])
                p_prev = float(klines[-2][4])
                price_change = (p_now - p_prev) / p_prev * 100
                vwap = self._vwap(klines)
                if vwap == 0:
                    continue
                bias = (p_now - vwap) / vwap * 100

                if price_change >= SETUP["price_rise_threshold"] and bias >= SETUP["strong_bias_threshold"] and p_now > vwap:
                    long_candidates.append(sym)

                if price_change <= SETUP["price_drop_threshold"] and bias <= -SETUP["strong_bias_threshold"] and p_now < vwap:
                    short_candidates.append(sym)

            except Exception as e:
                self.logger.warning(f"[strong_candidate ERROR] {sym}: {str(e)}")

        return {
            "long": long_candidates,
            "short": short_candidates
        }

    def _vwap(self, klines: List[List[Any]]) -> float:
        total_pv = sum(float(k[4]) * float(k[5]) for k in klines if k[4] and k[5])
        total_vol = sum(float(k[5]) for k in klines if k[5])
        return total_pv / total_vol if total_vol else 0.0

# 🔧 Çalıştırma
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    with open("/opt/tradebot/future_trade/config.json") as f:
        cfg = json.load(f)

    api_key = cfg["binance"]["key"]
    api_secret = cfg["binance"]["secret"]

    client = BinanceClient(api_key, api_secret)
    tester = FilterTester(client)

    async def run():
        print("🔍 Hacim filtresi test ediliyor...")
        await client.initialize()
        volume_pass = await tester.test_volume()
        print(f"📊 volume_test geçen semboller ({len(volume_pass)}): {volume_pass}")

        print("\n🔍 Fiyat artışı filtresi test ediliyor...")
        rise_pass = await tester.test_price_rise()
        print(f"📈 price_rise_test geçen semboller ({len(rise_pass)}): {rise_pass}")

        print("\n🔍 Fiyat düşüşü filtresi test ediliyor...")
        drop_pass = await tester.test_price_drop()
        print(f"📉 price_drop_test geçen semboller ({len(drop_pass)}): {drop_pass}")

        print("\n🔍 VWAP bias filtresi test ediliyor...")
        vwap_pass = await tester.test_vwap_bias()
        print(f"📐 vwap_test geçen semboller ({len(vwap_pass)}): {vwap_pass}")

        print("\n🔍 Güçlü long ve short adayları analiz ediliyor...")
        strong = await tester.find_strong_candidates()
        print(f"🚀 Güçlü long adayları ({len(strong['long'])}): {strong['long']}")
        print(f"🧨 Güçlü short adayları ({len(strong['short'])}): {strong['short']}")

        await client.close()

    asyncio.run(run())
