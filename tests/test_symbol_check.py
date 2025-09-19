# tests/test_symbol_check.py

import pytest
import httpx

def get_exchange_symbols():
    url = "https://testnet.binancefuture.com/fapi/v1/exchangeInfo"
    response = httpx.get(url)
    data = response.json()
    return [s["symbol"] for s in data["symbols"]]

def check_symbol_exists(symbol: str, symbol_list: list) -> bool:
    return symbol.upper() in symbol_list

@pytest.mark.parametrize("symbol", ["BTCUSDT", "ETHUSDT", "SOLUSDT", ""])  # Boş sembol dahil
def test_symbol_check(symbol):
    symbol_list = get_exchange_symbols()

    if symbol.strip() == "":
        print("\n📌 Binance Futures Testnet'teki tüm semboller:")
        for s in symbol_list:
            print("-", s)
        assert len(symbol_list) > 0
    else:
        exists = check_symbol_exists(symbol, symbol_list)
        print(f"\n🔍 Sembol '{symbol}' Binance testnet'te {'VAR' if exists else 'YOK'}")
        assert exists, f"Sembol '{symbol}' Binance testnet'te bulunamadı"


if __name__ == "__main__":
    import sys

    symbol = input("Kontrol edilecek sembolü girin (boş bırakırsan tüm semboller listelenir): ").strip()
    symbol_list = get_exchange_symbols()

    if symbol == "":
        print("\n📌 Binance Futures Testnet'teki tüm semboller:")
        for i, s in enumerate(symbol_list, start=1):
            print(f"{i:03d}. {s}")
    else:
        exists = check_symbol_exists(symbol, symbol_list)
        if exists:
            print(f"\n✅ Sembol '{symbol}' Binance testnet'te işlem görebilir.")
        else:
            print(f"\n❌ Sembol '{symbol}' Binance testnet'te bulunamadı.")
            print("\n📌 Geçerli semboller:")
            for i, s in enumerate(symbol_list, start=1):
                print(f"{i:03d}. {s}")
            sys.exit(1)
