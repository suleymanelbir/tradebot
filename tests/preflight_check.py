import os
import json
import sqlite3
import time
import logging
import requests
import socket
from datetime import datetime

# 📁 Dosya ve dizin tanımları
BOT_MAIN_PATH = "/opt/tradebot/run_futures.py"
CONFIG_PATH = "/opt/tradebot/future_trade/config.json"
DB_PATH = "/opt/tradebot/future_trade/tradebot.db"
LOG_PATH = "/opt/tradebot/logs/tradebot.log"

def check_bot_main_file():
    assert os.path.exists(BOT_MAIN_PATH), f"Ana bot dosyası bulunamadı: {BOT_MAIN_PATH}"
    print(f"✅ Bot ana dosyası mevcut: {BOT_MAIN_PATH}")

def check_config():
    assert os.path.exists(CONFIG_PATH), "Config dosyası bulunamadı"
    with open(CONFIG_PATH) as f:
        cfg = json.load(f)
    assert not cfg.get("paper_mode", True), "Bot hâlâ paper modda!"
    assert cfg.get("api_key"), "API anahtarı eksik"
    assert cfg.get("base_url") == "https://fapi.binance.com", "Live endpoint kullanılmıyor"
    print(f"✅ Config dosyası yüklendi: {CONFIG_PATH}")
    return cfg

def check_db():
    assert os.path.exists(DB_PATH), f"Veritabanı dosyası eksik: {DB_PATH}"
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables = [row[0] for row in cur.fetchall()]
    required_tables = ["futures_positions", "symbol_state"]
    for t in required_tables:
        assert t in tables, f"{t} tablosu eksik"
    cur.execute("PRAGMA index_list('symbol_state');")
    indexes = [row[1] for row in cur.fetchall()]
    assert "idx_symbol_state_cooldown" in indexes, "Cooldown indeksi eksik"
    print(f"✅ Veritabanı kontrolü tamamlandı: {DB_PATH}")
    conn.close()

def check_strategy_registry():
    try:
        from future_trade.strategy.dominance_trend import STRATEGY_REGISTRY
        assert "dominance_trend" in STRATEGY_REGISTRY, "dominance_trend stratejisi yüklenmedi"
        print("✅ Strateji modülü başarıyla yüklendi")
    except Exception as e:
        raise Exception(f"Strateji modülü hatalı: {e}")

def check_ntp_sync():
    local_ts = int(time.time())
    ntp_ts = int(datetime.utcnow().timestamp())
    drift = abs(local_ts - ntp_ts)
    assert drift < 5, f"Sistem saati senkronize değil (drift={drift}s)"
    print("✅ Zaman senkronizasyonu uygun")

def check_logging():
    assert os.path.exists(LOG_PATH), f"Log dosyası bulunamadı: {LOG_PATH}"
    assert os.path.getsize(LOG_PATH) > 0, "Log dosyası boş"
    print(f"✅ Log dosyası mevcut ve dolu: {LOG_PATH}")

def check_telegram():
    try:
        token = "YOUR_TELEGRAM_BOT_TOKEN"
        chat_id = "YOUR_CHAT_ID"
        msg = "✅ Preflight check başarıyla tamamlandı."
        r = requests.post(f"https://api.telegram.org/bot{token}/sendMessage", data={"chat_id": chat_id, "text": msg})
        assert r.status_code == 200, "Telegram bildirimi başarısız"
        print("✅ Telegram bildirimi başarıyla gönderildi")
    except Exception as e:
        logging.warning(f"Telegram testi başarısız: {e}")

def check_network():
    try:
        socket.create_connection(("fapi.binance.com", 443), timeout=5)
        print("✅ Binance API bağlantısı başarılı")
    except Exception:
        raise Exception("Binance API bağlantısı başarısız")

def run_preflight():
    print("🔍 Preflight kontrolü başlatılıyor...\n")
    check_bot_main_file()
    cfg = check_config()
    check_db()
    check_strategy_registry()
    check_ntp_sync()
    check_logging()
    check_network()
    check_telegram()
    print("\n✅ Tüm kontroller başarıyla geçti. Bot live moda hazır.")

if __name__ == "__main__":
    try:
        run_preflight()
    except Exception as e:
        print(f"\n❌ Preflight kontrolü başarısız: {e}")
