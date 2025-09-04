# servisi başlatma komuut: sudo systemctl start tradebot-global.service
# sudo systemctl stop tradebot-global.service

# Servisin Durumunu Kontrol Etmek: sudo systemctl status tradebot-global.service
# Servisin çıktısını, logları ve olası hataları canlı izlemek için: sudo journalctl -u tradebot-global.service -f
# Servisi Yeniden Başlatmak: sudo systemctl restart tradebot-global.service
"""Servisi başlat: sudo systemctl start tradebot-global.service

Logları izle: sudo journalctl -u tradebot-global.service -f
"""

# manuel çıkıştan sonra : driver.quit()     kullanılması gerekiyor.
# python3 /opt/tradebot/globalislemler/database_manager_5.py
# ps aux | grep database_manager_5.py

# ***************************************************************************************************************************
#  alias tradebot-start='sudo systemctl start tradebot-global.service && sudo journalctl -u tradebot-global.service -n 20'  *
#  alias tradebot-stop='sudo systemctl stop tradebot-global.service'                                                        *
#  alias tradebot-log='sudo journalctl -u tradebot-global.service -f'                                                       *
#                                                                                                                           *
# ***************************************************************************************************************************
import os
import json
import sqlite3
import logging
import asyncio
import time
import tempfile, shutil, random
from pathlib import Path
from datetime import datetime, timedelta, timezone
from typing import List, Dict, Optional, Any
import requests
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
# Kullanacaksan açık bırak; kullanmayacaksan Ruff için kaldır:
from selenium.common.exceptions import TimeoutException, WebDriverException

print("✅ Script başladı")

# ─────────────────────────────────────────────────────────────
# 1) Merkezî yol sabitleri (ENV ile override edilebilir)
# ─────────────────────────────────────────────────────────────
CONFIG_DIR = os.getenv("TRADEBOT_CONFIG_DIR", "/opt/tradebot/globalislemler/config")
DB_PATH    = os.getenv("TRADEBOT_DB_PATH",    "/opt/tradebot/veritabani/global_data.db")
LOG_DIR    = os.getenv("TRADEBOT_LOG_DIR",    "/opt/tradebot/log")  # JSON'da da /opt/tradebot/log

# Dizinleri garantiye al
Path(LOG_DIR).mkdir(parents=True, exist_ok=True)
Path(Path(DB_PATH).parent).mkdir(parents=True, exist_ok=True)

# Config dosya isimleri (varsayılan)
GLOBAL_CFG   = os.path.join(CONFIG_DIR, "global_data_config.json")
TELEGRAM_CFG = os.path.join(CONFIG_DIR, "telegram_bots.json")
SYMBOLS_CFG  = os.path.join(CONFIG_DIR, "symbols.json")

# ─────────────────────────────────────────────────────────────
# 2) Logging (klasör hazırlandıktan sonra!)
# ─────────────────────────────────────────────────────────────
logging.basicConfig(
    filename=os.path.join(LOG_DIR, "database_manager_5.log"),
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logging.info("Database manager başlatıldı.")
logging.info("RUNNING_FROM_FILE=%s", __file__)

# ─────────────────────────────────────────────────────────────
# 3) Tablolar, veritabanı, limitler
# ─────────────────────────────────────────────────────────────
DB_NAME = DB_PATH  # sqlite için tam yol

GLOBAL_LIVE_TABLE    = "global_live_data"
GLOBAL_CLOSING_TABLE = "global_closing_data"

RECORD_LIMITS = {
    GLOBAL_LIVE_TABLE: 1500,
    GLOBAL_CLOSING_TABLE: 5000
}

LIMITS = {
    "CRYPTOCAP:USDT.D": {"lower": 3.0, "upper": 7.0},
    "CRYPTOCAP:BTC.D":  {"lower": 40.0, "upper": 70.0}
}

# Aynı kapanışı tekrarlamamak için toleranslar
INTERVAL_DUPLICATE_CHECK = {
    "15m": timedelta(minutes=12),                      # 12 dk
    "1h":  timedelta(minutes=53, seconds=30),          # 53 dk 30 sn
    "4h":  timedelta(hours=3, minutes=50, seconds=15), # 3 sa 50 dk 15 sn
    "1d":  timedelta(hours=23, minutes=50, seconds=45) # 23 sa 50 dk 45 sn
}

# ─────────────────────────────────────────────────────────────
# 4) Yardımcı: Telegram konfig yükleyici (iki şemayı da destekler)
# ─────────────────────────────────────────────────────────────
def load_telegram_config(config_path: str) -> dict:
    """
    Desteklenen formatlar:
    A) {
         "bots": [
           {"name":"main_bot","token":"XXX","chat_id":"111"},
           {"name":"alerts_bot","token":"YYY","chat_id":"222"}
         ]
       }
    B) {
         "main_bot":   {"token":"XXX","chat_id":"111"},
         "alerts_bot": {"token":"YYY","chat_id":"222"}
       }
    """
    with open(config_path, "r", encoding="utf-8") as file:
        raw = json.load(file)

    bots: dict = {}

    # A) Üstte "bots" anahtarıyla liste
    if isinstance(raw, dict) and isinstance(raw.get("bots"), list):
        for b in raw["bots"]:
            if not isinstance(b, dict):
                continue
            name   = b.get("name")
            token  = b.get("token")
            chatid = b.get("chat_id")
            if name and token and chatid:
                bots[name] = {"token": token, "chat_id": str(chatid)}
            else:
                logging.warning(f"Geçersiz bot girdisi atlandı: {b!r}")

    # B) Ad → yapı sözlüğü
    elif isinstance(raw, dict):
        for name, vals in raw.items():
            if isinstance(vals, dict) and "token" in vals and "chat_id" in vals:
                bots[name] = {"token": vals["token"], "chat_id": str(vals["chat_id"])}

    # C) Düz liste
    elif isinstance(raw, list):
        for b in raw:
            if not isinstance(b, dict):
                continue
            name   = b.get("name")
            token  = b.get("token")
            chatid = b.get("chat_id")
            if name and token and chatid:
                bots[name] = {"token": token, "chat_id": str(chatid)}

    if not bots:
        raise ValueError("Telegram config okunamadı veya geçersiz format.")

    if "main_bot" not in bots:
        logging.warning("Telegram config içinde 'main_bot' yok (opsiyonel uyarı).")

    logging.info("Telegram configuration loaded successfully.")
    return bots

# ─────────────────────────────────────────────────────────────
# 6) GİRİŞ NOKTASI
# ─────────────────────────────────────────────────────────────

# Telegram mesaj gönderme fonksiyonu
def send_telegram_message(message: str, bot_name: str, bots: dict, retries: int = 3, timeout: int = 10):
    """
    Sends a message to a Telegram bot.

    Args:
        message (str): The message to send.
        bot_name (str): The name of the bot as defined in the configuration.
        bots (dict): A dictionary of bot configurations.
        retries (int): The number of retry attempts on failure.
        timeout (int): Timeout for the HTTP request in seconds.

    Raises:
        ValueError: If the bot configuration is missing or invalid.
        Exception: If all retries fail to send the message.
    """
    import requests
    import time

    # Validate bot configuration
    bot_config = bots.get(bot_name)
    if not bot_config:
        raise ValueError(f"Bot configuration for '{bot_name}' not found.")
    if not bot_config.get("token") or not bot_config.get("chat_id"):
        raise ValueError(f"Bot configuration for '{bot_name}' is incomplete. Please check 'token' and 'chat_id'.")

    # Construct the Telegram API URL
    url = f"https://api.telegram.org/bot{bot_config['token']}/sendMessage"

    for attempt in range(retries):
        try:
            payload = {
                "chat_id": bot_config["chat_id"],
                "text": message,
                "parse_mode": "HTML"
            }
            logging.debug(f"Sending message using bot '{bot_name}': {payload}")
            response = requests.post(url, json=payload, timeout=timeout)
            response.raise_for_status()  # Raise an error for non-2xx responses
            logging.info(f"Message sent successfully using bot '{bot_name}': {message}")
            return  # Exit on successful send

        except requests.Timeout:
            logging.warning(f"Timeout occurred while sending message using bot '{bot_name}', Attempt {attempt + 1}/{retries}.")

        except requests.RequestException as e:
            logging.error(f"Request error while sending message using bot '{bot_name}', Attempt {attempt + 1}/{retries}: {e}")

        # Wait before retrying (exponential backoff)
        if attempt < retries - 1:
            backoff_time = 2 ** attempt
            logging.info(f"Retrying in {backoff_time} seconds...")
            time.sleep(backoff_time)

    # If all retries fail
    error_message = f"Failed to send Telegram message after {retries} attempts using bot '{bot_name}'."
    logging.error(error_message)
    raise Exception(error_message)


def send_startup_info():
    """
    Kodun başlangıcında Telegram'a bilgi mesajı gönderir.
    """
    try:
        script_path = os.path.abspath(__file__)
        log_file_name = os.path.basename(logging.root.handlers[0].baseFilename)
        message = (
            f"📂 Kod Başlatıldı\n\n"
            f"Dosya Adı: {os.path.basename(script_path)}\n"
            f"Dosya Konumu: {script_path}\n"
            f"Log Dosyası: {log_file_name}\n"
            f"Veritabanı Adı: {DB_NAME}\n"
            f"Tablo İsimleri:\n"
            f"  - Canlı Veriler: {GLOBAL_LIVE_TABLE}\n"
            f"  - Kapanış Verileri: {GLOBAL_CLOSING_TABLE}\n"
        )
        send_telegram_message(message, "main_bot", bots)  # Kapatma hatası da düzeltilmeli
    except Exception as e:
        logging.error(f"Failed to send startup info: {e}")


def connect_db(db_path: str | None = None):
    """
    SQLite veritabanına bağlanır, güvenli PRAGMA'ları ayarlar ve (conn, cursor) döner.
    Hata halinde ConnectionError raise eder; Telegram bildirimi dışarıda yapılır.
    """
    try:
        path = db_path or DB_NAME  # DB_NAME tepedeki sabitten geliyor
        # Dizin yoksa oluştur
        Path(Path(path).parent).mkdir(parents=True, exist_ok=True)

        conn = sqlite3.connect(
            path,
            timeout=30,             # yoğun I/O'da bekleme
            isolation_level=None,   # autocommit; istersen "DEFERRED" yapabilirsin
            check_same_thread=False # asyncio/thread kullanımında güvenli
        )
        cursor = conn.cursor()

        # Performans/güvenlik için yaygın PRAGMA'lar
        cursor.execute("PRAGMA journal_mode = WAL;")
        cursor.execute("PRAGMA synchronous = NORMAL;")
        cursor.execute("PRAGMA foreign_keys = ON;")

        logging.info("SQLite connected: %s", path)
        return conn, cursor

    except Exception as e:
        logging.error("DB connection error: %s", e, exc_info=True)
        raise ConnectionError(f"Database open failed: {e}") from e


def create_global_tables(cursor):
    """
    Gerekli tabloları oluşturur.
    """
    cursor.execute(f"""
        CREATE TABLE IF NOT EXISTS {GLOBAL_LIVE_TABLE} (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp DATETIME,
            symbol TEXT,
            live_price FLOAT,
            change_15M FLOAT,
            change_1H FLOAT,
            change_4H FLOAT,
            change_1D FLOAT
        )
    """)
    cursor.execute(f"""
        CREATE TABLE IF NOT EXISTS {GLOBAL_CLOSING_TABLE} (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp DATETIME,
            symbol TEXT,
            price FLOAT,
            interval TEXT,
            UNIQUE(symbol, timestamp, interval)
        )
    """)

async def clean_old_data_task(cursor, conn):
    """
    Eski verileri düzenli olarak temizler.
    """
    while True:
        try:
            for table_name, limit in RECORD_LIMITS.items():
                cursor.execute(f"""
                    DELETE FROM {table_name}
                    WHERE id NOT IN (
                        SELECT id FROM {table_name}
                        ORDER BY timestamp DESC LIMIT ?
                    )
                """, (limit,))
                deleted_rows = cursor.rowcount
                if deleted_rows > 0:
                    logging.info(f"Cleaned {deleted_rows} old rows from {table_name}.")
            conn.commit()
        except Exception as e:
            logging.error(f"Error in clean_old_data_task: {e}")
            send_telegram_message(f"❌ Temizlik Hatası: {str(e)}", "main_bot", bots)


        await asyncio.sleep(3600)  # Her saat başı temizlik yap

def is_duplicate_closing(cursor, symbol, db_interval, closing_time):
    """
    GLOBAL_CLOSING_TABLE'da duplicate kayıt kontrolü.
    """
    try:
        # Duplicate kontrolü için ilgili interval süresini al
        tolerance_seconds = INTERVAL_DUPLICATE_CHECK.get(db_interval, 0)

        # Veritabanında duplicate kontrolü yap
        cursor.execute(f"""
            SELECT 1 FROM global_closing_data
            WHERE symbol = ? AND interval = ? 
              AND ABS(strftime('%s', timestamp) - strftime('%s', ?)) <= ?
        """, (symbol, db_interval, closing_time, tolerance_seconds))
        return cursor.fetchone() is not None
    except Exception as e:
        logging.error(f"Error checking duplicate closing: {e}")
        return False



def is_closing_time(interval, now=None, tolerance_seconds=250):
    """
    Belirtilen zaman dilimi için şu an kapanış zamanı mı kontrol eder.
    
    Args:
        interval (str): Zaman dilimi ("15M", "1H", "4H", "1D").
        now (datetime.datetime, optional): Kontrol edilecek zaman. Varsayılan olarak UTC'deki şu anki zaman.
        tolerance_seconds (int): Kapanış zamanı için tolerans süresi (saniye).

    Returns:
        bool: Şu an kapanış zamanıysa True, aksi halde False.
    """
    if now is None:
        now = datetime.now(timezone.utc)

    # Şu anki saat, dakika ve saniyeyi al
    minute = now.minute
    hour = now.hour
    second = now.second

    # Tolerans aralığını hesapla
    lower_tolerance = -tolerance_seconds
    upper_tolerance = tolerance_seconds

    # Zaman dilimlerine göre kapanış zamanı kontrolü
    if interval == "15M":
        # Her 15 dakikada bir kapanış zamanı
        return (minute % 15 == 0 and
                lower_tolerance <= second <= upper_tolerance)

    elif interval == "1H":
        # Her saatin başında kapanış zamanı
        return (minute == 0 and
                lower_tolerance <= second <= upper_tolerance)

    elif interval == "4H":
        # Her 4 saatte bir, saatin başında kapanış zamanı
        return (hour % 4 == 0 and
                minute == 0 and
                lower_tolerance <= second <= upper_tolerance)

    elif interval == "1D":
        # Günün başında (UTC 00:00:00) kapanış zamanı
        return (hour == 0 and
                minute == 0 and
                lower_tolerance <= second <= upper_tolerance)

    # Geçersiz bir zaman dilimi verildiğinde uyarı logla
    logging.warning(f"Invalid interval provided: {interval}")
    return False





def get_closing_time(interval, now=None):
    """
    Belirtilen zaman dilimine göre kapanış zamanını hesaplar.

    Args:
        interval (str): Zaman dilimi ("15M", "1H", "4H", "1D").
        now (datetime, optional): Şu anki zaman. Varsayılan UTC şimdiki zaman.

    Returns:
        datetime: Kapanış zamanı.
    """
    if now is None:
        now = datetime.now(timezone.utc)

    if interval == "15M":
        minutes = (now.minute // 15) * 15
        return now.replace(minute=minutes, second=0, microsecond=0)
    elif interval == "1H":
        return now.replace(minute=0, second=0, microsecond=0)
    elif interval == "4H":
        hours = (now.hour // 4) * 4
        return now.replace(hour=hours, minute=0, second=0, microsecond=0)
    elif interval == "1D":
        return now.replace(hour=0, minute=0, second=0, microsecond=0)

    logging.warning(f"Invalid interval provided for closing time calculation: {interval}")
    return now


def check_price_limits(symbol, price, bots):
    """
    Fiyatı belirlenen sınırlarla karşılaştırır ve gerekirse log ve Telegram mesajı gönderir.
    Diğer semboller için yalnızca uyarı loglanır, ancak hata verilmez.

    Args:
        symbol (str): Kontrol edilecek sembol.
        price (float): Kontrol edilecek fiyat.
        bots (dict): Telegram botlarının bilgilerini içeren sözlük.

    Raises:
        ValueError: Eğer fiyat `float` ya da `int` değilse hata yükseltir.
    """
    try:
        # Tür kontrolü: Price, int ya da float olmalı
        if not isinstance(price, (int, float)):
            raise ValueError(f"Invalid price type for {symbol}: {type(price).__name__}")

        # Tanımlı olmayan semboller için uyarı logla ancak işleme devam et
        if symbol not in LIMITS:
            logging.warning(f"No limits defined for {symbol}. Skipping limit check.")
            return  # Limit kontrolü yapılmadan devam edilir

        # Alt ve üst limitleri al
        lower_limit = LIMITS[symbol]["lower"]
        upper_limit = LIMITS[symbol]["upper"]

        # Limitlerin altına düşerse uyarı gönder
        if price < lower_limit:
            message = f"⚠️ {symbol}: Fiyat {price} alt limite ({lower_limit}) düştü!"
            logging.warning(message)
            send_telegram_message(message, "alerts_bot", bots)

        # Limitlerin üstüne çıkarsa uyarı gönder
        elif price > upper_limit:
            message = f"⚠️ {symbol}: Fiyat {price} üst limite ({upper_limit}) çıktı!"
            logging.warning(message)
            send_telegram_message(message, "alerts_bot", bots)

        # Eğer fiyat sınırlar içinde ise bilgi logu
        else:
            logging.info(f"{symbol}: Fiyat ({price}) sınırlar içinde.")
    except ValueError as e:
        logging.error(f"ValueError in check_price_limits: {e}")
    except Exception as e:
        logging.error(f"Unexpected error in check_price_limits for {symbol}: {e}")



async def fetch_multiple_prices(
    symbols: List[str],
    retries: int = 3,
    delay_between_requests: int = 1,
    notify_failures: bool = False,
    bots: Optional[Dict[str, Any]] = None,
    bot_name: str = "alerts_bot",
    chromedriver_path: str = "/usr/bin/chromedriver",
    timeout_sec: int = 30,
    concurrency: int = 1,                 # JSON: selenium_pool.size
    use_user_data_dir: bool = True,       # profil çakışırsa otomatik False'a düşer
    profile_base_dir: Optional[str] = None,  # ENV/varsayılanla belirlenir
) -> Dict[str, Optional[float]]:
    """
    Birden fazla sembol için fiyatları eşzamanlı çeker.
    - Varsayılan: Her görev için benzersiz --user-data-dir ve farklı debug port (profil kilidi hatasını önler).
    - Eğer 'user data directory is already in use' hatası alınırsa o sembol için PROFİLSİZ moda otomatik düşer.
    - retry/backoff, timeout, concurrency parametreleri desteklenir.
    - notify_failures=True ve bots verilirse başarısızlıklar Telegram'a bildirilir.
    """
    # Profil kök dizini (systemd altında /tmp izolasyonundan kaçınmak için /opt kullan)
    base_dir = (
        profile_base_dir
        or os.getenv("CHROME_PROFILE_BASE")
        or "/opt/tradebot/tmp/chrome-profiles"
    )
    try:
        Path(base_dir).mkdir(parents=True, exist_ok=True)
    except Exception as e:
        # Dizini oluşturamıyorsak profil kullanmayalım
        logging.warning(f"Profile base dir not usable ({base_dir}): {e}; falling back to no profile.")
        use_user_data_dir = False

    semaphore = asyncio.Semaphore(max(1, concurrency))

    async def fetch_with_limit(symbol: str) -> Optional[float]:
        async with semaphore:
            tmp_profile: Optional[str] = None
            # Bu sembol özelinde profil kullanılsın mı? (hata olursa False'a çekilecek)
            local_use_profile = use_user_data_dir

            for attempt in range(retries):
                driver = None
                try:
                    options = Options()
                    # Klasik headless çoğu sistemde daha uyumlu (gerekirse 'new' kullanılabilir)
                    options.add_argument("--headless")
                    options.add_argument("--no-sandbox")
                    options.add_argument("--disable-dev-shm-usage")
                    options.add_argument("--disable-gpu")
                    options.add_argument("--disable-extensions")
                    options.add_argument("--disable-application-cache")
                    options.add_argument("--disable-blink-features=AutomationControlled")
                    options.add_argument("--no-first-run")
                    options.add_argument("--no-default-browser-check")
                    options.add_argument("--disable-background-networking")
                    options.add_argument("--metrics-recording-only")
                    options.add_argument("--mute-audio")
                    options.add_argument("--hide-scrollbars")
                    options.add_argument("--log-level=3")
                    # Debug port çakışmasını önle
                    options.add_argument("--remote-debugging-port=0")

                    tmp_profile = None
                    if local_use_profile:
                        # Her denemede benzersiz profil
                        tmp_profile = tempfile.mkdtemp(
                            prefix=f"tv-{symbol.replace(':','-')}-", dir=base_dir
                        )
                        options.add_argument(f"--user-data-dir={tmp_profile}")

                    service = Service(chromedriver_path)
                    driver = webdriver.Chrome(service=service, options=options)

                    url = f"https://www.tradingview.com/symbols/{symbol.replace(':', '-')}/"
                    logging.info(
                        f"[{symbol}] try {attempt+1}/{retries} | "
                        f"profile={'on' if local_use_profile else 'off'} "
                        f"| base={base_dir if local_use_profile else '-'}"
                    )
                    driver.get(url)

                    price_element = WebDriverWait(driver, timeout_sec).until(
                        EC.presence_of_element_located((By.CSS_SELECTOR, ".js-symbol-last"))
                    )
                    raw_price = (price_element.text or "").strip()
                    if not raw_price:
                        logging.warning(f"[{symbol}] empty price; retrying…")
                        await asyncio.sleep(2 ** attempt)
                        continue

                    logging.info(f"[{symbol}] raw price: {raw_price}")
                    cleaned = clean_price_string(raw_price)
                    if cleaned is None:
                        logging.warning(f"[{symbol}] cleaning failed; retrying…")
                        await asyncio.sleep(2 ** attempt)
                        continue

                    return cleaned

                except WebDriverException as e:
                    msg = str(e)
                    logging.error(f"[{symbol}] WebDriverException ({attempt+1}/{retries}): {msg}")
                    # Profil kilidi hatası → bu sembolde profili kapatıp yeniden dene
                    if "user data directory is already in use" in msg.lower() and local_use_profile:
                        logging.warning(f"[{symbol}] user-data-dir in use → switching to NO-PROFILE mode.")
                        local_use_profile = False
                    await asyncio.sleep(2 ** attempt)

                except Exception as e:
                    logging.error(f"[{symbol}] fetch error ({attempt+1}/{retries}): {e}")
                    await asyncio.sleep(2 ** attempt)

                finally:
                    if driver:
                        try:
                            driver.quit()
                        except Exception:
                            pass
                    if tmp_profile:
                        shutil.rmtree(tmp_profile, ignore_errors=True)

            # Tüm denemeler başarısız
            error_message = f"Failed to fetch price for {symbol} after {retries} attempts."
            logging.error(error_message)
            if notify_failures and bots:
                try:
                    send_telegram_message(error_message, bot_name, bots)
                except Exception:
                    logging.warning("[fetch_multiple_prices] Telegram notification failed.")
            return None

    tasks = {sym: fetch_with_limit(sym) for sym in symbols}
    results = await asyncio.gather(*tasks.values(), return_exceptions=True)

    out: Dict[str, Optional[float]] = {}
    for sym, res in zip(tasks.keys(), results):
        if isinstance(res, Exception):
            logging.error(f"[{sym}] task exception: {res}")
            out[sym] = None
        else:
            out[sym] = res

    await asyncio.sleep(delay_between_requests)
    return out



def clean_symbol(symbol): 
    return symbol.strip().replace(' ', '')

async def save_closing_price(cursor, conn, symbol, live_price, now=None):
    """
    Belirtilen sembol ve zaman dilimi için kapanış fiyatını kaydeder veya günceller.

    Args:
        cursor: Veritabanı cursor nesnesi.
        conn: Veritabanı bağlantı nesnesi.
        symbol (str): İşlem sembolü (ör. "BTCUSDT").
        live_price (float): Kaydedilecek mevcut fiyat.
        now (datetime, optional): Şu anki zaman. Varsayılan UTC şimdiki zaman.

    Raises:
        RuntimeError: Kaydetme sırasında hata oluşursa.
    """
    if now is None:
        now = datetime.now(timezone.utc)

    symbol = clean_symbol(symbol)

    intervals = {
        "15M": timedelta(minutes=15),
        "1H": timedelta(hours=1),
        "4H": timedelta(hours=4),
        "1D": timedelta(days=1)
    }

    try:
        for interval, duration in intervals.items():
            normalized_interval = interval.upper()

            # Zaman dilimi için kapanış zamanını hesapla
            closing_time = get_closing_time(normalized_interval, now)

            # Eğer kapanış zamanı henüz gelmemişse, atla
            if now < closing_time:
                logging.info(f"{symbol} için {normalized_interval} kapanış zamanı ({closing_time}) henüz gelmedi. Atlanıyor.")
                continue

            # UNIQUE kuralını işlemek için INSERT OR REPLACE kullan
            cursor.execute(f"""
                INSERT INTO global_closing_data (timestamp, symbol, price, interval)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(symbol, timestamp, interval)
                DO UPDATE SET price = excluded.price
            """, (closing_time, symbol, live_price, normalized_interval))

            conn.commit()
            if symbol == "CRYPTOCAP:USDT.D":
                logging.info(f"Closing price kaydedildi veya güncellendi: {symbol}, Interval: {normalized_interval}, Price: {live_price}")

    except sqlite3.Error as e:
        conn.rollback()
        logging.error(f"Database error while saving closing price for {symbol}: {e}")

    except Exception as e:
        conn.rollback()
        logging.error(f"Unexpected error while saving closing price for {symbol}: {e}")







async def save_live_data(cursor, conn, symbol, live_price, changes, now=None):
    """
    Save live price and percentage changes for a given symbol.

    Args:
        cursor: Database cursor for executing SQL commands.
        conn: Database connection object for committing changes.
        symbol (str): Trading symbol (e.g., "BTCUSD").
        live_price (float): The current live price of the symbol.
        changes (dict): A dictionary containing percentage changes (e.g., {"15M": -0.5, "1H": 0.1}).
        now (datetime, optional): Timestamp for the data. Defaults to current UTC time.

    Raises:
        RuntimeError: If there is an issue with database saving.
    """
    if now is None:
        now = datetime.now(timezone.utc)

    # Normalize symbol
    symbol = clean_symbol(symbol)

    # Alt ve üst limit kontrolü yalnızca CRYPTOCAP:USDT.D için uygulanır
    if symbol == "CRYPTOCAP:USDT.D":
        MIN_PRICE_LIMIT = 3.0
        MAX_PRICE_LIMIT = 8.0
        if live_price < MIN_PRICE_LIMIT or live_price > MAX_PRICE_LIMIT:
            logging.error(f"Live price for {symbol} is out of bounds: {live_price}. Must be between {MIN_PRICE_LIMIT} and {MAX_PRICE_LIMIT}.")
            send_telegram_message(f"⚠️ Live price for {symbol} out of bounds: {live_price}. Ignored.")
            return  # Limit dışındaki fiyatlar için kayıt yapmıyoruz

    # Son fiyatı kontrol et (fiyat değişmiş mi?)
    query_last_price = """
        SELECT live_price
        FROM global_live_data
        WHERE symbol = ?
        ORDER BY timestamp DESC
        LIMIT 1
    """
    cursor.execute(query_last_price, (symbol,))
    last_price_record = cursor.fetchone()

    if last_price_record:
        last_price = float(last_price_record[0])
        if live_price == last_price:
            logging.info(f"No change in live price for {symbol}. Skipping save.")
            return  # Fiyat değişmemişse kayıt yapmıyoruz

    # Prepare SQL query and parameters
    query = """
        INSERT INTO global_live_data (timestamp, symbol, live_price, change_15M, change_1H, change_4H, change_1D)
        VALUES (?, ?, ?, ?, ?, ?, ?)
    """
    parameters = (
        now,
        symbol,
        live_price,
        changes.get("15M", None),  # Use None if change value is missing
        changes.get("1H", None),
        changes.get("4H", None),
        changes.get("1D", None),
    )

    try:
        # Execute the insertion query
        cursor.execute(query, parameters)
        conn.commit()
        logging.info(f"Live data saved for {symbol}: Price: {live_price}, Changes: {changes}")

    except sqlite3.IntegrityError as e:
        # Handle database constraints (e.g., UNIQUE constraint violations)
        conn.rollback()
        logging.warning(f"Integrity error while saving live data for {symbol}: {e}")
        send_telegram_message(f"⚠️ Integrity error: Could not save data for {symbol}.", "alerts_bot", bots)

    except sqlite3.OperationalError as e:
        # Handle operational issues with the database
        conn.rollback()
        logging.error(f"Operational error while saving live data for {symbol}: {e}")
        send_telegram_message(f"⚠️ Database error: Could not save data for {symbol}.", "alerts_bot", bots)

    except Exception as e:
        # Handle other exceptions
        conn.rollback()
        logging.error(f"Unexpected error saving live data for {symbol}: {e}")
        send_telegram_message(f"❌ Unexpected error: Could not save data for {symbol}. Error: {str(e)}", "alerts_bot", bots)

    finally:
        # Log the completion of the operation, even if it fails
        logging.debug(f"Save live data operation completed for {symbol}.")





def calculate_changes(cursor, symbol, live_price):
    """
    Her zaman dilimi için yüzde değişimleri hesaplar.
    
    Args:
        cursor: Veritabanı cursor nesnesi.
        symbol (str): İşlem sembolü.
        live_price (float): Canlı fiyat.

    Returns:
        dict: Her zaman dilimi için yüzde değişimlerini içeren sözlük.
    """
    symbol = clean_symbol(symbol)  # Sembolü temizle
    intervals = ["15M", "1H", "4H", "1D"]
    changes = {}

    for interval in intervals:
        try:
            # Kapanış fiyatını sorgula
            cursor.execute(f"""
                SELECT price
                FROM global_closing_data
                WHERE symbol = ? AND interval = ?
                ORDER BY timestamp DESC
                LIMIT 1
            """, (symbol, interval))

            result = cursor.fetchone()
            if result and result[0]:
                closing_price = result[0]
                # Kapanış fiyatı sıfır ise hatalı sonuçları önlemek için kontrol
                if closing_price <= 0:
                    logging.error(f"Invalid closing price for {symbol} at interval {interval}. Closing price: {closing_price}")
                    changes[interval] = None
                else:
                    # Yüzde değişim hesapla
                    changes[interval] = round(((live_price - closing_price) / closing_price) * 100, 2)
                    logging.info(f"Change calculated for {symbol} at interval {interval}: {changes[interval]}%")
            else:
                # Kapanış fiyatı bulunamazsa uyarı logu
                changes[interval] = None
                logging.warning(f"No closing price found for {symbol} at interval {interval}. Using live price: {live_price}")

        except Exception as e:
            # Sorgulama sırasında hata oluşursa
            logging.error(f"Error calculating change for {symbol}, Interval {interval}: {e}", exc_info=True)
            changes[interval] = None

    return changes




def clean_price_string(price_str):
    """
    Fiyat metnini temizler ve float'a dönüştürür. Büyük birimlere (T, B, M, K) göre ölçeklendirme yapar.

    Args:
        price_str (str): Ham fiyat metni (örneğin '3.42 T', '1.44 B').

    Returns:
        float: İşlenmiş fiyat değeri, ya da hata durumunda None.
    """
    try:
        # Boş string kontrolü
        if not price_str or not price_str.strip():
            raise ValueError("Price string is empty or invalid.")
        
        price_str = price_str.replace('\u202f', '').replace(',', '').strip()
        if 'T' in price_str:
            return float(price_str.replace('T', '')) * 1e12
        elif 'B' in price_str:
            return float(price_str.replace('B', '')) * 1e9
        elif 'M' in price_str:
            return float(price_str.replace('M', '')) * 1e6
        elif 'K' in price_str:
            return float(price_str.replace('K', '')) * 1e3
        else:
            return float(price_str)
    except ValueError as e:
        logging.error(f"Error cleaning price string '{price_str}': {e}")
        return None



def get_latest_live_data(cursor, symbol):
    """
    Veritabanından bir sembolün en son canlı fiyatını ve zamanını döndürür.
    """
    try:
        cursor.execute(f"""
            SELECT live_price, timestamp
            FROM global_live_data
            WHERE symbol = ?
            ORDER BY timestamp DESC
            LIMIT 1
        """, (symbol,))
        result = cursor.fetchone()
        if result:
            return {"price": result[0], "timestamp": result[1]}
    except Exception as e:
        logging.error(f"Error fetching latest live data for {symbol}: {e}")
    return None


async def fetch_live_price(symbol: str, retries: int = 3, timeout: int = 30) -> float:
    options = Options()
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    options.add_argument('--disable-gpu')
    options.add_argument('--disable-extensions')
    options.add_argument('--disable-application-cache')
    options.add_argument('--disable-blink-features=AutomationControlled')

    service = Service("/usr/bin/chromedriver")

    for attempt in range(retries):
        driver = None
        try:
            # Selenium driver başlatılıyor
            logging.info(f"Fetching live price for {symbol}, attempt {attempt + 1}/{retries}")
            driver = webdriver.Chrome(service=service, options=options)
            url = f"https://www.tradingview.com/symbols/{symbol.replace(':', '-')}/"
            driver.get(url)

            # Fiyat bilgisini çek
            price_element = WebDriverWait(driver, timeout).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, ".js-symbol-last"))
            )
            raw_price = price_element.text
            logging.info(f"Fetched raw price for {symbol}: {raw_price}")

            # Fiyatı temizle ve döndür
            return clean_price_string(raw_price)

        except Exception as e:
            logging.error(f"Error fetching price for {symbol}, attempt {attempt + 1}/{retries}: {e}")
            if attempt == retries - 1:
                logging.error(f"Failed to fetch price for {symbol} after {retries} attempts.")
        finally:
            # Selenium driver'ı kapat
            if driver:
                driver.quit()
                logging.info("Chrome driver closed.")

    return None

def cleanup_zombie_chrome():
    """
    Clean up zombie Chrome processes left hanging in the system.
    This function terminates all Chrome processes running in headless mode.

    Logs:
        - Information about the cleanup process.
        - Warnings if no processes are found.
        - Errors if the cleanup process fails.
    """
    logging.info("Starting cleanup of zombie Chrome processes...")

    try:
        # Execute the system command to kill headless Chrome processes
        result = os.system("pkill -f 'chrome --headless'")
        
        if result == 0:
            logging.info("Zombie Chrome processes cleaned successfully.")
        else:
            # Handle cases where no processes are found
            logging.warning("No zombie Chrome processes found to clean.")

    except Exception as e:
        # Log unexpected errors during cleanup
        logging.error(f"Error while cleaning zombie Chrome processes: {e}")
        send_telegram_message(f"⚠️ Error cleaning Chrome processes: {str(e)}", "alerts_bot", bots)



def reconnect_db(conn, cursor):
    """
    Veritabanı bağlantısını yeniler.
    """
    if conn:
        try:
            conn.close()
        except Exception as e:
            logging.warning(f"Error closing database connection: {e}")
    return connect_db()

lock = asyncio.Lock()
last_cleanup_time = datetime.now()


async def run_fetching_cycle(conn, cursor, global_symbols, bots, delay_between_symbols=2, cleanup_interval=1800, min_cycle_duration=30):
    ...

    """
    Paralel veri alımı ve veri işleme döngüsü.

    Args:
        conn: Veritabanı bağlantısı.
        cursor: Veritabanı cursor.
        global_symbols (list): İşlem yapılacak semboller listesi.
        delay_between_symbols (int): Semboller arasında bekleme süresi (saniye).
        cleanup_interval (int): Zombi Chrome işlemlerini temizleme süresi (saniye).
        min_cycle_duration (int): Döngü başına minimum süre (saniye).
    """
    logging.info("Fetching cycle started.")
    last_cleanup_time = datetime.now()
    last_action_time = datetime.now()
    retry_attempts = 3  # Hata durumunda tekrar deneme sayısı

    while True:
        cycle_start = time.time()
        try:
            # Zombi Chrome temizliği
            if (datetime.now() - last_cleanup_time).total_seconds() > cleanup_interval:
                logging.info("Performing scheduled cleanup of zombie Chrome processes.")
                cleanup_zombie_chrome()
                last_cleanup_time = datetime.now()

            # Paralel fiyat alma
            logging.info("Fetching live prices for symbols...")
            for attempt in range(retry_attempts):
                try:
                    fetched_prices = await fetch_multiple_prices(global_symbols, delay_between_requests=delay_between_symbols)
                    break  # Başarılı olursa döngüden çık
                except Exception as fetch_error:
                    logging.warning(f"Retrying fetching prices. Attempt {attempt + 1}/{retry_attempts}. Error: {fetch_error}")
                    await asyncio.sleep(2 ** attempt)  # Exponential backoff
            else:
                # Tüm denemeler başarısız olduysa
                raise RuntimeError("Failed to fetch prices after multiple attempts.")

            now = datetime.now(timezone.utc)
            last_action_time = datetime.now()

            for symbol, live_price in fetched_prices.items():
                if live_price is not None and live_price > 0:
                    async with lock:
                        try:
                            # Yüzde değişimleri hesapla
                            logging.debug(f"Calculating changes for {symbol} with live price {live_price}.")
                            changes = calculate_changes(cursor, symbol, live_price)

                            # Canlı fiyatı ve değişimleri kaydet
                            await save_live_data(cursor, conn, symbol, live_price, changes, now)

                            # Kapanış fiyatını kaydet
                            await save_closing_price(cursor, conn, symbol, live_price, now)

                            logging.info(f"Processed data for {symbol}: Live Price: {live_price}, Changes: {changes}")

                        except Exception as process_error:
                            # Veri işleme sırasında oluşan hatalar
                            logging.error(f"Error processing data for {symbol}: {process_error}")
                            send_telegram_message(f"⚠️ Veri işleme hatası: {symbol}\nHata: {str(process_error)}", "alerts_bot", bots)
                else:
                    # Fiyat alınamayan semboller için log ve bildirim
                    error_message = f"Failed to fetch live price for {symbol}."
                    logging.error(error_message)
                    send_telegram_message(f"⚠️ {error_message}", "alerts_bot", bots)

        except Exception as e:
            # Döngü genelindeki hatalar
            logging.error(f"Error in fetching cycle: {e}")
            send_telegram_message(f"❌ Veri Kaydı Hatası: Hata: {str(e)}", "alerts_bot", bots)

        # Döngünün kalan süresini bekle
        cycle_duration = time.time() - cycle_start
        sleep_duration = max(min_cycle_duration - cycle_duration, 0)
        logging.debug(f"Cycle completed in {cycle_duration:.2f} seconds. Sleeping for {sleep_duration:.2f} seconds.")

        # Uyku modunu kontrol et
        time_since_last_action = (datetime.now() - last_action_time).total_seconds()
        if time_since_last_action > 300:  # 5 dakikalık inaktiflik süresi
            warning_message = f"⚠️ Uyarı: Bot 5 dakikadır işlem yapmıyor. Son işlem zamanı: {last_action_time}"
            logging.warning(warning_message)
            send_telegram_message(warning_message, "alerts_bot", bots)

        await asyncio.sleep(sleep_duration)

async def main_trading(
    symbols: list,
    bots: dict,
    conn,
    cursor,
    fetch_cfg: dict,
    pool_cfg: dict,
):
    """
    Semboller için fiyatları çeker, limit kontrolü yapar ve DB'ye kaydeder.
    Çalışma parametreleri JSON config'ten (fetch_cfg / pool_cfg) gelir.
    """
    # JSON → çalışma parametreleri
    retries           = int(fetch_cfg.get("retries", 3))
    timeout_sec       = int(fetch_cfg.get("wait_seconds", 30))
    fetch_every_sec   = int(fetch_cfg.get("fetch_every_sec", 60))
    concurrency       = max(1, int(pool_cfg.get("size", 1)))
    chromedriver_path = pool_cfg.get("chromedriver_path", "/usr/bin/chromedriver")

    # Bildirim tercihleri
    notify_failures = True
    bot_name = "alerts_bot" if isinstance(bots, dict) and "alerts_bot" in bots else "main_bot"

    # Başlatma mesajı (opsiyonel)
    try:
        script_path = os.path.abspath(__file__)
        message = (
            "✅ <b>Program Başlatıldı</b>\n\n"
            f"📂 <b>Dosya Adı:</b> {os.path.basename(script_path)}\n"
            f"📁 <b>Dosya Konumu:</b> {script_path}\n"
            f"🗃️ <b>Veritabanı:</b> {DB_NAME}\n"
            f"📊 <b>Tablolar:</b>\n"
            f"  - Canlı: {GLOBAL_LIVE_TABLE}\n"
            f"  - Kapanış: {GLOBAL_CLOSING_TABLE}\n"
        )
        if bots:
            send_telegram_message(message, "main_bot", bots)
    except Exception:
        logging.warning("Başlatma mesajı gönderilemedi.", exc_info=True)

    # Sürekli döngü
    while True:
        try:
            # Fiyatları çek
            prices = await fetch_multiple_prices(
                symbols,
                retries=retries,
                delay_between_requests=1,       # kısa ara (isteğe bağlı)
                notify_failures=notify_failures,
                bots=bots,
                bot_name=bot_name,
                chromedriver_path=chromedriver_path,
                timeout_sec=timeout_sec,
                concurrency=concurrency,
            )

            now = datetime.now(timezone.utc)

            for symbol, price in prices.items():
                if price is None:
                    logging.warning(f"Price for {symbol} could not be fetched.")
                    continue

                # Limit kontrolü (yalnızca tanımlı semboller)
                if symbol in LIMITS:
                    check_price_limits(symbol, price, bots)

                # Yüzde değişimleri hesapla & kaydet
                changes = calculate_changes(cursor, symbol, price)
                await save_live_data(cursor, conn, symbol, price, changes, now)
                await save_closing_price(cursor, conn, symbol, price, now)

            # Döngü beklemesi: JSON'dan
            await asyncio.sleep(fetch_every_sec)

        except Exception as e:
            logging.error(f"Error in main_trading loop: {e}", exc_info=True)
            if bots:
                try:
                    send_telegram_message(f"❌ Error in main_trading loop: {str(e)}", "main_bot", bots)
                except Exception:
                    logging.warning("Telegram bildirimi gönderilemedi (loop error).")


# Ana giriş noktası
if __name__ == "__main__":
    bots = {}     # Telegram config okunamazsa bile servis çalışsın
    conn = None
    cursor = None

    try:
        # 6.1) Global konfigürasyonu oku (tek kaynak)
        with open(GLOBAL_CFG, "r", encoding="utf-8") as f:
            cfg = json.load(f)
            fetch_cfg = cfg.get("fetch", {})
            pool_cfg  = cfg.get("selenium_pool", {})
        # 6.2) Log seviyesi (basicConfig sonrası level güncellenebilir)
        level_name = (cfg.get("logging_level") or "INFO").upper()
        logging.getLogger().setLevel(getattr(logging, level_name, logging.INFO))

        # 6.3) Paths override (ENV > JSON > defaults mantığı: ENV zaten en başta uygulandı)
        paths = cfg.get("paths") or {}
        db_override  = paths.get("db_path")
        log_override = paths.get("log_dir")
        telegram_cfg_override = paths.get("telegram_config")

        if db_override and db_override != DB_NAME:
            DB_NAME = db_override
            Path(Path(DB_NAME).parent).mkdir(parents=True, exist_ok=True)
            logging.info(f"DB_PATH override edildi: {DB_NAME}")

        if log_override and os.path.abspath(log_override) != os.path.abspath(LOG_DIR):
            Path(log_override).mkdir(parents=True, exist_ok=True)
            logging.info(f"LOG_DIR (JSON) tespit edildi: {log_override} (handler runtime'da değiştirilmedi)")

        if telegram_cfg_override and os.path.abspath(telegram_cfg_override) != os.path.abspath(TELEGRAM_CFG):
            TELEGRAM_CFG = telegram_cfg_override
            logging.info(f"TELEGRAM_CFG override edildi: {TELEGRAM_CFG}")

        # 6.4) Telegram yapılandırmasını yükle (opsiyonel)
        try:
            bots = load_telegram_config(TELEGRAM_CFG)
        except FileNotFoundError:
            logging.warning(f"Telegram config bulunamadı, devam: {TELEGRAM_CFG}")
            bots = {}
        except PermissionError:
            logging.error(f"Telegram config izin hatası, Telegram devre dışı: {TELEGRAM_CFG}")
            bots = {}
        except Exception:
            logging.exception("Telegram config yüklenemedi, Telegram devre dışı")
            bots = {}

        # 6.5) Veritabanı bağlantısı + tablolar
        conn, cursor = connect_db()
        if not conn or not cursor:
            raise ConnectionError("Failed to connect to the database.")

        create_global_tables(cursor)
        conn.commit()

        # 6.6) Semboller (symbols veya global_symbols anahtarını destekle)
        global_symbols = cfg.get("symbols") or cfg.get("global_symbols") or []
        if not global_symbols:
            raise ValueError(
                "No symbols found. Lütfen global_data_config.json içinde "
                "'symbols' veya 'global_symbols' anahtarını doldurun."
            )
        logging.info(f"Loaded global symbols: {global_symbols}")

        # 6.7) Price limits (JSON varsa sabiti güncelle)
        if "price_limits" in cfg and isinstance(cfg["price_limits"], dict):
            LIMITS.clear()
            LIMITS.update(cfg["price_limits"])

        # 6.8) Retention: hem doğru tablo adlarını hem olası kısa adları destekle
        # JSON önerilen: {"global_live_data":5000,"global_closing_data":8000}
        # Eski alışkanlık için destek: {"global_live":5000,"global_closing":8000}
        if "retention" in cfg and isinstance(cfg["retention"], dict):
            mapping = {
                "global_live": GLOBAL_LIVE_TABLE,
                "global_closing": GLOBAL_CLOSING_TABLE,
                GLOBAL_LIVE_TABLE: GLOBAL_LIVE_TABLE,
                GLOBAL_CLOSING_TABLE: GLOBAL_CLOSING_TABLE,
            }
            for key, limit in cfg["retention"].items():
                table = mapping.get(key)
                if table:
                    try:
                        RECORD_LIMITS[table] = int(limit)
                    except Exception:
                        logging.warning(f"Retention değeri sayıya çevrilemedi: {key} -> {limit}")
                else:
                    logging.warning(f"Bilinmeyen retention anahtarı: {key}")

        # 6.9) Ana işlem döngüsü
        asyncio.run(main_trading(global_symbols, bots, conn, cursor, fetch_cfg, pool_cfg))

    except FileNotFoundError as e:
        logging.error(f"File not found: {e}", exc_info=True)
        if bots:
            try:
                send_telegram_message(f"❌ Configuration file error: {e}", "main_bot", bots)
            except Exception:
                logging.warning("Telegram bildirimi gönderilemedi (FileNotFoundError).")

    except ConnectionError as e:
        logging.error(f"Database connection error: {e}", exc_info=True)
        if bots:
            try:
                send_telegram_message(f"❌ Database connection error: {e}", "main_bot", bots)
            except Exception:
                logging.warning("Telegram bildirimi gönderilemedi (ConnectionError).")

    except ValueError as e:
        logging.error(f"Configuration error: {e}", exc_info=True)
        if bots:
            try:
                send_telegram_message(f"❌ Configuration error: {e}", "main_bot", bots)
            except Exception:
                logging.warning("Telegram bildirimi gönderilemedi (ValueError).")

    except Exception as e:
        logging.error(f"Unexpected fatal error: {e}", exc_info=True)
        if bots:
            try:
                send_telegram_message(f"❌ Fatal Error: {e}", "main_bot", bots)
            except Exception:
                logging.warning("Telegram bildirimi gönderilemedi (Fatal).")

    finally:
        try:
            if conn:
                conn.close()
                logging.info("Database connection closed.")
                if bots:
                    try:
                        send_telegram_message("🔌 Database connection closed.", "main_bot", bots)
                    except Exception:
                        logging.warning("Telegram bildirimi gönderilemedi (shutdown).")
        except Exception:
            logging.exception("DB kapanışı sırasında hata")
