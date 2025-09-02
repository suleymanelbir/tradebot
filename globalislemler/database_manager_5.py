# manuel çıkıştan sonra : driver.quit()     kullanılması gerekiyor.
# python3 /opt/tradebot/globalislemler/database_manager_5.py
# ps aux | grep database_manager_5.py


import requests
import os
import json
import sqlite3
import logging
import asyncio
import time
from datetime import datetime, timedelta, timezone
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from selenium.common.exceptions import TimeoutException, WebDriverException

print("✅ Script başladı")

# Logging ayarı
logging.basicConfig(
    filename='/opt/tradebot/log/database_manager_5.log',  # Log dosyasının adı
    level=logging.INFO,  # Log seviyesini INFO olarak ayarladık
    format='%(asctime)s %(levelname)s %(message)s',  # Log formatı
    datefmt='%Y-%m-%d %H:%M:%S'  # Tarih formatı (isteğe bağlı)
)

logging.info("Database manager başlatıldı.")

INTERVAL_DUPLICATE_CHECK = {
    "15m": timedelta(minutes=12),                      # 10 dakika
    "1h": timedelta(minutes=53, seconds=30),           # 45 dakika 30 saniye
    "4h": timedelta(hours=3, minutes=50, seconds=15),  # 3 saat 50 dakika 15 saniye
    "1d": timedelta(hours=23, minutes=50, seconds=45)  # 23 saat 45 dakika 45 saniye
}



# Veritabanı adı
DB_NAME = "trading_data.db"
GLOBAL_LIVE_TABLE = "global_live_data"
GLOBAL_CLOSING_TABLE = "global_closing_data"

# Kayıt limitleri
RECORD_LIMITS = {
    GLOBAL_LIVE_TABLE: 1500,
    GLOBAL_CLOSING_TABLE: 5000
}

LIMITS = {
    "CRYPTOCAP:USDT.D": {"lower": 3.0, "upper": 7.0},
    "CRYPTOCAP:BTC.D": {"lower": 40.0, "upper": 70.0}
}


def load_telegram_config(config_path):
    """
    Telegram yapılandırmasını bir JSON dosyasından yükler ve doğrulama yapar.

    Args:
        config_path (str): Yükleme yapılacak yapılandırma dosyasının yolu.

    Returns:
        dict: Bot yapılandırmalarını içeren bir sözlük. Anahtar olarak bot isimlerini, değer olarak ise
        "token" ve "chat_id" bilgilerini içerir.

    Raises:
        FileNotFoundError: Eğer yapılandırma dosyası bulunamazsa.
        json.JSONDecodeError: Eğer yapılandırma dosyası geçersiz JSON formatında ise.
        KeyError: Eğer dosyada beklenen anahtarlar eksikse.
        ValueError: Eğer "main_bot" yapılandırması eksikse.
    """
    try:
        # Dosyayı aç ve JSON içeriğini yükle
        with open(config_path, "r") as file:
            config = json.load(file)

        # "bots" anahtarının varlığını kontrol et
        if "bots" not in config:
            raise KeyError("'bots' key is missing in the configuration file.")

        # Bot yapılandırmalarını sözlük formatında düzenle
        bots = {
            bot["name"]: {"token": bot["token"], "chat_id": bot["chat_id"]}
            for bot in config["bots"]
        }

        # "main_bot" anahtarının mevcut olup olmadığını kontrol et
        if not bots.get("main_bot"):
            raise ValueError("Bot configuration for 'main_bot' not found.")

        logging.info("Telegram configuration loaded successfully.")
        return bots

    except FileNotFoundError:
        logging.error(f"Configuration file not found: {config_path}")
        raise
    except json.JSONDecodeError as e:
        logging.error(f"Error decoding configuration file: {e}")
        raise
    except KeyError as e:
        logging.error(f"Missing expected key in Telegram configuration: {e}")
        raise
    except ValueError as e:
        logging.error(f"Invalid configuration: {e}")
        raise
    except Exception as e:
        logging.error(f"Unexpected error loading Telegram configuration: {e}")
        raise




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


def connect_db():
    """
    Veritabanına bağlanır ve bağlantı nesnesi döndürür.
    """
    try:
        conn = sqlite3.connect(DB_NAME, check_same_thread=False)
        cursor = conn.cursor()
        return conn, cursor
    except Exception as e:
        logging.error(f"Error connecting to database: {e}")
        send_telegram_message(f"Error connecting to database: {e}", "main_bot", bots)

        return None, None

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



async def fetch_multiple_prices(symbols, retries=3, delay_between_requests=1):
    """
    Fetch live prices for multiple symbols asynchronously.

    Args:
        symbols (list): List of symbols to fetch prices for.
        retries (int): Number of retry attempts for each symbol.
        delay_between_requests (int): Delay between symbol fetches in seconds.

    Returns:
        dict: A dictionary mapping symbols to their fetched prices or None if fetching failed.
    """
    semaphore = asyncio.Semaphore(5)  # Limit concurrent tasks to 5

    async def fetch_with_limit(symbol, driver):
        """
        Fetch the live price for a single symbol with retry and delay.

        Args:
            symbol (str): Trading symbol (e.g., "BTCUSD").
            driver: Shared WebDriver instance.

        Returns:
            float or None: Fetched price as float, or None if fetching failed.
        """
        async with semaphore:
            for attempt in range(retries):
                try:
                    url = f"https://www.tradingview.com/symbols/{symbol.replace(':', '-')}/"
                    logging.info(f"Fetching price for {symbol} (Attempt {attempt + 1}/{retries})")
                    driver.get(url)

                    # Wait for the price element to be located
                    price_element = WebDriverWait(driver, 30).until(
                        EC.presence_of_element_located((By.CSS_SELECTOR, ".js-symbol-last"))
                    )
                    raw_price = price_element.text

                    # Check if the price is empty or invalid
                    if not raw_price.strip():
                        logging.warning(f"Fetched price for {symbol} is empty. Retrying...")
                        await asyncio.sleep(2 ** attempt)  # Exponential backoff
                        continue

                    logging.info(f"Fetched raw price for {symbol}: {raw_price}")

                    # Clean and return the price
                    cleaned_price = clean_price_string(raw_price)
                    if cleaned_price is None:
                        logging.warning(f"Price cleaning failed for {symbol}. Retrying...")
                        await asyncio.sleep(2 ** attempt)
                        continue

                    return cleaned_price

                except Exception as e:
                    logging.error(f"Error fetching price for {symbol}, Attempt {attempt + 1}/{retries}: {e}")
                    await asyncio.sleep(2 ** attempt)  # Exponential backoff for retries

            logging.error(f"Failed to fetch price for {symbol} after {retries} attempts.")
            return None

    # Initialize Chrome WebDriver
    options = Options()
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    options.add_argument('--headless')
    options.add_argument('--disable-gpu')
    options.add_argument('--disable-extensions')
    options.add_argument('--disable-application-cache')
    options.add_argument('--disable-blink-features=AutomationControlled')

    service = Service("/usr/bin/chromedriver")
    driver = webdriver.Chrome(service=service, options=options)

    try:
        # Create tasks for fetching prices
        tasks = {symbol: fetch_with_limit(symbol, driver) for symbol in symbols}
        results = await asyncio.gather(*tasks.values(), return_exceptions=True)

        # Map symbols to their results
        fetched_prices = {}
        for symbol, result in zip(tasks.keys(), results):
            if isinstance(result, Exception):
                logging.error(f"Error with {symbol}: {result}")
                fetched_prices[symbol] = None
            else:
                fetched_prices[symbol] = result

        return fetched_prices

    finally:
        driver.quit()  # Ensure driver is closed after execution
        logging.info("Chrome driver closed.")







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
        send_telegram_message(f"⚠️ Integrity error: Could not save data for {symbol}.")

    except sqlite3.OperationalError as e:
        # Handle operational issues with the database
        conn.rollback()
        logging.error(f"Operational error while saving live data for {symbol}: {e}")
        send_telegram_message(f"⚠️ Database error: Could not save data for {symbol}.")

    except Exception as e:
        # Handle other exceptions
        conn.rollback()
        logging.error(f"Unexpected error saving live data for {symbol}: {e}")
        send_telegram_message(f"❌ Unexpected error: Could not save data for {symbol}. Error: {str(e)}")

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


async def fetch_multiple_prices(symbols, retries=3, delay_between_requests=1, notify_failures=False):
    """
    Birden fazla sembol için fiyatları eşzamanlı olarak çeker.

    Args:
        symbols (list): Çekilecek semboller listesi.
        retries (int): Hata durumunda tekrar deneme sayısı.
        delay_between_requests (int): Talepler arasında gecikme süresi (saniye).
        notify_failures (bool): Başarısızlık durumlarında Telegram bildirimi yapılacak mı.

    Returns:
        dict: Sembol-fiyat eşlemesini içeren bir sözlük.
    """
    semaphore = asyncio.Semaphore(5)  # Maksimum 5 paralel işlem

    async def fetch_with_limit(symbol):
        """
        Tek bir sembol için fiyat alımı.
        """
        async with semaphore:
            driver = None
            for attempt in range(retries):
                try:
                    # ChromeDriver seçeneklerini tanımla
                    options = Options()
                    options.add_argument('--no-sandbox')
                    options.add_argument('--disable-dev-shm-usage')
                    options.add_argument('--headless')
                    options.add_argument('--disable-gpu')
                    options.add_argument('--disable-extensions')
                    options.add_argument('--disable-application-cache')
                    options.add_argument('--disable-blink-features=AutomationControlled')
                    options.add_argument('--log-level=3')

                    service = Service("/usr/bin/chromedriver")
                    driver = webdriver.Chrome(service=service, options=options)

                    # TradingView sayfasına git
                    url = f"https://www.tradingview.com/symbols/{symbol.replace(':', '-')}/"
                    logging.info(f"Fetching price for {symbol} (Attempt {attempt + 1}/{retries})")
                    driver.get(url)

                    # Fiyat öğesini bekle ve al
                    price_element = WebDriverWait(driver, 30).until(
                        EC.presence_of_element_located((By.CSS_SELECTOR, ".js-symbol-last"))
                    )
                    raw_price = price_element.text
                    logging.info(f"Fetched raw price for {symbol}: {raw_price}")

                    # Fiyatı temizle ve döndür
                    cleaned_price = clean_price_string(raw_price)
                    if cleaned_price is None:
                        raise ValueError(f"Invalid price fetched for {symbol}: {raw_price}")

                    return cleaned_price

                except Exception as e:
                    logging.error(f"Error fetching price for {symbol}, Attempt {attempt + 1}/{retries}: {e}")
                    await asyncio.sleep(2 ** attempt)  # Exponential backoff

                finally:
                    if driver:
                        driver.quit()
                        logging.info(f"Chrome driver closed for {symbol}.")

            # Tüm denemeler başarısız olursa None döndür
            error_message = f"Failed to fetch price for {symbol} after {retries} attempts."
            logging.error(error_message)
            if notify_failures:
                send_telegram_message(error_message)
            return None

    # Paralel görevler oluştur ve çalıştır
    tasks = {symbol: fetch_with_limit(symbol) for symbol in symbols}
    results = await asyncio.gather(*tasks.values(), return_exceptions=True)

    # Sembol-fiyat eşlemesi oluştur
    fetched_prices = {}
    for symbol, result in zip(tasks.keys(), results):
        if isinstance(result, Exception):
            logging.error(f"Error with {symbol}: {result}")
            fetched_prices[symbol] = None
        else:
            fetched_prices[symbol] = result

    # İşlemler arasında gecikme uygulayın
    await asyncio.sleep(delay_between_requests)

    return fetched_prices





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
        send_telegram_message(f"⚠️ Error cleaning Chrome processes: {str(e)}")




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

async def run_fetching_cycle(conn, cursor, global_symbols, delay_between_symbols=2, cleanup_interval=1800, min_cycle_duration=30):
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
                            send_telegram_message(f"⚠️ Veri işleme hatası: {symbol}\nHata: {str(process_error)}")
                else:
                    # Fiyat alınamayan semboller için log ve bildirim
                    error_message = f"Failed to fetch live price for {symbol}."
                    logging.error(error_message)
                    send_telegram_message(f"⚠️ {error_message}")

        except Exception as e:
            # Döngü genelindeki hatalar
            logging.error(f"Error in fetching cycle: {e}")
            send_telegram_message(f"❌ Veri Kaydı Hatası: Hata: {str(e)}")

        # Döngünün kalan süresini bekle
        cycle_duration = time.time() - cycle_start
        sleep_duration = max(min_cycle_duration - cycle_duration, 0)
        logging.debug(f"Cycle completed in {cycle_duration:.2f} seconds. Sleeping for {sleep_duration:.2f} seconds.")

        # Uyku modunu kontrol et
        time_since_last_action = (datetime.now() - last_action_time).total_seconds()
        if time_since_last_action > 300:  # 5 dakikalık inaktiflik süresi
            warning_message = f"⚠️ Uyarı: Bot 5 dakikadır işlem yapmıyor. Son işlem zamanı: {last_action_time}"
            logging.warning(warning_message)
            send_telegram_message(warning_message)

        await asyncio.sleep(sleep_duration)


async def main_trading():
    """
    Ana işlem döngüsü: Semboller JSON dosyasından yüklenir, fiyatlar kontrol edilir ve veriler işlenir.
    """
    try:
        # Telegram yapılandırmasını yükle
        telegram_config_path = "/opt/tradebot/globalislemler/config/telegram_bots.json"
        bots = load_telegram_config(telegram_config_path)

        # Sembolleri JSON dosyasından yükle
        symbols_file_path = "/opt/tradebot/globalislemler/config/global_data_config.json"
        with open(symbols_file_path, "r") as file:
            config = json.load(file)

        # global_symbols anahtarını al
        symbols = config.get("global_symbols", [])
        if not symbols:
            raise ValueError("No symbols found in the 'global_symbols' key of symbols.json.")

        logging.info(f"Loaded symbols for trading: {symbols}")

        # Başlatma mesajını gönder
        script_path = os.path.abspath(__file__)
        db_name = DB_NAME
        message = (
            "✅ <b>Program Başlatıldı</b>\n\n"
            f"📂 <b>Dosya Adı:</b> {os.path.basename(script_path)}\n"
            f"📁 <b>Dosya Konumu:</b> {script_path}\n"
            f"🗃️ <b>Veritabanı Adı:</b> {db_name}\n"
            f"📊 <b>Tablo İsimleri:</b>\n"
            f"  - Canlı Veriler: {GLOBAL_LIVE_TABLE}\n"
            f"  - Kapanış Verileri: {GLOBAL_CLOSING_TABLE}\n"
        )
        send_telegram_message(message, "main_bot", bots)

        # Ana döngü
        while True:
            try:
                # Fiyatları al
                prices = await fetch_multiple_prices(symbols)
                for symbol, price in prices.items():
                    if price is not None:
                        # Sadece tanımlı semboller için limit kontrolü yap
                        if symbol in LIMITS:
                            check_price_limits(symbol, price, bots)

                        # Veritabanına canlı veri ve kapanış fiyatlarını kaydet
                        changes = calculate_changes(cursor, symbol, price)
                        await save_live_data(cursor, conn, symbol, price, changes)
                        await save_closing_price(cursor, conn, symbol, price)
                    else:
                        logging.warning(f"Price for {symbol} could not be fetched.")

                await asyncio.sleep(60)  # Döngü bekleme süresi

            except Exception as e:
                logging.error(f"Error in main_trading loop: {e}")
                send_telegram_message(f"❌ Error in main_trading loop: {str(e)}", "main_bot", bots)

    except FileNotFoundError as e:
        logging.error(f"Configuration or symbols file not found: {e}")
        send_telegram_message(f"❌ Configuration or symbols file not found: {e}", "main_bot", bots if 'bots' in locals() else {})
    except json.JSONDecodeError as e:
        logging.error(f"Error decoding symbols.json: {e}")
        send_telegram_message(f"❌ Error decoding symbols.json: {e}", "main_bot", bots if 'bots' in locals() else {})
    except Exception as e:
        logging.error(f"Fatal error in main_trading: {e}")
        send_telegram_message(f"❌ Fatal error in main_trading: {str(e)}", "main_bot", bots if 'bots' in locals() else {})


# Ana giriş noktası
if __name__ == "__main__":
    try:
        # Telegram yapılandırma yolunu belirle
        telegram_config_path = "/root/Botson/9_simsar/Dominance/simsar/Dominace_2/Devin_2/futures_trading_tegram.json"

        # Telegram yapılandırmasını yükle
        bots = load_telegram_config(telegram_config_path)

        # Veritabanı bağlantısını oluştur
        conn, cursor = connect_db()
        if not conn or not cursor:
            raise ConnectionError("Failed to connect to the database.")

        # Veritabanı tablolarını oluştur
        create_global_tables(cursor)
        conn.commit()

        # Başlatma mesajını gönder
        #send_telegram_message("✅ Program başarıyla başlatıldı.", "main_bot", bots)

        # Sembolleri kontrol et ve yükle
        symbols_path = "/root/Botson/9_simsar/Dominance/simsar/Dominace_2/Devin_2/symbols.json"
        if not os.path.exists(symbols_path):
            raise FileNotFoundError(f"Symbols configuration file not found: {symbols_path}")

        with open(symbols_path, "r") as file:
            config = json.load(file)

        global_symbols = config.get("global_symbols", [])
        if not global_symbols:
            raise ValueError("No symbols found in the 'global_symbols' key of the symbols configuration file.")

        logging.info(f"Loaded global symbols: {global_symbols}")

        # Ana işlem döngüsünü başlat
        asyncio.run(main_trading())
    
    except FileNotFoundError as e:
        logging.error(f"File not found: {e}", exc_info=True)
        if 'bots' in locals():
            send_telegram_message(f"❌ Configuration file error: {e}", "main_bot", bots)

    except ConnectionError as e:
        logging.error(f"Database connection error: {e}", exc_info=True)
        if 'bots' in locals():
            send_telegram_message(f"❌ Database connection error: {e}", "main_bot", bots)

    except ValueError as e:
        logging.error(f"Configuration error: {e}", exc_info=True)
        if 'bots' in locals():
            send_telegram_message(f"❌ Configuration error: {e}", "main_bot", bots)

    except Exception as e:
        logging.error(f"Unexpected fatal error: {e}", exc_info=True)
        if 'bots' in locals():
            send_telegram_message(f"❌ Fatal Error: {e}", "main_bot", bots)
    
    finally:
        # Veritabanı bağlantısını kapat
        if 'conn' in locals() and conn:
            conn.close()
            logging.info("Database connection closed.")
            if 'bots' in locals():
                send_telegram_message("🔌 Database connection closed.", "main_bot", bots)
import time
time.sleep(30)
