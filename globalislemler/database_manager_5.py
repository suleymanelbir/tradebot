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
# 🔧 Sistem ve dosya işlemleri
# ─────────────────────────────────────────────
# 📦 Standart Kütüphaneler
# ─────────────────────────────────────────────
import os
from dotenv import load_dotenv
load_dotenv(dotenv_path="/opt/tradebot/trade_env/.env")
import sys
import json
import sqlite3
import logging
import asyncio
import time
import tempfile
import shutil
import random
import math
import html
from pathlib import Path
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Literal, Tuple
from logging.handlers import RotatingFileHandler

# JSON logger için
from pythonjsonlogger import jsonlogger as JsonFormatter

# HTTP istekleri için
import requests

# Playwright (aktif kullanım için)
from playwright.async_api import async_playwright, TimeoutError as PWTimeout
print("✅ Script başladı")
# ─────────────────────────────────────────────
# 📁 Merkezî Yol Sabitleri (ENV ile override edilebilir)
# ─────────────────────────────────────────────
CONFIG_DIR = os.getenv("TRADEBOT_CONFIG_DIR", "/opt/tradebot/globalislemler/config")
DB_PATH    = os.getenv("TRADEBOT_DB_PATH",    "/opt/tradebot/veritabani/global_data.db")
LOG_DIR    = os.getenv("TRADEBOT_LOG_DIR",    "/opt/tradebot/log")

# 📂 Dizinleri garantiye al
Path(LOG_DIR).mkdir(parents=True, exist_ok=True)
Path(Path(DB_PATH).parent).mkdir(parents=True, exist_ok=True)

# 📄 Config dosyaları (varsayılan)
GLOBAL_CFG   = os.path.join(CONFIG_DIR, "global_data_config.json")
TELEGRAM_CFG = os.path.join(CONFIG_DIR, "telegram_bots.json")
SYMBOLS_CFG  = os.path.join(CONFIG_DIR, "symbols.json")

# ─────────────────────────────────────────────
# 🗃️ Veritabanı ve Tablo Sabitleri
# ─────────────────────────────────────────────
DB_NAME = DB_PATH  # sqlite için tam yol

GLOBAL_LIVE_TABLE    = "global_live_data"
GLOBAL_CLOSING_TABLE = "global_closing_data"

RECORD_LIMITS = {
    GLOBAL_LIVE_TABLE: 1500,
    GLOBAL_CLOSING_TABLE: 5000,
    "global_close_15m": 100000,
    "global_close_1h":  50000,
    "global_close_4h":  30000,
}

# ─────────────────────────────────────────────
# 📊 Sembol Limitleri (JSON ile override edilebilir)
# ─────────────────────────────────────────────
LIMITS = {
    "CRYPTOCAP:USDT.D": {"lower": 3.0, "upper": 7.0},
    "CRYPTOCAP:BTC.D":  {"lower": 40.0, "upper": 70.0}
}

# ─────────────────────────────────────────────
# 📅 Kapanış Tablosu Şeması
# ─────────────────────────────────────────────
DAILY_TABLE_NAME     = "global_closing_data"
DAILY_PRICE_COL      = "price"
DAILY_TS_COL         = "timestamp"
DAILY_INTERVAL_COL   = "interval"
DAILY_INTERVAL_VALUE = "1D"

# 🔁 Kapanış Tekrarını Önleme Toleransları
INTERVAL_DUPLICATE_CHECK = {
    "15m": timedelta(minutes=12),
    "1h":  timedelta(minutes=53, seconds=30),
    "4h":  timedelta(hours=3, minutes=50, seconds=15),
    "1d":  timedelta(hours=23, minutes=50, seconds=45)
}

# ─────────────────────────────────────────────
# 📡 Canlı Veri Tablosu Şeması
# ─────────────────────────────────────────────
LIVE_TABLE_NAME = "global_live_data"
LIVE_PRICE_COL  = "live_price"
LIVE_TS_COL     = "timestamp"

TIMEFRAME_SECONDS = {
    "15m": 15 * 60,
    "1h": 60 * 60,
    "4h": 4 * 60 * 60,
    "1d": 24 * 60 * 60,
}

# ─────────────────────────────────────────────
# 💓 Heartbeat Sayaçları ve Durum İzleyiciler
# ─────────────────────────────────────────────
PROCESS_START_TS: float = time.time()
FETCH_SUCCESS_COUNT: int = 0
FETCH_ERROR_COUNT: int = 0

LAST_HEARTBEAT_TS: Optional[int] = None
LAST_LIVE_INSERT_TS: Optional[int] = None
LAST_LOOP_TS: Optional[int] = None

SYMBOL_LAST_OK_TS: Dict[str, int] = {}  # sembol bazında son başarılı insert epoch

# 🔔 Uyarı Cooldown Mekanizması
LAST_ALERT_TS: Dict[str, int] = {"global_stale": 0}
SYMBOL_ALERT_TS: Dict[str, int] = {}
ALERT_COOLDOWN_SEC = 900  # 15 dk

# 🧠 JSON'dan yüklenecek sağlık konfigürasyonu
HEALTH_CFG: Dict[str, Any] = {}

# USDT.D / BTC.D yüzde cinsinden -> 0.01% mutlak tolerans (1 bp)
# TOTAL, TOTAL2, TOTAL3 market cap -> çok küçük göreli tolerans + 1 birim mutlak
PRICE_TOLERANCES = {
    "CRYPTOCAP:USDT.D": {"abs": 0.01, "rel": 0.0},
    "CRYPTOCAP:BTC.D":  {"abs": 0.01, "rel": 0.0},
    "CRYPTOCAP:TOTAL":  {"abs": 1.0,  "rel": 1e-12},
    "CRYPTOCAP:TOTAL2": {"abs": 1.0,  "rel": 1e-12},
    "CRYPTOCAP:TOTAL3": {"abs": 1.0,  "rel": 1e-12},
}

# Varsayılan (diğer semboller): makul bir tolerans
DEFAULT_REL_TOL = 1e-9
DEFAULT_ABS_TOL = 1e-8

# Zorunlu snapshot aralığı (saniye)
FORCE_SAVE_INTERVAL_SEC = 300  # 5 dakika

def get_tolerances(symbol: str) -> Tuple[float, float]:
    cfg = PRICE_TOLERANCES.get(symbol, {})
    abs_tol = float(cfg.get("abs", DEFAULT_ABS_TOL))
    rel_tol = float(cfg.get("rel", DEFAULT_REL_TOL))
    return rel_tol, abs_tol

def prices_equivalent(new_price: float, last_price: float, symbol: str) -> bool:
    rel_tol, abs_tol = get_tolerances(symbol)
    return math.isclose(float(new_price), float(last_price),
                        rel_tol=rel_tol, abs_tol=abs_tol)

def should_force_save(symbol: str, now_dt, symbol_last_ok_ts: dict) -> bool:
    """
    symbol_last_ok_ts: SYMBOL_LAST_OK_TS sözlüğünüz (symbol -> epoch seconds)
    """
    try:
        last_ok_epoch = symbol_last_ok_ts.get(symbol)
        if last_ok_epoch is None:
            return True  # İlk kayıt
        # now_dt aware/naive olabilir; epoch'a çevir
        now_epoch = getattr(now_dt, "timestamp", lambda: None)()
        if now_epoch is None:
            import time as _t
            now_epoch = _t.time()
        return (now_epoch - last_ok_epoch) >= FORCE_SAVE_INTERVAL_SEC
    except Exception:
        return True


class JsonFormatter(logging.Formatter):
    def format(self, record):
        log_record = {
            "timestamp": self.formatTime(record, self.datefmt),
            "level": record.levelname,
            "message": record.getMessage(),
            "source": record.name,
            "module": record.module,
            "function": record.funcName,
            "line": record.lineno
        }
        return json.dumps(log_record)
    
def setup_logging(log_path: str, max_bytes=2_000_000, backup_count=3):
    # 🔧 Log klasörü yoksa oluştur
    os.makedirs(os.path.dirname(log_path), exist_ok=True)

    # 🔧 Logger tanımı
    logger = logging.getLogger("tradebot_logger")
    logger.handlers.clear()

    # 🔧 Log seviyesi .env üzerinden alınır, yoksa INFO
    log_level = os.getenv("LOG_LEVEL", "INFO").upper()
    logger.setLevel(getattr(logging, log_level, logging.INFO))

    # 📁 Genel log dosyası
    main_handler = RotatingFileHandler(log_path, maxBytes=max_bytes, backupCount=backup_count)
    main_handler.setFormatter(JsonFormatter.JsonFormatter(datefmt="%Y-%m-%dT%H:%M:%S"))
    logger.addHandler(main_handler)

    # 🔔 Hatalar için ayrı alerts.log dosyası
    alerts_path = os.path.join(os.path.dirname(log_path), "alerts.log")
    alerts_handler = RotatingFileHandler(alerts_path, maxBytes=1_000_000, backupCount=2)
    alerts_handler.setLevel(logging.ERROR)
    alerts_handler.setFormatter(JsonFormatter.JsonFormatter(datefmt="%Y-%m-%dT%H:%M:%S"))
    logger.addHandler(alerts_handler)

    # 🖥️ Systemd journal için terminale yönlendirme
    stream_handler = logging.StreamHandler(sys.stdout)
    stream_handler.setFormatter(JsonFormatter.JsonFormatter(datefmt="%Y-%m-%dT%H:%M:%S"))
    logger.addHandler(stream_handler)

    # 🧾 Başlangıç logları
    logger.info("Log sistemi JSON formatında başlatıldı.")
    logger.info("RUNNING_FROM_FILE=%s", __file__)

    return logger




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

def to_sqlite_dt(dt: datetime) -> str:
    """UTC datetime → 'YYYY-MM-DD HH:MM:SS' (SQLite ile sıralanabilir)"""
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


def bucket_start(ts_dt: datetime, tf: str) -> int:
    """UTC datetime'i tf periyodunun başlangıç epoch’una indirger."""
    s = TIMEFRAME_SECONDS[tf]
    ts = int(ts_dt.replace(tzinfo=timezone.utc).timestamp())
    return (ts // s) * s

def last_completed_bucket_start(now_dt: datetime, tf: str) -> int:
    """Şu an için bitmiş son periyodun başlangıç epoch’u."""
    s = TIMEFRAME_SECONDS[tf]
    cur_bucket = bucket_start(now_dt, tf)
    return cur_bucket - s


# ─────────────────────────────────────────────────────────────
# Hearbeat için kullanılan foonkisyonlar
# ─────────────────────────────────────────────────────────────
def _max_int(cursor, sql: str, params: tuple = ()) -> Optional[int]:
    try:
        cursor.execute(sql, params)
        row = cursor.fetchone()
        return int(row[0]) if row and row[0] is not None else None
    except Exception as e:
        logging.debug(f"_max_int failed: {e}")
        return None

def _count_int(cursor, sql: str, params: tuple = ()) -> Optional[int]:
    try:
        cursor.execute(sql, params)
        row = cursor.fetchone()
        return int(row[0]) if row and row[0] is not None else None
    except Exception as e:
        logging.debug(f"_count_int failed: {e}")
        return None

def _fmt_age(sec: Optional[int]) -> str:
    if not sec or sec < 0:
        return "?"
    if sec < 90:
        return f"{sec}s"
    m = sec // 60
    if m < 90:
        return f"{m}m"
    h = m // 60
    if h < 36:
        return f"{h}h"
    d = h // 24
    return f"{d}d"

def collect_health_snapshot(conn, cursor, symbols: list[str]) -> Dict[str, Any]:
    """
    DB ve sayaçlardan hafif bir sağlık özeti üretir.
    Ağır sorgu yok; MAX/COUNT gibi hızlı toplamlardır.
    """
    now_epoch = int(time.time())
    snapshot: Dict[str, Any] = {
        "now_epoch": now_epoch,
        "uptime_sec": now_epoch - int(PROCESS_START_TS),
        "symbols_count": len(symbols),
        "fetch_success": FETCH_SUCCESS_COUNT,
        "fetch_error": FETCH_ERROR_COUNT,
        "last_live_insert_ts": LAST_LIVE_INSERT_TS,
        "per_symbol_last_ok_ts": dict(SYMBOL_LAST_OK_TS),
        "db": {"live": {}, "close": {}},
    }

    # global_live_data
    last_live = _max_int(cursor,
        "SELECT MAX(COALESCE(ts_utc, CAST(strftime('%s', timestamp) AS INTEGER))) FROM global_live_data")
    live_rows = _count_int(cursor, "SELECT COUNT(*) FROM global_live_data")
    snapshot["db"]["live"] = {
        "last_ts": last_live,
        "last_age_sec": (now_epoch - last_live) if last_live else None,
        "rows": live_rows,
    }

    # periyot kapanış kovaları (varsa)
    for tbl, key in (("global_close_15m", "15m"),
                     ("global_close_1h",  "1h"),
                     ("global_close_4h",  "4h")):
        last_bucket = None
        rows = None
        try:
            last_bucket = _max_int(cursor, f"SELECT MAX(ts_bucket_utc) FROM {tbl}")
            rows = _count_int(cursor, f"SELECT COUNT(*) FROM {tbl}")
        except Exception:
            pass
        snapshot["db"]["close"][key] = {
            "last_bucket": last_bucket,
            "last_age_sec": (now_epoch - last_bucket) if last_bucket else None,
            "rows": rows,
        }

    # 1D kapanışlar (global_closing_data)
    last_1d = _max_int(cursor, """
        SELECT MAX(CAST(strftime('%s', timestamp) AS INTEGER))
        FROM global_closing_data
        WHERE UPPER(interval)='1D'
    """)
    rows_1d = _count_int(cursor, "SELECT COUNT(*) FROM global_closing_data WHERE UPPER(interval)='1D'")
    snapshot["db"]["close"]["1d"] = {
        "last_ts": last_1d,
        "last_age_sec": (now_epoch - last_1d) if last_1d else None,
        "rows": rows_1d,
    }

    # DB dosya boyutu (opsiyonel)
    try:
        db_path = DB_NAME  # zaten global
        if db_path and os.path.exists(db_path):
            snapshot["db"]["file_size_bytes"] = os.path.getsize(db_path)
    except Exception:
        pass

    return snapshot

def format_heartbeat_text(s: Dict[str, Any]) -> str:
    up = _fmt_age(s.get("uptime_sec"))
    live = s["db"]["live"]
    live_age = _fmt_age(live.get("last_age_sec"))
    rows = live.get("rows")

    c15 = s["db"]["close"].get("15m", {})
    c1h = s["db"]["close"].get("1h", {})
    c4h = s["db"]["close"].get("4h", {})
    c1d = s["db"]["close"].get("1d", {})

    parts = [
        "✅ <b>Heartbeat</b> (yaşıyorum)",
        f"⏱️ Uptime: <b>{up}</b>",
        f"📈 Sembol sayısı: <b>{s.get('symbols_count')}</b>",
        f"🧩 Fetch: ok={s.get('fetch_success')} err={s.get('fetch_error')}",
        f"💾 Canlı yazım: <b>{live_age}</b> önce (rows={rows})",
        f"🕒 Kova 15m: {_fmt_age(c15.get('last_age_sec'))}",
        f"🕒 Kova 1h:  {_fmt_age(c1h.get('last_age_sec'))}",
        f"🕒 Kova 4h:  {_fmt_age(c4h.get('last_age_sec'))}",
        f"📅 1D close: {_fmt_age(c1d.get('last_age_sec'))}",
    ]
    return "\n".join(parts)

def post_n8n_if_configured(payload: Dict[str, Any], health_cfg: Dict[str, Any]) -> None:
    """
    n8n Webhook URL'i ENV veya JSON'dan alınır. Boşsa gönderim yapılmaz.
    """
    url = os.environ.get("N8N_WEBHOOK_URL") or (health_cfg.get("n8n_webhook_url") or "")
    if not url:
        return
    headers = {"Content-Type": "application/json"}
    auth_hdr = os.environ.get("N8N_AUTH_HEADER") or health_cfg.get("n8n_auth_header")
    if auth_hdr:
        headers["Authorization"] = auth_hdr
    try:
        resp = requests.post(url, json=payload, headers=headers, timeout=5)
        resp.raise_for_status()
        logging.info("Heartbeat snapshot n8n'a gönderildi.")
    except Exception as e:
        logging.warning(f"n8n gönderimi başarısız: {e}")



# ─────────────────────────────────────────────────────────────
# playwright için kullanılan fonksiyon
# ─────────────────────────────────────────────────────────────
async def fetch_multiple_prices_playwright(
    symbols: List[str],
    retries: int = 3,
    timeout_ms: int = 30000,
    concurrency: int = 2,
) -> Dict[str, Optional[float]]:
    """
    Playwright ile fiyatları çeker.
    - Tek Chromium process; her sembol için izole BrowserContext.
    - concurrency ile aynı anda kaç context/page açılacağı kontrol edilir.
    - retry + exponential backoff desteklenir.
    """
    sem = asyncio.Semaphore(max(1, concurrency))

    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=True,
            args=[
                "--no-sandbox",
                "--disable-dev-shm-usage",
                "--disable-gpu",
                "--disable-extensions",
                "--disable-background-networking",
                "--metrics-recording-only",
                "--mute-audio",
                "--hide-scrollbars",
            ],
        )

        async def fetch_one(sym: str) -> Optional[float]:
            for attempt in range(retries):
                context = None
                page = None
                try:
                    async with sem:
                        context = await browser.new_context()   # izole session
                        page = await context.new_page()
                        url = f"https://www.tradingview.com/symbols/{sym.replace(':','-')}/"
                        logging.info(f"[PW:{sym}] try {attempt+1}/{retries}")
                        await page.goto(url, wait_until="domcontentloaded", timeout=timeout_ms)

                        # TradingView fiyatı (Selenium’da kullandığın CSS ile aynı)
                        await page.wait_for_selector(".js-symbol-last", timeout=timeout_ms)
                        raw = await page.text_content(".js-symbol-last")
                        raw = (raw or "").strip()
                        if not raw:
                            logging.warning(f"[PW:{sym}] empty price; retrying…")
                            await asyncio.sleep(2 ** attempt)
                            continue

                        price = clean_price_string(raw)
                        if price is None:
                            logging.warning(f"[PW:{sym}] cleaning failed; retrying…")
                            await asyncio.sleep(2 ** attempt)
                            continue

                        return price

                except (PWTimeout, Exception) as e:
                    logging.error(f"[PW:{sym}] error (try {attempt+1}/{retries}): {e}")
                    await asyncio.sleep(2 ** attempt)

                finally:
                    try:
                        if page:
                            await page.close()
                        if context:
                            await context.close()
                    except Exception:
                        pass

            logging.error(f"[PW:{sym}] failed after {retries} attempts")
            return None

        results = await asyncio.gather(*(fetch_one(s) for s in symbols))
        await browser.close()

    return dict(zip(symbols, results))


# ─────────────────────────────────────────────────────────────
# 6) GİRİŞ NOKTASI
# ─────────────────────────────────────────────────────────────

# Telegram mesaj gönderme fonksiyonu (güncellenmiş)
def send_telegram_message(message: str, bot_name: str, bots: dict, retries: int = 3, timeout: int = 10):
    """
    Robust Telegram sender:
    - Logs Telegram response body on 4xx/5xx
    - Splits long texts (>4096)
    - Falls back from HTML to plain text on parse errors
    - Detects 'chat not found' / 403 and stops retrying
    - Respects 429 Retry-After (doesn't consume attempts)
    - Exponential backoff with jitter
    """
    import requests, time, logging, html, random

    MAX_LEN = 4096

    def _split(msg: str) -> list[str]:
        if msg is None:
            return [""]
        msg = str(msg)
        if len(msg) <= MAX_LEN:
            return [msg]
        parts, rest = [], msg
        while len(rest) > MAX_LEN:
            cut = rest.rfind('\n', 0, MAX_LEN)
            if cut == -1 or cut < int(MAX_LEN * 0.6):
                cut = MAX_LEN
            parts.append(rest[:cut])
            rest = rest[cut:]
        if rest:
            parts.append(rest)
        return parts

    def _log_body(resp):
        try:
            logging.error(f"Telegram response body: {resp.text}")
        except Exception:
            pass

    # --- Config checks
    bot = bots.get(bot_name) if isinstance(bots, dict) else None
    if not bot or not bot.get("token") or not bot.get("chat_id"):
        raise ValueError(f"Invalid bot config for '{bot_name}'. Check 'token' and 'chat_id'.")

    if message is None or not str(message).strip():
        raise ValueError("Telegram: Empty message cannot be sent.")

    url = f"https://api.telegram.org/bot{bot['token']}/sendMessage"
    chat_id = str(bot["chat_id"])  # güvenli tarafta kal
    chunks = _split(str(message))

    # Uzun metinlerde HTML tag bölünmesini önlemek için baştan düz metin tercih et
    prefer_plain_for_all = (len(chunks) > 1)

    for idx, chunk in enumerate(chunks, 1):
        try_html = (not prefer_plain_for_all)  # kısa tek parça ise önce HTML dene
        attempt = 0
        sent = False

        while attempt < retries and not sent:
            payload = {
                "chat_id": chat_id,
                "text": chunk,
                "disable_web_page_preview": True,
                "disable_notification": False
            }
            if try_html:
                payload["parse_mode"] = "HTML"

            try:
                r = requests.post(url, json=payload, timeout=timeout)

                # 4xx/5xx için gövdeyi logla
                if r.status_code >= 400:
                    _log_body(r)
                    body = (r.text or "").lower()

                    # 403 => bot engellenmiş / yetkisiz → retry etme
                    if r.status_code == 403 or "forbidden" in body or "bot was blocked" in body:
                        logging.error(f"Forbidden/blocked for bot '{bot_name}' and chat_id '{chat_id}'.")
                        break  # attempt harcamayı bırak

                    # 429 => hız limiti → Retry-After'a saygı duy, attempt'ı tüketme
                    if r.status_code == 429 or "too many requests" in body:
                        # Telegram çoğu zaman Retry-After döndürür
                        ra = r.headers.get("Retry-After")
                        wait_sec = int(ra) if ra and ra.isdigit() else 3
                        # küçük jitter
                        wait_sec = max(1, wait_sec) + random.uniform(0, 0.5)
                        logging.warning(f"Rate limited (429). Waiting {wait_sec:.2f}s then retrying (won't consume attempt).")
                        time.sleep(wait_sec)
                        # attempt'ı tüketmemek için azalt
                        # (bu döngünün başında artıracağız)
                        # NOT: Burada attempt zaten artırılmadıysa değiştirmeye gerek yok; ama biz başta artıracağız:
                        # Bu nedenle önce attempt artırma modelini değiştiriyoruz:
                        # -> attempt artırmayı request'ten SONRA yapacağız.
                        # Ancak mevcut yapıyı bozmayalım; pratik çözüm:
                        attempt -= 1
                        continue

                    # chat not found → retry etmeye gerek yok
                    if "chat not found" in body:
                        logging.error(f"Telegram chat_id not found for bot '{bot_name}': {chat_id}")
                        break

                    # HTML parse hatası → düz metne düş
                    if "can't parse entities" in body or "parse" in body and "html" in body:
                        logging.warning(f"Telegram HTML parse hatası ({bot_name}) part {idx}; düz metne geçiliyor.")
                        try_html = False
                        chunk = html.unescape(chunk)
                        # aynı attempt'i tüketmeden tekrar dene
                        attempt -= 1
                        continue

                r.raise_for_status()
                logging.info(f"Telegram OK ({bot_name}) part {idx}/{len(chunks)}")
                sent = True

            except requests.Timeout:
                logging.warning(f"Telegram timeout ({bot_name}) try {attempt+1}/{retries} (part {idx}).")
            
            except requests.RequestException as e:
                resp = getattr(e, "response", None)
                if resp is not None:
                    _log_body(resp)
                logging.error(f"Telegram error ({bot_name}) try {attempt+1}/{retries} (part {idx}): {e}")

            # next attempt (exponential backoff + jitter), eğer gönderilemediyse
            if not sent:
                attempt += 1
                if attempt < retries:
                    backoff = (2 ** (attempt - 1)) + random.uniform(0, 0.25)
                    logging.info(f"Retrying in {backoff:.2f}s...")
                    time.sleep(backoff)

        if not sent:
            raise Exception(f"Failed to send Telegram message for '{bot_name}' after {retries} tries (part {idx}).")


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

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS global_close_15m (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        symbol TEXT NOT NULL,
        ts_bucket_utc INTEGER NOT NULL,
        close_price REAL NOT NULL,
        updated_at_utc INTEGER NOT NULL,
        UNIQUE(symbol, ts_bucket_utc)
    )
    """)
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_gc15_symbol ON global_close_15m(symbol)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_gc15_bucket ON global_close_15m(ts_bucket_utc)")

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS global_close_1h (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        symbol TEXT NOT NULL,
        ts_bucket_utc INTEGER NOT NULL,
        close_price REAL NOT NULL,
        updated_at_utc INTEGER NOT NULL,
        UNIQUE(symbol, ts_bucket_utc)
    )
    """)
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_gc1h_symbol ON global_close_1h(symbol)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_gc1h_bucket ON global_close_1h(ts_bucket_utc)")

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS global_close_4h (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        symbol TEXT NOT NULL,
        ts_bucket_utc INTEGER NOT NULL,
        close_price REAL NOT NULL,
        updated_at_utc INTEGER NOT NULL,
        UNIQUE(symbol, ts_bucket_utc)
    )
    """)
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_gc4h_symbol ON global_close_4h(symbol)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_gc4h_bucket ON global_close_4h(ts_bucket_utc)")

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

def save_period_close(cursor, conn, symbol: str, price: float, now_dt: datetime, tf: str) -> None:
    """
    Verilen periyot için (15m/1h/4h) o anki bucket’a close fiyatını UPSERT eder.
    Periyot bitimine kadar her döngüde güncellenir; periyot bittiğinde son yazılan değer kapanıştır.
    """
    assert tf in ("15m", "1h", "4h")
    table = {"15m": "global_close_15m", "1h": "global_close_1h", "4h": "global_close_4h"}[tf]
    bstart = bucket_start(now_dt, tf)
    now_epoch = int(now_dt.replace(tzinfo=timezone.utc).timestamp())

    cursor.execute(f"""
        INSERT INTO {table} (symbol, ts_bucket_utc, close_price, updated_at_utc)
        VALUES (?, ?, ?, ?)
        ON CONFLICT(symbol, ts_bucket_utc)
        DO UPDATE SET close_price=excluded.close_price, updated_at_utc=excluded.updated_at_utc
    """, (symbol, bstart, price, now_epoch))
    conn.commit()

def get_reference_close(
    cursor,
    symbol: str,
    tf: Literal["15m", "1h", "4h", "1d"],
    now_dt: datetime
) -> Optional[float]:
    """
    Yüzde hesabında kullanılacak referans kapanışını döndürür.
    - 15m/1h/4h: bitmiş son kovayı hedefler; yoksa en yakın önceki kovayı alır.
    - 1d: global_closing_data’dan son *tam gün* kapanışını (bugün hariç) alır.

    Dönüş: float (kapanış fiyatı) veya None
    """
    # Sembolü güvenle temizle
    try:
        sym = clean_symbol(symbol) if "clean_symbol" in globals() else symbol
    except Exception:
        sym = symbol

    try:
        if tf in ("15m", "1h", "4h"):
            table = {
                "15m": "global_close_15m",
                "1h":  "global_close_1h",
                "4h":  "global_close_4h",
            }[tf]

            # Bitmiş son kovanın başlangıcı (UTC)
            target_bucket = last_completed_bucket_start(now_dt, tf)

            # 1) Tam hedef kova varsa onu al
            cursor.execute(
                f"SELECT close_price FROM {table} WHERE symbol=? AND ts_bucket_utc=? LIMIT 1",
                (sym, int(target_bucket)),
            )
            row = cursor.fetchone()
            if row is not None and row[0] is not None:
                try:
                    val = float(row[0])
                    return val if val > 0 else None
                except Exception:
                    pass  # parsing hatası durumunda aşağıdaki fallback'e düş

            # 2) En yakın önceki kova
            cursor.execute(
                f"""
                SELECT close_price
                FROM {table}
                WHERE symbol=? AND ts_bucket_utc < ?
                ORDER BY ts_bucket_utc DESC
                LIMIT 1
                """,
                (sym, int(target_bucket)),
            )
            row = cursor.fetchone()
            if row is not None and row[0] is not None:
                try:
                    val = float(row[0])
                    return val if val > 0 else None
                except Exception:
                    return None

            return None

        elif tf == "1d":
            # Bugünün UTC 00:00 sınırı (bugün hariç en yakın günlük kapanışı alacağız)
            midnight_today_epoch = bucket_start(now_dt, "1d")  # epoch (int)
            boundary = to_sqlite_dt(datetime.fromtimestamp(midnight_today_epoch, tz=timezone.utc))

            # 1D kapanışlar 'global_closing_data' tablosunda: (timestamp, symbol, price, interval)
            # interval için hem '1D' hem '1d' kayıtları varsa ikisini de kapsayalım
            cursor.execute(
                f"""
                SELECT {DAILY_PRICE_COL}
                FROM {DAILY_TABLE_NAME}
                WHERE symbol = ?
                AND {DAILY_INTERVAL_COL} IN ('1D', '1d')
                AND {DAILY_TS_COL} < ?
                ORDER BY {DAILY_TS_COL} DESC
                LIMIT 1
                """,
                (sym, boundary),
            )
            row = cursor.fetchone()
            if row is not None and row[0] is not None:
                try:
                    val = float(row[0])
                    return val if val > 0 else None
                except Exception:
                    return None

            return None


        else:
            logging.warning(f"[{sym}] get_reference_close: bilinmeyen timeframe: {tf}")
            return None

    except Exception as e:
        logging.warning(f"[{sym}] get_reference_close error for {tf}: {e}", exc_info=False)
        return None


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





def clean_symbol(symbol): 
    return symbol.strip().replace(' ', '')

async def save_closing_price(
    cursor,
    conn,
    symbol: str,
    live_price: float,
    now: datetime | None = None
) -> None:
    """
    15m/1h/4h kapanışlarını periyot tablolarına (global_close_15m/_1h/_4h),
    1D kapanışını ise global_closing_data tablosuna yazar.

    - Kova etiketleri: last_completed_bucket_start(now, tf) (UTC epoch, bitmiş kova)
    - Günlük etiket:   last_completed_bucket_start(now, "1d") → UTC midnight (TEXT)
    - UPSERT kullanır (UNIQUE kısıtlarına göre günceller).
    """
    if now is None:
        now = datetime.now(timezone.utc)

    try:
        sym = clean_symbol(symbol)
    except Exception:
        sym = symbol

    try:
        # -------------------------
        # 1) 15m / 1h / 4h kapanışları
        # -------------------------
        tf_table_map = {
            "15m": "global_close_15m",
            "1h":  "global_close_1h",
            "4h":  "global_close_4h",
        }

        for tf, table in tf_table_map.items():
            try:
                bucket_start_epoch = last_completed_bucket_start(now, tf)  # int (UTC epoch)
                now_epoch = int(now.replace(tzinfo=timezone.utc).timestamp())  # ya da int(datetime.now(timezone.utc).timestamp())

                cursor.execute(
                    f"""
                    INSERT INTO {table} (symbol, ts_bucket_utc, close_price, updated_at_utc)
                    VALUES (?, ?, ?, ?)
                    ON CONFLICT(symbol, ts_bucket_utc) DO UPDATE SET
                        close_price     = excluded.close_price,
                        updated_at_utc  = excluded.updated_at_utc
                    """,
                    (sym, int(bucket_start_epoch), float(live_price), now_epoch),
                )
            except sqlite3.OperationalError as oe:
                # Tablo yoksa ya da şema uyumsuzsa anlaşılır log üret
                logging.error(f"[{sym}] save_closing_price: {table} yazımı sırasında OperationalError: {oe}")
                # İstersen burada tablo oluşturmayı tetikleyebilirsin (create_global_tables)
                # raise  # İstersen yükselt

        # -------------------------
        # 2) Günlük (1D) kapanışı
        # -------------------------
        try:
            day_bucket_epoch = last_completed_bucket_start(now, "1d")
            day_bucket_dt    = datetime.fromtimestamp(day_bucket_epoch, tz=timezone.utc)
            day_ts_text      = to_sqlite_dt(day_bucket_dt)  # 'YYYY-MM-DD HH:MM:SS' (UTC)

            cursor.execute(
                f"""
                INSERT INTO {DAILY_TABLE_NAME}
                    ({DAILY_TS_COL}, symbol, {DAILY_PRICE_COL}, {DAILY_INTERVAL_COL})
                VALUES (?, ?, ?, ?)
                ON CONFLICT(symbol, {DAILY_TS_COL}, {DAILY_INTERVAL_COL}) DO UPDATE SET
                    {DAILY_PRICE_COL} = excluded.{DAILY_PRICE_COL}
                """,
                (day_ts_text, sym, float(live_price), DAILY_INTERVAL_VALUE),
            )
        except sqlite3.OperationalError as oe:
            logging.error(f"[{sym}] save_closing_price: {DAILY_TABLE_NAME} yazımı sırasında OperationalError: {oe}")
            # raise  # İstersen yükselt

        # Tek commit
        conn.commit()

        # İsteğe bağlı, sembol bazlı bilgi logları
        # if sym == "CRYPTOCAP:USDT.D":
        #     logging.info(f"[{sym}] close upsert OK | 15m/1h/4h + 1D")

    except sqlite3.Error as e:
        conn.rollback()
        logging.error(f"[{sym}] save_closing_price: Database error: {e}", exc_info=True)

    except Exception as e:
        conn.rollback()
        logging.error(f"[{sym}] save_closing_price: Unexpected error: {e}", exc_info=True)






async def save_live_data(cursor, conn, symbol, live_price, changes, now=None):
    """
    Canlı fiyatı ve yüzde değişimlerini veritabanına yazar.

    Eklemeler:
      - Fiyat eşitliği toleranslı kontrol (symbol bazlı abs/rel tolerans).
      - Uzun süre değişim yoksa zorunlu snapshot (default 5 dk).
      - Zaman damgası to_sqlite_dt(now) ile ISO string olarak yazılır.
      - Uyarılar Telegram'a JSON formatında iletilir (parse sorunlarını azaltır).

    Notlar:
      - İmza ve genel akış korunmuştur.
      - CRYPTOCAP:USDT.D için emniyet bandı (3–8) KORUNDU (band dışıysa kayıt yapılmaz).
      - Kayıt başarılı olursa heartbeat sayaçları güncellenir:
          LAST_LIVE_INSERT_TS ve SYMBOL_LAST_OK_TS[symbol]
      - Telegram gönderimleri güvenli-opsiyonel (bots global değişkeninden okunur).
    """
    # ----------------------------- Hazırlık -----------------------------
    from datetime import datetime, timezone
    import math, json, time, logging, sqlite3

    if now is None:
        now = datetime.now(timezone.utc)

    # Sembol normalize
    try:
        clean_sym = clean_symbol(symbol)
    except Exception:
        clean_sym = symbol

    # --- İç yardımcılar (dosyayı bozmamak için fonksiyon içi tanımlandı) ---
    PRICE_TOLERANCES = {
        # Yüzdelikler için makul abs tolerans (0.01 birim ~ 1bp)
        "CRYPTOCAP:USDT.D": {"abs": 0.01, "rel": 0.0},
        "CRYPTOCAP:BTC.D":  {"abs": 0.01, "rel": 0.0},
        # Market cap tarafında çok küçük göreli tolerans + 1 birim mutlak tampon
        "CRYPTOCAP:TOTAL":  {"abs": 1.0,  "rel": 1e-12},
        "CRYPTOCAP:TOTAL2": {"abs": 1.0,  "rel": 1e-12},
        "CRYPTOCAP:TOTAL3": {"abs": 1.0,  "rel": 1e-12},
    }
    DEFAULT_REL_TOL = 1e-9
    DEFAULT_ABS_TOL = 1e-8

    def _get_tolerances(sym: str):
        cfg = PRICE_TOLERANCES.get(sym, {})
        return float(cfg.get("rel", DEFAULT_REL_TOL)), float(cfg.get("abs", DEFAULT_ABS_TOL))

    def _prices_equivalent(new_p: float, last_p: float, sym: str) -> bool:
        rel, abs_ = _get_tolerances(sym)
        return math.isclose(float(new_p), float(last_p), rel_tol=rel, abs_tol=abs_)

    FORCE_SAVE_INTERVAL_SEC = 300  # 5 dk

    def _should_force_save(sym: str, now_dt, sym_last_ok: dict) -> bool:
        try:
            last_ok_epoch = sym_last_ok.get(sym)
            if last_ok_epoch is None:
                return True  # İlk kayıt
            try:
                now_epoch = int(now_dt.timestamp())
            except Exception:
                now_epoch = int(time.time())
            return (now_epoch - last_ok_epoch) >= FORCE_SAVE_INTERVAL_SEC
        except Exception:
            return True

    def _send_alert_json(event: str, **fields):
        """alerts_bot'a JSON metin olarak uyarı gönderir; başarısızlık durumunda sessiz geçer."""
        try:
            _bots = globals().get("bots", {})
            payload = {"event": event, "ts": int(time.time()), **fields}
            text = json.dumps(payload, ensure_ascii=False)
            send_telegram_message(text, "alerts_bot", _bots)  # parse-mode otomatik yönetiliyor
        except Exception:
            pass

    # ----------------------- USDT.D emniyet bandı ----------------------
    if clean_sym == "CRYPTOCAP:USDT.D":
        MIN_PRICE_LIMIT = 3.0
        MAX_PRICE_LIMIT = 8.0
        if live_price < MIN_PRICE_LIMIT or live_price > MAX_PRICE_LIMIT:
            logging.error(
                f"Live price for {clean_sym} is out of bounds: {live_price}. "
                f"Must be between {MIN_PRICE_LIMIT} and {MAX_PRICE_LIMIT}."
            )
            _send_alert_json(
                "out_of_bounds",
                symbol=clean_sym, price=float(live_price),
                min=MIN_PRICE_LIMIT, max=MAX_PRICE_LIMIT,
                note="ignored"
            )
            return  # Band dışı → kayıt yapılmaz (mevcut davranış KORUNDU)

    # --------------------- Son fiyatı toleranslı kontrol ---------------------
    try:
        cursor.execute(
            """
            SELECT live_price
            FROM global_live_data
            WHERE symbol = ?
            ORDER BY timestamp DESC
            LIMIT 1
            """,
            (clean_sym,),
        )
        row = cursor.fetchone()
        if row is not None:
            last_price = float(row[0])
            force_save = _should_force_save(clean_sym, now, globals().get("SYMBOL_LAST_OK_TS", {}))
            if _prices_equivalent(live_price, last_price, clean_sym) and not force_save:
                logging.info(f"No material change in live price for {clean_sym}. Skipping save.")
                return
    except Exception as e:
        # Son fiyat okuma başarısız olsa bile yazımı denemeye devam ederiz
        logging.debug(f"Last-price lookup failed for {clean_sym}: {e}")

    # ----------------------- changes alanlarını topla -----------------------
    ch_15m = changes.get("15M", changes.get("ch_15m"))
    ch_1h  = changes.get("1H",  changes.get("ch_1h"))
    ch_4h  = changes.get("4H",  changes.get("ch_4h"))
    ch_1d  = changes.get("1D",  changes.get("ch_1d"))

    # ----------------------- INSERT (ISO zaman damgası) ----------------------
    try:
        ts_text = to_sqlite_dt(now)  # 'YYYY-MM-DD HH:MM:SS' (UTC) – dosyada mevcut yardımcı
    except Exception:
        # Yedek: ISO8601'e düş
        ts_text = now.strftime("%Y-%m-%d %H:%M:%S")

    sql = """
        INSERT INTO global_live_data (
            timestamp, symbol, live_price, change_15M, change_1H, change_4H, change_1D
        ) VALUES (?, ?, ?, ?, ?, ?, ?)
    """
    params = (
        ts_text,
        clean_sym,
        float(live_price),
        None if ch_15m is None else float(ch_15m),
        None if ch_1h  is None else float(ch_1h),
        None if ch_4h  is None else float(ch_4h),
        None if ch_1d  is None else float(ch_1d),
    )

    try:
        cursor.execute(sql, params)
        conn.commit()
        logging.info(f"Live data saved for {clean_sym}: Price={live_price}, Changes={changes}")

        # --- Heartbeat sayaçlarını güncelle ---
        try:
            epoch_now = int(now.timestamp())
            global LAST_LIVE_INSERT_TS, SYMBOL_LAST_OK_TS
            LAST_LIVE_INSERT_TS = epoch_now
            if isinstance(SYMBOL_LAST_OK_TS, dict):
                SYMBOL_LAST_OK_TS[clean_sym] = epoch_now
        except Exception as _e:
            logging.debug(f"Heartbeat counters update skipped ({_e})")

        # (Opsiyonel) Kalıcı sağlık kaydı: global_health varsa güncelle
        try:
            cursor.execute(
                "UPDATE global_health SET last_global_live_utc=? WHERE id=1",
                (int(now.timestamp()),),
            )
            conn.commit()
        except Exception:
            # tablo yoksa veya yetki yoksa sessiz geç
            pass

    except sqlite3.IntegrityError as e:
        conn.rollback()
        logging.warning(f"Integrity error while saving live data for {clean_sym}: {e}")
        _send_alert_json("db_integrity_error", symbol=clean_sym, error=str(e))

    except sqlite3.OperationalError as e:
        conn.rollback()
        logging.error(f"Operational error while saving live data for {clean_sym}: {e}")
        _send_alert_json("db_operational_error", symbol=clean_sym, error=str(e))

    except Exception as e:
        conn.rollback()
        logging.error(f"Unexpected error saving live data for {clean_sym}: {e}")
        _send_alert_json("db_unexpected_error", symbol=clean_sym, error=str(e))

    finally:
        logging.debug(f"Save live data operation completed for {clean_sym}.")


def calculate_changes(
    cursor,
    symbol: str,
    live_price: float,
    now_dt: Optional[datetime] = None
) -> dict:
    """
    Yüzde değişimleri *kapanış referanslarına* göre hesaplar.
    - 15m/1h/4h: global_close_15m/_1h/_4h tablosundaki bitmiş son kovayı baz alır.
    - 1d: global_closing_data'daki son TAM gün kapanışını baz alır.
    - Referans yoksa global_live_data'dan yaklaşık geçmiş snapshot ile fallback yapar.

    Dönüş:
      Hem "15M/1H/4H/1D" anahtarları, hem de "ch_15m/ch_1h/ch_4h/ch_1d" anahtarları set edilir.
    """

    # Temizlik ve zaman
    try:
        sym = clean_symbol(symbol) if "clean_symbol" in globals() else symbol
    except Exception:
        sym = symbol

    if now_dt is None:
        now_dt = datetime.now(timezone.utc)

    out: dict[str, Optional[float]] = {
        "15M": None, "1H": None, "4H": None, "1D": None,
        "ch_15m": None, "ch_1h": None, "ch_4h": None, "ch_1d": None,
    }

    # tf -> (display_key, ch_key)
    tf_map = {
        "15m": ("15M", "ch_15m"),
        "1h":  ("1H",  "ch_1h"),
        "4h":  ("4H",  "ch_4h"),
        "1d":  ("1D",  "ch_1d"),
    }

    for tf, (disp_key, ch_key) in tf_map.items():
        # 1) Ana referansı (kapanış tablolarından) almaya çalış
        try:
            ref = get_reference_close(cursor, sym, tf, now_dt)  # 15m/1h/4h: global_close_XX, 1d: global_closing_data
        except Exception as e:
            logging.warning(f"[{sym}] reference close lookup error for {tf}: {e}", exc_info=False)
            ref = None

        # 2) Referans yoksa fallback: global_live_data’dan ~tf önceki snapshot
        if ref is None or (isinstance(ref, (int, float)) and ref <= 0):
            try:
                secs = TIMEFRAME_SECONDS[tf]
                target_dt = now_dt - timedelta(seconds=secs)
                target_ts_text = to_sqlite_dt(target_dt)  # 'YYYY-MM-DD HH:MM:SS' (UTC)

                # NOT: Şemanıza göre sabitler dosyanın başında tanımlı olmalı:
                # LIVE_TABLE_NAME = "global_live_data"
                # LIVE_PRICE_COL  = "live_price"
                # LIVE_TS_COL     = "timestamp"
                cursor.execute(f"""
                    SELECT {LIVE_PRICE_COL}
                    FROM {LIVE_TABLE_NAME}
                    WHERE symbol = ? AND {LIVE_TS_COL} <= ?
                    ORDER BY {LIVE_TS_COL} DESC
                    LIMIT 1
                """, (sym, target_ts_text))
                row = cursor.fetchone()
                if row and row[0] and float(row[0]) > 0:
                    ref = float(row[0])
            except Exception as e:
                logging.warning(f"[{sym}] fallback lookup error for {tf}: {e}", exc_info=False)
                ref = None

        # 3) Yüzdeyi hesapla
        try:
            if ref is not None and ref > 0:
                ch = ((live_price - ref) / ref) * 100.0
                out[disp_key] = round(ch, 2)
                out[ch_key] = out[disp_key]
            else:
                out[disp_key] = None
                out[ch_key] = None
        except Exception as e:
            logging.error(f"[{sym}] change calc error for {tf}: {e}", exc_info=False)
            out[disp_key] = None
            out[ch_key] = None

    return out



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




async def run_fetching_cycle(
    conn,
    cursor,
    global_symbols: list[str],
    bots: dict,
    delay_between_symbols: int = 2,    # DEPRECATED: Playwright ile kullanılmıyor
    cleanup_interval: int = 1800,      # DEPRECATED: Selenium zombi temizliği yok
    min_cycle_duration: int = 30,
    *,
    retries: int = 3,
    timeout_sec: int = 30,
    concurrency: int = 2,
    fetch_every_sec: Optional[int] = None,
    lock: Optional[asyncio.Lock] = None,
) -> None:
    """
    Paralel veri alımı ve veri işleme döngüsü (Playwright).

    Args:
        conn: Veritabanı bağlantısı (sqlite3.Connection).
        cursor: Veritabanı cursor (sqlite3.Cursor).
        global_symbols: İşlem yapılacak semboller listesi.
        bots: Telegram bot config dict (name -> {token, chat_id}).
        delay_between_symbols: (DEPRECATED) Semboller arası bekleme (kullanılmıyor).
        cleanup_interval: (DEPRECATED) Selenium zombi temizliği (kullanılmıyor).
        min_cycle_duration: Geriye uyumlu minimum döngü süresi (saniye).
    Keyword Args:
        retries: Her sembol için deneme sayısı (Playwright toplayıcı).
        timeout_sec: Sayfa/selektör timeout (saniye).
        concurrency: Aynı anda açılacak context/page sayısı.
        fetch_every_sec: Döngü aralığı; verilmezse min_cycle_duration kullanılır.
        lock: DB yazımı için opsiyonel asyncio.Lock.
    """
    logging.info("Fetching cycle (Playwright) started.")
    lock = lock or asyncio.Lock()
    last_action_time = datetime.now(timezone.utc)

    while True:
        cycle_start = time.monotonic()
        try:
            # --- Fiyatları çek (Playwright) ---
            fetched_prices = await fetch_multiple_prices_playwright(
                symbols=global_symbols,
                retries=retries,
                timeout_ms=timeout_sec * 1000,
                concurrency=concurrency,
            )

            now = datetime.now(timezone.utc)
            last_action_time = now

            # --- Kayıt akışı ---
            for symbol, live_price in fetched_prices.items():
                if live_price is not None and live_price > 0:
                    async with lock:
                        try:
                            changes = calculate_changes(cursor, symbol, live_price)
                            await save_live_data(cursor, conn, symbol, live_price, changes, now)
                            await save_closing_price(cursor, conn, symbol, live_price, now)
                            logging.info(f"Processed {symbol}: price={live_price}, changes={changes}")
                        except Exception as process_error:
                            logging.error(f"Error processing {symbol}: {process_error}")
                            try:
                                send_telegram_message(
                                    f"⚠️ Veri işleme hatası: {symbol}\nHata: {str(process_error)}",
                                    "alerts_bot",
                                    bots,
                                )
                            except Exception:
                                logging.warning("Telegram bildirimi gönderilemedi (process error).")
                else:
                    error_message = f"Failed to fetch live price for {symbol}."
                    logging.error(error_message)
                    try:
                        send_telegram_message(f"⚠️ {error_message}", "alerts_bot", bots)
                    except Exception:
                        logging.warning("Telegram bildirimi gönderilemedi (price missing).")

        except Exception as e:
            logging.error(f"Error in fetching cycle: {e}", exc_info=True)
            try:
                send_telegram_message(f"❌ Veri Kaydı Hatası: {e}", "alerts_bot", bots)
            except Exception:
                logging.warning("Telegram bildirimi gönderilemedi (cycle error).")

        # --- Döngü aralığı (fetch_every_sec tercih edilir, yoksa min_cycle_duration) ---
        interval = fetch_every_sec if fetch_every_sec is not None else min_cycle_duration
        elapsed = time.monotonic() - cycle_start
        sleep_duration = max(interval - elapsed, 0.0)
        logging.debug(f"Cycle completed in {elapsed:.2f}s. Sleeping for {sleep_duration:.2f}s.")

        # İnaktivite uyarısı (5 dk)
        if (datetime.now(timezone.utc) - last_action_time).total_seconds() > 300:
            warning_message = (
                f"⚠️ Uyarı: Bot 5 dakikadır işlem yapmıyor. "
                f"Son işlem zamanı (UTC): {last_action_time.isoformat()}"
            )
            logging.warning(warning_message)
            try:
                send_telegram_message(warning_message, "alerts_bot", bots)
            except Exception:
                logging.warning("Telegram bildirimi gönderilemedi (inactive warn).")

        await asyncio.sleep(sleep_duration)

async def main_trading(
    symbols: list[str],
    bots: dict,
    conn,
    cursor,
    fetch_cfg: dict,
) -> None:
    """
    Semboller için fiyatları çeker, limitleri kontrol eder ve DB'ye yazar.
    Playwright tabanlı toplayıcı kullanır ve 15m/1h/4h/1D kapanışlarını kalıcı tablolara işler.

    Args:
        symbols: İzlenecek semboller listesi (örn. ["CRYPTOCAP:USDT.D", ...]).
        bots: Telegram bot yapılandırmaları (name -> {token, chat_id}).
        conn: SQLite bağlantı nesnesi.
        cursor: SQLite cursor nesnesi.
        fetch_cfg: global_data_config.json içindeki "fetch" bölümü:
            - retries (int, varsayılan: 3)
            - timeout_sec (int, varsayılan: 30)  # wait_seconds ile geri uyumlu
            - fetch_every_sec (int, varsayılan: 60)
            - concurrency (int, varsayılan: 1)
    """
    # --- JSON → çalışma parametreleri ---
    retries         = int(fetch_cfg.get("retries", 3))
    timeout_sec     = int(fetch_cfg.get("timeout_sec", fetch_cfg.get("wait_seconds", 30)))
    fetch_every_sec = int(fetch_cfg.get("fetch_every_sec", 60))
    concurrency     = max(1, int(fetch_cfg.get("concurrency", 1)))

    # ✅ yeni eklenenler (isimler birebir JSON’la aynı)
    heartbeat_sec       = int(fetch_cfg.get("heartbeat_every_sec", 21600))  # 6 saat
    stale_live_max_sec  = int(fetch_cfg.get("stale_live_max_sec", 600))     # 10 dk
    stale_symbol_max_sec= int(fetch_cfg.get("stale_symbol_max_sec", 900))   # 15 dk

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
            f"  - Günlük Kapanış: {GLOBAL_CLOSING_TABLE}\n"
            f"  - Periyot Kapanışları: global_close_15m / _1h / _4h\n"
        )
        if bots:
            send_telegram_message(message, "main_bot", bots)
    except Exception:
        logging.warning("Başlatma mesajı gönderilemedi.", exc_info=True)

    # --- Sürekli döngü ---
    while True:
        try:
            # loop başında döngü zamanını güncelle
            now = datetime.now(timezone.utc)
            try:
                
                LAST_LOOP_TS = int(now.timestamp())
            except Exception:
                pass


            # 1) Fiyatları çek (Playwright)
            prices = await fetch_multiple_prices_playwright(
                symbols=symbols,
                retries=retries,
                timeout_ms=timeout_sec * 1000,
                concurrency=concurrency,
            )

            now = datetime.now(timezone.utc)

            # Heartbeat: son döngü zamanını güncelle (tanımlıysa)
            try:
                
                LAST_LOOP_TS = int(now.timestamp())
            except Exception:
                pass

            # 2) Her sembol için işlemleri yap
            for symbol, price in prices.items():
                if price is None:
                    logging.warning(f"[{symbol}] price fetch failed (None).")
                    continue

                # 2.1) Limit kontrolü (tanımlı olanlarda)
                try:
                    if symbol in LIMITS:
                        check_price_limits(symbol, price, bots)
                except Exception:
                    logging.exception(f"[{symbol}] limit kontrolü sırasında hata")

                # 2.2) Yüzde değişimleri (kapanış referanslarına göre)
                try:
                    # Yeni imza: calculate_changes(cursor, symbol, price, now)
                    changes = calculate_changes(cursor, symbol, price, now)
                except TypeError:
                    # Geri uyumluluk: eski imza (now parametresi yoksa)
                    changes = calculate_changes(cursor, symbol, price)
                except Exception:
                    logging.exception(f"[{symbol}] change hesaplama hatası")
                    changes = {
                        "15M": None, "1H": None, "4H": None, "1D": None,
                        "ch_15m": None, "ch_1h": None, "ch_4h": None, "ch_1d": None
                    }

                # 2.3) Canlı veriyi kaydet
                try:
                    await save_live_data(cursor, conn, symbol, price, changes, now)
                except Exception:
                    logging.exception(f"[{symbol}] save_live_data hatası")

                # 2.4) Periyot kapanışlarını UPSERT et (15m/1h/4h kovaları)
                #     - Kova bitene kadar close_price güncellenir
                #     - Kova bittiğinde aynı satır "kapanış" olur
                try:
                    save_period_close(cursor, conn, symbol, price, now, "15m")
                    save_period_close(cursor, conn, symbol, price, now, "1h")
                    save_period_close(cursor, conn, symbol, price, now, "4h")
                except AssertionError:
                    logging.error(f"[{symbol}] save_period_close yanlış timeframe parametresi")
                except Exception:
                    logging.exception(f"[{symbol}] save_period_close hatası")

                # 2.5) Günlük kapanışı işle (aynı gün için ikinci kez yazmaz)
                try:
                    await save_closing_price(cursor, conn, symbol, price, now)
                except Exception:
                    logging.exception(f"[{symbol}] save_closing_price hatası")

            # global ve sembol-bazlı “akış durdu mu?” kontrollerini yapar,
            #  6 saatte bir (veya config’ten ayarlanabilir) heartbeat mesajı yollar.
                        # 3) Döngü arası bekleme ÖNCESİ: Heartbeat & Stale kontrolleri
            try:
                now_epoch = int(now.timestamp())
                global LAST_HEARTBEAT_TS, LAST_LIVE_INSERT_TS, SYMBOL_LAST_OK_TS

                # Eşikler: fetch_cfg ile override edilebilir
                hb_every      = int(fetch_cfg.get("heartbeat_every_sec", 6 * 60 * 60))   # 6 saat
                stale_liveMax = int(fetch_cfg.get("stale_live_max_sec", 10 * 60))        # 10 dk
                stale_symMax  = int(fetch_cfg.get("stale_symbol_max_sec", 15 * 60))      # 15 dk

                # --- Global stale: son canlı insert çok eski mi? ---
                try:
                    if LAST_LIVE_INSERT_TS:
                        delta_live = now_epoch - int(LAST_LIVE_INSERT_TS)
                        if delta_live > stale_liveMax:
                            msg = (
                                f"⚠️ <b>Stale uyarısı</b>\n"
                                f"Son canlı kayıt {delta_live} sn önce atıldı.\n"
                                f"Beklenen max: {stale_liveMax} sn."
                            )
                            send_telegram_message(msg, "alerts_bot", bots)
                            logging.warning(msg)
                except Exception:
                    logging.exception("Global stale kontrolü sırasında hata")

                # --- Sembol-bazlı stale: hangi semboller gecikti? ---
                try:
                    late_syms = []
                    if isinstance(SYMBOL_LAST_OK_TS, dict):
                        for s in symbols:
                            last_ok = int(SYMBOL_LAST_OK_TS.get(s, 0) or 0)
                            if last_ok == 0:
                                # henüz hiç başarı kaydı yoksa ilk turda es geçebiliriz
                                continue
                            if (now_epoch - last_ok) > stale_symMax:
                                late_syms.append(s)
                    if late_syms:
                        msg = (
                            "⚠️ <b>Sembol-bazlı gecikme</b>\n"
                            f"Geciken: {', '.join(late_syms)}\n"
                            f"Eşik: {stale_symMax} sn"
                        )
                        send_telegram_message(msg, "alerts_bot", bots)
                        logging.warning(msg)
                except Exception:
                    logging.exception("Sembol-bazlı stale kontrolü sırasında hata")

                # --- Heartbeat (periyodik) ---
                try:
                    need_hb = (LAST_HEARTBEAT_TS is None) or ((now_epoch - int(LAST_HEARTBEAT_TS)) >= hb_every)
                    if need_hb:
                        ok_count = 0
                        if isinstance(SYMBOL_LAST_OK_TS, dict) and SYMBOL_LAST_OK_TS:
                            ok_count = sum(1 for s in symbols
                                           if int(SYMBOL_LAST_OK_TS.get(s, 0) or 0) >= (now_epoch - stale_symMax))
                        loop_age = (now_epoch - int(LAST_LOOP_TS or now_epoch))
                        live_age = (now_epoch - int(LAST_LIVE_INSERT_TS or now_epoch))

                        hb_msg = (
                            "💓 <b>Heartbeat</b>\n"
                            f"OK Sembol: {ok_count}/{len(symbols)}\n"
                            f"Son döngü: {loop_age} sn önce\n"
                            f"Son canlı insert: {live_age} sn önce\n"
                            f"DB: {DB_NAME}"
                        )
                        send_telegram_message(hb_msg, "main_bot", bots)
                        logging.info("Heartbeat gönderildi.")
                        LAST_HEARTBEAT_TS = now_epoch
                except Exception:
                    logging.exception("Heartbeat gönderimi sırasında hata")

            except Exception:
                logging.exception("Heartbeat & stale checks bloğu hata verdi")



            # 3) Döngü arası bekleme
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
    from pathlib import Path
    import asyncio

    bots: dict = {}
    conn = None
    cursor = None

    # 0) Log sistemini başlat
    LOG_PATH = os.path.join(LOG_DIR, "database_manager_5.log")
    logger = setup_logging(LOG_PATH)

    try:
        # 1) Global konfigürasyonu oku
        with open(GLOBAL_CFG, "r", encoding="utf-8") as f:
            cfg = json.load(f)
            fetch_cfg = cfg.get("fetch", {}) or {}

        # 2) Log seviyesi override (JSON'dan)
        level_name = (cfg.get("logging_level") or "INFO").upper()
        logger.setLevel(getattr(logging, level_name, logging.INFO))

        # 3) Paths override
        paths = cfg.get("paths") or {}
        db_override  = paths.get("db_path")
        log_override = paths.get("log_dir")
        telegram_cfg_override = paths.get("telegram_config")

        if db_override and db_override != DB_NAME:
            DB_NAME = db_override
            Path(Path(DB_NAME).parent).mkdir(parents=True, exist_ok=True)
            logger.info(f"DB_PATH override edildi: {DB_NAME}")

        if log_override and os.path.abspath(log_override) != os.path.abspath(LOG_DIR):
            Path(log_override).mkdir(parents=True, exist_ok=True)
            logger.info(f"LOG_DIR (JSON) tespit edildi: {log_override} (handler runtime'da değiştirilmedi)")

        if telegram_cfg_override and os.path.abspath(telegram_cfg_override) != os.path.abspath(TELEGRAM_CFG):
            TELEGRAM_CFG = telegram_cfg_override
            logger.info(f"TELEGRAM_CFG override edildi: {TELEGRAM_CFG}")

        # 4) Telegram yapılandırması
        try:
            bots = load_telegram_config(TELEGRAM_CFG)
            logger.info("Telegram configuration loaded successfully.")
            send_telegram_message("✅ alerts_bot ping", "alerts_bot", bots)
            send_telegram_message("✅ main_bot ping",   "main_bot",   bots)

        except FileNotFoundError:
            logger.warning(f"Telegram config bulunamadı, devam: {TELEGRAM_CFG}")
        except PermissionError:
            logger.error(f"Telegram config izin hatası, Telegram devre dışı: {TELEGRAM_CFG}")
        except Exception:
            logger.exception("Telegram config yüklenemedi, Telegram devre dışı")

        # 5) Veritabanı bağlantısı
        conn, cursor = connect_db()
        if not conn or not cursor:
            raise ConnectionError("Failed to connect to the database.")
        create_global_tables(cursor)
        conn.commit()

        # 6) Semboller
        global_symbols = cfg.get("symbols") or cfg.get("global_symbols") or []
        if not global_symbols:
            raise ValueError("No symbols found. Lütfen config dosyasını kontrol edin.")
        logger.info(f"Loaded global symbols: {global_symbols}")

        # 7) Price limits
        if "price_limits" in cfg and isinstance(cfg["price_limits"], dict):
            LIMITS.clear()
            LIMITS.update(cfg["price_limits"])

        # 8) Retention
        if "retention" in cfg and isinstance(cfg["retention"], dict):
            mapping = {
                "global_live": GLOBAL_LIVE_TABLE,
                "global_closing": GLOBAL_CLOSING_TABLE,
                GLOBAL_LIVE_TABLE: GLOBAL_LIVE_TABLE,
                GLOBAL_CLOSING_TABLE: GLOBAL_CLOSING_TABLE,
                "global_close_15m": "global_close_15m",
                "global_close_1h":  "global_close_1h",
                "global_close_4h":  "global_close_4h",
            }
            for key, limit in cfg["retention"].items():
                table = mapping.get(key)
                if table:
                    try:
                        RECORD_LIMITS[table] = int(limit)
                    except Exception:
                        logger.warning(f"Retention değeri sayıya çevrilemedi: {key} -> {limit}")
                else:
                    logger.warning(f"Bilinmeyen retention anahtarı: {key}")

        # 9) Ana işlem döngüsü
        asyncio.run(main_trading(global_symbols, bots, conn, cursor, fetch_cfg))

    except FileNotFoundError as e:
        logger.error(f"File not found: {e}", exc_info=True)
        if bots:
            try:
                send_telegram_message(f"❌ Configuration file error: {e}", "main_bot", bots)
            except Exception:
                logger.warning("Telegram bildirimi gönderilemedi (FileNotFoundError).")

    except ConnectionError as e:
        logger.error(f"Database connection error: {e}", exc_info=True)
        if bots:
            try:
                send_telegram_message(f"❌ Database connection error: {e}", "main_bot", bots)
            except Exception:
                logger.warning("Telegram bildirimi gönderilemedi (ConnectionError).")

    except ValueError as e:
        logger.error(f"Configuration error: {e}", exc_info=True)
        if bots:
            try:
                send_telegram_message(f"❌ Configuration error: {e}", "main_bot", bots)
            except Exception:
                logger.warning("Telegram bildirimi gönderilemedi (ValueError).")

    except Exception as e:
        logger.error(f"Unexpected fatal error: {e}", exc_info=True)
        if bots:
            try:
                send_telegram_message(f"❌ Fatal Error: {e}", "main_bot", bots)
            except Exception:
                logger.warning("Telegram bildirimi gönderilemedi (Fatal).")

 