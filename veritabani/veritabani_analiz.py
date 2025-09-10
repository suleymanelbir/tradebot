import sqlite3
import os
import json
import csv
from datetime import datetime, timedelta
import statistics

# 🔹 Yardımcı Fonksiyonlar
def get_db_metadata(db_path):
    return {
        "dosya_yolu": db_path,
        "boyut_mb": round(os.path.getsize(db_path) / (1024 * 1024), 2),
        "son_guncelleme": datetime.fromtimestamp(os.path.getmtime(db_path)).isoformat()
    }

def get_tables(cursor):
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
    return [row[0] for row in cursor.fetchall()]

def get_columns(cursor, table):
    cursor.execute(f"PRAGMA table_info({table})")
    return [{
        "ad": col[1],
        "tip": col[2],
        "primary_key": bool(col[5]),
        "not_null": bool(col[3]),
        "default": col[4]
    } for col in cursor.fetchall()]
def get_last_price(cursor, table, symbol):
    try:
        cursor.execute(f"""
            SELECT close FROM {table}
            WHERE symbol = ?
            ORDER BY timestamp DESC LIMIT 1
        """, (symbol,))
        result = cursor.fetchone()
        return float(result[0]) if result else None
    except:
        return None
    
def get_last_timestamp(cursor: sqlite3.Cursor, table: str, symbol: str) -> str | None:
    """
    Belirtilen tablo ve sembol için en son timestamp değerini döndürür.

    Args:
        cursor (sqlite3.Cursor): Veritabanı imleci.
        table (str): Tablo adı.
        symbol (str): Sembol (örneğin 'CRYPTOCAP:USDT.D').

    Returns:
        str | None: En son timestamp değeri veya None.
    """
    try:
        query = f"""
            SELECT timestamp FROM {table}
            WHERE symbol = ?
            ORDER BY timestamp DESC
            LIMIT 1
        """
        cursor.execute(query, (symbol,))
        result = cursor.fetchone()
        return result[0] if result else None
    except sqlite3.Error as e:
        print(f"⚠️ get_last_timestamp hata: {e}")
        return None



def analyze_write_attempt(symbol, live_price, table="global_close_4h"):
    result = {
        "tablo": table,
        "sembol": symbol,
        "veri_yazimi": "başarılı",
        "neden": None,
        "son_fiyat": live_price,
        "beklenen_aralık": None,
        "son_kayıt_zamanı": None,
        "n8n_trigger": {
            "alert_level": "normal",
            "action_required": False,
            "change_detected": True
        }
    }

    # 🔍 Fiyat aralık kontrolü (USDT.D için özel durum)
    if symbol == "CRYPTOCAP:USDT.D":
        result["beklenen_aralık"] = "3.0–8.0"
        if live_price < 3.0 or live_price > 8.0:
            result["veri_yazimi"] = "başarısız"
            result["neden"] = "price out of bounds"
            result["n8n_trigger"]["alert_level"] = "critical"
            result["n8n_trigger"]["action_required"] = True
            result["n8n_trigger"]["change_detected"] = False

    # 🔍 Fiyat geçersizse
    if live_price is None or live_price <= 0:
        result["veri_yazimi"] = "başarısız"
        result["neden"] = "price missing or invalid"
        result["n8n_trigger"]["alert_level"] = "critical"
        result["n8n_trigger"]["action_required"] = True
        result["n8n_trigger"]["change_detected"] = False

    # 🔍 Aynı fiyat varsa yazım atlanabilir
    try:
        conn = sqlite3.connect("/opt/tradebot/veritabani/global_data.db")
        cursor = conn.cursor()
        last_price = get_last_price(cursor, table, symbol)
        last_ts = get_last_timestamp(cursor, table, symbol)
        result["son_kayıt_zamanı"] = last_ts

        if last_price == live_price:
            result["veri_yazimi"] = "atlandı"
            result["neden"] = "no price change"
            result["n8n_trigger"]["alert_level"] = "warning"
            result["n8n_trigger"]["action_required"] = False
            result["n8n_trigger"]["change_detected"] = False

        conn.close()
    except Exception as e:
        result["veri_yazimi"] = "başarısız"
        result["neden"] = f"veritabanı hatası: {str(e)}"
        result["n8n_trigger"]["alert_level"] = "critical"
        result["n8n_trigger"]["action_required"] = True

    return result


def deep_table_analysis(cursor: sqlite3.Cursor, table: str) -> dict:
    """
    Belirtilen tablo için yapısal ve içeriksel analiz yapar.
    10 temel başlıkta tabloyu değerlendirir.
    """
    try:
        # 1. Sütun bilgileri
        cursor.execute(f"PRAGMA table_info({table})")
        columns_raw = cursor.fetchall()
        columns = [{
            "ad": col[1],
            "tip": col[2],
            "primary_key": bool(col[5]),
            "not_null": bool(col[3]),
            "default": col[4]
        } for col in columns_raw]
        column_names = [col["ad"] for col in columns]

        # 2. Kayıt sayısı
        cursor.execute(f"SELECT COUNT(*) FROM {table}")
        record_count = cursor.fetchone()[0]

        # 3. Zaman sütunu var mı?
        timestamp_column = next((c for c in column_names if "timestamp" in c.lower()), None)

        # 4. Veri sütunu var mı?
        value_column = next((c for c in ["close", "price", "live_price", "value"] if c in column_names), None)

        # 5. Sembol sütunu var mı?
        symbol_column = next((c for c in column_names if "symbol" in c.lower()), None)

        # 6. İlk ve son kayıt zamanı
        first_timestamp = last_timestamp = None
        if timestamp_column:
            cursor.execute(f"SELECT MIN({timestamp_column}), MAX({timestamp_column}) FROM {table}")
            result = cursor.fetchone()
            first_timestamp, last_timestamp = result if result else (None, None)

        # 7. Son kayıt edilen veri
        last_value = None
        if timestamp_column and value_column:
            cursor.execute(f"""
                SELECT {value_column}
                FROM {table}
                ORDER BY {timestamp_column} DESC
                LIMIT 1
            """)
            result = cursor.fetchone()
            if result:
                last_value = result[0]

        # 8. Veri kalitesi: NULL sayısı
        null_counts = {}
        for col in column_names:
            cursor.execute(f"SELECT COUNT(*) FROM {table} WHERE {col} IS NULL")
            null_counts[col] = cursor.fetchone()[0]

        # 9. Veri tipi tutarlılığı (örnek veri)
        sample_data = {}
        cursor.execute(f"SELECT * FROM {table} LIMIT 1")
        row = cursor.fetchone()
        if row:
            sample_data = dict(zip(column_names, row))

        # 10. İlişkili tablo tahmini (symbol üzerinden)
        related_tables = []
        if symbol_column:
            cursor.execute(f"SELECT DISTINCT {symbol_column} FROM {table} LIMIT 5")
            symbols = [row[0] for row in cursor.fetchall()]
            related_tables = symbols

        return {
            "tablo": table,
            "sütunlar": columns,
            "kayıt_sayısı": record_count,
            "zaman_sütunu": timestamp_column,
            "veri_sütunu": value_column,
            "sembol_sütunu": symbol_column,
            "ilk_kayıt_zamanı": first_timestamp,
            "son_kayıt_zamanı": last_timestamp,
            "son_kayıt_değeri": last_value,
            "null_sayısı": null_counts,
            "örnek_kayıt": sample_data,
            "ilişkili_varlıklar": related_tables
        }

    except Exception as e:
        print(f"⚠️ deep_table_analysis hata: {e}")
        return {"tablo": table, "hata": str(e)}


def get_recent_data(cursor, table, days=30):
    since = datetime.now() - timedelta(days=days)
    # Veri sütunu adayları
    candidate_columns = ["close", "price", "live_price", "value"]
    
    # Tablo sütunlarını al
    cursor.execute(f"PRAGMA table_info({table})")
    columns = [col[1] for col in cursor.fetchall()]
    
    # Uygun veri sütununu bul
    value_column = next((c for c in candidate_columns if c in columns), None)
    if not value_column or "timestamp" not in columns:
        return []  # Analiz için uygun değil

    try:
        cursor.execute(f"""
            SELECT timestamp, {value_column}
            FROM {table}
            WHERE timestamp >= ?
            ORDER BY timestamp ASC
        """, (since.isoformat(),))
        return cursor.fetchall()
    except:
        return []




# 🔹 Korelasyon Analizi
def compute_correlation(data1, data2):
    try:
        values1 = [row[1] for row in data1]
        values2 = [row[1] for row in data2]
        min_len = min(len(values1), len(values2))
        if min_len < 2:
            return None
        values1 = values1[-min_len:]
        values2 = values2[-min_len:]
        mean1 = statistics.mean(values1)
        mean2 = statistics.mean(values2)
        cov = sum((x - mean1) * (y - mean2) for x, y in zip(values1, values2)) / min_len
        std1 = statistics.stdev(values1)
        std2 = statistics.stdev(values2)
        return round(cov / (std1 * std2), 3)
    except:
        return None

# 🔹 Momentum ve RSI benzeri gösterge
def compute_momentum(values):
    """
    Basit momentum göstergesi: son değer - ilk değer
    """
    if not values or len(values) < 2:
        return None
    try:
        values = [float(v) for v in values if v is not None]
        return round(values[-1] - values[0], 3)
    except Exception as e:
        return None


def compute_rsi(values, period=14):
    """
    RSI (Relative Strength Index) hesaplayıcı
    """
    if not values or len(values) < period + 1:
        return None
    try:
        values = [float(v) for v in values if v is not None]
        deltas = [values[i+1] - values[i] for i in range(len(values)-1)]
        gains = [delta if delta > 0 else 0 for delta in deltas]
        losses = [-delta if delta < 0 else 0 for delta in deltas]

        avg_gain = statistics.mean(gains[-period:])
        avg_loss = statistics.mean(losses[-period:])

        if avg_loss == 0:
            return 100.0  # Aşırı alım durumu

        rs = avg_gain / avg_loss
        rsi = 100 - (100 / (1 + rs))
        return round(rsi, 2)
    except Exception as e:
        return None


# 🔹 Sinyal Üretimi
def generate_signal(momentum, rsi):
    if momentum is None or rsi is None:
        return "belirsiz"
    if momentum > 0 and rsi < 70:
        return "long"
    elif momentum < 0 and rsi > 30:
        return "short"
    else:
        return "bekle"

# 🔹 Anomali Tespiti
def detect_anomalies(values):
    anomalies = []
    if not values or len(values) < 2:
        return anomalies  # boş liste döner
    for i in range(1, len(values)):
        change = abs(values[i] - values[i-1])
        if change > (statistics.mean(values) * 0.1):
            anomalies.append({
                "index": i,
                "deger": values[i],
                "degisim": round(change, 3)
            })
    return anomalies

def analyze_table_structure(cursor: sqlite3.Cursor, table: str) -> dict:
    """
    Belirtilen tablo için yapısal analiz yapar:
    - Sütun bilgileri
    - Kayıt sayısı
    - Veri ve zaman sütunlarının varlığı
    - En son kayıt zamanı ve değeri

    Args:
        cursor (sqlite3.Cursor): Veritabanı imleci
        table (str): Tablo adı

    Returns:
        dict: Yapısal analiz sonuçları
    """
    try:
        # 🔍 Sütun bilgileri
        cursor.execute(f"PRAGMA table_info({table})")
        columns_raw = cursor.fetchall()
        columns = [{
            "ad": col[1],
            "tip": col[2],
            "primary_key": bool(col[5]),
            "not_null": bool(col[3]),
            "default": col[4]
        } for col in columns_raw]

        # 🔢 Kayıt sayısı
        cursor.execute(f"SELECT COUNT(*) FROM {table}")
        record_count = cursor.fetchone()[0]

        # 🔎 Veri ve zaman sütunları
        column_names = [col["ad"] for col in columns]
        value_column = next((c for c in ["close", "price", "live_price", "value"] if c in column_names), None)
        timestamp_column = "timestamp" if "timestamp" in column_names else None

        # 🕒 En son kayıt zamanı ve veri
        last_timestamp = None
        last_value = None
        if value_column and timestamp_column:
            cursor.execute(f"""
                SELECT {timestamp_column}, {value_column}
                FROM {table}
                ORDER BY {timestamp_column} DESC
                LIMIT 1
            """)
            result = cursor.fetchone()
            if result:
                last_timestamp, last_value = result

        return {
            "tablo": table,
            "sütunlar": columns,
            "kayıt_sayısı": record_count,
            "zaman_sütunu": timestamp_column is not None,
            "veri_sütunu": value_column if value_column else None,
            "son_kayıt_zamanı": last_timestamp,
            "son_kayıt_değeri": last_value
        }

    except sqlite3.Error as e:
        print(f"⚠️ analyze_table_structure hata: {e}")
        return {
            "tablo": table,
            "hata": str(e)
        }

def analyze_table(cursor: sqlite3.Cursor, table: str) -> dict:
    """
    Belirtilen tablo için son 30 günlük veri üzerinden analiz yapar:
    - Momentum
    - RSI
    - Sinyal üretimi
    - Anomali tespiti

    Args:
        cursor (sqlite3.Cursor): Veritabanı imleci
        table (str): Tablo adı

    Returns:
        dict: Analiz sonuçları
    """
    try:
        data = get_recent_data(cursor, table, days=30)
        if not data or len(data[0]) < 2:
            return {
                "tablo": table,
                "uyarı": "analiz için uygun veri sütunu yok",
                "momentum": None,
                "rsi": None,
                "sinyal": "belirsiz",
                "anomaliler": [],
                "son_kayit_sayisi": 0,
                "son_kayit_zamani": None,
                "n8n_trigger": {
                    "alert_level": "normal",
                    "action_required": False,
                    "change_detected": False
                }
            }

        timestamps = [row[0] for row in data]
        closes = [row[1] for row in data if row[1] is not None]

        momentum = compute_momentum(closes)
        rsi = compute_rsi(closes)
        signal = generate_signal(momentum, rsi)
        anomalies = detect_anomalies(closes)

        return {
            "tablo": table,
            "son_kayit_sayisi": len(data),
            "momentum": momentum,
            "rsi": rsi,
            "sinyal": signal,
            "anomaliler": anomalies,
            "son_kayit_zamani": timestamps[-1] if timestamps else None,
            "n8n_trigger": {
                "alert_level": "critical" if signal != "bekle" else "normal",
                "action_required": signal != "bekle",
                "change_detected": len(anomalies) > 0
            }
        }

    except Exception as e:
        print(f"⚠️ analyze_table hata: {e}")
        return {
            "tablo": table,
            "uyarı": f"analiz hatası: {str(e)}",
            "momentum": None,
            "rsi": None,
            "sinyal": "belirsiz",
            "anomaliler": [],
            "son_kayit_sayisi": 0,
            "son_kayit_zamani": None,
            "n8n_trigger": {
                "alert_level": "critical",
                "action_required": True,
                "change_detected": False
            }
        }


# 🔹 JSON → CSV Dönüştürme
def export_to_csv(json_data, filename="analiz_raporu.csv"):
    with open(filename, "w", newline='', encoding="utf-8") as csvfile:
        fieldnames = ["tablo", "momentum", "rsi", "sinyal", "alert_level", "action_required", "change_detected"]
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()

        for table, data in json_data["tablolar"].items():
            # Eğer analiz verileri eksikse, boş değerlerle yaz
            momentum = data.get("momentum", "")
            rsi = data.get("rsi", "")
            sinyal = data.get("sinyal", "")
            trigger = data.get("n8n_trigger", {})

            writer.writerow({
                "tablo": table,
                "momentum": momentum,
                "rsi": rsi,
                "sinyal": sinyal,
                "alert_level": trigger.get("alert_level", ""),
                "action_required": trigger.get("action_required", ""),
                "change_detected": trigger.get("change_detected", "")
            })


# 🔹 Ana Fonksiyon
def analyze_table_structure(cursor, table):
    cursor.execute(f"PRAGMA table_info({table})")
    columns_raw = cursor.fetchall()
    columns = [{
        "ad": col[1],
        "tip": col[2],
        "primary_key": bool(col[5]),
        "not_null": bool(col[3]),
        "default": col[4]
    } for col in columns_raw]

    cursor.execute(f"SELECT COUNT(*) FROM {table}")
    record_count = cursor.fetchone()[0]

    column_names = [col["ad"] for col in columns]
    value_column = next((c for c in ["close", "price", "live_price", "value"] if c in column_names), None)
    timestamp_column = "timestamp" if "timestamp" in column_names else None

    last_timestamp = None
    last_value = None
    if value_column and timestamp_column:
        cursor.execute(f"""
            SELECT {timestamp_column}, {value_column}
            FROM {table}
            ORDER BY {timestamp_column} DESC
            LIMIT 1
        """)
        result = cursor.fetchone()
        if result:
            last_timestamp, last_value = result

    return {
        "tablo": table,
        "sütunlar": columns,
        "kayıt_sayısı": record_count,
        "zaman_sütunu": timestamp_column is not None,
        "veri_sütunu": value_column if value_column else None,
        "son_kayıt_zamanı": last_timestamp,
        "son_kayıt_değeri": last_value
    }
def main():
    db_path = input("📁 Veritabanı dosya yolunu girin: ").strip()
    if not os.path.exists(db_path):
        print("❌ Dosya bulunamadı.")
        return

    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    rapor = {
        "veritabani": get_db_metadata(db_path),
        "tablolar": {}
    }

    tables = get_tables(cursor)

    # Korelasyon analizi (isteğe bağlı)
    if "BTC_D" in tables and "Total3" in tables:
        btc_data = get_recent_data(cursor, "BTC_D", days=30)
        total_data = get_recent_data(cursor, "Total3", days=30)
        korelasyon = compute_correlation(btc_data, total_data)
        rapor["korelasyon"] = {"BTC_D_vs_Total3": korelasyon}

    for table in tables:
        # 🔍 Önce yapısal ve içeriksel analiz (10 başlık)
        detayli_analiz = deep_table_analysis(cursor, table)

        # ⛔ Veri sütunu veya zaman sütunu yoksa istatistiksel analiz atlanır
        if detayli_analiz.get("veri_sütunu") and detayli_analiz.get("zaman_sütunu"):
            istatistiksel_analiz = analyze_table(cursor, table)
        else:
            istatistiksel_analiz = {
                "uyarı": "İstatistiksel analiz için uygun veri/zaman sütunu bulunamadı.",
                "momentum": None,
                "rsi": None,
                "sinyal": "belirsiz",
                "anomaliler": [],
                "son_kayit_sayisi": 0,
                "son_kayit_zamani": None,
                "n8n_trigger": {
                    "alert_level": "normal",
                    "action_required": False,
                    "change_detected": False
                }
            }

        # 📦 Raporu tabloya ekle
        rapor["tablolar"][table] = {
            "yapı": detayli_analiz,
            "istatistik": istatistiksel_analiz
        }

    conn.close()

    # 📄 JSON çıktısı
    json_file = os.path.basename(db_path).replace(".db", "") + "_dna_raporu.json"
    with open(json_file, "w", encoding="utf-8") as f:
        json.dump(rapor, f, indent=2, ensure_ascii=False)

    # 📊 CSV çıktısı
    export_to_csv(rapor)

    print(f"✅ JSON ve CSV analiz tamamlandı.\n📄 JSON: {json_file}\n📄 CSV: analiz_raporu.csv")

if __name__ == "__main__":
    main()
