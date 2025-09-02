
# Trade_Boyu_İçin_Global_Veriler — Global Data Ingestor (24/7)

Bu servis **yalnızca global sembollerin** (örn. `CRYPTOCAP:USDT.D`, `CRYPTOCAP:BTC.D`) verilerini toplar ve
`15m`, `1h`, `4h`, `1d` kapanışlarına hizalı biçimde **veritabanına yazar**. Trade açma/kapama bu paketin işi değildir
— ayrı bir trade botu bu veriyi okur.

## Dizin Yapısı (Önerilen)
```
/opt/tradebot
├─ log/                      # JSON ve düz loglar (rotation)
├─ veritabanı/               # SQLite (WAL)
└─ globalislemler/
   ├─ database_manager_5.py  # Servis ana dosyası
   ├─ core/
   │  ├─ logging_config.py   # Log kurulum
   │  ├─ db.py               # SQLite + UPSERT + WAL + run kayıtları
   │  ├─ utils.py            # Zaman yardımcıları
   │  ├─ validator.py        # Fiyat temizleme/limit kontrolü
   │  ├─ aggregator.py       # % değişim hesabı
   │  └─ notify.py           # Telegram gönderici
   ├─ sources/
   │  ├─ selenium_pool.py    # WebDriver havuzu
   │  └─ tradingview_client.py # TradingView canlı fiyat (Selenium)
   └─ config/
      ├─ global_data_config.json  # Merkez kumanda
      └─ telegram_bots.json       # Telegram bot tanımları
```

## Hızlı Kurulum
1) ZIP'i açıp `/opt/tradebot` altına kopyalayın.
2) `globalislemler/config/global_data_config.json` içindeki yolları kendi dizinlerinize göre düzenleyin.
3) `python3 globalislemler/database_manager_5.py` ile çalıştırın (veya systemd).
