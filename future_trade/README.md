# Futures Trade Bot (ONE-WAY per Symbol)


Bu proje, **farklı sembollerde eşzamanlı** long/short pozisyon açabilen; aynı sembolde ise ONE-WAY (ya long ya short) kuralıyla çalışan **USDT-M Binance Futures** botunun modüler bir iskeletidir.


## Özellikler
- **ONE-WAY per symbol**, çoklu sembolde eşzamanlı işlem
- **Whitelist** ile sembol kontrolü
- **Isolated/Cross** margin seçimi (config)
- **Kapasite/kota**: global/simetrik/ sembol başı sınırlar
- **Strateji plug-in**: `dominance_trend` (EMA/RSI/ADX + dominance)
- **Trailing stop**: percent/ATR/chandelier
- **Risk & boyutlandırma**: per-trade risk, günlük max loss, kill-switch
- **Emir yönlendirme**: entry + SL/TP, reduceOnly koruma
- **Reconcile**: borsa ↔ DB durum eşitleme
- **Telegram**: iki bot ayrımı (alerts/trades), JSON mesaj
- **n8n**: olay gönderme + komut alma iskeleti
- **SQLite** kalıcılık: klines, orders, positions, trades, state, risk_journal, signal_audit


## Dizin Yapısı

future_trade/ app.py # giriş, async orkestrasyon config.json # tek merkez konfig binance_client.py # borsa istemcisi market_stream.py # veri akışı strategy/ # stratejiler risk_manager.py # risk ve boyutlandırma trailing_stop.py # iz-süren stop order_router.py # emir yönlendirme order_reconciler.py # reconcile position_supervisor.py # kotalar & kabul/ret portfolio.py # portföy/PnL persistence.py # DB şeması ve kayıt telegram_notifier.py # bildirimler n8n_bridge.py # entegrasyon

## Kurulum
1. Python 3.10+
2. Kütüphaneler: `httpx`
3. `config.json` değerlerini doldurun (API anahtarları, DB yolu, whitelist, telegram vb.)
4. İlk çalıştırma: `python app.py`


## Akış
1. `market_stream` 1H/4H kapanış barlarını yayımlar (endeksler + whitelist).
2. `strategy` kapanışta sinyal üretir.
3. `position_supervisor` kapasite/whitelist/kotalar ile kabul/ret yapar.
4. `risk_manager` boyut ve SL/TP planını çıkarır.
5. `order_router` entry + koruma emirlerini gönderir; `trailing_stop` düzenli günceller.
6. `order_reconciler` emir/pozisyonu borsa ile uzlaştırır.
7. `persistence` tüm olayları DB’ye yazar; `telegram_notifier` ve `n8n_bridge` bildirim/entegrasyon sağlar.


## Notlar
- Bu bir **iskelet**tir. WS akışı, gerçek indikatör hesapları (EMA/RSI/ADX), exchangeInfo filtreleri (tick/step/minNotional) ve tam reconcile mantığı **TODO** olarak işaretlenmiştir.
- Adım adım ilerleyip her modülü güvenli şekilde dolduracağız: *data → strateji → risk → emir → trailing → reconcile → n8n*.

İskeleti tuvalde (canvas) oluşturdum 🎯 — içinde:

modül modül dosyalar,

örnek config.json,

her dosyanın ne yaptığını anlatan kapsamlı README,

ve çalıştırılabilir seviyede kod iskeletleri (TODO’larla).

İzlemek istediğin yol şu:

config.json’u kendi anahtarların ve whitelist’inle doldur.

persistence.init_schema() DB’yi oluşturacak (yol: /opt/tradebot/veritabani/futures_data.db).

python /opt/tradebot/future_trade/app.py ile başlat; ilk sürüm placeholder veri akışıyla ayağa kalkar (WS/indikatorler TODO).

Sonraki iterasyonda birlikte şu parçaları dolduralım:

market_stream: gerçek 1H/4H klines ve endeksler (TOTAL3/USDT.D/BTC.D),

dominance_trend: EMA/RSI/ADX hesapları,

order_router: gerçek entry + SL/TP + trailing güncellemeleri,

position_supervisor: kapasite/kota/soğuma mantığını tamamlamak,

order_reconciler: Binance’le tam senkron,
