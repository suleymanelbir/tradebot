📄 /opt/tradebot/tests/README.md
markdown
# 🧪 TradeBot Test Kılavuzu

Bu klasör, TradeBot projesine ait test dosyalarını içerir. Testler, sistemin doğru çalıştığını doğrulamak, API bağlantılarını kontrol etmek ve hata ayıklamayı kolaylaştırmak için kullanılır.

---

## 📁 Klasör Yapısı

tests/ ├── init.py ├── test_binance_connection.py ├── test_order_router.py ├── ...

Kod

Her test dosyası `test_*.py` formatında adlandırılır ve `pytest` ile çalıştırılabilir.

---

## 🚀 Testleri Çalıştırma

### 🔹 Tüm testleri çalıştırmak için:
```bash
make test
Bu komut, tüm test dosyalarını otomatik olarak çalıştırır.

🔹 Belirli bir test dosyasını çalıştırmak için:
bash
make test-file file=test_binance_connection.py
🔹 Belirli bir test fonksiyonunu çalıştırmak için:
bash
make test-func file=test_binance_connection.py func=test_binance_connection
Not: Bu komutlar Makefile içinde tanımlıdır. Proje kök dizininde çalıştırılmalıdır.

🧠 Test Yazım Kuralları
Dosya adı: test_*.py

Fonksiyon adı: test_*

Async testler için pytest-asyncio kullanılır:

python
import pytest

@pytest.mark.asyncio
async def test_something():
    ...
🔧 Ortam Gereksinimleri
Testlerin çalışabilmesi için aşağıdaki paketlerin kurulu olması gerekir:

bash
pip install pytest pytest-asyncio
Ayrıca proje kök dizini PYTHONPATH olarak tanımlanmalıdır. Makefile bunu otomatik yapar.

📌 Ek Bilgiler
__init__.py dosyası, tests klasörünü Python modülü haline getirir.

Testler, future_trade modülünü kullanır. Bu nedenle proje kökünden çalıştırılmalıdır.

🛠️ Yardımcı Komutlar
Komut	Açıklama
make test	Tüm testleri çalıştırır
make test-file file=...	Belirli dosyayı çalıştırır
make test-func file=... func=...	Belirli fonksiyonu çalıştırır
Her yeni test dosyasını bu klasöre ekleyebilir, make test ile doğrulayabilirsin. Test altyapısı otomatik olarak tüm dosyaları tanır ve çalıştırır.

Test loglarını ve önbelleği temizle: make clean
