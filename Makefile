# Makefile - Test komutları

# Makefile - TradeBot Test Komutları

.PHONY: test test-file test-func clean

# Tüm test dosyalarını çalıştırır (tests/ klasöründeki tüm test_*.py dosyaları)
test:
	PYTHONPATH=/opt/tradebot pytest /opt/tradebot/tests -v

# Belirli bir test dosyasını çalıştırmak için:
# make test-file file=test_binance_connection.py
test-file:
	PYTHONPATH=/opt/tradebot pytest /opt/tradebot/tests/$(file) -v

# Belirli bir test fonksiyonunu çalıştırmak için:
# make test-func file=test_binance_connection.py func=test_binance_connection
test-func:
	PYTHONPATH=/opt/tradebot pytest /opt/tradebot/tests/$(file)::$(func) -v

# Test loglarını ve önbelleği temizler
clean:
	rm -f /opt/tradebot/tests/report.json
	find /opt/tradebot/tests -name "*.pyc" -delete
	find /opt/tradebot/tests -name "__pycache__" -type d -exec rm -r {} +

