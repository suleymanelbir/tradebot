# Python 3.11 slim imajı
FROM python:3.11-slim

# Çalışma dizini
WORKDIR /app

# Gereksinimleri yükle
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Uygulama dosyalarını kopyala
COPY . .

# Ortam değişkeni örneği
ENV FLASK_ENV=production

# Uygulamayı başlat
CMD ["python3", "app.py"]
