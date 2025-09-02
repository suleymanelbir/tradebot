from flask import Flask
import requests

app = Flask(__name__)

@app.route('/')
def home():
    return "✅ Flask çalışıyor!"

@app.route('/test-request')
def test_request():
    response = requests.get("https://api.github.com")
    return {"status_code": response.status_code, "url": response.url}

if __name__ == '__main__':
    app.run(host="0.0.0.0",port=3000)

