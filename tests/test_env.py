import os
import pytest

def load_api_keys():
    api_key = os.getenv("API_KEY")
    api_secret = os.getenv("API_SECRET")

    if not api_key or not api_secret:
        return None, None

    return api_key, api_secret

def test_env_api_keys():
    api_key, api_secret = load_api_keys()

    if not api_key or not api_secret:
        print("❌ Ortam değişkenlerinde API_KEY veya API_SECRET tanımlı değil.")
        assert False, "API ortam değişkenleri eksik"
    else:
        print("✅ API ortam değişkenleri tanımlı.")
        print(f"🔑 API_KEY: {api_key}")
        # Secret gösterilmez, güvenlik için
        assert True
