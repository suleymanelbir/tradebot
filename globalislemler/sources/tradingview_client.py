
import logging
from typing import Optional
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

from ..core.validator import clean_price_text
from .selenium_pool import SeleniumPool

logger = logging.getLogger(__name__)

class TradingViewClient:
    def __init__(self, pool: SeleniumPool, wait_seconds: int=25):
        self.pool = pool
        self.wait_seconds = wait_seconds

    def _url(self, symbol: str) -> str:
        return f"https://www.tradingview.com/symbols/{symbol.replace(':','-')}/"

    def get_live_price(self, symbol: str, retries: int=3) -> Optional[float]:
        for attempt in range(retries):
            driver = None
            try:
                driver = self.pool.acquire()
                driver.get(self._url(symbol))
                el = WebDriverWait(driver, self.wait_seconds).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, ".js-symbol-last"))
                )
                price = clean_price_text(el.text)
                if price is not None:
                    return price
                logger.warning("Price parse returned None, retrying", extra={"symbol": symbol, "attempt": attempt+1})
            except Exception as e:
                logger.warning("Live price fetch failed", extra={"symbol": symbol, "attempt": attempt+1})
            finally:
                if driver:
                    self.pool.release(driver)
        return None
