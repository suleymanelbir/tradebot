
import queue
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
import logging

logger = logging.getLogger(__name__)

class SeleniumPool:
    def __init__(self, size: int=1, chromedriver_path: str="/usr/bin/chromedriver"):
        self.size = size
        self.path = chromedriver_path
        self.pool = queue.Queue(maxsize=size)
        self._make_drivers()

    def _make_options(self) -> Options:
        opts = Options()
        opts.add_argument("--headless=new")
        opts.add_argument("--no-sandbox")
        opts.add_argument("--disable-dev-shm-usage")
        opts.add_argument("--disable-gpu")
        opts.add_argument("--disable-extensions")
        opts.add_argument("--disable-application-cache")
        opts.add_argument("--disable-blink-features=AutomationControlled")
        opts.add_argument("--log-level=3")
        return opts

    def _make_drivers(self):
        for _ in range(self.size):
            try:
                service = Service(self.path)
                driver = webdriver.Chrome(service=service, options=self._make_options())
                self.pool.put(driver)
            except Exception as e:
                logger.exception("Failed to create Chrome driver")

    def acquire(self, timeout: float=30.0):
        return self.pool.get(timeout=timeout)

    def release(self, driver):
        try:
            self.pool.put_nowait(driver)
        except queue.Full:
            try:
                driver.quit()
            except Exception:
                pass

    def shutdown(self):
        while not self.pool.empty():
            d = self.pool.get_nowait()
            try:
                d.quit()
            except Exception:
                pass
