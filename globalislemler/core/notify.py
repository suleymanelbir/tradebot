
import logging
import time
import requests

logger = logging.getLogger(__name__)

class Telegram:
    def __init__(self, cfg: dict):
        self.bots = {b["name"]: b for b in cfg.get("bots", [])}

    def send(self, bot_name: str, text: str, retries: int=3, timeout: int=10) -> None:
        bot = self.bots.get(bot_name)
        if not bot:
            logger.warning("Bot not configured", extra={"bot": bot_name})
            return
        url = f"https://api.telegram.org/bot{bot['token']}/sendMessage"
        payload = {"chat_id": bot["chat_id"], "text": text, "parse_mode": "HTML"}
        for i in range(retries):
            try:
                r = requests.post(url, json=payload, timeout=timeout)
                r.raise_for_status()
                return
            except requests.RequestException as e:
                logger.warning("Telegram send failed, retrying", extra={"bot": bot_name, "attempt": i+1})
                time.sleep(2 ** i)
        logger.error("Telegram send failed permanently", extra={"bot": bot_name})
