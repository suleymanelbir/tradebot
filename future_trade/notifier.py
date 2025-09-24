import os, asyncio, logging, json, urllib.request

class Notifier:
    def __init__(self, cfg: dict):
        self.cfg = cfg or {}
        self.logger = logging.getLogger("notifier")

    async def _post(self, bot_cfg: dict, payload: dict):
        token = (bot_cfg or {}).get("token")
        chat_id = (bot_cfg or {}).get("chat_id")
        if not token or not chat_id:
            return
        data = json.dumps({"chat_id": chat_id, "text": json.dumps(payload, ensure_ascii=False)}).encode()
        url = f"https://api.telegram.org/bot{token}/sendMessage"
        await asyncio.to_thread(lambda: urllib.request.urlopen(urllib.request.Request(url, data=data, headers={"Content-Type":"application/json"})))

    async def info_trades(self, payload: dict):
        await self._post((self.cfg or {}).get("trades_bot", {}), payload)

    async def alert(self, payload: dict):
        await self._post((self.cfg or {}).get("alerts_bot", {}), payload)

    async def aclose(self):  # uyumluluk
        return
