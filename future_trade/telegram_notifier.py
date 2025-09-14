"""İki ayrı bot: alerts_bot (hata/risk) ve trades_bot (işlem olayları)
Mesajlar JSON olarak gönderilir. (Parse sorunlarını azaltır)
"""
import json, logging
from typing import Dict, Any
import httpx


class Notifier:
    def __init__(self, cfg: Dict[str, Any]):
        self.cfg = cfg
        
    async def _send(self, bot_key: str, payload: Dict[str, Any]) -> None:
        bot = self.cfg.get(bot_key) or {}
        token, chat = bot.get("token"), str(bot.get("chat_id"))
        if not token or not chat:
            return
        url = f"https://api.telegram.org/bot{token}/sendMessage"
        data = {"chat_id": chat, "text": json.dumps(payload, ensure_ascii=False), "disable_web_page_preview": True}
        async with httpx.AsyncClient(timeout=8) as cli:
            try:
                r = await cli.post(url, json=data)
                if r.status_code >= 400:
                    logging.error(f"telegram body: {r.text}")
                r.raise_for_status()
            except Exception as e:
                logging.error(f"telegram error: {e}")
        
    async def alert(self, payload: Dict[str, Any]):
        await self._send("alerts_bot", payload)
        
    async def trade(self, payload: Dict[str, Any]):
        await self._send("trades_bot", payload)
    
    async def info_trades(self, payload: Dict[str, Any]):
        await self.trade({"level": "info", **payload})
    
    async def debug_trades(self, payload: Dict[str, Any]):
        await self.trade({"level": "debug", **payload})