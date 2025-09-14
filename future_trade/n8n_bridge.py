"""n8n köprüsü: OUT (olay post), IN (komut dinleme) iskeleti
- OUT: notifier benzeri httpx POST
- IN: (TODO) küçük bir aiohttp sunucu ile webhook dinleme ve HMAC doğrulama
"""
import json, hmac, hashlib
from typing import Dict, Any
import httpx


class N8NBridge:
    def __init__(self, cfg: Dict[str, Any], notifier):
        self.cfg = cfg; self.notifier = notifier
        
    async def post_event(self, event: Dict[str, Any]) -> None:
        url = self.cfg.get("outgoing_webhook"); token = self.cfg.get("auth_token")
        if not url or not token: return
        async with httpx.AsyncClient(timeout=8) as cli:
            await cli.post(url, json=event, headers={"Authorization": f"Bearer {token}"})

    async def run(self):
        # TODO: aiohttp ile incoming_webhook dinle, HMAC doğrula, komut uygula
        pass