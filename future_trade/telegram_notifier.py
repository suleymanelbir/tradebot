# /opt/tradebot/future_trade/telegram_notifier.py
# -*- coding: utf-8 -*-
from __future__ import annotations

import json
import logging
from typing import Any, Dict, Optional
import re
import aiohttp

def escape_markdown_v2(text: str) -> str:
    """Telegram MarkdownV2 özel karakterlerini kaçır"""
    special_chars = '_*[]()~`>#+-=|{}.!'
    return re.sub(f'([{re.escape(special_chars)}])', r'\\\1', text)


class Notifier:
    """
    Telegram bildirim yöneticisi.
    - alerts_bot ve trades_bot kanallarına gönderim
    - persistence varsa gönderilen tüm payload'ları notifications_log tablosuna yazar (ayna)
    """

    # 1) Kurulum / bağımlılıklar
    def __init__(
        self,
        cfg: Dict[str, Any],
        persistence: Any = None,
        logger: Optional[logging.Logger] = None,
    ) -> None:
        self.cfg = cfg or {}
        self.persistence = persistence
        self.logger = logger or logging.getLogger("notifier")

        # bot yapılandırmaları
        tg = self.cfg
        self._alerts = tg.get("alerts_bot") or {}
        self._trades = tg.get("trades_bot") or {}

        # aiohttp oturumu
        self._session: Optional[aiohttp.ClientSession] = None

    # 2) İç yardımcılar
    async def _ensure_session(self) -> aiohttp.ClientSession:
        if self._session is None:
            self._session = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=15))
        return self._session

    def _mirror_to_db(self, channel: str, topic: str, level: str, payload: Dict[str, Any] | str) -> None:
        """
        Telegram'a gönderilen mesajların DB'ye aynalanması (opsiyonel).
        """
        try:
            if self.persistence and hasattr(self.persistence, "log_notification"):
                self.persistence.log_notification(channel=channel, topic=topic, level=level, payload=payload)
        except Exception as e:
                # DB aynalama tek başına kritik değil; log seviyesini düşük tut
                self.logger.debug(f"notification mirror skipped: {e}")

    async def _send(self, bot: Dict[str, Any], text: str, parse_mode: Optional[str] = None) -> None:
        """
        Tek bir bota (token/chat_id) mesaj gönder.
        """
        token = bot.get("token")
        chat_id = bot.get("chat_id")
        if not token or not chat_id:
            # yapılandırma eksikse sessiz geç
            self.logger.debug("telegram config missing (token/chat_id)")
            return

        api_url = f"https://api.telegram.org/bot{token}/sendMessage"
        payload = {
            "chat_id": str(chat_id),
            "text": text,
            "disable_web_page_preview": True,
        }
        if parse_mode:
            payload["parse_mode"] = parse_mode

        try:
            sess = await self._ensure_session()
            async with sess.post(api_url, json=payload) as resp:
                # telegram çoğu zaman 200 döner; problemde hata logla
                if resp.status >= 300:
                    body = await resp.text()
                    self.logger.warning(f"telegram send {resp.status}: {body}")
        except Exception as e:
            self.logger.warning(f"telegram send error: {e}")






    # 3) Genel amaçlı gönderim API'leri

    async def alert(self, payload: Dict[str, Any] | str) -> None:
        """
        Kritik/uyarı niteliğindeki mesajlar (alerts_bot).
        payload dict ise 'event' alanını topic olarak kullanır.
        """
        topic = None
        if isinstance(payload, dict):
            topic = str(payload.get("event") or "alert")
            raw_json = json.dumps(payload, ensure_ascii=False, indent=2)
            safe_json = escape_markdown_v2(raw_json)
            safe_topic = escape_markdown_v2(topic)
            text = f"⚠️ {safe_topic}\n```json\n{safe_json}\n```"
            parse_mode = "MarkdownV2"
        else:
            text = f"⚠️ {payload}"
            parse_mode = None
            topic = "alert"

        self._mirror_to_db(channel="alerts_bot", topic=topic, level="WARN", payload=payload)
        await self._send(self._alerts, text, parse_mode=parse_mode)

    async def info_trades(self, payload: Dict[str, Any] | str) -> None:
        """
        Operasyonel/ bilgi mesajları (trades_bot).
        """
        topic = None
        if isinstance(payload, dict):
            topic = str(payload.get("event") or "info")
            raw_json = json.dumps(payload, ensure_ascii=False, indent=2)
            safe_json = escape_markdown_v2(raw_json)
            safe_topic = escape_markdown_v2(topic)
            text = f"ℹ️ {safe_topic}\n```json\n{safe_json}\n```"
            parse_mode = "MarkdownV2"
        else:
            text = f"ℹ️ {payload}"
            parse_mode = None
            topic = "info"

        self._mirror_to_db(channel="trades_bot", topic=topic, level="INFO", payload=payload)
        await self._send(self._trades, text, parse_mode=parse_mode)

    # 4) Hazır yardımcılar (konulu bildirimler)
    async def notify_pnl_daily(self, summary: Dict[str, Any]) -> None:
        """
        Gün sonu PnL özeti (genişletilmiş).
        """
        topic = str(summary.get("event") or "pnl_daily")
        self._mirror_to_db(channel="trades_bot", topic=topic, level="INFO", payload=summary)

        # Güvenli alımlar
        def _fmt(x, nd=4):
            try:
                return f"{float(x):.{nd}f}"
            except Exception:
                return str(x)

        lines = [
            f"📊 Gün Sonu PnL — {summary.get('date')}",
            f"🔢 Trades: {summary.get('trades',0)}  |  Wins: {summary.get('wins',0)}  Losses: {summary.get('losses',0)}  Winrate: {_fmt(100*summary.get('winrate',0),2)}%",
            f"💰 Realized(Net): {_fmt(summary.get('realized_pnl'),4)}   (Gross: {_fmt(summary.get('realized_pnl_gross'),4)}  Fees: {_fmt(summary.get('fees'),4)})",
            f"📈 Unrealized: {_fmt(summary.get('unrealized_pnl'),4)}",
            f"🏆 Avg Win: {_fmt(summary.get('avg_win'),4)}   |   💥 Avg Loss: {_fmt(summary.get('avg_loss'),4)}",
            f"📐 Profit Factor: {summary.get('profit_factor')}",
            f"📉 Max DD (realized curve): {_fmt(summary.get('max_dd'),4)}",
            f"⏱️ Streak — Max WIN: {summary.get('max_win_streak',0)}  |  Max LOSS: {summary.get('max_loss_streak',0)}  |  Current: {summary.get('current_streak_type','NONE')} x{summary.get('current_streak_len',0)}",
        ]

        text = "\n".join(lines)
        await self._send(self._trades, text)


    async def notify_position_risk(self, payload: Dict[str, Any]) -> None:
        """
        Likidasyon yakınlığı vb. risk uyarıları.
        payload: {"event":"position_risk","symbol":...,"qty":...,"mark":...,"liq":...,"dist_pct":...}
        """
        topic = str(payload.get("event") or "position_risk")
        self._mirror_to_db(channel="alerts_bot", topic=topic, level="WARN", payload=payload)
        text = (
            f"🧯 Position Risk — {payload.get('symbol')}\n"
            f"qty={payload.get('qty')}  mark={payload.get('mark')}  liq={payload.get('liq')}\n"
            f"distance={(payload.get('dist_pct') and round(float(payload['dist_pct'])*100,2))}%"
        )
        await self._send(self._alerts, text)

    async def notify_kill_switch(self, payload: Dict[str, Any]) -> None:
        """
        Kill-Switch tetikleri / resetleri.
        payload: {"event":"kill_switch","reason":...,"dd_pct":...,"pnl_pct":...}
        """
        topic = str(payload.get("event") or "kill_switch")
        lvl = "ERROR" if str(payload).lower().find("trigger") >= 0 else "INFO"
        self._mirror_to_db(channel="alerts_bot", topic=topic, level=lvl, payload=payload)
        text = f"⛔ Kill-Switch\n```json\n{json.dumps(payload, ensure_ascii=False, indent=2)}\n```"
        await self._send(self._alerts, text, parse_mode="Markdown")

    async def notify_order_event(self, payload: Dict[str, Any]) -> None:
        """
        Sipariş olayları (filled/canceled/rejected).
        payload: {"event":"order_filled","symbol":...,"side":...,"qty":...,"price":...,"client_id":...}
        """
        topic = str(payload.get("event") or "order_event")
        level = "INFO"
        if "rejected" in topic or "error" in topic:
            level = "ERROR"
        elif "canceled" in topic:
            level = "WARN"

        self._mirror_to_db(channel="trades_bot", topic=topic, level=level, payload=payload)
        text = (
            f"🧾 {topic.upper()} — {payload.get('symbol')}\n"
            f"{payload.get('side')} {payload.get('qty')} @ {payload.get('price')}  (id={payload.get('client_id')})"
        )
        await self._send(self._trades, text)

    async def notify_tp_update(self, payload: Dict[str, Any]) -> None:
        """
        TP upsert/replace bilgilendirmeleri.
        """
        topic = str(payload.get("event") or "tp_update")
        self._mirror_to_db(channel="trades_bot", topic=topic, level="INFO", payload=payload)
        text = f"🎯 TP Update\n```json\n{json.dumps(payload, ensure_ascii=False, indent=2)}\n```"
        await self._send(self._trades, text, parse_mode="Markdown")

    async def notify_sl_update(self, payload: Dict[str, Any]) -> None:
        """
        SL upsert/replace bilgilendirmeleri.
        """
        topic = str(payload.get("event") or "sl_update")
        self._mirror_to_db(channel="trades_bot", topic=topic, level="INFO", payload=payload)
        text = f"🛡️ SL Update\n```json\n{json.dumps(payload, ensure_ascii=False, indent=2)}\n```"
        await self._send(self._trades, text, parse_mode="Markdown")

    # 5) Temizlik
    async def aclose(self) -> None:
        if self._session:
            try:
                await self._session.close()
            except Exception:
                pass
            self._session = None
