# /opt/tradebot/future_trade/order_reconciler.py
# -*- coding: utf-8 -*-
from __future__ import annotations

import asyncio
import logging
from typing import Optional, Dict, Any, List

# Eski: from .order_router import OrderRouter, Mode
# Yeni: Mode yok; yalnızca OrderRouter kullanıyoruz
from .order_router import OrderRouter


class OrderReconciler:
    """
    Emir/pozisyon uzlaştırma katmanı:
      - (opsiyonel) açık pozisyonların durumunu DB ile senkron tutma
      - toplu kapatma (kill-switch vb.)
      - ileride: fill/partial fill/stop tetikleriyle eşleştirme (genişletilebilir)
    """

    def __init__(self, router: OrderRouter, logger: Optional[logging.Logger] = None, persistence=None):
        self.router = router
        self.logger = logger or logging.getLogger("reconciler")
        self.persistence = persistence

    # ------- yardımcılar -------
    def _is_paper(self) -> bool:
        # router.client.mode genelde "paper" | "live"
        mode = getattr(getattr(self.router, "client", None), "mode", "paper")
        return str(mode).lower() == "paper"

    def _list_open_positions(self) -> List[Dict[str, Any]]:
        if hasattr(self.persistence, "list_open_positions"):
            try:
                return self.persistence.list_open_positions() or []
            except Exception as e:
                self.logger.debug(f"list_open_positions failed: {e}")
        return []

    # ------- toplu kapatma (Kill-Switch vs.) -------
    def close_all_positions(self, reason: str = "kill_switch") -> Dict[str, Any]:
        """
        Tüm açık pozisyonları reduceOnly-MARKET ile kapatmayı dener.
        """
        results = {"closed": 0, "errors": []}
        positions = self._list_open_positions()
        for p in positions:
            try:
                sym = p.get("symbol")
                side_pos = (p.get("side") or "").upper()  # LONG/SHORT
                qty = abs(float(p.get("qty", 0) or 0))
                if not sym or qty <= 0 or side_pos not in ("LONG", "SHORT"):
                    continue

                # Kapatma için ters yön gerekir
                side_close = "SELL" if side_pos == "LONG" else "BUY"
                self.router.close_position_market(sym, side_close, qty, tag=reason)
                results["closed"] += 1

                # DB cache güncelle (varsa)
                if hasattr(self.persistence, "cache_close_position"):
                    try:
                        self.persistence.cache_close_position(sym)
                    except Exception:
                        pass
            except Exception as e:
                self.logger.error(f"[RECON] close_all_positions error: {e}")
                results["errors"].append({"position": p, "error": str(e)})
        return results

    # ------- ana döngü -------
    async def run(self) -> None:
        """
        Minimal bir uzlaştırma döngüsü.
        (Şimdilik pasif; ileride fill/event tabanlı eşleştirmeler eklenebilir.)
        """
        self.logger.info("OrderReconciler loop started (mode=%s)", "paper" if self._is_paper() else "live")
        try:
            while True:
                # Burada periyodik kontrol/uyumlama yapılabilir.
                # Örn: self._reconcile_positions()
                await asyncio.sleep(1.0)
        except asyncio.CancelledError:
            self.logger.info("OrderReconciler loop cancelled")
        except Exception as e:
            self.logger.error(f"OrderReconciler loop error: {e}")
            # Döngü çökmesin diye kısa bekleme
            await asyncio.sleep(1.0)
