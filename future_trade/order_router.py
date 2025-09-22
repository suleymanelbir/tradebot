# /opt/tradebot/future_trade/order_router.py
"""
Paper emir yönlendirici:
- place_entry_with_protection(plan): MARKET entry'yi anında 'FILLED' simüle eder, SL/TP sanal emirleri DB'ye yazar.
- update_trailing_for_open_positions(stream, trailing_cfg): açık pozisyonlar için trail_stop'u günceller ve SL sanal emrini 'yenile' mantığını uygular.
Notlar:
- Gerçekte Binance çağrıları yok; tüm hareketler futures_data.db üzerinde gerçekleşir.
- ONE_WAY per symbol varsayımı: aynı sembolde tek yön açık kabul edilir (long ya da short).
"""

from __future__ import annotations
from typing import Dict, Any, Optional
import time
import math
import logging
import sqlite3


def _now() -> int:
    return int(time.time())


class OrderRouter:
    def __init__(self, client, cfg: Dict[str, Any], notifier, persistence):
        self.client = client
        self.cfg = cfg or {}
        self.notifier = notifier
        self.persistence = persistence

        # Config kısayolları
        self.entry_type = self.cfg.get("entry", "MARKET")
        self.tp_mode = self.cfg.get("tp_mode", "limit")
        self.sl_working_type = self.cfg.get("sl_working_type", "MARK_PRICE")
        self.tif = self.cfg.get("time_in_force", "GTC")
        self.reduce_only_prot = bool(self.cfg.get("reduce_only_protection", True))

    # --- DB yardımcıları -----------------------------------------------------

    def _conn(self):
        return self.persistence._conn()

    def _upsert_position(self, symbol: str, side: str, qty: float, entry_price: float, leverage: int):
        ts = _now()
        with self._conn() as c:
            cur = c.cursor()
            # ONE_WAY: sembolde tek satır
            cur.execute(
                "INSERT INTO futures_positions(symbol, side, qty, entry_price, leverage, unrealized_pnl, updated_at) "
                "VALUES(?,?,?,?,?,0,?) "
                "ON CONFLICT(symbol) DO UPDATE SET side=excluded.side, qty=excluded.qty, "
                "entry_price=excluded.entry_price, leverage=excluded.leverage, updated_at=excluded.updated_at;",
                (symbol, side, float(qty), float(entry_price), int(leverage), ts),
            )
            c.commit()

    def _get_position(self, symbol: str) -> Optional[Dict[str, Any]]:
        with self._conn() as c:
            c.row_factory = sqlite3.Row
            cur = c.cursor()
            cur.execute("SELECT * FROM futures_positions WHERE symbol=?", (symbol,))
            row = cur.fetchone()
            return dict(row) if row else None

    def _insert_order(self, client_id: str, symbol: str, side: str, otype: str,
                      status: str, price: Optional[float], qty: float,
                      reduce_only: bool, extra: Optional[Dict[str, Any]] = None) -> int:
        ts = _now()
        with self._conn() as c:
            cur = c.cursor()
            cur.execute(
                "INSERT INTO futures_orders(client_id, symbol, side, type, status, price, qty, reduce_only, created_at, updated_at, extra_json) "
                "VALUES(?,?,?,?,?,?,?,?,?,?,?)",
                (
                    client_id, symbol, side, otype, status,
                    None if price is None else float(price),
                    float(qty),
                    1 if reduce_only else 0,
                    ts, ts,
                    None if not extra else str(extra),
                ),
            )
            oid = cur.lastrowid
            c.commit()
            return int(oid)

    def _set_symbol_state(self, symbol: str, **fields):
        # symbol_state: (symbol TEXT PRIMARY KEY, state TEXT, cooldown_until INTEGER, last_signal_ts INTEGER,
        #                trail_stop REAL, peak REAL, trough REAL, updated_at INTEGER)
        ts = _now()
        cols = []
        vals = []
        for k, v in fields.items():
            cols.append(f"{k}=?")
            vals.append(v)
        vals.append(ts)
        set_clause = ", ".join(cols + ["updated_at=?"])
        with self._conn() as c:
            cur = c.cursor()
            cur.execute(
                f"INSERT INTO symbol_state(symbol, {', '.join(k for k in fields.keys())}, updated_at) "
                f"VALUES(?, {', '.join('?' for _ in fields)}, ?) "
                f"ON CONFLICT(symbol) DO UPDATE SET {set_clause};",
                (symbol, *vals)
            )
            c.commit()

    def _get_symbol_state(self, symbol: str) -> Dict[str, Any]:
        with self._conn() as c:
            c.row_factory = sqlite3.Row
            cur = c.cursor()
            cur.execute("SELECT * FROM symbol_state WHERE symbol=?", (symbol,))
            row = cur.fetchone()
            return dict(row) if row else {}

    # --- Paper entry + SL/TP -------------------------------------------------

    async def place_entry_with_protection(self, plan):
        """
        Plan alanları (RiskManager.TradePlan):
          - symbol, side, entry, qty, leverage
          - sl, tp
          - entry_type, tp_mode, sl_working_type, time_in_force, reduce_only_protection
        Paper davranış:
          - Entry MARKET → anında 'FILLED'
          - SL: STOP_MARKET reduceOnly sanal emri (DB)
          - TP: 'limit' ise LIMIT reduceOnly sanal emri (DB)
        """
        sym = plan.symbol
        side = plan.side
        qty = float(plan.qty)
        entry = float(plan.entry or 0.0)
        lev = int(plan.leverage or 1)

        # 1) ENTRY (FILLED)
        entry_cid = f"entry_{_now()}"
        self._insert_order(
            client_id=entry_cid,
            symbol=sym,
            side=side,
            otype="MARKET",
            status="FILLED",
            price=entry,
            qty=qty,
            reduce_only=False,
            extra={"paper": True},
        )
        await self.notifier.trade({"event": "entry", "symbol": sym, "side": side, "qty": qty})

        # 2) POZİSYON (ONE_WAY) — qty işareti
        signed_qty = qty if side == "LONG" else -qty
        self._upsert_position(sym, side, signed_qty, entry, lev)

        # 3) Koruma emirleri (sanal)
        # SL
        if plan.sl:
            # LONG için sl < entry, SHORT için sl > entry olmalı — aksi durumda yazmayalım
            good = (side == "LONG" and float(plan.sl) < entry) or (side == "SHORT" and float(plan.sl) > entry)
            if good:
                sl_side = "SELL" if side == "LONG" else "BUY"
                sl_cid = f"sl_{_now()}"
                self._insert_order(
                    client_id=sl_cid,
                    symbol=sym,
                    side=sl_side,
                    otype="STOP_MARKET",
                    status="NEW",
                    price=float(plan.sl),
                    qty=abs(signed_qty),
                    reduce_only=True,
                    extra={"workingType": plan.sl_working_type, "paper": True},
                )
                await self.notifier.debug_trades({"event": "sl_ack", "symbol": sym, "orderId": sl_cid, "stop": float(plan.sl)})

                # symbol_state trail_stop başlangıcı
                self._set_symbol_state(sym, trail_stop=float(plan.sl))
        # TP
        if plan.tp and str(plan.tp_mode).lower() == "limit":
            tp_side = "SELL" if side == "LONG" else "BUY"
            tp_cid = f"tp_{_now()}"
            self._insert_order(
                client_id=tp_cid,
                symbol=sym,
                side=tp_side,
                otype="LIMIT",
                status="NEW",
                price=float(plan.tp),
                qty=abs(signed_qty),
                reduce_only=True,
                extra={"timeInForce": self.tif, "paper": True},
            )
            await self.notifier.debug_trades({"event": "tp_ack", "symbol": sym, "orderId": tp_cid, "tp": float(plan.tp)})

    # --- Trailing SL güncelle ------------------------------------------------

    async def update_trailing_for_open_positions(self, stream, trailing_cfg: Dict[str, Any]):
        """
        Basit yüzde bazlı trailing:
          - trailing_cfg: {"type": "percent", "percent": 2.0} ya da config'teki ATR/chandelier gelecekte eklenecek.
        Çalışma:
          - Açık pozisyonları al
          - Son fiyatı (paper için stream.get_last_price(symbol)) çek
          - Kâr yönünde trail_stop'u sıkılaştır
          - SL sanal emrini (DB'de STOP_MARKET satırı) güncellenmiş stopPrice ile "yenile" (DB’de sadece price alanını güncelleriz)
        """
        ttype = trailing_cfg.get("type", "percent").lower()
        pct = float(trailing_cfg.get("percent", 1.0))
        if pct <= 0:
            return

        with self._conn() as c:
            c.row_factory = sqlite3.Row
            cur = c.cursor()
            cur.execute("SELECT * FROM futures_positions WHERE ABS(qty) > 0")
            rows = cur.fetchall()

        for r in rows:
            sym = r["symbol"]
            side = r["side"]
            qty = float(r["qty"])
            last = stream.get_last_price(sym)
            if last is None:
                continue

            st = self._get_symbol_state(sym)
            prev_trail = st.get("trail_stop")

            # Yeni trail hesabı (yüzde)
            new_trail = self._percent_trail(side, last, prev_trail, pct)

            # Sadece kâr yönünde ve sıkılaştırma yapılır
            if prev_trail is not None:
                if side == "LONG" and new_trail <= prev_trail:
                    continue
                if side == "SHORT" and new_trail >= prev_trail:
                    continue

            # DB: symbol_state trail_stop güncelle
            self._set_symbol_state(sym, trail_stop=float(new_trail))

            # DB: SL sanal emrini güncelle (ilk bulunan STOP_MARKET reduceOnly)
            self._refresh_virtual_sl(sym, side, abs(qty), new_trail)

            await self.notifier.debug_trades({
                "event": "trailing_updated",
                "symbol": sym,
                "side": side,
                "stop": float(new_trail)
            })

    def _percent_trail(self, side: str, last_price: float, prev_trail: Optional[float], pct: float) -> float:
        if prev_trail is None:
            if side == "LONG":
                return last_price * (1 - pct / 100.0)
            else:
                return last_price * (1 + pct / 100.0)
        if side == "LONG":
            return max(prev_trail, last_price * (1 - pct / 100.0))
        else:
            return min(prev_trail, last_price * (1 + pct / 100.0))

    def _refresh_virtual_sl(self, symbol: str, side: str, qty_abs: float, new_stop: float) -> None:
        """
        futures_orders tablosunda STOP_MARKET reduceOnly emrini bularak price'ını günceller.
        (Paper: tek bir SL satırı varsayımı; birden fazla ise ilkini günceller.)
        """
        with self._conn() as c:
            c.row_factory = sqlite3.Row
            cur = c.cursor()
            cur.execute(
                "SELECT id FROM futures_orders WHERE symbol=? AND type='STOP_MARKET' AND reduce_only=1 ORDER BY id ASC LIMIT 1",
                (symbol,),
            )
            row = cur.fetchone()
            if not row:
                # SL hiç yoksa yeni bir tane ekle (güvenli taraf)
                sl_side = "SELL" if side == "LONG" else "BUY"
                self._insert_order(
                    client_id=f"sl_{_now()}",
                    symbol=symbol,
                    side=sl_side,
                    otype="STOP_MARKET",
                    status="NEW",
                    price=float(new_stop),
                    qty=float(qty_abs),
                    reduce_only=True,
                    extra={"paper": True},
                )
                return

            oid = int(row["id"])
            ts = _now()
            cur.execute(
                "UPDATE futures_orders SET price=?, updated_at=? WHERE id=?",
                (float(new_stop), ts, oid),
            )
            c.commit()
