"""Coinbase Exchange public market-data WebSocket (no API key needed).
Docs: https://docs.cdp.coinbase.com/exchange/websocket-feed/channels

Channel `matches` emits one message per executed trade:
  {"type":"match","trade_id":10,"sequence":50,"maker_order_id":"..","taker_order_id":"..",
   "time":"2014-11-07T08:19:27.028459Z","product_id":"BTC-USD","size":"5.23512",
   "price":"400.23","side":"sell"}
`side` is the MAKER side, so the taker (aggressor) side is the opposite.
Coinbase warns that matches can be dropped, so gaps in trade_id are repaired from the
public REST endpoint GET /products/{id}/trades.
"""
from __future__ import annotations

import asyncio
import json
import logging
import time
import urllib.request

from app import config
from app.exchanges.base import ExchangeAdapter
from app.models import Trade, now_ms, parse_iso_ms

log = logging.getLogger(__name__)


def _taker_side(maker_side: str) -> str:
    return "buy" if maker_side == "sell" else "sell"


class CoinbaseAdapter(ExchangeAdapter):
    name = "coinbase"
    url = config.COINBASE_WS_URL

    def subscribe_messages(self) -> list[dict]:
        return [{"type": "subscribe", "product_ids": self.symbols, "channels": ["matches", "heartbeat"]}]

    def parse(self, msg: dict, ingest_ts: int) -> list[Trade]:
        mtype = msg.get("type")
        if mtype in ("match", "last_match"):
            return [Trade(
                exchange=self.name,
                symbol=msg["product_id"],
                trade_id=int(msg["trade_id"]),
                price=float(msg["price"]),
                size=float(msg["size"]),
                side=_taker_side(msg["side"]),
                event_ts=parse_iso_ms(msg["time"]),
                ingest_ts=ingest_ts,
            )]
        if mtype == "error":
            log.error("coinbase error message: %s", msg)
        return []  # subscriptions, heartbeat, ...

    # ---- gap repair -------------------------------------------------------
    def on_gap(self, symbol: str, first_missing: int, last_missing: int) -> None:
        super().on_gap(symbol, first_missing, last_missing)
        n = last_missing - first_missing + 1
        if n > config.BACKFILL_MAX_TRADES:
            log.warning("coinbase %s: gap of %d too large to backfill", symbol, n)
            return
        asyncio.get_running_loop().create_task(self._backfill(symbol, first_missing, last_missing))

    async def _backfill(self, symbol: str, first: int, last: int) -> None:
        try:
            trades = await asyncio.to_thread(self.fetch_range, symbol, first, last)
        except Exception as exc:
            log.warning("coinbase %s: backfill failed: %r", symbol, exc)
            return
        for t in trades:
            await self.out.put(t)
        self.backfilled += len(trades)
        log.info("coinbase %s: backfilled %d/%d missing trades via REST", symbol, len(trades), last - first + 1)

    def fetch_range(self, symbol: str, first: int, last: int) -> list[Trade]:
        """REST pagination: `after=<trade_id>` returns trades OLDER than that id, newest first."""
        cursor, pages, found = last + 1, 0, []
        while cursor > first and pages < 10:
            url = f"{config.COINBASE_REST_URL}/products/{symbol}/trades?limit=1000&after={cursor}"
            req = urllib.request.Request(url, headers={"User-Agent": "kafka-market-surveillance/1.0",
                                                       "Accept": "application/json"})
            with urllib.request.urlopen(req, timeout=10) as resp:
                rows = json.load(resp)
            if not rows:
                break
            ingest = now_ms()
            for r in rows:
                tid = int(r["trade_id"])
                if first <= tid <= last:
                    found.append(Trade(self.name, symbol, tid, float(r["price"]), float(r["size"]),
                                       _taker_side(r["side"]), parse_iso_ms(r["time"]), ingest,
                                       source="rest_backfill"))
            oldest = min(int(r["trade_id"]) for r in rows)
            if oldest <= first:
                break
            cursor, pages = oldest, pages + 1
            time.sleep(0.25)  # stay well below the public rate limit
        found.sort(key=lambda t: t.trade_id)
        return found
