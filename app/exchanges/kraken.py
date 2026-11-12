"""Kraken Spot WebSocket API v2, public `trade` channel (no API key needed).
Docs: https://docs.kraken.com/api/docs/websocket-v2/trade

Update message:
  {"channel":"trade","type":"update","data":[{"symbol":"MATIC/USD","side":"sell","price":0.5117,
   "qty":40.0,"ord_type":"market","trade_id":4665905,"timestamp":"2023-09-25T07:49:37.708706Z"}]}
`side` is the taker side. Symbols use "/" (BTC/USD); we normalize to "BTC-USD".
"""
from __future__ import annotations

import logging

from app import config
from app.exchanges.base import ExchangeAdapter
from app.models import Trade, parse_iso_ms

log = logging.getLogger(__name__)


class KrakenAdapter(ExchangeAdapter):
    name = "kraken"
    url = config.KRAKEN_WS_URL

    def subscribe_messages(self) -> list[dict]:
        return [{"method": "subscribe",
                 "params": {"channel": "trade", "symbol": [s.replace("-", "/") for s in self.symbols],
                            "snapshot": False}}]

    def parse(self, msg: dict, ingest_ts: int) -> list[Trade]:
        if msg.get("channel") == "trade" and msg.get("type") in ("update", "snapshot"):
            return [Trade(
                exchange=self.name,
                symbol=d["symbol"].replace("/", "-"),
                trade_id=int(d["trade_id"]),
                price=float(d["price"]),
                size=float(d["qty"]),
                side=d["side"],
                event_ts=parse_iso_ms(d["timestamp"]),
                ingest_ts=ingest_ts,
            ) for d in msg.get("data", [])]
        if msg.get("method") == "subscribe" and msg.get("success") is False:
            log.error("kraken subscribe failed: %s", msg)
        return []  # heartbeat, status, subscribe acks
