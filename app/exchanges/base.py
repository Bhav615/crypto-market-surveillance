"""Common WebSocket client loop: connect, subscribe, watchdog, reconnect with backoff,
sequence-gap detection. Exchange-specific subclasses only define URL, subscribe and parse."""
from __future__ import annotations

import asyncio
import json
import logging
import random

from websockets.asyncio.client import connect

from app import config
from app.models import BadRecord, Trade, now_ms

log = logging.getLogger(__name__)


class GapTracker:
    """Exchanges assign sequential trade ids per symbol. A jump means we missed trades."""

    def __init__(self) -> None:
        self.last: dict[str, int] = {}
        self.gaps = 0
        self.missing = 0
        self.duplicates = 0

    def observe(self, symbol: str, trade_id: int) -> tuple[int, int] | None:
        last = self.last.get(symbol)
        if last is None:
            self.last[symbol] = trade_id
            return None
        if trade_id <= last:
            self.duplicates += 1
            return None
        self.last[symbol] = trade_id
        if trade_id > last + 1:
            self.gaps += 1
            self.missing += trade_id - last - 1
            return last + 1, trade_id - 1
        return None


class ExchangeAdapter:
    name = "base"
    url = ""

    def __init__(self, symbols: list[str], out: asyncio.Queue) -> None:
        self.symbols = symbols
        self.out = out
        self.gaps = GapTracker()
        self.received = 0
        self.reconnects = 0
        self.parse_errors = 0
        self.backfilled = 0
        self.connected = False

    # ---- to implement -------------------------------------------------
    def subscribe_messages(self) -> list[dict]:
        raise NotImplementedError

    def parse(self, msg: dict, ingest_ts: int) -> list[Trade]:
        """Return trades in the message ([] for control messages). Raise on malformed trades."""
        raise NotImplementedError

    def on_gap(self, symbol: str, first_missing: int, last_missing: int) -> None:
        log.warning("%s %s: gap of %d trades (%d..%d)", self.name, symbol,
                    last_missing - first_missing + 1, first_missing, last_missing)

    # ---- shared -------------------------------------------------------
    def stats(self) -> dict:
        total = self.received + self.gaps.missing
        return {
            "exchange": self.name, "connected": self.connected,
            "received": self.received, "reconnects": self.reconnects,
            "gaps": self.gaps.gaps, "missing": self.gaps.missing,
            "duplicates": self.gaps.duplicates, "backfilled": self.backfilled,
            "parse_errors": self.parse_errors,
            "completeness_pct": round(100.0 * (self.received + self.backfilled) / total, 4) if total else 100.0,
        }

    async def handle_raw(self, raw: str | bytes) -> None:
        ingest = now_ms()
        try:
            msg = json.loads(raw)
            trades = self.parse(msg, ingest)
            for t in trades:
                t.validate()
        except Exception as exc:  # malformed -> DLQ, keep streaming
            self.parse_errors += 1
            text = raw.decode(errors="replace") if isinstance(raw, bytes) else raw
            await self.out.put(BadRecord("ingest", repr(exc), text, self.name))
            return
        for t in trades:
            self.received += 1
            gap = self.gaps.observe(t.symbol, t.trade_id)
            if gap:
                self.on_gap(t.symbol, *gap)
            await self.out.put(t)

    async def run(self) -> None:
        backoff = 1.0
        while True:
            try:
                log.info("%s: connecting to %s for %s", self.name, self.url, self.symbols)
                async with connect(self.url, open_timeout=15, ping_interval=20, ping_timeout=20,
                                   close_timeout=5, max_size=2 ** 23) as ws:
                    for sub in self.subscribe_messages():
                        await ws.send(json.dumps(sub))
                    self.connected = True
                    log.info("%s: connected and subscribed", self.name)
                    backoff = 1.0
                    while True:
                        raw = await asyncio.wait_for(ws.recv(), timeout=config.WS_WATCHDOG_SECONDS)
                        await self.handle_raw(raw)
            except asyncio.CancelledError:
                raise
            except asyncio.TimeoutError:
                log.warning("%s: no data for %.0fs, reconnecting", self.name, config.WS_WATCHDOG_SECONDS)
            except Exception as exc:
                log.warning("%s: connection error: %r", self.name, exc)
            self.connected = False
            self.reconnects += 1
            delay = backoff + random.uniform(0, backoff / 2)
            log.info("%s: reconnect #%d in %.1fs", self.name, self.reconnects, delay)
            await asyncio.sleep(delay)
            backoff = min(backoff * 2, 60.0)
