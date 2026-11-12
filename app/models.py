"""Normalized trade record shared by every exchange adapter and every consumer."""
from __future__ import annotations

import time
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone

_EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)
_SIDES = {"buy", "sell"}


def now_ms() -> int:
    return time.time_ns() // 1_000_000


def parse_iso_ms(ts: str) -> int:
    """Parse an exchange RFC3339 timestamp ('2023-09-25T07:49:37.708706Z') to epoch millis.
    Handles any number of fractional digits (Python's parser only accepts up to 6)."""
    ts = ts.strip()
    if ts.endswith("Z"):
        ts = ts[:-1] + "+00:00"
    if "." in ts:
        main, rest = ts.split(".", 1)
        i = 0
        while i < len(rest) and rest[i].isdigit():
            i += 1
        frac, tz = rest[:i], rest[i:]
        ts = f"{main}.{(frac + '000000')[:6]}{tz}"
    dt = datetime.fromisoformat(ts)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return (dt - _EPOCH) // timedelta(milliseconds=1)


@dataclass(slots=True)
class Trade:
    exchange: str        # "coinbase" | "kraken"
    symbol: str          # normalized, e.g. "BTC-USD"
    trade_id: int        # exchange trade id (sequential per symbol)
    price: float
    size: float          # base-currency quantity
    side: str            # taker side: "buy" | "sell"
    event_ts: int        # exchange match time, epoch ms
    ingest_ts: int       # time our producer received it, epoch ms
    source: str = "ws"   # "ws" (live) | "rest_backfill" (gap repair)

    @property
    def notional(self) -> float:
        return self.price * self.size

    @property
    def key(self) -> tuple[str, str]:
        return self.exchange, self.symbol

    def to_dict(self) -> dict:
        d = asdict(self)
        d["notional"] = self.notional
        return d

    @classmethod
    def from_dict(cls, d: dict) -> "Trade":
        t = cls(
            exchange=str(d["exchange"]),
            symbol=str(d["symbol"]),
            trade_id=int(d["trade_id"]),
            price=float(d["price"]),
            size=float(d["size"]),
            side=str(d["side"]),
            event_ts=int(d["event_ts"]),
            ingest_ts=int(d["ingest_ts"]),
            source=str(d.get("source", "ws")),
        )
        t.validate()
        return t

    def validate(self) -> None:
        if not self.exchange or not self.symbol:
            raise ValueError("missing exchange/symbol")
        if self.trade_id < 0:
            raise ValueError(f"bad trade_id {self.trade_id}")
        if not (self.price > 0) or not (self.size > 0):
            raise ValueError(f"non-positive price/size {self.price}/{self.size}")
        if self.side not in _SIDES:
            raise ValueError(f"bad side {self.side!r}")
        if self.event_ts <= 0:
            raise ValueError("bad event_ts")


@dataclass(slots=True)
class BadRecord:
    """Raw payload that could not be parsed; routed to the dead-letter topic."""
    stage: str
    error: str
    payload: str
    exchange: str = ""

    def to_dict(self) -> dict:
        return {"stage": self.stage, "error": self.error, "payload": self.payload[:10_000],
                "exchange": self.exchange, "ts": now_ms()}
