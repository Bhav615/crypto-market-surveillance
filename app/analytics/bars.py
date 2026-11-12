"""Event-time tumbling-window OHLCV bars per (exchange, symbol), with a watermark and
allowed lateness. A bar [start, start+size) closes once the watermark (max event time seen
for that key) passes start+size+lateness, or when the key goes idle (wall clock)."""
from __future__ import annotations

from dataclasses import asdict, dataclass

from app.models import Trade


@dataclass(slots=True)
class Bar:
    exchange: str
    symbol: str
    bar_start: int
    bar_seconds: int
    open: float
    high: float
    low: float
    close: float
    volume: float = 0.0
    notional: float = 0.0
    buy_notional: float = 0.0
    sell_notional: float = 0.0
    trades: int = 0
    first_trade_id: int = 0
    last_trade_id: int = 0

    @property
    def key(self) -> tuple[str, str]:
        return self.exchange, self.symbol

    @property
    def vwap(self) -> float:
        return self.notional / self.volume if self.volume else self.close

    def add(self, t: Trade) -> None:
        if self.trades == 0:
            self.first_trade_id = t.trade_id
        self.high = max(self.high, t.price)
        self.low = min(self.low, t.price)
        # close = price of the trade with the latest trade id (robust to out-of-order arrival)
        if t.trade_id >= self.last_trade_id:
            self.close = t.price
            self.last_trade_id = t.trade_id
        if t.trade_id < self.first_trade_id:
            self.first_trade_id = t.trade_id
            self.open = t.price
        self.volume += t.size
        self.notional += t.notional
        if t.side == "buy":
            self.buy_notional += t.notional
        else:
            self.sell_notional += t.notional
        self.trades += 1

    def to_dict(self) -> dict:
        d = asdict(self)
        d["bar_end"] = self.bar_start + self.bar_seconds * 1000
        d["vwap"] = self.vwap
        return d


class BarAggregator:
    def __init__(self, bar_seconds: int, lateness_ms: int, idle_ms: int | None = None) -> None:
        self.bar_ms = bar_seconds * 1000
        self.bar_seconds = bar_seconds
        self.lateness_ms = lateness_ms
        self.idle_ms = idle_ms if idle_ms is not None else self.bar_ms + lateness_ms
        self.open: dict[tuple[str, str], dict[int, Bar]] = {}
        self.watermark: dict[tuple[str, str], int] = {}
        self.closed_until: dict[tuple[str, str], int] = {}
        self.last_seen_wall: dict[tuple[str, str], int] = {}
        self.late_trades = 0

    def add(self, t: Trade, wall_now: int) -> list[Bar]:
        key = t.key
        start = t.event_ts - (t.event_ts % self.bar_ms)
        self.last_seen_wall[key] = wall_now
        self.watermark[key] = max(self.watermark.get(key, 0), t.event_ts)
        if start <= self.closed_until.get(key, -1):
            self.late_trades += 1  # bar already emitted; trade still stored by the sink
        else:
            bars = self.open.setdefault(key, {})
            bar = bars.get(start)
            if bar is None:
                bar = bars[start] = Bar(t.exchange, t.symbol, start, self.bar_seconds,
                                        t.price, t.price, t.price, t.price,
                                        first_trade_id=t.trade_id, last_trade_id=t.trade_id)
            bar.add(t)
        return self._close_ready(key)

    def _close_ready(self, key) -> list[Bar]:
        wm = self.watermark.get(key, 0)
        bars = self.open.get(key, {})
        ready = sorted(s for s in bars if s + self.bar_ms + self.lateness_ms <= wm)
        return [self._close(key, s) for s in ready]

    def _close(self, key, start: int) -> Bar:
        bar = self.open[key].pop(start)
        self.closed_until[key] = max(self.closed_until.get(key, -1), start)
        return bar

    def flush_idle(self, wall_now: int) -> list[Bar]:
        """Close bars of keys that received nothing for a while (illiquid symbols)."""
        out: list[Bar] = []
        for key, bars in self.open.items():
            if bars and wall_now - self.last_seen_wall.get(key, wall_now) >= self.idle_ms:
                out.extend(self._close(key, s) for s in sorted(bars))
        return out
