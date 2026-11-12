"""Market-surveillance anomaly detectors (business objective: real-time anomaly detection
on live crypto trading, e.g. for a trading desk / compliance team).

1. WHALE_TRADE          single trade whose USD notional is an outlier for that market
2. PRICE_SPIKE          bar-to-bar return that is an outlier vs recent volatility
3. VOLUME_BURST         bar notional volume far above its recent level
4. CROSS_EXCHANGE_DIVERGENCE  same asset trades at materially different prices on
                              Coinbase vs Kraken at (almost) the same time
All thresholds are adaptive (rolling z-scores) plus an absolute floor, and every anomaly
gets a deterministic id so re-processing after a crash never creates duplicates.
"""
from __future__ import annotations

import hashlib
import math
from dataclasses import asdict, dataclass, field

from app import config
from app.analytics.bars import Bar, BarAggregator
from app.analytics.rolling import RollingStats
from app.models import Trade, now_ms


def anomaly_id(*parts) -> str:
    return hashlib.sha1("|".join(map(str, parts)).encode()).hexdigest()[:24]


@dataclass(slots=True)
class Anomaly:
    anomaly_id: str
    type: str
    exchange: str
    symbol: str
    event_ts: int
    detected_ts: int
    score: float
    value: float
    threshold: float
    description: str
    details: dict = field(default_factory=dict)

    def to_dict(self) -> dict:
        return asdict(self)


class WhaleTradeDetector:
    def __init__(self, window=config.WHALE_WINDOW, min_history=config.WHALE_MIN_HISTORY,
                 z=config.WHALE_Z, min_notional=config.WHALE_MIN_NOTIONAL_USD) -> None:
        self.window, self.min_history, self.z, self.min_notional = window, min_history, z, min_notional
        self.stats: dict[tuple[str, str], RollingStats] = {}

    def on_trade(self, t: Trade) -> Anomaly | None:
        st = self.stats.setdefault(t.key, RollingStats(self.window))
        x = math.log10(t.notional)  # trade sizes are ~log-normal
        result = None
        if st.n >= self.min_history and t.notional >= self.min_notional:
            z = st.zscore(x)
            if z is not None and z >= self.z:
                typical = 10 ** st.mean
                result = Anomaly(
                    anomaly_id("WHALE_TRADE", t.exchange, t.symbol, t.trade_id), "WHALE_TRADE",
                    t.exchange, t.symbol, t.event_ts, now_ms(), round(z, 3), t.notional, self.min_notional,
                    f"{t.side.upper()} ${t.notional:,.0f} of {t.symbol} on {t.exchange} "
                    f"(~{t.notional / typical:,.0f}x a typical trade)",
                    {"trade_id": t.trade_id, "price": t.price, "size": t.size, "side": t.side,
                     "typical_notional": round(typical, 2)})
        st.add(x)
        return result


class PriceSpikeDetector:
    def __init__(self, window=config.BAR_WINDOW, min_history=config.BAR_MIN_HISTORY,
                 z=config.SPIKE_Z, min_bps=config.SPIKE_MIN_BPS) -> None:
        self.window, self.min_history, self.z, self.min_bps = window, min_history, z, min_bps
        self.stats: dict[tuple[str, str], RollingStats] = {}
        self.last_close: dict[tuple[str, str], float] = {}

    def on_bar(self, b: Bar) -> Anomaly | None:
        prev = self.last_close.get(b.key)
        self.last_close[b.key] = b.close
        if prev is None:
            return None
        r = math.log(b.close / prev)
        st = self.stats.setdefault(b.key, RollingStats(self.window))
        result = None
        if st.n >= self.min_history:
            z = st.zscore(r)
            bps = r * 1e4
            if z is not None and abs(z) >= self.z and abs(bps) >= self.min_bps:
                result = Anomaly(
                    anomaly_id("PRICE_SPIKE", b.exchange, b.symbol, b.bar_start), "PRICE_SPIKE",
                    b.exchange, b.symbol, b.bar_start + b.bar_seconds * 1000, now_ms(), round(z, 3),
                    round(bps, 2), self.min_bps,
                    f"{b.symbol} on {b.exchange} moved {bps:+.1f} bps in {b.bar_seconds}s "
                    f"({abs(z):.1f} sigma)",
                    {"bar_start": b.bar_start, "prev_close": prev, "close": b.close,
                     "high": b.high, "low": b.low})
        st.add(r)
        return result


class VolumeBurstDetector:
    def __init__(self, window=config.BAR_WINDOW, min_history=config.BAR_MIN_HISTORY,
                 z=config.VOLUME_Z, min_ratio=config.VOLUME_MIN_RATIO) -> None:
        self.window, self.min_history, self.z, self.min_ratio = window, min_history, z, min_ratio
        self.stats: dict[tuple[str, str], RollingStats] = {}
        self.raw: dict[tuple[str, str], RollingStats] = {}

    def on_bar(self, b: Bar) -> Anomaly | None:
        st = self.stats.setdefault(b.key, RollingStats(self.window))
        raw = self.raw.setdefault(b.key, RollingStats(self.window))
        x = math.log1p(b.notional)
        result = None
        if st.n >= self.min_history:
            z = st.zscore(x)
            median = raw.median()
            ratio = b.notional / median if median > 0 else float("inf")
            if z is not None and z >= self.z and ratio >= self.min_ratio:
                result = Anomaly(
                    anomaly_id("VOLUME_BURST", b.exchange, b.symbol, b.bar_start), "VOLUME_BURST",
                    b.exchange, b.symbol, b.bar_start + b.bar_seconds * 1000, now_ms(), round(z, 3),
                    round(b.notional, 2), round(median * self.min_ratio, 2),
                    f"{b.symbol} on {b.exchange}: ${b.notional:,.0f} traded in {b.bar_seconds}s "
                    f"({ratio:,.1f}x the median)",
                    {"bar_start": b.bar_start, "trades": b.trades, "median_notional": round(median, 2),
                     "buy_notional": round(b.buy_notional, 2), "sell_notional": round(b.sell_notional, 2)})
        st.add(x)
        raw.add(b.notional)
        return result


class CrossExchangeDivergenceDetector:
    def __init__(self, threshold_bps=config.DIVERGENCE_BPS, max_age_ms=config.DIVERGENCE_MAX_AGE_MS,
                 cooldown_ms=config.DIVERGENCE_COOLDOWN_MS) -> None:
        self.threshold_bps, self.max_age_ms, self.cooldown_ms = threshold_bps, max_age_ms, cooldown_ms
        self.last: dict[str, dict[str, tuple[float, int]]] = {}
        self.last_alert: dict[str, int] = {}

    def on_trade(self, t: Trade) -> Anomaly | None:
        if t.source != "ws":
            return None  # backfilled trades are old; do not compare with live prices
        book = self.last.setdefault(t.symbol, {})
        book[t.exchange] = (t.price, t.event_ts)
        for other, (price, ts) in book.items():
            if other == t.exchange or abs(t.event_ts - ts) > self.max_age_ms:
                continue
            bps = (t.price - price) / ((t.price + price) / 2) * 1e4
            if abs(bps) >= self.threshold_bps and \
                    t.event_ts - self.last_alert.get(t.symbol, -10 ** 15) >= self.cooldown_ms:
                self.last_alert[t.symbol] = t.event_ts
                return Anomaly(
                    anomaly_id("CROSS_EXCHANGE_DIVERGENCE", t.exchange, t.symbol, t.trade_id),
                    "CROSS_EXCHANGE_DIVERGENCE", t.exchange, t.symbol, t.event_ts, now_ms(),
                    round(abs(bps) / self.threshold_bps, 3), round(bps, 2), self.threshold_bps,
                    f"{t.symbol}: {t.exchange} {t.price:g} vs {other} {price:g} ({bps:+.1f} bps)",
                    {"trade_id": t.trade_id, "price": t.price, "other_exchange": other,
                     "other_price": price, "other_age_ms": t.event_ts - ts})
        return None


class SurveillanceEngine:
    """Stateful per-symbol processing used by the detector service."""

    def __init__(self) -> None:
        self.bars = BarAggregator(config.BAR_SECONDS, config.BAR_LATENESS_MS)
        self.whale = WhaleTradeDetector()
        self.spike = PriceSpikeDetector()
        self.volume = VolumeBurstDetector()
        self.divergence = CrossExchangeDivergenceDetector()

    def process(self, t: Trade, wall_now: int) -> tuple[list[Bar], list[Anomaly]]:
        anomalies = [a for a in (self.whale.on_trade(t), self.divergence.on_trade(t)) if a]
        bars = self.bars.add(t, wall_now)
        anomalies += self._on_bars(bars)
        return bars, anomalies

    def tick(self, wall_now: int) -> tuple[list[Bar], list[Anomaly]]:
        bars = self.bars.flush_idle(wall_now)
        return bars, self._on_bars(bars)

    def _on_bars(self, bars: list[Bar]) -> list[Anomaly]:
        out = []
        for b in bars:
            out += [a for a in (self.spike.on_bar(b), self.volume.on_bar(b)) if a]
        return out
