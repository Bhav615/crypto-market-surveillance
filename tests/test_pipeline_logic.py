"""Unit tests for parsing, gap detection, bars and detectors.
The JSON messages below are the documented example payloads from the Coinbase and Kraken
API docs; they are used ONLY to test the parsers. The running pipeline ingests live data only.
Run:  docker compose run --rm detector pytest -q
"""
import asyncio
import json

from app.analytics.bars import BarAggregator
from app.analytics.detectors import (CrossExchangeDivergenceDetector, PriceSpikeDetector,
                                     VolumeBurstDetector, WhaleTradeDetector)
from app.analytics.rolling import RollingStats
from app.exchanges.base import GapTracker
from app.exchanges.coinbase import CoinbaseAdapter
from app.exchanges.kraken import KrakenAdapter
from app.models import BadRecord, Trade, parse_iso_ms

COINBASE_MATCH = {"type": "match", "trade_id": 10, "sequence": 50,
                  "maker_order_id": "ac928c66-ca53-498f-9c13-a110027a60e8",
                  "taker_order_id": "132fb6ae-456b-4654-b4e0-d681ac05cea1",
                  "time": "2014-11-07T08:19:27.028459Z", "product_id": "BTC-USD",
                  "size": "5.23512", "price": "400.23", "side": "sell"}
KRAKEN_UPDATE = {"channel": "trade", "type": "update", "data": [
    {"symbol": "MATIC/USD", "side": "sell", "price": 0.5117, "qty": 40.0, "ord_type": "market",
     "trade_id": 4665905, "timestamp": "2023-09-25T07:49:37.708706Z"}]}


def trade(tid, price, size=1.0, ex="coinbase", sym="BTC-USD", ts=1_700_000_000_000, side="buy", src="ws"):
    return Trade(ex, sym, tid, price, size, side, ts, ts + 50, src)


# ---------------------------------------------------------------- parsing
def test_parse_iso_variants():
    assert parse_iso_ms("1970-01-01T00:00:01.5Z") == 1500
    assert parse_iso_ms("2023-09-25T07:49:37.708706Z") == parse_iso_ms("2023-09-25T07:49:37.708706123Z")
    assert parse_iso_ms("1970-01-01T00:00:02Z") == 2000


def test_coinbase_match_parsing_and_taker_side():
    a = CoinbaseAdapter(["BTC-USD"], asyncio.Queue())
    [t] = a.parse(COINBASE_MATCH, ingest_ts=1)
    assert (t.exchange, t.symbol, t.trade_id, t.price, t.size) == ("coinbase", "BTC-USD", 10, 400.23, 5.23512)
    assert t.side == "buy"  # maker sold -> taker bought
    assert a.parse({"type": "heartbeat", "sequence": 1}, 1) == []
    assert a.subscribe_messages()[0]["channels"] == ["matches", "heartbeat"]


def test_kraken_parsing_and_symbol_normalization():
    a = KrakenAdapter(["MATIC-USD"], asyncio.Queue())
    [t] = a.parse(KRAKEN_UPDATE, ingest_ts=1)
    assert (t.symbol, t.side, t.size, t.trade_id) == ("MATIC-USD", "sell", 40.0, 4665905)
    assert a.parse({"channel": "heartbeat"}, 1) == []
    assert a.subscribe_messages()[0]["params"]["symbol"] == ["MATIC/USD"]


def test_malformed_message_goes_to_dlq():
    q = asyncio.Queue()
    a = CoinbaseAdapter(["BTC-USD"], q)
    bad = dict(COINBASE_MATCH, price="-1")
    asyncio.run(a.handle_raw(json.dumps(bad)))
    asyncio.run(a.handle_raw("not json"))
    items = [q.get_nowait() for _ in range(q.qsize())]
    assert len(items) == 2 and all(isinstance(i, BadRecord) for i in items)
    assert a.parse_errors == 2


def test_trade_roundtrip():
    t = trade(1, 100.0, 2.0)
    d = json.loads(json.dumps(t.to_dict()))
    assert Trade.from_dict(d) == t and d["notional"] == 200.0


# ---------------------------------------------------------------- accuracy
def test_gap_tracker():
    g = GapTracker()
    assert g.observe("X", 1) is None
    assert g.observe("X", 2) is None
    assert g.observe("X", 6) == (3, 5)
    assert g.observe("X", 4) is None  # late/duplicate
    assert (g.gaps, g.missing, g.duplicates) == (1, 3, 1)


def test_rolling_stats():
    r = RollingStats(3)
    for x in (1, 2, 3, 4):
        r.add(x)
    assert r.n == 3 and r.mean == 3 and abs(r.std - 1.0) < 1e-9 and r.median() == 3


# ---------------------------------------------------------------- bars
def test_bars_event_time_watermark_and_lateness():
    agg = BarAggregator(bar_seconds=10, lateness_ms=2000)
    t0 = 1_700_000_000_000  # multiple of 10s
    assert agg.add(trade(1, 100, ts=t0 + 1000), 0) == []
    assert agg.add(trade(2, 105, ts=t0 + 5000), 0) == []
    assert agg.add(trade(3, 99, ts=t0 + 11_000), 0) == []        # next bar, watermark not past lateness
    [bar] = agg.add(trade(4, 101, ts=t0 + 12_000), 0)            # watermark = end + 2s -> close
    assert (bar.open, bar.high, bar.low, bar.close, bar.trades) == (100, 105, 100, 105, 2)
    agg.add(trade(5, 1, ts=t0 + 3000), 0)                       # too late for the closed bar
    assert agg.late_trades == 1
    flushed = agg.flush_idle(wall_now=10 ** 9)
    assert len(flushed) == 1 and flushed[0].trades == 2


# ---------------------------------------------------------------- detectors
def test_whale_trade_detector():
    d = WhaleTradeDetector(window=500, min_history=50, z=3.5, min_notional=50_000)
    sizes = [0.01, 0.02, 0.015, 0.03, 0.012]
    for i in range(100):
        assert d.on_trade(trade(i, 60_000, sizes[i % 5])) is None
    a = d.on_trade(trade(1000, 60_000, 10.0))  # $600k
    assert a is not None and a.type == "WHALE_TRADE" and a.details["trade_id"] == 1000
    assert d.on_trade(trade(1001, 60_000, 0.02)) is None


def _bar(start, close, notional, ex="coinbase"):
    from app.analytics.bars import Bar
    return Bar(ex, "BTC-USD", start, 10, close, close, close, close, volume=notional / close,
               notional=notional, trades=10)


def test_price_spike_and_volume_burst():
    spike = PriceSpikeDetector(window=100, min_history=20, z=4, min_bps=10)
    vol = VolumeBurstDetector(window=100, min_history=20, z=4, min_ratio=5)
    price, wiggle = 60_000.0, [1, -1, 2, -2, 0.5, -0.5]
    for i in range(40):
        price += wiggle[i % 6]
        b = _bar(i * 10_000, price, 100_000 + (i % 4) * 5_000)
        assert spike.on_bar(b) is None and vol.on_bar(b) is None
    big = _bar(40 * 10_000, price * 1.01, 5_000_000)  # +100 bps, 50x volume
    a1, a2 = spike.on_bar(big), vol.on_bar(big)
    assert a1 and a1.type == "PRICE_SPIKE" and a1.value > 90
    assert a2 and a2.type == "VOLUME_BURST"


def test_cross_exchange_divergence_with_cooldown():
    d = CrossExchangeDivergenceDetector(threshold_bps=25, max_age_ms=5000, cooldown_ms=30_000)
    t0 = 1_700_000_000_000
    assert d.on_trade(trade(1, 100.0, ex="kraken", ts=t0)) is None
    assert d.on_trade(trade(1, 100.1, ex="coinbase", ts=t0 + 100)) is None   # 10 bps -> ok
    a = d.on_trade(trade(2, 100.5, ex="coinbase", ts=t0 + 200))             # 50 bps
    assert a and a.type == "CROSS_EXCHANGE_DIVERGENCE"
    assert d.on_trade(trade(3, 100.6, ex="coinbase", ts=t0 + 300)) is None   # cooldown
    assert d.on_trade(trade(4, 101.0, ex="coinbase", ts=t0 + 60_000)) is None  # kraken price too old


def test_anomaly_ids_are_deterministic():
    mk = lambda: WhaleTradeDetector(window=500, min_history=50, min_notional=1)
    ids = []
    for d in (mk(), mk()):
        for i in range(60):
            d.on_trade(trade(i, 100, 0.01 * (1 + i % 3)))
        ids.append(d.on_trade(trade(999, 100, 100)).anomaly_id)
    assert ids[0] == ids[1]


def test_price_spike_ignores_returns_across_downtime():
    spike = PriceSpikeDetector(window=100, min_history=20, z=4, min_bps=10)
    price = 60_000.0
    for i in range(40):
        price += [1, -1, 2, -2][i % 4]
        spike.on_bar(_bar(i * 10_000, price, 100_000))
    # laptop asleep for 3 hours, price moved 2 %: must NOT be a 10-second spike
    assert spike.on_bar(_bar(40 * 10_000 + 3 * 3600 * 1000, price * 1.02, 100_000)) is None
