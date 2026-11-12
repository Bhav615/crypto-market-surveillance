"""Sink: Kafka (trades, bars, anomalies, metrics) -> PostgreSQL.

Batches up to 2000 records, writes them in one DB transaction with idempotent upserts, and
only then commits the Kafka offsets. If the database is down the process exits and Docker
restarts it; it resumes from the last committed offset, so nothing is lost or duplicated.
"""
from __future__ import annotations

import json
import logging
import signal
import time

import psycopg
from confluent_kafka import KafkaError, KafkaException
from psycopg.types.json import Jsonb

from app import config
from app.kafka_io import make_consumer, make_producer, produce_json
from app.metrics import MetricsReporter
from app.models import BadRecord, Trade, now_ms

log = logging.getLogger("sink")
running = True

SQL_TRADE = """
INSERT INTO trades (exchange, symbol, trade_id, price, size, notional, side, source,
                    event_ts, ingest_ts, sink_ts, kafka_partition, kafka_offset,
                    ingest_latency_ms, pipeline_latency_ms, e2e_latency_ms)
VALUES (%s,%s,%s,%s,%s,%s,%s,%s, to_timestamp(%s/1000.0), to_timestamp(%s/1000.0),
        to_timestamp(%s/1000.0), %s,%s,%s,%s,%s)
ON CONFLICT (exchange, symbol, trade_id) DO NOTHING"""

SQL_BAR = """
INSERT INTO bars (exchange, symbol, bar_seconds, bar_start, open, high, low, close, vwap,
                  volume, notional, buy_notional, sell_notional, trades)
VALUES (%s,%s,%s, to_timestamp(%s/1000.0), %s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
ON CONFLICT (exchange, symbol, bar_seconds, bar_start) DO UPDATE SET
  open=EXCLUDED.open, high=EXCLUDED.high, low=EXCLUDED.low, close=EXCLUDED.close,
  vwap=EXCLUDED.vwap, volume=EXCLUDED.volume, notional=EXCLUDED.notional,
  buy_notional=EXCLUDED.buy_notional, sell_notional=EXCLUDED.sell_notional, trades=EXCLUDED.trades"""

SQL_ANOMALY = """
INSERT INTO anomalies (anomaly_id, type, exchange, symbol, event_ts, detected_ts, sink_ts,
                       score, value, threshold, description, details)
VALUES (%s,%s,%s,%s, to_timestamp(%s/1000.0), to_timestamp(%s/1000.0), to_timestamp(%s/1000.0),
        %s,%s,%s,%s,%s)
ON CONFLICT (anomaly_id) DO NOTHING"""

SQL_METRIC = """
INSERT INTO pipeline_metrics (ts, service, instance, payload)
VALUES (to_timestamp(%s/1000.0), %s, %s, %s)"""


def _stop(*_):
    global running
    running = False


def connect_db(retries: int = 30) -> psycopg.Connection:
    for i in range(retries):
        try:
            return psycopg.connect(config.POSTGRES_DSN, autocommit=False)
        except psycopg.OperationalError as exc:
            log.info("waiting for postgres (%d/%d): %s", i + 1, retries, exc)
            time.sleep(2)
    raise SystemExit("postgres not reachable")


def to_rows(msgs, sink_ts: int, metrics: MetricsReporter, dlq: list):
    trades, bars, anomalies, mets = [], [], [], []
    for m in msgs:
        topic, raw = m.topic(), m.value()
        try:
            d = json.loads(raw)
            if topic == config.TOPIC_TRADES:
                t = Trade.from_dict(d)
                live = t.source == "ws"
                trades.append((t.exchange, t.symbol, t.trade_id, t.price, t.size, t.notional, t.side,
                               t.source, t.event_ts, t.ingest_ts, sink_ts, m.partition(), m.offset(),
                               t.ingest_ts - t.event_ts if live else None,
                               sink_ts - t.ingest_ts if live else None,
                               sink_ts - t.event_ts if live else None))
                if live:
                    metrics.observe("ingest_to_sink_ms", sink_ts - t.ingest_ts)
                    metrics.observe("exchange_to_sink_ms", sink_ts - t.event_ts)
            elif topic == config.TOPIC_BARS:
                bars.append((d["exchange"], d["symbol"], d["bar_seconds"], d["bar_start"], d["open"],
                             d["high"], d["low"], d["close"], d["vwap"], d["volume"], d["notional"],
                             d["buy_notional"], d["sell_notional"], d["trades"]))
            elif topic == config.TOPIC_ANOMALIES:
                anomalies.append((d["anomaly_id"], d["type"], d["exchange"], d["symbol"], d["event_ts"],
                                  d["detected_ts"], sink_ts, d["score"], d["value"], d["threshold"],
                                  d["description"], Jsonb(d.get("details", {}))))
                metrics.observe("anomaly_event_to_sink_ms", sink_ts - d["event_ts"])
            elif topic == config.TOPIC_METRICS:
                mets.append((d["ts"], d["service"], d["instance"], Jsonb(d)))
        except Exception as exc:
            dlq.append(BadRecord(f"sink:{topic}", repr(exc), raw.decode(errors="replace")))
    return trades, bars, anomalies, mets


def main() -> None:
    signal.signal(signal.SIGTERM, _stop)
    signal.signal(signal.SIGINT, _stop)
    conn = connect_db()
    consumer = make_consumer("postgres-sink")
    producer = make_producer("sink")
    written = {"trades": 0, "bars": 0, "anomalies": 0, "metrics": 0, "dlq": 0}
    metrics = MetricsReporter("sink", producer, extra=lambda: dict(written))
    consumer.subscribe([config.TOPIC_TRADES, config.TOPIC_BARS, config.TOPIC_ANOMALIES, config.TOPIC_METRICS])
    log.info("sink started")

    while running:
        msgs = [m for m in consumer.consume(num_messages=2000, timeout=0.5) if not _is_error(m)]
        if msgs:
            sink_ts = now_ms()
            dlq: list[BadRecord] = []
            trades, bars, anomalies, mets = to_rows(msgs, sink_ts, metrics, dlq)
            metrics.incr_in(len(msgs), sum(len(m.value()) for m in msgs))
            with conn.cursor() as cur:  # any DB error -> exception -> container restarts
                for sql, rows, name in ((SQL_TRADE, trades, "trades"), (SQL_BAR, bars, "bars"),
                                        (SQL_ANOMALY, anomalies, "anomalies"), (SQL_METRIC, mets, "metrics")):
                    if rows:
                        cur.executemany(sql, rows)
                        written[name] += len(rows)
            conn.commit()
            for bad in dlq:
                produce_json(producer, config.TOPIC_DLQ, "sink", bad.to_dict())
            written["dlq"] += len(dlq)
            producer.flush(10)
            metrics.incr_out(len(msgs))
            try:
                consumer.commit(asynchronous=False)
            except KafkaException as exc:
                log.warning("offset commit failed: %s", exc)
        producer.poll(0)
        metrics.maybe_report()

    consumer.close()
    conn.close()


def _is_error(m) -> bool:
    if m.error() is None:
        return False
    if m.error().code() != KafkaError._PARTITION_EOF:
        log.error("consumer error: %s", m.error())
    return True


if __name__ == "__main__":
    logging.basicConfig(level=config.LOG_LEVEL, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    main()
