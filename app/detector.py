"""Stream processor: consumes `trades.raw`, builds OHLCV bars and detects anomalies.
Outputs: `ohlcv.bars`, `market.anomalies`. Scale out with
    docker compose up -d --scale detector=3
Kafka assigns partitions across instances; since trades are keyed by symbol, each symbol's
state lives in exactly one instance.

Delivery guarantee: offsets are committed only after all outputs of a batch are acknowledged
by Kafka (at-least-once). Outputs have deterministic ids/keys, and the sink upserts, so a replay
after a crash produces no duplicates in the database (effectively-once results).
"""
from __future__ import annotations

import json
import logging
import signal

from confluent_kafka import KafkaError, KafkaException

from app import config
from app.analytics.detectors import SurveillanceEngine
from app.kafka_io import DeliveryStats, make_consumer, make_producer, produce_json
from app.metrics import MetricsReporter
from app.models import BadRecord, Trade, now_ms

log = logging.getLogger("detector")
running = True


def _stop(*_):
    global running
    running = False


def main() -> None:
    signal.signal(signal.SIGTERM, _stop)
    signal.signal(signal.SIGINT, _stop)

    engine = SurveillanceEngine()
    consumer = make_consumer("anomaly-detector")
    producer = make_producer("detector")
    delivery = DeliveryStats()
    counts = {"bars": 0, "anomalies": 0, "dlq": 0}

    def extra() -> dict:
        return {**counts, "late_trades": engine.bars.late_trades, "delivery_failed": delivery.failed,
                "assigned_partitions": len(consumer.assignment())}

    metrics = MetricsReporter("detector", producer, extra=extra)

    def on_revoke(c, partitions):
        # Make sure everything produced so far is durable before we lose the partitions.
        producer.flush(30)
        try:
            c.commit(asynchronous=False)
        except KafkaException:
            pass
        log.info("partitions revoked: %s", [p.partition for p in partitions])

    def on_assign(c, partitions):
        log.info("partitions assigned: %s", [p.partition for p in partitions])

    consumer.subscribe([config.TOPIC_TRADES], on_assign=on_assign, on_revoke=on_revoke)

    def emit(bars, anomalies):
        for b in bars:
            produce_json(producer, config.TOPIC_BARS, b.symbol, b.to_dict(), on_delivery=delivery)
        for a in anomalies:
            produce_json(producer, config.TOPIC_ANOMALIES, a.symbol, a.to_dict(), on_delivery=delivery)
            log.warning("ANOMALY %-26s %s", a.type, a.description)
        counts["bars"] += len(bars)
        counts["anomalies"] += len(anomalies)
        metrics.incr_out(len(bars) + len(anomalies))

    log.info("detector started (bar=%ss)", config.BAR_SECONDS)
    while running:
        msgs = consumer.consume(num_messages=1000, timeout=0.5)
        wall = now_ms()
        real = 0
        for m in msgs:
            if m.error():
                if m.error().code() != KafkaError._PARTITION_EOF:
                    log.error("consumer error: %s", m.error())
                continue
            real += 1
            raw = m.value()
            metrics.incr_in(nbytes=len(raw))
            try:
                t = Trade.from_dict(json.loads(raw))
            except Exception as exc:
                counts["dlq"] += 1
                produce_json(producer, config.TOPIC_DLQ, "detector",
                             BadRecord("detector", repr(exc), raw.decode(errors="replace")).to_dict())
                continue
            emit(*engine.process(t, wall))
            if t.source == "ws":
                done = now_ms()
                metrics.observe("ingest_to_detect_ms", done - t.ingest_ts)
                metrics.observe("exchange_to_detect_ms", done - t.event_ts)
        emit(*engine.tick(wall))

        if real:
            if producer.flush(30) > 0:
                raise RuntimeError("outputs not acknowledged by Kafka; not committing offsets")
            try:
                consumer.commit(asynchronous=False)
            except KafkaException as exc:
                log.warning("commit failed (will retry next batch): %s", exc)
        producer.poll(0)
        metrics.maybe_report()

    log.info("stopping detector")
    producer.flush(30)
    try:
        consumer.commit(asynchronous=False)
    except KafkaException:
        pass
    consumer.close()


if __name__ == "__main__":
    logging.basicConfig(level=config.LOG_LEVEL, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    main()
