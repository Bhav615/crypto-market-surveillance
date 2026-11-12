"""Producer service: live exchange WebSocket -> Kafka topic `trades.raw`.

Run:  EXCHANGE=coinbase python -m app.producer   (or EXCHANGE=kraken)
Messages are keyed by symbol, so every trade of a symbol (from both exchanges) lands in the
same partition -> per-symbol ordering and stateful processing in the detector.
"""
from __future__ import annotations

import asyncio
import json
import logging
import signal
import time
import uuid

from confluent_kafka import KafkaError, TopicPartition

from app import config
from app.exchanges.coinbase import CoinbaseAdapter
from app.exchanges.kraken import KrakenAdapter
from app.kafka_io import DeliveryStats, make_consumer, make_producer, produce_json
from app.metrics import MetricsReporter
from app.models import BadRecord, Trade, now_ms

ADAPTERS = {"coinbase": CoinbaseAdapter, "kraken": KrakenAdapter}
log = logging.getLogger("producer")


def last_trade_ids_from_kafka(exchange: str, per_partition: int = 5000) -> dict[str, int]:
    """Crash/restart recovery: read the tail of `trades.raw` to find the last trade id we already
    published per symbol. The first live trade after a restart then reveals the downtime gap,
    which is backfilled from REST (Coinbase) or reported as missing (Kraken)."""
    c = make_consumer(f"producer-seed-{uuid.uuid4()}", **{"enable.partition.eof": True})
    last: dict[str, int] = {}
    try:
        meta = c.list_topics(config.TOPIC_TRADES, timeout=15)
        tps = []
        for p in meta.topics[config.TOPIC_TRADES].partitions:
            lo, hi = c.get_watermark_offsets(TopicPartition(config.TOPIC_TRADES, p), timeout=15)
            if hi > lo:
                tps.append(TopicPartition(config.TOPIC_TRADES, p, max(lo, hi - per_partition)))
        if not tps:
            return last
        c.assign(tps)
        remaining, deadline = len(tps), time.time() + 30
        while remaining and time.time() < deadline:
            m = c.poll(1.0)
            if m is None:
                continue
            if m.error():
                if m.error().code() == KafkaError._PARTITION_EOF:
                    remaining -= 1
                continue
            try:
                d = json.loads(m.value())
                if d.get("exchange") == exchange:
                    last[d["symbol"]] = max(last.get(d["symbol"], 0), int(d["trade_id"]))
            except Exception:
                continue
    finally:
        c.close()
    return last


async def main() -> None:
    if config.EXCHANGE not in ADAPTERS:
        raise SystemExit(f"EXCHANGE must be one of {list(ADAPTERS)}")
    queue: asyncio.Queue = asyncio.Queue(maxsize=200_000)
    adapter = ADAPTERS[config.EXCHANGE](config.SYMBOLS, queue)
    try:
        seeded = last_trade_ids_from_kafka(adapter.name)
        adapter.gaps.last.update(seeded)
        log.info("recovered last published trade ids from Kafka: %s", seeded or "none (first start)")
    except Exception as exc:
        log.warning("could not recover last trade ids from Kafka (%r); starting fresh", exc)
    producer = make_producer(f"producer-{adapter.name}")
    delivery = DeliveryStats()
    dlq_count = 0

    def extra() -> dict:
        return {**adapter.stats(), "delivered": delivery.delivered, "delivery_failed": delivery.failed,
                "dlq": dlq_count, "queue_depth": queue.qsize()}

    metrics = MetricsReporter(f"producer-{adapter.name}", producer, extra=extra)
    ws_task = asyncio.create_task(adapter.run())

    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, stop.set)
        except NotImplementedError:  # Windows without Docker
            pass

    log.info("producing %s trades to %s (bootstrap=%s)", adapter.name, config.TOPIC_TRADES, config.KAFKA_BOOTSTRAP)
    while not stop.is_set():
        if ws_task.done():  # adapter loop never returns normally; surface crashes
            ws_task.result()
        try:
            item = await asyncio.wait_for(queue.get(), timeout=1.0)
        except asyncio.TimeoutError:
            item = None
        if isinstance(item, Trade):
            payload = item.to_dict()
            produce_json(producer, config.TOPIC_TRADES, item.symbol, payload, on_delivery=delivery)
            metrics.incr_in()
            metrics.incr_out()
            if item.source == "ws":
                metrics.observe("exchange_to_ingest_ms", item.ingest_ts - item.event_ts)
                metrics.observe("ingest_to_produce_ms", now_ms() - item.ingest_ts)
        elif isinstance(item, BadRecord):
            dlq_count += 1
            produce_json(producer, config.TOPIC_DLQ, adapter.name, item.to_dict(), on_delivery=delivery)
        producer.poll(0)
        metrics.maybe_report()

    log.info("shutting down, flushing producer")
    ws_task.cancel()
    remaining = producer.flush(30)
    metrics.maybe_report(force=True)
    producer.flush(5)
    if remaining:
        log.error("%d messages were not delivered", remaining)


if __name__ == "__main__":
    logging.basicConfig(level=config.LOG_LEVEL, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    asyncio.run(main())
