"""Kafka producer/consumer factories with reliability-focused defaults."""
from __future__ import annotations

import json
import logging
import socket

from confluent_kafka import Consumer, Producer

from app import config

log = logging.getLogger(__name__)


def make_producer(client_id: str, **overrides) -> Producer:
    conf = {
        "bootstrap.servers": config.KAFKA_BOOTSTRAP,
        "client.id": f"{client_id}-{socket.gethostname()}",
        # Reliability: wait for all in-sync replicas, no duplicates on retry, keep order.
        "acks": "all",
        "enable.idempotence": True,
        "max.in.flight.requests.per.connection": 5,
        "delivery.timeout.ms": 120_000,
        # Throughput: small batching window + compression.
        "linger.ms": 5,
        "batch.size": 262_144,
        "compression.type": "lz4",
        "queue.buffering.max.messages": 500_000,
    }
    conf.update(overrides)
    return Producer(conf)


def make_consumer(group_id: str, **overrides) -> Consumer:
    conf = {
        "bootstrap.servers": config.KAFKA_BOOTSTRAP,
        "group.id": group_id,
        "client.id": f"{group_id}-{socket.gethostname()}",
        # We commit manually only after results are safely written -> at-least-once.
        "enable.auto.commit": False,
        "auto.offset.reset": "earliest",
        "session.timeout.ms": 10_000,
        "heartbeat.interval.ms": 3_000,
        "max.poll.interval.ms": 300_000,
        "fetch.min.bytes": 1,
        "fetch.wait.max.ms": 50,
    }
    conf.update(overrides)
    return Consumer(conf)


class DeliveryStats:
    """Delivery-report callback that counts acked / failed messages."""

    def __init__(self) -> None:
        self.delivered = 0
        self.failed = 0

    def __call__(self, err, msg) -> None:
        if err is not None:
            self.failed += 1
            log.error("delivery failed topic=%s: %s", msg.topic() if msg else "?", err)
        else:
            self.delivered += 1


def produce_json(producer: Producer, topic: str, key: str | None, value: dict, on_delivery=None) -> None:
    data = json.dumps(value, separators=(",", ":")).encode()
    k = key.encode() if key else None
    while True:
        try:
            producer.produce(topic, key=k, value=data, on_delivery=on_delivery)
            return
        except BufferError:
            # Local queue full: serve delivery reports to make room (back-pressure).
            producer.poll(0.5)
