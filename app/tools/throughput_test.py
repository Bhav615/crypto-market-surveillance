"""Throughput / scalability benchmark of the Kafka layer using REAL recorded trades.

1. Reads up to --messages real trades already captured in `trades.raw` (live exchange data).
2. Re-publishes them as fast as possible to the separate topic `loadtest.trades`
   (acks=all, idempotent, lz4 – the same settings as the live producer).
3. Consumes them back and reports producer and consumer throughput.
The live topics are not touched, so the benchmark never pollutes the real pipeline.

Run:  docker compose run --rm detector python -m app.tools.throughput_test --messages 50000 --repeat 5
"""
from __future__ import annotations

import argparse
import time
import uuid

from app import config
from app.kafka_io import DeliveryStats, make_consumer, make_producer


def read_real_trades(limit: int, idle_s: float = 10.0) -> list[tuple[bytes, bytes]]:
    c = make_consumer(f"loadtest-reader-{uuid.uuid4()}")
    c.subscribe([config.TOPIC_TRADES])
    out, last = [], time.time()
    while len(out) < limit and time.time() - last < idle_s:
        for m in c.consume(1000, 1.0):
            if m.error() is None:
                out.append((m.key(), m.value()))
                last = time.time()
    c.close()
    return out[:limit]


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--messages", type=int, default=50_000, help="real trades to read from trades.raw")
    ap.add_argument("--repeat", type=int, default=5, help="replay the recorded set this many times")
    args = ap.parse_args()

    print(f"Reading up to {args.messages:,} real trades from '{config.TOPIC_TRADES}' ...")
    records = read_real_trades(args.messages)
    if len(records) < 1000:
        raise SystemExit(f"Only {len(records)} trades recorded so far – let the pipeline run a few minutes first.")
    total = len(records) * args.repeat
    nbytes = sum(len(v) for _, v in records) * args.repeat
    print(f"Loaded {len(records):,} trades (avg {nbytes / total:.0f} bytes); publishing {total:,} messages")

    delivery = DeliveryStats()
    p = make_producer("loadtest")
    t0 = time.perf_counter()
    for _ in range(args.repeat):
        for k, v in records:
            while True:
                try:
                    p.produce(config.TOPIC_LOADTEST, key=k, value=v, on_delivery=delivery)
                    break
                except BufferError:
                    p.poll(0.1)
            p.poll(0)
    p.flush(120)
    t_prod = time.perf_counter() - t0

    c = make_consumer(f"loadtest-consumer-{uuid.uuid4()}")
    c.subscribe([config.TOPIC_LOADTEST])
    got, t1, started = 0, None, time.time()
    while got < total and time.time() - started < 180:
        batch = c.consume(5000, 1.0)
        if batch and t1 is None:
            t1 = time.perf_counter()
        got += sum(1 for m in batch if m.error() is None)
    t_cons = time.perf_counter() - (t1 or time.perf_counter())
    c.close()

    print("\n================ Kafka throughput benchmark (real trade payloads) ================")
    print(f"Producer : {delivery.delivered:,} acked, {delivery.failed} failed in {t_prod:.2f}s "
          f"-> {delivery.delivered / t_prod:,.0f} msg/s, {nbytes / t_prod / 1e6:.2f} MB/s (acks=all, RF=3)")
    print(f"Consumer : {got:,} consumed in {t_cons:.2f}s -> {got / max(t_cons, 1e-6):,.0f} msg/s")
    print("Note: the consumer total also includes messages from earlier benchmark runs (topic retention 1h).")


if __name__ == "__main__":
    main()
