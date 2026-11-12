"""Lightweight in-process metrics, published every N seconds to the `pipeline.metrics` topic."""
from __future__ import annotations

import logging
import socket
import time
from typing import Callable

from app import config
from app.kafka_io import produce_json
from app.models import now_ms

log = logging.getLogger(__name__)


def percentile(sorted_values: list[float], p: float) -> float | None:
    if not sorted_values:
        return None
    k = (len(sorted_values) - 1) * p
    lo = int(k)
    hi = min(lo + 1, len(sorted_values) - 1)
    return sorted_values[lo] + (sorted_values[hi] - sorted_values[lo]) * (k - lo)


class MetricsReporter:
    def __init__(self, service: str, producer, extra: Callable[[], dict] | None = None,
                 interval: float = config.METRICS_INTERVAL_SECONDS) -> None:
        self.service = service
        self.instance = socket.gethostname()
        self.producer = producer
        self.extra = extra
        self.interval = interval
        self._reset(time.monotonic())
        self.total_in = 0
        self.total_out = 0

    def _reset(self, t: float) -> None:
        self.window_start = t
        self.msgs_in = 0
        self.msgs_out = 0
        self.bytes_in = 0
        self.latencies: dict[str, list[float]] = {}

    def incr_in(self, n: int = 1, nbytes: int = 0) -> None:
        self.msgs_in += n
        self.total_in += n
        self.bytes_in += nbytes

    def incr_out(self, n: int = 1) -> None:
        self.msgs_out += n
        self.total_out += n

    def observe(self, name: str, value_ms: float) -> None:
        bucket = self.latencies.setdefault(name, [])
        if len(bucket) < 200_000:
            bucket.append(value_ms)

    def maybe_report(self, force: bool = False) -> None:
        t = time.monotonic()
        elapsed = t - self.window_start
        if not force and elapsed < self.interval:
            return
        elapsed = max(elapsed, 1e-6)
        lat = {}
        for name, values in self.latencies.items():
            values.sort()
            lat[name] = {"p50": percentile(values, 0.50), "p95": percentile(values, 0.95),
                         "p99": percentile(values, 0.99), "max": values[-1] if values else None,
                         "n": len(values)}
        payload = {
            "service": self.service, "instance": self.instance, "ts": now_ms(),
            "window_s": round(elapsed, 3),
            "msgs_in": self.msgs_in, "msgs_out": self.msgs_out,
            "rate_in": round(self.msgs_in / elapsed, 2), "rate_out": round(self.msgs_out / elapsed, 2),
            "mb_in_per_s": round(self.bytes_in / elapsed / 1e6, 4),
            "total_in": self.total_in, "total_out": self.total_out,
            "latency_ms": lat,
            "extra": self.extra() if self.extra else {},
        }
        try:
            produce_json(self.producer, config.TOPIC_METRICS, self.service, payload)
        except Exception:  # metrics must never break the pipeline
            log.exception("could not publish metrics")
        p95 = {k: round(v["p95"], 1) for k, v in lat.items() if v["p95"] is not None}
        log.info("[metrics] %s in=%.1f/s out=%.1f/s p95=%s extra=%s",
                 self.service, payload["rate_in"], payload["rate_out"], p95, payload["extra"])
        self._reset(t)
