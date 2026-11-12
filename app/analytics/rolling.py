"""O(1) rolling mean / standard deviation over the last N observations."""
from __future__ import annotations

import math
import statistics
from collections import deque


class RollingStats:
    def __init__(self, maxlen: int) -> None:
        self.maxlen = maxlen
        self.buf: deque[float] = deque()
        self._sum = 0.0
        self._sumsq = 0.0
        self._adds = 0

    def add(self, x: float) -> None:
        if len(self.buf) == self.maxlen:
            old = self.buf.popleft()
            self._sum -= old
            self._sumsq -= old * old
        self.buf.append(x)
        self._sum += x
        self._sumsq += x * x
        self._adds += 1
        if self._adds % (self.maxlen * 20) == 0:  # avoid floating-point drift
            self._sum = math.fsum(self.buf)
            self._sumsq = math.fsum(v * v for v in self.buf)

    @property
    def n(self) -> int:
        return len(self.buf)

    @property
    def mean(self) -> float:
        return self._sum / self.n if self.n else 0.0

    @property
    def std(self) -> float:
        n = self.n
        if n < 2:
            return 0.0
        var = (self._sumsq - self._sum * self._sum / n) / (n - 1)
        return math.sqrt(var) if var > 0 else 0.0

    def zscore(self, x: float) -> float | None:
        s = self.std
        if self.n < 2 or s <= 1e-12:
            return None
        return (x - self.mean) / s

    def median(self) -> float:
        return statistics.median(self.buf) if self.buf else 0.0
