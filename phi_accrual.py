"""
Phi Accrual Failure Detector (Hayashibara et al., 2004).

Instead of a hardcoded "N missed heartbeats = dead" timeout, this fits the
recent inter-arrival time distribution per monitored node and outputs a
continuous suspicion level phi(t):

    phi(t) = -log10( P_later(t - t_last) )

where P_later(delta) is, under the node's own observed heartbeat jitter, the
probability that a gap at least this long could still occur naturally. Larger
phi => more improbable => more confidently dead. This adapts automatically to
each node's own network/GC jitter and to overall cluster load, eliminating the
false positives a fixed timeout produces under transient slowness.
"""

from __future__ import annotations
import math
import time
from collections import deque
from dataclasses import dataclass, field


def _std_normal_cdf(x: float) -> float:
    return 0.5 * (1.0 + math.erf(x / math.sqrt(2.0)))


@dataclass
class PhiAccrualFailureDetector:
    window_size: int = 200
    min_std_dev_ms: float = 50.0     # floor to avoid over-confident phi when jitter ~0
    suspect_threshold: float = 8.0
    dead_threshold: float = 12.0

    _last_heartbeat_ms: float | None = field(default=None, init=False)
    _intervals_ms: deque[float] = field(default_factory=lambda: deque(maxlen=200), init=False)

    def __post_init__(self):
        self._intervals_ms = deque(maxlen=self.window_size)

    def heartbeat(self, now_ms: float | None = None) -> None:
        now_ms = now_ms if now_ms is not None else time.time() * 1000.0
        if self._last_heartbeat_ms is not None:
            interval = now_ms - self._last_heartbeat_ms
            if interval > 0:
                self._intervals_ms.append(interval)
        self._last_heartbeat_ms = now_ms

    def _mean_std(self) -> tuple[float, float]:
        n = len(self._intervals_ms)
        if n == 0:
            return 1000.0, self.min_std_dev_ms  # no data yet: assume 1s cadence
        mean = sum(self._intervals_ms) / n
        if n == 1:
            return mean, self.min_std_dev_ms
        var = sum((x - mean) ** 2 for x in self._intervals_ms) / n
        std = max(math.sqrt(var), self.min_std_dev_ms)
        return mean, std

    def phi(self, now_ms: float | None = None) -> float:
        if self._last_heartbeat_ms is None:
            return 0.0
        now_ms = now_ms if now_ms is not None else time.time() * 1000.0
        elapsed = now_ms - self._last_heartbeat_ms
        mean, std = self._mean_std()

        # P_later(elapsed) = P(X > elapsed) under Normal(mean, std) fit to
        # observed inter-arrival times.
        z = (elapsed - mean) / std
        p_later = 1.0 - _std_normal_cdf(z)
        p_later = max(p_later, 1e-15)  # avoid log(0)
        return -math.log10(p_later)

    def status(self, now_ms: float | None = None) -> str:
        p = self.phi(now_ms)
        if p >= self.dead_threshold:
            return "DOWN"
        if p >= self.suspect_threshold:
            return "SUSPECT"
        return "ALIVE"


if __name__ == "__main__":
    fd = PhiAccrualFailureDetector()
    t = 0.0
    # 20 heartbeats at ~200ms with small jitter
    import random
    random.seed(1)
    for _ in range(20):
        t += 200 + random.uniform(-10, 10)
        fd.heartbeat(t)

    print("phi right after last heartbeat:", round(fd.phi(t), 3), fd.status(t))
    print("phi after 400ms silence:", round(fd.phi(t + 400), 3), fd.status(t + 400))
    print("phi after 1200ms silence:", round(fd.phi(t + 1200), 3), fd.status(t + 1200))
    print("phi after 3000ms silence:", round(fd.phi(t + 3000), 3), fd.status(t + 3000))
