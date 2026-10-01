from __future__ import annotations

import random
import threading
import time
from abc import ABC, abstractmethod
from typing import Callable, Iterable, Optional

from .models import MAX_PRIORITY, MIN_PRIORITY


# ---------- clock ----------

class Clock(ABC):
    @abstractmethod
    def now(self) -> float:
        ...


class SystemClock(Clock):
    def now(self) -> float:
        return time.monotonic()


class FakeClock(Clock):
    """Manually advanced clock so lease/backoff tests are deterministic."""

    def __init__(self, start: float = 0.0):
        self._now = start
        self._lock = threading.Lock()

    def now(self) -> float:
        with self._lock:
            return self._now

    def advance(self, seconds: float) -> None:
        with self._lock:
            self._now += seconds


# ---------- retry ----------

class RetryPolicy(ABC):
    @abstractmethod
    def next_delay(self, attempt: int) -> float:
        """Delay before the next attempt, given the attempt number (1-based) that just failed."""


class ExponentialBackoff(RetryPolicy):
    """delay = min(cap, base * 2^(attempt-1)) * (1 + U(0, jitter))"""

    def __init__(self, base: float = 1.0, cap: float = 60.0, jitter: float = 0.1,
                 rng: Optional[random.Random] = None):
        self.base = base
        self.cap = cap
        self.jitter = jitter
        self._rng = rng or random.Random()

    def next_delay(self, attempt: int) -> float:
        delay = min(self.cap, self.base * (2 ** (attempt - 1)))
        if self.jitter:
            delay *= 1 + self._rng.uniform(0, self.jitter)
        return delay


# ---------- priority scheduling ----------

class PriorityScheduler(ABC):
    @abstractmethod
    def pick(self, eligible: Iterable[int]) -> int:
        """Choose one priority level among the non-empty eligible levels."""


class WeightedRoundRobinScheduler(PriorityScheduler):
    """Each cycle, priority p is served up to weight(p) = p + 1 times, highest first.

    When every eligible level has used its credits, a new cycle starts. So any
    non-empty level is served at least once per cycle (<= 55 claims): no starvation.
    """

    def __init__(self, weight: Callable[[int], int] = lambda p: p + 1):
        self._weight = weight
        self._credits = self._full_credits()

    def _full_credits(self) -> dict:
        return {p: self._weight(p) for p in range(MIN_PRIORITY, MAX_PRIORITY + 1)}

    def pick(self, eligible: Iterable[int]) -> int:
        eligible = list(eligible)
        candidates = [p for p in eligible if self._credits[p] > 0]
        if not candidates:
            self._credits = self._full_credits()
            candidates = eligible
        chosen = max(candidates)
        self._credits[chosen] -= 1
        return chosen
