"""SLO tracking: SLI, burn rate, multi-window alerts and forecasts over recorded outcomes."""

from __future__ import annotations

import math
import time
from collections import deque
from dataclasses import dataclass


@dataclass(frozen=True)
class SloSnapshot:
    name: str
    target: float
    good: int
    bad: int
    sli: float
    burn_rate: float


@dataclass(frozen=True)
class BurnAlert:
    alerting: bool
    short_burn_rate: float
    long_burn_rate: float


@dataclass(frozen=True)
class SloForecast:
    alerting: bool
    projected_sli: float


class SloTracker:
    """Good and bad events inside a sliding window of ``window_seconds``.

    Example:
        >>> slo = SloTracker("checkout", target=0.999, window_seconds=3600)
        >>> slo.record(response.status_code < 500)
        >>> slo.snapshot().burn_rate
    """

    def __init__(self, name: str, target: float, window_seconds: float) -> None:
        if not 0 < target < 1:
            raise ValueError("target must be between 0 and 1, exclusive")
        self.name = name
        self.target = target
        self.window_seconds = window_seconds
        self._events: deque[tuple[float, bool]] = deque()

    def record(self, good: bool) -> None:
        self._events.append((time.monotonic(), good))
        self._prune()

    def snapshot(self) -> SloSnapshot:
        self._prune()
        total = len(self._events)
        good = sum(1 for _, outcome in self._events if outcome)
        bad = total - good
        return SloSnapshot(
            name=self.name,
            target=self.target,
            good=good,
            bad=bad,
            sli=good / total if total else math.nan,
            burn_rate=(bad / total) / (1 - self.target) if total else 0.0,
        )

    def forecast(self, expected_events: int, failure_fraction: float) -> SloForecast:
        """Project the SLI if ``expected_events`` arrive at ``failure_fraction`` failures."""
        current = self.snapshot()
        total = current.good + current.bad + expected_events
        bad = current.bad + round(expected_events * failure_fraction)
        projected = 1 - bad / total if total else math.nan
        return SloForecast(alerting=projected < self.target, projected_sli=projected)

    @staticmethod
    def evaluate(
        short: SloSnapshot, long: SloSnapshot, short_threshold: float, long_threshold: float
    ) -> BurnAlert:
        """Alert when both windows burn too fast, as multi-window SLO alerts do."""
        return BurnAlert(
            alerting=short.burn_rate >= short_threshold and long.burn_rate >= long_threshold,
            short_burn_rate=short.burn_rate,
            long_burn_rate=long.burn_rate,
        )

    def _prune(self) -> None:
        cutoff = time.monotonic() - self.window_seconds
        while self._events and self._events[0][0] < cutoff:
            self._events.popleft()
