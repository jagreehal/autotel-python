"""Python runtime metrics, the counterpart of autotel-rust's Tokio metrics."""

from __future__ import annotations

import gc
import os
import sys
import threading
from collections.abc import Iterable

from opentelemetry import metrics
from opentelemetry.metrics import CallbackOptions, Observation


def _gc_collections(_: CallbackOptions) -> Iterable[Observation]:
    for generation, stats in enumerate(gc.get_stats()):
        yield Observation(stats["collections"], {"cpython.gc.generation": generation})


def _cpu_time(_: CallbackOptions) -> Iterable[Observation]:
    times = os.times()
    yield Observation(times.user, {"cpu.mode": "user"})
    yield Observation(times.system, {"cpu.mode": "system"})


def _threads(_: CallbackOptions) -> Iterable[Observation]:
    yield Observation(threading.active_count())


def _peak_memory(_: CallbackOptions) -> Iterable[Observation]:
    import resource

    peak = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    yield Observation(peak if sys.platform == "darwin" else peak * 1024)


def register_runtime_metrics() -> None:
    """Report GC, CPU, thread and peak-memory metrics. Call once, after ``init``."""
    meter = metrics.get_meter("autotel")
    meter.create_observable_counter(
        "cpython.gc.collections", [_gc_collections], unit="{collection}"
    )
    meter.create_observable_counter("process.cpu.time", [_cpu_time], unit="s")
    meter.create_observable_up_down_counter("process.thread.count", [_threads], unit="{thread}")
    if sys.platform != "win32":
        meter.create_observable_gauge("process.memory.peak", [_peak_memory], unit="By")
