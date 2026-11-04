"""Duration histograms from the OpenTelemetry semantic conventions."""

from __future__ import annotations

from collections.abc import Callable, Iterator, Mapping
from contextlib import contextmanager
from functools import lru_cache
from time import perf_counter
from typing import Any

from opentelemetry import metrics
from opentelemetry.metrics import Histogram, MeterProvider

# Boundaries the conventions advise; SDK defaults are tuned for milliseconds.
_SECONDS = [0.005, 0.01, 0.025, 0.05, 0.075, 0.1, 0.25, 0.5, 0.75, 1, 2.5, 5, 7.5, 10]


@lru_cache(maxsize=32)
def _histogram(provider: MeterProvider, name: str, description: str) -> Histogram:
    return provider.get_meter("autotel").create_histogram(
        name, unit="s", description=description, explicit_bucket_boundaries_advisory=_SECONDS
    )


def _record(name: str, description: str, seconds: float, attributes: Mapping[str, Any]) -> None:
    present = {key: value for key, value in attributes.items() if value is not None}
    _histogram(metrics.get_meter_provider(), name, description).record(seconds, present)


def record_http_server(seconds: float, attributes: Mapping[str, Any]) -> None:
    _record("http.server.request.duration", "HTTP server request", seconds, attributes)


def record_http_client(seconds: float, attributes: Mapping[str, Any]) -> None:
    _record("http.client.request.duration", "HTTP client request", seconds, attributes)


def record_db_operation(seconds: float, attributes: Mapping[str, Any]) -> None:
    _record("db.client.operation.duration", "Database client operation", seconds, attributes)


def record_rpc_server(seconds: float, attributes: Mapping[str, Any]) -> None:
    _record("rpc.server.call.duration", "RPC server call", seconds, attributes)


@contextmanager
def measured(
    record: Callable[[float, Mapping[str, Any]], None], attributes: Mapping[str, Any]
) -> Iterator[dict[str, Any]]:
    """Time the block and record it, adding ``error.type`` if it raises.

    Yields the labels, so the block can add what it learns, like a status code.
    """
    labels = dict(attributes)
    started = perf_counter()
    try:
        yield labels
    except Exception as error:
        labels["error.type"] = type(error).__qualname__
        raise
    finally:
        record(perf_counter() - started, labels)
