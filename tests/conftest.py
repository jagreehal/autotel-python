"""Pytest configuration and fixtures."""

from contextlib import suppress
from typing import Any

import pytest
from opentelemetry import _logs, metrics, trace
from opentelemetry.sdk._logs import LoggerProvider
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.trace import TracerProvider


@pytest.fixture(autouse=True)
def clean_otel() -> Any:
    """Reset OpenTelemetry state between tests."""
    # Reset global state BEFORE test runs
    import autotel.init as init_module

    init_module._INITIALIZED = False  # noqa: SLF001
    _reset_metrics_and_logs()

    yield

    # Cleanup AFTER test runs - flush and shutdown existing provider
    try:
        # Get current provider and shutdown/flush it
        current_provider = trace.get_tracer_provider()
        if isinstance(current_provider, TracerProvider):
            with suppress(Exception):
                # Force flush all spans before shutdown
                if hasattr(current_provider, "_span_processors"):
                    for processor in current_provider._span_processors:  # noqa: SLF001
                        with suppress(Exception):
                            processor.force_flush(timeout_millis=1000)
                current_provider.shutdown()

        # Force reset provider using internal API (for testing only)
        # OpenTelemetry doesn't allow overriding, so we need to use internal state
        # We clear the internal state but DON'T set a new provider
        # Let the next test's fixture do that
        with suppress(Exception):
            # Clear the internal provider state to allow re-initialization
            if hasattr(trace, "_TRACER_PROVIDER"):
                trace._TRACER_PROVIDER = None  # noqa: SLF001

        # Reset initialization flag
        init_module._INITIALIZED = False  # noqa: SLF001
        _reset_metrics_and_logs()
    except Exception:
        # If reset fails, that's okay for tests
        pass


def _reset_metrics_and_logs() -> None:
    """Reset OTel metrics/logs globals so tests can reinitialize cleanly."""
    with suppress(Exception):
        current_meter_provider = metrics.get_meter_provider()
        if isinstance(current_meter_provider, MeterProvider):
            current_meter_provider.shutdown()
    with suppress(Exception):
        current_logger_provider = _logs.get_logger_provider()
        if isinstance(current_logger_provider, LoggerProvider):
            current_logger_provider.shutdown()  # type: ignore[no-untyped-call]

    with suppress(Exception):
        metrics._internal._METER_PROVIDER = None  # noqa: SLF001
        metrics._internal._METER_PROVIDER_SET_ONCE = metrics._internal.Once()  # noqa: SLF001
        metrics._internal._PROXY_METER_PROVIDER._real_meter_provider = None  # noqa: SLF001
        metrics._internal._PROXY_METER_PROVIDER._meters.clear()  # noqa: SLF001
    with suppress(Exception):
        _logs._internal._LOGGER_PROVIDER = None  # noqa: SLF001
        _logs._internal._LOGGER_PROVIDER_SET_ONCE = _logs._internal.Once()  # noqa: SLF001
