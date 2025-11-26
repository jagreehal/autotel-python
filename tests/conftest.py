"""Pytest configuration and fixtures."""

from contextlib import suppress
from typing import Any

import pytest
from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider


@pytest.fixture(autouse=True)
def clean_otel() -> Any:
    """Reset OpenTelemetry state between tests."""
    # Reset global state BEFORE test runs
    import autotel.init as init_module

    init_module._INITIALIZED = False  # noqa: SLF001

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
    except Exception:
        # If reset fails, that's okay for tests
        pass
