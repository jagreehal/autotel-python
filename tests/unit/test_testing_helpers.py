"""Tests for testing helper utilities."""

from typing import Any

import pytest

from autotel import init, span
from autotel.exporters import InMemorySpanExporter
from autotel.processors import SimpleSpanProcessor
from autotel.testing import (
    assert_no_errors,
    assert_trace_created,
    assert_trace_duration,
    assert_trace_failed,
    assert_trace_succeeded,
    get_span_attribute,
    get_trace_duration,
)


@pytest.fixture
def exporter() -> Any:
    """Create in-memory exporter for testing."""
    exp = InMemorySpanExporter()
    init(service="test", span_processor=SimpleSpanProcessor(exp))
    return exp


def test_assert_trace_created(exporter: Any) -> None:
    """Test assert_trace_created helper."""
    with span("test.operation"):
        pass

    assert_trace_created(exporter, "test.operation")

    with pytest.raises(AssertionError):
        assert_trace_created(exporter, "nonexistent.operation")


def test_assert_trace_succeeded(exporter: Any) -> None:
    """Test assert_trace_succeeded helper."""
    with span("test.success"):
        pass

    assert_trace_succeeded(exporter, "test.success")


def test_assert_trace_failed(exporter: Any) -> None:
    """Test assert_trace_failed helper."""
    from opentelemetry.trace import StatusCode

    with span("test.error") as ctx:
        ctx.set_status(StatusCode.ERROR, "Test error")

    assert_trace_failed(exporter, "test.error")


def test_assert_no_errors(exporter: Any) -> None:
    """Test assert_no_errors helper."""
    with span("test.success"):
        pass

    assert_no_errors(exporter)

    # Add an error span
    from opentelemetry.trace import StatusCode

    with span("test.error") as ctx:
        ctx.set_status(StatusCode.ERROR, "Error")

    with pytest.raises(AssertionError):
        assert_no_errors(exporter)


def test_get_trace_duration(exporter: Any) -> None:
    """Test get_trace_duration helper."""
    import time

    with span("test.duration"):
        time.sleep(0.1)

    duration = get_trace_duration(exporter, "test.duration")
    assert duration is not None
    assert duration >= 100  # At least 100ms

    # Non-existent span
    assert get_trace_duration(exporter, "nonexistent") is None


def test_assert_trace_duration(exporter: Any) -> None:
    """Test assert_trace_duration helper."""
    import time

    with span("test.fast"):
        time.sleep(0.05)

    assert_trace_duration(exporter, "test.fast", max_duration_ms=200)

    with pytest.raises(AssertionError):
        assert_trace_duration(exporter, "test.fast", max_duration_ms=10)


def test_get_span_attribute(exporter: Any) -> None:
    """Test get_span_attribute helper."""
    with span("test.attr") as ctx:
        ctx.set_attribute("test.key", "test.value")

    value = get_span_attribute(exporter, "test.attr", "test.key")
    assert value == "test.value"

    # Non-existent attribute
    assert get_span_attribute(exporter, "test.attr", "nonexistent") is None

    # Non-existent span
    assert get_span_attribute(exporter, "nonexistent", "key") is None
