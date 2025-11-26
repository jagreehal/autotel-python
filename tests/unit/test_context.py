"""Tests for TraceContext class."""

from typing import Any

import pytest
from opentelemetry.trace import StatusCode

from autotel import init, span
from autotel.exporters import InMemorySpanExporter
from autotel.processors import SimpleSpanProcessor


@pytest.fixture
def exporter() -> Any:
    """Create in-memory exporter for testing."""
    exp = InMemorySpanExporter()
    init(service="test", span_processor=SimpleSpanProcessor(exp))
    return exp


def test_trace_context_set_attribute(exporter: Any) -> None:
    """Test setting attributes on trace context."""
    with span("test.operation") as ctx:
        ctx.set_attribute("test.key", "test.value")
        ctx.set_attribute("test.number", 42)
        ctx.set_attribute("test.bool", True)
        ctx.set_attribute("test.float", 3.14)

    spans = exporter.get_finished_spans()
    assert len(spans) == 1
    assert spans[0].attributes is not None
    assert spans[0].attributes.get("test.key") == "test.value"
    assert spans[0].attributes is not None
    assert spans[0].attributes.get("test.number") == 42
    assert spans[0].attributes is not None
    assert spans[0].attributes.get("test.bool") is True
    assert spans[0].attributes is not None
    assert spans[0].attributes.get("test.float") == 3.14


def test_trace_context_add_event(exporter: Any) -> None:
    """Test adding events to trace context."""
    with span("test.operation") as ctx:
        ctx.add_event("test.event", {"event.key": "event.value"})

    spans = exporter.get_finished_spans()
    assert len(spans) == 1
    # Events are stored in span.events
    assert len(spans[0].events) == 1
    assert spans[0].events[0].name == "test.event"


def test_trace_context_set_status(exporter: Any) -> None:
    """Test setting status on trace context."""
    with span("test.operation") as ctx:
        ctx.set_status(StatusCode.ERROR, "Test error")

    spans = exporter.get_finished_spans()
    assert len(spans) == 1
    assert spans[0].status.status_code == StatusCode.ERROR
    assert spans[0].status.description == "Test error"


def test_trace_context_record_exception(exporter: Any) -> None:
    """Test recording exceptions on trace context."""
    with span("test.operation") as ctx:
        try:
            raise ValueError("Test exception")
        except ValueError as e:
            ctx.record_exception(e)

    spans = exporter.get_finished_spans()
    assert len(spans) == 1
    # Exceptions are recorded as events
    assert len(spans[0].events) > 0


def test_trace_context_span_id(exporter: Any) -> None:  # noqa: ARG001
    """Test getting span ID from trace context."""
    with span("test.operation") as ctx:
        span_id = ctx.span_id
        assert span_id is not None
        assert isinstance(span_id, str)
        assert len(span_id) == 16  # Hex string of 8 bytes


def test_trace_context_trace_id(exporter: Any) -> None:  # noqa: ARG001
    """Test getting trace ID from trace context."""
    with span("test.operation") as ctx:
        trace_id = ctx.trace_id
        assert trace_id is not None
        assert isinstance(trace_id, str)
        assert len(trace_id) == 32  # Hex string of 16 bytes
