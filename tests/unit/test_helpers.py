"""Tests for convenience helper functions."""

from typing import Any

import pytest
from opentelemetry import context as otel_context
from opentelemetry.sdk.trace.export import SimpleSpanProcessor

from autotel import (
    add_event,
    get_all_baggage,
    get_baggage,
    get_span_id,
    get_trace_id,
    init,
    record_exception,
    set_attribute,
    set_attributes,
    set_baggage_value,
    span,
    with_baggage,
)
from autotel.exporters import InMemorySpanExporter


@pytest.fixture
def setup_tracing() -> Any:
    """Setup test tracing."""
    exporter = InMemorySpanExporter()
    processor = SimpleSpanProcessor(exporter)

    init(service="test", span_processor=processor)
    yield exporter
    exporter.clear()


def test_set_attributes(setup_tracing: Any) -> None:
    """Test setting multiple attributes at once."""
    exporter = setup_tracing

    with span("test"):
        set_attributes(
            {
                "user.id": "123",
                "user.role": "admin",
                "request.size": 1024,
                "is_premium": True,
            }
        )

    spans = exporter.get_finished_spans()
    assert len(spans) == 1
    attrs = spans[0].attributes

    assert attrs["user.id"] == "123"
    assert attrs["user.role"] == "admin"
    assert attrs["request.size"] == 1024
    assert attrs["is_premium"] is True


def test_set_attribute(setup_tracing: Any) -> None:
    """Test setting a single attribute."""
    exporter = setup_tracing

    with span("test"):
        set_attribute("operation.type", "query")

    spans = exporter.get_finished_spans()
    assert len(spans) == 1
    assert spans[0].attributes is not None
    assert spans[0].attributes.get("operation.type") == "query"


def test_add_event_helper(setup_tracing: Any) -> None:
    """Test adding an event."""
    exporter = setup_tracing

    with span("test"):
        add_event("user.action", {"action": "click", "target": "button"})

    spans = exporter.get_finished_spans()
    assert len(spans) == 1
    events = spans[0].events
    assert len(events) == 1
    assert events[0].name == "user.action"
    assert events[0].attributes is not None
    assert events[0].attributes.get("action") == "click"


def test_record_exception_helper(setup_tracing: Any) -> None:
    """Test recording an exception."""
    exporter = setup_tracing

    with span("test"):
        try:
            raise ValueError("Test error")
        except ValueError as e:
            record_exception(e, {"context": "test"})

    spans = exporter.get_finished_spans()
    assert len(spans) == 1
    events = spans[0].events
    # Should have one event for the exception
    assert len(events) >= 1
    exception_event = events[0]
    assert exception_event.name == "exception"
    assert "exception.type" in exception_event.attributes


def test_get_trace_id(setup_tracing: Any) -> None:
    """Test getting trace ID."""
    _ = setup_tracing
    trace_id = None
    with span("test"):
        trace_id = get_trace_id()

    assert trace_id is not None
    assert isinstance(trace_id, str)
    assert len(trace_id) == 32  # Hex string format


def test_get_span_id(setup_tracing: Any) -> None:
    """Test getting span ID."""
    _ = setup_tracing
    span_id = None
    with span("test"):
        span_id = get_span_id()

    assert span_id is not None
    assert isinstance(span_id, str)
    assert len(span_id) == 16  # Hex string format


def test_get_baggage_helper(setup_tracing: Any) -> None:
    _ = setup_tracing
    """Test getting baggage value."""
    with with_baggage({"tenant.id": "tenant-123"}):
        value = get_baggage("tenant.id")
        assert value == "tenant-123"

        missing = get_baggage("nonexistent")
        assert missing is None


def test_get_all_baggage_helper(setup_tracing: Any) -> None:
    _ = setup_tracing
    """Test getting all baggage."""
    with with_baggage({"tenant.id": "tenant-123", "user.id": "user-456"}):
        baggage = get_all_baggage()
        assert baggage == {"tenant.id": "tenant-123", "user.id": "user-456"}


def test_set_baggage_value_helper(setup_tracing: Any) -> None:
    _ = setup_tracing
    """Test setting baggage value."""
    with span("test"):
        token = set_baggage_value("request.id", "req-789")
        value = get_baggage("request.id")
        assert value == "req-789"
        otel_context.detach(token)


def test_helpers_with_no_active_span() -> None:
    """Test that helpers don't crash when no span is active."""
    # Should not raise exceptions
    set_attributes({"key": "value"})
    set_attribute("key", "value")
    add_event("test.event")

    trace_id = get_trace_id()
    assert trace_id is None

    span_id = get_span_id()
    assert span_id is None


def test_attribute_type_conversion(setup_tracing: Any) -> None:
    """Test that unsupported attribute types are converted to strings."""
    exporter = setup_tracing

    with span("test"):
        set_attributes(
            {
                "string": "value",
                "int": 123,
                "float": 45.67,
                "bool": True,
                "list": [1, 2, 3],
                "dict": {"key": "value"},  # Should convert to string
            }
        )

    spans = exporter.get_finished_spans()
    assert len(spans) == 1
    attrs = spans[0].attributes

    assert attrs["string"] == "value"
    assert attrs["int"] == 123
    assert attrs["float"] == 45.67
    assert attrs["bool"] is True
    assert attrs["list"] == (1, 2, 3)  # OTEL converts to tuple
    assert attrs["dict"] == "{'key': 'value'}"  # Converted to string
