"""Tests for trace helper functions."""

from typing import Any

import pytest
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.trace import StatusCode

from autotel import (
    create_deterministic_trace_id,
    finalize_span,
    flatten_metadata,
    get_tracer,
    init,
    span,
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


# ============================================================================
# flatten_metadata tests
# ============================================================================


def test_flatten_metadata_basic() -> None:
    """Test flattening simple metadata."""
    metadata = {"user_id": "123", "user_tier": "premium", "items": 5}

    flattened = flatten_metadata(metadata)

    assert flattened == {
        "metadata.user_id": "123",
        "metadata.user_tier": "premium",
        "metadata.items": "5",  # Number converted to JSON string
    }


def test_flatten_metadata_nested() -> None:
    """Test flattening nested metadata."""
    metadata = {
        "user": {"id": "123", "tier": "premium"},
        "payment": {"method": "card", "processor": "stripe"},
    }

    flattened = flatten_metadata(metadata)

    # Nested objects are flattened into dot notation
    assert flattened["metadata.user.id"] == "123"
    assert flattened["metadata.user.tier"] == "premium"
    assert flattened["metadata.payment.method"] == "card"
    assert flattened["metadata.payment.processor"] == "stripe"


def test_flatten_metadata_with_prefix() -> None:
    """Test flattening with custom prefix."""
    metadata = {"key1": "value1", "key2": "value2"}

    flattened = flatten_metadata(metadata, prefix="custom")

    assert flattened == {"custom.key1": "value1", "custom.key2": "value2"}


def test_flatten_metadata_skips_none() -> None:
    """Test that None values are skipped."""
    metadata = {"key1": "value1", "key2": None, "key3": "value3"}

    flattened = flatten_metadata(metadata)

    assert "metadata.key1" in flattened
    assert "metadata.key2" not in flattened  # None should be skipped
    assert "metadata.key3" in flattened


def test_flatten_metadata_handles_lists() -> None:
    """Test flattening metadata with lists."""
    metadata = {"tags": ["python", "opentelemetry", "observability"], "counts": [1, 2, 3]}

    flattened = flatten_metadata(metadata)

    # Lists are JSON serialized
    assert "metadata.tags" in flattened
    assert "python" in flattened["metadata.tags"]
    assert "metadata.counts" in flattened


def test_flatten_metadata_handles_booleans() -> None:
    """Test flattening metadata with booleans."""
    metadata = {"is_active": True, "is_premium": False}

    flattened = flatten_metadata(metadata)

    assert flattened["metadata.is_active"] == "true"  # JSON serialized
    assert flattened["metadata.is_premium"] == "false"


def test_flatten_metadata_complex() -> None:
    """Test flattening complex metadata structure."""
    metadata = {
        "user": {"id": "123", "tier": "premium"},
        "payment": {"method": "card", "processor": "stripe"},
        "items": 5,
        "active": True,
        "tags": ["important", "urgent"],
        "null_field": None,
    }

    flattened = flatten_metadata(metadata)

    # All fields except None should be present
    assert len(flattened) == 7  # Excluding null_field
    assert flattened["metadata.user.id"] == "123"
    assert flattened["metadata.user.tier"] == "premium"
    assert flattened["metadata.payment.method"] == "card"
    assert flattened["metadata.payment.processor"] == "stripe"
    assert flattened["metadata.items"] == "5"
    assert flattened["metadata.active"] == "true"
    assert "metadata.tags" in flattened
    assert "metadata.null_field" not in flattened


def test_flatten_metadata_with_span(setup_tracing: Any) -> None:
    """Test using flattened metadata with a span."""
    from autotel import set_attributes

    exporter = setup_tracing

    metadata = {"user_id": "123", "role": "admin", "priority": 1}

    with span("test"):
        flattened = flatten_metadata(metadata)
        set_attributes(flattened)

    spans = exporter.get_finished_spans()
    assert len(spans) == 1

    attrs = spans[0].attributes
    assert attrs["metadata.user_id"] == "123"
    assert attrs["metadata.role"] == "admin"
    assert attrs["metadata.priority"] == "1"


# ============================================================================
# create_deterministic_trace_id tests
# ============================================================================


def test_create_deterministic_trace_id_basic() -> None:
    """Test creating deterministic trace ID."""
    trace_id = create_deterministic_trace_id("test-seed")

    # Should be 32 hex characters (128 bits)
    assert len(trace_id) == 32
    assert all(c in "0123456789abcdef" for c in trace_id)


def test_create_deterministic_trace_id_consistency() -> None:
    """Test that same seed produces same trace ID."""
    seed = "TICKET-12345"

    trace_id1 = create_deterministic_trace_id(seed)
    trace_id2 = create_deterministic_trace_id(seed)

    assert trace_id1 == trace_id2


def test_create_deterministic_trace_id_different_seeds() -> None:
    """Test that different seeds produce different trace IDs."""
    trace_id1 = create_deterministic_trace_id("seed1")
    trace_id2 = create_deterministic_trace_id("seed2")

    assert trace_id1 != trace_id2


def test_create_deterministic_trace_id_with_various_inputs() -> None:
    """Test creating trace IDs from various input types."""
    # Order ID
    order_trace_id = create_deterministic_trace_id("ORDER-67890")
    assert len(order_trace_id) == 32

    # Session ID
    session_trace_id = create_deterministic_trace_id("session_abc123")
    assert len(session_trace_id) == 32

    # Request ID
    request_trace_id = create_deterministic_trace_id("req-uuid-1234-5678")
    assert len(request_trace_id) == 32

    # All should be different
    assert order_trace_id != session_trace_id != request_trace_id


def test_create_deterministic_trace_id_unicode() -> None:
    """Test creating trace ID from unicode strings."""
    unicode_seed = "用户-12345"
    trace_id = create_deterministic_trace_id(unicode_seed)

    assert len(trace_id) == 32
    assert all(c in "0123456789abcdef" for c in trace_id)


# ============================================================================
# finalize_span tests
# ============================================================================


def test_finalize_span_success(setup_tracing: Any) -> None:
    """Test finalizing span without error."""
    exporter = setup_tracing

    tracer = get_tracer(__name__)
    test_span = tracer.start_span("test.operation")

    # Finalize without error
    finalize_span(test_span)

    spans = exporter.get_finished_spans()
    assert len(spans) == 1

    finished_span = spans[0]
    assert finished_span.name == "test.operation"
    assert finished_span.status.status_code == StatusCode.OK


def test_finalize_span_with_error(setup_tracing: Any) -> None:
    """Test finalizing span with error."""
    exporter = setup_tracing

    tracer = get_tracer(__name__)
    test_span = tracer.start_span("test.operation")

    # Create an error
    error = ValueError("Test error message")

    # Finalize with error
    finalize_span(test_span, error)

    spans = exporter.get_finished_spans()
    assert len(spans) == 1

    finished_span = spans[0]
    assert finished_span.name == "test.operation"
    assert finished_span.status.status_code == StatusCode.ERROR
    assert "Test error message" in finished_span.status.description

    # Verify exception was recorded
    assert len(finished_span.events) >= 1
    exception_event = finished_span.events[0]
    assert exception_event.name == "exception"


def test_finalize_span_in_try_except(setup_tracing: Any) -> None:
    """Test finalize_span pattern in try-except block."""
    exporter = setup_tracing

    tracer = get_tracer(__name__)
    test_span = tracer.start_span("risky.operation")

    try:
        # Simulate some work
        raise RuntimeError("Something went wrong")
    except RuntimeError as e:
        finalize_span(test_span, e)

    spans = exporter.get_finished_spans()
    assert len(spans) == 1

    finished_span = spans[0]
    assert finished_span.status.status_code == StatusCode.ERROR
    assert "Something went wrong" in finished_span.status.description


def test_finalize_span_success_pattern(setup_tracing: Any) -> None:
    """Test finalize_span success pattern."""
    exporter = setup_tracing

    tracer = get_tracer(__name__)
    test_span = tracer.start_span("safe.operation")

    try:
        # Simulate successful work
        finalize_span(test_span)
    except Exception as e:
        finalize_span(test_span, e)
        raise

    spans = exporter.get_finished_spans()
    assert len(spans) == 1

    finished_span = spans[0]
    assert finished_span.status.status_code == StatusCode.OK


def test_finalize_span_multiple_calls() -> None:
    """Test that finalize_span can be called on multiple spans."""
    from opentelemetry.sdk.trace import TracerProvider

    provider = TracerProvider()
    tracer = provider.get_tracer(__name__)

    span1 = tracer.start_span("operation1")
    span2 = tracer.start_span("operation2")

    finalize_span(span1)
    finalize_span(span2, ValueError("Error in operation2"))

    # Both spans should be finalized without errors
    # (We're just verifying no exceptions are raised)


# ============================================================================
# Integration tests
# ============================================================================


def test_trace_helpers_integration(setup_tracing: Any) -> None:
    """Test using multiple trace helpers together."""
    from autotel import set_attributes

    exporter = setup_tracing

    # Create metadata
    metadata = {
        "user": {"id": "123", "tier": "premium"},
        "request": {"method": "POST", "path": "/api/users"},
    }

    # Flatten it
    flattened = flatten_metadata(metadata, prefix="request")

    # Create deterministic trace ID for correlation
    ticket_id = "SUPPORT-98765"
    trace_id = create_deterministic_trace_id(ticket_id)

    with span("integration.test") as s:
        # Add flattened metadata
        set_attributes(flattened)

        # Add trace ID correlation
        s.set_attribute("support.ticket_id", ticket_id)
        s.set_attribute("support.trace_id", trace_id)

    spans = exporter.get_finished_spans()
    assert len(spans) == 1

    attrs = spans[0].attributes
    assert attrs["request.user.id"] == "123"
    assert attrs["request.user.tier"] == "premium"
    assert attrs["support.ticket_id"] == ticket_id
    assert attrs["support.trace_id"] == trace_id

    # Verify trace ID is consistent
    assert create_deterministic_trace_id(ticket_id) == trace_id
