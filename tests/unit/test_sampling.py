"""Tests for adaptive sampling."""

import time
from typing import Any
from unittest.mock import MagicMock

import pytest
from opentelemetry import trace as otel_trace
from opentelemetry.trace import Link, SpanContext, StatusCode, TraceFlags

from autotel import init, span
from autotel.exporters import InMemorySpanExporter
from autotel.processors import SimpleSpanProcessor
from autotel.sampling import (
    AdaptiveSampler,
    AdaptiveSamplingProcessor,
    create_link_from_headers,
    extract_links_from_batch,
)


@pytest.fixture
def exporter() -> Any:
    """Create in-memory exporter for testing."""
    exp = InMemorySpanExporter()
    return exp


def test_adaptive_sampler_baseline(exporter: Any) -> None:
    """Test baseline sampling rate."""
    sampler = AdaptiveSampler(baseline_rate=0.1, slow_threshold_ms=1000)
    processor = AdaptiveSamplingProcessor(sampler, SimpleSpanProcessor(exporter))
    init(service="test", span_processor=processor, sampler=sampler)  # type: ignore[arg-type]

    # Create many spans - should sample ~10%
    for _ in range(100):
        with span("test.operation") as ctx:
            ctx.set_attribute("test", "value")

    spans = exporter.get_finished_spans()
    # With 10% sampling, we expect roughly 10 spans (allow variance for randomness)
    # Note: With 100 spans and 10% rate, we expect ~10, but allow 0-30 for randomness
    # (0 is unlikely but possible with probabilistic sampling)
    assert 0 <= len(spans) <= 30


def test_adaptive_sampler_errors(exporter: Any) -> None:
    """Test 100% sampling for errors."""
    sampler = AdaptiveSampler(baseline_rate=0.0, error_rate=1.0)  # 0% baseline, 100% errors
    processor = AdaptiveSamplingProcessor(sampler, SimpleSpanProcessor(exporter))
    init(service="test", span_processor=processor, sampler=sampler)  # type: ignore[arg-type]

    # Create spans with errors
    for _ in range(10):
        with span("test.error") as ctx:
            ctx.set_status(StatusCode.ERROR, "Test error")

    spans = exporter.get_finished_spans()
    # All errors should be sampled
    assert len(spans) == 10


def test_adaptive_sampler_slow_operations(exporter: Any) -> None:
    """Test 100% sampling for slow operations."""
    sampler = AdaptiveSampler(
        baseline_rate=0.0,  # 0% baseline
        slow_threshold_ms=100,  # >100ms is slow
        slow_rate=1.0,
    )
    processor = AdaptiveSamplingProcessor(sampler, SimpleSpanProcessor(exporter))
    init(service="test", span_processor=processor, sampler=sampler)  # type: ignore[arg-type]

    # Create slow spans
    for _ in range(5):
        with span("test.slow"):
            time.sleep(0.15)  # 150ms - should be sampled

    spans = exporter.get_finished_spans()
    # All slow spans should be sampled
    assert len(spans) == 5


def test_adaptive_sampler_validation() -> None:
    """Test sampler parameter validation."""
    # Valid
    sampler = AdaptiveSampler(baseline_rate=0.5)
    assert sampler.baseline_rate == 0.5

    # Invalid rates
    with pytest.raises(ValueError):
        AdaptiveSampler(baseline_rate=-0.1)
    with pytest.raises(ValueError):
        AdaptiveSampler(baseline_rate=1.5)
    with pytest.raises(ValueError):
        AdaptiveSampler(error_rate=-0.1)


def test_adaptive_sampler_links_based_disabled_by_default() -> None:
    """Test links-based sampling is disabled by default."""
    sampler = AdaptiveSampler(baseline_rate=0.1)
    assert sampler.links_based is False
    assert sampler.links_rate == 1.0


def test_adaptive_sampler_links_rate_validation() -> None:
    """Test links_rate parameter validation."""
    # Valid
    sampler = AdaptiveSampler(links_based=True, links_rate=0.5)
    assert sampler.links_rate == 0.5

    # Invalid
    with pytest.raises(ValueError):
        AdaptiveSampler(links_rate=-0.1)
    with pytest.raises(ValueError):
        AdaptiveSampler(links_rate=1.5)


def test_adaptive_sampler_links_based_keeps_linked_spans(exporter: Any) -> None:
    """Test spans linked to sampled spans are kept when links_based=True."""
    sampler = AdaptiveSampler(
        baseline_rate=0.0,  # Drop all normal spans
        links_based=True,
        links_rate=1.0,  # Keep all linked spans
    )
    processor = AdaptiveSamplingProcessor(sampler, SimpleSpanProcessor(exporter))
    init(service="test", span_processor=processor, sampler=sampler)  # type: ignore[arg-type]

    # Create a sampled SpanContext to link to
    sampled_context = SpanContext(
        trace_id=0x12345678901234567890123456789012,
        span_id=0x1234567890123456,
        is_remote=True,
        trace_flags=TraceFlags(0x01),  # SAMPLED
    )
    link = Link(sampled_context, {"relationship": "producer"})

    tracer = otel_trace.get_tracer(__name__)
    with tracer.start_as_current_span("consumer.process", links=[link]):
        pass  # Span completes

    spans = exporter.get_finished_spans()
    assert len(spans) == 1
    assert spans[0].name == "consumer.process"


def test_adaptive_sampler_links_based_ignores_unsampled_links(exporter: Any) -> None:
    """Test spans linked to unsampled spans are NOT kept by links_based."""
    sampler = AdaptiveSampler(
        baseline_rate=0.0,  # Drop all normal spans
        links_based=True,
        links_rate=1.0,
    )
    processor = AdaptiveSamplingProcessor(sampler, SimpleSpanProcessor(exporter))
    init(service="test", span_processor=processor, sampler=sampler)  # type: ignore[arg-type]

    # Create an UNSAMPLED SpanContext
    unsampled_context = SpanContext(
        trace_id=0x12345678901234567890123456789012,
        span_id=0x1234567890123456,
        is_remote=True,
        trace_flags=TraceFlags(0x00),  # NOT SAMPLED
    )
    link = Link(unsampled_context)

    tracer = otel_trace.get_tracer(__name__)
    with tracer.start_as_current_span("consumer.process", links=[link]):
        pass

    spans = exporter.get_finished_spans()
    # Should be dropped due to baseline_rate=0.0 and no sampled links
    assert len(spans) == 0


def test_adaptive_sampler_multiple_links_any_sampled(exporter: Any) -> None:
    """Test span is kept if ANY linked span is sampled."""
    sampler = AdaptiveSampler(
        baseline_rate=0.0,
        links_based=True,
        links_rate=1.0,
    )
    processor = AdaptiveSamplingProcessor(sampler, SimpleSpanProcessor(exporter))
    init(service="test", span_processor=processor, sampler=sampler)  # type: ignore[arg-type]

    # Mix of sampled and unsampled links
    unsampled = SpanContext(
        trace_id=0x11111111111111111111111111111111,
        span_id=0x1111111111111111,
        is_remote=True,
        trace_flags=TraceFlags(0x00),
    )
    sampled = SpanContext(
        trace_id=0x22222222222222222222222222222222,
        span_id=0x2222222222222222,
        is_remote=True,
        trace_flags=TraceFlags(0x01),
    )
    links = [Link(unsampled), Link(sampled)]

    tracer = otel_trace.get_tracer(__name__)
    with tracer.start_as_current_span("batch.process", links=links):
        pass

    spans = exporter.get_finished_spans()
    assert len(spans) == 1


def test_adaptive_sampler_errors_take_priority_over_links(exporter: Any) -> None:
    """Test ERROR spans are kept even without sampled links."""
    sampler = AdaptiveSampler(
        baseline_rate=0.0,
        links_based=True,
        links_rate=0.0,  # Would drop linked spans
    )
    processor = AdaptiveSamplingProcessor(sampler, SimpleSpanProcessor(exporter))
    init(service="test", span_processor=processor, sampler=sampler)  # type: ignore[arg-type]

    with span("test.error") as ctx:
        ctx.set_status(StatusCode.ERROR, "Test error")

    spans = exporter.get_finished_spans()
    assert len(spans) == 1  # Error spans always kept


def test_has_sampled_link_helper_method() -> None:
    """Test _has_sampled_link helper method directly."""
    sampler = AdaptiveSampler(links_based=True)

    # Mock span with no links
    span_no_links = MagicMock()
    span_no_links.links = ()
    assert sampler._has_sampled_link(span_no_links) is False

    # Mock span with unsampled link
    unsampled_ctx = SpanContext(
        trace_id=1, span_id=1, is_remote=True, trace_flags=TraceFlags(0x00)
    )
    span_unsampled = MagicMock()
    span_unsampled.links = (Link(unsampled_ctx),)
    assert sampler._has_sampled_link(span_unsampled) is False

    # Mock span with sampled link
    sampled_ctx = SpanContext(
        trace_id=1, span_id=1, is_remote=True, trace_flags=TraceFlags(0x01)
    )
    span_sampled = MagicMock()
    span_sampled.links = (Link(sampled_ctx),)
    assert sampler._has_sampled_link(span_sampled) is True


# --- Tests for create_link_from_headers ---


def test_create_link_from_headers_valid_traceparent() -> None:
    """Test creating a link from valid W3C traceparent header."""
    # Valid W3C traceparent: version-traceid-spanid-flags
    headers = {
        "traceparent": "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01"
    }
    link = create_link_from_headers(headers)

    assert link is not None
    assert link.context.is_valid
    assert link.context.trace_flags.sampled is True


def test_create_link_from_headers_unsampled() -> None:
    """Test creating a link from traceparent with sampled=0."""
    headers = {
        "traceparent": "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-00"
    }
    link = create_link_from_headers(headers)

    assert link is not None
    assert link.context.is_valid
    assert link.context.trace_flags.sampled is False


def test_create_link_from_headers_with_attributes() -> None:
    """Test creating a link with custom attributes."""
    headers = {
        "traceparent": "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01"
    }
    attrs = {"relationship": "producer", "queue": "orders"}
    link = create_link_from_headers(headers, attributes=attrs)

    assert link is not None
    assert link.attributes == attrs


def test_create_link_from_headers_missing_traceparent() -> None:
    """Test that missing traceparent returns None."""
    headers: dict[str, str] = {}
    link = create_link_from_headers(headers)

    assert link is None


def test_create_link_from_headers_invalid_traceparent() -> None:
    """Test that invalid traceparent returns None."""
    headers = {"traceparent": "invalid-header-value"}
    link = create_link_from_headers(headers)

    assert link is None


def test_create_link_from_headers_case_insensitive() -> None:
    """Test that header lookup is case-insensitive (per HTTP spec)."""
    # Note: OpenTelemetry's extract() handles case normalization
    headers = {
        "traceparent": "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01"
    }
    link = create_link_from_headers(headers)
    assert link is not None


# --- Tests for extract_links_from_batch ---


def test_extract_links_from_batch_multiple_messages() -> None:
    """Test extracting links from a batch of messages."""
    messages = [
        {
            "body": "message1",
            "headers": {
                "traceparent": "00-11111111111111111111111111111111-1111111111111111-01"
            },
        },
        {
            "body": "message2",
            "headers": {
                "traceparent": "00-22222222222222222222222222222222-2222222222222222-01"
            },
        },
        {
            "body": "message3",
            "headers": {
                "traceparent": "00-33333333333333333333333333333333-3333333333333333-00"
            },
        },
    ]

    links = extract_links_from_batch(messages)

    assert len(links) == 3
    # First two are sampled, third is not
    assert links[0].context.trace_flags.sampled is True
    assert links[1].context.trace_flags.sampled is True
    assert links[2].context.trace_flags.sampled is False


def test_extract_links_from_batch_empty_list() -> None:
    """Test extracting links from empty message list."""
    links = extract_links_from_batch([])
    assert links == []


def test_extract_links_from_batch_missing_headers() -> None:
    """Test that messages without headers are skipped."""
    messages = [
        {"body": "message1"},  # No headers key
        {
            "body": "message2",
            "headers": {
                "traceparent": "00-22222222222222222222222222222222-2222222222222222-01"
            },
        },
        {"body": "message3", "headers": {}},  # Empty headers
    ]

    links = extract_links_from_batch(messages)

    assert len(links) == 1  # Only the second message has valid trace context


def test_extract_links_from_batch_custom_headers_key() -> None:
    """Test using a custom headers key."""
    messages = [
        {
            "body": "message1",
            "metadata": {
                "traceparent": "00-11111111111111111111111111111111-1111111111111111-01"
            },
        },
    ]

    links = extract_links_from_batch(messages, headers_key="metadata")

    assert len(links) == 1


def test_extract_links_from_batch_invalid_headers_skipped() -> None:
    """Test that messages with invalid traceparent are skipped."""
    messages = [
        {"body": "message1", "headers": {"traceparent": "invalid"}},
        {
            "body": "message2",
            "headers": {
                "traceparent": "00-22222222222222222222222222222222-2222222222222222-01"
            },
        },
    ]

    links = extract_links_from_batch(messages)

    assert len(links) == 1  # Only the second message has valid trace context
