"""Tests for messaging decorators and helpers."""

import inspect
from typing import Any

import pytest
from opentelemetry.trace import SpanKind, StatusCode

from autotel import init
from autotel.exporters import InMemorySpanExporter
from autotel.messaging import (
    extract_trace_context,
    inject_trace_headers,
    record_consumer_lag,
    record_dlq,
    record_retry,
    trace_batch_consumer,
    trace_consumer,
    trace_producer,
)
from autotel.processors import SimpleSpanProcessor


@pytest.fixture
def exporter() -> Any:
    """Create in-memory exporter for testing."""
    exp = InMemorySpanExporter()
    init(service="test", span_processor=SimpleSpanProcessor(exp))
    return exp


# --- trace_consumer tests ---


def test_trace_consumer_basic(exporter: Any) -> None:
    """Test basic consumer decorator."""

    @trace_consumer(system="kafka", destination="orders")
    def handle_order(message: dict[str, Any]) -> str:
        return f"processed: {message['id']}"

    result = handle_order({"id": "123"})
    assert result == "processed: 123"

    spans = exporter.get_finished_spans()
    assert len(spans) == 1

    span = spans[0]
    assert span.name == "orders receive"
    assert span.kind == SpanKind.CONSUMER
    assert span.attributes["messaging.system"] == "kafka"
    assert span.attributes["messaging.operation"] == "receive"
    assert span.attributes["messaging.destination.name"] == "orders"


def test_trace_consumer_with_ctx(exporter: Any) -> None:
    """Test consumer decorator with TraceContext."""

    @trace_consumer(system="rabbitmq", destination="notifications")
    def handle_notification(ctx: Any, message: dict[str, Any]) -> str:
        ctx.set_attribute("notification.type", message["type"])
        return "handled"

    result = handle_notification({"type": "email"})
    assert result == "handled"

    spans = exporter.get_finished_spans()
    assert len(spans) == 1
    assert spans[0].attributes["notification.type"] == "email"


@pytest.mark.asyncio
async def test_trace_consumer_async(exporter: Any) -> None:
    """Test async consumer decorator."""

    @trace_consumer(system="sqs", destination="tasks")
    async def handle_task(ctx: Any, message: dict[str, Any]) -> str:
        ctx.set_attribute("task.id", message["task_id"])
        return "completed"

    result = await handle_task({"task_id": "task-456"})
    assert result == "completed"

    spans = exporter.get_finished_spans()
    assert len(spans) == 1
    assert spans[0].kind == SpanKind.CONSUMER
    assert spans[0].attributes["task.id"] == "task-456"


def test_trace_consumer_signature_rewrite(exporter: Any) -> None:
    """Test that ctx parameter is hidden from signature."""

    @trace_consumer(system="kafka", destination="events")
    def handler(ctx: Any, event_id: str, data: dict[str, Any]) -> str:
        return f"{event_id}"

    sig = inspect.signature(handler)
    param_names = list(sig.parameters.keys())

    assert "ctx" not in param_names
    assert "event_id" in param_names
    assert "data" in param_names


def test_trace_consumer_with_link_extraction(exporter: Any) -> None:
    """Test consumer decorator extracts links from message headers."""
    # First, create a producer span to get valid trace context
    headers = inject_trace_headers()

    @trace_consumer(system="kafka", destination="orders", link_from_headers=True)
    def handle_order(message: dict[str, Any]) -> str:
        return "processed"

    # Message with headers containing trace context
    message = {"id": "123", "headers": headers}
    result = handle_order(message)
    assert result == "processed"

    spans = exporter.get_finished_spans()
    assert len(spans) == 1
    # Note: Links are only created if there's a valid trace context
    # In tests without a parent span, traceparent may not be set


# --- trace_producer tests ---


def test_trace_producer_basic(exporter: Any) -> None:
    """Test basic producer decorator."""

    @trace_producer(system="kafka", destination="order-events")
    def publish_event(event: dict[str, Any]) -> dict[str, Any]:
        return {"sent": True, **event}

    result = publish_event({"type": "created"})
    assert result["sent"] is True

    spans = exporter.get_finished_spans()
    assert len(spans) == 1

    span = spans[0]
    assert span.name == "order-events send"
    assert span.kind == SpanKind.PRODUCER
    assert span.attributes["messaging.system"] == "kafka"
    assert span.attributes["messaging.operation"] == "send"
    assert span.attributes["messaging.destination.name"] == "order-events"


def test_trace_producer_with_ctx(exporter: Any) -> None:
    """Test producer decorator with TraceContext."""

    @trace_producer(system="redis", destination="stream:orders")
    def publish_to_stream(ctx: Any, data: dict[str, Any]) -> dict[str, str]:
        ctx.set_attribute("stream.entry_id", "1234-0")
        return ctx.inject_headers()

    headers = publish_to_stream({"order_id": "123"})

    # Verify headers are injected
    assert "traceparent" in headers

    spans = exporter.get_finished_spans()
    assert len(spans) == 1
    assert spans[0].attributes["stream.entry_id"] == "1234-0"


@pytest.mark.asyncio
async def test_trace_producer_async(exporter: Any) -> None:
    """Test async producer decorator."""

    @trace_producer(system="kafka", destination="notifications")
    async def send_notification(ctx: Any, notification: dict[str, Any]) -> dict[str, str]:
        ctx.set_attribute("notification.recipient", notification["to"])
        return ctx.inject_headers()

    headers = await send_notification({"to": "user@example.com"})
    assert "traceparent" in headers

    spans = exporter.get_finished_spans()
    assert len(spans) == 1
    assert spans[0].kind == SpanKind.PRODUCER


# --- trace_batch_consumer tests ---


def test_trace_batch_consumer_basic(exporter: Any) -> None:
    """Test batch consumer decorator."""

    @trace_batch_consumer(system="sqs", destination="tasks")
    def process_batch(messages: list[dict[str, Any]]) -> int:
        return len(messages)

    messages = [{"id": "1"}, {"id": "2"}, {"id": "3"}]
    result = process_batch(messages)
    assert result == 3

    spans = exporter.get_finished_spans()
    assert len(spans) == 1

    span = spans[0]
    assert span.name == "tasks batch receive"
    assert span.kind == SpanKind.CONSUMER


def test_trace_batch_consumer_with_ctx(exporter: Any) -> None:
    """Test batch consumer with TraceContext."""

    @trace_batch_consumer(system="kafka", destination="events")
    def process_events(ctx: Any, events: list[dict[str, Any]]) -> list[str]:
        ctx.set_attribute("batch.size", len(events))
        return [e["id"] for e in events]

    events = [{"id": "a"}, {"id": "b"}]
    result = process_events(events)
    assert result == ["a", "b"]

    spans = exporter.get_finished_spans()
    assert spans[0].attributes["batch.size"] == 2


# --- inject/extract helpers tests ---


def test_inject_trace_headers(exporter: Any) -> None:
    """Test inject_trace_headers helper."""
    # Create a span context first
    from autotel import span

    with span("test-span"):
        headers = inject_trace_headers()
        assert "traceparent" in headers


def test_inject_trace_headers_with_existing(exporter: Any) -> None:
    """Test inject_trace_headers preserves existing headers."""
    from autotel import span

    with span("test-span"):
        existing = {"Content-Type": "application/json"}
        headers = inject_trace_headers(existing)
        assert headers["Content-Type"] == "application/json"
        assert "traceparent" in headers


def test_extract_trace_context() -> None:
    """Test extract_trace_context helper."""
    # Create headers with valid traceparent
    headers = {
        "traceparent": "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01"
    }
    ctx = extract_trace_context(headers)
    # Context should be returned (even if not valid, it shouldn't error)
    assert ctx is not None


# --- error handling tests ---


def test_trace_consumer_records_exception(exporter: Any) -> None:
    """Test that exceptions are recorded on consumer spans."""
    from opentelemetry.trace import StatusCode

    @trace_consumer(system="kafka", destination="orders")
    def failing_handler(message: dict[str, Any]) -> None:
        raise ValueError("Processing failed")

    with pytest.raises(ValueError):
        failing_handler({"id": "bad"})

    spans = exporter.get_finished_spans()
    assert len(spans) == 1
    assert spans[0].status.status_code == StatusCode.ERROR


def test_trace_producer_records_exception(exporter: Any) -> None:
    """Test that exceptions are recorded on producer spans."""
    from opentelemetry.trace import StatusCode

    @trace_producer(system="kafka", destination="events")
    def failing_producer(event: dict[str, Any]) -> None:
        raise ConnectionError("Broker unavailable")

    with pytest.raises(ConnectionError):
        failing_producer({"type": "test"})

    spans = exporter.get_finished_spans()
    assert len(spans) == 1
    assert spans[0].status.status_code == StatusCode.ERROR


# --- DLQ/Retry/Lag helper tests ---


def test_record_dlq_basic(exporter: Any) -> None:
    """Test basic DLQ recording."""

    @trace_consumer(system="kafka", destination="orders")
    def handler(ctx: Any, message: dict[str, Any]) -> str:
        record_dlq(
            ctx,
            original_destination="orders",
            dlq_destination="orders.dlq",
            reason="max_retries_exceeded",
            retry_count=3,
        )
        return "dlq"

    handler({"id": "123"})

    spans = exporter.get_finished_spans()
    assert len(spans) == 1
    span = spans[0]

    # Check attributes
    assert span.attributes["messaging.dlq.original_destination"] == "orders"
    assert span.attributes["messaging.dlq.destination"] == "orders.dlq"
    assert span.attributes["messaging.dlq.reason"] == "max_retries_exceeded"
    assert span.attributes["messaging.dlq.retry_count"] == 3

    # Check event
    events = [e for e in span.events if e.name == "message.dlq"]
    assert len(events) == 1


def test_record_dlq_with_message_id(exporter: Any) -> None:
    """Test DLQ recording with original message ID."""

    @trace_consumer(system="sqs", destination="tasks")
    def handler(ctx: Any, message: dict[str, Any]) -> str:
        record_dlq(
            ctx,
            original_destination="tasks",
            dlq_destination="tasks-dlq",
            reason="poison_pill",
            original_message_id="msg-abc-123",
        )
        return "dlq"

    handler({"id": "bad"})

    spans = exporter.get_finished_spans()
    assert spans[0].attributes["messaging.dlq.original_message_id"] == "msg-abc-123"


def test_record_retry_basic(exporter: Any) -> None:
    """Test basic retry recording."""

    @trace_consumer(system="kafka", destination="orders")
    def handler(ctx: Any, message: dict[str, Any]) -> str:
        record_retry(
            ctx,
            attempt=2,
            max_attempts=3,
            backoff_ms=2000,
            last_error="Connection timeout",
        )
        return "retrying"

    handler({"id": "123"})

    spans = exporter.get_finished_spans()
    span = spans[0]

    assert span.attributes["messaging.retry.attempt"] == 2
    assert span.attributes["messaging.retry.max_attempts"] == 3
    assert span.attributes["messaging.retry.backoff_ms"] == 2000
    assert span.attributes["messaging.retry.last_error"] == "Connection timeout"

    # Check event
    events = [e for e in span.events if e.name == "message.retry"]
    assert len(events) == 1


def test_record_retry_with_next_retry_at(exporter: Any) -> None:
    """Test retry recording with next retry timestamp."""

    @trace_consumer(system="rabbitmq", destination="tasks")
    def handler(ctx: Any, message: dict[str, Any]) -> str:
        record_retry(
            ctx,
            attempt=1,
            max_attempts=5,
            next_retry_at="2025-01-15T10:30:00Z",
        )
        return "scheduled"

    handler({"id": "123"})

    spans = exporter.get_finished_spans()
    assert spans[0].attributes["messaging.retry.next_retry_at"] == "2025-01-15T10:30:00Z"


def test_record_consumer_lag_basic(exporter: Any) -> None:
    """Test basic consumer lag recording."""

    @trace_consumer(system="kafka", destination="orders")
    def handler(ctx: Any, message: dict[str, Any]) -> str:
        record_consumer_lag(
            ctx,
            lag_ms=1500,
            lag_messages=100,
            partition=2,
        )
        return "processed"

    handler({"id": "123"})

    spans = exporter.get_finished_spans()
    span = spans[0]

    assert span.attributes["messaging.consumer.lag_ms"] == 1500
    assert span.attributes["messaging.consumer.lag_messages"] == 100
    assert span.attributes["messaging.kafka.partition"] == 2


def test_record_consumer_lag_full(exporter: Any) -> None:
    """Test consumer lag recording with all fields."""

    @trace_consumer(system="kafka", destination="events")
    def handler(ctx: Any, message: dict[str, Any]) -> str:
        record_consumer_lag(
            ctx,
            lag_ms=500,
            lag_messages=50,
            partition=0,
            consumer_group="my-service",
            committed_offset=12345,
            high_watermark=12395,
        )
        return "processed"

    handler({"id": "123"})

    spans = exporter.get_finished_spans()
    span = spans[0]

    assert span.attributes["messaging.kafka.consumer_group"] == "my-service"
    assert span.attributes["messaging.kafka.committed_offset"] == 12345
    assert span.attributes["messaging.kafka.high_watermark"] == 12395


def test_record_dlq_with_custom_attributes(exporter: Any) -> None:
    """Test DLQ recording with custom attributes."""

    @trace_consumer(system="kafka", destination="orders")
    def handler(ctx: Any, message: dict[str, Any]) -> str:
        record_dlq(
            ctx,
            original_destination="orders",
            dlq_destination="orders.dlq",
            reason="validation_failed",
            attributes={"order.id": "ord-123", "validation.error": "invalid_amount"},
        )
        return "dlq"

    handler({"id": "123"})

    spans = exporter.get_finished_spans()
    span = spans[0]

    assert span.attributes["order.id"] == "ord-123"
    assert span.attributes["validation.error"] == "invalid_amount"


# ---------------------------------------------------------------------------
# Characterization tests pinning behavior for the async/sync dedup refactor.
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_trace_batch_consumer_async(exporter: Any) -> None:
    """Batch consumer works on async functions like the sync path."""

    @trace_batch_consumer(system="kafka", destination="events")
    async def process(messages: list[dict[str, Any]]) -> int:
        return len(messages)

    assert await process([{"id": "1"}, {"id": "2"}]) == 2
    spans = exporter.get_finished_spans()
    assert spans[0].name == "events batch receive"
    assert spans[0].kind == SpanKind.CONSUMER


def test_trace_batch_consumer_sets_message_count(exporter: Any) -> None:
    """Batch consumer records messaging.batch.message_count from extracted links."""

    @trace_batch_consumer(system="sqs", destination="tasks")
    def process(messages: list[dict[str, Any]]) -> int:
        return len(messages)

    process([{"id": "1"}, {"id": "2"}, {"id": "3"}])
    span = exporter.get_finished_spans()[0]
    # No trace headers on the messages, so no producer links are extracted.
    assert span.attributes["messaging.batch.message_count"] == 0


def test_trace_batch_consumer_records_exception(exporter: Any) -> None:
    """Batch consumer marks the span ERROR when the function raises."""

    @trace_batch_consumer(system="kafka", destination="events")
    def process(messages: list[dict[str, Any]]) -> None:
        raise ValueError("batch boom")

    with pytest.raises(ValueError, match="batch boom"):
        process([{"id": "1"}])
    span = exporter.get_finished_spans()[0]
    assert span.status.status_code == StatusCode.ERROR


def test_record_dlq_accepts_raw_span(exporter: Any) -> None:
    """record_dlq works when passed a raw Span (not just a TraceContext)."""
    from opentelemetry import trace as otel_trace

    tracer = otel_trace.get_tracer(__name__)
    with tracer.start_as_current_span("manual") as span:
        record_dlq(
            span,
            original_destination="orders",
            dlq_destination="orders.dlq",
            reason="poison_pill",
        )

    finished = exporter.get_finished_spans()[0]
    assert finished.attributes["messaging.dlq.original_destination"] == "orders"
    assert finished.attributes["messaging.dlq.reason"] == "poison_pill"


def test_trace_consumer_custom_attributes_stringify(exporter: Any) -> None:
    """Primitive custom attributes are kept as-is; non-primitives are stringified."""

    @trace_consumer(
        system="kafka",
        destination="orders",
        attributes={"count": 3, "meta": {"a": 1}},
    )
    def handle(message: dict[str, Any]) -> None:
        return None

    handle({"id": "1"})
    span = exporter.get_finished_spans()[0]
    assert span.attributes["count"] == 3
    assert span.attributes["meta"] == str({"a": 1})
