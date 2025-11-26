"""Tests for Event class and subscriber pattern."""

import asyncio
from typing import Any

import pytest

from autotel import track
from autotel.events import Event
from autotel.subscribers import EventSubscriber


class MockSubscriber(EventSubscriber):
    """Mock subscriber for testing."""

    def __init__(self: Any) -> None:
        self.events: list[tuple[str, dict[str, Any]]] = []

    async def send(self: Any, event: str, properties: dict[str, Any]) -> None:
        """Store event."""
        self.events.append((event, properties))

    async def shutdown(self: Any) -> None:
        """No-op."""


@pytest.fixture
def mock_subscriber() -> Any:
    """Create mock subscriber."""
    return MockSubscriber()


@pytest.fixture
def event(mock_subscriber: Any) -> Any:
    """Create Event instance."""
    event = Event(subscribers=[mock_subscriber], queue_size=100, flush_interval=0.1)
    return event


@pytest.mark.asyncio
async def test_event_track_enriches_with_trace_context(event: Any, mock_subscriber: Any) -> None:
    """Test that trackEvent() enriches events with trace context."""
    from autotel import init, span
    from autotel.exporters import InMemorySpanExporter
    from autotel.processors import SimpleSpanProcessor
    from autotel.track import set_event

    # Initialize autotel so spans are created
    init(service="test", span_processor=SimpleSpanProcessor(InMemorySpanExporter()))

    set_event(event)
    await event.start()

    # Create a span
    with span("test.operation"):
        track("test_event", {"key": "value"})

    # Wait for worker to process
    await asyncio.sleep(0.2)

    # Check event was sent
    assert len(mock_subscriber.events) == 1
    event_name, properties = mock_subscriber.events[0]
    assert event_name == "test_event"
    assert properties["key"] == "value"
    assert "trace_id" in properties
    assert "span_id" in properties
    assert properties["operation.name"] == "test.operation"

    await event.shutdown()


@pytest.mark.asyncio
async def test_event_queue_full_graceful_degradation(event: Any, mock_subscriber: Any) -> None:  # noqa: ARG001
    """Test that queue full doesn't crash."""
    from autotel.track import set_event

    set_event(event)
    await event.start()

    # Fill queue
    for i in range(150):  # More than queue_size
        track("test_event", {"index": i})

    # Should not crash
    await asyncio.sleep(0.2)

    await event.shutdown()


@pytest.mark.asyncio
async def test_event_shutdown_drains_queue(event: Any, mock_subscriber: Any) -> None:
    """Test that shutdown drains queue."""
    from autotel.track import set_event

    set_event(event)
    await event.start()

    # Add events
    track("event1", {"key": "value1"})
    track("event2", {"key": "value2"})

    # Shutdown should drain queue
    await event.shutdown()

    # Check events were sent
    assert len(mock_subscriber.events) >= 2


def test_event_without_subscribers() -> None:
    """Test Event without subscribers."""
    event = Event(subscribers=[])
    assert event.subscribers == []


@pytest.mark.asyncio
async def test_event_direct_track(event: Any, mock_subscriber: Any) -> None:
    """Test calling trackEvent() directly on Event instance."""
    await event.start()

    # Direct call to trackEvent
    event.trackEvent("direct_event", {"property": "value"})

    # Wait for worker to process
    await asyncio.sleep(0.2)

    # Check event was sent
    assert len(mock_subscriber.events) == 1
    event_name, properties = mock_subscriber.events[0]
    assert event_name == "direct_event"
    assert properties["property"] == "value"

    await event.shutdown()


@pytest.mark.asyncio
async def test_event_multiple_subscribers() -> None:
    """Test Event with multiple subscribers."""
    subscriber1 = MockSubscriber()
    subscriber2 = MockSubscriber()
    event = Event(subscribers=[subscriber1, subscriber2], queue_size=100, flush_interval=0.1)

    await event.start()

    event.trackEvent("multi_event", {"key": "value"})

    # Wait for worker to process
    await asyncio.sleep(0.2)

    # Both subscribers should receive event
    assert len(subscriber1.events) == 1
    assert len(subscriber2.events) == 1

    await event.shutdown()


@pytest.mark.asyncio
async def test_event_preserves_trace_context_from_init(event: Any, mock_subscriber: Any) -> None:
    """Test that events include trace context when using init() and track()."""
    from autotel import init, span
    from autotel.exporters import InMemorySpanExporter
    from autotel.processors import SimpleSpanProcessor
    from autotel.track import set_event

    # Initialize with service metadata
    init(
        service="test-service",
        service_version="1.2.3",
        environment="production",
        span_processor=SimpleSpanProcessor(InMemorySpanExporter()),
    )

    set_event(event)
    await event.start()

    # Track event within a span to get trace context
    with span("test.operation"):
        track("context_test", {"key": "value"})

    # Wait for worker to process
    await asyncio.sleep(0.2)

    # Check event includes trace context
    assert len(mock_subscriber.events) == 1
    _, properties = mock_subscriber.events[0]
    # Current implementation adds trace context but not service metadata
    assert "trace_id" in properties
    assert "span_id" in properties
    assert properties["key"] == "value"

    await event.shutdown()


@pytest.mark.asyncio
async def test_event_circuit_breaker_protection(mock_subscriber: Any) -> None:
    """Test that circuit breaker protects against failing subscribers."""

    class FailingSubscriber(EventSubscriber):
        """Subscriber that always fails."""

        async def send(self: Any, _event: str, _properties: dict[str, Any]) -> None:
            raise Exception("Subscriber failure")

        async def shutdown(self: Any) -> None:
            pass

    failing_subscriber = FailingSubscriber()
    event = Event(
        subscribers=[failing_subscriber, mock_subscriber],
        queue_size=100,
        flush_interval=0.1,
        circuit_breaker_threshold=3,
    )

    await event.start()

    # Send events - circuit breaker should trip after threshold
    for i in range(10):
        event.trackEvent(f"event_{i}", {"index": i})

    # Wait for worker to process
    await asyncio.sleep(0.2)

    # Mock subscriber should still receive some events
    # (before circuit breaker trips for failing subscriber)
    assert len(mock_subscriber.events) > 0

    await event.shutdown()
