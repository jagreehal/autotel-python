"""Tests for shutdown functionality."""

from typing import Any

import pytest

from autotel import init, shutdown, track
from autotel.exporters import InMemorySpanExporter
from autotel.processors import SimpleSpanProcessor
from autotel.subscribers import EventSubscriber


class MockSubscriber(EventSubscriber):
    """Mock subscriber for testing."""

    def __init__(self: Any) -> None:
        self.shutdown_called = False

    async def send(self: Any, event: str, properties: dict[str, Any]) -> None:
        """No-op."""

    async def shutdown(self: Any) -> None:
        """Mark shutdown called."""
        self.shutdown_called = True


@pytest.mark.asyncio
async def test_shutdown_with_events() -> None:
    """Test shutdown with events initialized."""
    import asyncio

    mock_subscriber = MockSubscriber()
    init(
        service="test",
        span_processor=SimpleSpanProcessor(InMemorySpanExporter()),
        subscribers=[mock_subscriber],
    )

    # Track an event
    track("test_event", {"key": "value"})

    # Give event worker time to start and process
    await asyncio.sleep(0.1)

    # Shutdown
    await shutdown(timeout=1.0)

    # Verify subscriber was shut down
    assert mock_subscriber.shutdown_called


@pytest.mark.asyncio
async def test_shutdown_without_events() -> None:
    """Test shutdown without events."""
    init(
        service="test",
        span_processor=SimpleSpanProcessor(InMemorySpanExporter()),
    )

    # Should not raise
    await shutdown(timeout=1.0)


def test_shutdown_sync() -> None:
    """Test synchronous shutdown."""
    init(
        service="test",
        span_processor=SimpleSpanProcessor(InMemorySpanExporter()),
    )

    # Should not raise
    from autotel import shutdown_sync

    shutdown_sync(timeout=1.0)
