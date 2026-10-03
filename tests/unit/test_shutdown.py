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


@pytest.mark.asyncio
async def test_shutdown_sync_with_running_loop_still_shuts_providers() -> None:
    """shutdown_sync() inside a running loop shuts down the OTel providers."""
    from autotel import shutdown_sync

    processor = RecordingProcessor()
    init(service="test", span_processor=processor)
    shutdown_sync(timeout=1.0)
    assert processor.was_shut_down


class RecordingProcessor(SimpleSpanProcessor):
    """Records whether shutdown() reached it."""

    def __init__(self: Any) -> None:
        super().__init__(InMemorySpanExporter())
        self.was_shut_down = False

    def shutdown(self: Any) -> None:
        self.was_shut_down = True
        super().shutdown()


@pytest.mark.asyncio
async def test_shutdown_after_reinit_shuts_down_the_new_providers() -> None:
    """A second init()/shutdown() cycle shuts down the second set of providers."""
    init(service="test", span_processor=RecordingProcessor())
    await shutdown(timeout=1.0)

    second = RecordingProcessor()
    init(service="test", span_processor=second)
    await shutdown(timeout=1.0)

    assert second.was_shut_down
