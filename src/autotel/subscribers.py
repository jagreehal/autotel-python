"""Event tracking system for autotel."""

import asyncio
import logging
from typing import Any, Protocol

from opentelemetry import trace

from .circuit_breaker import CircuitBreaker

logger = logging.getLogger(__name__)


class EventSubscriber(Protocol):
    """
    Base protocol for event subscribers.

    Event subscribers receive product events and forward them to external platforms.
    """

    async def send(self, event: str, properties: dict[str, Any]) -> None:
        """Send event to destination."""
        ...

    async def shutdown(self) -> None:
        """Shutdown subscriber."""
        ...


class Event:
    """
    Core event tracking class with queue and auto-enrichment.

    Manages a queue of events and sends them to configured subscribers
    with automatic enrichment from trace context.
    """

    def __init__(
        self,
        subscribers: list[EventSubscriber] | None = None,
        *,
        queue_size: int = 1000,
        flush_interval: float = 1.0,
        circuit_breaker_threshold: int = 5,
    ):
        """
        Initialize event tracking system.

        Args:
            subscribers: List of event subscribers
            queue_size: Maximum queue size
            flush_interval: Interval in seconds for flushing queue
            circuit_breaker_threshold: Failure threshold for circuit breakers
        """
        self.subscribers = subscribers or []
        self.queue: asyncio.Queue[dict[str, Any]] = asyncio.Queue(maxsize=queue_size)
        self.flush_interval = flush_interval
        self._worker_task: asyncio.Task[None] | None = None
        self._running = False
        self._started = False  # Track if start() has been called

        # Create circuit breaker for each subscriber
        self._circuit_breakers: dict[EventSubscriber, CircuitBreaker] = {
            subscriber: CircuitBreaker(failure_threshold=circuit_breaker_threshold)
            for subscriber in self.subscribers
        }

    async def start(self) -> None:
        """Start background worker."""
        if self._running or self._started:
            return

        self._started = True
        self._running = True
        # Get the current running loop (must be called from async context)
        loop = asyncio.get_running_loop()
        self._worker_task = loop.create_task(self._worker())

    async def _worker(self) -> None:
        """Background worker that processes events queue."""
        while self._running:
            try:
                # Wait for event or timeout
                try:
                    event = await asyncio.wait_for(
                        self.queue.get(),
                        timeout=self.flush_interval,
                    )
                except asyncio.TimeoutError:
                    # Timeout - flush any pending events
                    continue

                # Send to all subscribers (with circuit breaker protection)
                await self._send_to_subscribers(event)

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Analytics worker error: {e}", exc_info=True)

    async def _send_to_subscribers(self, event: dict[str, Any]) -> None:
        """Send event to all subscribers with circuit breaker protection."""
        for subscriber in self.subscribers:
            breaker = self._circuit_breakers[subscriber]

            if breaker.is_open():
                continue  # Skip this subscriber

            try:
                await subscriber.send(event["name"], event["properties"])
                breaker.record_success()
            except Exception as e:
                breaker.record_failure()
                # Log error but don't fail (graceful degradation)
                logger.error(f"Event subscriber failed: {e}", exc_info=True)

    def track(self, event: str, properties: dict[str, Any] | None = None) -> None:
        """
        Track event (async, non-blocking).

        Auto-enriches with:
        - trace_id
        - span_id
        - operation.name (from operation context)

        Args:
            event: Event name
            properties: Event properties
        """
        props = properties or {}

        # Auto-enrich with telemetry context
        span = trace.get_current_span()
        if span and span.is_recording():
            span_context = span.get_span_context()
            props["trace_id"] = format(span_context.trace_id, "032x")
            props["span_id"] = format(span_context.span_id, "016x")

        # Get operation name from operation context (set by @trace decorator)
        from .operation_context import get_operation_context

        operation_name = get_operation_context()
        if operation_name:
            props["operation.name"] = operation_name
        else:
            # Fallback: Try to get operation name from span
            if span and span.is_recording():
                try:
                    if hasattr(span, "name"):
                        props["operation.name"] = span.name
                except Exception:
                    pass  # Graceful degradation if name not available

        # Add to queue (non-blocking)
        try:
            self.queue.put_nowait({"name": event, "properties": props})
        except asyncio.QueueFull:
            logger.warning("Event queue full, dropping event")

    async def shutdown(self) -> None:
        """Gracefully shutdown (flush pending events)."""
        if not self._running:
            return

        self._running = False

        # Cancel worker task
        if self._worker_task:
            self._worker_task.cancel()
            try:
                await self._worker_task
            except asyncio.CancelledError:
                pass
            except Exception as e:
                logger.error(f"Error cancelling worker task: {e}", exc_info=True)

        self._started = False

        # Drain queue (send remaining events)
        while not self.queue.empty():
            try:
                event = self.queue.get_nowait()
                await self._send_to_subscribers(event)
            except Exception as e:
                logger.error(f"Error draining event queue: {e}", exc_info=True)

        # Shutdown subscribers
        for subscriber in self.subscribers:
            try:
                await subscriber.shutdown()
            except Exception as e:
                logger.error(f"Error shutting down subscriber: {e}", exc_info=True)
