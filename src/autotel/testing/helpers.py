"""Enhanced testing utilities for autotel."""

import logging
from typing import Any

from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter
from opentelemetry.trace import StatusCode


def assert_trace_created(exporter: InMemorySpanExporter, span_name: str) -> None:
    """
    Assert that a trace with the given name was created.

    Args:
        exporter: InMemorySpanExporter instance
        span_name: Expected span name

    Raises:
        AssertionError: If span not found
    """
    spans = exporter.get_finished_spans()
    span_names = [span.name for span in spans]
    assert span_name in span_names, f"Span '{span_name}' not found. Found spans: {span_names}"


def assert_trace_succeeded(exporter: InMemorySpanExporter, span_name: str) -> None:
    """
    Assert that a trace succeeded (not in error state).

    Args:
        exporter: InMemorySpanExporter instance
        span_name: Span name to check

    Raises:
        AssertionError: If span not found or in error state
    """
    spans = exporter.get_finished_spans()
    matching_spans = [span for span in spans if span.name == span_name]

    if not matching_spans:
        raise AssertionError(f"Span '{span_name}' not found")

    for span in matching_spans:
        assert span.status.status_code != StatusCode.ERROR, f"Span '{span_name}' is in error state"


def assert_trace_failed(exporter: InMemorySpanExporter, span_name: str) -> None:
    """
    Assert that a trace failed (in error state).

    Args:
        exporter: InMemorySpanExporter instance
        span_name: Span name to check

    Raises:
        AssertionError: If span not found or not in error state
    """
    spans = exporter.get_finished_spans()
    matching_spans = [span for span in spans if span.name == span_name]

    if not matching_spans:
        raise AssertionError(f"Span '{span_name}' not found")

    for span in matching_spans:
        assert (
            span.status.status_code == StatusCode.ERROR
        ), f"Span '{span_name}' is not in error state"


def assert_no_errors(exporter: InMemorySpanExporter) -> None:
    """
    Assert that no spans are in error state.

    Args:
        exporter: InMemorySpanExporter instance

    Raises:
        AssertionError: If any span is in error state
    """
    spans = exporter.get_finished_spans()
    error_spans = [span for span in spans if span.status.status_code == StatusCode.ERROR]

    if error_spans:
        error_names = [span.name for span in error_spans]
        raise AssertionError(f"Found {len(error_spans)} error spans: {error_names}")


def get_trace_duration(exporter: InMemorySpanExporter, span_name: str) -> float | None:
    """
    Get duration of a trace in milliseconds.

    Args:
        exporter: InMemorySpanExporter instance
        span_name: Span name

    Returns:
        Duration in milliseconds, or None if span not found
    """
    spans = exporter.get_finished_spans()
    matching_spans = [span for span in spans if span.name == span_name]

    if not matching_spans:
        return None

    span = matching_spans[0]
    if span.end_time is None or span.start_time is None:
        return None
    duration_ns = span.end_time - span.start_time
    return duration_ns / 1_000_000  # Convert to milliseconds


def assert_trace_duration(
    exporter: InMemorySpanExporter,
    span_name: str,
    max_duration_ms: float,
) -> None:
    """
    Assert that a trace duration is within the maximum.

    Args:
        exporter: InMemorySpanExporter instance
        span_name: Span name to check
        max_duration_ms: Maximum duration in milliseconds

    Raises:
        AssertionError: If duration exceeds maximum
    """
    duration_ms = get_trace_duration(exporter, span_name)

    if duration_ms is None:
        raise AssertionError(f"Span '{span_name}' not found")

    assert (
        duration_ms <= max_duration_ms
    ), f"Span '{span_name}' duration {duration_ms:.2f}ms exceeds maximum {max_duration_ms}ms"


def get_span_attribute(
    exporter: InMemorySpanExporter,
    span_name: str,
    attribute_key: str,
) -> Any | None:
    """
    Get an attribute value from a span.

    Args:
        exporter: InMemorySpanExporter instance
        span_name: Span name
        attribute_key: Attribute key

    Returns:
        Attribute value, or None if not found
    """
    spans = exporter.get_finished_spans()
    matching_spans = [span for span in spans if span.name == span_name]

    if not matching_spans:
        return None

    span = matching_spans[0]
    if not hasattr(span, "attributes") or span.attributes is None:
        return None
    return span.attributes.get(attribute_key)


def create_mock_logger(name: str = "test") -> logging.Logger:
    """
    Create a mock logger for testing.

    Captures log records for assertions.

    Args:
        name: Logger name

    Returns:
        Logger instance with handler that captures records

    Example:
        >>> logger = create_mock_logger()
        >>> logger.info("test message")
        >>> assert len(logger.handlers[0].records) == 1
    """
    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)

    # Create handler that captures records
    handler = logging.StreamHandler()
    handler.setLevel(logging.DEBUG)
    logger.addHandler(handler)

    return logger


class AnalyticsCollector:
    """
    Analytics collector for testing.

    Captures events events for assertions in tests.
    """

    def __init__(self) -> None:
        """Initialize events collector."""
        self._events: list[dict[str, Any]] = []

    def record_event(self, event: str, properties: dict[str, Any] | None = None) -> None:
        """Record an events event."""
        self._events.append(
            {
                "name": event,
                "properties": properties or {},
            }
        )

    def get_events(self) -> list[dict[str, Any]]:
        """Get all recorded events."""
        return self._events.copy()

    def get_event(self, name: str) -> dict[str, Any] | None:
        """Get first event with given name."""
        for event in self._events:
            if event["name"] == name:
                return event
        return None

    def clear(self) -> None:
        """Clear all recorded events."""
        self._events.clear()


def create_events_collector() -> AnalyticsCollector:
    """
    Create an events collector for testing.

    Returns:
        AnalyticsCollector instance

    Example:
        >>> collector = create_events_collector()
        >>> track("user_created", {"user_id": "123"})
        >>> events = collector.get_events()
        >>> assert len(events) == 1
    """
    return AnalyticsCollector()


def create_trace_collector() -> InMemorySpanExporter:
    """
    Create a trace collector (InMemorySpanExporter) for testing.

    Returns:
        InMemorySpanExporter instance

    Example:
        >>> collector = create_trace_collector()
        >>> init(service="test", span_processor=SimpleSpanProcessor(collector))
        >>> # Run traced code
        >>> assert_trace_created(collector, "my.operation")
    """
    return InMemorySpanExporter()


async def wait_for_trace(
    exporter: InMemorySpanExporter,
    span_name: str,
    timeout: float = 5.0,
) -> bool:
    """
    Wait for a trace to appear (async helper).

    Args:
        exporter: InMemorySpanExporter instance
        span_name: Span name to wait for
        timeout: Timeout in seconds

    Returns:
        True if trace found, False if timeout

    Example:
        >>> found = await wait_for_trace(exporter, "my.operation", timeout=2.0)
        >>> assert found
    """
    import asyncio
    import time

    start = time.time()
    while time.time() - start < timeout:
        spans = exporter.get_finished_spans()
        if any(span.name == span_name for span in spans):
            return True
        await asyncio.sleep(0.1)
    return False
