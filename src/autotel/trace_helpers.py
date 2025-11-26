"""Trace helper functions for advanced use cases."""

import hashlib
import json
from collections.abc import Callable
from typing import Any, TypeVar

from opentelemetry import context as otel_context
from opentelemetry import trace
from opentelemetry.trace import StatusCode

T = TypeVar("T")


def get_tracer(name: str | None = None) -> trace.Tracer:
    """
    Get configured tracer instance.

    Args:
        name: Tracer name (defaults to __name__)

    Returns:
        Tracer instance

    Example:
        >>> tracer = get_tracer(__name__)
        >>> with tracer.start_as_current_span("my.operation") as span:
        ...     span.set_attribute("key", "value")
    """
    return trace.get_tracer(name or __name__)


def get_active_span() -> trace.Span | None:
    """
    Get currently active span.

    Returns:
        Active span if available, None otherwise

    Example:
        >>> span = get_active_span()
        >>> if span:
        ...     span.set_attribute("custom", "value")
    """
    return trace.get_current_span()


def get_active_context() -> otel_context.Context:
    """
    Get current OpenTelemetry context.

    Returns:
        Current context

    Example:
        >>> ctx = get_active_context()
        >>> # Use context for manual span creation
    """
    return otel_context.get_current()


def run_with_span(span: trace.Span, func: Callable[[], T]) -> T:
    """
    Run function with specific span active.

    Args:
        span: Span to make active
        func: Function to execute

    Returns:
        Function result

    Example:
        >>> span = tracer.start_span("test.operation")
        >>> result = run_with_span(span, lambda: my_function())
        >>> span.end()
    """
    token = otel_context.attach(trace.set_span_in_context(span))
    try:
        return func()
    finally:
        otel_context.detach(token)


def flatten_metadata(metadata: dict[str, Any], prefix: str = "metadata") -> dict[str, str]:
    """
    Flatten nested metadata objects into dot-notation span attributes.

    Converts complex nested objects into flat key-value pairs suitable for
    OpenTelemetry span attributes. Nested mappings are recursively flattened
    using dot notation. Non-mapping values are JSON serialized. Filters out
    None values automatically.

    Args:
        metadata: Nested metadata object to flatten
        prefix: Prefix for all attribute keys (default: 'metadata')

    Returns:
        Flattened attributes as dict with string keys and values

    Example:
        >>> from autotel import flatten_metadata, set_attributes
        >>>
        >>> metadata = {
        ...     "user": {"id": "123", "tier": "premium"},
        ...     "payment": {"method": "card", "processor": "stripe"},
        ...     "items": 5
        ... }
        >>> flattened = flatten_metadata(metadata)
        >>> # Result:
        >>> # {
        >>> #     'metadata.user.id': '123',
        >>> #     'metadata.user.tier': 'premium',
        >>> #     'metadata.payment.method': 'card',
        >>> #     'metadata.payment.processor': 'stripe',
        >>> #     'metadata.items': '5'
        >>> # }
        >>>
        >>> # Use with current span
        >>> set_attributes(flattened)

    Best Practices:
        - Use for complex structured data that needs to be searchable
        - Avoid deeply nested structures (keep it 2-3 levels max)
        - Be mindful of cardinality (don't flatten unbounded lists)
        - Sanitize PII before flattening
    """
    flattened: dict[str, str] = {}

    def _flatten(obj: Any, current_prefix: str) -> None:
        if obj is None:
            return

        # Recursively flatten mappings
        if isinstance(obj, dict):
            for k, v in obj.items():
                _flatten(v, f"{current_prefix}.{k}")
            return

        # Primitive types pass through as strings for attribute safety
        if isinstance(obj, str):
            flattened[current_prefix] = obj
            return

        # Serialize non-string values to JSON for consistency
        try:
            flattened[current_prefix] = json.dumps(obj)
        except (TypeError, ValueError):
            flattened[current_prefix] = "<serialization-failed>"

    _flatten(metadata, prefix)

    return flattened


def create_deterministic_trace_id(seed: str) -> str:
    """
    Create a deterministic trace ID from a seed string.

    Generates a consistent 128-bit trace ID (32 hex characters) from an input
    seed using SHA-256 hashing. Useful for correlating external system IDs
    with OpenTelemetry trace IDs.

    Args:
        seed: Input string (e.g., request ID, order ID, session ID)

    Returns:
        32-character hex trace ID (128 bits)

    Example:
        >>> from autotel import create_deterministic_trace_id
        >>>
        >>> # Correlate support ticket with trace
        >>> ticket_id = "TICKET-12345"
        >>> trace_id = create_deterministic_trace_id(ticket_id)
        >>> trace_url = f"https://your-backend.com/traces/{trace_id}"
        >>> print(f"View trace: {trace_url}")
        >>>
        >>> # Same seed always produces same trace ID
        >>> assert create_deterministic_trace_id("test") == create_deterministic_trace_id("test")

    Use Cases:
        - Link external request IDs to traces for debugging
        - Correlate customer support tickets with trace data
        - Create consistent trace IDs for replay/testing scenarios
        - Link business identifiers (order IDs, session IDs) to traces

    Note:
        This generates a trace ID but does NOT start a span or set it as active.
        Use with manual span creation if you need to set a specific trace ID.
    """
    # Encode seed to bytes
    data = seed.encode("utf-8")

    # Generate SHA-256 hash (256 bits)
    hash_digest = hashlib.sha256(data).digest()

    # Convert to hex and truncate to 32 characters (128 bits)
    return hash_digest[:16].hex()  # 16 bytes = 128 bits = 32 hex chars


def finalize_span(span: trace.Span, error: Exception | None = None) -> None:
    """
    Finalize a span with appropriate status and optional error recording.

    Convenience function that records exceptions, sets span status, and ends
    the span in one call. Useful for manual span management.

    Args:
        span: The span to finalize
        error: Optional error to record

    Example:
        >>> from autotel import get_tracer, finalize_span
        >>>
        >>> tracer = get_tracer(__name__)
        >>> span = tracer.start_span("operation")
        >>>
        >>> try:
        ...     do_work()
        ...     finalize_span(span)  # Sets OK status and ends span
        ... except Exception as e:
        ...     finalize_span(span, e)  # Records exception, sets ERROR status, and ends span
        ...     raise

    Note:
        The span status is set to ERROR if an error is provided, OK otherwise.
        The span is always ended, regardless of error.
    """
    if error:
        span.record_exception(error)
        span.set_status(StatusCode.ERROR, str(error))
    else:
        span.set_status(StatusCode.OK)
    span.end()
