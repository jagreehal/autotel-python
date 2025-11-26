"""Convenience helper functions for common operations."""

from typing import Any

from opentelemetry import context as otel_context
from opentelemetry import trace
from opentelemetry.baggage import propagation


def set_attributes(attributes: dict[str, Any]) -> None:
    """
    Set multiple attributes on the current active span.

    Convenience function for setting multiple attributes at once without
    needing to get the span first.

    Example:
        >>> from autotel import set_attributes
        >>> set_attributes({
        ...     "user.id": "123",
        ...     "user.role": "admin",
        ...     "request.size": 1024
        ... })

    Args:
        attributes: Dictionary of attribute key-value pairs
    """
    span = trace.get_current_span()
    if span and span.is_recording():
        for key, value in attributes.items():
            # Convert value to supported types
            if isinstance(value, str | bool | int | float):
                span.set_attribute(key, value)
            elif isinstance(value, list | tuple):
                # OTEL supports sequences of primitives
                span.set_attribute(key, list(value))
            else:
                # Convert to string for unsupported types
                span.set_attribute(key, str(value))


def set_attribute(key: str, value: Any) -> None:
    """
    Set a single attribute on the current active span.

    Convenience function for setting an attribute without needing to get
    the span first.

    Example:
        >>> from autotel import set_attribute
        >>> set_attribute("user.id", "123")

    Args:
        key: Attribute key
        value: Attribute value
    """
    span = trace.get_current_span()
    if span and span.is_recording():
        if isinstance(value, str | bool | int | float):
            span.set_attribute(key, value)
        elif isinstance(value, list | tuple):
            span.set_attribute(key, list(value))
        else:
            span.set_attribute(key, str(value))


def add_event(name: str, attributes: dict[str, Any] | None = None) -> None:
    """
    Add an event to the current active span.

    Convenience function for adding span events without needing to get
    the span first.

    Example:
        >>> from autotel import add_event
        >>> add_event("user.login", {"user_id": "123", "method": "oauth"})

    Args:
        name: Event name
        attributes: Optional event attributes
    """
    span = trace.get_current_span()
    if span and span.is_recording():
        span.add_event(name, attributes or {})


def record_exception(exception: Exception, attributes: dict[str, Any] | None = None) -> None:
    """
    Record an exception on the current active span.

    Convenience function for recording exceptions without needing to get
    the span first. The span status is automatically set to ERROR.

    Example:
        >>> from autotel import record_exception
        >>> try:
        ...     risky_operation()
        ... except ValueError as e:
        ...     record_exception(e, {"operation": "risky_operation"})
        ...     # Handle or re-raise

    Args:
        exception: The exception to record
        attributes: Optional additional attributes
    """
    span = trace.get_current_span()
    if span and span.is_recording():
        from opentelemetry.trace import StatusCode

        span.record_exception(exception, attributes=attributes)
        span.set_status(StatusCode.ERROR, str(exception))


def get_trace_id() -> str | None:
    """
    Get the trace ID of the current active span as a hex string.

    Returns None if no active span.

    Example:
        >>> from autotel import get_trace_id
        >>> trace_id = get_trace_id()
        >>> print(f"Current trace: {trace_id}")

    Returns:
        Trace ID as hex string or None
    """
    span = trace.get_current_span()
    if span:
        span_context = span.get_span_context()
        if span_context.is_valid:
            return format(span_context.trace_id, "032x")
    return None


def get_span_id() -> str | None:
    """
    Get the span ID of the current active span as a hex string.

    Returns None if no active span.

    Example:
        >>> from autotel import get_span_id
        >>> span_id = get_span_id()
        >>> print(f"Current span: {span_id}")

    Returns:
        Span ID as hex string or None
    """
    span = trace.get_current_span()
    if span:
        span_context = span.get_span_context()
        if span_context.is_valid:
            return format(span_context.span_id, "016x")
    return None


def get_baggage(key: str) -> str | None:
    """
    Get a baggage value by key from the current context.

    Convenience function for reading baggage without needing a TraceContext.

    Example:
        >>> from autotel import get_baggage
        >>> tenant_id = get_baggage("tenant.id")
        >>> if tenant_id:
        ...     print(f"Processing for tenant: {tenant_id}")

    Args:
        key: Baggage key

    Returns:
        Baggage value or None
    """
    current_context = otel_context.get_current()
    baggage = propagation.get_all(current_context)
    if baggage and key in baggage:
        value = baggage[key]
        return str(value) if value is not None else None
    return None


def get_all_baggage() -> dict[str, str]:
    """
    Get all baggage entries from the current context.

    Convenience function for reading all baggage without needing a TraceContext.

    Example:
        >>> from autotel import get_all_baggage
        >>> baggage = get_all_baggage()
        >>> print(f"Context: {baggage}")

    Returns:
        Dictionary of all baggage entries
    """
    current_context = otel_context.get_current()
    baggage = propagation.get_all(current_context)
    if not baggage:
        return {}
    return {k: str(v) for k, v in baggage.items()}


def set_baggage_value(key: str, value: str) -> otel_context.Token[Any]:
    """
    Set a single baggage value in the current context.

    Note: For proper scoping across async boundaries, prefer using
    `with_baggage()` context manager instead.

    Example:
        >>> from autotel import set_baggage_value
        >>> set_baggage_value("user.id", "123")

    Args:
        key: Baggage key
        value: Baggage value (must be string)

    Returns:
        A context token that can be detached via `otel_context.detach(token)`
        when you want to restore the previous context.
    """
    current_context = otel_context.get_current()
    new_context = propagation.set_baggage(key, str(value), current_context)

    # Preserve span context
    current_span = trace.get_current_span()
    if current_span and current_span.get_span_context().is_valid:
        new_context = trace.set_span_in_context(current_span, new_context)

    return otel_context.attach(new_context)
