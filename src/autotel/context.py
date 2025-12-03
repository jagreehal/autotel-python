"""TraceContext class for ergonomic span operations."""

from collections.abc import Mapping, Sequence

from opentelemetry import context, trace
from opentelemetry.baggage import propagation
from opentelemetry.trace import Link, Span, SpanContext, StatusCode

# OpenTelemetry attribute value types - primitives and homogeneous sequences
AttributeValue = (
    str
    | int
    | float
    | bool
    | Sequence[str]
    | Sequence[int]
    | Sequence[float]
    | Sequence[bool]
)


class TraceContext:
    """Ergonomic wrapper around OpenTelemetry Span."""

    def __init__(self, span: Span) -> None:
        self._span = span

    def set_attribute(self, key: str, value: AttributeValue) -> None:
        """
        Set a span attribute.

        Supports primitive values and homogeneous arrays per OpenTelemetry spec.

        Args:
            key: Attribute key
            value: Primitive (str, int, float, bool) or homogeneous array
        """
        self._span.set_attribute(key, value)

    def set_attributes(self, attributes: Mapping[str, AttributeValue]) -> None:
        """
        Set multiple span attributes at once.

        More efficient than multiple set_attribute() calls for batch updates.

        Args:
            attributes: Dictionary of attribute key-value pairs
        """
        self._span.set_attributes(dict(attributes))

    def add_event(
        self, name: str, attributes: dict[str, str | int | float | bool] | None = None
    ) -> None:
        """Add a span event."""
        self._span.add_event(name, attributes or {})

    def set_status(self, code: StatusCode, description: str | None = None) -> None:
        """Set span status."""
        self._span.set_status(code, description)

    def record_exception(self, exception: Exception) -> None:
        """Record an exception."""
        self._span.record_exception(exception)

    def add_link(
        self,
        span_context: SpanContext,
        attributes: Mapping[str, AttributeValue] | None = None,
    ) -> None:
        """
        Add a link to another span.

        Links establish relationships between spans that may not have a
        parent-child relationship (e.g., batch processing, fan-out operations).

        Note: In OpenTelemetry, links are typically added at span creation time.
        Adding links after creation may not be supported by all backends.

        Args:
            span_context: The SpanContext of the span to link to
            attributes: Optional attributes for this link
        """
        self._span.add_link(span_context, dict(attributes) if attributes else None)

    def add_links(self, links: Sequence[Link]) -> None:
        """
        Add multiple links to other spans.

        Args:
            links: Sequence of Link objects to add
        """
        for link in links:
            self._span.add_link(link.context, dict(link.attributes) if link.attributes else None)

    def update_name(self, name: str) -> None:
        """
        Update the span name dynamically.

        Useful when the final operation name isn't known until later
        (e.g., after parsing a request or determining the handler).

        Args:
            name: New name for the span
        """
        self._span.update_name(name)

    def is_recording(self) -> bool:
        """
        Check if this span is recording events.

        Returns False if the span is a no-op span (e.g., due to sampling).
        Useful for avoiding expensive attribute computation when not needed.

        Returns:
            True if the span is recording, False otherwise
        """
        return self._span.is_recording()

    @property
    def span_id(self) -> str:
        """Get span ID as hex string."""
        return format(self._span.get_span_context().span_id, "016x")

    @property
    def trace_id(self) -> str:
        """Get trace ID as hex string."""
        return format(self._span.get_span_context().trace_id, "032x")

    def get_baggage(self, key: str) -> str | None:
        """
        Get a baggage entry by key.

        Args:
            key: Baggage key

        Returns:
            Baggage entry value or None
        """
        current_context = context.get_current()
        baggage = propagation.get_all(current_context)
        if not baggage:
            return None
        # Baggage is a Mapping, so we can use .get() directly
        value = baggage.get(key)
        return str(value) if value is not None else None

    def set_baggage(self, key: str, value: str) -> str:
        """
        Set a baggage entry.

        Note: OpenTelemetry contexts are immutable. For proper scoping across async
        boundaries, use with_baggage() instead. This method updates baggage in the
        current context which may not propagate to all child operations.

        Args:
            key: Baggage key
            value: Baggage value

        Returns:
            The baggage value that was set (for chaining)
        """
        current_context = context.get_current()
        # Set the baggage entry using the correct API (takes individual key-value pair)
        new_context = propagation.set_baggage(key, value, current_context)
        # Ensure span context is preserved in the new context
        # propagation.set_baggage() should preserve it, but we explicitly ensure it's there
        current_span = trace.get_current_span()
        if current_span and current_span.get_span_context().is_valid:
            # Set the span context in the new context to ensure it's preserved
            new_context = trace.set_span_in_context(current_span, new_context)
        # Attach the new context to make it active for downstream spans/propagators
        # Note: This updates the active context, so inject() and new spans will see updated baggage
        context.attach(new_context)
        return value

    def delete_baggage(self, key: str) -> None:
        """
        Delete a baggage entry.

        Note: OpenTelemetry contexts are immutable. For proper scoping across async
        boundaries, use with_baggage() with only the entries you want instead.

        Args:
            key: Baggage key
        """
        current_context = context.get_current()
        baggage = propagation.get_all(current_context)
        if baggage and key in baggage:
            # To delete, rebuild context with all baggage entries except the one to delete
            # Get all current baggage entries
            baggage_dict = dict(baggage)
            # Remove the key to delete
            del baggage_dict[key]
            # Get span context to preserve it
            current_span = trace.get_current_span()
            # Start from a fresh context with just the span context (if any)
            if current_span and current_span.get_span_context().is_valid:
                # Create new context with span context but no baggage
                new_context = trace.set_span_in_context(current_span, context.Context())
            else:
                # No span, start from empty context
                new_context = context.Context()
            # Add back all baggage entries except the deleted one
            for k, v in baggage_dict.items():
                new_context = propagation.set_baggage(k, str(v), new_context)
            # Attach the new context to make it active for downstream spans/propagators
            context.attach(new_context)
        else:
            # No baggage to delete, but ensure context is still attached
            # This ensures consistency even when there's no baggage
            context.attach(current_context)

    def get_all_baggage(self) -> dict[str, str]:
        """
        Get all baggage entries.

        Returns:
            Dictionary of all baggage entries
        """
        current_context = context.get_current()
        baggage = propagation.get_all(current_context)
        if not baggage:
            return {}
        # Convert baggage Mapping to dict, converting values to strings
        return {k: str(v) for k, v in baggage.items()}
