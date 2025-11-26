"""
Span processor that copies baggage entries to span attributes.

This makes baggage visible in trace UIs without manual attribute setting.
Enabled via init(baggage=True) or init(baggage='custom-prefix')
"""

from typing import Any

from opentelemetry import context
from opentelemetry.baggage import propagation
from opentelemetry.sdk.trace import SpanProcessor
from opentelemetry.trace import Span


class BaggageSpanProcessor(SpanProcessor):
    """
    Span processor that automatically copies baggage entries to span attributes.

    This makes baggage visible in trace UIs (Jaeger, Grafana, DataDog, etc.)
    without manually calling ctx.set_attribute() for each baggage entry.

    Example:
        Enable in init()
        ```python
        init(
            service='my-app',
            baggage=True  # Uses default 'baggage.' prefix
        )

        # Now baggage automatically appears as span attributes
        with with_baggage({'tenant.id': 't1', 'user.id': 'u1'}):
            # Span has baggage.tenant.id and baggage.user.id attributes!
            pass
        ```

    Example:
        Custom prefix
        ```python
        init(
            service='my-app',
            baggage='ctx'  # Uses 'ctx.' prefix
        )
        # Creates attributes: ctx.tenant.id, ctx.user.id
        ```
    """

    def __init__(self, prefix: str = "baggage.") -> None:
        """
        Initialize the baggage span processor.

        Args:
            prefix: Prefix for baggage attributes (default: 'baggage.')
        """
        self._prefix = prefix

    def on_start(self, span: Span, parent_context: context.Context | None = None) -> None:
        """Copy baggage entries to span attributes when span starts."""
        # Prefer explicitly provided parent context, otherwise fall back to current
        active_context = parent_context or context.get_current()
        baggage = propagation.get_all(active_context)
        if not baggage:
            return

        # Copy all baggage entries to span attributes
        # Baggage is a Mapping, so we can iterate over it directly
        for key, value in baggage.items():
            span.set_attribute(f"{self._prefix}{key}", str(value))

    def on_end(self, span: Any) -> None:
        """No-op when span ends."""
        pass

    def shutdown(self) -> None:
        """No-op shutdown."""
        pass

    def force_flush(self, _timeout_millis: int = 30000) -> bool:
        """No-op force flush."""
        return True
