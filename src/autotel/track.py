"""Global track() function for events."""

from typing import Any

from .events import Event

_event: Event | None = None


def set_event(event: Event) -> None:
    """Set global event instance."""
    global _event
    _event = event


def track(event_name: str, properties: dict[str, Any] | None = None) -> None:
    """
    Global track function for events.

    Auto-enriches events with trace context (trace_id, span_id, operation.name).

    Example:
        >>> from autotel import track
        >>> track("user.created", {"userId": "123"})

    Args:
        event_name: Event name
        properties: Event properties dictionary
    """
    if _event:
        _event.trackEvent(event_name, properties)
