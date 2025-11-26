"""Debug mode for development and progressive development."""

import json
import logging
import os
from typing import Any

logger = logging.getLogger(__name__)


def is_production() -> bool:
    """
    Check if running in production environment.

    Returns:
        True if production, False otherwise
    """
    env = os.environ.get("ENVIRONMENT", "").lower()
    return env in ("production", "prod", "live")


def should_enable_debug(debug: bool | None = None) -> bool:
    """
    Determine if debug mode should be enabled.

    Args:
        debug: Explicit debug flag (None = auto-detect)

    Returns:
        True if debug should be enabled
    """
    if debug is False:
        return False

    if debug is True:
        return True

    # Auto-detect: enable in non-production environments
    return not is_production()


class DebugPrinter:
    """Prints debug information to console."""

    def __init__(self, enabled: bool = True):
        """
        Initialize debug printer.

        Args:
            enabled: Whether debug printing is enabled
        """
        self.enabled = enabled

    def print_span(self, span_data: dict[str, Any]) -> None:
        """Print span data to console."""
        if not self.enabled:
            return

        print(f"[DEBUG] Span: {span_data.get('name', 'unknown')}")
        if "attributes" in span_data:
            print(f"  Attributes: {json.dumps(span_data['attributes'], indent=2)}")
        if "status" in span_data:
            print(f"  Status: {span_data['status']}")

    def print_metric(self, metric_data: dict[str, Any]) -> None:
        """Print metric data to console."""
        if not self.enabled:
            return

        print(f"[DEBUG] Metric: {metric_data.get('name', 'unknown')}")
        if "value" in metric_data:
            print(f"  Value: {metric_data['value']}")
        if "attributes" in metric_data:
            print(f"  Attributes: {json.dumps(metric_data['attributes'], indent=2)}")

    def print_events(self, event_data: dict[str, Any]) -> None:
        """Print events event to console."""
        if not self.enabled:
            return

        print(f"[DEBUG] Analytics Event: {event_data.get('name', 'unknown')}")
        if "properties" in event_data:
            print(f"  Properties: {json.dumps(event_data['properties'], indent=2)}")


# Global debug printer
_debug_printer: DebugPrinter | None = None


def set_debug_printer(printer: DebugPrinter) -> None:
    """Set global debug printer."""
    global _debug_printer
    _debug_printer = printer


def get_debug_printer() -> DebugPrinter | None:
    """Get global debug printer."""
    return _debug_printer
