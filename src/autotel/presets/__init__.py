"""Presets for common observability backends."""

from .datadog import datadog_preset
from .honeycomb import honeycomb_preset

__all__ = [
    "datadog_preset",
    "honeycomb_preset",
]
