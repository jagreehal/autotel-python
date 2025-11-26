"""
OpenTelemetry Span Processors

Re-exports commonly-needed OpenTelemetry span processors for custom configurations.
"""

from opentelemetry.sdk.trace.export import BatchSpanProcessor, SimpleSpanProcessor

__all__ = ["SimpleSpanProcessor", "BatchSpanProcessor"]
