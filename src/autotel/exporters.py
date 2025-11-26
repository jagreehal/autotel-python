"""
OpenTelemetry Exporters

Re-exports commonly-needed OpenTelemetry exporters for development and debugging.

These exporters are already included in autotel's dependencies, so re-exporting
them provides a "one install is all you need" developer experience without any
bundle size impact.

Use these for:
- Development debugging (see spans in console)
- Progressive development (verify instrumentation works)
- Example applications (demonstrate tracing)
- Testing (capture spans for assertions)
"""

from opentelemetry.sdk.trace.export import ConsoleSpanExporter
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter

__all__ = ["ConsoleSpanExporter", "InMemorySpanExporter"]
