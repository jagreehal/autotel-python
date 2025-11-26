"""Testing utilities for autotel.

For basic exporters and processors, import directly from autotel:
- from autotel import InMemorySpanExporter, ConsoleSpanExporter
- from autotel import SimpleSpanProcessor, BatchSpanProcessor

For enhanced testing utilities:
- from autotel.testing import assert_trace_created, assert_trace_succeeded, etc.
"""

from .helpers import (
    AnalyticsCollector,
    assert_no_errors,
    assert_trace_created,
    assert_trace_duration,
    assert_trace_failed,
    assert_trace_succeeded,
    create_events_collector,
    create_mock_logger,
    create_trace_collector,
    get_span_attribute,
    get_trace_duration,
    wait_for_trace,
)

__all__ = [
    "assert_trace_created",
    "assert_trace_succeeded",
    "assert_trace_failed",
    "assert_no_errors",
    "get_trace_duration",
    "assert_trace_duration",
    "get_span_attribute",
    "create_mock_logger",
    "create_events_collector",
    "create_trace_collector",
    "wait_for_trace",
    "AnalyticsCollector",
]
