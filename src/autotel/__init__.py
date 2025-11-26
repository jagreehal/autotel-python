"""
autotel: Ergonomic OpenTelemetry instrumentation for Python

One-line initialization, ergonomic decorators, and production-ready by default.
"""

from .__version__ import __version__
from .baggage_span_processor import BaggageSpanProcessor
from .circuit_breaker import CircuitBreaker, CircuitState
from .context import TraceContext
from .db import instrument_database, trace_db_query
from .debug import (
    DebugPrinter,
    get_debug_printer,
    is_production,
    set_debug_printer,
    should_enable_debug,
)
from .decorators import trace
from .events import Event, EventSubscriber
from .exporters import ConsoleSpanExporter, InMemorySpanExporter
from .functional import instrument, span, with_baggage, with_new_context
from .helpers import (
    add_event,
    get_all_baggage,
    get_baggage,
    get_span_id,
    get_trace_id,
    record_exception,
    set_attribute,
    set_attributes,
    set_baggage_value,
)
from .http import http_instrumented, inject_trace_context, trace_http_request
from .init import init
from .logging import Logger, instrument_logger
from .mcp import (
    McpInstrumentationConfig,
    McpTraceMeta,
    activate_trace_context,
    enable_mcp_auto_instrumentation,
    extract_otel_context_from_meta,
    inject_otel_context_to_meta,
    instrument_mcp_client,
    instrument_mcp_server,
)
from .metrics import (
    Metric,
    MetricsCollector,
    create_counter,
    create_histogram,
    create_observable_gauge,
    create_up_down_counter,
    get_metrics,
    set_metrics,
)
from .openllmetry import configure_openllmetry
from .operation_context import get_operation_context, run_in_operation_context
from .pii_redaction import PIIRedactor
from .processors import BatchSpanProcessor, SimpleSpanProcessor
from .rate_limiter import RateLimiter
from .sampling import AdaptiveSampler, AdaptiveSamplingProcessor
from .semantic_helpers import trace_db, trace_http, trace_llm, trace_messaging
from .serverless import auto_flush_if_serverless, is_serverless, register_auto_flush
from .shutdown import shutdown, shutdown_sync
from .subscribers import EventSubscriber as EventSubscriberBase
from .subscribers import (
    PostHogSubscriber,
    SlackSubscriber,
    StreamingEventSubscriber,
    WebhookSubscriber,
)
from .testing.helpers import (
    assert_no_errors,
    assert_trace_created,
    assert_trace_duration,
    assert_trace_failed,
    assert_trace_succeeded,
    get_span_attribute,
    get_trace_duration,
)
from .trace_helpers import (
    create_deterministic_trace_id,
    finalize_span,
    flatten_metadata,
    get_active_context,
    get_active_span,
    get_tracer,
    run_with_span,
)
from .tracer_provider import (
    get_autotel_tracer,
    get_autotel_tracer_provider,
    set_autotel_tracer_provider,
)
from .track import set_event, track
from .validation import ValidationConfig, Validator, get_validator, set_validator

# Config is optional (requires pydantic)
try:
    from .config import autotelConfig
except ImportError:
    autotelConfig = None  # type: ignore[assignment, misc]

__all__ = [
    # Core API
    "init",
    "trace",
    "TraceContext",
    # Functional API
    "instrument",
    "span",
    "with_new_context",
    "with_baggage",
    # Convenience Helpers
    "set_attributes",
    "set_attribute",
    "add_event",
    "record_exception",
    "get_trace_id",
    "get_span_id",
    "get_baggage",
    "get_all_baggage",
    "set_baggage_value",
    # Baggage
    "BaggageSpanProcessor",
    # Production hardening
    "AdaptiveSampler",
    "AdaptiveSamplingProcessor",
    "RateLimiter",
    "CircuitBreaker",
    "CircuitState",
    # MCP
    "instrument_mcp_client",
    "instrument_mcp_server",
    "inject_otel_context_to_meta",
    "extract_otel_context_from_meta",
    "activate_trace_context",
    "McpInstrumentationConfig",
    "McpTraceMeta",
    "enable_mcp_auto_instrumentation",
    "PIIRedactor",
    # Event tracking
    "Event",
    "EventSubscriber",
    "EventSubscriberBase",
    "PostHogSubscriber",
    "SlackSubscriber",
    "StreamingEventSubscriber",
    "WebhookSubscriber",
    "track",
    "set_event",
    # Logging
    "Logger",
    "instrument_logger",
    # HTTP Instrumentation
    "http_instrumented",
    "trace_http_request",
    "inject_trace_context",
    # Database Instrumentation
    "instrument_database",
    "trace_db_query",
    # Testing Utilities
    "assert_trace_created",
    "assert_trace_succeeded",
    "assert_trace_failed",
    "assert_no_errors",
    "get_trace_duration",
    "assert_trace_duration",
    "get_span_attribute",
    # Lifecycle
    "shutdown",
    "shutdown_sync",
    # Configuration (optional, requires pydantic)
    "autotelConfig",
    # Exporters (for development and testing)
    "ConsoleSpanExporter",
    "InMemorySpanExporter",
    # Processors (for custom configurations)
    "SimpleSpanProcessor",
    "BatchSpanProcessor",
    # Operation Context
    "get_operation_context",
    "run_in_operation_context",
    # Trace Helpers
    "get_tracer",
    "get_active_span",
    "get_active_context",
    "run_with_span",
    "flatten_metadata",
    "create_deterministic_trace_id",
    "finalize_span",
    # Semantic Convention Helpers
    "trace_llm",
    "trace_db",
    "trace_http",
    "trace_messaging",
    # Isolated Tracer Provider
    "set_autotel_tracer_provider",
    "get_autotel_tracer_provider",
    "get_autotel_tracer",
    # Validation
    "ValidationConfig",
    "Validator",
    "get_validator",
    "set_validator",
    # Metrics
    "Metric",
    "MetricsCollector",
    "create_counter",
    "create_histogram",
    "create_up_down_counter",
    "create_observable_gauge",
    "get_metrics",
    "set_metrics",
    # Debug
    "DebugPrinter",
    "is_production",
    "should_enable_debug",
    "get_debug_printer",
    "set_debug_printer",
    # Serverless
    "is_serverless",
    "auto_flush_if_serverless",
    "register_auto_flush",
    # OpenLLMetry
    "configure_openllmetry",
    # Version
    "__version__",
]
