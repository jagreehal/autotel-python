"""
autotel: Ergonomic OpenTelemetry instrumentation for Python

One-line initialization, ergonomic decorators, and production-ready by default.
"""

from .__version__ import __version__
from .baggage_span_processor import BaggageSpanProcessor
from .business_baggage import (
    BaggageFieldDefinition,
    BusinessBaggage,
    BusinessBaggageConfig,
    SafeBaggageSchema,
    configure_business_baggage,
    create_safe_baggage_schema,
    define_business_baggage,
    get_business_baggage,
)
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
from .functional import trace as trace_func
from .gen_ai_cost import (
    GEN_AI_COST_ATTRIBUTE,
    MODEL_PRICING,
    ModelPricing,
    TokenUsage,
    estimate_llm_cost,
    record_llm_cost,
)
from .gen_ai_events import (
    record_gen_ai_retry,
    record_prompt_sent,
    record_response_received,
    record_stream_first_token,
    record_tool_call,
)
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
from .messaging import (
    ConsumerContext,
    ConsumerGroupState,
    ConsumerGroupTrackingConfig,
    DLQOptions,
    DLQReplayOptions,
    OrderingConfig,
    OutOfOrderInfo,
    PartitionAssignment,
    PartitionLag,
    ProducerContext,
    RebalanceEvent,
    clear_ordering_state,
    extract_trace_context,
    inject_trace_headers,
    record_consumer_lag,
    record_dlq,
    record_retry,
    trace_batch_consumer,
    trace_consumer,
    trace_producer,
)
from .messaging_adapters import (
    ConsumerAdapter,
    MessagingAdapter,
    ProducerAdapter,
    b3_context_extractor,
    cloudflare_queues_adapter,
    create_multi_format_extractor,
    datadog_context_extractor,
    default_multi_format_extractor,
    jaeger_context_extractor,
    nats_adapter,
    redis_streams_adapter,
    sqs_adapter,
    temporal_adapter,
    xray_context_extractor,
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
from .span_processors import (
    AttributeRedactingProcessor,
    AttributeRedactorConfig,
    FilteringSpanProcessor,
    SpanNameNormalizingProcessor,
    create_attribute_redactor,
    create_redacted_span,
    normalize_rest_api_span_name,
)
from .structured_error import (
    ParsedError,
    StructuredError,
    create_structured_error,
    get_structured_error_attributes,
    parse_error,
    record_structured_error,
    structured_error_to_json,
)
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
from .webhook import (
    CallbackContext,
    InMemoryTraceContextStore,
    ParkingLot,
    StoredTraceContext,
    TraceContextStore,
    create_correlation_key,
    create_parking_lot,
    to_span_context,
)
from .workflow import (
    Saga,
    SagaFailed,
    SagaStep,
    StepResult,
    Workflow,
    WorkflowStatus,
    WorkflowStep,
    trace_workflow,
)
from .workflow_distributed import (
    DistributedStepContext,
    DistributedWorkflowContext,
    WorkflowBaggage,
    WorkflowBaggageValues,
    create_workflow_headers,
    generate_workflow_id,
    get_workflow_progress,
    is_in_distributed_workflow,
    trace_distributed_step,
    trace_distributed_workflow,
)

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
    "trace_func",
    "with_new_context",
    "with_baggage",
    # GenAI Helpers
    "GEN_AI_COST_ATTRIBUTE",
    "MODEL_PRICING",
    "ModelPricing",
    "TokenUsage",
    "estimate_llm_cost",
    "record_llm_cost",
    "record_prompt_sent",
    "record_response_received",
    "record_gen_ai_retry",
    "record_tool_call",
    "record_stream_first_token",
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
    # Structured Errors
    "StructuredError",
    "ParsedError",
    "create_structured_error",
    "structured_error_to_json",
    "get_structured_error_attributes",
    "record_structured_error",
    "parse_error",
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
    "SpanNameNormalizingProcessor",
    "FilteringSpanProcessor",
    "AttributeRedactingProcessor",
    "AttributeRedactorConfig",
    "normalize_rest_api_span_name",
    "create_attribute_redactor",
    "create_redacted_span",
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
    # Messaging Decorators
    "trace_consumer",
    "trace_producer",
    "trace_batch_consumer",
    "inject_trace_headers",
    "extract_trace_context",
    # Messaging Context Classes
    "ProducerContext",
    "ConsumerContext",
    "DLQOptions",
    "DLQReplayOptions",
    "OrderingConfig",
    "OutOfOrderInfo",
    "ConsumerGroupState",
    "ConsumerGroupTrackingConfig",
    "PartitionAssignment",
    "PartitionLag",
    "RebalanceEvent",
    "clear_ordering_state",
    # Messaging Helpers (DLQ/Retry/Lag)
    "record_dlq",
    "record_retry",
    "record_consumer_lag",
    # Messaging Adapters
    "MessagingAdapter",
    "ProducerAdapter",
    "ConsumerAdapter",
    "nats_adapter",
    "temporal_adapter",
    "cloudflare_queues_adapter",
    "sqs_adapter",
    "redis_streams_adapter",
    # Context Extractors
    "datadog_context_extractor",
    "b3_context_extractor",
    "xray_context_extractor",
    "jaeger_context_extractor",
    "create_multi_format_extractor",
    "default_multi_format_extractor",
    # Business Baggage
    "BusinessBaggage",
    "BusinessBaggageConfig",
    "SafeBaggageSchema",
    "BaggageFieldDefinition",
    "configure_business_baggage",
    "define_business_baggage",
    "get_business_baggage",
    "create_safe_baggage_schema",
    # Workflow/Saga Tracing (Local)
    "Workflow",
    "WorkflowStatus",
    "WorkflowStep",
    "StepResult",
    "Saga",
    "SagaStep",
    "SagaFailed",
    "trace_workflow",
    # Distributed Workflow Tracing
    "trace_distributed_workflow",
    "trace_distributed_step",
    "DistributedWorkflowContext",
    "DistributedStepContext",
    "WorkflowBaggage",
    "WorkflowBaggageValues",
    "generate_workflow_id",
    "is_in_distributed_workflow",
    "get_workflow_progress",
    "create_workflow_headers",
    # Webhook/Parking Lot Pattern
    "ParkingLot",
    "InMemoryTraceContextStore",
    "TraceContextStore",
    "StoredTraceContext",
    "CallbackContext",
    "create_parking_lot",
    "create_correlation_key",
    "to_span_context",
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
