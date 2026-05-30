"""Initialize autotel with OpenTelemetry SDK."""

from collections.abc import Mapping
from dataclasses import dataclass
from importlib import import_module
from typing import Any, Literal

from opentelemetry import _logs as otel_logs
from opentelemetry import metrics as otel_metrics
from opentelemetry import trace
from opentelemetry.exporter.otlp.proto.http._log_exporter import (
    OTLPLogExporter as HTTPLogExporter,
)
from opentelemetry.exporter.otlp.proto.http.metric_exporter import (
    OTLPMetricExporter as HTTPMetricExporter,
)
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter as HTTPExporter
from opentelemetry.sdk._logs import LoggerProvider, LogRecordProcessor
from opentelemetry.sdk._logs.export import BatchLogRecordProcessor
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import MetricReader, PeriodicExportingMetricReader
from opentelemetry.sdk.resources import SERVICE_NAME, SERVICE_VERSION, Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import (
    BatchSpanProcessor,
    SpanExporter,
    SpanProcessor,
)

# gRPC exporter is optional
try:
    from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import (
        OTLPSpanExporter as GRPCExporter,
    )
except ImportError:
    GRPCExporter = None  # type: ignore[assignment, misc]

try:
    from opentelemetry.exporter.otlp.proto.grpc.metric_exporter import (
        OTLPMetricExporter as GRPCMetricExporter,
    )
except ImportError:
    GRPCMetricExporter = None  # type: ignore[assignment, misc]

try:
    from opentelemetry.exporter.otlp.proto.grpc._log_exporter import (
        OTLPLogExporter as GRPCLogExporter,
    )
except ImportError:
    GRPCLogExporter = None  # type: ignore[assignment, misc]

from .env_config import parse_otlp_headers, parse_resource_attributes, resolve_config_from_env
from .events import EventSubscriber
from .sampling import AdaptiveSampler, AdaptiveSamplingProcessor
from .span_processors import (
    AttributeRedactingProcessor,
    AttributeRedactor,
    AttributeRedactorConfig,
    AttributeRedactorPreset,
    FilteringSpanProcessor,
    SpanFilter,
    SpanNameNormalizer,
    SpanNameNormalizerPreset,
    SpanNameNormalizingProcessor,
)

_INITIALIZED = False


@dataclass(frozen=True)
class DevtoolsConfig:
    """Resolved local autotel-devtools config."""

    enabled: bool
    endpoint: str | None
    host: str
    port: int


def _resolve_devtools_config(config: bool | Mapping[str, Any] | None) -> DevtoolsConfig:
    """Resolve the TypeScript-style devtools shortcut."""
    default_host = "127.0.0.1"
    default_port = 4318

    if not config:
        return DevtoolsConfig(
            enabled=False,
            endpoint=None,
            host=default_host,
            port=default_port,
        )

    if config is True:
        return DevtoolsConfig(
            enabled=True,
            endpoint=f"http://{default_host}:{default_port}",
            host=default_host,
            port=default_port,
        )

    enabled = bool(config.get("enabled", True))
    host = str(config.get("host", default_host))
    port = int(config.get("port", default_port))
    endpoint = str(config.get("endpoint", f"http://{host}:{port}"))

    return DevtoolsConfig(
        enabled=enabled,
        endpoint=endpoint if enabled else None,
        host=host,
        port=port,
    )


def _format_endpoint_url(
    endpoint: str,
    signal: Literal["traces", "metrics", "logs"],
    protocol: Literal["http", "grpc"],
) -> str:
    """Format an OTLP endpoint for the selected protocol."""
    if protocol == "grpc":
        for suffix in ("/v1/traces", "/v1/metrics", "/v1/logs", "/traces", "/metrics", "/logs"):
            if endpoint.endswith(suffix):
                return endpoint[: -len(suffix)]
        return endpoint

    suffix = f"/v1/{signal}"
    if endpoint.endswith(suffix):
        return endpoint
    return f"{endpoint.rstrip('/')}{suffix}"


def _instrument_pydantic_ai() -> None:
    """Enable Pydantic AI's built-in OpenTelemetry hooks when installed."""
    try:
        pydantic_ai_module = import_module("pydantic_ai")
    except ImportError as exc:
        raise ImportError(
            "Pydantic AI instrumentation requires the pydantic-ai extra. "
            "Install with: pip install 'autotel[pydantic-ai]'"
        ) from exc

    Agent = pydantic_ai_module.Agent
    Agent.instrument_all()


def _wrap_span_processor(
    processor: SpanProcessor,
    *,
    span_filter: SpanFilter | None,
    span_name_normalizer: SpanNameNormalizer | SpanNameNormalizerPreset | None,
    attribute_redactor: AttributeRedactorConfig | AttributeRedactorPreset | AttributeRedactor | None,
) -> SpanProcessor:
    """Apply safety/migration processors around a sink processor."""
    wrapped = processor
    if attribute_redactor is not None:
        wrapped = AttributeRedactingProcessor(wrapped, attribute_redactor)
    if span_filter is not None:
        wrapped = FilteringSpanProcessor(wrapped, span_filter)
    if span_name_normalizer is not None:
        wrapped = SpanNameNormalizingProcessor(wrapped, span_name_normalizer)
    return wrapped


def init(
    # NOTE: This function MUST remain synchronous (not async) for compatibility
    # with synchronous initialization patterns and framework integrations
    service: str | None = None,  # Now optional, can be set via OTEL_SERVICE_NAME
    *,
    # Event subscribers
    subscribers: list[EventSubscriber] | None = None,
    # Logger (bring your own)
    logger: Any | None = None,  # Any logger (logging, structlog, loguru, etc.)
    # Built-in instrumentation toggles
    instrumentation: list[str] | None = None,
    # OTLP configuration (can be set via env vars)
    endpoint: str | None = None,  # OTEL_EXPORTER_OTLP_ENDPOINT
    traces_endpoint: str | None = None,  # OTEL_EXPORTER_OTLP_TRACES_ENDPOINT
    metrics_endpoint: str | None = None,  # OTEL_EXPORTER_OTLP_METRICS_ENDPOINT
    logs_endpoint: str | None = None,  # OTEL_EXPORTER_OTLP_LOGS_ENDPOINT
    protocol: Literal["http", "grpc"] | None = None,  # OTEL_EXPORTER_OTLP_PROTOCOL
    headers: dict[str, str] | str | None = None,  # OTEL_EXPORTER_OTLP_HEADERS
    insecure: bool = True,  # Allow HTTP in dev
    devtools: bool | Mapping[str, Any] | None = None,
    # Service configuration
    service_version: str | None = None,
    environment: str | None = None,
    resource: Resource | None = None,
    resource_attributes: dict[str, str] | str | None = None,  # OTEL_RESOURCE_ATTRIBUTES
    # Advanced configuration
    batch_timeout: int = 5000,  # 5s batch timeout
    max_queue_size: int = 2048,
    max_export_batch_size: int = 512,
    span_processor_mode: Literal["batch", "simple"] = "batch",
    span_processor: SpanProcessor | None = None,  # For testing/custom configs
    span_processors: list[SpanProcessor] | None = None,  # Multiple processors
    span_exporters: list[SpanExporter] | None = None,  # Multiple exporters
    metric_readers: list[MetricReader] | None = None,  # Multiple metric readers
    log_record_processors: list[LogRecordProcessor] | None = None,  # Multiple log processors
    sampler: AdaptiveSampler | None = None,  # Adaptive sampling
    debug: bool | None = None,  # Debug mode (None = auto-detect)
    auto_flush: bool = False,  # Auto-flush for serverless
    preset: dict[str, Any] | None = None,  # Preset configuration (Datadog, Honeycomb)
    validation: dict[str, Any] | None = None,  # Validation configuration (ValidationConfig)
    metrics: bool | dict[str, Any] | None = None,  # Metrics configuration
    logs: bool | Literal["auto"] | None = None,  # OTLP logs configuration
    span_filter: SpanFilter | None = None,  # Drop unwanted spans before export
    span_name_normalizer: SpanNameNormalizer | SpanNameNormalizerPreset | None = None,
    attribute_redactor: AttributeRedactorConfig | AttributeRedactorPreset | AttributeRedactor | None = None,
    openllmetry: dict[str, Any] | None = None,  # OpenLLMetry configuration
    pydantic_ai: bool = False,  # Instrument all Pydantic AI agents
    baggage: bool | str | None = None,  # Auto-copy baggage to span attributes
) -> None:
    """
    Initialize autotel with OpenTelemetry SDK.

    Configuration priority (highest to lowest):
    1. Explicit parameters
    2. Environment variables (OTEL_SERVICE_NAME, OTEL_EXPORTER_OTLP_ENDPOINT, etc.)
    3. Defaults

    Example:
        >>> from autotel import init
        >>> init(service="my-app", endpoint="http://localhost:4318")

    Example with environment variables:
        >>> # Set OTEL_SERVICE_NAME=my-app and OTEL_EXPORTER_OTLP_ENDPOINT=http://localhost:4318
        >>> init()  # Reads from environment variables

    Args:
        service: Service name (or set OTEL_SERVICE_NAME)
        subscribers: Event subscribers (PostHog, Mixpanel, etc.)
        endpoint: Shared OTLP endpoint URL (or set OTEL_EXPORTER_OTLP_ENDPOINT)
        traces_endpoint: Trace-specific OTLP endpoint.
        metrics_endpoint: Metric-specific OTLP endpoint.
        logs_endpoint: Log-specific OTLP endpoint.
        protocol: OTLP protocol (or set OTEL_EXPORTER_OTLP_PROTOCOL)
        headers: Optional HTTP headers (or set OTEL_EXPORTER_OTLP_HEADERS).
            Accepts a dict or the standard comma-separated OTEL string format.
        insecure: Allow insecure connections (HTTP)
        devtools: Local autotel-devtools shortcut. True defaults to http://127.0.0.1:4318.
            Pass {"port": 4319} when 4318 is already in use.
        instrumentation: Enable built-in instrumentation modules (e.g., ["mcp"])
        service_version: Service version
        environment: Environment name
        resource: Existing OpenTelemetry resource to merge with autotel service metadata.
        resource_attributes: Additional resource attributes (or set OTEL_RESOURCE_ATTRIBUTES).
            Accepts a dict or the standard comma-separated OTEL string format.
        batch_timeout: Batch timeout in milliseconds
        max_queue_size: Maximum queue size
        max_export_batch_size: Maximum export batch size
        span_processor_mode: "batch" for production export, "simple" for immediate
            export in notebooks, scripts, and local demos.
        span_processor: Custom span processor (single, legacy param)
        span_processors: Multiple span processors (mutually exclusive with span_exporters)
        span_exporters: Multiple span exporters (wrapped in BatchSpanProcessor)
        metric_readers: Custom metric readers (e.g., OTLP + Prometheus)
        log_record_processors: Custom log record processors for OTEL logs
        sampler: Adaptive sampler
        debug: Enable debug mode (None = auto-detect)
        auto_flush: Auto-flush for serverless environments
        preset: Preset configuration (from presets module)
        validation: Validation configuration
        metrics: Metrics configuration. True creates an OTLP metric reader from endpoint.
            Defaults to True when devtools is enabled.
        logs: True creates an OTLP log processor from endpoint.
            Defaults to True when devtools is enabled.
        span_filter: Predicate returning True to keep a completed span.
        span_name_normalizer: Normalizer function or preset ("rest-api", "graphql", "minimal").
        attribute_redactor: Redaction preset/config/callable for span attributes.
        openllmetry: OpenLLMetry configuration
        pydantic_ai: Instrument all Pydantic AI agents via Agent.instrument_all().
        baggage: Automatically copy baggage entries to span attributes.
            - True: adds baggage with 'baggage.' prefix (e.g. baggage.tenant.id)
            - str: uses custom prefix (e.g. 'ctx' → ctx.tenant.id, '' → tenant.id)
            - None/False: disabled (default)
    """
    global _INITIALIZED

    # Resolve environment variables (standard OTEL env vars)
    env_config = resolve_config_from_env()

    # Merge configs: explicit config > env vars > defaults
    # Priority: explicit parameters override env vars
    if service is None:
        service = env_config.get("service")
    if endpoint is None:
        endpoint = env_config.get("endpoint")
    if traces_endpoint is None:
        traces_endpoint = env_config.get("traces_endpoint")
    if metrics_endpoint is None:
        metrics_endpoint = env_config.get("metrics_endpoint")
    if logs_endpoint is None:
        logs_endpoint = env_config.get("logs_endpoint")
    if protocol is None:
        protocol = env_config.get("protocol")
    if isinstance(headers, str):
        headers = parse_otlp_headers(headers)
    if headers is None and "headers" in env_config:
        headers = env_config["headers"]

    if isinstance(resource_attributes, str):
        resource_attributes = parse_resource_attributes(resource_attributes)

    resolved_devtools = _resolve_devtools_config(devtools)
    if resolved_devtools.enabled and endpoint is None:
        endpoint = resolved_devtools.endpoint
    if resolved_devtools.enabled and metrics is None:
        metrics = True
    if resolved_devtools.enabled and logs is None:
        logs = True

    # Merge resource attributes
    env_resource_attrs = env_config.get("resource_attributes", {})
    if resource_attributes:
        resource_attributes = {**env_resource_attrs, **resource_attributes}
    elif env_resource_attrs:
        resource_attributes = env_resource_attrs

    # Validate service name after environment variable resolution
    if service is None:
        raise ValueError(
            "Service name is required. Either pass service='my-app' "
            "or set OTEL_SERVICE_NAME environment variable."
        )

    # Apply preset configuration if provided (overrides env vars)
    if preset:
        endpoint = preset.get("endpoint", endpoint)
        protocol = preset.get("protocol", protocol)
        insecure = preset.get("insecure", insecure)
        preset_headers = preset.get("headers", {})
        headers = {**preset_headers, **headers} if headers else preset_headers
        preset_resource_attrs = preset.get("resource_attributes", {})
        resource_attributes = (
            {**preset_resource_attrs, **resource_attributes}
            if resource_attributes
            else preset_resource_attrs
        )

    # Set up validation if provided
    if validation:
        from .validation import ValidationConfig, Validator, set_validator

        validator = Validator(
            ValidationConfig(**validation) if isinstance(validation, dict) else validation
        )
        set_validator(validator)

    # Set up debug mode
    from .debug import DebugPrinter, set_debug_printer, should_enable_debug

    debug_enabled = should_enable_debug(debug)
    if debug_enabled:
        debug_printer = DebugPrinter(enabled=True)
        set_debug_printer(debug_printer)
    # Allow re-initialization for testing - shutdown existing provider if needed
    if _INITIALIZED:
        # Try to shutdown existing provider to allow re-initialization
        from contextlib import suppress

        try:
            existing_provider = trace.get_tracer_provider()
            if isinstance(existing_provider, TracerProvider):
                # Force flush all spans before shutdown
                with suppress(Exception):
                    # Get all span processors and flush them
                    if hasattr(existing_provider, "_span_processors"):
                        for processor in existing_provider._span_processors:  # noqa: SLF001
                            with suppress(Exception):
                                processor.force_flush(timeout_millis=1000)
                    existing_provider.shutdown()
                # Clear internal state to allow override (for testing)
                # This allows re-initialization in test environments
                with suppress(Exception):
                    if hasattr(trace, "_TRACER_PROVIDER"):
                        trace._TRACER_PROVIDER = None  # noqa: SLF001
            current_meter_provider = otel_metrics.get_meter_provider()
            if isinstance(current_meter_provider, MeterProvider):
                with suppress(Exception):
                    current_meter_provider.shutdown()

            current_logger_provider = otel_logs.get_logger_provider()
            if isinstance(current_logger_provider, LoggerProvider):
                with suppress(Exception):
                    current_logger_provider.shutdown()  # type: ignore[no-untyped-call]
        except Exception:
            pass  # Ignore errors during shutdown

    # Build resource attributes
    attrs: dict[str, str] = {
        SERVICE_NAME: service,
    }
    if service_version:
        attrs[SERVICE_VERSION] = service_version
    if environment:
        attrs["deployment.environment"] = environment
    if resource_attributes:
        attrs.update(resource_attributes)

    autotel_resource = Resource(attributes=attrs)
    resource = resource.merge(autotel_resource) if resource is not None else autotel_resource

    tracer_provider_kwargs: dict[str, Any] = {"resource": resource}
    if sampler:
        tracer_provider_kwargs["sampler"] = sampler.get_sampler()
    provider = TracerProvider(**tracer_provider_kwargs)

    # Build span processor pipeline (supports multiple processors/exporters)
    processors: list[SpanProcessor] = []

    if span_processors:
        processors.extend(span_processors)

    if span_processor and not span_processors:
        processors.append(span_processor)

    # Track if we should create a default OTLP exporter
    # Skip if debug mode is enabled and no explicit endpoint was provided
    # (user only wants console output, not OTLP export to localhost)
    endpoint_was_explicitly_set = endpoint is not None or traces_endpoint is not None

    if not processors:
        exporter_list: list[SpanExporter] = []

        if span_exporters:
            exporter_list.extend(span_exporters)
        elif endpoint_was_explicitly_set or not debug_enabled:
            # Only create default OTLP exporter if:
            # 1. Endpoint was explicitly provided, OR
            # 2. Debug mode is NOT enabled (need some exporter)
            # Set default endpoint if not provided
            if endpoint is None:
                endpoint = "http://localhost:4318"

            # Set default protocol if not provided
            if protocol is None:
                protocol = "http"

            trace_export_endpoint = traces_endpoint or endpoint
            exporter_endpoint = _format_endpoint_url(trace_export_endpoint, "traces", protocol)

            # Create default exporter
            if protocol == "http":
                exporter: Any = HTTPExporter(
                    endpoint=exporter_endpoint,
                    headers=headers or {},
                )
            else:
                if GRPCExporter is None:
                    raise ImportError(
                        "gRPC exporter not available. Install with: "
                        "pip install opentelemetry-exporter-otlp-proto-grpc"
                    )
                exporter = GRPCExporter(
                    endpoint=exporter_endpoint,
                    headers=headers or {},
                    insecure=insecure,
                )

            exporter_list.append(exporter)

        for exporter_item in exporter_list:
            if span_processor_mode == "simple":
                from .processors import SimpleSpanProcessor

                processors.append(SimpleSpanProcessor(exporter_item))
            else:
                processors.append(
                    BatchSpanProcessor(
                        exporter_item,
                        max_queue_size=max_queue_size,
                        schedule_delay_millis=batch_timeout,
                        max_export_batch_size=max_export_batch_size,
                    )
                )

    # Wrap processors with adaptive sampling if configured
    if sampler and processors:
        processors = [AdaptiveSamplingProcessor(sampler, processor) for processor in processors]

    # Add baggage span processor if enabled
    # baggage can be True, a string (including empty string ""), or False/None
    if baggage is not False and baggage is not None:
        from .baggage_span_processor import BaggageSpanProcessor

        # Determine prefix
        # If baggage is a string, use it as prefix (empty string means no prefix)
        # If baggage is True, use default "baggage." prefix
        prefix = (f"{baggage}." if baggage else "") if isinstance(baggage, str) else "baggage."

        baggage_processor = BaggageSpanProcessor(prefix=prefix)
        processors.append(baggage_processor)

    # Add console exporter when debug is enabled (in addition to other processors)
    if debug_enabled:
        from .exporters import ConsoleSpanExporter
        from .processors import SimpleSpanProcessor

        processors.append(SimpleSpanProcessor(ConsoleSpanExporter()))

    for processor in processors:
        provider.add_span_processor(
            _wrap_span_processor(
                processor,
                span_filter=span_filter,
                span_name_normalizer=span_name_normalizer,
                attribute_redactor=attribute_redactor,
            )
        )

    # Set provider - use internal API for test re-initialization
    # OpenTelemetry doesn't allow overriding via set_tracer_provider(), so we bypass it in tests
    current_provider = trace.get_tracer_provider()
    is_sdk_provider = isinstance(current_provider, TracerProvider)

    if _INITIALIZED or is_sdk_provider:
        # Force override for testing by directly setting internal state
        # This bypasses OpenTelemetry's "no override" check
        with suppress(Exception):
            if hasattr(trace, "_TRACER_PROVIDER"):
                # Directly set the provider to bypass the override check
                trace._TRACER_PROVIDER = provider
            else:
                # Fallback to normal method if internal API not available
                trace.set_tracer_provider(provider)
    else:
        # Normal initialization - use standard API
        trace.set_tracer_provider(provider)

    resolved_endpoint = endpoint or "http://localhost:4318"
    resolved_protocol: Literal["http", "grpc"] = protocol or "http"

    meter_provider: MeterProvider | None = None
    resolved_metric_readers = list(metric_readers or [])
    if metrics is True:
        metric_exporter_endpoint = _format_endpoint_url(
            metrics_endpoint or resolved_endpoint,
            "metrics",
            resolved_protocol,
        )
        if resolved_protocol == "grpc":
            if GRPCMetricExporter is None:
                raise ImportError(
                    "gRPC metric exporter not available. Install with: "
                    "pip install opentelemetry-exporter-otlp-proto-grpc"
                )
            metric_exporter: Any = GRPCMetricExporter(
                endpoint=metric_exporter_endpoint,
                headers=headers or {},
                insecure=insecure,
            )
        else:
            metric_exporter = HTTPMetricExporter(
                endpoint=metric_exporter_endpoint,
                headers=headers or {},
            )
        resolved_metric_readers.append(PeriodicExportingMetricReader(metric_exporter))

    if resolved_metric_readers:
        meter_provider = MeterProvider(resource=resource, metric_readers=resolved_metric_readers)
        otel_metrics.set_meter_provider(meter_provider)
        from .shutdown import set_meter_provider_for_shutdown

        set_meter_provider_for_shutdown(meter_provider)

    logger_provider: LoggerProvider | None = None
    resolved_log_processors = list(log_record_processors or [])
    if logs is True:
        log_exporter_endpoint = _format_endpoint_url(
            logs_endpoint or resolved_endpoint,
            "logs",
            resolved_protocol,
        )
        if resolved_protocol == "grpc":
            if GRPCLogExporter is None:
                raise ImportError(
                    "gRPC log exporter not available. Install with: "
                    "pip install opentelemetry-exporter-otlp-proto-grpc"
                )
            log_exporter: Any = GRPCLogExporter(
                endpoint=log_exporter_endpoint,
                headers=headers or {},
                insecure=insecure,
            )
        else:
            log_exporter = HTTPLogExporter(
                endpoint=log_exporter_endpoint,
                headers=headers or {},
            )
        resolved_log_processors.append(BatchLogRecordProcessor(log_exporter))

    if resolved_log_processors:
        logger_provider = LoggerProvider(resource=resource)
        for processor in resolved_log_processors:
            logger_provider.add_log_record_processor(processor)
        otel_logs.set_logger_provider(logger_provider)
        from .shutdown import set_logger_provider_for_shutdown

        set_logger_provider_for_shutdown(logger_provider)

    # Initialize event tracking if subscribers provided
    if subscribers:
        import asyncio

        from .events import Event
        from .track import set_event

        event = Event(subscribers=subscribers)
        set_event(event)

        # Register for shutdown
        from .shutdown import set_event_for_shutdown

        set_event_for_shutdown(event)

        # Start event worker (non-blocking)
        # Note: Event worker will start automatically when first track() is called
        # if no event loop is available. For async contexts, call event.start() explicitly.
        try:
            loop = asyncio.get_running_loop()
            # If loop is running, schedule start
            loop.create_task(event.start())
        except RuntimeError:
            # No running loop - event.start() will be called when needed
            # or can be called explicitly in async context
            pass

    # Set up metrics if provided
    if isinstance(metrics, dict):
        from .metrics import Metric, set_metrics

        metrics_instance = Metric(**metrics) if isinstance(metrics, dict) else metrics
        set_metrics(metrics_instance)
        # Register for shutdown
        from .shutdown import set_metrics_for_shutdown

        set_metrics_for_shutdown(metrics_instance)

    # Instrument logger if provided (bring your own logger)
    if logger:
        from .logging import instrument_logger

        instrument_logger(logger)

    # Set up OpenLLMetry if provided
    if openllmetry:
        from .openllmetry import configure_openllmetry

        configure_openllmetry(
            api_endpoint=openllmetry.get("api_endpoint", "https://api.traceloop.com"),
            api_key=openllmetry.get("api_key"),
            enabled=openllmetry.get("enabled", True),
        )

    if pydantic_ai:
        _instrument_pydantic_ai()

    # Enable built-in instrumentation
    if instrumentation and "mcp" in instrumentation:
        from .mcp import enable_mcp_auto_instrumentation

        enable_mcp_auto_instrumentation(logger=logger)

    # Set up auto-flush for serverless
    if auto_flush:
        from .serverless import auto_flush_if_serverless
        from .shutdown import shutdown_sync

        auto_flush_if_serverless(shutdown_sync)

    # Set initialized flag
    _INITIALIZED = True
