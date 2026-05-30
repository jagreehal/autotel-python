"""Tests for autotel initialization."""

import sys
from typing import Any

from opentelemetry import _logs as otel_logs
from opentelemetry import context, trace
from opentelemetry import metrics as otel_metrics
from opentelemetry.baggage import propagation
from opentelemetry.sdk._logs import LoggerProvider, LogRecordProcessor
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import InMemoryMetricReader
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor

from autotel import init, span
from autotel.exporters import InMemorySpanExporter
from autotel.processors import SimpleSpanProcessor


def _set_baggage_dict(ctx: Any, baggage_dict: Any) -> Any:
    """Helper to set multiple baggage entries from a dict."""
    new_context = ctx
    for key, value in baggage_dict.items():
        new_context = propagation.set_baggage(key, value, new_context)
    return new_context


class DummyLogRecordProcessor(LogRecordProcessor):
    """Test log record processor to verify registration."""

    def __init__(self: Any) -> None:
        self.seen = 0

    def on_emit(self: Any, _log_data: Any) -> None:  # noqa: ANN001
        self.seen += 1

    def shutdown(self: Any) -> None:  # pragma: no cover - simple stub
        return None

    def force_flush(self: Any, timeout_millis: int | None = None) -> bool:  # noqa: ARG002
        return True


def test_init_basic() -> None:
    """Test basic initialization."""
    init(service="test-service")
    provider = trace.get_tracer_provider()
    assert provider is not None


def test_init_with_custom_endpoint() -> None:
    """Test initialization with custom endpoint."""
    init(service="test", endpoint="http://custom:4318")
    provider = trace.get_tracer_provider()
    assert provider is not None


def test_init_with_resource_attributes() -> None:
    """Test initialization with resource attributes."""
    init(service="test", resource_attributes={"custom.key": "value"})
    provider = trace.get_tracer_provider()
    assert provider is not None


def test_init_accepts_raw_otel_resource_attribute_string() -> None:
    """Raw OTel env-style resource attributes should work as direct init input."""
    init(
        service="test",
        resource_attributes="service.version=1.2.3,deployment.environment=prod,team=payments",
    )

    provider = trace.get_tracer_provider()
    assert isinstance(provider, TracerProvider)
    assert provider.resource.attributes["service.name"] == "test"
    assert provider.resource.attributes["service.version"] == "1.2.3"
    assert provider.resource.attributes["deployment.environment"] == "prod"
    assert provider.resource.attributes["team"] == "payments"


def test_init_merges_existing_otel_resource() -> None:
    """Users migrating from raw OTel can pass their existing Resource."""
    existing_resource = Resource.create({
        "cloud.provider": "aws",
        "service.namespace": "checkout",
    })

    init(
        service="test",
        resource=existing_resource,
        resource_attributes={"service.version": "2.0.0"},
    )

    provider = trace.get_tracer_provider()
    assert isinstance(provider, TracerProvider)
    assert provider.resource.attributes["service.name"] == "test"
    assert provider.resource.attributes["service.version"] == "2.0.0"
    assert provider.resource.attributes["service.namespace"] == "checkout"
    assert provider.resource.attributes["cloud.provider"] == "aws"


def test_init_accepts_raw_otel_headers_string() -> None:
    """Raw OTel header strings should not need manual parsing during migration."""
    init(
        service="test",
        endpoint="http://collector:4318",
        headers="authorization=Bearer token,x-honeycomb-team=abc123",
    )

    provider = trace.get_tracer_provider()
    assert isinstance(provider, TracerProvider)
    processors = provider._active_span_processor._span_processors
    batch_processors = [p for p in processors if isinstance(p, BatchSpanProcessor)]

    assert len(batch_processors) == 1
    exporter = batch_processors[0].span_exporter
    assert exporter._headers["authorization"] == "Bearer token"
    assert exporter._headers["x-honeycomb-team"] == "abc123"


def test_init_with_baggage_true() -> None:
    """Test initialization with baggage=True (default prefix)."""
    exporter = InMemorySpanExporter()
    init(service="test", span_processor=SimpleSpanProcessor(exporter), baggage=True)

    # Set baggage and create span
    active_context = context.get_current()
    baggage_dict = {"tenant.id": "tenant-123"}
    context_with_baggage = _set_baggage_dict(active_context, baggage_dict)

    token = context.attach(context_with_baggage)
    try:
        with span("test.operation"):
            pass
    finally:
        context.detach(token)

    spans = exporter.get_finished_spans()
    assert len(spans) == 1
    assert spans[0].attributes is not None
    assert spans[0].attributes.get("baggage.tenant.id") == "tenant-123"


def test_init_with_baggage_custom_prefix() -> None:
    """Test initialization with baggage='custom' (custom prefix)."""
    exporter = InMemorySpanExporter()
    init(service="test", span_processor=SimpleSpanProcessor(exporter), baggage="ctx")

    # Set baggage and create span
    active_context = context.get_current()
    baggage_dict = {"tenant.id": "tenant-123"}
    context_with_baggage = _set_baggage_dict(active_context, baggage_dict)

    token = context.attach(context_with_baggage)
    try:
        with span("test.operation"):
            pass
    finally:
        context.detach(token)

    spans = exporter.get_finished_spans()
    assert len(spans) == 1
    assert spans[0].attributes is not None
    assert spans[0].attributes.get("ctx.tenant.id") == "tenant-123"


def test_init_with_baggage_empty_string() -> None:
    """Test initialization with baggage='' (no prefix)."""
    exporter = InMemorySpanExporter()
    init(service="test", span_processor=SimpleSpanProcessor(exporter), baggage="")

    # Set baggage and create span
    active_context = context.get_current()
    baggage_dict = {"tenant.id": "tenant-123"}
    context_with_baggage = _set_baggage_dict(active_context, baggage_dict)

    token = context.attach(context_with_baggage)
    try:
        with span("test.operation"):
            pass
    finally:
        context.detach(token)

    spans = exporter.get_finished_spans()
    assert len(spans) == 1
    assert spans[0].attributes is not None
    assert spans[0].attributes.get("tenant.id") == "tenant-123"


def test_init_with_baggage_false() -> None:
    """Test initialization with baggage=False (disabled)."""
    exporter = InMemorySpanExporter()
    init(service="test", span_processor=SimpleSpanProcessor(exporter), baggage=False)

    # Set baggage and create span
    active_context = context.get_current()
    baggage_dict = {"tenant.id": "tenant-123"}
    context_with_baggage = _set_baggage_dict(active_context, baggage_dict)

    token = context.attach(context_with_baggage)
    try:
        with span("test.operation"):
            pass
    finally:
        context.detach(token)

    spans = exporter.get_finished_spans()
    assert len(spans) == 1
    # Should not have baggage attributes when disabled
    assert spans[0].attributes is not None
    assert "baggage.tenant.id" not in spans[0].attributes
    assert "tenant.id" not in spans[0].attributes


def test_init_with_multiple_span_processors() -> None:
    """Ensure multiple span processors are supported."""
    exporter_one = InMemorySpanExporter()
    exporter_two = InMemorySpanExporter()
    processors = [
        SimpleSpanProcessor(exporter_one),
        SimpleSpanProcessor(exporter_two),
    ]

    init(service="test", span_processors=processors)  # type: ignore[arg-type]

    with span("test.operation"):
        pass

    assert len(exporter_one.get_finished_spans()) == 1
    assert len(exporter_two.get_finished_spans()) == 1


def test_init_with_multiple_span_exporters() -> None:
    """Ensure multiple span exporters are wrapped and exported."""
    exporter_one = InMemorySpanExporter()
    exporter_two = InMemorySpanExporter()

    init(
        service="test",
        span_exporters=[exporter_one, exporter_two],
        batch_timeout=10,
    )

    with span("test.exporters"):
        pass

    getattr(trace.get_tracer_provider(), "force_flush", lambda: None)()

    assert len(exporter_one.get_finished_spans()) == 1
    assert len(exporter_two.get_finished_spans()) == 1


def test_init_with_metric_readers() -> None:
    """Ensure custom metric readers are registered."""
    reader = InMemoryMetricReader()

    init(
        service="test",
        span_processors=[SimpleSpanProcessor(InMemorySpanExporter())],
        metric_readers=[reader],
    )

    provider = otel_metrics.get_meter_provider()
    assert isinstance(provider, MeterProvider)
    assert reader in getattr(provider, "_all_metric_readers", [])


def test_init_with_log_record_processors() -> None:
    """Ensure custom log record processors are registered."""
    processor = DummyLogRecordProcessor()

    init(
        service="test",
        span_processors=[SimpleSpanProcessor(InMemorySpanExporter())],
        log_record_processors=[processor],
    )

    provider = otel_logs.get_logger_provider()
    assert isinstance(provider, LoggerProvider)
    assert processor in provider._multi_log_record_processor._log_record_processors


def test_init_debug_only_no_otlp_exporter() -> None:
    """Debug mode without endpoint should only use ConsoleSpanExporter, no OTLP.

    When debug=True and no endpoint is explicitly provided, we should NOT
    create a default OTLP exporter (which would try to connect to localhost:4318).
    This prevents shutdown errors when no OTLP collector is running.
    """
    from opentelemetry.sdk.trace import TracerProvider
    from opentelemetry.sdk.trace.export import BatchSpanProcessor

    from autotel.exporters import ConsoleSpanExporter
    from autotel.processors import SimpleSpanProcessor as autotelSimpleSpanProcessor

    init(service="test-debug-only", debug=True)

    provider = trace.get_tracer_provider()
    assert isinstance(provider, TracerProvider)

    # Get processors from the provider
    processors = provider._active_span_processor._span_processors

    # Should have exactly one processor (ConsoleSpanExporter wrapped in SimpleSpanProcessor)
    # and NO BatchSpanProcessor (which would be wrapping an OTLP exporter)
    batch_processors = [p for p in processors if isinstance(p, BatchSpanProcessor)]
    console_processors = [
        p for p in processors if isinstance(p, autotelSimpleSpanProcessor)
    ]

    assert len(batch_processors) == 0, "Should not have OTLP BatchSpanProcessor in debug-only mode"
    assert len(console_processors) == 1, "Should have ConsoleSpanExporter in debug mode"
    assert isinstance(
        console_processors[0].span_exporter, ConsoleSpanExporter
    ), "Should use ConsoleSpanExporter"


def test_init_debug_with_explicit_endpoint_has_otlp() -> None:
    """Debug mode WITH explicit endpoint should have both OTLP and console exporters."""
    from opentelemetry.sdk.trace import TracerProvider
    from opentelemetry.sdk.trace.export import BatchSpanProcessor

    from autotel.processors import SimpleSpanProcessor as autotelSimpleSpanProcessor

    init(service="test-debug-with-endpoint", debug=True, endpoint="http://localhost:4318")

    provider = trace.get_tracer_provider()
    assert isinstance(provider, TracerProvider)

    processors = provider._active_span_processor._span_processors

    # Should have both OTLP (BatchSpanProcessor) and Console (SimpleSpanProcessor)
    batch_processors = [p for p in processors if isinstance(p, BatchSpanProcessor)]
    console_processors = [
        p for p in processors if isinstance(p, autotelSimpleSpanProcessor)
    ]

    assert len(batch_processors) == 1, "Should have OTLP BatchSpanProcessor when endpoint is explicit"
    assert len(console_processors) == 1, "Should have ConsoleSpanExporter in debug mode"


def test_init_devtools_true_uses_local_otlp_http_endpoint() -> None:
    """devtools=True should mirror the TypeScript local-dev shortcut."""
    from opentelemetry.sdk.trace import TracerProvider
    from opentelemetry.sdk.trace.export import BatchSpanProcessor

    init(service="test-devtools", devtools=True)

    provider = trace.get_tracer_provider()
    assert isinstance(provider, TracerProvider)

    processors = provider._active_span_processor._span_processors
    batch_processors = [p for p in processors if isinstance(p, BatchSpanProcessor)]

    assert len(batch_processors) == 1
    exporter = batch_processors[0].span_exporter
    assert exporter._endpoint == "http://127.0.0.1:4318/v1/traces"

    meter_provider = otel_metrics.get_meter_provider()
    assert isinstance(meter_provider, MeterProvider)
    metric_endpoints = {
        reader._exporter._endpoint
        for reader in getattr(meter_provider, "_all_metric_readers", [])
        if hasattr(reader, "_exporter")
    }
    assert "http://127.0.0.1:4318/v1/metrics" in metric_endpoints

    logger_provider = otel_logs.get_logger_provider()
    assert isinstance(logger_provider, LoggerProvider)
    log_processor = logger_provider._multi_log_record_processor._log_record_processors[0]
    assert log_processor._batch_processor._exporter._endpoint == "http://127.0.0.1:4318/v1/logs"


def test_init_devtools_allows_port_override() -> None:
    """devtools should support Cursor-safe local ports such as 4319."""
    from opentelemetry.sdk.trace import TracerProvider
    from opentelemetry.sdk.trace.export import BatchSpanProcessor

    init(service="test-devtools-port", devtools={"port": 4319})

    provider = trace.get_tracer_provider()
    assert isinstance(provider, TracerProvider)

    processors = provider._active_span_processor._span_processors
    batch_processors = [p for p in processors if isinstance(p, BatchSpanProcessor)]

    assert len(batch_processors) == 1
    exporter = batch_processors[0].span_exporter
    assert exporter._endpoint == "http://127.0.0.1:4319/v1/traces"


def test_init_devtools_allows_metrics_and_logs_opt_out() -> None:
    """devtools can be trace-only when users explicitly opt out."""
    init(
        service="test-devtools-trace-only",
        devtools=True,
        metrics=False,
        logs=False,
    )

    assert not isinstance(otel_metrics.get_meter_provider(), MeterProvider)
    assert not isinstance(otel_logs.get_logger_provider(), LoggerProvider)


def test_init_metrics_and_logs_true_use_signal_paths() -> None:
    """Metrics and logs should be one flag away for OTel migrants."""
    init(
        service="test-all-signals",
        endpoint="http://collector:4318",
        metrics=True,
        logs=True,
    )

    meter_provider = otel_metrics.get_meter_provider()
    assert isinstance(meter_provider, MeterProvider)
    metric_endpoints = {
        reader._exporter._endpoint
        for reader in getattr(meter_provider, "_all_metric_readers", [])
        if hasattr(reader, "_exporter")
    }
    assert "http://collector:4318/v1/metrics" in metric_endpoints

    logger_provider = otel_logs.get_logger_provider()
    assert isinstance(logger_provider, LoggerProvider)
    log_processor = logger_provider._multi_log_record_processor._log_record_processors[0]
    assert log_processor._batch_processor._exporter._endpoint == "http://collector:4318/v1/logs"


def test_init_supports_signal_specific_otlp_endpoints() -> None:
    """Raw OTel signal-specific endpoints should not require custom exporters."""
    init(
        service="test-signal-endpoints",
        traces_endpoint="https://traces.example.com/otlp",
        metrics_endpoint="https://metrics.example.com/otlp",
        logs_endpoint="https://logs.example.com/otlp",
        metrics=True,
        logs=True,
    )

    provider = trace.get_tracer_provider()
    assert isinstance(provider, TracerProvider)
    processors = provider._active_span_processor._span_processors
    batch_processors = [p for p in processors if isinstance(p, BatchSpanProcessor)]
    assert batch_processors[0].span_exporter._endpoint == "https://traces.example.com/otlp/v1/traces"

    meter_provider = otel_metrics.get_meter_provider()
    assert isinstance(meter_provider, MeterProvider)
    metric_endpoints = {
        reader._exporter._endpoint
        for reader in getattr(meter_provider, "_all_metric_readers", [])
        if hasattr(reader, "_exporter")
    }
    assert "https://metrics.example.com/otlp/v1/metrics" in metric_endpoints

    logger_provider = otel_logs.get_logger_provider()
    assert isinstance(logger_provider, LoggerProvider)
    log_processor = logger_provider._multi_log_record_processor._log_record_processors[0]
    assert log_processor._batch_processor._exporter._endpoint == "https://logs.example.com/otlp/v1/logs"


def test_init_http_endpoint_is_normalized_to_traces_path() -> None:
    """HTTP OTLP exporters need the signal-specific traces path."""
    from opentelemetry.sdk.trace import TracerProvider
    from opentelemetry.sdk.trace.export import BatchSpanProcessor

    init(service="test-http-path", endpoint="http://collector:4318")

    provider = trace.get_tracer_provider()
    assert isinstance(provider, TracerProvider)

    processors = provider._active_span_processor._span_processors
    batch_processors = [p for p in processors if isinstance(p, BatchSpanProcessor)]

    assert len(batch_processors) == 1
    exporter = batch_processors[0].span_exporter
    assert exporter._endpoint == "http://collector:4318/v1/traces"


def test_init_simple_span_processor_mode_exports_immediately() -> None:
    """Simple mode should use SimpleSpanProcessor for notebooks and scripts."""
    from opentelemetry.sdk.trace import TracerProvider
    from opentelemetry.sdk.trace.export import BatchSpanProcessor

    from autotel.processors import SimpleSpanProcessor as autotelSimpleSpanProcessor

    init(
        service="test-simple-mode",
        endpoint="http://collector:4318",
        span_processor_mode="simple",
        debug=False,
    )

    provider = trace.get_tracer_provider()
    assert isinstance(provider, TracerProvider)

    processors = provider._active_span_processor._span_processors
    batch_processors = [p for p in processors if isinstance(p, BatchSpanProcessor)]
    simple_processors = [
        p for p in processors if isinstance(p, autotelSimpleSpanProcessor)
    ]

    assert len(batch_processors) == 0
    assert len(simple_processors) == 1
    assert simple_processors[0].span_exporter._endpoint == "http://collector:4318/v1/traces"


def test_init_pydantic_ai_instruments_all_agents(monkeypatch: Any) -> None:
    """pydantic_ai=True should call Agent.instrument_all when available."""

    class FakeAgent:
        called = False

        @classmethod
        def instrument_all(cls) -> None:
            cls.called = True

    class FakePydanticAi:
        Agent = FakeAgent

    monkeypatch.setitem(sys.modules, "pydantic_ai", FakePydanticAi)

    init(
        service="test-pydantic-ai",
        span_processors=[SimpleSpanProcessor(InMemorySpanExporter())],
        pydantic_ai=True,
    )

    assert FakeAgent.called is True
