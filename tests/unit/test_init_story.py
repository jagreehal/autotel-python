"""Executable story tests for autotel initialization ergonomics."""

import sys
from typing import Any

import pytest
from opentelemetry import _logs as otel_logs
from opentelemetry import metrics as otel_metrics
from opentelemetry import trace
from opentelemetry.sdk._logs import LoggerProvider
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor

from autotel import init, span
from autotel.exporters import InMemorySpanExporter
from autotel.processors import SimpleSpanProcessor

story = pytest.importorskip("executable_stories").story


def test_devtools_true_configures_local_otlp_endpoint_story() -> None:
    story.init(
        "devtools=True configures the local OTLP traces endpoint",
        tags=["init", "devtools", "otlp"],
    )

    story.given("a developer wants local autotel-devtools tracing")
    story.code("Initializer", 'init(service="story-devtools", devtools=True)', lang="python")

    story.when("autotel is initialized with devtools enabled")
    init(service="story-devtools", devtools=True)

    story.then("the trace exporter points to the default local devtools receiver")
    assert _trace_exporter_endpoint() == "http://127.0.0.1:4318/v1/traces"

    story.and_("metrics and logs are enabled for the same receiver")
    assert "http://127.0.0.1:4318/v1/metrics" in _metric_exporter_endpoints()
    assert _log_exporter_endpoint() == "http://127.0.0.1:4318/v1/logs"


def test_devtools_port_override_avoids_4318_conflicts_story() -> None:
    story.init(
        "devtools port override avoids local 4318 conflicts",
        tags=["init", "devtools", "otlp"],
    )

    story.given("port 4318 is already used by another local process")
    story.kv("Alternative port", "4319")

    story.when("autotel is initialized with a devtools port override")
    init(service="story-devtools-port", devtools={"port": 4319})

    story.then("the trace exporter points to the requested local receiver")
    assert _trace_exporter_endpoint() == "http://127.0.0.1:4319/v1/traces"


def test_simple_processor_mode_is_immediate_for_notebooks_story() -> None:
    story.init(
        "simple span processor mode exports immediately for notebooks",
        tags=["init", "notebooks", "otlp"],
    )

    story.given("a short-lived notebook or script")
    story.and_("an OTLP endpoint without the signal path")
    story.code(
        "Initializer",
        "\n".join(
            [
                "init(",
                '    service="story-simple-mode",',
                '    endpoint="http://collector:4318",',
                '    span_processor_mode="simple",',
                "    debug=False,",
                ")",
            ]
        ),
        lang="python",
    )

    story.when("autotel is initialized in simple processor mode")
    init(
        service="story-simple-mode",
        endpoint="http://collector:4318",
        span_processor_mode="simple",
        debug=False,
    )

    story.then("the default exporter is wrapped by a SimpleSpanProcessor")
    processors = _span_processors()
    assert not any(isinstance(processor, BatchSpanProcessor) for processor in processors)
    assert _simple_processor_endpoints(processors) == ["http://collector:4318/v1/traces"]


def test_pydantic_ai_flag_instruments_all_agents_story(monkeypatch: Any) -> None:
    story.init(
        "pydantic_ai=True instruments every Pydantic AI agent",
        tags=["init", "pydantic-ai"],
    )

    class FakeAgent:
        called = False

        @classmethod
        def instrument_all(cls) -> None:
            cls.called = True

    class FakePydanticAi:
        Agent = FakeAgent

    story.given("Pydantic AI is importable")
    monkeypatch.setitem(sys.modules, "pydantic_ai", FakePydanticAi)

    story.when("autotel is initialized with pydantic_ai=True")
    init(
        service="story-pydantic-ai",
        span_processors=[SimpleSpanProcessor(InMemorySpanExporter())],
        pydantic_ai=True,
    )

    story.then("Agent.instrument_all is called")
    assert FakeAgent.called is True


def test_raw_otel_config_strings_can_be_reused_story() -> None:
    story.init(
        "raw OTel header and resource strings can be reused directly",
        tags=["init", "migration", "otlp"],
    )

    story.given("an app already has standard OpenTelemetry string configuration")
    story.code(
        "Initializer",
        "\n".join(
            [
                "init(",
                '    service="story-migration",',
                '    endpoint="http://collector:4318",',
                '    headers="authorization=Bearer token,x-honeycomb-team=abc123",',
                '    resource_attributes="service.version=1.2.3,team=payments",',
                ")",
            ]
        ),
        lang="python",
    )

    story.when("autotel is initialized with those raw OTel strings")
    init(
        service="story-migration",
        endpoint="http://collector:4318",
        headers="authorization=Bearer token,x-honeycomb-team=abc123",
        resource_attributes="service.version=1.2.3,team=payments",
    )

    story.then("the exporter receives parsed headers")
    exporter = _trace_exporter()
    assert exporter._headers["authorization"] == "Bearer token"  # noqa: SLF001
    assert exporter._headers["x-honeycomb-team"] == "abc123"  # noqa: SLF001

    story.and_("the resource keeps service metadata and custom attributes")
    provider = trace.get_tracer_provider()
    assert isinstance(provider, TracerProvider)
    assert provider.resource.attributes["service.name"] == "story-migration"
    assert provider.resource.attributes["service.version"] == "1.2.3"
    assert provider.resource.attributes["team"] == "payments"


def test_migration_safety_processors_reduce_cardinality_and_secrets_story() -> None:
    story.init(
        "migration safety processors reduce cardinality and secrets",
        tags=["init", "migration", "processors"],
    )

    exporter = InMemorySpanExporter()

    story.given("existing spans contain high-cardinality names and sensitive values")
    story.when("autotel is initialized with migration safety presets")
    init(
        service="story-safe-migration",
        span_processor=SimpleSpanProcessor(exporter),
        span_name_normalizer="rest-api",
        attribute_redactor="default",
        span_filter=lambda finished_span: "/health" not in finished_span.name,
    )

    with span("GET /users/123"):
        pass

    with span("POST /checkout"):
        trace.get_current_span().set_attribute("authorization", "Bearer secret-token")
        trace.get_current_span().set_attribute("customer.email", "ada@example.com")
        trace.get_current_span().set_attribute("payment.card", "4242 4242 4242 4242")

    with span("GET /health"):
        pass

    story.then("high-cardinality IDs are normalized before export")
    spans = exporter.get_finished_spans()
    names = [finished_span.name for finished_span in spans]
    assert "GET /users/:id" in names
    assert "GET /health" not in names

    story.and_("sensitive attributes are redacted before they leave the process")
    checkout_span = next(finished_span for finished_span in spans if finished_span.name == "POST /checkout")
    assert checkout_span.attributes["authorization"] == "[REDACTED]"
    assert checkout_span.attributes["customer.email"] == "a***@***.com"
    assert checkout_span.attributes["payment.card"] == "****4242"


def _span_processors() -> tuple[Any, ...]:
    provider = trace.get_tracer_provider()
    assert isinstance(provider, TracerProvider)
    return tuple(provider._active_span_processor._span_processors)  # noqa: SLF001


def _trace_exporter_endpoint() -> str:
    return str(_trace_exporter()._endpoint)  # noqa: SLF001


def _trace_exporter() -> Any:
    batch_processors = [
        processor
        for processor in _span_processors()
        if isinstance(processor, BatchSpanProcessor)
    ]
    assert len(batch_processors) == 1
    return batch_processors[0].span_exporter


def _metric_exporter_endpoints() -> set[str]:
    meter_provider = otel_metrics.get_meter_provider()
    assert isinstance(meter_provider, MeterProvider)
    return {
        str(reader._exporter._endpoint)  # noqa: SLF001
        for reader in getattr(meter_provider, "_all_metric_readers", [])
        if hasattr(reader, "_exporter")
    }


def _log_exporter_endpoint() -> str:
    logger_provider = otel_logs.get_logger_provider()
    assert isinstance(logger_provider, LoggerProvider)
    log_processor = logger_provider._multi_log_record_processor._log_record_processors[0]  # noqa: SLF001
    return str(log_processor._batch_processor._exporter._endpoint)  # noqa: SLF001


def _simple_processor_endpoints(processors: tuple[Any, ...]) -> list[str]:
    simple_processors = [
        processor
        for processor in processors
        if isinstance(processor, SimpleSpanProcessor)
    ]
    return [
        str(processor.span_exporter._endpoint)  # noqa: SLF001
        for processor in simple_processors
    ]
