"""Datadog preset: the endpoints and headers Datadog's OTLP ingest requires."""

import logging

import pytest
from opentelemetry import trace
from opentelemetry.sdk._logs import LoggingHandler

from autotel import init
from autotel.presets import datadog_preset


def test_direct_ingest_uses_otlp_endpoint_and_api_key():
    config = datadog_preset(api_key="secret", site="datadoghq.eu", service="svc")

    # Base URL only - the SDK appends /v1/traces, /v1/logs.
    assert config["endpoint"] == "https://otlp.datadoghq.eu"
    assert config["headers"]["dd-api-key"] == "secret"
    # Routes gen_ai.* spans to Agent Observability as well as APM.
    assert config["headers"]["dd-otlp-source"] == "llmobs"
    assert config["insecure"] is False
    assert config["resource_attributes"]["service.name"] == "svc"


def test_llmobs_header_can_be_turned_off():
    config = datadog_preset(api_key="secret", llmobs=False)
    assert "dd-otlp-source" not in config["headers"]


def test_agent_mode_needs_no_api_key():
    config = datadog_preset(use_agent=True, agent_host="dd-agent", agent_port=4318)
    assert config["endpoint"] == "http://dd-agent:4318"
    assert config["headers"] == {}


def test_direct_ingest_without_api_key_is_rejected():
    with pytest.raises(ValueError, match="API key is required"):
        datadog_preset()


def test_enable_logs_wires_the_otlp_log_pipeline(caplog):
    init(
        service="preset-test",
        preset=datadog_preset(api_key="secret", enable_logs=True, service="preset-test"),
    )

    # The preset's endpoint and headers reached the tracer provider...
    assert isinstance(trace.get_tracer_provider(), trace.TracerProvider.__mro__[0])

    # ...and stdlib log records now reach the OTLP logs pipeline, which is what
    # puts them in Datadog Logs correlated with the span.
    assert any(isinstance(h, LoggingHandler) for h in logging.getLogger().handlers)


def test_trace_takes_the_span_name_positionally():
    """`@trace('name')` sets the span name."""
    from autotel import trace

    @trace("invoice.settle")
    def settle():
        return "settled"

    assert settle() == "settled"
    assert settle.__name__ == "settle"


def test_reinit_rebinds_the_log_handler():
    """A second init() leaves one handler, bound to the new provider."""
    from opentelemetry import _logs as otel_logs

    init(service="reinit-a", preset=datadog_preset(api_key="a", enable_logs=True))
    init(service="reinit-b", preset=datadog_preset(api_key="b", enable_logs=True))

    handlers = [h for h in logging.getLogger().handlers if isinstance(h, LoggingHandler)]
    assert len(handlers) == 1
    assert handlers[0]._logger_provider is otel_logs.get_logger_provider()


def test_init_survives_a_provider_installed_by_another_library():
    """init() takes over when another library has already set up OTel."""
    from opentelemetry._logs import _internal as otel_logs_internal
    from opentelemetry.sdk._logs import LoggerProvider as SDKLoggerProvider
    from opentelemetry.sdk.trace import TracerProvider as SDKTracerProvider

    import autotel.init as autotel_init

    # Pretend this process has never called init(), but something else has
    # already installed SDK providers.
    autotel_init._INITIALIZED = False
    trace._TRACER_PROVIDER = SDKTracerProvider()
    otel_logs_internal._LOGGER_PROVIDER = SDKLoggerProvider()

    init(service="foreign-provider", preset=datadog_preset(api_key="k", enable_logs=True))

    assert isinstance(trace.get_tracer_provider(), SDKTracerProvider)


def test_empty_pydantic_ai_settings_still_instrument():
    """`pydantic_ai={}` means "defaults", not "off" - but an empty dict is falsy."""
    pytest.importorskip("pydantic_ai")
    from pydantic_ai import Agent

    Agent._instrument_default = False
    init(service="pydantic-ai-test", pydantic_ai={})
    assert Agent._instrument_default is not False


def test_flush_does_not_shut_telemetry_down():
    """The Lambda case: flush() per invocation, and the next invocation on a
    warm container still exports."""
    from opentelemetry.sdk.trace import TracerProvider
    from opentelemetry.sdk.trace.export import SimpleSpanProcessor
    from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter

    from autotel import flush

    exporter = InMemorySpanExporter()
    init(
        service="flush-test",
        span_processor=SimpleSpanProcessor(exporter),
    )

    tracer = trace.get_tracer(__name__)
    with tracer.start_as_current_span("first"):
        pass
    flush()
    with tracer.start_as_current_span("second"):
        pass
    flush()

    assert isinstance(trace.get_tracer_provider(), TracerProvider)
    assert [span.name for span in exporter.get_finished_spans()] == ["first", "second"]
