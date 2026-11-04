"""Behaviour shared with autotel-rust: env, metrics, logs, SLOs, signals, helpers."""

import asyncio
import json
import logging
import math
import os
import signal
import subprocess
import sys
import textwrap
from collections import Counter
from pathlib import Path
from typing import Any

import httpx
import pytest
from opentelemetry import metrics, trace
from opentelemetry.sdk._logs.export import InMemoryLogRecordExporter
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import InMemoryMetricReader, MetricExporter
from opentelemetry.trace import SpanKind, StatusCode

from autotel import (
    GenAiMetrics,
    InMemorySpanExporter,
    LogFile,
    SimpleSpanProcessor,
    SloTracker,
    fetch,
    init,
    record_evaluation,
    register_runtime_metrics,
    sanitize_query,
    trace_db,
    trace_db_query,
    trace_http,
    trace_http_request,
    trace_llm,
    trace_messaging,
)
from autotel.context import TraceContext
from autotel.env_config import signal_disabled
from autotel.logging import JsonWithTraceIds, configure_log_output
from autotel.testing import TraceCollector


class InMemoryMetricExporter(MetricExporter):
    def export(self, metrics_data: Any, timeout_millis: float = 10_000, **kwargs: Any) -> Any:
        return None

    def force_flush(self, timeout_millis: float = 10_000) -> bool:
        return True

    def shutdown(self, timeout_millis: float = 30_000, **kwargs: Any) -> None:
        return None


def memory_init(**kwargs: Any) -> InMemorySpanExporter:
    exporter = InMemorySpanExporter()
    init(service="p", span_processor=SimpleSpanProcessor(exporter), **kwargs)
    return exporter


def metric_points(reader: InMemoryMetricReader) -> dict[str, list[Any]]:
    data = reader.get_metrics_data()
    found: dict[str, list[Any]] = {}
    for resource in data.resource_metrics if data else []:
        for scope in resource.scope_metrics:
            for metric in scope.metrics:
                found.setdefault(metric.name, []).extend(metric.data.data_points)
    return found


# Environment


def test_unusable_protocol_fails_init_and_names_the_variable(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("OTEL_EXPORTER_OTLP_PROTOCOL", "carrier-pigeon")

    with pytest.raises(ValueError, match="OTEL_EXPORTER_OTLP_PROTOCOL=carrier-pigeon"):
        init(service="p")


def test_exporter_none_silences_one_signal(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("OTEL_TRACES_EXPORTER", "none")
    monkeypatch.setenv("OTEL_METRICS_EXPORTER", "none")
    monkeypatch.setenv("OTEL_LOGS_EXPORTER", "None")

    assert all(signal_disabled(s) for s in ("traces", "metrics", "logs"))  # type: ignore[arg-type]
    init(service="p", endpoint="http://collector:4318", metrics=True, logs=True)

    provider: Any = trace.get_tracer_provider()
    assert provider._active_span_processor._span_processors == ()  # noqa: SLF001


def test_init_says_where_telemetry_goes(caplog: pytest.LogCaptureFixture) -> None:
    with caplog.at_level(logging.INFO, logger="autotel"):
        init(service="orders", endpoint="https://user:secret@collector:4318")

    assert "orders: exporting traces to https://collector:4318/v1/traces, metrics" in caplog.text
    assert "logs to https://collector:4318/v1/logs over http" in caplog.text
    assert "secret" not in caplog.text


def test_otel_variables_beat_code(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("OTEL_SERVICE_NAME", "from-env")
    monkeypatch.setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://env-host:4318")
    monkeypatch.setenv("OTEL_EXPORTER_OTLP_HEADERS", "Authorization=Basic%20abc")
    monkeypatch.setenv("OTEL_RESOURCE_ATTRIBUTES", "service.name=wrong,team=a,service.version=9")
    seen: dict[str, Any] = {}
    module = sys.modules["autotel.init"]
    real = module.HTTPExporter

    def spy(**kwargs: Any) -> Any:
        seen.update(kwargs)
        return real(**kwargs)

    monkeypatch.setattr(module, "HTTPExporter", spy)

    init(
        service="from-code",
        endpoint="http://code-host:4318",
        headers={"Authorization": "code"},
        service_version="1",
        metrics=False,
        logs=False,
    )

    provider: Any = trace.get_tracer_provider()
    assert provider.resource.attributes["service.name"] == "from-env"
    assert provider.resource.attributes["service.version"] == "9"
    assert provider.resource.attributes["team"] == "a"
    assert seen["endpoint"] == "http://env-host:4318/v1/traces"
    assert seen["headers"] == {"Authorization": "Basic abc"}


def test_signal_endpoint_variable_is_used_as_is(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT", "https://x.io/custom/ingest")
    seen: dict[str, Any] = {}
    module = sys.modules["autotel.init"]
    real = module.HTTPExporter

    def spy(**kwargs: Any) -> Any:
        seen.update(kwargs)
        return real(**kwargs)

    monkeypatch.setattr(module, "HTTPExporter", spy)

    init(service="p", metrics=False, logs=False)

    assert seen["endpoint"] == "https://x.io/custom/ingest"


def test_an_endpoint_turns_on_every_signal(monkeypatch: pytest.MonkeyPatch) -> None:
    module = sys.modules["autotel.init"]
    monkeypatch.setattr(module, "HTTPMetricExporter", lambda **_: InMemoryMetricExporter())
    monkeypatch.setattr(module, "HTTPLogExporter", lambda **_: InMemoryLogRecordExporter())
    built = []
    real = module.BatchLogRecordProcessor
    monkeypatch.setattr(
        module, "BatchLogRecordProcessor", lambda e, **k: built.append(e) or real(e, **k)
    )

    init(service="p", endpoint="http://collector:4318")
    assert built
    assert isinstance(metrics.get_meter_provider(), MeterProvider)


def test_opting_out_and_presets_keep_signals_off(monkeypatch: pytest.MonkeyPatch) -> None:
    module = sys.modules["autotel.init"]
    calls: list[str] = []
    monkeypatch.setattr(module, "HTTPMetricExporter", lambda **_: calls.append("metrics"))
    monkeypatch.setattr(module, "HTTPLogExporter", lambda **_: calls.append("logs"))

    init(service="p", endpoint="http://collector:4318", metrics=False, logs=False)
    init(service="p", preset={"endpoint": "http://vendor:4318"})

    assert calls == []


def test_preset_endpoint_is_not_redirected_by_the_environment(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://env-host:4318")
    seen: dict[str, Any] = {}
    module = sys.modules["autotel.init"]
    real = module.HTTPExporter

    def spy(**kwargs: Any) -> Any:
        seen.update(kwargs)
        return real(**kwargs)

    monkeypatch.setattr(module, "HTTPExporter", spy)

    init(service="p", preset={"endpoint": "http://vendor:4318"})

    assert seen["endpoint"] == "http://vendor:4318/v1/traces"


def test_sampler_variable_reaches_the_sdk(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("OTEL_TRACES_SAMPLER", "always_off")
    exporter = memory_init()

    with trace.get_tracer("t").start_as_current_span("work"):
        pass

    assert exporter.get_finished_spans() == ()


# Metrics


def test_second_init_replaces_the_meter_provider() -> None:
    first, second = InMemoryMetricReader(), InMemoryMetricReader()
    memory_init(metric_readers=[first])
    memory_init(metric_readers=[second])

    with trace_db_query("SELECT", "users", "postgresql"):
        pass

    assert "db.client.operation.duration" in metric_points(second)


def test_client_helpers_record_duration_histograms() -> None:
    reader = InMemoryMetricReader()
    memory_init(metric_readers=[reader])

    with trace_db_query("SELECT", "users", "postgresql", "shop"):
        pass
    with trace_http_request("GET", "https://api.example.com/users/1"):
        pass

    points = metric_points(reader)
    (db,) = points["db.client.operation.duration"]
    assert db.attributes["db.system.name"] == "postgresql"
    assert db.attributes["db.namespace"] == "shop"
    (http,) = points["http.client.request.duration"]
    assert http.attributes["server.address"] == "api.example.com"
    assert "url.full" not in http.attributes


def test_server_duration_is_recorded_once_with_a_bounded_route() -> None:
    from fastapi import FastAPI
    from fastapi.testclient import TestClient

    from autotel.integrations.fastapi import autotelMiddleware

    reader = InMemoryMetricReader()
    memory_init(metric_readers=[reader])
    off = FastAPI(telemetry={"tracing": False, "metrics": False, "logs": False})
    native = FastAPI()
    for app in (off, native):
        app.add_middleware(autotelMiddleware)  # type: ignore[arg-type]

        @app.get("/items/{item}")
        def item_view(item: int) -> int:
            return item

    TestClient(off).get("/items/1")
    TestClient(off).get("/items/2")
    TestClient(off).get("/scan/one")
    TestClient(native).get("/items/1")

    points = metric_points(reader)["http.server.request.duration"]
    routes: Counter[str | None] = Counter()
    for point in points:
        routes[point.attributes.get("http.route")] += point.count
    assert routes == {"/items/{item}": 3, None: 1}


def test_gen_ai_metrics_and_evaluation() -> None:
    reader = InMemoryMetricReader()
    exporter = memory_init(metric_readers=[reader])
    ai = GenAiMetrics()

    ai.record_success("chat", "openai", "gpt", 120, 40, duration=1.5)
    ai.record_failure("chat", "openai", "gpt", duration=0.1, error_type="timeout")
    with trace.get_tracer("t").start_as_current_span("judge") as span:
        record_evaluation(TraceContext(span), "relevance", 0.9, "pass")

    points = metric_points(reader)
    assert sum(p.value for p in points["gen_ai.client.operation.count"]) == 2
    types = {p.attributes["gen_ai.token.type"]: p.sum for p in points["gen_ai.client.token.usage"]}
    assert types == {"input": 120, "output": 40}
    assert exporter.get_finished_spans()[0].attributes["gen_ai.evaluation.score.value"] == 0.9


def test_runtime_metrics() -> None:
    reader = InMemoryMetricReader()
    memory_init(metric_readers=[reader])

    register_runtime_metrics()

    points = metric_points(reader)
    assert {"cpython.gc.collections", "process.cpu.time", "process.thread.count"} <= set(points)


# Semantic helpers


@pytest.mark.parametrize(
    ("decorator", "kind"),
    [
        (trace_http(method="GET", url="https://x.io/a"), SpanKind.CLIENT),
        (trace_db(system="postgresql", operation="SELECT"), SpanKind.CLIENT),
        (trace_llm(model="m"), SpanKind.CLIENT),
        (trace_messaging(system="kafka", operation="publish"), SpanKind.PRODUCER),
        (trace_messaging(system="kafka", operation="process"), SpanKind.CONSUMER),
    ],
)
def test_async_helpers_work_without_ctx_and_set_a_kind(decorator: Any, kind: SpanKind) -> None:
    exporter = memory_init()

    @decorator
    async def work() -> int:
        return 1

    assert asyncio.run(work()) == 1
    assert exporter.get_finished_spans()[0].kind is kind


def test_helpers_hide_url_credentials() -> None:
    exporter = memory_init()

    @trace_http(method="GET", url="https://u:p@x.io/a?key=abc")
    def call() -> None:
        pass

    call()
    assert exporter.get_finished_spans()[0].attributes["url.full"] == "https://x.io/a?key=REDACTED"


# fetch


def test_fetch_injects_context_and_records_failure(monkeypatch: pytest.MonkeyPatch) -> None:
    exporter = memory_init()
    seen: dict[str, str] = {}

    def respond(request: httpx.Request) -> httpx.Response:
        seen.update(request.headers)
        return httpx.Response(503)

    original = httpx.AsyncClient

    def client(**kwargs: Any) -> httpx.AsyncClient:
        return original(transport=httpx.MockTransport(respond), **kwargs)

    monkeypatch.setattr(httpx, "AsyncClient", client)

    response = asyncio.run(fetch("GET", "https://api.example.com/a?key=abc"))

    assert response.status_code == 503
    (span,) = exporter.get_finished_spans()
    assert span.kind is SpanKind.CLIENT
    assert span.name == "GET"
    assert span.attributes["url.full"] == "https://api.example.com/a?key=REDACTED"
    assert span.attributes["error.type"] == "503"
    assert span.status.status_code is StatusCode.ERROR
    assert format(span.context.trace_id, "032x") in seen["traceparent"]


# SQL


@pytest.mark.parametrize(
    ("sql", "system", "expected"),
    [
        (
            "SELECT * FROM t WHERE a = 'x''y' AND b = 12.5",
            "",
            "SELECT * FROM t WHERE a = ? AND b = ?",
        ),
        ("SELECT 1 -- secret\nFROM t", "", "SELECT ? FROM t"),
        ("SELECT /* secret */ 1", "", "SELECT ?"),
        ('SELECT "col" FROM t', "postgresql", 'SELECT "col" FROM t'),
        ('SELECT "secret" FROM t', "mysql", "SELECT ? FROM t"),
        ("SELECT `col` FROM t # secret", "mysql", "SELECT `col` FROM t"),
        (r"SELECT 'it\'s' , 2", "mysql", "SELECT ? , ?"),
        (r"SELECT E'it\'s secret'", "postgresql", "SELECT ?"),
        ("SELECT $$secret$$, $tag$more$tag$", "postgresql", "SELECT ?, ?"),
        ("SELECT $1, col2 FROM t3", "postgresql", "SELECT $1, col2 FROM t3"),
        ("SELECT 'unterminated secret", "", "SELECT ?"),
        ("SELECT $$unterminated secret", "postgresql", "SELECT ?"),
    ],
)
def test_sanitize_query(sql: str, system: str, expected: str) -> None:
    assert sanitize_query(sql, system) == expected


def test_set_query_text_uses_the_span_dialect() -> None:
    exporter = memory_init()

    with trace_db_query("SELECT", "t", "mysql") as ctx:
        ctx.set_query_text('SELECT "secret"')

    assert exporter.get_finished_spans()[0].attributes["db.query.text"] == "SELECT ?"


# Log files


@pytest.fixture
def no_log_output() -> Any:
    level = logging.getLogger().level
    yield
    configure_log_output("p", None, False)
    logging.getLogger().setLevel(level)


def test_log_file_rotation_validation() -> None:
    with pytest.raises(ValueError, match="rotation"):
        LogFile("logs", rotation="weekly")  # type: ignore[arg-type]
    with pytest.raises(ValueError, match="max_files"):
        LogFile("logs", max_files=1)


def test_json_log_file_carries_trace_ids(tmp_path: Path, no_log_output: None) -> None:
    memory_init(log_file=LogFile(tmp_path, prefix="app", rotation="never", json=True))

    with trace.get_tracer("t").start_as_current_span("work") as span:
        logging.getLogger("orders").info("created")
    trace_id = format(span.get_span_context().trace_id, "032x")

    line = json.loads((tmp_path / "app.log").read_text().splitlines()[-1])
    assert line["message"] == "created"
    assert line["trace_id"] == trace_id


def test_log_file_from_env_and_reinit_replaces_it(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, no_log_output: None
) -> None:
    monkeypatch.setenv("AUTOTEL_LOG_FILE_DIR", str(tmp_path / "a"))
    monkeypatch.setenv("AUTOTEL_LOG_FILE_ROTATION", "hourly")
    memory_init()
    monkeypatch.setenv("AUTOTEL_LOG_FILE_DIR", str(tmp_path / "b"))
    memory_init()

    logging.getLogger("orders").info("once")

    assert (tmp_path / "a" / "p.log").read_text() == ""
    assert "once" in (tmp_path / "b" / "p.log").read_text()


def test_log_file_honours_redact_pii(tmp_path: Path, no_log_output: None) -> None:
    from autotel.redaction import set_redact_pii

    memory_init(log_file=LogFile(tmp_path, rotation="never"), redact_pii=True)
    try:
        logging.getLogger("orders").info("for jo@example.com")
    finally:
        set_redact_pii(False)

    assert "jo@example.com" not in (tmp_path / "p.log").read_text()


def test_json_formatter_outside_a_span() -> None:
    record = logging.LogRecord("n", logging.INFO, "", 0, "hi %s", ("there",), None)

    entry = json.loads(JsonWithTraceIds().format(record))

    assert entry["message"] == "hi there"
    assert "trace_id" not in entry


# SLOs


def test_slo_snapshot_and_alerts() -> None:
    slo = SloTracker("checkout", target=0.9, window_seconds=60)
    assert math.isnan(slo.snapshot().sli)
    assert slo.snapshot().burn_rate == 0

    for good in [True] * 8 + [False] * 2:
        slo.record(good)

    snapshot = slo.snapshot()
    assert (snapshot.good, snapshot.bad, snapshot.sli) == (8, 2, 0.8)
    assert snapshot.burn_rate == pytest.approx(2)
    assert SloTracker.evaluate(snapshot, snapshot, 1, 1).alerting
    assert not SloTracker.evaluate(snapshot, snapshot, 1, 3).alerting
    forecast = slo.forecast(10, 0.0)
    assert forecast.projected_sli == pytest.approx(0.9)
    assert not forecast.alerting


def test_slo_events_leave_the_window() -> None:
    slo = SloTracker("x", target=0.9, window_seconds=0)
    slo.record(False)
    assert slo.snapshot().bad == 0


def test_slo_rejects_an_impossible_target() -> None:
    with pytest.raises(ValueError, match="target"):
        SloTracker("x", target=1.0, window_seconds=60)


# Collector


def test_trace_collector() -> None:
    collector = TraceCollector("orders")
    tracer = trace.get_tracer("t")

    with tracer.start_as_current_span("root") as root:
        root.set_attribute("order.id", "42")
        with tracer.start_as_current_span("mid"), tracer.start_as_current_span("leaf"):
            pass

    span = collector.expect_span("root")
    assert {s.name for s in collector.descendants(span)} == {"mid", "leaf"}
    assert [s.name for s in collector.by_attributes({"order.id": "42"})] == ["root"]
    with pytest.raises(AssertionError, match="Recorded"):
        collector.expect_span("missing")


# Signals


def test_sigterm_flushes_then_terminates(tmp_path: Path) -> None:
    script = textwrap.dedent(
        f"""
        import os, signal
        from opentelemetry.sdk.trace.export import BatchSpanProcessor, SpanExporter, SpanExportResult
        from autotel import init, install_signal_handlers
        from opentelemetry import trace

        class ToFile(SpanExporter):
            def export(self, spans):
                with open({str(tmp_path / "out")!r}, "a") as f:
                    f.writelines(s.name + "\\n" for s in spans)
                return SpanExportResult.SUCCESS

        init(service="p", span_processor=BatchSpanProcessor(ToFile(), schedule_delay_millis=60000))
        install_signal_handlers()
        with trace.get_tracer("t").start_as_current_span("last-batch"):
            pass
        os.kill(os.getpid(), signal.SIGTERM)
        print("survived")
        """
    )

    result = subprocess.run(
        [sys.executable, "-c", script], capture_output=True, text=True, timeout=30, check=False
    )

    assert result.returncode == -signal.SIGTERM, result.stderr
    assert "survived" not in result.stdout
    assert (tmp_path / "out").read_text() == "last-batch\n"


def test_previous_signal_handler_still_runs() -> None:
    from autotel import install_signal_handlers

    calls: list[int] = []
    previous = signal.signal(signal.SIGUSR1, lambda number, frame: calls.append(number))
    try:
        memory_init()
        install_signal_handlers([signal.SIGUSR1])
        os.kill(os.getpid(), signal.SIGUSR1)
    finally:
        signal.signal(signal.SIGUSR1, previous)

    assert calls == [signal.SIGUSR1]
