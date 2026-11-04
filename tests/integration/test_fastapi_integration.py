"""Integration tests for FastAPI and Starlette."""

from typing import Any

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from opentelemetry import trace
from opentelemetry.trace import SpanKind, StatusCode
from starlette.applications import Starlette
from starlette.responses import PlainTextResponse
from starlette.routing import Route

from autotel import InMemorySpanExporter, SimpleSpanProcessor, init
from autotel.integrations.fastapi import autotelMiddleware

TRACE_ID = "0af7651916cd43dd8448eb211c80319c"
TRACEPARENT = f"00-{TRACE_ID}-b7ad6b7169203331-01"
NATIVE_OFF = {"tracing": False, "metrics": False, "logs": False}


@pytest.fixture
def exporter() -> Any:
    exp = InMemorySpanExporter()
    init(service="test-api", span_processor=SimpleSpanProcessor(exp))
    return exp


def build(telemetry: dict[str, Any] | None = None, **options: Any) -> FastAPI:
    app = FastAPI(telemetry=telemetry)  # type: ignore[arg-type]
    app.add_middleware(autotelMiddleware, **options)  # type: ignore[arg-type]

    @app.get("/users/{user_id}")
    def get_user(user_id: int) -> dict[str, int]:
        with trace.get_tracer("test").start_as_current_span("inner"):
            return {"user_id": user_id}

    @app.get("/health")
    def health() -> str:
        return "ok"

    @app.get("/boom")
    def boom() -> None:
        raise RuntimeError("boom")

    return app


def server_spans(exporter: Any) -> list[Any]:
    return [s for s in exporter.get_finished_spans() if s.kind is SpanKind.SERVER]


@pytest.mark.parametrize("telemetry", [None, NATIVE_OFF], ids=["native", "middleware"])
def test_one_server_span_joined_to_the_caller(exporter: Any, telemetry: Any) -> None:
    client = TestClient(build(telemetry))

    response = client.get(
        "/users/123?token=secret&page=2",
        headers={"traceparent": TRACEPARENT, "user-agent": "probe/1"},
    )

    assert response.status_code == 200
    (span,) = server_spans(exporter)
    assert span.name == "GET /users/{user_id}"
    assert format(span.context.trace_id, "032x") == TRACE_ID
    attrs = span.attributes
    assert attrs["http.request.method"] == "GET"
    assert attrs["http.route"] == "/users/{user_id}"
    assert attrs["http.response.status_code"] == 200
    assert attrs["url.path"] == "/users/123"
    assert attrs["url.query"] == "token=REDACTED&page=REDACTED"
    assert attrs["user_agent.original"] == "probe/1"
    assert attrs["server.address"] == "testserver"
    assert "http.url" not in attrs


def test_handler_spans_are_children_of_the_server_span(exporter: Any) -> None:
    TestClient(build(NATIVE_OFF)).get("/users/1")

    (span,) = server_spans(exporter)
    inner = next(s for s in exporter.get_finished_spans() if s.name == "inner")
    assert inner.parent.span_id == span.context.span_id


def test_unmatched_route_never_names_the_span_after_the_path(exporter: Any) -> None:
    TestClient(build(NATIVE_OFF)).get("/wp-admin/setup.php")

    (span,) = server_spans(exporter)
    assert span.name == "GET"
    assert "http.route" not in span.attributes
    assert span.attributes["http.response.status_code"] == 404
    assert span.status.status_code is not StatusCode.ERROR


def test_server_error_marks_the_span(exporter: Any) -> None:
    client = TestClient(build(NATIVE_OFF), raise_server_exceptions=False)

    assert client.get("/boom").status_code == 500

    (span,) = server_spans(exporter)
    assert span.status.status_code is StatusCode.ERROR
    assert span.attributes["error.type"] == "RuntimeError"


def test_skip_routes_records_nothing(exporter: Any) -> None:
    TestClient(build(NATIVE_OFF, skip_routes=["/health"])).get("/health")

    assert server_spans(exporter) == []


def test_native_exclude_is_respected(exporter: Any) -> None:
    def exclude(scope: Any) -> bool:
        return bool(scope["path"] == "/health")

    TestClient(build({"exclude": exclude})).get("/health")

    assert server_spans(exporter) == []


@pytest.mark.parametrize("telemetry", [None, NATIVE_OFF], ids=["native", "middleware"])
def test_trace_response_header(exporter: Any, telemetry: Any) -> None:
    client = TestClient(build(telemetry, trace_response_header=True))

    header = client.get("/users/1", headers={"traceparent": TRACEPARENT}).headers["traceparent"]

    assert header.split("-")[1] == TRACE_ID


def test_forwarded_headers_need_opting_in(exporter: Any) -> None:
    headers = {"x-forwarded-for": "203.0.113.7, 10.0.0.1", "x-forwarded-proto": "https"}

    TestClient(build(NATIVE_OFF)).get("/health", headers=headers)
    TestClient(build(NATIVE_OFF, trust_forwarded_for=True)).get("/health", headers=headers)

    untrusted, trusted = server_spans(exporter)
    assert untrusted.attributes["client.address"] != "203.0.113.7"
    assert untrusted.attributes["url.scheme"] == "http"
    assert trusted.attributes["client.address"] == "203.0.113.7"
    assert trusted.attributes["url.scheme"] == "https"


def test_raw_query_keeps_values(exporter: Any) -> None:
    TestClient(build(NATIVE_OFF, raw_query=True)).get("/health?q=shoes")

    assert server_spans(exporter)[0].attributes["url.query"] == "q=shoes"


def test_plain_starlette_gets_a_route(exporter: Any) -> None:
    async def item(request: Any) -> PlainTextResponse:
        return PlainTextResponse("ok")

    app = Starlette(routes=[Route("/items/{id}", item)])
    app.add_middleware(autotelMiddleware)

    TestClient(app).get("/items/7")

    (span,) = server_spans(exporter)
    assert span.attributes["http.response.status_code"] == 200
    assert span.name == "GET /items/{id}"


def _span_processors() -> tuple[Any, ...]:
    provider: Any = trace.get_tracer_provider()
    return tuple(provider._active_span_processor._span_processors)  # noqa: SLF001


def test_fastapi_does_not_add_a_second_exporter(
    exporter: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    # On by default in FastAPI 0.142, opt-in by 0.162; this covers both.
    monkeypatch.setenv("FASTAPI_OTEL_AUTO_CONFIGURE", "true")
    monkeypatch.setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://collector:4318")
    monkeypatch.setenv("OTEL_METRICS_EXPORTER", "none")
    monkeypatch.setenv("OTEL_LOGS_EXPORTER", "none")
    before = _span_processors()

    with TestClient(build()) as client:
        client.get("/health")

    assert _span_processors() == before


def test_without_the_middleware_fastapi_exports_for_itself(
    exporter: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The control for the test above: this is the behaviour the middleware turns off."""
    # On by default in FastAPI 0.142, opt-in by 0.162; this covers both.
    monkeypatch.setenv("FASTAPI_OTEL_AUTO_CONFIGURE", "true")
    monkeypatch.setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://collector:4318")
    monkeypatch.setenv("OTEL_METRICS_EXPORTER", "none")
    monkeypatch.setenv("OTEL_LOGS_EXPORTER", "none")
    before = _span_processors()

    with TestClient(FastAPI()):
        assert len(_span_processors()) == len(before) + 1
