"""Integration tests for Flask."""

from typing import Any

import pytest
from flask import Flask
from opentelemetry import trace
from opentelemetry.trace import SpanKind, StatusCode

from autotel import InMemorySpanExporter, SimpleSpanProcessor, init
from autotel.integrations.flask import init_autotel

TRACE_ID = "0af7651916cd43dd8448eb211c80319c"
TRACEPARENT = f"00-{TRACE_ID}-b7ad6b7169203331-01"


@pytest.fixture
def exporter() -> Any:
    exp = InMemorySpanExporter()
    init(service="test-flask", span_processor=SimpleSpanProcessor(exp))
    return exp


def build(**options: Any) -> Flask:
    app = Flask(__name__)
    init_autotel(app, **options)

    @app.get("/users/<int:user_id>")
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


def test_server_span_joins_the_caller_and_parents_handler_spans(exporter: Any) -> None:
    response = build().test_client().get("/users/7?token=abc", headers={"traceparent": TRACEPARENT})

    assert response.status_code == 200
    spans = {s.name: s for s in exporter.get_finished_spans()}
    server = spans["GET /users/<int:user_id>"]
    assert server.kind is SpanKind.SERVER
    assert format(server.context.trace_id, "032x") == TRACE_ID
    assert spans["inner"].parent.span_id == server.context.span_id
    assert server.attributes["http.route"] == "/users/<int:user_id>"
    assert server.attributes["http.response.status_code"] == 200
    assert server.attributes["url.query"] == "token=REDACTED"


def test_unmatched_route_stays_unnamed(exporter: Any) -> None:
    build().test_client().get("/wp-admin")

    (span,) = exporter.get_finished_spans()
    assert span.name == "GET"
    assert span.attributes["http.response.status_code"] == 404


def test_unhandled_exception_ends_the_span_once(exporter: Any) -> None:
    app = build()
    app.config["PROPAGATE_EXCEPTIONS"] = False

    assert app.test_client().get("/boom").status_code == 500

    (span,) = exporter.get_finished_spans()
    assert span.status.status_code is StatusCode.ERROR
    assert span.attributes["error.type"] == "RuntimeError"
    assert span.attributes["http.response.status_code"] == 500


def test_context_does_not_leak_between_requests(exporter: Any) -> None:
    client = build().test_client()
    client.get("/health")
    client.get("/health")

    first, second = exporter.get_finished_spans()
    assert first.context.trace_id != second.context.trace_id
    assert trace.get_current_span().get_span_context().is_valid is False


def test_skip_routes_and_response_header(exporter: Any) -> None:
    client = build(skip_routes=["/health"], trace_response_header=True).test_client()

    assert "traceparent" not in client.get("/health").headers
    assert exporter.get_finished_spans() == ()
    assert len(client.get("/users/1").headers["traceparent"].split("-")) == 4
