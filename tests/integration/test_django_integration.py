"""Integration tests for Django, sync and async."""

import asyncio
from typing import Any

import django
import pytest
from django.conf import settings
from django.http import HttpResponse, JsonResponse
from django.test import AsyncClient, Client
from django.urls import path
from opentelemetry import trace
from opentelemetry.trace import SpanKind, StatusCode

from autotel import InMemorySpanExporter, SimpleSpanProcessor, init

TRACE_ID = "0af7651916cd43dd8448eb211c80319c"
TRACEPARENT = f"00-{TRACE_ID}-b7ad6b7169203331-01"


def get_user(request: Any, user_id: int) -> JsonResponse:
    with trace.get_tracer("test").start_as_current_span("inner"):
        return JsonResponse({"id": user_id})


def health(request: Any) -> HttpResponse:
    return HttpResponse("ok")


def boom(request: Any) -> HttpResponse:
    raise RuntimeError("boom")


async def async_view(request: Any) -> HttpResponse:
    return HttpResponse("ok")


urlpatterns = [
    path("users/<int:user_id>", get_user),
    path("health", health),
    path("boom", boom),
    path("async", async_view),
]

if not settings.configured:
    settings.configure(
        ROOT_URLCONF=__name__,
        ALLOWED_HOSTS=["*"],
        MIDDLEWARE=["autotel.integrations.django.autotelMiddleware"],
        AUTOTEL={"SERVICE_NAME": "test-django", "SKIP_ROUTES": ["/health"]},
    )
    django.setup()


@pytest.fixture
def exporter() -> Any:
    exp = InMemorySpanExporter()
    init(service="test-django", span_processor=SimpleSpanProcessor(exp))
    return exp


def test_server_span_joins_the_caller_and_parents_handler_spans(exporter: Any) -> None:
    response = Client().get("/users/7?token=abc", headers={"traceparent": TRACEPARENT})

    assert response.status_code == 200
    spans = {s.name: s for s in exporter.get_finished_spans()}
    server = spans["GET /users/<int:user_id>"]
    assert server.kind is SpanKind.SERVER
    assert format(server.context.trace_id, "032x") == TRACE_ID
    assert spans["inner"].parent.span_id == server.context.span_id
    assert server.attributes["http.route"] == "/users/<int:user_id>"
    assert server.attributes["url.query"] == "token=REDACTED"


def test_view_exception_is_recorded(exporter: Any) -> None:
    client = Client(raise_request_exception=False)

    assert client.get("/boom").status_code == 500

    (span,) = exporter.get_finished_spans()
    assert span.status.status_code is StatusCode.ERROR
    assert span.attributes["error.type"] == "RuntimeError"


def test_unmatched_route_stays_unnamed(exporter: Any) -> None:
    Client().get("/wp-admin")

    (span,) = exporter.get_finished_spans()
    assert span.name == "GET"
    assert span.attributes["http.response.status_code"] == 404


def test_skip_routes(exporter: Any) -> None:
    Client().get("/health")

    assert exporter.get_finished_spans() == ()


def test_async_request(exporter: Any) -> None:
    response = asyncio.run(AsyncClient().get("/async"))

    assert response.status_code == 200
    (span,) = exporter.get_finished_spans()
    assert span.name == "GET /async"
