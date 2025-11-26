"""Tests for HTTP instrumentation helpers."""

from typing import Any

import pytest

from autotel import init
from autotel.exporters import InMemorySpanExporter
from autotel.http import http_instrumented, inject_trace_context, trace_http_request
from autotel.processors import SimpleSpanProcessor


@pytest.fixture
def exporter() -> Any:
    """Create in-memory exporter for testing."""
    exp = InMemorySpanExporter()
    init(service="test", span_processor=SimpleSpanProcessor(exp))
    return exp


def test_trace_http_request(exporter: Any) -> None:
    """Test manual HTTP request tracing."""
    with trace_http_request("GET", "https://api.example.com/users") as ctx:
        ctx.set_attribute("http.status_code", 200)

    spans = exporter.get_finished_spans()
    assert len(spans) == 1
    assert spans[0].name == "GET /users"
    assert spans[0].attributes is not None
    assert spans[0].attributes.get("http.method") == "GET"
    assert spans[0].attributes is not None
    assert spans[0].attributes.get("http.url") == "https://api.example.com/users"
    assert spans[0].attributes is not None
    assert spans[0].attributes.get("http.status_code") == 200


def test_inject_trace_context() -> None:
    """Test trace context injection."""
    # Need an active span for trace context injection
    from autotel import init, span
    from autotel.exporters import InMemorySpanExporter
    from autotel.processors import SimpleSpanProcessor

    init(service="test", span_processor=SimpleSpanProcessor(InMemorySpanExporter()))

    with span("test.operation"):
        headers = inject_trace_context()
        assert isinstance(headers, dict)
        # Should contain traceparent header when span is active
        assert "traceparent" in headers or len(headers) > 0


@pytest.mark.asyncio
async def test_http_instrumented_class_decorator(exporter: Any) -> None:
    """Test @http_instrumented class decorator."""

    @http_instrumented(slow_threshold_ms=1000)
    class ApiClient:
        async def get_user(self: Any, user_id: str) -> Any:  # noqa: ARG002
            """Mock HTTP GET."""

            # Simulate response object
            class Response:
                status_code = 200

            return Response()

        async def create_user(self: Any, data: dict[str, Any]) -> Any:  # noqa: ARG002
            """Mock HTTP POST."""

            class Response:
                status_code = 201

            return Response()

    client = ApiClient()
    await client.get_user("123")

    spans = exporter.get_finished_spans()
    assert len(spans) == 1
    assert spans[0].name == "GET get_user"
    assert spans[0].attributes is not None
    assert spans[0].attributes.get("http.method") == "GET"
    assert spans[0].attributes is not None
    assert spans[0].attributes.get("http.status_code") == 200


def test_extract_path() -> None:
    """Test URL path extraction."""
    from autotel.http import _extract_path

    assert _extract_path("https://api.example.com/users") == "/users"
    assert _extract_path("https://api.example.com/users/123") == "/users/123"
    assert _extract_path("https://api.example.com/") == "/"
