"""Integration tests for FastAPI."""

from typing import Any

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from autotel import InMemorySpanExporter, SimpleSpanProcessor, init
from autotel.integrations.fastapi import autotelMiddleware


@pytest.fixture
def exporter() -> Any:
    """Create in-memory exporter for testing."""
    exp = InMemorySpanExporter()
    return exp


@pytest.fixture
def app(exporter: Any) -> Any:
    """Create FastAPI app with autotel."""
    # Initialize autotel with test exporter
    init(service="test-api", span_processor=SimpleSpanProcessor(exporter))

    app = FastAPI()
    app.add_middleware(autotelMiddleware, service="test-api")  # type: ignore[arg-type]

    @app.get("/")
    def read_root() -> dict[str, str]:
        return {"message": "Hello World"}

    @app.get("/users/{user_id}")
    def get_user(user_id: int) -> dict[str, int | str]:
        return {"user_id": user_id, "name": "John Doe"}

    @app.post("/users")
    def create_user(user_data: dict[str, Any]) -> dict[str, Any]:
        result = {"id": 123}
        result.update(user_data)
        return result

    return app


def test_fastapi_middleware_traces_requests(app: Any, exporter: Any) -> None:
    """Test that FastAPI middleware traces requests."""
    client = TestClient(app)

    response = client.get("/")
    assert response.status_code == 200

    spans = exporter.get_finished_spans()
    assert len(spans) == 1
    assert spans[0].name == "GET /"
    assert spans[0].attributes is not None
    assert spans[0].attributes.get("http.method") == "GET"
    assert spans[0].attributes is not None
    assert spans[0].attributes.get("http.status_code") == 200


def test_fastapi_middleware_captures_route_params(app: Any, exporter: Any) -> None:
    """Test that middleware captures route parameters."""
    client = TestClient(app)

    response = client.get("/users/123")
    assert response.status_code == 200

    spans = exporter.get_finished_spans()
    assert len(spans) == 1
    assert spans[0].name == "GET /users/{user_id}"
    assert spans[0].attributes is not None
    assert spans[0].attributes.get("http.route") == "/users/{user_id}"


def test_fastapi_middleware_captures_post_request(app: Any, exporter: Any) -> None:
    """Test that middleware captures POST requests."""
    client = TestClient(app)

    response = client.post("/users", json={"name": "John", "email": "john@example.com"})
    assert response.status_code == 200

    spans = exporter.get_finished_spans()
    assert len(spans) == 1
    assert spans[0].attributes is not None
    assert spans[0].attributes.get("http.method") == "POST"
    assert spans[0].attributes is not None
    assert spans[0].attributes.get("http.status_code") == 200
