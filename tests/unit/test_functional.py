"""Tests for functional API."""

from typing import Any

import pytest

from autotel import init, instrument, span, with_new_context
from autotel.exporters import InMemorySpanExporter
from autotel.processors import SimpleSpanProcessor


@pytest.fixture
def exporter() -> Any:
    """Create in-memory exporter for testing."""
    exp = InMemorySpanExporter()
    init(service="test", span_processor=SimpleSpanProcessor(exp))
    return exp


def test_instrument_batch(exporter: Any) -> None:
    """Test batch instrumentation."""
    operations = {
        "add": lambda a, b: a + b,
        "multiply": lambda a, b: a * b,
    }

    service = instrument(operations)
    assert service["add"](2, 3) == 5
    assert service["multiply"](2, 3) == 6

    spans = exporter.get_finished_spans()
    assert len(spans) == 2
    assert spans[0].name == "add"
    assert spans[1].name == "multiply"


def test_span_context_manager(exporter: Any) -> None:
    """Test span context manager."""
    with span("test.operation") as ctx:
        ctx.set_attribute("test", "value")
        assert ctx.span_id is not None

    spans = exporter.get_finished_spans()
    assert len(spans) == 1
    assert spans[0].name == "test.operation"
    assert spans[0].attributes is not None
    assert spans[0].attributes.get("test") == "value"


@pytest.mark.asyncio
async def test_span_context_manager_async(exporter: Any) -> None:
    """Test span context manager with async code."""

    async def async_operation() -> str:
        with span("async.operation") as ctx:
            ctx.set_attribute("async", True)
            return "done"

    result = await async_operation()
    assert result == "done"

    spans = exporter.get_finished_spans()
    assert len(spans) == 1
    assert spans[0].name == "async.operation"


def test_with_new_context(exporter: Any) -> None:
    """Test creating new root context."""
    # Get current trace ID
    with span("parent") as parent_ctx:
        parent_trace_id = parent_ctx.trace_id

        # Create new root context
        with with_new_context(), span("child") as child_ctx:
            # Should have different trace ID (new root)
            assert child_ctx.trace_id != parent_trace_id

    spans = exporter.get_finished_spans()
    assert len(spans) == 2
    # Spans may be returned in any order, check both exist
    span_names = {span.name for span in spans}
    assert "parent" in span_names
    assert "child" in span_names


def test_instrument_with_context_parameter(exporter: Any) -> None:
    """Test instrument with functions that need ctx parameter."""

    def create_user(ctx: Any, data: Any) -> Any:
        ctx.set_attribute("user.id", data["id"])
        return data

    def get_user(user_id: Any) -> Any:
        return {"id": user_id}

    service = instrument(
        {
            "create": create_user,
            "get": get_user,
        }
    )

    result = service["create"]({"id": "123"})
    assert result["id"] == "123"

    spans = exporter.get_finished_spans()
    assert len(spans) == 1
    assert spans[0].name == "create"
    assert spans[0].attributes is not None
    assert spans[0].attributes.get("user.id") == "123"
