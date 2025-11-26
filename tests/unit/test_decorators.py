"""Tests for @trace decorator."""

from typing import Any

import pytest

from autotel import init, trace
from autotel.exporters import InMemorySpanExporter
from autotel.processors import SimpleSpanProcessor


@pytest.fixture
def exporter() -> Any:
    """Create in-memory exporter for testing."""
    exp = InMemorySpanExporter()
    init(service="test", span_processor=SimpleSpanProcessor(exp))
    return exp


def test_trace_simple_function(exporter: Any) -> None:
    """Test tracing a simple function."""

    @trace
    def simple() -> Any:
        return "hello"

    result = simple()
    assert result == "hello"
    spans = exporter.get_finished_spans()
    assert len(spans) == 1
    assert spans[0].name == "simple"


@pytest.mark.asyncio
async def test_trace_async_function(exporter: Any) -> None:
    """Test tracing an async function."""

    @trace
    async def async_fn() -> Any:
        return "world"

    result = await async_fn()
    assert result == "world"
    spans = exporter.get_finished_spans()
    assert len(spans) == 1
    assert spans[0].name == "async_fn"


def test_trace_with_context(exporter: Any) -> None:
    """Test tracing with context parameter."""

    @trace
    def with_ctx(ctx: Any, value: Any) -> int:
        ctx.set_attribute("test.value", value)
        return value * 2  # type: ignore[no-any-return]

    result = with_ctx(5)  # type: ignore[call-arg]
    assert result == 10
    spans = exporter.get_finished_spans()
    assert len(spans) == 1
    assert spans[0].attributes is not None
    assert spans[0].attributes.get("test.value") == 5


def test_trace_records_exceptions(exporter: Any) -> None:
    """Test that exceptions are recorded."""

    @trace
    def failing() -> None:
        raise ValueError("test error")

    with pytest.raises(ValueError):
        failing()

        spans = exporter.get_finished_spans()
        assert len(spans) == 1
        from opentelemetry.trace import StatusCode

        assert spans[0].status.status_code == StatusCode.ERROR
