"""Tests for @trace decorator."""

import inspect
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


def test_trace_signature_excludes_ctx_parameter(exporter: Any) -> None:
    """Test that @trace rewrites signature to exclude ctx parameter."""

    @trace
    def with_ctx(ctx: Any, user_id: str, name: str = "default") -> dict[str, Any]:
        ctx.set_attribute("user.id", user_id)
        return {"user_id": user_id, "name": name}

    sig = inspect.signature(with_ctx)
    param_names = list(sig.parameters.keys())

    assert "ctx" not in param_names
    assert "user_id" in param_names
    assert "name" in param_names

    result = with_ctx("123", name="John")
    assert result == {"user_id": "123", "name": "John"}


@pytest.mark.asyncio
async def test_trace_async_signature_excludes_ctx(exporter: Any) -> None:
    """Test that @trace rewrites async function signature to exclude ctx."""

    @trace
    async def async_with_ctx(ctx: Any, item_id: int) -> dict[str, int]:
        ctx.set_attribute("item.id", item_id)
        return {"item_id": item_id}

    sig = inspect.signature(async_with_ctx)
    param_names = list(sig.parameters.keys())

    assert "ctx" not in param_names
    assert "item_id" in param_names

    result = await async_with_ctx(42)
    assert result == {"item_id": 42}


def test_trace_signature_preserved_without_ctx(exporter: Any) -> None:
    """Test that @trace preserves signature for functions without ctx."""

    @trace
    def no_ctx(user_id: str, name: str = "default") -> dict[str, Any]:
        return {"user_id": user_id, "name": name}

    sig = inspect.signature(no_ctx)
    param_names = list(sig.parameters.keys())

    assert "user_id" in param_names
    assert "name" in param_names
    assert len(param_names) == 2


def test_trace_preserves_parameter_order_and_defaults(exporter: Any) -> None:
    """Test that parameter order and defaults are preserved after ctx removal."""

    @trace
    def complex_fn(
        ctx: Any,
        required_param: str,
        optional_param: int = 10,
        *args: Any,
        kwonly: str = "default",
        **kwargs: Any,
    ) -> str:
        return f"{required_param}-{optional_param}-{kwonly}"

    sig = inspect.signature(complex_fn)
    params = list(sig.parameters.items())

    assert params[0][0] == "required_param"
    assert params[1][0] == "optional_param"
    assert params[1][1].default == 10
    assert params[2][0] == "args"
    assert params[2][1].kind == inspect.Parameter.VAR_POSITIONAL
    assert params[3][0] == "kwonly"
    assert params[3][1].default == "default"
    assert params[4][0] == "kwargs"
    assert params[4][1].kind == inspect.Parameter.VAR_KEYWORD


def test_trace_preserves_other_context_parameters(exporter: Any) -> None:
    """Test that only the FIRST ctx param is removed, not all 'context' params.

    A function might have ctx as the injected param AND another param named
    'context' for a different purpose (e.g., a Request object). We should only
    remove the first one.
    """

    @trace
    def handler(ctx: Any, context: str, user_id: int) -> dict[str, Any]:
        ctx.set_attribute("user.id", user_id)
        return {"context": context, "user_id": user_id}

    sig = inspect.signature(handler)
    param_names = list(sig.parameters.keys())

    # ctx should be removed, but 'context' should be preserved
    assert "ctx" not in param_names
    assert "context" in param_names
    assert "user_id" in param_names
    assert len(param_names) == 2

    # Verify the function still works
    result = handler("some-context", 123)
    assert result == {"context": "some-context", "user_id": 123}
