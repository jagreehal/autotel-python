"""Tests for MCP instrumentation and context propagation."""

from collections.abc import Callable
from typing import Any

import pytest
from opentelemetry import context, trace

from autotel import init
from autotel.exporters import InMemorySpanExporter
from autotel.mcp import (
    activate_trace_context,
    extract_otel_context_from_meta,
    inject_otel_context_to_meta,
    instrument_mcp_client,
    instrument_mcp_server,
)
from autotel.processors import SimpleSpanProcessor


@pytest.fixture
def exporter() -> Any:
    """Set up an in-memory exporter for each test."""
    exp = InMemorySpanExporter()
    init(service="mcp-test", span_processor=SimpleSpanProcessor(exp))
    return exp


def test_inject_and_extract_trace_context(exporter: InMemorySpanExporter) -> None:
    """Trace context should round-trip through the _meta structure."""
    _ = exporter
    tracer = trace.get_tracer(__name__)
    parent_trace_id = None
    child_trace_id = None

    with tracer.start_as_current_span("parent") as parent:
        parent_trace_id = parent.get_span_context().trace_id
        meta = inject_otel_context_to_meta()
        assert meta.get("traceparent")

        extracted = extract_otel_context_from_meta(meta)
        token = context.attach(extracted)
        try:
            with tracer.start_as_current_span("child") as child:
                child_trace_id = child.get_span_context().trace_id
        finally:
            context.detach(token)

    assert parent_trace_id == child_trace_id


def test_activate_trace_context_no_meta_returns_active(exporter: InMemorySpanExporter) -> None:
    """activate_trace_context should fall back to the active context when no _meta is provided."""
    _ = exporter
    active = context.get_current()
    assert activate_trace_context() == active
    assert activate_trace_context({}) == active


@pytest.mark.asyncio
async def test_instrument_mcp_client_injects_meta_and_traces(
    exporter: InMemorySpanExporter,
) -> None:
    """Client instrumentation should inject _meta and create traced spans."""

    class DummyClient:
        """Minimal MCP client surface for testing."""

        def __init__(self: Any) -> None:
            self.last_params: dict[str, Any] | None = None

        async def call_tool(self: Any, params: dict[str, Any]) -> dict[str, Any]:
            self.last_params = params
            return {"ok": True}

    client = instrument_mcp_client(DummyClient())
    tracer = trace.get_tracer(__name__)

    with tracer.start_as_current_span("parent") as parent:
        parent_trace_id = parent.get_span_context().trace_id
        result = await client.call_tool({"name": "echo", "arguments": {"message": "hi"}})

    assert result == {"ok": True}
    assert client.last_params is not None
    injected_meta = client.last_params.get("_meta")
    assert injected_meta and injected_meta.get("traceparent")

    spans = exporter.get_finished_spans()
    client_span = next(span for span in spans if span.name.startswith("mcp.client.call_tool"))

    assert client_span.context.trace_id == parent_trace_id
    assert client_span.attributes is not None
    assert client_span.attributes.get("mcp.client.name") == "echo"
    assert client_span.attributes.get("mcp.client.operation") == "call_tool"


@pytest.mark.asyncio
async def test_instrument_mcp_client_uses_meta_kwarg_for_mcp_v2(
    exporter: InMemorySpanExporter,
) -> None:
    """mcp>=2 renamed ``_meta`` to ``meta``; inject there and leave tool arguments alone."""
    _ = exporter

    class V2Client:
        """Mirrors ``mcp.client.Client.call_tool`` from mcp 2.x."""

        def __init__(self: Any) -> None:
            self.calls: list[tuple[dict[str, Any] | None, dict[str, Any] | None]] = []

        async def call_tool(
            self: Any,
            name: str,
            arguments: dict[str, Any] | None = None,
            *,
            meta: dict[str, Any] | None = None,
        ) -> dict[str, Any]:
            self.calls.append((arguments, meta))
            return {"ok": True}

    client = instrument_mcp_client(V2Client())

    with trace.get_tracer(__name__).start_as_current_span("parent"):
        await client.call_tool("echo", arguments={"message": "hi"})
        await client.call_tool("echo", {"message": "hi"})

    for arguments, meta in client.calls:
        assert arguments == {"message": "hi"}
        assert meta and meta.get("traceparent")


@pytest.mark.asyncio
async def test_instrument_mcp_client_skips_meta_when_unsupported(
    exporter: InMemorySpanExporter,
) -> None:
    """A client that accepts no meta kwarg must not be passed one."""
    _ = exporter

    class StrictClient:
        async def call_tool(self: Any, name: str, arguments: dict[str, Any] | None = None) -> str:
            return name

    client = instrument_mcp_client(StrictClient())
    assert await client.call_tool("echo", arguments={"message": "hi"}) == "echo"


@pytest.mark.asyncio
async def test_instrument_mcp_server_extracts_meta_and_traces(
    exporter: InMemorySpanExporter,
) -> None:
    """Server instrumentation should extract parent context and trace handlers."""

    class DummyServer:
        """Minimal MCP server surface for testing."""

        def __init__(self: Any) -> None:
            self.handlers: dict[str, Callable[..., object]] = {}

        def register_tool(self: Any, name: str, _config: dict[str, Any], handler: Any) -> None:
            self.handlers[name] = handler

    server = instrument_mcp_server(DummyServer())
    tracer = trace.get_tracer(__name__)

    async def handler(_args: Any, _meta: Any = None) -> None:
        # Return the active trace ID to assert context propagation
        span = trace.get_current_span()
        return {  # type: ignore[return-value]
            "handled": True,
            "trace_id": format(span.get_span_context().trace_id, "032x"),
        }

    server.register_tool("echo", {}, handler)
    wrapped = server.handlers["echo"]

    with tracer.start_as_current_span("parent") as parent:
        parent_trace_id = format(parent.get_span_context().trace_id, "032x")
        meta = inject_otel_context_to_meta()
        result = await wrapped({"message": "hi"}, _meta=meta)

    assert result["handled"] is True
    assert result["trace_id"] == parent_trace_id

    spans = exporter.get_finished_spans()
    server_span = next(span for span in spans if span.name.startswith("mcp.server.tool"))

    assert server_span.attributes is not None
    assert server_span.attributes.get("mcp.type") == "tool"
    assert server_span.attributes.get("mcp.tool.name") == "echo"
    assert server_span.context.trace_id == parent.get_span_context().trace_id


@pytest.mark.asyncio
async def test_instrument_real_mcp_client_propagates_over_stateless_protocol(
    exporter: InMemorySpanExporter,
) -> None:
    """Against the real mcp>=2 SDK, both call styles keep one trace across client and server."""
    _ = exporter
    mcp = pytest.importorskip("mcp")
    from mcp.server.mcpserver import MCPServer

    server = MCPServer("autotel-test")
    seen: list[int] = []

    @server.tool()
    def whoami() -> str:
        seen.append(trace.get_current_span().get_span_context().trace_id)
        return "ok"

    async with mcp.Client(server) as client:
        instrument_mcp_client(client)
        with trace.get_tracer(__name__).start_as_current_span("parent") as parent:
            await client.call_tool("whoami", arguments={})
            await client.call_tool("whoami", {})

    trace_id = parent.get_span_context().trace_id
    assert seen == [trace_id, trace_id]
