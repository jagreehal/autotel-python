"""Auto-instrumentation of third-party MCP servers (transport agnostic)."""

import sys
from collections.abc import Callable, Generator
from types import ModuleType
from typing import Any

import pytest
from opentelemetry import trace

from autotel import init
from autotel.exporters import InMemorySpanExporter
from autotel.mcp import inject_otel_context_to_meta
from autotel.processors import SimpleSpanProcessor


@pytest.fixture
def exporter() -> Any:
    exp = InMemorySpanExporter()
    init(service="mcp-auto", span_processor=SimpleSpanProcessor(exp), instrumentation=["mcp"])
    return exp


@pytest.fixture(autouse=True)
def fake_agents_module() -> Generator[None, None, None]:
    """Simulate presence of agents.mcp / fastmcp modules for auto-patching."""
    module = ModuleType("agents.mcp")

    class MCPServer:
        def __init__(self: Any) -> None:
            self.handlers: dict[str, Callable[..., object]] = {}

        def register_tool(self: Any, name: str, _config: dict[str, Any], handler: Any) -> None:
            self.handlers[name] = handler

    class MCPServerStdio(MCPServer):
        """Alias used by fastmcp examples."""

    module.MCPServer = MCPServer  # type: ignore[attr-defined]
    module.MCPServerStdio = MCPServerStdio  # type: ignore[attr-defined]

    # Install fake module under all names we patch
    sys.modules["agents.mcp"] = module
    sys.modules["mcp"] = module  # ensure baseline name exists
    yield
    for key in ("agents.mcp", "mcp"):
        sys.modules.pop(key, None)


@pytest.mark.asyncio
async def test_auto_patch_instruments_fake_fastmcp(exporter: InMemorySpanExporter) -> None:
    """Auto-instrumentation should patch MCPServer-like classes when present."""
    from agents.mcp import MCPServer  # type: ignore[import-not-found]

    server = MCPServer()
    assert getattr(server, "__autotel_mcp_server__", False) is True

    tracer = trace.get_tracer(__name__)

    async def handler(_args: Any, _meta: Any = None) -> None:
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
    assert server_span.attributes.get("mcp.tool.name") == "echo"
    assert server_span.context.trace_id == parent.get_span_context().trace_id
