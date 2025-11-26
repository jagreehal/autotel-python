"""
BEFORE autotel: Manual Context Propagation for MCP Servers
Based on: langfuse-examples/applications/mcp-tracing/src/utils/otel_utils.py

This requires 135 lines of manual code to handle context propagation!
"""

import asyncio
import functools
from collections.abc import Callable
from typing import Any, TypeVar

from opentelemetry import context, trace
from opentelemetry.context import Context
from opentelemetry.propagate import get_global_textmap

F = TypeVar("F", bound=Callable[..., Any])


# ==================== BOILERPLATE START (135 lines!) ====================

def extract_otel_context_from_meta(meta: dict | None) -> Context:
    """
    Extract OpenTelemetry context from MCP _meta field.

    Manual extraction of traceparent, tracestate, and baggage.
    """
    if not meta:
        return context.get_current()

    # Manually create carrier dict
    carrier = {}
    if "traceparent" in meta:
        carrier["traceparent"] = meta["traceparent"]
    if "tracestate" in meta:
        carrier["tracestate"] = meta["tracestate"]
    if "baggage" in meta:
        carrier["baggage"] = meta["baggage"]

    # Manually extract context using OpenTelemetry's propagator
    if carrier:
        propagator = get_global_textmap()
        return propagator.extract(carrier)
    return context.get_current()


def inject_otel_context_to_meta() -> dict:
    """
    Inject current OpenTelemetry context into _meta field format.

    Manually creates dictionary with trace context fields.
    """
    carrier = {}
    propagator = get_global_textmap()
    propagator.inject(carrier, context=context.get_current())
    return carrier


def with_otel_context_from_meta(func: F) -> F:
    """
    Decorator that manually extracts OpenTelemetry context from MCP request.

    This is complex because it needs to:
    - Handle both sync and async functions
    - Extract _meta from kwargs
    - Attach/detach context properly
    - Clean up in finally blocks
    """

    @functools.wraps(func)
    def sync_wrapper(*args, **kwargs):
        # Extract _meta from kwargs
        meta = kwargs.get("_meta")

        # Extract and activate the context
        ctx = extract_otel_context_from_meta(meta)
        token = context.attach(ctx)

        try:
            return func(*args, **kwargs)
        finally:
            context.detach(token)

    @functools.wraps(func)
    async def async_wrapper(*args, **kwargs):
        # Extract _meta from kwargs
        meta = kwargs.get("_meta")

        # Extract and activate the context
        ctx = extract_otel_context_from_meta(meta)
        token = context.attach(ctx)

        try:
            return await func(*args, **kwargs)
        finally:
            context.detach(token)

    # Return appropriate wrapper based on function type
    if asyncio.iscoroutinefunction(func):
        return async_wrapper
    else:
        return sync_wrapper


class TracedMCPServer:
    """
    Wrapper that manually adds OpenTelemetry context propagation to MCP servers.

    This is a LOT of code just to inject context into _meta fields!
    Requires implementing:
    - __init__ to store wrapped server
    - call_tool to inject context
    - __getattr__ to delegate other methods
    """

    def __init__(self, server):
        self._server = server

    async def call_tool(self, tool_name: str, arguments: dict[str, Any] | None = None) -> Any:
        if arguments is None:
            arguments = {}

        # Manually inject current OTEL context into _meta field
        arguments["_meta"] = inject_otel_context_to_meta()

        return await self._server.call_tool(tool_name, arguments)

    def __getattr__(self, name: str) -> Any:
        return getattr(self._server, name)

# ==================== BOILERPLATE END ====================


# Usage example - still requires manual decorator application!


@with_otel_context_from_meta  # Manual decorator
async def my_mcp_tool(query: str, _meta: dict[str, Any] = None) -> str:
    """
    MCP tool that needs context propagation.

    Developer must:
    1. Add @with_otel_context_from_meta decorator
    2. Add _meta parameter to function signature
    3. Wrap server with TracedMCPServer
    """
    # Now context is propagated, but look at all the boilerplate above!
    with trace.get_tracer(__name__).start_as_current_span("process_query"):
        result = f"Processed: {query}"
        return result


# And to use the wrapper:
# server = MCPServer(...)
# traced_server = TracedMCPServer(server)  # Manual wrapping
# agent = Agent(mcp_servers=[traced_server], ...)
