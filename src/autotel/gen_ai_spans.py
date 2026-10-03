"""Context managers for GenAI agent / tool spans (OTel GenAI conventions)."""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from typing import Any

from .functional import span
from .gen_ai_events import record_tool_call


@contextmanager
def invoke_agent(
    name: str = "invoke_agent",
    *,
    agent_name: str | None = None,
    model: str | None = None,
    system: str | None = None,
) -> Iterator[Any]:
    """
    Root span for one agent run (``gen_ai.operation.name=invoke_agent``).

    Example:
        >>> from autotel import invoke_agent
        >>> with invoke_agent(agent_name="support", model="gpt-4o") as ctx:
        ...     ...
    """
    with span(name) as ctx:
        ctx.set_attribute("gen_ai.operation.name", "invoke_agent")
        if agent_name is not None:
            ctx.set_attribute("agent.name", agent_name)
            ctx.set_attribute("gen_ai.agent.name", agent_name)
        if model is not None:
            ctx.set_attribute("gen_ai.request.model", model)
        if system is not None:
            ctx.set_attribute("gen_ai.system", system)
        yield ctx


@contextmanager
def execute_tool(
    tool_name: str,
    *,
    tool_call_id: str | None = None,
    arguments: str | None = None,
    record_event: bool = True,
) -> Iterator[Any]:
    """
    Child span for one tool call (``gen_ai.operation.name=execute_tool``).

    Also records a ``gen_ai.tool.call`` event when ``record_event`` is True.
    ``arguments`` goes on that event verbatim, so leave it out for payloads
    that may contain personal data.

    Example:
        >>> from autotel import execute_tool
        >>> with execute_tool("refund_lookup", arguments='{"order_id":"1"}') as ctx:
        ...     result = lookup(...)
    """
    with span("execute_tool") as ctx:
        ctx.set_attribute("gen_ai.operation.name", "execute_tool")
        ctx.set_attribute("gen_ai.tool.name", tool_name)
        if tool_call_id is not None:
            ctx.set_attribute("gen_ai.tool.call.id", tool_call_id)
        if record_event:
            record_tool_call(
                ctx,
                tool_name=tool_name,
                tool_call_id=tool_call_id,
                arguments=arguments,
            )
        yield ctx
