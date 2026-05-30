"""GenAI span event helpers aligned with OpenTelemetry semantic conventions."""

from __future__ import annotations

from collections.abc import Sequence
from typing import Protocol

EventAttributes = dict[str, str | int | float | bool]


class EventRecorder(Protocol):
    """Small protocol for TraceContext-like objects."""

    def add_event(self, name: str, attributes: EventAttributes | None = None) -> None:
        """Add a span event."""


def record_prompt_sent(
    ctx: EventRecorder,
    *,
    model: str | None = None,
    prompt_tokens: int | None = None,
    message_count: int | None = None,
    operation: str | None = None,
) -> None:
    """Record that a prompt was dispatched to an LLM provider."""
    attrs: EventAttributes = {}
    if model:
        attrs["gen_ai.request.model"] = model
    if prompt_tokens is not None:
        attrs["gen_ai.usage.input_tokens"] = prompt_tokens
    if message_count is not None:
        attrs["gen_ai.request.message_count"] = message_count
    if operation:
        attrs["gen_ai.operation.name"] = operation
    ctx.add_event("gen_ai.prompt.sent", attrs)


def record_response_received(
    ctx: EventRecorder,
    *,
    model: str | None = None,
    prompt_tokens: int | None = None,
    completion_tokens: int | None = None,
    total_tokens: int | None = None,
    finish_reasons: Sequence[str] | None = None,
) -> None:
    """Record that an LLM provider response was received."""
    attrs: EventAttributes = {}
    if model:
        attrs["gen_ai.response.model"] = model
    if prompt_tokens is not None:
        attrs["gen_ai.usage.input_tokens"] = prompt_tokens
    if completion_tokens is not None:
        attrs["gen_ai.usage.output_tokens"] = completion_tokens
    if total_tokens is not None:
        attrs["gen_ai.usage.total_tokens"] = total_tokens
    if finish_reasons:
        attrs["gen_ai.response.finish_reasons"] = ",".join(finish_reasons)
    ctx.add_event("gen_ai.response.received", attrs)


def record_gen_ai_retry(
    ctx: EventRecorder,
    *,
    attempt: int,
    reason: str | None = None,
    delay_ms: int | float | None = None,
    status_code: int | None = None,
) -> None:
    """Record an LLM retry decision before the next attempt."""
    attrs: EventAttributes = {"retry.attempt": attempt}
    if reason:
        attrs["retry.reason"] = reason
    if delay_ms is not None:
        attrs["retry.delay_ms"] = delay_ms
    if status_code is not None:
        attrs["http.response.status_code"] = status_code
    ctx.add_event("gen_ai.retry", attrs)


def record_tool_call(
    ctx: EventRecorder,
    *,
    tool_name: str,
    tool_call_id: str | None = None,
    arguments: str | None = None,
) -> None:
    """Record a tool/function call made during an agent step."""
    attrs: EventAttributes = {"gen_ai.tool.name": tool_name}
    if tool_call_id:
        attrs["gen_ai.tool.call.id"] = tool_call_id
    if arguments:
        attrs["gen_ai.tool.arguments"] = arguments
    ctx.add_event("gen_ai.tool.call", attrs)


def record_stream_first_token(
    ctx: EventRecorder,
    *,
    tokens_so_far: int | None = None,
) -> None:
    """Record time-to-first-token for a streaming response."""
    attrs: EventAttributes = {}
    if tokens_so_far is not None:
        attrs["gen_ai.stream.tokens_so_far"] = tokens_so_far
    ctx.add_event("gen_ai.stream.first_token", attrs)
