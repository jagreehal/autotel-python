"""Tests for GenAI helper functions ported from Node autotel."""

from typing import Any

import pytest

from autotel import (
    GEN_AI_COST_ATTRIBUTE,
    ModelPricing,
    TokenUsage,
    estimate_llm_cost,
    init,
    record_gen_ai_retry,
    record_llm_cost,
    record_prompt_sent,
    record_response_received,
    record_stream_first_token,
    record_tool_call,
    span,
)
from autotel.exporters import InMemorySpanExporter
from autotel.processors import SimpleSpanProcessor


@pytest.fixture
def exporter() -> Any:
    exp = InMemorySpanExporter()
    init(service="test-gen-ai", span_processor=SimpleSpanProcessor(exp))
    return exp


def test_estimate_llm_cost_known_model() -> None:
    cost = estimate_llm_cost(
        "gpt-4o-mini",
        TokenUsage(input_tokens=1_000_000, output_tokens=1_000_000),
    )

    assert cost == 0.75


def test_estimate_llm_cost_uses_longest_model_prefix() -> None:
    cost = estimate_llm_cost(
        "claude-sonnet-4-20260530",
        {"input_tokens": 1_000_000, "output_tokens": 1_000_000},
    )

    assert cost == 18


def test_estimate_llm_cost_can_use_custom_pricing() -> None:
    cost = estimate_llm_cost(
        "local-model-v1",
        {"inputTokens": 1000, "outputTokens": 1000},
        pricing={"local-model": ModelPricing(input_per_1m=1, output_per_1m=2)},
    )

    assert cost == 0.003


def test_record_llm_cost_sets_span_attribute(exporter: Any) -> None:
    with span("llm.call") as ctx:
        cost = record_llm_cost(
            ctx,
            "gpt-4o-mini",
            {"input_tokens": 1000, "output_tokens": 2000},
        )

    spans = exporter.get_finished_spans()
    assert cost == 0.00135
    assert spans[0].attributes is not None
    assert spans[0].attributes[GEN_AI_COST_ATTRIBUTE] == 0.00135


def test_gen_ai_event_helpers_record_semconv_events(exporter: Any) -> None:
    with span("agent.run") as ctx:
        record_prompt_sent(
            ctx,
            model="gpt-4o-mini",
            prompt_tokens=12,
            message_count=2,
            operation="chat",
        )
        record_response_received(
            ctx,
            model="gpt-4o-mini-2026-05-30",
            prompt_tokens=12,
            completion_tokens=8,
            total_tokens=20,
            finish_reasons=["stop"],
        )
        record_gen_ai_retry(ctx, attempt=2, reason="rate_limit", delay_ms=250, status_code=429)
        record_tool_call(ctx, tool_name="search", tool_call_id="call_123", arguments='{"q":"otel"}')
        record_stream_first_token(ctx, tokens_so_far=1)

    finished = exporter.get_finished_spans()
    events = finished[0].events
    names = [event.name for event in events]

    assert names == [
        "gen_ai.prompt.sent",
        "gen_ai.response.received",
        "gen_ai.retry",
        "gen_ai.tool.call",
        "gen_ai.stream.first_token",
    ]
    assert events[0].attributes["gen_ai.request.model"] == "gpt-4o-mini"
    assert events[1].attributes["gen_ai.response.finish_reasons"] == "stop"
    assert events[2].attributes["http.response.status_code"] == 429
    assert events[3].attributes["gen_ai.tool.name"] == "search"
    assert events[4].attributes["gen_ai.stream.tokens_so_far"] == 1
