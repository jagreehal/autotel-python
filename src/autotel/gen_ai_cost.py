"""GenAI token cost estimation helpers."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol

GEN_AI_COST_ATTRIBUTE = "gen_ai.usage.cost.usd"


class AttributeSetter(Protocol):
    """Small protocol for TraceContext-like objects."""

    def set_attribute(self, key: str, value: str | int | float | bool) -> None:
        """Set one span attribute."""


@dataclass(frozen=True)
class ModelPricing:
    """Pricing for a model in USD per one million tokens."""

    input_per_1m: float
    output_per_1m: float
    cached_input_per_1m: float | None = None


@dataclass(frozen=True)
class TokenUsage:
    """Token counts for a single LLM call."""

    input_tokens: int = 0
    output_tokens: int = 0
    cached_input_tokens: int = 0


MODEL_PRICING: dict[str, ModelPricing] = {
    # OpenAI
    "gpt-4o": ModelPricing(input_per_1m=2.5, output_per_1m=10),
    "gpt-4o-mini": ModelPricing(input_per_1m=0.15, output_per_1m=0.6),
    "gpt-4.1": ModelPricing(input_per_1m=2, output_per_1m=8),
    "gpt-4.1-mini": ModelPricing(input_per_1m=0.4, output_per_1m=1.6),
    "gpt-4.1-nano": ModelPricing(input_per_1m=0.1, output_per_1m=0.4),
    "o3-mini": ModelPricing(input_per_1m=1.1, output_per_1m=4.4),
    # Anthropic Claude
    "claude-opus-4": ModelPricing(input_per_1m=15, output_per_1m=75),
    "claude-sonnet-4": ModelPricing(input_per_1m=3, output_per_1m=15),
    "claude-3-5-sonnet": ModelPricing(input_per_1m=3, output_per_1m=15),
    "claude-3-5-haiku": ModelPricing(input_per_1m=0.8, output_per_1m=4),
    "claude-3-opus": ModelPricing(input_per_1m=15, output_per_1m=75),
    "claude-3-haiku": ModelPricing(input_per_1m=0.25, output_per_1m=1.25),
    # Google Gemini
    "gemini-1.5-pro": ModelPricing(input_per_1m=1.25, output_per_1m=5),
    "gemini-1.5-flash": ModelPricing(input_per_1m=0.075, output_per_1m=0.3),
    "gemini-2.0-flash": ModelPricing(input_per_1m=0.1, output_per_1m=0.4),
}


def estimate_llm_cost(
    model: str,
    usage: TokenUsage | dict[str, int],
    *,
    pricing: dict[str, ModelPricing] | None = None,
) -> float | None:
    """
    Estimate USD cost for a model call from token usage.

    Matching is exact first, then by longest pricing-key prefix so versioned
    model ids can resolve to a base model entry.
    """
    price = _resolve_pricing({**MODEL_PRICING, **(pricing or {})}, model)
    if price is None:
        return None

    token_usage = _coerce_usage(usage)
    cached_input = max(0, token_usage.cached_input_tokens)
    billed_input = max(0, token_usage.input_tokens - cached_input)
    cached_rate = price.cached_input_per_1m or price.input_per_1m

    cost = (
        (billed_input / 1_000_000) * price.input_per_1m
        + (cached_input / 1_000_000) * cached_rate
        + (token_usage.output_tokens / 1_000_000) * price.output_per_1m
    )
    return round(cost, 6)


def record_llm_cost(
    ctx: AttributeSetter,
    model: str,
    usage: TokenUsage | dict[str, int],
    *,
    pricing: dict[str, ModelPricing] | None = None,
) -> float | None:
    """Estimate LLM call cost and record it on a TraceContext-like object."""
    cost = estimate_llm_cost(model, usage, pricing=pricing)
    if cost is not None:
        ctx.set_attribute(GEN_AI_COST_ATTRIBUTE, cost)
    return cost


def _resolve_pricing(table: dict[str, ModelPricing], model: str) -> ModelPricing | None:
    exact = table.get(model)
    if exact:
        return exact

    best: ModelPricing | None = None
    best_length = 0
    for key, value in table.items():
        if model.startswith(key) and len(key) > best_length:
            best = value
            best_length = len(key)
    return best


def _coerce_usage(usage: TokenUsage | dict[str, int]) -> TokenUsage:
    if isinstance(usage, TokenUsage):
        return usage

    return TokenUsage(
        input_tokens=usage.get("input_tokens", usage.get("inputTokens", 0)),
        output_tokens=usage.get("output_tokens", usage.get("outputTokens", 0)),
        cached_input_tokens=usage.get(
            "cached_input_tokens",
            usage.get("cachedInputTokens", 0),
        ),
    )
