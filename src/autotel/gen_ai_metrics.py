"""GenAI client metrics and evaluation attributes (OTel GenAI conventions)."""

from __future__ import annotations

from typing import Any

from opentelemetry import metrics

_DURATION_BUCKETS = [0.01 * 2**power for power in range(14)]
_TOKEN_BUCKETS = [float(4**power) for power in range(14)]


class GenAiMetrics:
    """Build after ``init``.

    Example:
        >>> ai = GenAiMetrics()
        >>> ai.record_success("chat", "openai", "gpt-4o", 120, 40, duration=1.2)
    """

    def __init__(self) -> None:
        meter = metrics.get_meter("autotel")
        self.operation_duration = meter.create_histogram(
            "gen_ai.client.operation.duration",
            unit="s",
            description="GenAI operation duration",
            explicit_bucket_boundaries_advisory=_DURATION_BUCKETS,
        )
        self.token_usage = meter.create_histogram(
            "gen_ai.client.token.usage",
            unit="{token}",
            description="Number of tokens processed",
            explicit_bucket_boundaries_advisory=_TOKEN_BUCKETS,
        )
        self.operation_count = meter.create_counter(
            "gen_ai.client.operation.count", description="Number of GenAI operations"
        )

    def record_success(
        self,
        operation: str,
        provider: str,
        model: str,
        input_tokens: int,
        output_tokens: int,
        *,
        duration: float,
    ) -> None:
        base = _base(operation, provider, model)
        self.operation_duration.record(duration, base)
        self.operation_count.add(1, base)
        self.token_usage.record(input_tokens, {**base, "gen_ai.token.type": "input"})
        self.token_usage.record(output_tokens, {**base, "gen_ai.token.type": "output"})

    def record_failure(
        self, operation: str, provider: str, model: str, *, duration: float, error_type: str
    ) -> None:
        attributes = {**_base(operation, provider, model), "error.type": error_type}
        self.operation_duration.record(duration, attributes)
        self.operation_count.add(1, attributes)


def _base(operation: str, provider: str, model: str) -> dict[str, str]:
    return {
        "gen_ai.operation.name": operation,
        "gen_ai.provider.name": provider,
        "gen_ai.request.model": model,
    }


def record_evaluation(ctx: Any, name: str, score: float, label: str) -> None:
    """Attach an evaluation score to the GenAI span behind ``ctx``."""
    ctx.set_attributes(
        {
            "gen_ai.evaluation.name": name,
            "gen_ai.evaluation.score.value": score,
            "gen_ai.evaluation.score.label": label,
        }
    )
