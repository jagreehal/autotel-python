"""Cohort analysis helpers for finding which field separates slow from normal."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

AnalysisEvent = Mapping[str, Any]

DEFAULT_MAX_VALUES_PER_FIELD = 50
DEFAULT_MAX_UNIQUE_RATIO = 0.5
DEFAULT_MIN_DIFFERENCE = 0.1
DEFAULT_LIMIT = 20


@dataclass(frozen=True)
class CohortDifference:
    """How strongly one field/value pair separates outliers from the baseline."""

    field: str
    value: str
    outlier_fraction: float
    baseline_fraction: float
    difference: float
    outlier_count: int
    baseline_count: int


def _value_key(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, bool | int | float | str):
        return str(value)
    return None


def compare_cohorts(
    *,
    outlier: list[AnalysisEvent] | tuple[AnalysisEvent, ...],
    baseline: list[AnalysisEvent] | tuple[AnalysisEvent, ...],
    fields: list[str] | None = None,
    ignore_fields: list[str] | None = None,
    max_values_per_field: int = DEFAULT_MAX_VALUES_PER_FIELD,
    max_unique_ratio: float = DEFAULT_MAX_UNIQUE_RATIO,
    min_difference: float = DEFAULT_MIN_DIFFERENCE,
    limit: int = DEFAULT_LIMIT,
) -> list[CohortDifference]:
    """
    Rank field/value pairs that separate an outlier group from a baseline.

    Returns an empty list when either group is empty.
    """
    if not outlier or not baseline:
        return []

    ignored = set(ignore_fields or ())
    selected = set(fields) if fields is not None else None

    def includes(field: str) -> bool:
        if field in ignored:
            return False
        return selected is None or field in selected

    # field -> value -> {outlier, baseline}
    tally: dict[str, dict[str, dict[str, int]]] = {}

    def accumulate(events: list[AnalysisEvent] | tuple[AnalysisEvent, ...], group: str) -> None:
        for event in events:
            for field, raw in event.items():
                if not includes(field):
                    continue
                value = _value_key(raw)
                if value is None:
                    continue
                values = tally.setdefault(field, {})
                counts = values.setdefault(value, {"outlier": 0, "baseline": 0})
                counts[group] += 1

    accumulate(outlier, "outlier")
    accumulate(baseline, "baseline")

    results: list[CohortDifference] = []
    total = len(outlier) + len(baseline)

    for field, values in tally.items():
        if len(values) > max_values_per_field or len(values) > total * max_unique_ratio:
            continue
        for value, counts in values.items():
            outlier_fraction = counts["outlier"] / len(outlier)
            baseline_fraction = counts["baseline"] / len(baseline)
            difference = outlier_fraction - baseline_fraction
            if abs(difference) < min_difference:
                continue
            results.append(
                CohortDifference(
                    field=field,
                    value=value,
                    outlier_fraction=outlier_fraction,
                    baseline_fraction=baseline_fraction,
                    difference=difference,
                    outlier_count=counts["outlier"],
                    baseline_count=counts["baseline"],
                )
            )

    results.sort(
        key=lambda item: (
            -abs(item.difference),
            -item.difference,
            item.field,
            item.value,
        )
    )
    return results[:limit]


def bucket(value: float, boundaries: list[float] | tuple[float, ...]) -> str:
    """Label a numeric value with the range it falls in."""
    if not isinstance(value, (int, float)) or isinstance(value, bool):
        return "unknown"
    if value != value or value in (float("inf"), float("-inf")):  # NaN / inf
        return "unknown"
    if not boundaries:
        return "unknown"
    ordered = sorted(boundaries)
    for index, boundary in enumerate(ordered):
        if value < boundary:
            if index == 0:
                return f"<{boundary}"
            return f"{ordered[index - 1]}-{boundary}"
    return f">={ordered[-1]}"
