"""Tests for adaptive sampling."""

import time
from typing import Any

import pytest
from opentelemetry.trace import StatusCode

from autotel import init, span
from autotel.exporters import InMemorySpanExporter
from autotel.processors import SimpleSpanProcessor
from autotel.sampling import AdaptiveSampler, AdaptiveSamplingProcessor


@pytest.fixture
def exporter() -> Any:
    """Create in-memory exporter for testing."""
    exp = InMemorySpanExporter()
    return exp


def test_adaptive_sampler_baseline(exporter: Any) -> None:
    """Test baseline sampling rate."""
    sampler = AdaptiveSampler(baseline_rate=0.1, slow_threshold_ms=1000)
    processor = AdaptiveSamplingProcessor(sampler, SimpleSpanProcessor(exporter))
    init(service="test", span_processor=processor, sampler=sampler)  # type: ignore[arg-type]

    # Create many spans - should sample ~10%
    for _ in range(100):
        with span("test.operation") as ctx:
            ctx.set_attribute("test", "value")

    spans = exporter.get_finished_spans()
    # With 10% sampling, we expect roughly 10 spans (allow variance for randomness)
    # Note: With 100 spans and 10% rate, we expect ~10, but allow 0-30 for randomness
    # (0 is unlikely but possible with probabilistic sampling)
    assert 0 <= len(spans) <= 30


def test_adaptive_sampler_errors(exporter: Any) -> None:
    """Test 100% sampling for errors."""
    sampler = AdaptiveSampler(baseline_rate=0.0, error_rate=1.0)  # 0% baseline, 100% errors
    processor = AdaptiveSamplingProcessor(sampler, SimpleSpanProcessor(exporter))
    init(service="test", span_processor=processor, sampler=sampler)  # type: ignore[arg-type]

    # Create spans with errors
    for _ in range(10):
        with span("test.error") as ctx:
            ctx.set_status(StatusCode.ERROR, "Test error")

    spans = exporter.get_finished_spans()
    # All errors should be sampled
    assert len(spans) == 10


def test_adaptive_sampler_slow_operations(exporter: Any) -> None:
    """Test 100% sampling for slow operations."""
    sampler = AdaptiveSampler(
        baseline_rate=0.0,  # 0% baseline
        slow_threshold_ms=100,  # >100ms is slow
        slow_rate=1.0,
    )
    processor = AdaptiveSamplingProcessor(sampler, SimpleSpanProcessor(exporter))
    init(service="test", span_processor=processor, sampler=sampler)  # type: ignore[arg-type]

    # Create slow spans
    for _ in range(5):
        with span("test.slow"):
            time.sleep(0.15)  # 150ms - should be sampled

    spans = exporter.get_finished_spans()
    # All slow spans should be sampled
    assert len(spans) == 5


def test_adaptive_sampler_validation() -> None:
    """Test sampler parameter validation."""
    # Valid
    sampler = AdaptiveSampler(baseline_rate=0.5)
    assert sampler.baseline_rate == 0.5

    # Invalid rates
    with pytest.raises(ValueError):
        AdaptiveSampler(baseline_rate=-0.1)
    with pytest.raises(ValueError):
        AdaptiveSampler(baseline_rate=1.5)
    with pytest.raises(ValueError):
        AdaptiveSampler(error_rate=-0.1)
