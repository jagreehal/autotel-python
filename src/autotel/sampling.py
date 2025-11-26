"""Adaptive sampling for autotel."""

import random
from typing import Any

from opentelemetry import trace
from opentelemetry.sdk.trace.export import ReadableSpan as ReadWriteSpan
from opentelemetry.sdk.trace.sampling import (
    ParentBased,
    TraceIdRatioBased,
)
from opentelemetry.trace import StatusCode


class AdaptiveSampler:
    """
    Adaptive sampler that samples:
    - 10% baseline for successful operations
    - 100% for errors
    - 100% for slow operations (via tail sampling)

    This uses a hybrid approach:
    - Head-based sampling: Sample all spans initially (to catch errors)
    - Tail-based sampling: Drop successful/fast spans via SpanProcessor
    """

    def __init__(
        self,
        baseline_rate: float = 0.1,
        error_rate: float = 1.0,
        slow_threshold_ms: int = 1000,
        slow_rate: float = 1.0,
    ):
        """
        Initialize adaptive sampler.

        Args:
            baseline_rate: Sampling rate for successful operations (0.0-1.0)
            error_rate: Sampling rate for errors (typically 1.0)
            slow_threshold_ms: Duration threshold in milliseconds for "slow" operations
            slow_rate: Sampling rate for slow operations (typically 1.0)
        """
        if not 0.0 <= baseline_rate <= 1.0:
            raise ValueError("baseline_rate must be between 0.0 and 1.0")
        if not 0.0 <= error_rate <= 1.0:
            raise ValueError("error_rate must be between 0.0 and 1.0")
        if not 0.0 <= slow_rate <= 1.0:
            raise ValueError("slow_rate must be between 0.0 and 1.0")

        self.baseline_rate = baseline_rate
        self.error_rate = error_rate
        self.slow_threshold_ms = slow_threshold_ms
        self.slow_rate = slow_rate

        # Use ParentBased sampler with high ratio to sample everything initially
        # We'll do tail sampling in the processor
        self._head_sampler = ParentBased(root=TraceIdRatioBased(1.0))

    def get_sampler(self) -> Any:
        """Get the OpenTelemetry sampler instance."""
        return self._head_sampler

    def should_keep_span(self, span: ReadWriteSpan) -> bool:
        """
        Determine if a span should be kept after completion (tail sampling).

        This is called by AdaptiveSamplingProcessor after span ends.

        Args:
            span: The completed span

        Returns:
            True if span should be kept, False if it should be dropped
        """
        # Always keep errors
        if span.status.status_code == StatusCode.ERROR:
            return True

        # Check if span is slow
        if span.end_time is not None and span.start_time is not None:
            duration_ms = (
                span.end_time - span.start_time
            ) / 1_000_000  # nanoseconds to milliseconds
            if duration_ms > self.slow_threshold_ms:
                return True

        # For successful, fast operations: use baseline rate
        return bool(random.random() < self.baseline_rate)


class AdaptiveSamplingProcessor:
    """
    Span processor that implements tail sampling using AdaptiveSampler.

    This processor drops spans after they complete based on the adaptive sampling rules.
    """

    def __init__(self, sampler: AdaptiveSampler, next_processor: Any) -> None:
        """
        Initialize adaptive sampling processor.

        Args:
            sampler: The AdaptiveSampler instance
            next_processor: The next span processor in the chain
        """
        self.sampler = sampler
        self.next_processor = next_processor

    def on_start(
        self, span: ReadWriteSpan, parent_context: trace.SpanContext | None = None
    ) -> None:
        """Called when a span starts."""
        if hasattr(self.next_processor, "on_start"):
            self.next_processor.on_start(span, parent_context)

    def on_end(self, span: ReadWriteSpan) -> None:
        """Called when a span ends - implement tail sampling here."""
        # Check if we should keep this span
        if self.sampler.should_keep_span(span):
            # Keep the span - pass to next processor
            self.next_processor.on_end(span)
        # Otherwise, drop the span silently

    def shutdown(self) -> None:
        """Shutdown the processor."""
        if hasattr(self.next_processor, "shutdown"):
            self.next_processor.shutdown()

    def force_flush(self, timeout_millis: int = 30000) -> bool:
        """Force flush pending spans."""
        if hasattr(self.next_processor, "force_flush"):
            return self.next_processor.force_flush(timeout_millis)  # type: ignore[no-any-return]
        return True
