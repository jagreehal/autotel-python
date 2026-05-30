"""Adaptive sampling for autotel."""

import random
from typing import Any

from opentelemetry import trace
from opentelemetry.context import Context
from opentelemetry.sdk.trace import SpanProcessor
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
    - Configurable rate for spans linked to sampled spans

    This uses a hybrid approach:
    - Head-based sampling: Sample all spans initially (to catch errors)
    - Tail-based sampling: Drop successful/fast spans via SpanProcessor
    - Links-based sampling: Keep spans linked to sampled spans (for event-driven systems)
    """

    def __init__(
        self,
        baseline_rate: float = 0.1,
        error_rate: float = 1.0,
        slow_threshold_ms: int = 1000,
        slow_rate: float = 1.0,
        links_based: bool = False,
        links_rate: float = 1.0,
    ):
        """
        Initialize adaptive sampler.

        Args:
            baseline_rate: Sampling rate for successful operations (0.0-1.0)
            error_rate: Sampling rate for errors (typically 1.0)
            slow_threshold_ms: Duration threshold in milliseconds for "slow" operations
            slow_rate: Sampling rate for slow operations (typically 1.0)
            links_based: Enable links-based sampling for event-driven architectures
            links_rate: Sampling rate for spans linked to sampled spans (0.0-1.0)
        """
        if not 0.0 <= baseline_rate <= 1.0:
            raise ValueError("baseline_rate must be between 0.0 and 1.0")
        if not 0.0 <= error_rate <= 1.0:
            raise ValueError("error_rate must be between 0.0 and 1.0")
        if not 0.0 <= slow_rate <= 1.0:
            raise ValueError("slow_rate must be between 0.0 and 1.0")
        if not 0.0 <= links_rate <= 1.0:
            raise ValueError("links_rate must be between 0.0 and 1.0")

        self.baseline_rate = baseline_rate
        self.error_rate = error_rate
        self.slow_threshold_ms = slow_threshold_ms
        self.slow_rate = slow_rate
        self.links_based = links_based
        self.links_rate = links_rate

        # Use ParentBased sampler with high ratio to sample everything initially
        # We'll do tail sampling in the processor
        self._head_sampler = ParentBased(root=TraceIdRatioBased(1.0))

    def get_sampler(self) -> Any:
        """Get the OpenTelemetry sampler instance."""
        return self._head_sampler

    def _has_sampled_link(self, span: ReadWriteSpan) -> bool:
        """
        Check if the span has any links to sampled spans.

        A span is considered linked to a sampled span if any of its links
        have trace_flags with the sampled bit set (0x01).

        Args:
            span: The completed span to check

        Returns:
            True if any linked span is sampled, False otherwise
        """
        links = span.links
        if not links:
            return False

        return any(link.context and link.context.trace_flags.sampled for link in links)

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

        # Check for sampled links (links-based sampling for event-driven systems)
        if self.links_based and self._has_sampled_link(span):
            return bool(random.random() < self.links_rate)

        # For successful, fast operations: use baseline rate
        return bool(random.random() < self.baseline_rate)


class AdaptiveSamplingProcessor(SpanProcessor):
    """
    Span processor that implements tail sampling using AdaptiveSampler.

    This processor drops spans after they complete based on the adaptive sampling rules.

    Subclasses ``SpanProcessor`` so it inherits the full processor protocol
    (including the internal ``_on_ending`` hook the SDK invokes on every
    registered processor); duck-typing alone breaks on newer opentelemetry-sdk.
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
        self, span: ReadWriteSpan, parent_context: Context | None = None
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


def create_link_from_headers(
    headers: dict[str, str],
    attributes: dict[str, Any] | None = None,
) -> trace.Link | None:
    """
    Create a Link from trace context headers (e.g., from a message queue).

    This is useful for message consumers that need to link to the producer span.

    Args:
        headers: Dictionary containing traceparent/tracestate headers
        attributes: Optional attributes for the link

    Returns:
        Link object if context could be extracted, None otherwise

    Example:
        >>> headers = {"traceparent": "00-abc123...-def456...-01"}
        >>> link = create_link_from_headers(headers)
        >>> if link:
        ...     ctx.add_link(link.context, link.attributes)
    """
    from opentelemetry.propagate import extract
    from opentelemetry.trace import get_current_span

    ctx = extract(headers)
    span_context = get_current_span(ctx).get_span_context()

    if span_context.is_valid:
        return trace.Link(span_context, attributes or {})
    return None


def extract_links_from_batch(
    messages: list[dict[str, Any]],
    headers_key: str = "headers",
) -> list[trace.Link]:
    """
    Extract Links from a batch of messages for fan-in scenarios.

    Useful for batch processing where multiple producer spans should be linked.

    Args:
        messages: List of message dictionaries
        headers_key: Key in each message containing trace headers

    Returns:
        List of Link objects for all valid trace contexts

    Example:
        >>> messages = [{"body": "...", "headers": {"traceparent": "..."}}]
        >>> links = extract_links_from_batch(messages)
        >>> with tracer.start_as_current_span("process_batch", links=links):
        ...     process_all(messages)
    """
    links: list[trace.Link] = []
    for msg in messages:
        msg_headers = msg.get(headers_key, {})
        if msg_headers:
            link = create_link_from_headers(msg_headers)
            if link:
                links.append(link)
    return links
