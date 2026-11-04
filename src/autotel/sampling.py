"""Adaptive sampling for autotel.

Process-local tail sampling: every span of a trace is held until its local root
(no parent, or a remote one) and everything started under it have ended, then the
whole trace is kept or dropped, so a dropped trace leaves no orphan spans. Each
service decides alone; for one decision across services, run the Collector's
``tail_sampling`` processor.

Fail-open: a trace whose root has not ended within 30s, or the oldest trace once
20,000 spans are held, is exported as it stands.
"""

import hashlib
import threading
import time
from collections import OrderedDict
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any

from opentelemetry import baggage, trace
from opentelemetry.context import Context
from opentelemetry.sdk.trace import SpanProcessor
from opentelemetry.sdk.trace.export import ReadableSpan as ReadWriteSpan
from opentelemetry.sdk.trace.sampling import (
    ParentBased,
    TraceIdRatioBased,
)
from opentelemetry.trace import StatusCode

from .constants import (
    AUTOTEL_DEBUG_BAGGAGE_KEY,
    AUTOTEL_SAMPLING_TAIL_EVALUATED,
    AUTOTEL_SAMPLING_TAIL_KEEP,
)

_MAX_BUFFERED_SPANS = 20_000
_TRACE_TIMEOUT_SECONDS = 30.0
_MAX_DECIDED_TRACES = 10_000


def _hash_unit_interval(trace_id: int, salt: str = "") -> float:
    """Map a trace id to [0, 1), so every span and processor reaches the same verdict."""
    digest = hashlib.sha256(f"{salt}{trace_id:032x}".encode()).digest()
    return int.from_bytes(digest[:8], "big") / 2**64


def force_keep() -> None:
    """Keep the current trace whatever the sampler decides: a payment, an audit event."""
    span = trace.get_current_span()
    span.set_attribute(AUTOTEL_SAMPLING_TAIL_EVALUATED, True)
    span.set_attribute(AUTOTEL_SAMPLING_TAIL_KEEP, True)


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
        self._keep_rules: list[tuple[str, Callable[[Any], bool]]] = []

        # Use ParentBased sampler with high ratio to sample everything initially
        # We'll do tail sampling in the processor
        self._head_sampler = ParentBased(root=TraceIdRatioBased(1.0))

    @classmethod
    def production(cls) -> "AdaptiveSampler":
        """Errors, anything past one second, and one in ten of the rest."""
        return cls(baseline_rate=0.1, slow_threshold_ms=1000)

    @classmethod
    def errors_only(cls) -> "AdaptiveSampler":
        """Only failures and slow traces survive."""
        return cls(baseline_rate=0.0, slow_threshold_ms=1000)

    def keep_when(self, key: str, predicate: Callable[[Any], bool]) -> "AdaptiveSampler":
        """Keep every trace with a span whose ``key`` attribute matches ``predicate``."""
        self._keep_rules.append((key, predicate))
        return self

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

    def _matches_rule(self, span: ReadWriteSpan) -> bool:
        attributes = span.attributes or {}
        return any(key in attributes and rule(attributes[key]) for key, rule in self._keep_rules)

    def _is_slow(self, span: ReadWriteSpan) -> bool:
        if span.end_time is None or span.start_time is None:
            return False
        return (span.end_time - span.start_time) / 1_000_000 > self.slow_threshold_ms

    def should_keep_span(self, span: ReadWriteSpan) -> bool:
        """Decide one span as if it were a whole trace."""
        return self.should_keep_trace(span, [span])

    def should_keep_trace(self, root: ReadWriteSpan, spans: list[ReadWriteSpan]) -> bool:
        """
        Decide a trace from its local root and every span this process recorded for it.

        Args:
            root: The trace's local root span
            spans: All of the trace's spans, root included

        Returns:
            True if the trace should be kept, False if it should be dropped
        """
        attributes = [span.attributes or {} for span in spans]
        if any(item.get(AUTOTEL_SAMPLING_TAIL_KEEP) is True for item in attributes):
            return True
        if any(span.status.status_code == StatusCode.ERROR for span in spans):
            return True
        if any(self._matches_rule(span) for span in spans) or self._is_slow(root):
            return True

        trace_id = root.context.trace_id
        # Links-based sampling for event-driven systems
        if self.links_based and any(self._has_sampled_link(span) for span in spans):
            return _hash_unit_interval(trace_id, "links") < self.links_rate

        return _hash_unit_interval(trace_id) < self.baseline_rate


@dataclass
class _PendingTrace:
    deadline: float
    spans: list[ReadWriteSpan] = field(default_factory=list)
    open: int = 0
    root: ReadWriteSpan | None = None
    keep: bool = False


class AdaptiveSamplingProcessor(SpanProcessor):
    """
    Span processor that keeps or drops whole traces using AdaptiveSampler.

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
        self._lock = threading.Lock()
        # Insertion order is creation order, so deadlines ascend.
        self._pending: OrderedDict[int, _PendingTrace] = OrderedDict()
        self._decided: OrderedDict[int, bool] = OrderedDict()
        self._buffered = 0

    def _trace(self, trace_id: int) -> _PendingTrace:
        pending = self._pending.get(trace_id)
        if pending is None:
            pending = _PendingTrace(deadline=time.monotonic() + _TRACE_TIMEOUT_SECONDS)
            self._pending[trace_id] = pending
        return pending

    def _decide(self, trace_id: int, *, fail_open: bool = False) -> list[ReadWriteSpan]:
        """Remove a trace from the buffer and return its spans if kept."""
        pending = self._pending.pop(trace_id)
        self._buffered -= len(pending.spans)
        keep = fail_open or pending.keep
        if not keep and pending.root is not None:
            keep = self.sampler.should_keep_trace(pending.root, pending.spans)
        self._decided[trace_id] = keep
        if len(self._decided) > _MAX_DECIDED_TRACES:
            self._decided.popitem(last=False)
        return pending.spans if keep else []

    def _sweep(self) -> list[ReadWriteSpan]:
        """Decide traces past their deadline, or beyond the buffer limit, oldest first."""
        released: list[ReadWriteSpan] = []
        now = time.monotonic()
        while self._pending:
            trace_id, oldest = next(iter(self._pending.items()))
            if oldest.deadline > now and self._buffered <= _MAX_BUFFERED_SPANS:
                break
            released += self._decide(trace_id, fail_open=oldest.root is None)
        return released

    def on_start(self, span: ReadWriteSpan, parent_context: Context | None = None) -> None:
        """Count the span open, and honour an ``autotel.debug`` baggage request."""
        requested = baggage.get_baggage(AUTOTEL_DEBUG_BAGGAGE_KEY, parent_context)
        with self._lock:
            trace_id = span.context.trace_id
            if trace_id not in self._decided:
                pending = self._trace(trace_id)
                pending.open += 1
                pending.keep = pending.keep or requested not in (None, "", "0")
        self.next_processor.on_start(span, parent_context)

    def on_end(self, span: ReadWriteSpan) -> None:
        """Hold the span until its trace is decided."""
        trace_id = span.context.trace_id
        with self._lock:
            verdict = self._decided.get(trace_id)
            if verdict is not None:
                released = [span] if verdict else []
            else:
                pending = self._trace(trace_id)
                pending.spans.append(span)
                self._buffered += 1
                # A span started before this processor existed was never counted.
                pending.open = max(pending.open - 1, 0)
                if span.parent is None or span.parent.is_remote:
                    pending.root = span
                released = []
                if pending.root is not None and pending.open == 0:
                    released = self._decide(trace_id)
                released += self._sweep()
        for finished in released:
            self.next_processor.on_end(finished)

    def shutdown(self) -> None:
        """Decide everything: nothing more will end. Rootless traces export as they stand."""
        with self._lock:
            released: list[ReadWriteSpan] = []
            for trace_id, pending in list(self._pending.items()):
                released += self._decide(trace_id, fail_open=pending.root is None)
            self._decided.clear()
        for finished in released:
            self.next_processor.on_end(finished)
        self.next_processor.shutdown()

    def force_flush(self, timeout_millis: int = 30000) -> bool:
        """Flush decided spans. A trace waiting on running children stays pending,
        because deciding it now would miss a late error or ``force_keep()``."""
        return self.next_processor.force_flush(timeout_millis)  # type: ignore[no-any-return]


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
