"""Messaging decorators for event-driven architectures.

Provides ergonomic decorators for tracing message consumers and producers
with automatic context propagation and semantic conventions.

Example:
    >>> from autotel.messaging import trace_producer, trace_consumer
    >>>
    >>> @trace_producer(system="kafka", destination="user-events")
    ... async def publish_event(ctx, event: dict):
    ...     headers = ctx.get_trace_headers()
    ...     await producer.send(topic="user-events", value=event, headers=headers)
    >>>
    >>> @trace_consumer(
    ...     system="kafka",
    ...     destination="user-events",
    ...     consumer_group="event-processor",
    ... )
    ... async def process_event(ctx, message):
    ...     # Link to producer span is automatically created
    ...     await handle_event(message)
"""

from __future__ import annotations

import functools
import inspect
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from typing import Any, Literal, ParamSpec, TypeVar

from opentelemetry import context as otel_context
from opentelemetry import trace as otel_trace
from opentelemetry.propagate import extract, inject
from opentelemetry.trace import Link, Span, SpanContext, SpanKind, StatusCode

from .context import TraceContext
from .decorators import CTX_PARAM_NAMES, _rewrite_signature_without_ctx
from .operation_context import run_in_operation_context
from .sampling import create_link_from_headers

P = ParamSpec("P")
R = TypeVar("R")


def _set_messaging_conventions(
    span: Span, system: str, operation: str, destination: str | None
) -> None:
    """Set the standard messaging semantic-convention attributes on a span."""
    span.set_attribute("messaging.system", system)
    span.set_attribute("messaging.operation", operation)
    if destination:
        span.set_attribute("messaging.destination.name", destination)


def _apply_message_attributes(span: Span, attributes: dict[str, Any] | None) -> None:
    """Set user-provided attributes on a span, stringifying non-primitive values."""
    if not attributes:
        return
    for key, value in attributes.items():
        if isinstance(value, str | bool | int | float):
            span.set_attribute(key, value)
        else:
            span.set_attribute(key, str(value))


# ============================================================================
# Types and Configuration
# ============================================================================

DLQReasonCategory = Literal["validation", "processing", "timeout", "poison", "unknown"]
RebalanceType = Literal["assigned", "revoked", "lost"]


@dataclass
class DLQOptions:
    """Options for enhanced DLQ recording."""

    link_to_producer: bool = True
    """Automatically link to the producer span context."""

    reason_category: DLQReasonCategory | None = None
    """Category of the failure that caused DLQ routing."""

    attempt_count: int | None = None
    """Number of processing attempts before DLQ routing."""

    original_error: Exception | None = None
    """The original error that caused DLQ routing."""

    metadata: dict[str, str | int | bool] | None = None
    """Additional metadata to record with the DLQ event."""


@dataclass
class DLQReplayOptions:
    """Options for recording DLQ replay."""

    original_dlq_span_context: SpanContext | None = None
    """Original span context from DLQ message."""

    dlq_dwell_time_ms: float | None = None
    """Time spent in DLQ before replay (milliseconds)."""

    replay_attempt: int | None = None
    """Retry attempt number for this replay."""


@dataclass
class OutOfOrderInfo:
    """Information about out-of-order message."""

    current_sequence: int
    """Current sequence number."""

    expected_sequence: int
    """Expected sequence number."""

    partition_key: str | None = None
    """Partition key (if available)."""

    gap: int = 0
    """Gap size (positive = gap, negative = out of order)."""


@dataclass
class PartitionAssignment:
    """Partition assignment information."""

    topic: str
    """Topic name."""

    partition: int
    """Partition number."""

    offset: int | None = None
    """Initial offset (if available)."""


@dataclass
class RebalanceEvent:
    """Rebalance event information."""

    type: RebalanceType
    """Type of rebalance event."""

    partitions: list[PartitionAssignment]
    """Partitions affected by the rebalance."""

    timestamp: float
    """Timestamp of the rebalance event."""

    generation: int | None = None
    """Generation ID (increments on each rebalance)."""

    member_id: str | None = None
    """Consumer member ID."""

    reason: str | None = None
    """Reason for the rebalance (if available)."""


@dataclass
class PartitionLag:
    """Partition lag information."""

    topic: str
    """Topic name."""

    partition: int
    """Partition number."""

    current_offset: int
    """Current consumer offset."""

    end_offset: int
    """End offset (high watermark)."""

    lag: int
    """Calculated lag."""

    timestamp: float
    """Timestamp of measurement."""


@dataclass
class ConsumerGroupState:
    """Consumer group state snapshot."""

    group_id: str
    """Consumer group name."""

    member_id: str | None = None
    """Consumer member ID."""

    group_instance_id: str | None = None
    """Static instance ID (if using static membership)."""

    assigned_partitions: list[PartitionAssignment] = field(default_factory=list)
    """Currently assigned partitions."""

    generation: int | None = None
    """Group generation ID."""

    is_active: bool = True
    """Whether the consumer is currently active."""

    last_heartbeat: float | None = None
    """Last heartbeat timestamp."""

    state: str | None = None
    """Consumer state (stable, preparing_rebalance, etc.)."""


@dataclass
class OrderingConfig:
    """Configuration for message ordering tracking."""

    sequence_from: Callable[[Any], int | None] | None = None
    """Extract sequence number from message."""

    partition_key_from: Callable[[Any], str | None] | None = None
    """Extract partition key from message."""

    message_id_from: Callable[[Any], str | None] | None = None
    """Extract message ID for deduplication."""

    detect_out_of_order: bool = False
    """Enable out-of-order detection."""

    detect_duplicates: bool = False
    """Enable deduplication detection."""

    deduplication_window_size: int = 1000
    """Deduplication window size (number of message IDs to track)."""

    on_out_of_order: Callable[[Any, OutOfOrderInfo], None] | None = None
    """Callback when out-of-order message detected."""

    on_duplicate: Callable[[Any, str], None] | None = None
    """Callback when duplicate message detected."""


@dataclass
class ConsumerGroupTrackingConfig:
    """Configuration for consumer group tracking."""

    member_id: str | Callable[[], str | None] | None = None
    """Consumer member ID."""

    group_instance_id: str | Callable[[], str | None] | None = None
    """Static group instance ID."""

    on_rebalance: Callable[[Any, RebalanceEvent], None] | None = None
    """Callback when rebalance occurs."""

    on_partitions_assigned: Callable[[Any, list[PartitionAssignment]], None] | None = None
    """Callback when partitions are assigned."""

    on_partitions_revoked: Callable[[Any, list[PartitionAssignment]], None] | None = None
    """Callback when partitions are revoked."""

    track_partition_lag: bool = True
    """Track consumer lag per partition."""


# ============================================================================
# Extended Context Classes
# ============================================================================


class ProducerContext(TraceContext):
    """Extended trace context for producers with header injection.

    Provides methods for injecting trace context into message headers.
    """

    def __init__(
        self,
        span: Span,
        *,
        propagate_baggage: bool = False,
        custom_headers_fn: Callable[[ProducerContext], dict[str, str]] | None = None,
    ) -> None:
        """Initialize the producer context.

        Args:
            span: The span for this producer.
            propagate_baggage: Whether to include baggage in propagation headers.
            custom_headers_fn: Optional function to generate custom headers.
        """
        super().__init__(span)
        self._propagate_baggage = propagate_baggage
        self._custom_headers_fn = custom_headers_fn

    def get_trace_headers(self) -> dict[str, str]:
        """Get W3C trace context headers to inject into message.

        Returns:
            Headers object with traceparent and optionally tracestate.

        Example:
            >>> headers = ctx.get_trace_headers()
            >>> await producer.send(topic="events", value=data, headers=headers)
        """
        headers: dict[str, str] = {}
        inject(headers)

        # Filter to just trace context headers
        result: dict[str, str] = {}
        if "traceparent" in headers:
            result["traceparent"] = headers["traceparent"]
        if "tracestate" in headers:
            result["tracestate"] = headers["tracestate"]

        return result

    def get_all_propagation_headers(self) -> dict[str, str]:
        """Get all propagation headers including baggage if enabled.

        Returns:
            Headers object with all W3C trace context headers.
        """
        headers: dict[str, str] = {}
        inject(headers)

        if self._propagate_baggage:
            from opentelemetry.baggage import get_all

            baggage = get_all(otel_context.get_current())
            if baggage:
                from urllib.parse import quote

                entries = [f"{quote(k)}={quote(str(v))}" for k, v in baggage.items()]
                if entries:
                    headers["baggage"] = ",".join(entries)

        return headers

    def get_full_headers(self) -> dict[str, str]:
        """Get all headers including custom headers from hook.

        Combines W3C trace context headers, baggage (if enabled),
        and any custom headers defined via the customHeaders hook.

        Returns:
            Combined headers object.
        """
        headers = self.get_all_propagation_headers()

        if self._custom_headers_fn:
            custom_headers = self._custom_headers_fn(self)
            headers.update(custom_headers)

        return headers


class ConsumerContext(TraceContext):
    """Extended trace context for consumers with DLQ/retry helpers.

    Provides methods for recording DLQ routing, retries, and accessing
    producer span links.
    """

    def __init__(
        self,
        span: Span,
        *,
        producer_links: list[Link] | None = None,
        ordering_state: dict[str, Any] | None = None,
        group_state: ConsumerGroupState | None = None,
        consumer_group: str | None = None,
        on_dlq: Callable[[ConsumerContext, str], None] | None = None,
    ) -> None:
        """Initialize the consumer context.

        Args:
            span: The span for this consumer.
            producer_links: Links extracted from producer spans.
            ordering_state: Message ordering state.
            group_state: Consumer group state.
            consumer_group: Consumer group name.
            on_dlq: Callback when message goes to DLQ.
        """
        super().__init__(span)
        self._producer_links = producer_links or []
        self._ordering_state = ordering_state or {}
        self._group_state = group_state
        self._consumer_group = consumer_group
        self._on_dlq = on_dlq

    def record_dlq(
        self,
        reason: str,
        dlq_name: str | None = None,
        options: DLQOptions | None = None,
    ) -> None:
        """Record that a message is being sent to DLQ.

        Args:
            reason: Human-readable reason for DLQ routing.
            dlq_name: DLQ name/topic (optional).
            options: Enhanced DLQ options.

        Example:
            >>> ctx.record_dlq("Schema validation failed", "orders-dlq")
            >>>
            >>> ctx.record_dlq("Processing timeout", "orders-dlq", DLQOptions(
            ...     reason_category="timeout",
            ...     attempt_count=3,
            ...     original_error=error,
            ... ))
        """
        opts = options or DLQOptions()

        # Set basic attributes
        self._span.set_attribute("messaging.dlq.reason", reason)
        if dlq_name:
            self._span.set_attribute("messaging.dlq.name", dlq_name)

        # Set enhanced attributes
        if opts.reason_category:
            self._span.set_attribute("messaging.dlq.reason_category", opts.reason_category)
        if opts.attempt_count is not None:
            self._span.set_attribute("messaging.dlq.attempt_count", opts.attempt_count)
        if opts.original_error:
            self._span.set_attribute("messaging.dlq.error.type", type(opts.original_error).__name__)
            self._span.set_attribute("messaging.dlq.error.message", str(opts.original_error))

        # Set custom metadata
        if opts.metadata:
            for key, value in opts.metadata.items():
                self._span.set_attribute(f"messaging.dlq.metadata.{key}", value)

        # Auto-link to producer span if available and enabled
        if opts.link_to_producer and self._producer_links:
            producer_link = self._producer_links[0]
            self._span.set_attribute(
                "messaging.dlq.producer_trace_id",
                format(producer_link.context.trace_id, "032x"),
            )
            self._span.set_attribute(
                "messaging.dlq.producer_span_id",
                format(producer_link.context.span_id, "016x"),
            )

        # Record event
        event_attrs: dict[str, Any] = {"messaging.dlq.reason": reason}
        if dlq_name:
            event_attrs["messaging.dlq.name"] = dlq_name
        if opts.reason_category:
            event_attrs["messaging.dlq.reason_category"] = opts.reason_category
        if opts.attempt_count is not None:
            event_attrs["messaging.dlq.attempt_count"] = opts.attempt_count

        self._span.add_event("dlq_routed", event_attrs)

        # Call user's onDLQ callback
        if self._on_dlq:
            self._on_dlq(self, reason)

    def record_replay(self, options: DLQReplayOptions | None = None) -> None:
        """Record replay of a message from DLQ.

        Args:
            options: Replay tracking options.
        """
        opts = options or DLQReplayOptions()

        self._span.set_attribute("messaging.replay", True)

        if opts.replay_attempt is not None:
            self._span.set_attribute("messaging.replay.attempt", opts.replay_attempt)
        if opts.dlq_dwell_time_ms is not None:
            self._span.set_attribute("messaging.replay.dwell_time_ms", opts.dlq_dwell_time_ms)

        event_attrs: dict[str, Any] = {"messaging.replay": True}
        if opts.replay_attempt is not None:
            event_attrs["messaging.replay.attempt"] = opts.replay_attempt
        if opts.dlq_dwell_time_ms is not None:
            event_attrs["messaging.replay.dwell_time_ms"] = opts.dlq_dwell_time_ms

        self._span.add_event("dlq_replay", event_attrs)

    def record_retry(self, attempt_number: int, max_attempts: int | None = None) -> None:
        """Record retry attempt.

        Args:
            attempt_number: Current retry attempt (1-based).
            max_attempts: Maximum retry attempts.
        """
        self._span.set_attribute("messaging.retry.count", attempt_number)
        if max_attempts is not None:
            self._span.set_attribute("messaging.retry.max_attempts", max_attempts)

        event_attrs: dict[str, Any] = {"messaging.retry.count": attempt_number}
        if max_attempts is not None:
            event_attrs["messaging.retry.max_attempts"] = max_attempts

        self._span.add_event("retry_attempt", event_attrs)

    def get_producer_links(self) -> list[Link]:
        """Get the producer span context links extracted from message headers.

        Returns:
            Array of span links extracted from the message, or empty array.
        """
        return list(self._producer_links)

    def is_duplicate(self) -> bool:
        """Check if the current message is a duplicate.

        Returns:
            True if the message has been seen before.
        """
        return self._ordering_state.get("is_duplicate", False)  # type: ignore[no-any-return]

    def get_out_of_order_info(self) -> OutOfOrderInfo | None:
        """Check if the current message arrived out of order.

        Returns:
            Out of order info, or None if in order.
        """
        return self._ordering_state.get("out_of_order_info")

    def get_sequence_number(self) -> int | None:
        """Get current sequence number.

        Returns:
            The sequence number, or None if not configured.
        """
        return self._ordering_state.get("sequence_number")

    def get_partition_key(self) -> str | None:
        """Get partition key.

        Returns:
            The partition key, or None if not configured.
        """
        return self._ordering_state.get("partition_key")

    def record_rebalance(self, event: RebalanceEvent) -> None:
        """Record a rebalance event.

        Args:
            event: The rebalance event details.
        """
        if self._group_state:
            if event.type == "assigned":
                self._group_state.assigned_partitions = list(event.partitions)
                self._group_state.is_active = True
                self._group_state.state = "stable"
            elif event.type in ("revoked", "lost"):
                revoked_set = {
                    f"{p.topic}:{p.partition}" for p in event.partitions
                }
                self._group_state.assigned_partitions = [
                    p
                    for p in self._group_state.assigned_partitions
                    if f"{p.topic}:{p.partition}" not in revoked_set
                ]
                if event.type == "lost":
                    self._group_state.is_active = False
                    self._group_state.state = "dead"

            if event.generation is not None:
                self._group_state.generation = event.generation
            if event.member_id:
                self._group_state.member_id = event.member_id

        # Set span attributes
        self._span.set_attribute("messaging.consumer_group.rebalance.type", event.type)
        self._span.set_attribute(
            "messaging.consumer_group.rebalance.partition_count",
            len(event.partitions),
        )
        if event.generation is not None:
            self._span.set_attribute("messaging.consumer_group.generation", event.generation)
        if event.member_id:
            self._span.set_attribute("messaging.consumer_group.member_id", event.member_id)
        if event.reason:
            self._span.set_attribute("messaging.consumer_group.rebalance.reason", event.reason)

        # Record event
        event_attrs: dict[str, Any] = {
            "messaging.consumer_group.rebalance.type": event.type,
            "messaging.consumer_group.rebalance.partition_count": len(event.partitions),
            "messaging.consumer_group.rebalance.timestamp": event.timestamp,
        }
        if event.generation is not None:
            event_attrs["messaging.consumer_group.generation"] = event.generation

        self._span.add_event(f"consumer_group_{event.type}", event_attrs)

    def record_heartbeat(self, healthy: bool, latency_ms: float | None = None) -> None:
        """Record a heartbeat event.

        Args:
            healthy: Whether the heartbeat was successful.
            latency_ms: Optional latency of the heartbeat in milliseconds.
        """
        import time

        if self._group_state:
            self._group_state.last_heartbeat = time.time()

        self._span.set_attribute("messaging.consumer_group.heartbeat.healthy", healthy)
        if latency_ms is not None:
            self._span.set_attribute("messaging.consumer_group.heartbeat.latency_ms", latency_ms)

        event_attrs: dict[str, Any] = {
            "messaging.consumer_group.heartbeat.healthy": healthy,
        }
        if latency_ms is not None:
            event_attrs["messaging.consumer_group.heartbeat.latency_ms"] = latency_ms

        self._span.add_event("consumer_group_heartbeat", event_attrs)

    def record_partition_lag(self, lag: PartitionLag) -> None:
        """Record partition lag for a specific partition.

        Args:
            lag: The partition lag information.
        """
        prefix = f"messaging.consumer_group.lag.{lag.topic}.{lag.partition}"

        self._span.set_attribute(f"{prefix}.current_offset", lag.current_offset)
        self._span.set_attribute(f"{prefix}.end_offset", lag.end_offset)
        self._span.set_attribute(f"{prefix}.lag", lag.lag)

        self._span.add_event(
            "partition_lag_recorded",
            {
                "messaging.consumer_group.lag.topic": lag.topic,
                "messaging.consumer_group.lag.partition": lag.partition,
                "messaging.consumer_group.lag.lag": lag.lag,
            },
        )

    def get_consumer_group_state(self) -> ConsumerGroupState | None:
        """Get the current consumer group state.

        Returns:
            The current consumer group state, or None if not configured.
        """
        if not self._consumer_group:
            return None

        if self._group_state:
            return ConsumerGroupState(
                group_id=self._consumer_group,
                member_id=self._group_state.member_id,
                group_instance_id=self._group_state.group_instance_id,
                assigned_partitions=list(self._group_state.assigned_partitions),
                generation=self._group_state.generation,
                is_active=self._group_state.is_active,
                last_heartbeat=self._group_state.last_heartbeat,
                state=self._group_state.state,
            )

        return ConsumerGroupState(group_id=self._consumer_group)

    def get_member_id(self) -> str | None:
        """Get the consumer member ID.

        Returns:
            The member ID, or None if not available.
        """
        return self._group_state.member_id if self._group_state else None


# ============================================================================
# Global State for Ordering/Deduplication
# ============================================================================

_sequence_trackers: dict[str, int] = {}
_deduplication_window: dict[str, float] = {}


def clear_ordering_state() -> None:
    """Clear sequence tracking state (useful for testing)."""
    _sequence_trackers.clear()
    _deduplication_window.clear()


# ============================================================================
# Header Injection/Extraction Helpers
# ============================================================================


def inject_trace_headers(headers: dict[str, str] | None = None) -> dict[str, str]:
    """
    Inject current trace context into headers for message propagation.

    Use this when producing messages to include trace context that consumers
    can extract to link spans.

    Args:
        headers: Optional existing headers dict to add to. If None, creates new dict.

    Returns:
        Headers dict with traceparent/tracestate added.

    Example:
        >>> headers = inject_trace_headers()
        >>> kafka_producer.send("topic", value=data, headers=headers)

        >>> # Or add to existing headers
        >>> headers = {"content-type": "application/json"}
        >>> headers = inject_trace_headers(headers)
    """
    result = dict(headers) if headers else {}
    inject(result)
    return result


def extract_trace_context(headers: Mapping[str, str]) -> otel_context.Context:
    """
    Extract trace context from message headers.

    Use this when consuming messages to continue the trace from the producer.

    Args:
        headers: Message headers containing traceparent/tracestate.

    Returns:
        OpenTelemetry context that can be used to start a linked span.

    Example:
        >>> ctx = extract_trace_context(message.headers)
        >>> with tracer.start_as_current_span("process", context=ctx):
        ...     process_message(message)
    """
    return extract(dict(headers))


def trace_consumer(
    system: str,
    destination: str | None = None,
    *,
    name: str | None = None,
    link_from_headers: bool = True,
    headers_key: str = "headers",
    attributes: dict[str, Any] | None = None,
) -> Callable[[Callable[P, R]], Callable[P, R]]:
    """
    Decorator for message consumer handlers with automatic trace linking.

    Creates a CONSUMER span with semantic convention attributes and optionally
    links to the producer span via headers.

    Args:
        system: Messaging system (e.g., "kafka", "rabbitmq", "sqs", "redis")
        destination: Topic/queue name (e.g., "orders", "notifications")
        name: Custom span name. Defaults to "{destination} receive" or function name.
        link_from_headers: If True, extracts trace context from message headers
            and creates a link to the producer span.
        headers_key: Key in message dict/object containing headers. Used for
            automatic link extraction. Default: "headers".
        attributes: Additional span attributes to set.

    Returns:
        Decorated function with consumer tracing.

    Example:
        >>> @trace_consumer(system="kafka", destination="orders")
        ... async def handle_order(ctx, message: dict[str, Any]):
        ...     ctx.set_attribute("order.id", message["order_id"])
        ...     await process_order(message)

        >>> # With automatic header extraction
        >>> @trace_consumer(
        ...     system="rabbitmq",
        ...     destination="notifications",
        ...     headers_key="properties"
        ... )
        ... def process_notification(ctx, delivery):
        ...     # Link to producer span is automatically created
        ...     ctx.set_attribute("notification.type", delivery.type)
    """

    def decorator(func: Callable[P, R]) -> Callable[P, R]:
        # Detect if function expects ctx parameter
        sig = inspect.signature(func)
        params = list(sig.parameters.keys())
        needs_ctx = len(params) > 0 and params[0] in CTX_PARAM_NAMES

        # Infer span name
        span_name = name or (f"{destination} receive" if destination else func.__name__)

        def _extract_links_from_args(args: tuple[Any, ...], kwargs: dict[str, Any]) -> list[Link]:
            """Try to extract links from message headers in args."""
            links: list[Link] = []
            if not link_from_headers:
                return links

            # Check first positional arg (after ctx if present)
            msg_arg_index = 1 if needs_ctx else 0
            message = args[msg_arg_index] if len(args) > msg_arg_index else None

            if message is None:
                # Try to get from kwargs
                # Common param names for messages
                for key in ("message", "msg", "event", "record", "delivery"):
                    if key in kwargs:
                        message = kwargs[key]
                        break

            if message is not None:
                headers = None
                if isinstance(message, dict):
                    headers = message.get(headers_key, {})
                elif hasattr(message, headers_key):
                    headers = getattr(message, headers_key, {})

                if headers:
                    link = create_link_from_headers(
                        dict(headers), {"messaging.link.type": "consumer"}
                    )
                    if link:
                        links.append(link)

            return links

        # Shared span body for the async and sync wrappers; only the func call
        # (await vs direct) differs between the two below.
        def _setup(span: Span) -> None:
            _set_messaging_conventions(span, system, "receive", destination)
            _apply_message_attributes(span, attributes)

        if inspect.iscoroutinefunction(func):

            @functools.wraps(func)
            async def async_wrapper(*args: P.args, **kwargs: P.kwargs) -> R:
                tracer = otel_trace.get_tracer(__name__)
                links = _extract_links_from_args(args, kwargs)
                with (
                    run_in_operation_context(span_name),
                    tracer.start_as_current_span(
                        span_name, kind=SpanKind.CONSUMER, links=links
                    ) as span,
                ):
                    try:
                        _setup(span)
                        if needs_ctx:
                            ctx = ConsumerContext(span, producer_links=links)
                            return await func(ctx, *args, **kwargs)  # type: ignore[arg-type, no-any-return]
                        return await func(*args, **kwargs)  # type: ignore[no-any-return]
                    except Exception as e:
                        span.record_exception(e)
                        span.set_status(StatusCode.ERROR, str(e))
                        raise

            if needs_ctx:
                _rewrite_signature_without_ctx(async_wrapper, func)
            return async_wrapper  # type: ignore[return-value]
        else:

            @functools.wraps(func)
            def sync_wrapper(*args: P.args, **kwargs: P.kwargs) -> R:
                tracer = otel_trace.get_tracer(__name__)
                links = _extract_links_from_args(args, kwargs)
                with (
                    run_in_operation_context(span_name),
                    tracer.start_as_current_span(
                        span_name, kind=SpanKind.CONSUMER, links=links
                    ) as span,
                ):
                    try:
                        _setup(span)
                        if needs_ctx:
                            ctx = ConsumerContext(span, producer_links=links)
                            return func(ctx, *args, **kwargs)  # type: ignore[arg-type]
                        return func(*args, **kwargs)
                    except Exception as e:
                        span.record_exception(e)
                        span.set_status(StatusCode.ERROR, str(e))
                        raise

            if needs_ctx:
                _rewrite_signature_without_ctx(sync_wrapper, func)
            return sync_wrapper

    return decorator


def trace_producer(
    system: str,
    destination: str | None = None,
    *,
    name: str | None = None,
    attributes: dict[str, Any] | None = None,
    propagate_baggage: bool = False,
    custom_headers: Callable[[ProducerContext], dict[str, str]] | None = None,
) -> Callable[[Callable[P, R]], Callable[P, R]]:
    """
    Decorator for message producer functions with automatic context injection.

    Creates a PRODUCER span with semantic convention attributes. The context
    provides methods for injecting trace headers into message headers.

    Args:
        system: Messaging system (e.g., "kafka", "rabbitmq", "sqs", "redis")
        destination: Topic/queue name (e.g., "orders", "notifications")
        name: Custom span name. Defaults to "{destination} send" or function name.
        attributes: Additional span attributes to set.
        propagate_baggage: If True, includes baggage in propagation headers.
        custom_headers: Optional function to generate additional custom headers.

    Returns:
        Decorated function with producer tracing.

    Example:
        >>> @trace_producer(system="kafka", destination="order-events")
        ... async def publish_order_event(ctx, order: Order):
        ...     ctx.set_attribute("order.id", order.id)
        ...     headers = ctx.get_trace_headers()  # Get trace context headers
        ...     await kafka.send("order-events", value=order.dict(), headers=headers)

        >>> # Or get all headers including baggage
        >>> @trace_producer(system="kafka", destination="events", propagate_baggage=True)
        ... async def publish_event(ctx, event: dict):
        ...     headers = ctx.get_all_propagation_headers()
        ...     await kafka.send("events", value=event, headers=headers)
    """

    def decorator(func: Callable[P, R]) -> Callable[P, R]:
        # Detect if function expects ctx parameter
        sig = inspect.signature(func)
        params = list(sig.parameters.keys())
        needs_ctx = len(params) > 0 and params[0] in CTX_PARAM_NAMES

        # Infer span name
        span_name = name or (f"{destination} send" if destination else func.__name__)

        # Shared span body for the async and sync wrappers; only the func call
        # (await vs direct) differs between the two below.
        def _setup(span: Span) -> None:
            _set_messaging_conventions(span, system, "send", destination)
            _apply_message_attributes(span, attributes)

        def _make_ctx(span: Span) -> ProducerContext:
            return ProducerContext(
                span,
                propagate_baggage=propagate_baggage,
                custom_headers_fn=custom_headers,
            )

        if inspect.iscoroutinefunction(func):

            @functools.wraps(func)
            async def async_wrapper(*args: P.args, **kwargs: P.kwargs) -> R:
                tracer = otel_trace.get_tracer(__name__)
                with (
                    run_in_operation_context(span_name),
                    tracer.start_as_current_span(span_name, kind=SpanKind.PRODUCER) as span,
                ):
                    try:
                        _setup(span)
                        if needs_ctx:
                            ctx = _make_ctx(span)
                            return await func(ctx, *args, **kwargs)  # type: ignore[arg-type, no-any-return]
                        return await func(*args, **kwargs)  # type: ignore[no-any-return]
                    except Exception as e:
                        span.record_exception(e)
                        span.set_status(StatusCode.ERROR, str(e))
                        raise

            if needs_ctx:
                _rewrite_signature_without_ctx(async_wrapper, func)
            return async_wrapper  # type: ignore[return-value]
        else:

            @functools.wraps(func)
            def sync_wrapper(*args: P.args, **kwargs: P.kwargs) -> R:
                tracer = otel_trace.get_tracer(__name__)
                with (
                    run_in_operation_context(span_name),
                    tracer.start_as_current_span(span_name, kind=SpanKind.PRODUCER) as span,
                ):
                    try:
                        _setup(span)
                        if needs_ctx:
                            ctx = _make_ctx(span)
                            return func(ctx, *args, **kwargs)  # type: ignore[arg-type]
                        return func(*args, **kwargs)
                    except Exception as e:
                        span.record_exception(e)
                        span.set_status(StatusCode.ERROR, str(e))
                        raise

            if needs_ctx:
                _rewrite_signature_without_ctx(sync_wrapper, func)
            return sync_wrapper

    return decorator


# --- DLQ/Retry Recording Helpers ---


def record_dlq(
    ctx_or_span: TraceContext | otel_trace.Span,
    *,
    original_destination: str,
    dlq_destination: str,
    reason: str,
    retry_count: int | None = None,
    original_message_id: str | None = None,
    attributes: dict[str, Any] | None = None,
) -> None:
    """
    Record that a message was sent to a Dead Letter Queue (DLQ).

    Use this when a message cannot be processed and is being moved to a DLQ.
    Records standardized attributes for DLQ observability.

    Args:
        ctx_or_span: TraceContext or Span to record the DLQ event on.
        original_destination: Original topic/queue the message came from.
        dlq_destination: DLQ topic/queue the message is being sent to.
        reason: Human-readable reason for DLQ (e.g., "max_retries_exceeded", "poison_pill").
        retry_count: Number of retry attempts before DLQ (optional).
        original_message_id: Original message ID for correlation (optional).
        attributes: Additional custom attributes (optional).

    Example:
        >>> @trace_consumer(system="kafka", destination="orders")
        ... async def handle_order(ctx, message):
        ...     try:
        ...         await process_order(message)
        ...     except MaxRetriesExceeded:
        ...         record_dlq(
        ...             ctx,
        ...             original_destination="orders",
        ...             dlq_destination="orders.dlq",
        ...             reason="max_retries_exceeded",
        ...             retry_count=3,
        ...         )
        ...         await send_to_dlq(message)
    """
    span = ctx_or_span._span if isinstance(ctx_or_span, TraceContext) else ctx_or_span

    # Record DLQ event with standardized attributes
    dlq_attrs: dict[str, Any] = {
        "messaging.dlq.original_destination": original_destination,
        "messaging.dlq.destination": dlq_destination,
        "messaging.dlq.reason": reason,
    }

    if retry_count is not None:
        dlq_attrs["messaging.dlq.retry_count"] = retry_count

    if original_message_id is not None:
        dlq_attrs["messaging.dlq.original_message_id"] = original_message_id

    if attributes:
        dlq_attrs.update(attributes)

    # Set span attributes
    for key, value in dlq_attrs.items():
        if isinstance(value, str | bool | int | float):
            span.set_attribute(key, value)
        else:
            span.set_attribute(key, str(value))

    # Add event for timeline visibility
    span.add_event("message.dlq", dlq_attrs)


def record_retry(
    ctx_or_span: TraceContext | otel_trace.Span,
    *,
    attempt: int,
    max_attempts: int,
    backoff_ms: int | None = None,
    last_error: str | None = None,
    next_retry_at: str | None = None,
    attributes: dict[str, Any] | None = None,
) -> None:
    """
    Record a message retry attempt.

    Use this when a message processing fails and is being retried.
    Records standardized attributes for retry observability.

    Args:
        ctx_or_span: TraceContext or Span to record the retry on.
        attempt: Current retry attempt number (1-indexed).
        max_attempts: Maximum number of retry attempts allowed.
        backoff_ms: Backoff delay in milliseconds before next retry (optional).
        last_error: Error message from the last attempt (optional).
        next_retry_at: ISO timestamp of next retry attempt (optional).
        attributes: Additional custom attributes (optional).

    Example:
        >>> @trace_consumer(system="kafka", destination="orders")
        ... async def handle_order(ctx, message):
        ...     for attempt in range(1, 4):
        ...         try:
        ...             await process_order(message)
        ...             break
        ...         except TransientError as e:
        ...             record_retry(
        ...                 ctx,
        ...                 attempt=attempt,
        ...                 max_attempts=3,
        ...                 backoff_ms=1000 * attempt,
        ...                 last_error=str(e),
        ...             )
        ...             await asyncio.sleep(attempt)
    """
    span = ctx_or_span._span if isinstance(ctx_or_span, TraceContext) else ctx_or_span

    retry_attrs: dict[str, Any] = {
        "messaging.retry.attempt": attempt,
        "messaging.retry.max_attempts": max_attempts,
    }

    if backoff_ms is not None:
        retry_attrs["messaging.retry.backoff_ms"] = backoff_ms

    if last_error is not None:
        retry_attrs["messaging.retry.last_error"] = last_error

    if next_retry_at is not None:
        retry_attrs["messaging.retry.next_retry_at"] = next_retry_at

    if attributes:
        retry_attrs.update(attributes)

    # Set span attributes
    for key, value in retry_attrs.items():
        if isinstance(value, str | bool | int | float):
            span.set_attribute(key, value)
        else:
            span.set_attribute(key, str(value))

    # Add event for timeline visibility
    span.add_event("message.retry", retry_attrs)


# --- Consumer Lag Metrics ---


def record_consumer_lag(
    ctx_or_span: TraceContext | otel_trace.Span,
    *,
    lag_ms: int | None = None,
    lag_messages: int | None = None,
    partition: int | None = None,
    consumer_group: str | None = None,
    committed_offset: int | None = None,
    high_watermark: int | None = None,
    attributes: dict[str, Any] | None = None,
) -> None:
    """
    Record consumer lag metrics on the current span.

    Consumer lag is critical for monitoring message queue health. This helper
    records standardized attributes for lag observability.

    Args:
        ctx_or_span: TraceContext or Span to record the lag on.
        lag_ms: Time lag in milliseconds (message age).
        lag_messages: Number of messages behind (offset lag).
        partition: Partition number (for partitioned queues like Kafka).
        consumer_group: Consumer group ID.
        committed_offset: Last committed offset.
        high_watermark: High watermark offset (latest available).
        attributes: Additional custom attributes.

    Example:
        >>> @trace_consumer(system="kafka", destination="orders")
        ... async def handle_order(ctx, message):
        ...     # Record lag before processing
        ...     record_consumer_lag(
        ...         ctx,
        ...         lag_ms=int(time.time() * 1000) - message.timestamp,
        ...         lag_messages=message.high_watermark - message.offset,
        ...         partition=message.partition,
        ...     )
        ...     await process_order(message)
    """
    span = ctx_or_span._span if isinstance(ctx_or_span, TraceContext) else ctx_or_span

    lag_attrs: dict[str, Any] = {}

    if lag_ms is not None:
        lag_attrs["messaging.consumer.lag_ms"] = lag_ms

    if lag_messages is not None:
        lag_attrs["messaging.consumer.lag_messages"] = lag_messages

    if partition is not None:
        lag_attrs["messaging.kafka.partition"] = partition

    if consumer_group is not None:
        lag_attrs["messaging.kafka.consumer_group"] = consumer_group

    if committed_offset is not None:
        lag_attrs["messaging.kafka.committed_offset"] = committed_offset

    if high_watermark is not None:
        lag_attrs["messaging.kafka.high_watermark"] = high_watermark

    if attributes:
        lag_attrs.update(attributes)

    # Set span attributes
    for key, value in lag_attrs.items():
        if isinstance(value, str | bool | int | float):
            span.set_attribute(key, value)
        else:
            span.set_attribute(key, str(value))


def trace_batch_consumer(
    system: str,
    destination: str | None = None,
    *,
    name: str | None = None,
    headers_key: str = "headers",
    attributes: dict[str, Any] | None = None,
) -> Callable[[Callable[P, R]], Callable[P, R]]:
    """
    Decorator for batch message consumer handlers with multi-link support.

    Creates a CONSUMER span that links to ALL producer spans in a batch.
    Useful for fan-in scenarios where one handler processes multiple messages.

    Args:
        system: Messaging system (e.g., "kafka", "sqs")
        destination: Topic/queue name
        name: Custom span name. Defaults to "{destination} batch receive".
        headers_key: Key in each message dict containing headers.
        attributes: Additional span attributes.

    Returns:
        Decorated function with batch consumer tracing.

    Example:
        >>> @trace_batch_consumer(system="sqs", destination="tasks")
        ... async def process_batch(ctx, messages: list[dict]):
        ...     ctx.set_attribute("messaging.batch.message_count", len(messages))
        ...     for msg in messages:
        ...         await process_task(msg)

        >>> # Each message's trace context is extracted and linked
    """

    def decorator(func: Callable[P, R]) -> Callable[P, R]:
        sig = inspect.signature(func)
        params = list(sig.parameters.keys())
        needs_ctx = len(params) > 0 and params[0] in CTX_PARAM_NAMES

        span_name = name or (f"{destination} batch receive" if destination else func.__name__)

        def _extract_batch_links(args: tuple[Any, ...], kwargs: dict[str, Any]) -> list[Link]:
            """Extract links from all messages in a batch."""
            links: list[Link] = []

            # Find the messages argument
            msg_arg_index = 1 if needs_ctx else 0
            messages = args[msg_arg_index] if len(args) > msg_arg_index else None

            if messages is None:
                for key in ("messages", "batch", "records", "events"):
                    if key in kwargs:
                        messages = kwargs[key]
                        break

            if messages and hasattr(messages, "__iter__"):
                for msg in messages:
                    headers = None
                    if isinstance(msg, dict):
                        headers = msg.get(headers_key, {})
                    elif hasattr(msg, headers_key):
                        headers = getattr(msg, headers_key, {})

                    if headers:
                        link = create_link_from_headers(
                            dict(headers), {"messaging.link.type": "batch_consumer"}
                        )
                        if link:
                            links.append(link)

            return links

        # Shared span body for the async and sync wrappers; only the func call
        # (await vs direct) differs between the two below.
        def _setup(span: Span, link_count: int) -> None:
            _set_messaging_conventions(span, system, "receive", destination)
            span.set_attribute("messaging.batch.message_count", link_count)
            _apply_message_attributes(span, attributes)

        if inspect.iscoroutinefunction(func):

            @functools.wraps(func)
            async def async_wrapper(*args: P.args, **kwargs: P.kwargs) -> R:
                tracer = otel_trace.get_tracer(__name__)
                links = _extract_batch_links(args, kwargs)
                with (
                    run_in_operation_context(span_name),
                    tracer.start_as_current_span(
                        span_name, kind=SpanKind.CONSUMER, links=links
                    ) as span,
                ):
                    try:
                        _setup(span, len(links))
                        if needs_ctx:
                            ctx = TraceContext(span)
                            return await func(ctx, *args, **kwargs)  # type: ignore[arg-type, no-any-return]
                        return await func(*args, **kwargs)  # type: ignore[no-any-return]
                    except Exception as e:
                        span.record_exception(e)
                        span.set_status(StatusCode.ERROR, str(e))
                        raise

            if needs_ctx:
                _rewrite_signature_without_ctx(async_wrapper, func)
            return async_wrapper  # type: ignore[return-value]
        else:

            @functools.wraps(func)
            def sync_wrapper(*args: P.args, **kwargs: P.kwargs) -> R:
                tracer = otel_trace.get_tracer(__name__)
                links = _extract_batch_links(args, kwargs)
                with (
                    run_in_operation_context(span_name),
                    tracer.start_as_current_span(
                        span_name, kind=SpanKind.CONSUMER, links=links
                    ) as span,
                ):
                    try:
                        _setup(span, len(links))
                        if needs_ctx:
                            ctx = TraceContext(span)
                            return func(ctx, *args, **kwargs)  # type: ignore[arg-type]
                        return func(*args, **kwargs)
                    except Exception as e:
                        span.record_exception(e)
                        span.set_status(StatusCode.ERROR, str(e))
                        raise

            if needs_ctx:
                _rewrite_signature_without_ctx(sync_wrapper, func)
            return sync_wrapper

    return decorator
