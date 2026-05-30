"""Pre-built adapter configurations for common messaging systems.

These adapters provide ready-to-use hook configurations for systems
not explicitly supported by the core messaging module. Use them with
trace_producer/trace_consumer to get system-specific attributes.

Example:
    >>> from autotel.messaging import trace_consumer
    >>> from autotel.messaging_adapters import nats_adapter, datadog_context_extractor
    >>>
    >>> @trace_consumer(
    ...     system="nats",
    ...     destination="orders",
    ...     headers_from=nats_adapter.headers_from,
    ...     custom_attributes=nats_adapter.custom_attributes,
    ... )
    ... async def process_message(ctx, msg):
    ...     # msg.subject, msg.info.stream are captured as span attributes
    ...     await handle_order(msg.data)
    >>>
    >>> # Using Datadog context propagation:
    >>> @trace_consumer(
    ...     system="kafka",
    ...     destination="events",
    ...     custom_context_extractor=datadog_context_extractor,
    ... )
    ... async def process_dd_message(ctx, msg):
    ...     # Parent span from Datadog trace headers is linked
    ...     pass
"""

from __future__ import annotations

import re
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

from opentelemetry.trace import SpanContext, TraceFlags

from .context import TraceContext

# ============================================================================
# Adapter Types
# ============================================================================


@dataclass
class ProducerAdapter:
    """Producer adapter configuration."""

    custom_attributes: Callable[..., dict[str, Any]] | None = None
    """Hook to add system-specific attributes to producer spans."""

    custom_headers: Callable[[TraceContext], dict[str, str]] | None = None
    """Hook to inject custom headers beyond W3C traceparent."""


@dataclass
class ConsumerAdapter:
    """Consumer adapter configuration."""

    headers_from: Callable[[Any], dict[str, str] | None] | None = None
    """Extract headers from the message for trace context propagation."""

    custom_attributes: Callable[..., dict[str, Any]] | None = None
    """Hook to add system-specific attributes to consumer spans."""

    custom_context_extractor: Callable[[dict[str, str]], SpanContext | None] | None = None
    """Hook to extract parent span context from non-W3C header formats."""


@dataclass
class MessagingAdapter:
    """Combined producer and consumer adapter."""

    producer: ProducerAdapter | None = None
    consumer: ConsumerAdapter | None = None


# ============================================================================
# NATS JetStream Adapter
# ============================================================================


def _nats_producer_attributes(*args: Any, **_kwargs: Any) -> dict[str, Any]:
    """Extract NATS-specific attributes from producer arguments."""
    attrs: dict[str, Any] = {}

    if args:
        msg = args[0]
        if hasattr(msg, "subject"):
            attrs["nats.subject"] = msg.subject
        if hasattr(msg, "reply_to"):
            attrs["nats.reply_to"] = msg.reply_to
        if hasattr(msg, "stream"):
            attrs["nats.stream"] = msg.stream

    return attrs


def _nats_consumer_headers(msg: Any) -> dict[str, str] | None:
    """Extract headers from NATS message."""
    headers = getattr(msg, "headers", None)
    if not headers:
        return None

    # Try dict conversion methods
    if hasattr(headers, "to_dict"):
        try:
            result = headers.to_dict()
            return {str(k): str(v) for k, v in result.items()} if result else None
        except Exception:
            pass

    if callable(getattr(headers, "items", None)):
        try:
            return {str(k): str(v) for k, v in headers.items()}
        except Exception:
            pass

    # Try getting common trace headers individually
    if callable(getattr(headers, "get", None)):
        extracted: dict[str, str] = {}
        trace_headers = [
            "traceparent",
            "tracestate",
            "baggage",
            "x-b3-traceid",
            "x-b3-spanid",
            "x-b3-sampled",
            "b3",
        ]
        for key in trace_headers:
            try:
                value = headers.get(key)
                if value:
                    extracted[key] = str(value)
            except Exception:
                pass
        if extracted:
            return extracted

    return None


def _nats_consumer_attributes(_ctx: Any, msg: Any) -> dict[str, Any]:
    """Extract NATS-specific attributes from consumer message."""
    attrs: dict[str, Any] = {}

    if hasattr(msg, "subject"):
        attrs["nats.subject"] = msg.subject
    if hasattr(msg, "reply"):
        attrs["nats.reply_to"] = msg.reply

    info = getattr(msg, "info", None)
    if info:
        if hasattr(info, "stream"):
            attrs["nats.stream"] = info.stream
        if hasattr(info, "consumer"):
            attrs["nats.consumer"] = info.consumer
        if hasattr(info, "redelivery_count"):
            attrs["nats.delivered_count"] = info.redelivery_count
        if hasattr(info, "pending"):
            attrs["nats.pending"] = info.pending

    return attrs


nats_adapter = MessagingAdapter(
    producer=ProducerAdapter(custom_attributes=_nats_producer_attributes),
    consumer=ConsumerAdapter(
        headers_from=_nats_consumer_headers,
        custom_attributes=_nats_consumer_attributes,
    ),
)
"""NATS JetStream adapter.

Captures NATS-specific attributes following NATS observability conventions.

Example:
    >>> @trace_consumer(
    ...     system="nats",
    ...     destination="orders.created",
    ...     headers_from=nats_adapter.consumer.headers_from,
    ...     custom_attributes=nats_adapter.consumer.custom_attributes,
    ... )
    ... async def process_order(ctx, msg):
    ...     await handle_order(msg.data)
    ...     msg.ack()
"""


# ============================================================================
# Temporal Adapter
# ============================================================================


def _temporal_producer_attributes(*args: Any, **_kwargs: Any) -> dict[str, Any]:
    """Extract Temporal-specific attributes from producer arguments."""
    attrs: dict[str, Any] = {}

    if args:
        info = args[0]
        if hasattr(info, "workflow_id"):
            attrs["temporal.workflow_id"] = info.workflow_id
        if hasattr(info, "run_id"):
            attrs["temporal.run_id"] = info.run_id
        if hasattr(info, "task_queue"):
            attrs["temporal.task_queue"] = info.task_queue
        if hasattr(info, "workflow_type"):
            attrs["temporal.workflow_type"] = info.workflow_type

    return attrs


def _temporal_consumer_attributes(_ctx: Any, msg: Any) -> dict[str, Any]:
    """Extract Temporal-specific attributes from activity info."""
    attrs: dict[str, Any] = {}

    if hasattr(msg, "workflow_id"):
        attrs["temporal.workflow_id"] = msg.workflow_id
    if hasattr(msg, "run_id"):
        attrs["temporal.run_id"] = msg.run_id
    if hasattr(msg, "activity_id"):
        attrs["temporal.activity_id"] = msg.activity_id
    if hasattr(msg, "task_queue"):
        attrs["temporal.task_queue"] = msg.task_queue
    if hasattr(msg, "attempt"):
        attrs["temporal.attempt"] = msg.attempt
    if hasattr(msg, "activity_type"):
        attrs["temporal.activity_type"] = msg.activity_type

    return attrs


temporal_adapter = MessagingAdapter(
    producer=ProducerAdapter(custom_attributes=_temporal_producer_attributes),
    consumer=ConsumerAdapter(custom_attributes=_temporal_consumer_attributes),
)
"""Temporal adapter.

Captures Temporal-specific attributes for workflow activities.

Example:
    >>> @trace_consumer(
    ...     system="temporal",
    ...     destination="order-activities",
    ...     custom_attributes=temporal_adapter.consumer.custom_attributes,
    ... )
    ... async def process_order(ctx, info, input_data):
    ...     # Temporal attributes are captured automatically
    ...     return process_order_logic(input_data)
"""


# ============================================================================
# Cloudflare Queues Adapter
# ============================================================================


def _cloudflare_consumer_attributes(_ctx: Any, msg: Any) -> dict[str, Any]:
    """Extract Cloudflare Queue-specific attributes from message."""
    attrs: dict[str, Any] = {}

    if hasattr(msg, "id"):
        attrs["cloudflare.queue.message_id"] = msg.id
    if hasattr(msg, "timestamp"):
        timestamp = msg.timestamp
        if hasattr(timestamp, "timestamp"):
            attrs["cloudflare.queue.timestamp_ms"] = int(timestamp.timestamp() * 1000)
        elif isinstance(timestamp, (int, float)):
            attrs["cloudflare.queue.timestamp_ms"] = int(timestamp * 1000)
    if hasattr(msg, "attempts"):
        attrs["cloudflare.queue.attempts"] = msg.attempts

    return attrs


cloudflare_queues_adapter = MessagingAdapter(
    consumer=ConsumerAdapter(custom_attributes=_cloudflare_consumer_attributes),
)
"""Cloudflare Queues adapter.

Captures Cloudflare Queue-specific attributes.

Example:
    >>> @trace_consumer(
    ...     system="cloudflare_queues",
    ...     destination="my-queue",
    ...     custom_attributes=cloudflare_queues_adapter.consumer.custom_attributes,
    ... )
    ... async def process_message(ctx, msg):
    ...     await handle_message(msg.body)
    ...     msg.ack()
"""


# ============================================================================
# AWS SQS Adapter
# ============================================================================


def _sqs_consumer_headers(msg: Any) -> dict[str, str] | None:
    """Extract headers from SQS message."""
    # SQS puts trace context in message attributes
    attrs = getattr(msg, "message_attributes", None) or getattr(msg, "MessageAttributes", None)
    if not attrs:
        return None

    result: dict[str, str] = {}

    for key in ["traceparent", "tracestate", "baggage", "AWSTraceHeader"]:
        if key in attrs:
            attr = attrs[key]
            # SQS message attribute structure
            if isinstance(attr, dict):
                value = attr.get("StringValue") or attr.get("stringValue")
            else:
                value = str(attr)
            if value:
                result[key.lower().replace("aws", "x-amzn-")] = value

    return result if result else None


def _sqs_consumer_attributes(_ctx: Any, msg: Any) -> dict[str, Any]:
    """Extract SQS-specific attributes from message."""
    attrs: dict[str, Any] = {}

    # Handle both camelCase (SDK v2) and snake_case (boto3) attributes
    message_id = getattr(msg, "message_id", None) or getattr(msg, "MessageId", None)
    if message_id:
        attrs["aws.sqs.message_id"] = message_id

    receipt_handle = getattr(msg, "receipt_handle", None) or getattr(msg, "ReceiptHandle", None)
    if receipt_handle:
        # Only store part of it for debugging
        attrs["aws.sqs.receipt_handle_prefix"] = receipt_handle[:32]

    # Approximate receive count indicates retries
    attrs_dict = getattr(msg, "attributes", None) or getattr(msg, "Attributes", None)
    if attrs_dict:
        if "ApproximateReceiveCount" in attrs_dict:
            attrs["aws.sqs.approximate_receive_count"] = int(
                attrs_dict["ApproximateReceiveCount"]
            )
        if "SentTimestamp" in attrs_dict:
            attrs["aws.sqs.sent_timestamp"] = int(attrs_dict["SentTimestamp"])

    return attrs


sqs_adapter = MessagingAdapter(
    consumer=ConsumerAdapter(
        headers_from=_sqs_consumer_headers,
        custom_attributes=_sqs_consumer_attributes,
    ),
)
"""AWS SQS adapter.

Captures SQS-specific attributes and extracts trace context from message attributes.

Example:
    >>> @trace_consumer(
    ...     system="sqs",
    ...     destination="my-queue",
    ...     headers_from=sqs_adapter.consumer.headers_from,
    ...     custom_attributes=sqs_adapter.consumer.custom_attributes,
    ... )
    ... async def process_message(ctx, msg):
    ...     await handle_message(msg.body)
"""


# ============================================================================
# Redis Streams Adapter
# ============================================================================


def _redis_consumer_headers(msg: Any) -> dict[str, str] | None:
    """Extract headers from Redis stream message."""
    # Redis streams store messages as field-value pairs
    if isinstance(msg, dict):
        result: dict[str, str] = {}
        for key in ["traceparent", "tracestate", "baggage"]:
            if key in msg:
                result[key] = str(msg[key])
        return result if result else None

    # Handle tuple format (stream_id, {field: value})
    if isinstance(msg, tuple | list) and len(msg) >= 2:
        data = msg[1] if isinstance(msg[1], dict) else None
        if data:
            result = {}
            for key in ["traceparent", "tracestate", "baggage"]:
                if key in data:
                    result[key] = str(data[key])
            return result if result else None

    return None


def _redis_consumer_attributes(_ctx: Any, msg: Any) -> dict[str, Any]:
    """Extract Redis stream-specific attributes from message."""
    attrs: dict[str, Any] = {}

    # Handle tuple format (message_id, data)
    if isinstance(msg, tuple | list) and len(msg) >= 2:
        attrs["redis.stream.message_id"] = str(msg[0])

    return attrs


redis_streams_adapter = MessagingAdapter(
    consumer=ConsumerAdapter(
        headers_from=_redis_consumer_headers,
        custom_attributes=_redis_consumer_attributes,
    ),
)
"""Redis Streams adapter.

Captures Redis stream-specific attributes and extracts trace context.

Example:
    >>> @trace_consumer(
    ...     system="redis",
    ...     destination="mystream",
    ...     headers_from=redis_streams_adapter.consumer.headers_from,
    ...     custom_attributes=redis_streams_adapter.consumer.custom_attributes,
    ... )
    ... async def process_message(ctx, msg):
    ...     message_id, data = msg
    ...     await handle_data(data)
"""


# ============================================================================
# Context Extractors for Non-W3C Formats
# ============================================================================


def datadog_context_extractor(headers: dict[str, str]) -> SpanContext | None:
    """Datadog trace context extractor.

    Extracts parent span context from Datadog-format trace headers.
    Converts Datadog's decimal IDs to OpenTelemetry's hex format.

    Note: Datadog sends trace/span IDs as decimal strings, not hex.
    This extractor converts decimal -> hex before formatting for OTel.

    Args:
        headers: Request/message headers

    Returns:
        SpanContext if valid headers found, None otherwise

    Example:
        >>> @trace_consumer(
        ...     system="kafka",
        ...     destination="events",
        ...     custom_context_extractor=datadog_context_extractor,
        ... )
        ... async def process_message(ctx, msg):
        ...     # Links to parent Datadog span automatically
        ...     pass
    """
    trace_id_decimal = headers.get("x-datadog-trace-id")
    span_id_decimal = headers.get("x-datadog-parent-id")
    sampling_priority = headers.get("x-datadog-sampling-priority")

    if not trace_id_decimal or not span_id_decimal:
        return None

    try:
        # Convert decimal to hex and pad to OTel format
        # OTel trace IDs are 32 hex chars (128-bit), Datadog uses 64-bit
        otel_trace_id = format(int(trace_id_decimal), "032x")
        # OTel span IDs are 16 hex chars (64-bit)
        otel_span_id = format(int(span_id_decimal), "016x")
    except (ValueError, OverflowError):
        return None

    # Sampling priority > 0 means sampled
    sampled = int(sampling_priority) > 0 if sampling_priority else True

    return SpanContext(
        trace_id=int(otel_trace_id, 16),
        span_id=int(otel_span_id, 16),
        is_remote=True,
        trace_flags=TraceFlags.SAMPLED if sampled else TraceFlags(0),  # type: ignore[arg-type]
    )


def b3_context_extractor(headers: dict[str, str]) -> SpanContext | None:
    """B3 (Zipkin) trace context extractor.

    Extracts parent span context from B3 format headers.
    Supports both single-header (b3) and multi-header formats.

    See: https://github.com/openzipkin/b3-propagation

    Args:
        headers: Request/message headers

    Returns:
        SpanContext if valid headers found, None otherwise

    Example:
        >>> # Single-header: b3: 80f198ee56343ba864fe8b2a57d3eff7-e457b5a2e4d86bd1-1
        >>> @trace_consumer(
        ...     system="rabbitmq",
        ...     destination="events",
        ...     custom_context_extractor=b3_context_extractor,
        ... )
        ... async def process_message(ctx, msg):
        ...     # Links to parent Zipkin span
        ...     pass
    """
    # Try single-header format first: {TraceId}-{SpanId}-{SamplingState}-{ParentSpanId}
    b3_single = headers.get("b3") or headers.get("B3")
    if b3_single:
        # Handle "0" (not sampled, no trace) case
        if b3_single == "0":
            return None

        parts = b3_single.split("-")
        if len(parts) >= 2:
            trace_id = parts[0]
            span_id = parts[1]
            sampled_flag = parts[2] if len(parts) > 2 else None

            sampled = sampled_flag != "0" and sampled_flag != "d"

            try:
                return SpanContext(
                    trace_id=int(trace_id.zfill(32), 16),
                    span_id=int(span_id.zfill(16), 16),
                    is_remote=True,
                    trace_flags=TraceFlags.SAMPLED if sampled else TraceFlags(0),  # type: ignore[arg-type]
                )
            except ValueError:
                pass

    # Fall back to multi-header format
    trace_id_val = (
        headers.get("x-b3-traceid")
        or headers.get("X-B3-TraceId")
        or headers.get("X-B3-Traceid")
    )
    span_id_val = (
        headers.get("x-b3-spanid")
        or headers.get("X-B3-SpanId")
        or headers.get("X-B3-Spanid")
    )
    sampled_header = (
        headers.get("x-b3-sampled")
        or headers.get("X-B3-Sampled")
        or headers.get("x-b3-flags")
        or headers.get("X-B3-Flags")
    )

    if not trace_id_val or not span_id_val:
        return None

    # x-b3-sampled: "1" or "true" = sampled, "0" or "false" = not sampled
    # x-b3-flags: "1" = debug (implies sampled)
    sampled = sampled_header in ("1", "true", None)

    try:
        return SpanContext(
            trace_id=int(trace_id_val.zfill(32), 16),
            span_id=int(span_id_val.zfill(16), 16),
            is_remote=True,
            trace_flags=TraceFlags.SAMPLED if sampled else TraceFlags(0),  # type: ignore[arg-type]
        )
    except ValueError:
        return None


def xray_context_extractor(headers: dict[str, str]) -> SpanContext | None:
    """AWS X-Ray trace context extractor.

    Extracts parent span context from AWS X-Ray trace header.
    Format: Root=1-{timestamp}-{random};Parent={parent-id};Sampled={0|1}

    Args:
        headers: Request/message headers

    Returns:
        SpanContext if valid headers found, None otherwise

    Example:
        >>> @trace_consumer(
        ...     system="sqs",
        ...     destination="my-queue",
        ...     custom_context_extractor=xray_context_extractor,
        ... )
        ... async def process_message(ctx, msg):
        ...     # Links to parent X-Ray trace
        ...     pass
    """
    xray_header = headers.get("x-amzn-trace-id") or headers.get("X-Amzn-Trace-Id")

    if not xray_header:
        return None

    # Parse: Root=1-{8-char-timestamp}-{24-char-random};Parent={16-char-parent};Sampled=1
    root_match = re.search(r"Root=1-([a-f0-9]{8})-([a-f0-9]{24})", xray_header, re.I)
    parent_match = re.search(r"Parent=([a-f0-9]{16})", xray_header, re.I)
    sampled_match = re.search(r"Sampled=([01])", xray_header)

    if not root_match or not parent_match:
        return None

    # X-Ray trace ID format: 1-{timestamp}-{random} -> OTel: {timestamp}{random}
    timestamp = root_match.group(1)
    random_part = root_match.group(2)
    parent_id = parent_match.group(1)

    trace_id = f"{timestamp}{random_part}"
    span_id = parent_id
    sampled = sampled_match.group(1) == "1" if sampled_match else True

    try:
        return SpanContext(
            trace_id=int(trace_id, 16),
            span_id=int(span_id, 16),
            is_remote=True,
            trace_flags=TraceFlags.SAMPLED if sampled else TraceFlags(0),  # type: ignore[arg-type]
        )
    except ValueError:
        return None


def jaeger_context_extractor(headers: dict[str, str]) -> SpanContext | None:
    """Jaeger trace context extractor.

    Extracts parent span context from Jaeger propagation format.
    Format: {trace-id}:{span-id}:{parent-span-id}:{flags}

    Args:
        headers: Request/message headers

    Returns:
        SpanContext if valid headers found, None otherwise

    Example:
        >>> @trace_consumer(
        ...     system="kafka",
        ...     destination="events",
        ...     custom_context_extractor=jaeger_context_extractor,
        ... )
        ... async def process_message(ctx, msg):
        ...     # Links to parent Jaeger trace
        ...     pass
    """
    uber_trace_id = headers.get("uber-trace-id") or headers.get("Uber-Trace-Id")

    if not uber_trace_id:
        return None

    parts = uber_trace_id.split(":")
    if len(parts) < 4:
        return None

    trace_id = parts[0]
    span_id = parts[1]
    flags = parts[3]

    # Flags: 1 = sampled, 2 = debug
    sampled = int(flags, 16) & 0x01 == 1 if flags else True

    try:
        return SpanContext(
            trace_id=int(trace_id.zfill(32), 16),
            span_id=int(span_id.zfill(16), 16),
            is_remote=True,
            trace_flags=TraceFlags.SAMPLED if sampled else TraceFlags(0),  # type: ignore[arg-type]
        )
    except ValueError:
        return None


# ============================================================================
# Composite Context Extractor
# ============================================================================


def create_multi_format_extractor(
    extractors: list[Callable[[dict[str, str]], SpanContext | None]],
) -> Callable[[dict[str, str]], SpanContext | None]:
    """Create a context extractor that tries multiple formats.

    Useful when you receive messages from multiple systems with different
    trace header formats.

    Args:
        extractors: List of context extractors to try in order

    Returns:
        A composite extractor that returns the first successful result

    Example:
        >>> multi_extractor = create_multi_format_extractor([
        ...     datadog_context_extractor,
        ...     b3_context_extractor,
        ...     xray_context_extractor,
        ... ])
        >>>
        >>> @trace_consumer(
        ...     system="kafka",
        ...     destination="events",
        ...     custom_context_extractor=multi_extractor,
        ... )
        ... async def process_message(ctx, msg):
        ...     # Handles Datadog, B3, or X-Ray trace headers
        ...     pass
    """

    def composite_extractor(headers: dict[str, str]) -> SpanContext | None:
        for extractor in extractors:
            try:
                result = extractor(headers)
                if result is not None:
                    return result
            except Exception:
                continue
        return None

    return composite_extractor


# Default multi-format extractor
default_multi_format_extractor = create_multi_format_extractor([
    datadog_context_extractor,
    b3_context_extractor,
    xray_context_extractor,
    jaeger_context_extractor,
])
"""Pre-built extractor that handles Datadog, B3, X-Ray, and Jaeger formats.

Example:
    >>> @trace_consumer(
    ...     system="kafka",
    ...     destination="events",
    ...     custom_context_extractor=default_multi_format_extractor,
    ... )
    ... async def process_message(ctx, msg):
    ...     # Automatically handles multiple trace header formats
    ...     pass
"""
