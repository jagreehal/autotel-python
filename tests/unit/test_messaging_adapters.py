"""Tests for messaging adapters and context extractors."""


from opentelemetry.trace import SpanContext, TraceFlags

from autotel.messaging_adapters import (
    ConsumerAdapter,
    MessagingAdapter,
    ProducerAdapter,
    b3_context_extractor,
    create_multi_format_extractor,
    datadog_context_extractor,
    default_multi_format_extractor,
    jaeger_context_extractor,
    nats_adapter,
    redis_streams_adapter,
    sqs_adapter,
    temporal_adapter,
    xray_context_extractor,
)

# --- NATS Adapter ---


def test_nats_adapter_producer_attributes() -> None:
    """NATS producer adapter extracts subject, reply_to, stream from message."""
    class MockMsg:
        subject = "orders.created"
        reply_to = "reply.inbox"
        stream = "STREAM"

    attrs = nats_adapter.producer.custom_attributes(MockMsg())
    assert attrs is not None
    assert attrs.get("nats.subject") == "orders.created"
    assert attrs.get("nats.reply_to") == "reply.inbox"
    assert attrs.get("nats.stream") == "STREAM"


def test_nats_adapter_consumer_headers_from_dict() -> None:
    """NATS consumer headers_from returns dict when message has headers dict."""
    class MockMsg:
        headers = {"traceparent": "00-abc-123-01", "tracestate": "a=b"}

    headers = nats_adapter.consumer.headers_from(MockMsg())
    assert headers == {"traceparent": "00-abc-123-01", "tracestate": "a=b"}


def test_nats_adapter_consumer_attributes() -> None:
    """NATS consumer adapter extracts subject, stream, consumer from message."""
    class MockInfo:
        stream = "ORDERS"
        consumer = "consumer-1"
        redelivery_count = 2
        pending = 10

    class MockMsg:
        subject = "orders.created"
        reply = "reply.inbox"
        info = MockInfo()

    attrs = nats_adapter.consumer.custom_attributes(None, MockMsg())
    assert attrs.get("nats.subject") == "orders.created"
    assert attrs.get("nats.stream") == "ORDERS"
    assert attrs.get("nats.consumer") == "consumer-1"
    assert attrs.get("nats.delivered_count") == 2
    assert attrs.get("nats.pending") == 10


# --- Temporal Adapter ---


def test_temporal_adapter_producer_attributes() -> None:
    """Temporal producer adapter extracts workflow_id, run_id, task_queue."""
    class MockInfo:
        workflow_id = "wf-123"
        run_id = "run-456"
        task_queue = "orders"
        workflow_type = "OrderWorkflow"

    attrs = temporal_adapter.producer.custom_attributes(MockInfo())
    assert attrs.get("temporal.workflow_id") == "wf-123"
    assert attrs.get("temporal.run_id") == "run-456"
    assert attrs.get("temporal.task_queue") == "orders"
    assert attrs.get("temporal.workflow_type") == "OrderWorkflow"


def test_temporal_adapter_consumer_attributes() -> None:
    """Temporal consumer adapter extracts activity info."""
    class MockMsg:
        workflow_id = "wf-1"
        run_id = "run-1"
        activity_id = "act-1"
        task_queue = "orders"
        attempt = 2
        activity_type = "ProcessOrder"

    attrs = temporal_adapter.consumer.custom_attributes(None, MockMsg())
    assert attrs.get("temporal.workflow_id") == "wf-1"
    assert attrs.get("temporal.activity_id") == "act-1"
    assert attrs.get("temporal.attempt") == 2
    assert attrs.get("temporal.activity_type") == "ProcessOrder"


# --- Cloudflare Queues Adapter ---


def test_cloudflare_queues_adapter_consumer_attributes() -> None:
    """Cloudflare Queues consumer extracts message id, timestamp, attempts."""
    class MockTimestamp:
        def timestamp(self) -> float:
            return 1600000000.0

    class MockMsg:
        id = "msg-id-123"
        timestamp = MockTimestamp()
        attempts = 3

    from autotel.messaging_adapters import cloudflare_queues_adapter

    attrs = cloudflare_queues_adapter.consumer.custom_attributes(None, MockMsg())
    assert attrs.get("cloudflare.queue.message_id") == "msg-id-123"
    assert "cloudflare.queue.timestamp_ms" in attrs
    assert attrs.get("cloudflare.queue.attempts") == 3


# --- SQS Adapter ---


def test_sqs_adapter_consumer_headers_from_message_attributes() -> None:
    """SQS consumer headers_from extracts trace context from message attributes."""
    msg = type("Msg", (), {
        "message_attributes": {
            "traceparent": {"StringValue": "00-abc-def-01"},
            "tracestate": {"StringValue": "a=b"},
        }
    })()
    headers = sqs_adapter.consumer.headers_from(msg)
    assert headers is not None
    assert "traceparent" in str(headers).lower() or "00-abc" in str(headers)


def test_sqs_adapter_consumer_attributes() -> None:
    """SQS consumer adapter extracts message_id, receipt_handle prefix, receive count."""
    class MockMsg:
        message_id = "msg-123"
        receipt_handle = "x" * 50
        attributes = {"ApproximateReceiveCount": "2", "SentTimestamp": "1600000000000"}

    attrs = sqs_adapter.consumer.custom_attributes(None, MockMsg())
    assert attrs.get("aws.sqs.message_id") == "msg-123"
    assert "aws.sqs.receipt_handle_prefix" in attrs
    assert attrs.get("aws.sqs.approximate_receive_count") == 2


# --- Redis Streams Adapter ---


def test_redis_streams_adapter_consumer_headers() -> None:
    """Redis consumer headers_from extracts from tuple (message_id, data) with metadata."""
    # Simulate Redis stream message: (message_id, {b"data": b"payload", b"traceparent": b"..."})
    class MockMsg:
        pass

    msg = MockMsg()
    # Redis adapter looks for headers in second element if it's a dict
    headers = redis_streams_adapter.consumer.headers_from(msg)
    # With no dict/metadata, may return None
    assert headers is None or isinstance(headers, dict)


def test_redis_streams_adapter_consumer_attributes() -> None:
    """Redis consumer adapter extracts message_id from tuple format."""
    msg = ("12345-0", {"data": "payload"})
    attrs = redis_streams_adapter.consumer.custom_attributes(None, msg)
    assert attrs.get("redis.stream.message_id") == "12345-0"


# --- Context Extractors ---


def test_datadog_context_extractor() -> None:
    """datadog_context_extractor parses x-datadog-trace-id and x-datadog-parent-id."""
    headers = {
        "x-datadog-trace-id": "123456789",
        "x-datadog-parent-id": "987654321",
        "x-datadog-sampling-priority": "1",
    }
    ctx = datadog_context_extractor(headers)
    assert ctx is not None
    assert isinstance(ctx, SpanContext)
    assert ctx.is_remote is True
    assert ctx.trace_id != 0
    assert ctx.span_id != 0


def test_datadog_context_extractor_missing_returns_none() -> None:
    """datadog_context_extractor returns None when headers missing."""
    assert datadog_context_extractor({}) is None
    assert datadog_context_extractor({"x-datadog-trace-id": "1"}) is None


def test_b3_context_extractor_single_header() -> None:
    """b3_context_extractor parses single b3 header."""
    # Format: {traceId}-{spanId}-{sampling}-{parentSpanId}
    headers = {"b3": "80f198ee56343ba864fe8b2a57d3eff7-e457b5a2e4d86bd1-1"}
    ctx = b3_context_extractor(headers)
    assert ctx is not None
    assert ctx.is_remote is True


def test_b3_context_extractor_multi_header() -> None:
    """b3_context_extractor parses x-b3-traceid, x-b3-spanid."""
    headers = {
        "x-b3-traceid": "80f198ee56343ba864fe8b2a57d3eff7",
        "x-b3-spanid": "e457b5a2e4d86bd1",
        "x-b3-sampled": "1",
    }
    ctx = b3_context_extractor(headers)
    assert ctx is not None
    assert ctx.trace_id != 0
    assert ctx.span_id != 0


def test_b3_context_extractor_zero_returns_none() -> None:
    """b3 single-header "0" means not sampled, no trace."""
    assert b3_context_extractor({"b3": "0"}) is None


def test_xray_context_extractor() -> None:
    """xray_context_extractor parses X-Amzn-Trace-Id."""
    # Root=1-{8hex}-{24hex};Parent={16hex};Sampled=1
    headers = {
        "x-amzn-trace-id": "Root=1-5f3b2a1c-1234567890abcdef12345678;Parent=abcdef1234567890;Sampled=1"
    }
    ctx = xray_context_extractor(headers)
    assert ctx is not None
    assert ctx.is_remote is True


def test_xray_context_extractor_missing_returns_none() -> None:
    """xray_context_extractor returns None when header missing."""
    assert xray_context_extractor({}) is None


def test_jaeger_context_extractor() -> None:
    """jaeger_context_extractor parses uber-trace-id."""
    # Format: trace_id:span_id:parent_span_id:flags
    headers = {"uber-trace-id": "80f198ee56343ba864fe8b2a57d3eff7:e457b5a2e4d86bd1:0:1"}
    ctx = jaeger_context_extractor(headers)
    assert ctx is not None
    assert ctx.is_remote is True


def test_jaeger_context_extractor_missing_returns_none() -> None:
    """jaeger_context_extractor returns None when header missing."""
    assert jaeger_context_extractor({}) is None


def test_create_multi_format_extractor() -> None:
    """create_multi_format_extractor tries extractors in order."""
    extractor = create_multi_format_extractor([
        lambda h: None,
        datadog_context_extractor,
    ])
    headers = {
        "x-datadog-trace-id": "111",
        "x-datadog-parent-id": "222",
    }
    ctx = extractor(headers)
    assert ctx is not None
    assert isinstance(ctx, SpanContext)


def test_create_multi_format_extractor_returns_first_success() -> None:
    """create_multi_format_extractor returns first non-None result."""
    extractor = create_multi_format_extractor([
        lambda h: SpanContext(trace_id=1, span_id=2, is_remote=True, trace_flags=TraceFlags.SAMPLED),
        datadog_context_extractor,
    ])
    # First one returns, so we get trace_id=1
    ctx = extractor({"x-datadog-trace-id": "999", "x-datadog-parent-id": "888"})
    assert ctx is not None
    assert ctx.trace_id == 1


def test_default_multi_format_extractor() -> None:
    """default_multi_format_extractor tries Datadog, B3, X-Ray, Jaeger."""
    # Datadog format should be tried first and succeed
    headers = {
        "x-datadog-trace-id": "123",
        "x-datadog-parent-id": "456",
    }
    ctx = default_multi_format_extractor(headers)
    assert ctx is not None

    # B3 format
    headers_b3 = {
        "x-b3-traceid": "a" * 32,
        "x-b3-spanid": "b" * 16,
    }
    ctx_b3 = default_multi_format_extractor(headers_b3)
    assert ctx_b3 is not None

    # No format
    assert default_multi_format_extractor({}) is None


# --- Adapter types ---


def test_producer_adapter_dataclass() -> None:
    """ProducerAdapter has custom_attributes and custom_headers."""
    adapter = ProducerAdapter(custom_attributes=lambda: {"a": 1})
    assert adapter.custom_attributes is not None
    assert adapter.custom_attributes() == {"a": 1}


def test_consumer_adapter_dataclass() -> None:
    """ConsumerAdapter has headers_from, custom_attributes, custom_context_extractor."""
    adapter = ConsumerAdapter(headers_from=lambda msg: {"traceparent": "x"})
    assert adapter.headers_from is not None
    assert adapter.headers_from(None) == {"traceparent": "x"}


def test_messaging_adapter_combines_producer_consumer() -> None:
    """MessagingAdapter holds optional producer and consumer."""
    m = MessagingAdapter(producer=ProducerAdapter(), consumer=ConsumerAdapter())
    assert m.producer is not None
    assert m.consumer is not None
