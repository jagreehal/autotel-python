"""HTTP extract_trace_context."""

from opentelemetry import context as otel_context
from opentelemetry import trace as otel_trace

from autotel import init, span
from autotel.exporters import InMemorySpanExporter
from autotel.http import extract_trace_context, inject_trace_context
from autotel.processors import SimpleSpanProcessor


def test_inject_then_extract_continues_trace() -> None:
    exporter = InMemorySpanExporter()
    init(
        service="test-http-extract",
        span_processor=SimpleSpanProcessor(exporter),
        metrics=False,
        logs=False,
    )

    with span("client") as ctx:
        headers = inject_trace_context()
        parent_trace = ctx.trace_id

    incoming = extract_trace_context(headers)
    token = otel_context.attach(incoming)
    try:
        tracer = otel_trace.get_tracer(__name__)
        with tracer.start_as_current_span("server") as server_span:
            server_ctx = server_span.get_span_context()
            assert format(server_ctx.trace_id, "032x") == parent_trace
    finally:
        otel_context.detach(token)
