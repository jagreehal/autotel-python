"""executable-stories showcase for autotel.

Exercises autotel's public API and publishes the run as living documentation,
with the spans the run actually recorded attached to each scenario.
"""

from typing import Any

import pytest
from opentelemetry.sdk.trace import ReadableSpan
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.trace import StatusCode

from autotel import init, trace
from autotel.exporters import InMemorySpanExporter

# The plugin's published wheel needs Python 3.12+, while autotel supports 3.10+.
story = pytest.importorskip("executable_stories").story


@pytest.fixture
def exporter() -> Any:
    """An autotel provider that records spans in memory instead of exporting them."""
    exp = InMemorySpanExporter()
    init(service="autotel-stories", span_processor=SimpleSpanProcessor(exp))
    return exp


def _status_of(span: ReadableSpan) -> str:
    if span.status.status_code is StatusCode.ERROR:
        return "error"
    if span.status.status_code is StatusCode.OK:
        return "ok"
    return "unset"


def _serialize(spans: Any) -> list[dict[str, Any]]:
    """Convert recorded spans into the shape the report renders as a waterfall."""
    out: list[dict[str, Any]] = []
    for span in spans:
        entry: dict[str, Any] = {
            "spanId": format(span.context.span_id, "016x"),
            "name": span.name,
            "startTimeMs": span.start_time / 1_000_000,
            "durationMs": (span.end_time - span.start_time) / 1_000_000,
            "status": _status_of(span),
        }
        if span.parent is not None:
            entry["parentSpanId"] = format(span.parent.span_id, "016x")
        if span.status.description:
            entry["statusMessage"] = span.status.description
        out.append(entry)
    return out


@trace
def charge_card(amount: int) -> int:
    if amount <= 0:
        raise ValueError(f"invalid amount: {amount}")
    return amount


@trace
def checkout(amount: int) -> int:
    return charge_card(amount)


def test_traced_function_records_a_span(exporter: Any) -> None:
    story.init(
        "A traced function records a span named after the operation",
        tags=["otel", "tracing"],
    )

    story.given("a payment function wrapped once with the autotel @trace decorator")
    story.code(
        "The whole instrumentation",
        "@trace\ndef charge_card(amount: int) -> int:\n    return amount",
        lang="python",
    )

    story.when("the function is called")
    result = charge_card(2500)

    story.then("it returns its ordinary value, unchanged by instrumentation")
    story.kv("captured", result)
    assert result == 2500

    spans = exporter.get_finished_spans()
    story.and_("a span was recorded without a single line of tracing code")
    story.table(
        "Spans recorded by this scenario",
        ["name", "status"],
        [[span.name, _status_of(span)] for span in spans],
    )
    story.attach_spans(_serialize(spans))

    assert any(span.name.endswith("charge_card") for span in spans)


def test_traced_function_records_failure(exporter: Any) -> None:
    story.init(
        "A traced function that raises records the failure on its span",
        tags=["otel", "tracing"],
    )

    story.given("the same payment function, with no error handling added")

    story.when("it is called with an amount it rejects")
    with pytest.raises(ValueError, match="invalid amount"):
        charge_card(-1)

    spans = exporter.get_finished_spans()
    story.attach_spans(_serialize(spans))
    span = next(s for s in spans if s.name.endswith("charge_card"))

    story.then("the span is marked as failed")
    story.kv("status", _status_of(span))
    story.kv("message", span.status.description)
    assert _status_of(span) == "error"

    story.but("the exception still propagates to the caller unchanged")
    story.note(
        "autotel records the exception on the span and re-raises. It never swallows a failure."
    )


def test_nested_traced_calls_join_one_trace(exporter: Any) -> None:
    story.init("Nested traced calls join a single trace", tags=["otel", "tracing"])

    story.given("a checkout function that calls the payment function")

    story.when("checkout runs")
    checkout(4200)

    spans = exporter.get_finished_spans()
    story.attach_spans(_serialize(spans))
    outer = next(s for s in spans if s.name.endswith("checkout"))
    inner = next(s for s in spans if s.name.endswith("charge_card"))

    story.then("the inner span is a child of the outer one")
    story.mermaid(
        'graph TD\n  C["checkout"] --> P["charge_card"]',
        title="Trace shape recorded by this run",
    )
    assert inner.parent is not None
    assert inner.parent.span_id == outer.context.span_id

    story.and_("the caller wrote no context propagation code at all")
    story.note("Parenting comes from the active OTel context, which @trace manages.")
