"""Tail sampling keeps or drops whole traces, never single spans."""

from typing import Any

import pytest
from opentelemetry import baggage, context, trace
from opentelemetry.trace import StatusCode

import autotel.sampling as sampling
from autotel import AdaptiveSampler, InMemorySpanExporter, SimpleSpanProcessor, force_keep, init
from autotel.constants import AUTOTEL_DEBUG_BAGGAGE_KEY
from autotel.context import TraceContext
from autotel.sampling import AdaptiveSamplingProcessor


def setup(sampler: AdaptiveSampler) -> tuple[InMemorySpanExporter, AdaptiveSamplingProcessor]:
    exporter = InMemorySpanExporter()
    init(service="s", span_processor=SimpleSpanProcessor(exporter), sampler=sampler)
    provider: Any = trace.get_tracer_provider()
    processor = provider._active_span_processor._span_processors[0]  # noqa: SLF001
    assert isinstance(processor, AdaptiveSamplingProcessor)
    return exporter, processor


def tracer() -> trace.Tracer:
    return trace.get_tracer("test")


def names(exporter: InMemorySpanExporter) -> set[str]:
    return {span.name for span in exporter.get_finished_spans()}


def test_a_child_error_keeps_the_whole_trace() -> None:
    exporter, _ = setup(AdaptiveSampler.errors_only())

    with tracer().start_as_current_span("root"):
        with tracer().start_as_current_span("ok"):
            pass
        with tracer().start_as_current_span("failed") as span:
            span.set_status(StatusCode.ERROR)

    assert names(exporter) == {"root", "ok", "failed"}


def test_a_clean_trace_leaves_no_orphans() -> None:
    exporter, _ = setup(AdaptiveSampler.errors_only())

    with tracer().start_as_current_span("root"), tracer().start_as_current_span("child"):
        pass

    assert names(exporter) == set()


def test_baseline_agrees_for_every_span_of_a_trace() -> None:
    exporter, _ = setup(AdaptiveSampler(baseline_rate=0.5))

    for _ in range(50):
        with tracer().start_as_current_span("root"), tracer().start_as_current_span("child"):
            pass

    spans = exporter.get_finished_spans()
    per_trace: dict[int, int] = {}
    for span in spans:
        per_trace[span.context.trace_id] = per_trace.get(span.context.trace_id, 0) + 1
    assert per_trace
    assert len(per_trace) < 50
    assert set(per_trace.values()) == {2}


def test_force_keep_from_a_child() -> None:
    exporter, _ = setup(AdaptiveSampler.errors_only())

    with tracer().start_as_current_span("root"), tracer().start_as_current_span("payment"):
        force_keep()

    assert names(exporter) == {"root", "payment"}


def test_trace_context_force_keep() -> None:
    exporter, _ = setup(AdaptiveSampler.errors_only())

    with tracer().start_as_current_span("root") as span:
        TraceContext(span).force_keep()

    assert names(exporter) == {"root"}


@pytest.mark.parametrize(("value", "kept"), [("1", True), ("0", False), ("", False)])
def test_debug_baggage(value: str, kept: bool) -> None:
    exporter, _ = setup(AdaptiveSampler.errors_only())

    token = context.attach(baggage.set_baggage(AUTOTEL_DEBUG_BAGGAGE_KEY, value))
    try:
        with tracer().start_as_current_span("root") as span:
            assert TraceContext(span).debug_requested() is kept
    finally:
        context.detach(token)

    assert bool(names(exporter)) is kept


def test_keep_when_rule() -> None:
    sampler = AdaptiveSampler.errors_only().keep_when("auth.success", lambda value: value is False)
    exporter, _ = setup(sampler)

    with tracer().start_as_current_span("root"), tracer().start_as_current_span("login") as span:
        span.set_attribute("auth.success", False)
    with tracer().start_as_current_span("other") as span:
        span.set_attribute("auth.success", True)

    assert names(exporter) == {"root", "login"}


def test_late_child_follows_the_decided_trace() -> None:
    exporter, _ = setup(AdaptiveSampler.errors_only())

    root = tracer().start_span("root")
    root.set_status(StatusCode.ERROR)
    parent = trace.set_span_in_context(root)
    root.end()
    tracer().start_span("late", context=parent).end()

    assert names(exporter) == {"root", "late"}


def test_trace_with_a_running_child_waits_for_it() -> None:
    exporter, _ = setup(AdaptiveSampler.errors_only())

    root = tracer().start_span("root")
    child = tracer().start_span("child", context=trace.set_span_in_context(root))
    root.end()
    assert names(exporter) == set()
    child.set_status(StatusCode.ERROR)
    child.end()

    assert names(exporter) == {"root", "child"}


def test_shutdown_exports_a_trace_whose_root_never_ended() -> None:
    exporter = InMemorySpanExporter()
    processor = AdaptiveSamplingProcessor(
        AdaptiveSampler.errors_only(), SimpleSpanProcessor(exporter)
    )
    init(service="s", span_processor=processor)

    root = tracer().start_span("root")
    tracer().start_span("child", context=trace.set_span_in_context(root)).end()
    finished = exporter.get_finished_spans()
    processor.shutdown()

    assert finished == ()
    assert [span.name for span in exporter.get_finished_spans()] == ["child"]


def test_buffer_limit_fails_open(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(sampling, "_MAX_BUFFERED_SPANS", 1)
    exporter, _ = setup(AdaptiveSampler.errors_only())

    root = tracer().start_span("root")
    parent = trace.set_span_in_context(root)
    tracer().start_span("a", context=parent).end()
    tracer().start_span("b", context=parent).end()

    assert names(exporter) == {"a", "b"}
