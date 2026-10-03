"""trace('name', fn) wraps a function immediately."""

from autotel import AUTOTEL_DEBUG_BAGGAGE_KEY, init, trace
from autotel.exporters import InMemorySpanExporter
from autotel.processors import SimpleSpanProcessor


def test_trace_name_fn_wraps_callable() -> None:
    exporter = InMemorySpanExporter()
    init(
        service="test-trace-wrap",
        span_processor=SimpleSpanProcessor(exporter),
        metrics=False,
        logs=False,
    )

    def verify_pin(card: str, pin: str) -> bool:
        return pin == "1234"

    wrapped = trace("bank.verify_pin", verify_pin)
    assert wrapped("card", "1234") is True

    spans = exporter.get_finished_spans()
    assert len(spans) == 1
    assert spans[0].name == "bank.verify_pin"


def test_autotel_debug_baggage_key() -> None:
    assert AUTOTEL_DEBUG_BAGGAGE_KEY == "autotel.debug"
