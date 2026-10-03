"""Wide-event request logger."""

from autotel import get_request_logger, init, span
from autotel.exporters import InMemorySpanExporter
from autotel.processors import SimpleSpanProcessor


def test_get_request_logger_sets_flattened_attributes() -> None:
    exporter = InMemorySpanExporter()
    init(
        service="test-request-logger",
        span_processor=SimpleSpanProcessor(exporter),
        metrics=False,
        logs=False,
    )

    with span("atm.withdraw") as ctx:
        log = get_request_logger(ctx)
        log.set({"atm": {"branch": "bridge-street"}, "withdrawal": {"amount_pence": 2000}})
        log.set({"withdrawal": {"result": "dispensed"}})
        snapshot = log.emit_now()

    assert snapshot["context"]["atm"]["branch"] == "bridge-street"
    assert snapshot["context"]["withdrawal"]["result"] == "dispensed"
    finished = exporter.get_finished_spans()
    assert len(finished) == 1
    attrs = finished[0].attributes or {}
    assert attrs["atm.branch"] == "bridge-street"
    assert attrs["withdrawal.amount_pence"] == 2000
    assert attrs["withdrawal.result"] == "dispensed"
