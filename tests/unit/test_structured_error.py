"""Tests for structured error helpers."""

from typing import Any

import pytest
from opentelemetry.trace import StatusCode

from autotel import (
    create_structured_error,
    get_structured_error_attributes,
    init,
    parse_error,
    record_structured_error,
    span,
    structured_error_to_json,
)
from autotel.exporters import InMemorySpanExporter
from autotel.processors import SimpleSpanProcessor


@pytest.fixture
def exporter() -> Any:
    exp = InMemorySpanExporter()
    init(service="test-structured-error", span_processor=SimpleSpanProcessor(exp))
    return exp


def test_create_structured_error_formats_guidance() -> None:
    cause = ValueError("bad input")
    error = create_structured_error(
        "Payment failed",
        why="The payment gateway rejected the request",
        fix="Ask the user for a different card",
        link="https://example.com/runbook",
        code="PAYMENT_DECLINED",
        status=402,
        details={"gateway": {"name": "stripe"}},
        internal={"raw_response": {"secret": True}},
        cause=cause,
    )

    rendered = str(error)

    assert "StructuredError: Payment failed" in rendered
    assert "Why: The payment gateway rejected the request" in rendered
    assert "Fix: Ask the user for a different card" in rendered
    assert error.internal == {"raw_response": {"secret": True}}


def test_structured_error_to_json_omits_internal_context() -> None:
    error = create_structured_error(
        "Payment failed",
        why="Card declined",
        fix="Retry with a new card",
        code="PAYMENT_DECLINED",
        status=402,
        details={"processor": "stripe"},
        internal={"api_key": "secret"},
    )

    payload = structured_error_to_json(error)

    assert payload == {
        "name": "StructuredError",
        "message": "Payment failed",
        "status": 402,
        "data": {"why": "Card declined", "fix": "Retry with a new card"},
        "code": "PAYMENT_DECLINED",
        "details": {"processor": "stripe"},
    }


def test_get_structured_error_attributes_flattens_details() -> None:
    error = create_structured_error(
        "Payment failed",
        why="Card declined",
        fix="Retry with a new card",
        code="PAYMENT_DECLINED",
        status=402,
        details={"gateway": {"name": "stripe"}},
    )

    attrs = get_structured_error_attributes(error)

    assert attrs["error.type"] == "StructuredError"
    assert attrs["error.message"] == "Payment failed"
    assert attrs["error.why"] == "Card declined"
    assert attrs["error.fix"] == "Retry with a new card"
    assert attrs["error.code"] == "PAYMENT_DECLINED"
    assert attrs["error.status"] == 402
    assert attrs["error.details.gateway.name"] == "stripe"


def test_record_structured_error_marks_span_error(exporter: Any) -> None:
    error = create_structured_error("Payment failed", code="PAYMENT_DECLINED")

    with span("payment.charge") as ctx:
        record_structured_error(ctx, error)

    spans = exporter.get_finished_spans()
    assert len(spans) == 1
    assert spans[0].status.status_code == StatusCode.ERROR
    assert spans[0].attributes is not None
    assert spans[0].attributes["error.code"] == "PAYMENT_DECLINED"


def test_parse_error_handles_structured_error_and_mapping() -> None:
    structured = create_structured_error(
        "Payment failed",
        why="Card declined",
        code="PAYMENT_DECLINED",
        status=402,
    )
    parsed_structured = parse_error(structured)

    assert parsed_structured.message == "Payment failed"
    assert parsed_structured.status == 402
    assert parsed_structured.why == "Card declined"
    assert parsed_structured.code == "PAYMENT_DECLINED"

    parsed_mapping = parse_error({
        "message": "fallback",
        "data": {
            "status": "409",
            "message": "Conflict",
            "fix": "Retry later",
            "details": {"id": "order_123"},
        },
    })

    assert parsed_mapping.message == "Conflict"
    assert parsed_mapping.status == 409
    assert parsed_mapping.fix == "Retry later"
    assert parsed_mapping.details == {"id": "order_123"}
