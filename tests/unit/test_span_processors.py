"""Tests for migration safety span processors."""

from typing import Any

import pytest

from autotel import init, span
from autotel.exporters import InMemorySpanExporter
from autotel.processors import SimpleSpanProcessor
from autotel.span_processors import (
    AttributeRedactingProcessor,
    FilteringSpanProcessor,
    SpanNameNormalizingProcessor,
    create_attribute_redactor,
    normalize_rest_api_span_name,
)


@pytest.fixture
def exporter() -> Any:
    exp = InMemorySpanExporter()
    init(service="test-span-processors", span_processor=SimpleSpanProcessor(exp))
    return exp


def test_normalize_rest_api_span_name_reduces_cardinality() -> None:
    name = "GET /users/123/orders/550e8400-e29b-41d4-a716-446655440000"

    assert normalize_rest_api_span_name(name) == "GET /users/:id/orders/:uuid"


def test_create_attribute_redactor_default_masks_common_pii() -> None:
    redactor = create_attribute_redactor("default")

    assert redactor("password", "super-secret") == "[REDACTED]"
    assert redactor("user.email", "alice@example.com") == "a***@***.com"
    assert redactor("payment.card", "4111 1111 1111 1234") == "****1234"
    assert redactor("tags", ["alice@example.com", "ok"]) == ["a***@***.com", "ok"]


def test_span_name_normalizing_processor_updates_span_name() -> None:
    exporter = InMemorySpanExporter()
    init(
        service="test-normalizer",
        span_processor=SimpleSpanProcessor(exporter),
        span_name_normalizer="rest-api",
    )

    with span("GET /users/123/orders/507f1f77bcf86cd799439011"):
        pass

    spans = exporter.get_finished_spans()
    assert spans[0].name == "GET /users/:id/orders/:objectId"


def test_filtering_span_processor_drops_matching_spans() -> None:
    exporter = InMemorySpanExporter()
    init(
        service="test-filter",
        span_processor=SimpleSpanProcessor(exporter),
        span_filter=lambda finished_span: "/health" not in finished_span.name,
    )

    with span("GET /health"):
        pass
    with span("GET /orders"):
        pass

    spans = exporter.get_finished_spans()
    assert [finished_span.name for finished_span in spans] == ["GET /orders"]


def test_attribute_redacting_processor_redacts_before_export() -> None:
    exporter = InMemorySpanExporter()
    init(
        service="test-redaction",
        span_processor=SimpleSpanProcessor(exporter),
        attribute_redactor="strict",
    )

    with span("POST /login") as ctx:
        ctx.set_attribute("password", "super-secret")
        ctx.set_attribute("http.request.header.authorization", "Bearer abcdef123456789")
        ctx.set_attribute("user.email", "alice@example.com")

    spans = exporter.get_finished_spans()
    attrs = spans[0].attributes
    assert attrs is not None
    assert attrs["password"] == "[REDACTED]"
    assert attrs["http.request.header.authorization"] == "Bearer ***"
    assert attrs["user.email"] == "a***@***.com"


def test_processors_are_composable() -> None:
    exporter = InMemorySpanExporter()
    init(
        service="test-composable-processors",
        span_processor=SimpleSpanProcessor(exporter),
        span_name_normalizer="rest-api",
        span_filter=lambda finished_span: "/health" not in finished_span.name,
        attribute_redactor="default",
    )

    with span("GET /users/123") as ctx:
        ctx.set_attribute("email", "alice@example.com")
    with span("GET /health"):
        pass

    spans = exporter.get_finished_spans()
    assert len(spans) == 1
    assert spans[0].name == "GET /users/:id"
    assert spans[0].attributes is not None
    assert spans[0].attributes["email"] == "a***@***.com"


def test_processor_classes_can_wrap_custom_processors() -> None:
    exporter = InMemorySpanExporter()
    processor = SpanNameNormalizingProcessor(
        FilteringSpanProcessor(
            AttributeRedactingProcessor(SimpleSpanProcessor(exporter), "default"),
            lambda finished_span: "drop" not in finished_span.name,
        ),
        "minimal",
    )
    init(service="test-manual-wrapper", span_processor=processor)

    with span("GET /users/123") as ctx:
        ctx.set_attribute("user.email", "alice@example.com")
    with span("drop /users/456"):
        pass

    spans = exporter.get_finished_spans()
    assert len(spans) == 1
    assert spans[0].name == "GET /users/:id"
    assert spans[0].attributes is not None
    assert spans[0].attributes["user.email"] == "a***@***.com"
