"""init(redact_pii=True): one switch across spans, logs and events."""

import logging
import time
from typing import Any

import pytest
from opentelemetry import trace
from opentelemetry.sdk._logs import ReadableLogRecord
from opentelemetry.sdk._logs.export import (
    InMemoryLogRecordExporter,
    SimpleLogRecordProcessor,
)

from autotel import InMemorySpanExporter, SimpleSpanProcessor, init, track
from autotel.events import Event
from autotel.redaction import pii_key, redact_query, redact_url, scrub_text, set_redact_pii
from autotel.track import set_event


@pytest.fixture(autouse=True)
def redaction_off_after() -> Any:
    yield
    set_redact_pii(False)


@pytest.mark.parametrize(
    ("text", "expected"),
    [
        ("user jo.b+x@mail.example.com signed up.", "user [REDACTED] signed up."),
        ("paid with 4111-1111-1111-1111 today", "paid with [REDACTED] today"),
        ("card 4111111111111111", "card [REDACTED]"),
        ("login failed password=hunter2 for id 7", "login failed password=[REDACTED] for id 7"),
        (
            '{"password":"hunter2","token": "abc123"}',
            '{"password":"[REDACTED]","token": "[REDACTED]"}',
        ),
        ('password="hunter 2" ok', 'password="[REDACTED]" ok'),
        ('api_token: "abc.def", retry', 'api_token: "[REDACTED]", retry'),
        ("Authorization: Bearer eyJhbGciOi.x.y", "Authorization: Bearer [REDACTED]"),
    ],
)
def test_scrub_text_redacts_pii_shapes(text: str, expected: str) -> None:
    assert scrub_text(text) == expected


@pytest.mark.parametrize(
    "text",
    [
        "order 1234567890123 shipped",  # 13 digits, fails Luhn
        "used 1500 tokens: ok",
        "see https://example.com/a?b=c",
        "@here deploy at 10:30",
        "élan café ✓",
    ],
)
def test_scrub_text_leaves_clean_text_alone(text: str) -> None:
    assert scrub_text(text) == text


@pytest.mark.parametrize("text", ["token=x:" * 50_000, "a" * 400_000, "a.b-c" * 80_000])
def test_scrub_text_stays_linear_on_long_values(text: str) -> None:
    started = time.monotonic()
    scrub_text(text)
    assert time.monotonic() - started < 1


def test_token_counts_are_not_secrets() -> None:
    assert pii_key("api_token")
    assert pii_key("user.email")
    assert pii_key("access_tokens")
    assert not pii_key("gen_ai.usage.input_tokens")
    assert not pii_key("gen_ai.request.max_tokens")


def test_url_helpers() -> None:
    assert redact_query("a=1&secret") == "a=REDACTED&REDACTED"
    assert (
        redact_url("https://user:pw@api.example.com:8443/v1?key=abc#frag")
        == "https://api.example.com:8443/v1?key=REDACTED"
    )
    assert redact_url("http://[::1]:80/x") == "http://[::1]:80/x"


def test_spans_are_scrubbed_at_export() -> None:
    exporter = InMemorySpanExporter()
    init(service="r", span_processor=SimpleSpanProcessor(exporter), redact_pii=True)

    with trace.get_tracer("t").start_as_current_span("signup") as span:
        span.set_attribute("user.email", "jo@example.com")
        span.set_attribute("note", "ask jo@example.com")
        span.set_attribute("gen_ai.usage.input_tokens", 12)
        span.set_attribute("plan", "pro")
        span.set_attribute("url.query", "token=abc")
        span.set_attribute("url.full", "https://u:p@x.io/a?b=c")
        span.set_attribute("client.address", "203.0.113.7")
        span.add_event("charged jo@example.com", {"card_number": "4111111111111111"})
        span.set_status(trace.StatusCode.ERROR, "failed for jo@example.com")

    (finished,) = exporter.get_finished_spans()
    attrs = finished.attributes
    assert attrs["user.email"] == "[REDACTED]"
    assert attrs["note"] == "ask [REDACTED]"
    assert attrs["gen_ai.usage.input_tokens"] == 12
    assert attrs["plan"] == "pro"
    assert attrs["url.query"] == "token=REDACTED"
    assert attrs["url.full"] == "https://x.io/a?b=REDACTED"
    assert "client.address" not in attrs
    assert finished.events[0].name == "charged [REDACTED]"
    assert finished.events[0].attributes["card_number"] == "[REDACTED]"
    assert finished.status.description == "failed for [REDACTED]"


def test_exception_text_is_scrubbed() -> None:
    exporter = InMemorySpanExporter()
    init(service="r", span_processor=SimpleSpanProcessor(exporter), redact_pii=True)

    with (
        pytest.raises(ValueError),
        trace.get_tracer("t").start_as_current_span("work"),
    ):
        raise ValueError("no account for jo@example.com")

    (finished,) = exporter.get_finished_spans()
    assert "jo@example.com" not in str(finished.events[0].attributes)
    assert "jo@example.com" not in str(finished.status.description)


def test_off_by_default() -> None:
    exporter = InMemorySpanExporter()
    init(service="r", span_processor=SimpleSpanProcessor(exporter))

    with trace.get_tracer("t").start_as_current_span("work") as span:
        span.set_attribute("user.email", "jo@example.com")

    assert exporter.get_finished_spans()[0].attributes["user.email"] == "jo@example.com"


def test_logs_are_scrubbed_at_export() -> None:
    exporter = InMemoryLogRecordExporter()
    init(
        service="r",
        span_processor=SimpleSpanProcessor(InMemorySpanExporter()),
        log_record_processors=[SimpleLogRecordProcessor(exporter)],
        redact_pii=True,
    )

    logging.getLogger("app").warning("login for jo@example.com", extra={"password": "hunter2"})

    finished: list[ReadableLogRecord] = list(exporter.get_finished_logs())
    record = finished[-1].log_record
    assert record.body == "login for [REDACTED]"
    assert record.attributes["password"] == "[REDACTED]"


def test_business_events_are_scrubbed() -> None:
    seen: list[dict[str, Any]] = []

    class Capture:
        name = "capture"

        async def send(self, event: str, properties: dict[str, Any]) -> None: ...

        def send_sync(self, event: str, properties: dict[str, Any]) -> None:
            seen.append(properties)

        async def shutdown(self) -> None: ...

    init(service="r", span_processor=SimpleSpanProcessor(InMemorySpanExporter()), redact_pii=True)
    set_event(Event(subscribers=[Capture()]))

    track("signup", {"user": {"email": "jo@example.com"}, "note": "from jo@example.com", "n": 1})

    assert seen[0]["user"]["email"] == "[REDACTED]"
    assert seen[0]["note"] == "from [REDACTED]"
    assert seen[0]["n"] == 1
