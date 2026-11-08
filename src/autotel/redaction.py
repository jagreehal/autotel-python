"""One switch, ``init(redact_pii=True)``, that scrubs spans, logs and events at export."""

from __future__ import annotations

import re
from collections.abc import Mapping
from typing import Any
from urllib.parse import urlsplit, urlunsplit

from opentelemetry.attributes import BoundedAttributes
from opentelemetry.context import Context
from opentelemetry.sdk._logs import LogRecordProcessor, ReadWriteLogRecord
from opentelemetry.sdk.trace import Event, ReadableSpan, Span, SpanProcessor
from opentelemetry.sdk.util import BoundedList
from opentelemetry.trace import Status, StatusCode

REDACTED = "[REDACTED]"
_DROPPED_KEYS = frozenset({"client.address", "network.peer.address", "network.peer.port"})
_KEY_PARTS = ("email", "card", "password", "token")
# The lookbehinds start each match at a token's first character, which keeps these linear.
_EMAIL = re.compile(r"(?<![A-Za-z0-9._%+-])[A-Za-z0-9._%+-]+@[A-Za-z0-9-]+(?:\.[A-Za-z0-9-]+)+")
_CARD = re.compile(r"(?<![A-Za-z0-9])\d(?:[ -]?\d){12,18}(?![A-Za-z0-9])")
_BEARER = re.compile(r"(?i)\bbearer ([^\s,;&\"']+)")
_PAIR = re.compile(
    r"""(?<![\w.-])([\w.-]+)["']?\s*[=:]\s*(?:"([^"]*)"?|'([^']*)'?|([^\s,;&"']*))""",
)

_enabled = False


def set_redact_pii(enabled: bool) -> None:
    """Turn redaction on or off for this process (``init`` calls this)."""
    global _enabled
    _enabled = enabled


def redact_pii_enabled() -> bool:
    return _enabled


def pii_key(key: str) -> bool:
    """True for keys whose value is a secret. Token counts are not."""
    lower = key.lower()
    token_count = (
        lower == "tokens" or lower.startswith("gen_ai.usage.") or lower.endswith("max_tokens")
    )
    return any(part in lower for part in _KEY_PARTS) and not token_count


def _luhn(digits: str) -> bool:
    total = 0
    for position, char in enumerate(reversed(digits)):
        digit = int(char)
        if position % 2:
            digit = digit * 2 - 9 if digit > 4 else digit * 2
        total += digit
    return total % 10 == 0


def scrub_text(text: str) -> str:
    """Redact emails, Luhn-valid cards, bearer tokens and ``password=...`` pairs."""
    hits: list[tuple[int, int]] = [match.span() for match in _EMAIL.finditer(text)]
    for match in _CARD.finditer(text):
        if _luhn(re.sub(r"\D", "", match.group())):
            hits.append(match.span())
    hits.extend(match.span(1) for match in _BEARER.finditer(text))
    for match in _PAIR.finditer(text):
        if pii_key(match.group(1)):
            group = match.lastindex or 2
            if match.end(group) > match.start(group):
                hits.append(match.span(group))
    if not hits:
        return text
    out: list[str] = []
    copied = 0
    for start, end in sorted(hits):
        if end <= copied:
            continue
        if start >= copied:
            out += [text[copied:start], REDACTED]
        copied = end
    out.append(text[copied:])
    return "".join(out)


def redact_query(query: str) -> str:
    """``a=1&secret`` becomes ``a=REDACTED&REDACTED``: keys stay, values go."""
    pairs = (pair.partition("=") for pair in query.split("&") if pair)
    return "&".join(f"{key}=REDACTED" if sep else "REDACTED" for key, sep, _ in pairs)


def redact_url(url: str) -> str:
    """Drop credentials and the fragment, and hide query values."""
    try:
        parts = urlsplit(url)
        host = parts.hostname or ""
        port = parts.port
    except ValueError:
        return "REDACTED"
    if ":" in host:
        host = f"[{host}]"
    netloc = f"{host}:{port}" if port else host
    return urlunsplit((parts.scheme, netloc, parts.path, redact_query(parts.query), ""))


def redact_value(key: str, value: Any) -> Any:
    """Apply key and text rules to one value, descending into mappings and lists."""
    if pii_key(key):
        return REDACTED
    if key == "url.query" and isinstance(value, str):
        return redact_query(value)
    if key == "url.full" and isinstance(value, str):
        return redact_url(value)
    if isinstance(value, str):
        return scrub_text(value)
    if isinstance(value, Mapping):
        return {inner: redact_value(str(inner), item) for inner, item in value.items()}
    if isinstance(value, list | tuple):
        return [redact_value("", item) for item in value]
    return value


def redact_attributes(attributes: Mapping[str, Any] | None) -> dict[str, Any]:
    return {
        key: redact_value(key, value)
        for key, value in (attributes or {}).items()
        if key not in _DROPPED_KEYS
    }


def _redacted_span(span: ReadableSpan) -> ReadableSpan:
    """A scrubbed copy of a finished span."""
    description = span.status.description
    status = (
        Status(StatusCode.ERROR, scrub_text(description))
        if description and span.status.status_code is StatusCode.ERROR
        else span.status
    )
    attributes = BoundedAttributes(attributes=redact_attributes(span.attributes), immutable=True)
    attributes.dropped = span.dropped_attributes
    events = BoundedList.from_seq(
        None,
        [
            Event(scrub_text(event.name), redact_attributes(event.attributes), event.timestamp)
            for event in span.events
        ],
    )
    events.dropped = span.dropped_events
    links = BoundedList.from_seq(None, span.links)
    links.dropped = span.dropped_links
    return ReadableSpan(
        name=span.name,
        context=span.context,
        parent=span.parent,
        resource=span.resource,
        attributes=attributes,
        events=events,
        links=links,
        kind=span.kind,
        status=status,
        start_time=span.start_time,
        end_time=span.end_time,
        instrumentation_scope=span.instrumentation_scope,
    )


class RedactPiiProcessor(SpanProcessor):
    """Scrub spans on the way to ``inner``, whatever API created them."""

    def __init__(self, inner: SpanProcessor) -> None:
        self.inner = inner

    def on_start(self, span: Span, parent_context: Context | None = None) -> None:
        self.inner.on_start(span, parent_context)

    def on_end(self, span: ReadableSpan) -> None:
        self.inner.on_end(_redacted_span(span))

    def shutdown(self) -> None:
        self.inner.shutdown()

    def force_flush(self, timeout_millis: int = 30000) -> bool:
        return self.inner.force_flush(timeout_millis)


class RedactPiiLogProcessor(LogRecordProcessor):
    """Scrub log records in place. Register it before the exporting processor."""

    def on_emit(self, log_record: ReadWriteLogRecord) -> None:
        record = log_record.log_record
        record.body = redact_value("", record.body)
        record.attributes = redact_attributes(record.attributes)

    def shutdown(self) -> None:
        pass

    def force_flush(self, timeout_millis: int = 30000) -> bool:  # noqa: ARG002
        return True
