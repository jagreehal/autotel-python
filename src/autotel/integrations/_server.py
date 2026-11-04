"""HTTP SERVER span shared by the framework integrations."""

from __future__ import annotations

import ipaddress
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from time import perf_counter
from typing import Any
from urllib.parse import urlsplit

from opentelemetry import context as otel_context
from opentelemetry import trace
from opentelemetry.trace import Span, SpanKind, StatusCode

from ..http import extract_trace_context
from ..init import init, is_initialized
from ..instruments import record_http_server
from ..redaction import redact_query

_METHODS = frozenset(
    {"CONNECT", "DELETE", "GET", "HEAD", "OPTIONS", "PATCH", "POST", "PUT", "QUERY", "TRACE"}
)


def ensure_initialized(service: str | None, **init_kwargs: Any) -> None:
    """Run ``init`` unless the app already did. ``None`` falls back to OTEL_SERVICE_NAME."""
    if not is_initialized():
        init(service=service, **init_kwargs)


@dataclass(frozen=True)
class ServerOptions:
    """What a server integration records.

    Args:
        skip_routes: Exact paths to record nothing for, such as health checks.
        trace_response_header: Answer with ``traceparent``. Off by default: it tells
            clients your trace ids. Baggage is never echoed.
        trust_forwarded_for: Read ``client.address`` and ``url.scheme`` from
            ``X-Forwarded-For`` and ``X-Forwarded-Proto``. Use only behind a proxy
            that sets them, because any client can send the headers.
        raw_query: Record ``url.query`` as sent rather than hiding its values.
    """

    skip_routes: frozenset[str] = frozenset()
    trace_response_header: bool = False
    trust_forwarded_for: bool = False
    raw_query: bool = False

    @classmethod
    def build(
        cls,
        skip_routes: Iterable[str] = (),
        trace_response_header: bool = False,
        trust_forwarded_for: bool = False,
        raw_query: bool = False,
    ) -> ServerOptions:
        return cls(frozenset(skip_routes), trace_response_header, trust_forwarded_for, raw_query)


def _forwarded_client(value: str | None) -> str | None:
    """First ``X-Forwarded-For`` address, if it is one."""
    try:
        return str(ipaddress.ip_address((value or "").split(",")[0].strip()))
    except ValueError:
        return None


def request_attributes(
    *,
    method: str,
    scheme: str,
    path: str,
    query: str,
    headers: Mapping[str, str],
    peer: tuple[str, int | None] | None,
    http_version: str | None,
    options: ServerOptions,
) -> dict[str, Any]:
    """Semconv attributes for a request. ``headers`` keys are lowercase."""
    if options.trust_forwarded_for and headers.get("x-forwarded-proto") in ("http", "https"):
        scheme = headers["x-forwarded-proto"]
    attributes: dict[str, Any] = {"url.path": path, "url.scheme": scheme}
    if query:
        attributes["url.query"] = query if options.raw_query else redact_query(query)
    if method in _METHODS:
        attributes["http.request.method"] = method
    else:
        attributes["http.request.method"] = "_OTHER"
        attributes["http.request.method_original"] = method
    if http_version:
        attributes["network.protocol.version"] = http_version
    if agent := headers.get("user-agent"):
        attributes["user_agent.original"] = agent

    forwarded = (
        _forwarded_client(headers.get("x-forwarded-for")) if options.trust_forwarded_for else None
    )
    if client := forwarded or (peer[0] if peer else None):
        attributes["client.address"] = client
    if peer:
        attributes["network.peer.address"] = peer[0]
        if peer[1] is not None:
            attributes["network.peer.port"] = peer[1]

    if host := headers.get("host"):
        try:
            authority = urlsplit(f"//{host}")
            if authority.hostname:
                attributes["server.address"] = authority.hostname
            if authority.port:
                attributes["server.port"] = authority.port
        except ValueError:
            pass  # a client wrote a bad Host header; the span is fine without it
    return attributes


def traceparent(span: Span) -> str:
    context = span.get_span_context()
    return f"00-{context.trace_id:032x}-{context.span_id:016x}-{int(context.trace_flags):02x}"


class ServerSpan:
    """One request's SERVER span, joined to the caller's trace and made current."""

    def __init__(self, headers: Mapping[str, str], attributes: dict[str, Any]) -> None:
        self.method = attributes["http.request.method"]
        self._span_method = "HTTP" if self.method == "_OTHER" else self.method
        self._route: str | None = None
        self._error: BaseException | None = None
        self._started = perf_counter()
        parent = extract_trace_context(headers)
        self.span = trace.get_tracer("autotel").start_span(
            self._span_method, context=parent, kind=SpanKind.SERVER, attributes=attributes
        )
        self._token = otel_context.attach(trace.set_span_in_context(self.span, parent))

    def set_route(self, route: str | None) -> None:
        """Name the span for a route template. Never pass a raw path: an unmatched
        request stays unnamed, or scanners would mint a span name per URL."""
        if route:
            self._route = route
            self.span.set_attribute("http.route", route)
            self.span.update_name(f"{self._span_method} {route}")

    def set_error(self, error: BaseException) -> None:
        self._error = error
        self.span.record_exception(error)

    def finish(self, status: int | None) -> None:
        """Record the outcome. Servers fail on 5xx only: a 404 is the client's mistake."""
        labels: dict[str, Any] = {"http.request.method": self.method, "http.route": self._route}
        if status is not None:
            self.span.set_attribute("http.response.status_code", status)
            labels["http.response.status_code"] = status
        error_type = None
        if self._error is not None:
            error_type = type(self._error).__qualname__
        elif status is not None and status >= 500:
            error_type = str(status)
        if error_type is not None:
            self.span.set_attribute("error.type", error_type)
            self.span.set_status(StatusCode.ERROR, str(self._error or ""))
            labels["error.type"] = error_type
        record_http_server(perf_counter() - self._started, labels)
        self.span.end()
        otel_context.detach(self._token)
