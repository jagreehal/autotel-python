"""Flask integration for autotel."""

from __future__ import annotations

from collections.abc import Iterable
from typing import Any

from flask import Flask, g, request

from ._server import ServerOptions, ServerSpan, ensure_initialized, request_attributes, traceparent


def init_autotel(
    app: Flask,
    *,
    service: str | None = None,
    skip_routes: Iterable[str] = (),
    trace_response_header: bool = False,
    trust_forwarded_for: bool = False,
    raw_query: bool = False,
    **init_kwargs: Any,
) -> None:
    """
    Trace every request as a SERVER span joined to the caller's trace.

    Example:
        >>> from flask import Flask
        >>> from autotel.integrations.flask import init_autotel
        >>>
        >>> app = Flask(__name__)
        >>> init_autotel(app, service="my-flask-app", skip_routes=["/health"])

    Args:
        app: Flask application
        service: Service name. Unused if ``init()`` already ran.
        skip_routes: Exact paths to record nothing for, such as health checks.
        trace_response_header: Answer with ``traceparent``
        trust_forwarded_for: Trust ``X-Forwarded-For`` and ``X-Forwarded-Proto``
        raw_query: Record ``url.query`` as sent
        **init_kwargs: Additional arguments passed to ``autotel.init()``
    """
    options = ServerOptions.build(
        skip_routes, trace_response_header, trust_forwarded_for, raw_query
    )
    ensure_initialized(service, **init_kwargs)

    @app.before_request
    def before_request() -> None:
        if request.path in options.skip_routes:
            return
        headers = {name.lower(): value for name, value in request.headers.items()}
        environ = request.environ
        address = environ.get("REMOTE_ADDR")
        port = environ.get("REMOTE_PORT")
        server_span = ServerSpan(
            headers,
            request_attributes(
                method=request.method,
                scheme=request.scheme,
                path=request.path,
                query=request.query_string.decode("latin-1"),
                headers=headers,
                peer=(address, int(port) if port else None) if address else None,
                http_version=environ.get("SERVER_PROTOCOL", "").removeprefix("HTTP/") or None,
                options=options,
            ),
        )
        if request.url_rule is not None:
            server_span.set_route(request.url_rule.rule)
        g.autotel_span = server_span
        g.autotel_status = None

    @app.after_request
    def after_request(response: Any) -> Any:
        server_span = g.get("autotel_span")
        if server_span is not None:
            g.autotel_status = response.status_code
            if options.trace_response_header:
                response.headers["traceparent"] = traceparent(server_span.span)
        return response

    @app.teardown_request
    def teardown_request(exception: BaseException | None) -> None:
        server_span = g.pop("autotel_span", None)
        if server_span is None:
            return
        if exception is not None:
            server_span.set_error(exception)
        server_span.finish(g.get("autotel_status"))
