"""Flask integration for autotel."""

import time
from typing import Any

from flask import Flask, g, request

from ..init import init

_INITIALIZED = False


def init_autotel(
    app: Flask,
    *,
    service: str,
    endpoint: str = "http://localhost:4318",
    **init_kwargs: Any,
) -> None:
    """
    Initialize autotel for Flask application.

    Example:
        >>> from flask import Flask
        >>> from autotel.integrations.flask import init_autotel
        >>>
        >>> app = Flask(__name__)
        >>> init_autotel(app, service="my-flask-app")

    Args:
        app: Flask application
        service: Service name for tracing
        endpoint: OTLP endpoint (default: http://localhost:4318)
        **init_kwargs: Additional arguments passed to autotel.init()
    """
    global _INITIALIZED
    if not _INITIALIZED:
        init(service=service, endpoint=endpoint, **init_kwargs)
        _INITIALIZED = True

    @app.before_request
    def before_request() -> None:
        """Start trace span before request."""
        from opentelemetry import trace

        tracer = trace.get_tracer(__name__)
        method = request.method
        path = request.path
        span_name = f"{method} {path}"

        span = tracer.start_span(span_name)
        span.set_attribute("http.method", method)
        span.set_attribute("http.url", request.url)
        span.set_attribute("http.route", path)
        span.set_attribute("http.scheme", request.scheme)
        span.set_attribute("http.host", request.host)

        # Store span and start time in Flask g
        g.autotel_span = span
        g.autotel_start_time = time.time()

    @app.after_request
    def after_request(response: Any) -> Any:
        """End trace span after request."""
        if hasattr(g, "autotel_span"):
            span = g.autotel_span
            start_time = g.autotel_start_time

            # Set response attributes
            span.set_attribute("http.status_code", response.status_code)
            span.set_attribute("http.status_text", response.status_code)

            # Calculate duration
            duration_ms = (time.time() - start_time) * 1000
            span.set_attribute("http.duration_ms", duration_ms)

            # Mark slow requests
            if duration_ms > 1000:  # > 1 second
                span.set_attribute("http.slow_request", True)

            # End span
            span.end()

        return response

    @app.teardown_request
    def teardown_request(exception: Any) -> None:
        """Handle exceptions."""
        if exception and hasattr(g, "autotel_span"):
            from opentelemetry import trace

            span = g.autotel_span
            span.record_exception(exception)
            span.set_status(trace.StatusCode.ERROR, str(exception))
            span.end()
