"""Django integration for autotel."""

import time
from collections.abc import Callable
from typing import Any

try:
    from django.conf import settings
    from django.utils.deprecation import MiddlewareMixin
except ImportError:
    settings = None
    MiddlewareMixin = object

from ..init import init

_INITIALIZED = False


class autotelMiddleware(MiddlewareMixin):  # type: ignore[misc]
    """
    Django middleware that automatically traces all HTTP requests.

    Add to settings.py:
        MIDDLEWARE = [
            'autotel.integrations.django.autotelMiddleware',
            # ... other middleware
        ]

        autotel = {
            'SERVICE_NAME': 'my-django-app',
            'ENDPOINT': 'http://localhost:4318',  # Optional
        }
    """

    def __init__(self, get_response: Callable[..., Any]) -> None:
        """Initialize middleware."""
        super().__init__(get_response)
        self._ensure_initialized()

    def _ensure_initialized(self) -> None:
        """Ensure autotel is initialized."""
        global _INITIALIZED
        if not _INITIALIZED:
            autotel_config = getattr(settings, "autotel", {})
            service_name = autotel_config.get("SERVICE_NAME", "django-app")
            endpoint = autotel_config.get("ENDPOINT", "http://localhost:4318")

            init(service=service_name, endpoint=endpoint)
            _INITIALIZED = True

    def process_request(self, request: Any) -> None:
        """Process incoming request - start trace span."""
        from opentelemetry import trace

        tracer = trace.get_tracer(__name__)
        method = request.method
        path = request.path
        span_name = f"{method} {path}"

        span = tracer.start_span(span_name)
        span.set_attribute("http.method", method)
        span.set_attribute("http.url", request.build_absolute_uri())
        span.set_attribute("http.route", path)
        span.set_attribute("http.scheme", request.scheme)
        span.set_attribute("http.host", request.get_host())

        # Store span and start time in request for later use
        request._autotel_span = span
        request._autotel_start_time = time.time()

        return None

    def process_response(self, request: Any, response: Any) -> Any:
        """Process outgoing response - end trace span."""
        if hasattr(request, "_autotel_span"):
            span = request._autotel_span
            start_time = getattr(request, "_autotel_start_time", time.time())

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

    def process_exception(self, request: Any, exception: Any) -> None:
        """Process exception - record in span."""
        from opentelemetry import trace

        if hasattr(request, "_autotel_span"):
            span = request._autotel_span
            span.record_exception(exception)
            span.set_status(trace.StatusCode.ERROR, str(exception))
            span.end()

        return None
