"""FastAPI integration for autotel."""

import time
from collections.abc import Callable
from typing import Any

from opentelemetry import trace
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import Response

from ..init import init


class autotelMiddleware(BaseHTTPMiddleware):
    """
    FastAPI middleware that automatically traces all HTTP requests.

    Example:
        >>> from fastapi import FastAPI
        >>> from autotel.integrations.fastapi import autotelMiddleware
        >>>
        >>> app = FastAPI()
        >>> app.add_middleware(autotelMiddleware, service="my-api")
    """

    def __init__(
        self,
        app: Any,
        *,
        service: str,
        endpoint: str = "http://localhost:4318",
        **init_kwargs: Any,
    ) -> None:
        """
        Initialize autotel middleware.

        Args:
            app: FastAPI application
            service: Service name for tracing
            endpoint: OTLP endpoint (default: http://localhost:4318)
            **init_kwargs: Additional arguments passed to autotel.init()
        """
        super().__init__(app)
        # Initialize autotel only once to preserve existing span processors in tests/apps
        try:
            import importlib

            init_state = importlib.import_module("autotel.init")
            already_initialized = getattr(init_state, "_INITIALIZED", False)
        except Exception:
            already_initialized = False

        if not already_initialized:
            init(service=service, endpoint=endpoint, **init_kwargs)
        self.service = service

    async def dispatch(self, request: Request, call_next: Callable[..., Any]) -> Response:
        """
        Process request and create trace span.

        Args:
            request: Starlette request
            call_next: Next middleware/handler

        Returns:
            Response
        """
        tracer = trace.get_tracer(__name__)
        method = request.method
        path = request.url.path

        # Prefer template path to reduce cardinality (e.g., /users/{user_id})
        route = request.scope.get("route")
        route_path = getattr(route, "path_format", None) or getattr(route, "path", None) or path
        span_name = f"{method} {route_path}"

        with tracer.start_as_current_span(span_name) as span:
            # Set HTTP attributes
            span.set_attribute("http.method", method)
            span.set_attribute("http.url", str(request.url))
            span.set_attribute("http.route", route_path)
            span.set_attribute("http.scheme", request.url.scheme)
            span.set_attribute("http.host", request.url.hostname or "")
            if request.url.port:
                span.set_attribute("http.port", request.url.port)

            # Record start time
            start_time = time.time()

            try:
                response = await call_next(request)

                # After routing, ensure we use the resolved template if available
                resolved_route = request.scope.get("route")
                resolved_route_path = (
                    getattr(resolved_route, "path_format", None)
                    or getattr(resolved_route, "path", None)
                    or route_path
                )
                if resolved_route_path != route_path:
                    span.update_name(f"{method} {resolved_route_path}")
                    span.set_attribute("http.route", resolved_route_path)

                # Set response attributes
                span.set_attribute("http.status_code", response.status_code)
                span.set_attribute("http.status_text", response.status_code)

                # Calculate duration
                duration_ms = (time.time() - start_time) * 1000
                span.set_attribute("http.duration_ms", duration_ms)

                # Mark slow requests
                if duration_ms > 1000:  # > 1 second
                    span.set_attribute("http.slow_request", True)

                return response  # type: ignore[no-any-return]
            except Exception as e:
                # Record exception
                span.record_exception(e)
                span.set_status(trace.StatusCode.ERROR, str(e))
                raise
