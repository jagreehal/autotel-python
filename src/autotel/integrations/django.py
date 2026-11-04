"""Django integration for autotel."""

from __future__ import annotations

import os
from collections.abc import Callable
from typing import Any

from asgiref.sync import iscoroutinefunction, markcoroutinefunction
from django.conf import settings

from ._server import ServerOptions, ServerSpan, ensure_initialized, request_attributes, traceparent


def _service_name(config: dict[str, Any]) -> str | None:
    """``django-app`` only when nothing names the service: OTEL_SERVICE_NAME wins over it."""
    return config.get("SERVICE_NAME") or (None if os.getenv("OTEL_SERVICE_NAME") else "django-app")


class autotelMiddleware:
    """
    Trace every request as a SERVER span joined to the caller's trace.

    Add to settings.py:
        MIDDLEWARE = [
            'autotel.integrations.django.autotelMiddleware',
            # ... other middleware
        ]

        AUTOTEL = {
            'SERVICE_NAME': 'my-django-app',
            'ENDPOINT': 'http://localhost:4318',    # Optional
            'SKIP_ROUTES': ['/health'],             # Optional
            'TRACE_RESPONSE_HEADER': False,         # Optional
            'TRUST_FORWARDED_FOR': False,           # Optional
            'RAW_QUERY': False,                     # Optional
            'INIT': {'metrics': True},              # Optional: more init() arguments
        }
    """

    sync_capable = True
    async_capable = True

    def __init__(self, get_response: Callable[..., Any]) -> None:
        self.get_response = get_response
        self._async = iscoroutinefunction(get_response)
        if self._async:
            markcoroutinefunction(self)
        config = getattr(settings, "AUTOTEL", {})
        self.options = ServerOptions.build(
            config.get("SKIP_ROUTES", ()),
            config.get("TRACE_RESPONSE_HEADER", False),
            config.get("TRUST_FORWARDED_FOR", False),
            config.get("RAW_QUERY", False),
        )
        init_kwargs = dict(config.get("INIT", {}))
        if "ENDPOINT" in config:
            init_kwargs["endpoint"] = config["ENDPOINT"]
        ensure_initialized(_service_name(config), **init_kwargs)

    def __call__(self, request: Any) -> Any:
        if self._async:
            return self._acall(request)
        if request.path in self.options.skip_routes:
            return self.get_response(request)
        server_span = self._start(request)
        status = None
        try:
            response = self.get_response(request)
            status = response.status_code
            self._finish_response(server_span, response)
            return response
        except Exception as error:
            server_span.set_error(error)
            raise
        finally:
            self._finish(request, server_span, status)

    async def _acall(self, request: Any) -> Any:
        if request.path in self.options.skip_routes:
            return await self.get_response(request)
        server_span = self._start(request)
        status = None
        try:
            response = await self.get_response(request)
            status = response.status_code
            self._finish_response(server_span, response)
            return response
        except Exception as error:
            server_span.set_error(error)
            raise
        finally:
            self._finish(request, server_span, status)

    def process_exception(self, request: Any, exception: Exception) -> None:
        """Django turns view errors into a 500 before ``get_response`` returns."""
        server_span = getattr(request, "_autotel_span", None)
        if server_span is not None:
            server_span.set_error(exception)

    def _start(self, request: Any) -> ServerSpan:
        headers = {name.lower(): value for name, value in request.headers.items()}
        meta = request.META
        address = meta.get("REMOTE_ADDR")
        port = meta.get("REMOTE_PORT")
        server_span = ServerSpan(
            headers,
            request_attributes(
                method=request.method,
                scheme=request.scheme,
                path=request.path,
                query=meta.get("QUERY_STRING", ""),
                headers=headers,
                peer=(address, int(port) if port else None) if address else None,
                http_version=meta.get("SERVER_PROTOCOL", "").removeprefix("HTTP/") or None,
                options=self.options,
            ),
        )
        request._autotel_span = server_span
        return server_span

    def _finish_response(self, server_span: ServerSpan, response: Any) -> None:
        if self.options.trace_response_header:
            response.headers["traceparent"] = traceparent(server_span.span)

    def _finish(self, request: Any, server_span: ServerSpan, status: int | None) -> None:
        match = getattr(request, "resolver_match", None)
        if match is not None and match.route:
            server_span.set_route(f"/{match.route}")
        server_span.finish(status)
