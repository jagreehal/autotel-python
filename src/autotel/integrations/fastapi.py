"""ASGI middleware for FastAPI and Starlette."""

from __future__ import annotations

import logging
from collections.abc import Iterable
from typing import Any

from ._server import ServerOptions, ServerSpan, ensure_initialized, request_attributes, traceparent

logger = logging.getLogger(__name__)


def _route(scope: dict[str, Any]) -> str | None:
    route = scope.get("route")
    return getattr(route, "path_format", None) or getattr(route, "path", None)


def _headers(scope: dict[str, Any]) -> dict[str, str]:
    return {name.decode("latin-1"): value.decode("latin-1") for name, value in scope["headers"]}


def _stop_fastapi_exporting(scope: dict[str, Any]) -> None:
    """Leave export to autotel. FastAPI's ``auto_configure`` (on by default in 0.142,
    opt-in by 0.162) reads the same ``OTEL_*`` variables and adds its own exporters to
    our providers, which sends everything twice, and that copy skips ``redact_pii`` and
    tail sampling. It runs on the first lifespan message, after this. The setting has
    no public home on a built app;
    ``test_fastapi_does_not_add_a_second_exporter`` fails if that changes.
    """
    telemetry = getattr(scope.get("app"), "_telemetry", None)
    if isinstance(telemetry, dict):
        telemetry["auto_configure"] = False


class autotelMiddleware:
    """
    Trace every request as a SERVER span joined to the caller's trace.

    FastAPI 0.142+ starts its own SERVER span and duration metric before any
    middleware runs. This middleware then adds to that span, with query values
    hidden, instead of starting a second one. It also turns off FastAPI's own OTLP
    exporters, where a release enables them, which would send everything twice.

    Example:
        >>> from fastapi import FastAPI
        >>> from autotel.integrations.fastapi import autotelMiddleware
        >>>
        >>> app = FastAPI()
        >>> app.add_middleware(autotelMiddleware, service="my-api")

    Args:
        app: ASGI application
        service: Service name. Unused if ``init()`` already ran.
        skip_routes: Exact paths to record nothing for. FastAPI's native span starts
            first, so there pass ``FastAPI(telemetry={"exclude": ...})`` instead.
        trace_response_header: Answer with ``traceparent``
        trust_forwarded_for: Trust ``X-Forwarded-For`` and ``X-Forwarded-Proto``
        raw_query: Record ``url.query`` as sent
        **init_kwargs: Additional arguments passed to ``autotel.init()``
    """

    def __init__(
        self,
        app: Any,
        *,
        service: str | None = None,
        skip_routes: Iterable[str] = (),
        trace_response_header: bool = False,
        trust_forwarded_for: bool = False,
        raw_query: bool = False,
        **init_kwargs: Any,
    ) -> None:
        self.app = app
        self.options = ServerOptions.build(
            skip_routes, trace_response_header, trust_forwarded_for, raw_query
        )
        self._warned_native_skip = False
        ensure_initialized(service, **init_kwargs)

    async def __call__(self, scope: dict[str, Any], receive: Any, send: Any) -> None:
        if scope["type"] == "lifespan":
            _stop_fastapi_exporting(scope)
        # FastAPI marks a request its `exclude` setting skipped with None.
        excluded = "fastapi.telemetry" in scope and scope["fastapi.telemetry"] is None
        if scope["type"] != "http" or excluded:
            await self.app(scope, receive, send)
            return

        native = scope.get("fastapi.telemetry")
        if scope["path"] in self.options.skip_routes:
            if native is not None and not self._warned_native_skip:
                self._warned_native_skip = True
                logger.warning(
                    "skip_routes can't stop FastAPI's own span: "
                    'use FastAPI(telemetry={"exclude": ...}) for %s',
                    scope["path"],
                )
            await self.app(scope, receive, send)
            return

        headers = _headers(scope)
        client = scope.get("client")
        attributes = request_attributes(
            method=scope["method"],
            scheme=scope.get("scheme", "http"),
            path=scope["path"],
            query=scope.get("query_string", b"").decode("latin-1"),
            headers=headers,
            peer=(client[0], client[1]) if client else None,
            http_version=scope.get("http_version"),
            options=self.options,
        )
        native_span = getattr(native, "span", None)
        if native_span is not None:
            native_span.set_attributes(attributes)
            await self._respond(scope, receive, send, native_span, None)
            return

        server_span = ServerSpan(headers, attributes)
        await self._respond(scope, receive, send, server_span.span, server_span)

    async def _respond(
        self, scope: Any, receive: Any, send: Any, span: Any, own: ServerSpan | None
    ) -> None:
        status: int | None = None

        async def send_wrapper(message: dict[str, Any]) -> None:
            nonlocal status
            if message["type"] == "http.response.start":
                status = message["status"]
                if self.options.trace_response_header:
                    header = (b"traceparent", traceparent(span).encode())
                    message = {**message, "headers": [*message.get("headers", ()), header]}
            await send(message)

        try:
            await self.app(scope, receive, send_wrapper)
        except Exception as error:
            if own is not None:
                own.set_error(error)
            raise
        finally:
            if own is not None:
                own.set_route(_route(scope))
                own.finish(status)
