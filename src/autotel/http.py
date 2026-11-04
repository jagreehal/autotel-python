"""HTTP instrumentation helpers for autotel."""

import functools
import inspect
import time
from collections.abc import Callable, Iterator, Mapping
from contextlib import contextmanager
from typing import Any
from urllib.parse import urlparse

from opentelemetry import context as otel_context
from opentelemetry import trace
from opentelemetry.propagate import extract, inject
from opentelemetry.trace import SpanKind, StatusCode

from .context import TraceContext
from .instruments import measured, record_http_client
from .redaction import redact_url


def http_instrumented(*, slow_threshold_ms: int = 1000) -> Callable[[type], type]:
    """
    Class decorator that auto-instruments HTTP methods.

    Detects HTTP method from function name patterns:
    - get_* -> GET
    - post_* -> POST
    - put_* -> PUT
    - delete_* -> DELETE
    - patch_* -> PATCH

    Example:
        >>> @http_instrumented(slow_threshold_ms=1000)
        >>> class ApiClient:
        ...     async def get_user(self, user_id: str):
        ...         res = await httpx.get(f'https://api.example.com/users/{user_id}')
        ...         return res.json()
    """

    def decorator(cls: type) -> type:
        for name, method in inspect.getmembers(cls, predicate=inspect.isfunction):
            # Detect HTTP method from function name
            http_method = None
            for prefix in ["get", "post", "put", "delete", "patch"]:
                if name.startswith(prefix + "_"):
                    http_method = prefix.upper()
                    break

            if http_method:
                # Wrap with tracing
                setattr(cls, name, _wrap_http_method(method, http_method, slow_threshold_ms))

        return cls

    return decorator


def _record_response(span: Any, labels: dict[str, Any], status_code: int | None) -> None:
    """Set the status on the span and metric. 4xx and 5xx are client-side failures."""
    if not status_code:
        return
    span.set_attribute("http.response.status_code", status_code)
    labels["http.response.status_code"] = status_code
    if status_code >= 400:
        span.set_attribute("error.type", str(status_code))
        labels["error.type"] = str(status_code)
        span.set_status(StatusCode.ERROR)


def _url_labels(url: str | None) -> dict[str, Any]:
    """Bounded labels for the client metric: never the path."""
    if not url:
        return {}
    parsed = urlparse(url)
    return {"server.address": parsed.hostname, "server.port": parsed.port}


@contextmanager
def _client_span(
    method: str, url: str | None, slow_threshold_ms: int | None = None
) -> Iterator[tuple[Any, dict[str, Any]]]:
    """CLIENT span named for the method, as paths would mint one name per URL."""
    tracer = trace.get_tracer(__name__)
    labels = {"http.request.method": method, **_url_labels(url)}
    with (
        tracer.start_as_current_span(method, kind=SpanKind.CLIENT) as span,
        measured(record_http_client, labels) as recorded,
    ):
        span.set_attribute("http.request.method", method)
        if url:
            span.set_attribute("url.full", redact_url(url))
        start = time.perf_counter()
        try:
            yield span, recorded
        except Exception as e:
            span.set_attribute("error.type", type(e).__qualname__)
            span.record_exception(e)
            span.set_status(StatusCode.ERROR, str(e))
            raise
        finally:
            elapsed_ms = (time.perf_counter() - start) * 1000
            if slow_threshold_ms is not None and elapsed_ms > slow_threshold_ms:
                span.set_attribute("http.slow_request", True)


def _find_url(args: tuple[Any, ...], kwargs: dict[str, Any]) -> str | None:
    """First http(s) URL among the arguments (heuristic)."""
    for value in (*args, *kwargs.values()):
        if isinstance(value, str) and ("http://" in value or "https://" in value):
            return value
    return None


def _wrap_http_method(
    method: Callable[..., Any], http_method: str, slow_threshold_ms: int
) -> Callable[..., Any]:
    """Wrap HTTP method with automatic tracing."""

    @functools.wraps(method)
    async def async_wrapper(self: Any, *args: Any, **kwargs: Any) -> Any:
        url = _find_url(args, kwargs)
        with _client_span(http_method, url, slow_threshold_ms) as (span, labels):
            result = await method(self, *args, **kwargs)
            _record_response(span, labels, _extract_status_code(result))
            return result

    @functools.wraps(method)
    def sync_wrapper(self: Any, *args: Any, **kwargs: Any) -> Any:
        url = _find_url(args, kwargs)
        with _client_span(http_method, url, slow_threshold_ms) as (span, labels):
            result = method(self, *args, **kwargs)
            _record_response(span, labels, _extract_status_code(result))
            return result

    return async_wrapper if inspect.iscoroutinefunction(method) else sync_wrapper


def _extract_status_code(result: Any) -> int | None:
    """Extract HTTP status code from response object."""
    if hasattr(result, "status_code"):
        status_code: int = result.status_code
        return status_code
    if hasattr(result, "status"):
        status: int = result.status
        return status
    return None


@contextmanager
def trace_http_request(method: str, url: str) -> Iterator[TraceContext]:
    """
    Manual HTTP request tracing.

    Example:
        >>> with trace_http_request("GET", "https://api.example.com/users") as ctx:
        ...     headers = inject_trace_context()
        ...     response = httpx.get(url, headers=headers)
        ...     ctx.set_attribute("http.response.status_code", response.status_code)
    """
    with _client_span(method, url) as (span, _):
        yield TraceContext(span)


async def fetch(method: str, url: str, **kwargs: Any) -> Any:
    """
    Send an HTTP request in a CLIENT span with W3C context injected (needs httpx).

    Records the status, ``error.type`` on 4xx/5xx or transport failure, and
    ``http.client.request.duration``. ``kwargs`` go to ``httpx.AsyncClient.request``.

    Example:
        >>> response = await fetch("GET", "https://api.example.com/users")
    """
    try:
        import httpx
    except ImportError as e:
        raise ImportError("fetch() needs httpx: pip install 'autotel[events]'") from e

    with _client_span(method, url) as (span, labels):
        headers = {**kwargs.pop("headers", {}), **inject_trace_context()}
        async with httpx.AsyncClient() as client:
            response = await client.request(method, url, headers=headers, **kwargs)
        _record_response(span, labels, response.status_code)
        return response


def inject_trace_context() -> dict[str, str]:
    """
    Inject W3C Trace Context headers for distributed tracing.

    Example:
        >>> headers = inject_trace_context()
        >>> response = httpx.get(url, headers=headers)
    """
    headers: dict[str, str] = {}
    inject(headers)
    return headers


def extract_trace_context(headers: Mapping[str, str]) -> otel_context.Context:
    """
    Extract W3C Trace Context (and baggage) from inbound HTTP headers.

    Example:
        >>> from opentelemetry import context
        >>> incoming = extract_trace_context(dict(request.headers))
        >>> token = context.attach(incoming)
        >>> try:
        ...     handle(request)
        ... finally:
        ...     context.detach(token)
    """
    # Propagators look up lowercase keys
    normalised = {str(key).lower(): str(value) for key, value in headers.items()}
    return extract(normalised)
