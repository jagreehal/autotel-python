"""HTTP instrumentation helpers for autotel."""

import functools
import inspect
import time
from collections.abc import Callable
from contextlib import contextmanager
from typing import Any
from urllib.parse import urlparse

from opentelemetry import trace
from opentelemetry.propagate import inject
from opentelemetry.trace import StatusCode

from .context import TraceContext


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


def _wrap_http_method(
    method: Callable[..., Any], http_method: str, slow_threshold_ms: int
) -> Callable[..., Any]:
    """Wrap HTTP method with automatic tracing."""

    @functools.wraps(method)
    async def async_wrapper(self: Any, *args: Any, **kwargs: Any) -> Any:
        tracer = trace.get_tracer(__name__)

        # Infer URL from args (heuristic: first string arg with http)
        url = None
        for arg in args:
            if isinstance(arg, str) and ("http://" in arg or "https://" in arg):
                url = arg
                break

        # Also check kwargs
        if not url:
            for value in kwargs.values():
                if isinstance(value, str) and ("http://" in value or "https://" in value):
                    url = value
                    break

        span_name = f"{http_method} {_extract_path(url) if url else method.__name__}"

        with tracer.start_as_current_span(span_name) as span:
            span.set_attribute("http.method", http_method)
            if url:
                span.set_attribute("http.url", url)

            start = time.time()
            try:
                result = await method(self, *args, **kwargs)

                # Extract status code from result
                status_code = _extract_status_code(result)
                if status_code:
                    span.set_attribute("http.status_code", status_code)

                duration_ms = (time.time() - start) * 1000
                span.set_attribute("http.duration_ms", duration_ms)
                if duration_ms > slow_threshold_ms:
                    span.set_attribute("http.slow_request", True)

                return result
            except Exception as e:
                span.record_exception(e)
                span.set_status(StatusCode.ERROR, str(e))
                raise

    @functools.wraps(method)
    def sync_wrapper(self: Any, *args: Any, **kwargs: Any) -> Any:
        tracer = trace.get_tracer(__name__)

        # Infer URL from args
        url = None
        for arg in args:
            if isinstance(arg, str) and ("http://" in arg or "https://" in arg):
                url = arg
                break

        if not url:
            for value in kwargs.values():
                if isinstance(value, str) and ("http://" in value or "https://" in value):
                    url = value
                    break

        span_name = f"{http_method} {_extract_path(url) if url else method.__name__}"

        with tracer.start_as_current_span(span_name) as span:
            span.set_attribute("http.method", http_method)
            if url:
                span.set_attribute("http.url", url)

            start = time.time()
            try:
                result = method(self, *args, **kwargs)

                # Extract status code
                status_code = _extract_status_code(result)
                if status_code:
                    span.set_attribute("http.status_code", status_code)

                duration_ms = (time.time() - start) * 1000
                span.set_attribute("http.duration_ms", duration_ms)
                if duration_ms > slow_threshold_ms:
                    span.set_attribute("http.slow_request", True)

                return result
            except Exception as e:
                span.record_exception(e)
                span.set_status(StatusCode.ERROR, str(e))
                raise

    # Return appropriate wrapper based on whether method is async
    if inspect.iscoroutinefunction(method):
        return async_wrapper
    return sync_wrapper


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
def trace_http_request(method: str, url: str) -> Any:
    """
    Manual HTTP request tracing.

    Example:
        >>> with trace_http_request("GET", "https://api.example.com/users") as ctx:
        ...     headers = inject_trace_context()
        ...     response = httpx.get(url, headers=headers)
        ...     ctx.set_attribute("http.status_code", response.status_code)
    """
    tracer = trace.get_tracer(__name__)
    span_name = f"{method} {_extract_path(url)}"

    with tracer.start_as_current_span(span_name) as span:
        span.set_attribute("http.method", method)
        span.set_attribute("http.url", url)
        yield TraceContext(span)


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


def _extract_path(url: str) -> str:
    """Extract path from URL for span name."""
    parsed = urlparse(url)
    return parsed.path or "/"
