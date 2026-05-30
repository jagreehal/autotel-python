"""@trace decorator for automatic span lifecycle."""

import functools
import inspect
from collections.abc import Callable
from typing import Any, ParamSpec, TypeVar, overload

from opentelemetry import trace as otel_trace
from opentelemetry.trace import StatusCode

from .context import TraceContext
from .operation_context import run_in_operation_context

P = ParamSpec("P")
R = TypeVar("R")

# Parameter names that indicate context injection
CTX_PARAM_NAMES = ("ctx", "context", "tracecontext")


def _rewrite_signature_without_ctx(
    wrapper: Callable[..., Any], original_func: Callable[..., Any]
) -> None:
    """Remove the first (injected ctx) param from wrapper's signature.

    Frameworks like FastAPI inspect function signatures for dependency injection.
    Without this fix, the ctx parameter would appear as a required query parameter.

    Only removes the FIRST parameter (the injected TraceContext), preserving any
    other parameters that may happen to be named 'context' etc.
    """
    original_sig = inspect.signature(original_func)
    params = list(original_sig.parameters.values())
    # Only remove the first parameter (the injected ctx)
    new_params = params[1:] if params else []
    wrapper.__signature__ = original_sig.replace(parameters=new_params)  # type: ignore[attr-defined]


@overload
def trace(
    func: Callable[P, R],
    *,
    name: str | None = None,
) -> Callable[P, R]: ...


@overload
def trace(
    func: None = None,
    *,
    name: str | None = None,
) -> Callable[[Callable[P, R]], Callable[P, R]]: ...


def trace(
    func: Callable[P, R] | None = None,
    *,
    name: str | None = None,
) -> Callable[P, R] | Callable[[Callable[P, R]], Callable[P, R]]:
    """
    Decorator that wraps a function with automatic span lifecycle.

    Auto-detects if function expects a 'ctx' parameter (factory pattern).

    Example:
        >>> @trace
        >>> async def get_user(user_id: str):
        ...     return await db.users.find(user_id)

        >>> @trace
        >>> async def create_user(ctx, data: dict[str, Any]):
        ...     ctx.set_attribute('user.email', data['email'])
        ...     return await db.users.create(data)
    """

    def decorator(fn: Callable[P, R]) -> Callable[P, R]:
        # Detect if function expects ctx parameter
        sig = inspect.signature(fn)
        params = list(sig.parameters.keys())
        needs_ctx = len(params) > 0 and params[0] in CTX_PARAM_NAMES

        # Infer span name
        span_name = name or fn.__name__

        if inspect.iscoroutinefunction(fn):

            @functools.wraps(fn)
            async def async_wrapper(*args: P.args, **kwargs: P.kwargs) -> R:
                tracer = otel_trace.get_tracer(__name__)
                # Set operation context for events auto-enrichment
                with (
                    run_in_operation_context(span_name),
                    tracer.start_as_current_span(span_name) as span,
                ):
                    try:
                        if needs_ctx:
                            ctx = TraceContext(span)
                            result = await fn(ctx, *args, **kwargs)  # type: ignore[arg-type]
                            return result  # type: ignore[no-any-return]
                        result = await fn(*args, **kwargs)
                        return result  # type: ignore[no-any-return]
                    except Exception as e:
                        span.record_exception(e)
                        span.set_status(StatusCode.ERROR, str(e))
                        raise

            if needs_ctx:
                _rewrite_signature_without_ctx(async_wrapper, fn)
            return async_wrapper  # type: ignore[return-value]
        else:

            @functools.wraps(fn)
            def sync_wrapper(*args: P.args, **kwargs: P.kwargs) -> R:
                tracer = otel_trace.get_tracer(__name__)
                # Set operation context for events auto-enrichment
                with (
                    run_in_operation_context(span_name),
                    tracer.start_as_current_span(span_name) as span,
                ):
                    try:
                        if needs_ctx:
                            ctx = TraceContext(span)
                            result = fn(ctx, *args, **kwargs)  # type: ignore[arg-type]
                            return result
                        result = fn(*args, **kwargs)
                        return result
                    except Exception as e:
                        span.record_exception(e)
                        span.set_status(StatusCode.ERROR, str(e))
                        raise

            if needs_ctx:
                _rewrite_signature_without_ctx(sync_wrapper, fn)
            return sync_wrapper

    if func is None:
        # Called with arguments: @trace(name="...")
        return decorator
    return decorator(func)
