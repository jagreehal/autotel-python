"""@trace decorator for automatic span lifecycle."""

import functools
import inspect
from collections.abc import Callable
from typing import Any, ParamSpec, TypeVar, overload

from opentelemetry import trace as otel_trace
from opentelemetry.trace import Span, StatusCode

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


def _record_result(span: Span, result: object, fail_on_err: bool) -> None:
    """Record a Result-style return value: anything with an ``is_err()`` method.

    An ``Err`` is returned, not raised, so the span needs it recorded here. The status
    stays unset unless ``fail_on_err``, since an ``Err`` is often an expected outcome.
    """
    # Inspection runs user code (properties, __str__), so read it all before touching the span.
    try:
        is_err = getattr(result, "is_err", None)
        if not callable(is_err):
            return
        failed = is_err()
        if not isinstance(failed, bool):  # e.g. a mock
            return
        if failed:
            error = getattr(result, "error", None)
            if error is None:
                error = getattr(result, "err_value", None)
            error_type = type(error).__name__ if error is not None else "Err"
            message = str(error)
    except Exception:
        return
    span.set_attribute("result.ok", not failed)
    if not failed:
        return
    span.set_attribute("error.type", error_type)
    span.add_event("result.err", {"error.type": error_type, "error.message": message})
    if fail_on_err:
        span.set_status(StatusCode.ERROR, f"{error_type}: {message}")


@overload
def trace(
    func: Callable[P, R],
    *,
    name: str | None = None,
    fail_on_err: bool = False,
) -> Callable[P, R]: ...


@overload
def trace(
    func: str | None = None,
    *,
    name: str | None = None,
    fail_on_err: bool = False,
) -> Callable[[Callable[P, R]], Callable[P, R]]: ...


@overload
def trace(
    func: str,
    wrapped: Callable[P, R],
    *,
    fail_on_err: bool = False,
) -> Callable[P, R]: ...


def trace(
    func: Callable[P, R] | str | None = None,
    wrapped: Callable[P, R] | None = None,
    *,
    name: str | None = None,
    fail_on_err: bool = False,
) -> Callable[P, R] | Callable[[Callable[P, R]], Callable[P, R]]:
    """
    Decorator that wraps a function with automatic span lifecycle.

    Auto-detects if function expects a 'ctx' parameter (factory pattern).

    Example:
        >>> @trace
        >>> async def get_user(user_id: str):
        ...     return await db.users.find(user_id)

        >>> @trace('user.get')  # span name as the first argument
        >>> async def get_user(user_id: str):
        ...     return await db.users.find(user_id)

        >>> # Wrap an existing function
        >>> traced = trace("bank.verify_pin", verify_pin)

        >>> @trace
        >>> async def create_user(ctx, data: dict[str, Any]):
        ...     ctx.set_attribute('user.email', data['email'])
        ...     return await db.users.create(data)

    Result-returning functions (anything with an ``is_err()`` method)
    get ``result.ok`` on the span; an ``Err`` also gets ``error.type`` and a
    ``result.err`` event. The span status stays unset for an ``Err``, since it is
    usually an expected outcome. Pass ``fail_on_err=True`` to mark it as an error:

        >>> @trace(fail_on_err=True)
        >>> def charge(card: Card, total: int) -> Result[Receipt, CardDeclined]:
        ...     ...
    """

    # `trace("span.name", fn)`: wrap immediately.
    if isinstance(func, str) and wrapped is not None:
        name = func
        func = wrapped

    # `@trace('span.name')`: the span name given positionally.
    elif isinstance(func, str):
        name, func = func, None

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
                        else:
                            result = await fn(*args, **kwargs)
                        _record_result(span, result, fail_on_err)
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
                        else:
                            result = fn(*args, **kwargs)
                        _record_result(span, result, fail_on_err)
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
