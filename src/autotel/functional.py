"""Functional API for autotel - HOF patterns, batch instrumentation, and context managers."""

import inspect
import re
from collections.abc import Callable
from contextlib import contextmanager
from functools import lru_cache
from typing import Any, ParamSpec, TypeVar

from opentelemetry import context
from opentelemetry import trace as otel_trace
from opentelemetry.trace import StatusCode

from .context import TraceContext
from .operation_context import run_in_operation_context

P = ParamSpec("P")
R = TypeVar("R")


@lru_cache(maxsize=1024)
def _infer_name(func: Callable[..., Any]) -> str:
    """
    Infer trace name from function using multiple strategies:
    1. Function __name__ attribute
    2. Variable assignment (analyze call stack)
    3. Fallback to "unnamed"
    """
    # Strategy 1: Function name
    if hasattr(func, "__name__") and func.__name__ != "<lambda>":
        return func.__name__

    # Strategy 2: Analyze call stack for variable assignment
    try:
        frame = inspect.currentframe()
        if frame and frame.f_back and frame.f_back.f_back:
            caller_frame = frame.f_back.f_back
            code_context = inspect.getframeinfo(caller_frame).code_context
            if code_context:
                source_line = code_context[0].strip()

                # Match patterns like: variable_name = trace(...)
                match = re.match(r"(\w+)\s*=\s*trace\(", source_line)
                if match:
                    return match.group(1)
    except Exception:
        pass  # Graceful degradation

    # Fallback
    return "unnamed"


def instrument(operations: dict[str, Callable[..., Any]]) -> dict[str, Callable[..., Any]]:
    """
    Batch auto-instrumentation for a dictionary of functions.

    Example:
        >>> service = instrument({
        ...     'create': create_user,
        ...     'get': get_user,
        ...     'update': update_user,
        ... })
        >>> user = service['create'](data)
    """
    from .decorators import trace

    return {key: trace(func, name=key) for key, func in operations.items()}


@contextmanager
def span(name: str) -> Any:
    """
    Create a manual span as context manager.

    Example:
        >>> with span("database.query") as ctx:
        ...     ctx.set_attribute("query", "SELECT * FROM users")
        ...     results = db.query(...)
    """
    from .operation_context import run_in_operation_context

    tracer = otel_trace.get_tracer(__name__)
    # Set operation context for events auto-enrichment
    with run_in_operation_context(name), tracer.start_as_current_span(name) as otel_span:
        yield TraceContext(otel_span)


@contextmanager
def with_new_context() -> Any:
    """
    Create a new root trace context (not child of current).

    Useful for background jobs that shouldn't be children of web requests.

    Example:
        >>> def background_worker():
        ...     with with_new_context():
        ...         process_job()  # New root trace
    """
    # Detach from current context by creating a new empty context
    new_ctx = context.Context()
    token = context.attach(new_ctx)
    try:
        yield
    finally:
        context.detach(token)


def trace(
    fn: Callable[..., Any],
    *,
    name: str | None = None,
) -> Any:
    """
    Functional API that supports both factory and immediate execution patterns.

    Factory pattern: Returns a wrapped function
        >>> create_user = trace(lambda ctx: lambda data: create(data))
        >>> user = create_user({"id": "123"})

    Immediate execution: Executes immediately
        >>> result = trace(lambda ctx: create_user({"id": "123"}))

    IMPORTANT: Pattern detection uses inspect.signature() to inspect function
    signatures WITHOUT executing the function. This avoids side effects that could
    occur if the function starts coroutines or performs work during pattern detection
    (similar to the Node.js async function bug where calling async functions for
    pattern detection would cause them to start executing synchronously until the
    first await).

    The fix: We use inspect.signature() to inspect the function signature and
    determine if it's a factory pattern (returns a function) or immediate execution
    (returns a value), without ever calling the function during detection.

    Args:
        fn: Function to trace. Can be:
            - Factory pattern: func(ctx: TraceContext) -> Callable[..., T]
            - Immediate execution: func(ctx: TraceContext) -> T
        name: Optional span name. If not provided, inferred from function.

    Returns:
        For factory pattern: Returns a wrapped function
        For immediate execution: Returns the result of executing the function
    """
    # Use inspect.signature() to detect pattern WITHOUT executing the function
    sig = inspect.signature(fn)
    params = list(sig.parameters.keys())

    # Check if first parameter is TraceContext-compatible
    needs_ctx = len(params) > 0 and params[0].lower() in ("ctx", "context", "tracecontext")

    # IMPORTANT: For async/coroutine functions, we skip probing entirely and assume
    # immediate execution. This is because:
    # - Factory pattern: `(ctx) => async (...args) => result` - outer function is SYNC
    # - Immediate execution: `async (ctx) => result` - function itself is ASYNC
    #
    # Probing async functions by executing them causes side effects (like creating
    # orphan spans) because the async function starts executing synchronously until
    # the first await.
    if inspect.iscoroutinefunction(fn):
        is_factory = False
    else:
        # For sync functions, check return type annotation to detect factory pattern
        return_annotation = sig.return_annotation

        # Detect if return type is a Callable (factory pattern)
        # Only explicit Callable[...] type hints trigger factory pattern
        is_factory = (
            return_annotation != inspect.Signature.empty
            and hasattr(return_annotation, "__origin__")
            and return_annotation.__origin__ is Callable
        )

        # If we can't determine from annotation, assume immediate execution
        # (safer default - avoids potential side effects from probing)

    # Infer span name
    span_name = name or _infer_name(fn)

    if is_factory:
        # Factory pattern: return a wrapped function
        return _wrap_factory(fn, span_name, needs_ctx)
    else:
        # Immediate execution pattern: execute immediately
        return _execute_immediately(fn, span_name, needs_ctx)


def _wrap_factory(
    fn: Callable[..., Any],
    span_name: str,
    needs_ctx: bool,
) -> Callable[..., Any]:
    """Wrap a factory function that returns another function."""
    # Create wrapper that will be called with the factory's arguments
    def wrapper(*args: Any, **kwargs: Any) -> Any:
        tracer = otel_trace.get_tracer(__name__)
        with (
            run_in_operation_context(span_name),
            tracer.start_as_current_span(span_name) as otel_span,
        ):
            try:
                # Create TraceContext
                ctx = TraceContext(otel_span)

                # Call the factory function with TraceContext
                # Note: Pattern detection already happened using inspect.signature()
                # (no execution), so we know this is a factory pattern.
                # We only call it here to get the returned function, not for detection.
                factory_result = fn(ctx, *args, **kwargs) if needs_ctx else fn(*args, **kwargs)

                # Check if result is actually a function (runtime validation)
                if not callable(factory_result):
                    raise TypeError(
                        f"Factory function expected to return a callable, "
                        f"got {type(factory_result).__name__}"
                    )

                # Call the returned function
                result = factory_result()

                # Handle errors
                if isinstance(result, tuple) and len(result) > 0:
                    # Check if last element is an error
                    last = result[-1]
                    if isinstance(last, Exception):
                        otel_span.record_exception(last)
                        otel_span.set_status(StatusCode.ERROR, str(last))
                        raise last

                return result
            except Exception as e:
                otel_span.record_exception(e)
                otel_span.set_status(StatusCode.ERROR, str(e))
                raise

    # Create async wrapper if factory function is async
    if inspect.iscoroutinefunction(fn):
        async def async_wrapper(*args: Any, **kwargs: Any) -> Any:
            tracer = otel_trace.get_tracer(__name__)
            with (
                run_in_operation_context(span_name),
                tracer.start_as_current_span(span_name) as otel_span,
            ):
                try:
                    ctx = TraceContext(otel_span)
                    if needs_ctx:
                        factory_result = await fn(ctx, *args, **kwargs)
                    else:
                        factory_result = await fn(*args, **kwargs)

                    if not callable(factory_result):
                        raise TypeError(
                            f"Factory function expected to return a callable, "
                            f"got {type(factory_result).__name__}"
                        )

                    # Call the returned function (could be sync or async)
                    if inspect.iscoroutinefunction(factory_result):
                        result = await factory_result()
                    else:
                        result = factory_result()

                    if isinstance(result, tuple) and len(result) > 0:
                        last = result[-1]
                        if isinstance(last, Exception):
                            otel_span.record_exception(last)
                            otel_span.set_status(StatusCode.ERROR, str(last))
                            raise last

                    return result
                except Exception as e:
                    otel_span.record_exception(e)
                    otel_span.set_status(StatusCode.ERROR, str(e))
                    raise

        return async_wrapper

    return wrapper


def _execute_immediately(
    fn: Callable[..., Any],
    span_name: str,
    needs_ctx: bool,
) -> Any:
    """Execute a function immediately within a trace."""
    # Handle async functions
    if inspect.iscoroutinefunction(fn):
        async def async_executor() -> Any:
            tracer = otel_trace.get_tracer(__name__)
            with (
                run_in_operation_context(span_name),
                tracer.start_as_current_span(span_name) as otel_span,
            ):
                try:
                    if needs_ctx:
                        ctx = TraceContext(otel_span)
                        result = await fn(ctx)
                    else:
                        result = await fn()

                    # Handle errors
                    if isinstance(result, tuple) and len(result) > 0:
                        last = result[-1]
                        if isinstance(last, Exception):
                            otel_span.record_exception(last)
                            otel_span.set_status(StatusCode.ERROR, str(last))
                            raise last

                    return result
                except Exception as e:
                    otel_span.record_exception(e)
                    otel_span.set_status(StatusCode.ERROR, str(e))
                    raise

        # Return a coroutine that needs to be awaited
        return async_executor()

    # Handle sync functions
    tracer = otel_trace.get_tracer(__name__)
    with (
        run_in_operation_context(span_name),
        tracer.start_as_current_span(span_name) as otel_span,
    ):
        try:
            if needs_ctx:
                ctx = TraceContext(otel_span)
                result = fn(ctx)
            else:
                result = fn()

            # Handle errors
            if isinstance(result, tuple) and len(result) > 0:
                last = result[-1]
                if isinstance(last, Exception):
                    otel_span.record_exception(last)
                    otel_span.set_status(StatusCode.ERROR, str(last))
                    raise last

            return result
        except Exception as e:
            otel_span.record_exception(e)
            otel_span.set_status(StatusCode.ERROR, str(e))
            raise


@contextmanager
def with_baggage(baggage: dict[str, str]) -> Any:
    """
    Execute code with updated baggage entries.

    Baggage is immutable in OpenTelemetry, so this helper creates a new context
    with the specified baggage entries and runs the code within that context.
    All child spans created within the context will inherit the baggage.

    Example:
        Setting baggage for downstream services
        ```python
        from autotel import trace, with_baggage

        @trace
        def create_order(order: Order):
            # Set baggage that will be propagated to downstream HTTP calls
            with with_baggage({
                'tenant.id': order.tenant_id,
                'user.id': order.user_id,
            }):
                # This HTTP call will include the baggage in headers
                fetch('/api/charge', method='POST', body=json.dumps(order))
        ```

    Example:
        Using with existing baggage
        ```python
        @trace
        def process_order(order: Order, ctx: TraceContext):
            # Read existing baggage
            tenant_id = ctx.get_baggage('tenant.id')

            # Add additional baggage entries
            with with_baggage({
                'order.id': order.id,
                'order.amount': str(order.amount),
            }):
                charge(order)
        ```

    Args:
        baggage: Dictionary of baggage entries to set (key-value pairs)
    """
    from opentelemetry.baggage import propagation

    current_context = context.get_current()
    # Get existing baggage
    existing_baggage = propagation.get_all(current_context)
    existing_baggage_dict = dict(existing_baggage) if existing_baggage else {}
    # Merge with new baggage entries
    updated_baggage = {**existing_baggage_dict, **baggage}
    # Create new context with updated baggage by setting each entry
    new_context = current_context
    for key, value in updated_baggage.items():
        new_context = propagation.set_baggage(key, str(value), new_context)
    # Attach new context
    token = context.attach(new_context)
    try:
        yield
    finally:
        context.detach(token)
