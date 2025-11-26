"""Functional API for autotel - HOF patterns, batch instrumentation, and context managers."""

import inspect
import re
from collections.abc import Callable
from contextlib import contextmanager
from functools import lru_cache
from typing import Any, ParamSpec, TypeVar

from opentelemetry import context, trace

from .context import TraceContext

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

    tracer = trace.get_tracer(__name__)
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
