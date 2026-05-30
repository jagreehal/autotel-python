"""Distributed workflow tracing with cross-service correlation.

Enables tracking workflows that span multiple microservices by propagating
workflow identity (workflow_id, step_name, step_index) via baggage in message headers.

Unlike local workflow.py (which uses context managers), distributed workflows
propagate context across network boundaries using W3C baggage.

Example:
    >>> # Service A: Order Service
    >>> from autotel.workflow_distributed import trace_distributed_workflow, WorkflowBaggage
    >>> from autotel.messaging import trace_producer
    >>>
    >>> @trace_distributed_workflow(
    ...     name="OrderFulfillment",
    ...     workflow_id_from=lambda order: order["id"],
    ...     version="1.0.0",
    ... )
    ... async def create_order(ctx, order: dict):
    ...     # Workflow baggage is auto-set
    ...     await publish_to_inventory(order)
    >>>
    >>> @trace_producer(system="kafka", destination="inventory-requests")
    ... async def publish_to_inventory(ctx, order):
    ...     headers = ctx.inject_headers()  # Includes workflow.* baggage
    ...     await producer.send(topic="inventory-requests", value=order, headers=headers)
    >>>
    >>> # Service B: Inventory Service
    >>> from autotel.workflow_distributed import trace_distributed_step
    >>>
    >>> @trace_distributed_step(name="ReserveInventory")
    ... async def process_inventory(ctx, message):
    ...     # ctx.workflow_id === order.id (propagated from Service A)
    ...     print(f"Processing step for workflow {ctx.workflow_id}")
    ...     await reserve_items(message["items"])
"""

from __future__ import annotations

import functools
import inspect
import time
from collections.abc import Callable
from dataclasses import asdict, dataclass
from typing import Any, Literal, ParamSpec, TypeVar

from opentelemetry import trace as otel_trace
from opentelemetry.propagate import inject
from opentelemetry.trace import Span, SpanKind, StatusCode

from .business_baggage import create_safe_baggage_schema
from .context import TraceContext
from .decorators import CTX_PARAM_NAMES, _rewrite_signature_without_ctx
from .operation_context import run_in_operation_context

P = ParamSpec("P")
R = TypeVar("R")


def _apply_custom_attributes(span: Span, attributes: dict[str, Any] | None) -> None:
    """Set user-provided primitive attributes on a span (non-primitives skipped)."""
    if not attributes:
        return
    for key, value in attributes.items():
        if isinstance(value, str | bool | int | float):
            span.set_attribute(key, value)


# ============================================================================
# Workflow Baggage Schema
# ============================================================================

# Pre-built baggage schema for distributed workflows
WorkflowBaggage = create_safe_baggage_schema(
    {
        "workflow_id": {"type": "string", "max_length": 128, "required": True},
        "workflow_name": {"type": "string", "max_length": 64, "required": True},
        "workflow_version": {"type": "string", "max_length": 32},
        "step_name": {"type": "string", "max_length": 64},
        "step_index": {"type": "number"},
        "total_steps": {"type": "number"},
        "parent_workflow_id": {"type": "string", "max_length": 128},
        "correlation_id": {"type": "string", "max_length": 128},
        "priority": {"type": "enum", "values": ["low", "normal", "high", "critical"]},
        "initiated_by": {"type": "string", "max_length": 64},
        "started_at": {"type": "string", "max_length": 30},
    },
    prefix="workflow",
    hash_high_cardinality=False,  # Workflow IDs should be traceable
    redact_pii=False,  # Workflow fields are internal identifiers
)


# ============================================================================
# Types
# ============================================================================

WorkflowPriority = Literal["low", "normal", "high", "critical"]


@dataclass
class WorkflowBaggageValues:
    """Values stored in workflow baggage."""

    workflow_id: str
    workflow_name: str
    workflow_version: str | None = None
    step_name: str | None = None
    step_index: int | None = None
    total_steps: int | None = None
    parent_workflow_id: str | None = None
    correlation_id: str | None = None
    priority: WorkflowPriority | None = None
    initiated_by: str | None = None
    started_at: str | None = None

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary, excluding None values."""
        return {k: v for k, v in asdict(self).items() if v is not None}

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> WorkflowBaggageValues:
        """Create from dictionary."""
        return cls(
            workflow_id=data.get("workflow_id", ""),
            workflow_name=data.get("workflow_name", ""),
            workflow_version=data.get("workflow_version"),
            step_name=data.get("step_name"),
            step_index=data.get("step_index"),
            total_steps=data.get("total_steps"),
            parent_workflow_id=data.get("parent_workflow_id"),
            correlation_id=data.get("correlation_id"),
            priority=data.get("priority"),
            initiated_by=data.get("initiated_by"),
            started_at=data.get("started_at"),
        )


class DistributedWorkflowContext(TraceContext):
    """Extended context for distributed workflow root."""

    def __init__(
        self,
        span: Span,
        workflow_id: str,
        workflow_name: str,
        workflow_version: str | None,
        baggage_values: WorkflowBaggageValues,
    ) -> None:
        """Initialize distributed workflow context.

        Args:
            span: The span for this workflow.
            workflow_id: The workflow ID.
            workflow_name: The workflow name.
            workflow_version: The workflow version.
            baggage_values: The workflow baggage values.
        """
        super().__init__(span)
        self.workflow_id = workflow_id
        self.workflow_name = workflow_name
        self.workflow_version = workflow_version
        self._baggage_values = baggage_values

    def get_workflow_baggage(self) -> WorkflowBaggageValues:
        """Get workflow baggage for propagation to other services."""
        return WorkflowBaggageValues(
            workflow_id=self._baggage_values.workflow_id,
            workflow_name=self._baggage_values.workflow_name,
            workflow_version=self._baggage_values.workflow_version,
            step_name=self._baggage_values.step_name,
            step_index=self._baggage_values.step_index,
            total_steps=self._baggage_values.total_steps,
            parent_workflow_id=self._baggage_values.parent_workflow_id,
            correlation_id=self._baggage_values.correlation_id,
            priority=self._baggage_values.priority,
            initiated_by=self._baggage_values.initiated_by,
            started_at=self._baggage_values.started_at,
        )

    def set_workflow_baggage(self, **values: Any) -> None:
        """Set additional workflow baggage fields."""
        for key, value in values.items():
            if hasattr(self._baggage_values, key):
                setattr(self._baggage_values, key, value)
        WorkflowBaggage.set(None, self._baggage_values.to_dict())

    def get_workflow_headers(self) -> dict[str, str]:
        """Get headers with workflow baggage for outgoing requests."""
        headers: dict[str, str] = {}
        inject(headers)
        return headers

    def record_step_progress(self, step_name: str, step_index: int) -> None:
        """Record workflow step completion (for progress tracking)."""
        self._baggage_values.step_name = step_name
        self._baggage_values.step_index = step_index
        WorkflowBaggage.set(None, self._baggage_values.to_dict())

        self._span.add_event(
            "workflow.step_progress",
            {
                "workflow.step.name": step_name,
                "workflow.step.index": step_index,
            },
        )


class DistributedStepContext(TraceContext):
    """Extended context for distributed workflow step."""

    def __init__(
        self,
        span: Span,
        workflow_id: str | None,
        workflow_name: str | None,
        step_name: str,
        step_index: int | None,
        is_compensation: bool,
        baggage_values: WorkflowBaggageValues | None,
    ) -> None:
        """Initialize distributed step context.

        Args:
            span: The span for this step.
            workflow_id: The workflow ID (from baggage).
            workflow_name: The workflow name (from baggage).
            step_name: The current step name.
            step_index: The current step index.
            is_compensation: Whether this step is a compensation.
            baggage_values: The workflow baggage values.
        """
        super().__init__(span)
        self.workflow_id = workflow_id
        self.workflow_name = workflow_name
        self.step_name = step_name
        self.step_index = step_index
        self.is_compensation = is_compensation
        self._baggage_values = baggage_values
        self._compensation_data: dict[str, Any] | None = None

    def get_workflow_baggage(self) -> WorkflowBaggageValues | None:
        """Get the full workflow baggage."""
        if self._baggage_values:
            return WorkflowBaggageValues(
                workflow_id=self._baggage_values.workflow_id,
                workflow_name=self._baggage_values.workflow_name,
                workflow_version=self._baggage_values.workflow_version,
                step_name=self._baggage_values.step_name,
                step_index=self._baggage_values.step_index,
                total_steps=self._baggage_values.total_steps,
                parent_workflow_id=self._baggage_values.parent_workflow_id,
                correlation_id=self._baggage_values.correlation_id,
                priority=self._baggage_values.priority,
                initiated_by=self._baggage_values.initiated_by,
                started_at=self._baggage_values.started_at,
            )
        return None

    def update_workflow_baggage(self, **values: Any) -> None:
        """Update workflow baggage (e.g., increment step index)."""
        if self._baggage_values:
            for key, value in values.items():
                if hasattr(self._baggage_values, key):
                    setattr(self._baggage_values, key, value)
            WorkflowBaggage.set(None, self._baggage_values.to_dict())

    def get_workflow_headers(self) -> dict[str, str]:
        """Get headers with updated workflow baggage for downstream calls."""
        headers: dict[str, str] = {}
        inject(headers)
        return headers

    def requires_compensation(self, compensation_data: dict[str, Any] | None = None) -> None:
        """Mark step as requiring compensation on failure."""
        self._compensation_data = compensation_data
        self._span.set_attribute("workflow.step.requires_compensation", True)
        attrs: dict[str, Any] = {"workflow.step.name": self.step_name}
        if compensation_data:
            import json

            attrs["workflow.step.compensation_data"] = json.dumps(compensation_data)
        self._span.add_event("workflow.step.compensation_registered", attrs)


# ============================================================================
# Distributed Workflow Decorator
# ============================================================================


def trace_distributed_workflow(
    name: str,
    workflow_id_from: Callable[..., str],
    version: str | None = None,
    total_steps: int | None = None,
    parent_workflow_id: str | None = None,
    correlation_id: str | None = None,
    priority: WorkflowPriority | None = None,
    initiated_by: str | None = None,
    attributes: dict[str, Any] | None = None,
    on_start: Callable[[DistributedWorkflowContext], None] | None = None,
    on_complete: Callable[[DistributedWorkflowContext, Any], None] | None = None,
    on_error: Callable[[DistributedWorkflowContext, Exception], None] | None = None,
) -> Callable[[Callable[P, R]], Callable[P, R]]:
    """Decorator for distributed workflow entry points.

    Wraps a function as the entry point for a distributed workflow. Automatically:
    - Generates or extracts workflow ID
    - Sets workflow baggage for downstream propagation
    - Creates root span with workflow attributes

    Args:
        name: Workflow name/type (e.g., "OrderFulfillment", "UserOnboarding")
        workflow_id_from: Function to extract workflow ID from function arguments
        version: Workflow version (e.g., "1.0.0")
        total_steps: Total number of steps if known
        parent_workflow_id: Parent workflow ID (for sub-workflows)
        correlation_id: Correlation ID for external systems
        priority: Workflow priority
        initiated_by: User/system that initiated the workflow
        attributes: Additional span attributes
        on_start: Callback on workflow start
        on_complete: Callback on workflow completion
        on_error: Callback on workflow error

    Returns:
        Decorated function

    Example:
        >>> @trace_distributed_workflow(
        ...     name="OrderFulfillment",
        ...     workflow_id_from=lambda order: order["id"],
        ...     version="1.0.0",
        ... )
        ... async def create_order(ctx, order: dict):
        ...     ctx.record_step_progress("ValidateOrder", 0)
        ...     await validate_order(order)
        ...
        ...     ctx.record_step_progress("ReserveInventory", 1)
        ...     await publish_to_inventory_service(order)
        ...
        ...     return {"workflow_id": ctx.workflow_id, "status": "started"}
    """
    span_name = f"workflow.{name}"

    def decorator(func: Callable[P, R]) -> Callable[P, R]:
        sig = inspect.signature(func)
        params = list(sig.parameters.keys())
        needs_ctx = len(params) > 0 and params[0] in CTX_PARAM_NAMES

        # Shared logic for the async and sync wrappers below. Keeping it here
        # (rather than duplicated in each wrapper) means the ctx-binding and
        # error semantics live in exactly one place. The wrappers differ only
        # in `await func(...)` vs `func(...)`.
        def _prepare(
            args: tuple[Any, ...], kwargs: dict[str, Any]
        ) -> tuple[str, str, WorkflowBaggageValues]:
            workflow_id = workflow_id_from(*args, **kwargs)
            started_at = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
            baggage_values = WorkflowBaggageValues(
                workflow_id=workflow_id,
                workflow_name=name,
                workflow_version=version,
                step_index=0,
                total_steps=total_steps,
                parent_workflow_id=parent_workflow_id,
                correlation_id=correlation_id,
                priority=priority,
                initiated_by=initiated_by,
                started_at=started_at,
            )
            WorkflowBaggage.set(None, baggage_values.to_dict())
            return workflow_id, started_at, baggage_values

        def _create_ctx(
            span: Span,
            workflow_id: str,
            started_at: str,
            baggage_values: WorkflowBaggageValues,
        ) -> DistributedWorkflowContext:
            span.set_attribute("workflow.id", workflow_id)
            span.set_attribute("workflow.name", name)
            if version:
                span.set_attribute("workflow.version", version)
            if total_steps:
                span.set_attribute("workflow.total_steps", total_steps)
            if parent_workflow_id:
                span.set_attribute("workflow.parent_id", parent_workflow_id)
            if priority:
                span.set_attribute("workflow.priority", priority)
            if initiated_by:
                span.set_attribute("workflow.initiated_by", initiated_by)
            span.set_attribute("workflow.started_at", started_at)
            _apply_custom_attributes(span, attributes)
            return DistributedWorkflowContext(
                span=span,
                workflow_id=workflow_id,
                workflow_name=name,
                workflow_version=version,
                baggage_values=baggage_values,
            )

        def _on_start(span: Span, ctx: DistributedWorkflowContext, workflow_id: str) -> None:
            if on_start:
                on_start(ctx)
            span.add_event(
                "workflow.started",
                {"workflow.id": workflow_id, "workflow.name": name},
            )

        def _on_success(
            span: Span, ctx: DistributedWorkflowContext, result: Any, workflow_id: str
        ) -> None:
            if on_complete:
                on_complete(ctx, result)
            span.add_event("workflow.completed", {"workflow.id": workflow_id})

        def _on_error(
            span: Span,
            ctx: DistributedWorkflowContext | None,
            exc: Exception,
            workflow_id: str,
        ) -> None:
            if on_error and ctx is not None:
                on_error(ctx, exc)
            span.add_event(
                "workflow.failed",
                {"workflow.id": workflow_id, "workflow.error": str(exc)},
            )
            span.record_exception(exc)
            span.set_status(StatusCode.ERROR, str(exc))

        if inspect.iscoroutinefunction(func):

            @functools.wraps(func)
            async def async_wrapper(*args: P.args, **kwargs: P.kwargs) -> R:
                tracer = otel_trace.get_tracer(__name__)
                workflow_id, started_at, baggage_values = _prepare(args, kwargs)
                ctx: DistributedWorkflowContext | None = None
                with (
                    run_in_operation_context(span_name),
                    tracer.start_as_current_span(span_name, kind=SpanKind.INTERNAL) as span,
                ):
                    try:
                        ctx = _create_ctx(span, workflow_id, started_at, baggage_values)
                        _on_start(span, ctx, workflow_id)
                        if needs_ctx:
                            result = await func(ctx, *args, **kwargs)  # type: ignore[arg-type]
                        else:
                            result = await func(*args, **kwargs)
                        _on_success(span, ctx, result, workflow_id)
                        return result  # type: ignore[no-any-return]
                    except Exception as e:
                        _on_error(span, ctx, e, workflow_id)
                        raise

            if needs_ctx:
                _rewrite_signature_without_ctx(async_wrapper, func)
            return async_wrapper  # type: ignore[return-value]

        else:

            @functools.wraps(func)
            def sync_wrapper(*args: P.args, **kwargs: P.kwargs) -> R:
                tracer = otel_trace.get_tracer(__name__)
                workflow_id, started_at, baggage_values = _prepare(args, kwargs)
                ctx: DistributedWorkflowContext | None = None
                with (
                    run_in_operation_context(span_name),
                    tracer.start_as_current_span(span_name, kind=SpanKind.INTERNAL) as span,
                ):
                    try:
                        ctx = _create_ctx(span, workflow_id, started_at, baggage_values)
                        _on_start(span, ctx, workflow_id)
                        if needs_ctx:  # noqa: SIM108
                            result = func(ctx, *args, **kwargs)  # type: ignore[arg-type]
                        else:
                            result = func(*args, **kwargs)
                        _on_success(span, ctx, result, workflow_id)
                        return result
                    except Exception as e:
                        _on_error(span, ctx, e, workflow_id)
                        raise

            if needs_ctx:
                _rewrite_signature_without_ctx(sync_wrapper, func)
            return sync_wrapper

    return decorator


# ============================================================================
# Distributed Step Decorator
# ============================================================================


def trace_distributed_step(
    name: str,
    step_index: int | None = None,
    idempotent: bool | None = None,
    is_compensation: bool = False,
    attributes: dict[str, Any] | None = None,
    on_start: Callable[[DistributedStepContext], None] | None = None,
    on_complete: Callable[[DistributedStepContext, Any], None] | None = None,
    on_error: Callable[[DistributedStepContext, Exception], None] | None = None,
) -> Callable[[Callable[P, R]], Callable[P, R]]:
    """Decorator for distributed workflow steps.

    Use in downstream services to trace steps that are part of a distributed workflow.
    Automatically extracts workflow baggage from the current context.

    Args:
        name: Step name (e.g., "ReserveInventory", "ChargePayment")
        step_index: Override step index (otherwise uses baggage or auto-increments)
        idempotent: Whether this step is idempotent (safe to retry)
        is_compensation: Whether this step is a compensation/rollback step
        attributes: Additional span attributes
        on_start: Callback on step start
        on_complete: Callback on step completion
        on_error: Callback on step error

    Returns:
        Decorated function

    Example:
        >>> @trace_distributed_step(name="ReserveInventory", idempotent=True)
        ... async def reserve_inventory(ctx, request: dict):
        ...     if ctx.workflow_id:
        ...         print(f"Part of workflow {ctx.workflow_id}, step {ctx.step_index}")
        ...     return await inventory_service.reserve(request["items"])
    """
    span_name = f"workflow.step.{name}"

    def decorator(func: Callable[P, R]) -> Callable[P, R]:
        sig = inspect.signature(func)
        params = list(sig.parameters.keys())
        needs_ctx = len(params) > 0 and params[0] in CTX_PARAM_NAMES

        # Shared logic for the async and sync wrappers (see trace_distributed_workflow
        # for the rationale). The wrappers differ only in `await func` vs `func`.
        def _prepare() -> tuple[WorkflowBaggageValues | None, int | None]:
            extracted = WorkflowBaggage.get_all(None)
            baggage_values: WorkflowBaggageValues | None = None
            if extracted.get("workflow_id") and extracted.get("workflow_name"):
                baggage_values = WorkflowBaggageValues.from_dict(extracted)

            current_step_index: int | None
            if step_index is not None:
                current_step_index = step_index
            elif baggage_values and baggage_values.step_index is not None:
                current_step_index = baggage_values.step_index + 1
            else:
                current_step_index = None

            if baggage_values:
                baggage_values.step_name = name
                if current_step_index is not None:
                    baggage_values.step_index = current_step_index
                WorkflowBaggage.set(None, baggage_values.to_dict())

            return baggage_values, current_step_index

        def _create_ctx(
            span: Span,
            baggage_values: WorkflowBaggageValues | None,
            current_step_index: int | None,
        ) -> DistributedStepContext:
            span.set_attribute("workflow.step.name", name)
            if current_step_index is not None:
                span.set_attribute("workflow.step.index", current_step_index)
            if idempotent is not None:
                span.set_attribute("workflow.step.idempotent", idempotent)
            if is_compensation:
                span.set_attribute("workflow.step.is_compensation", True)

            if baggage_values:
                span.set_attribute("workflow.id", baggage_values.workflow_id)
                span.set_attribute("workflow.name", baggage_values.workflow_name)
                if baggage_values.workflow_version:
                    span.set_attribute("workflow.version", baggage_values.workflow_version)
                if baggage_values.total_steps:
                    span.set_attribute("workflow.total_steps", baggage_values.total_steps)

            _apply_custom_attributes(span, attributes)

            return DistributedStepContext(
                span=span,
                workflow_id=baggage_values.workflow_id if baggage_values else None,
                workflow_name=baggage_values.workflow_name if baggage_values else None,
                step_name=name,
                step_index=current_step_index,
                is_compensation=is_compensation,
                baggage_values=baggage_values,
            )

        def _on_start(
            span: Span,
            ctx: DistributedStepContext,
            baggage_values: WorkflowBaggageValues | None,
        ) -> None:
            if on_start:
                on_start(ctx)
            start_attrs: dict[str, Any] = {"workflow.step.name": name}
            if baggage_values:
                start_attrs["workflow.id"] = baggage_values.workflow_id
            span.add_event("workflow.step.started", start_attrs)

        def _on_success(span: Span, ctx: DistributedStepContext, result: Any) -> None:
            if on_complete:
                on_complete(ctx, result)
            span.add_event("workflow.step.completed", {"workflow.step.name": name})

        def _on_error(
            span: Span, ctx: DistributedStepContext | None, exc: Exception
        ) -> None:
            if on_error and ctx is not None:
                on_error(ctx, exc)
            span.add_event(
                "workflow.step.failed",
                {"workflow.step.name": name, "workflow.step.error": str(exc)},
            )
            span.record_exception(exc)
            span.set_status(StatusCode.ERROR, str(exc))

        if inspect.iscoroutinefunction(func):

            @functools.wraps(func)
            async def async_wrapper(*args: P.args, **kwargs: P.kwargs) -> R:
                tracer = otel_trace.get_tracer(__name__)
                baggage_values, current_step_index = _prepare()
                ctx: DistributedStepContext | None = None
                with (
                    run_in_operation_context(span_name),
                    tracer.start_as_current_span(span_name, kind=SpanKind.INTERNAL) as span,
                ):
                    try:
                        ctx = _create_ctx(span, baggage_values, current_step_index)
                        _on_start(span, ctx, baggage_values)
                        if needs_ctx:
                            result = await func(ctx, *args, **kwargs)  # type: ignore[arg-type]
                        else:
                            result = await func(*args, **kwargs)
                        _on_success(span, ctx, result)
                        return result  # type: ignore[no-any-return]
                    except Exception as e:
                        _on_error(span, ctx, e)
                        raise

            if needs_ctx:
                _rewrite_signature_without_ctx(async_wrapper, func)
            return async_wrapper  # type: ignore[return-value]

        else:

            @functools.wraps(func)
            def sync_wrapper(*args: P.args, **kwargs: P.kwargs) -> R:
                tracer = otel_trace.get_tracer(__name__)
                baggage_values, current_step_index = _prepare()
                ctx: DistributedStepContext | None = None
                with (
                    run_in_operation_context(span_name),
                    tracer.start_as_current_span(span_name, kind=SpanKind.INTERNAL) as span,
                ):
                    try:
                        ctx = _create_ctx(span, baggage_values, current_step_index)
                        _on_start(span, ctx, baggage_values)
                        if needs_ctx:  # noqa: SIM108
                            result = func(ctx, *args, **kwargs)  # type: ignore[arg-type]
                        else:
                            result = func(*args, **kwargs)
                        _on_success(span, ctx, result)
                        return result
                    except Exception as e:
                        _on_error(span, ctx, e)
                        raise

            if needs_ctx:
                _rewrite_signature_without_ctx(sync_wrapper, func)
            return sync_wrapper

    return decorator


# ============================================================================
# Utility Functions
# ============================================================================


def generate_workflow_id(prefix: str | None = None) -> str:
    """Generate a unique workflow ID.

    Args:
        prefix: Optional prefix for the ID

    Returns:
        A unique workflow ID

    Example:
        >>> workflow_id = generate_workflow_id("order")  # "order-abc123def456"
    """
    import secrets

    timestamp = hex(int(time.time()))[2:]
    random_part = secrets.token_hex(6)
    base_id = f"{timestamp}-{random_part}"
    return f"{prefix}-{base_id}" if prefix else base_id


def is_in_distributed_workflow(ctx: TraceContext | None = None) -> bool:
    """Check if the current context is part of a distributed workflow.

    Args:
        ctx: The trace context (uses current context if None)

    Returns:
        True if workflow baggage is present
    """
    baggage = WorkflowBaggage.get_all(ctx)
    return bool(baggage.get("workflow_id") and baggage.get("workflow_name"))


def get_workflow_progress(ctx: TraceContext | None = None) -> dict[str, Any] | None:
    """Get workflow progress information.

    Args:
        ctx: The trace context (uses current context if None)

    Returns:
        Progress info or None if not in a workflow
    """
    baggage = WorkflowBaggage.get_all(ctx)
    if not baggage.get("workflow_id") or not baggage.get("workflow_name"):
        return None

    total_steps = baggage.get("total_steps")
    step_index = baggage.get("step_index")

    percent_complete: int | None = None
    if total_steps and step_index is not None:
        percent_complete = round(((step_index + 1) / total_steps) * 100)

    return {
        "workflow_id": baggage["workflow_id"],
        "workflow_name": baggage["workflow_name"],
        "current_step": baggage.get("step_name"),
        "current_step_index": step_index,
        "total_steps": total_steps,
        "percent_complete": percent_complete,
    }


def create_workflow_headers(values: dict[str, Any]) -> dict[str, str]:
    """Create workflow correlation headers for manual propagation.

    Use when you need to manually add workflow context to outgoing requests.

    Args:
        values: Workflow baggage values

    Returns:
        Headers object with workflow baggage

    Example:
        >>> headers = create_workflow_headers({
        ...     "workflow_id": "order-123",
        ...     "workflow_name": "OrderFulfillment",
        ...     "step_index": 2,
        ... })
        >>> await fetch("/api/inventory", headers=headers)
    """
    from urllib.parse import quote

    headers: dict[str, str] = {}
    baggage_entries: list[str] = []

    key_mapping = {
        "workflow_id": "workflow.workflow_id",
        "workflow_name": "workflow.workflow_name",
        "workflow_version": "workflow.workflow_version",
        "step_name": "workflow.step_name",
        "step_index": "workflow.step_index",
        "total_steps": "workflow.total_steps",
        "priority": "workflow.priority",
        "correlation_id": "workflow.correlation_id",
        "parent_workflow_id": "workflow.parent_workflow_id",
        "initiated_by": "workflow.initiated_by",
        "started_at": "workflow.started_at",
    }

    for key, baggage_key in key_mapping.items():
        value = values.get(key)
        if value is not None:
            baggage_entries.append(f"{baggage_key}={quote(str(value))}")

    if baggage_entries:
        headers["baggage"] = ",".join(baggage_entries)

    return headers
