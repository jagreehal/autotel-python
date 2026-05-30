"""Workflow and Saga tracing for complex orchestration patterns.

This module provides tracing support for workflow/saga patterns commonly used
in event-driven architectures:

- **Workflows**: Multi-step processes with ordered execution
- **Sagas**: Workflows with compensation (rollback) handling

Implements feature parity with Go's workflow.Workflow and Node's traceWorkflow.
"""

from __future__ import annotations

import asyncio
import functools
import inspect
from collections.abc import AsyncIterator, Callable, Iterator
from contextlib import asynccontextmanager, contextmanager
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Generic, ParamSpec, TypeVar

from opentelemetry import trace as otel_trace
from opentelemetry.trace import Span, SpanKind, StatusCode

from .context import TraceContext
from .operation_context import run_in_operation_context

P = ParamSpec("P")
R = TypeVar("R")
T = TypeVar("T")


class WorkflowStatus(Enum):
    """Workflow execution status."""

    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    COMPENSATING = "compensating"
    COMPENSATED = "compensated"


@dataclass
class StepResult(Generic[T]):
    """Result of a workflow step execution."""

    name: str
    success: bool
    result: T | None = None
    error: Exception | None = None
    compensated: bool = False


@dataclass
class WorkflowStep:
    """Definition of a workflow step with optional compensation."""

    name: str
    handler: Callable[..., Any]
    compensation: Callable[..., Any] | None = None
    is_async: bool = False
    timeout_ms: int | None = None


class Workflow:
    """Workflow tracer for multi-step processes with saga support.

    Creates a parent span for the workflow and child spans for each step.
    Supports compensation (rollback) handlers for saga pattern.

    Example:
        >>> async def order_workflow():
        ...     wf = Workflow("order-fulfillment")
        ...
        ...     async with wf.run() as ctx:
        ...         # Each step gets its own span linked to the workflow
        ...         await wf.step("validate", validate_order)
        ...         await wf.step("reserve_inventory", reserve_items,
        ...                       compensation=release_items)
        ...         await wf.step("charge_payment", charge_card,
        ...                       compensation=refund_card)
        ...         await wf.step("ship_order", ship_items)
        ...
        ...     # On failure, compensations run automatically in reverse order

    Sync example:
        >>> def sync_workflow():
        ...     wf = Workflow("data-migration")
        ...
        ...     with wf.run_sync() as ctx:
        ...         wf.step_sync("extract", extract_data)
        ...         wf.step_sync("transform", transform_data)
        ...         wf.step_sync("load", load_data)
    """

    def __init__(
        self,
        name: str,
        *,
        auto_compensate: bool = True,
        attributes: dict[str, Any] | None = None,
    ) -> None:
        """Initialize a workflow.

        Args:
            name: Workflow name (used for span name).
            auto_compensate: If True, run compensations on failure (default True).
            attributes: Additional span attributes for the workflow span.
        """
        self.name = name
        self.auto_compensate = auto_compensate
        self.attributes = attributes or {}

        self._status = WorkflowStatus.PENDING
        self._steps: list[WorkflowStep] = []
        self._completed_steps: list[StepResult[Any]] = []
        self._workflow_span: Span | None = None
        self._tracer = otel_trace.get_tracer(__name__)

    @property
    def status(self) -> WorkflowStatus:
        """Get current workflow status."""
        return self._status

    @property
    def completed_steps(self) -> list[StepResult[Any]]:
        """Get list of completed step results."""
        return list(self._completed_steps)

    def _set_workflow_attributes(self, span: Span) -> None:
        """Set standard workflow attributes on the span."""
        span.set_attribute("workflow.name", self.name)
        span.set_attribute("workflow.status", self._status.value)
        span.set_attribute("workflow.step_count", len(self._completed_steps))

        for key, value in self.attributes.items():
            if isinstance(value, str | bool | int | float):
                span.set_attribute(f"workflow.{key}", value)

    async def _run_compensation(self, _ctx: TraceContext, step: StepResult[Any]) -> bool:
        """Run compensation for a single step."""
        # Find the step definition
        step_def = next((s for s in self._steps if s.name == step.name), None)
        if not step_def or not step_def.compensation:
            return False

        with self._tracer.start_as_current_span(
            f"{step.name}.compensate",
            kind=SpanKind.INTERNAL,
        ) as comp_span:
            comp_span.set_attribute("workflow.step.name", step.name)
            comp_span.set_attribute("workflow.step.type", "compensation")
            comp_ctx = TraceContext(comp_span)

            try:
                if step_def.is_async or asyncio.iscoroutinefunction(step_def.compensation):
                    await step_def.compensation(comp_ctx)
                else:
                    step_def.compensation(comp_ctx)
                return True
            except Exception as e:
                comp_span.record_exception(e)
                comp_span.set_status(StatusCode.ERROR, f"Compensation failed: {e}")
                return False

    def _run_compensation_sync(self, _ctx: TraceContext, step: StepResult[Any]) -> bool:
        """Run compensation for a single step (sync version)."""
        step_def = next((s for s in self._steps if s.name == step.name), None)
        if not step_def or not step_def.compensation:
            return False

        with self._tracer.start_as_current_span(
            f"{step.name}.compensate",
            kind=SpanKind.INTERNAL,
        ) as comp_span:
            comp_span.set_attribute("workflow.step.name", step.name)
            comp_span.set_attribute("workflow.step.type", "compensation")
            comp_ctx = TraceContext(comp_span)

            try:
                step_def.compensation(comp_ctx)
                return True
            except Exception as e:
                comp_span.record_exception(e)
                comp_span.set_status(StatusCode.ERROR, f"Compensation failed: {e}")
                return False

    async def compensate(self, ctx: TraceContext | None = None) -> list[StepResult[Any]]:
        """Run compensations for all completed steps in reverse order.

        Called automatically on workflow failure if auto_compensate=True.

        Args:
            ctx: Optional TraceContext for compensation span.

        Returns:
            List of compensation results.
        """
        self._status = WorkflowStatus.COMPENSATING
        if self._workflow_span:
            self._workflow_span.set_attribute("workflow.status", self._status.value)
            self._workflow_span.add_event("workflow.compensating")

        results: list[StepResult[Any]] = []

        # Run compensations in reverse order
        for step in reversed(self._completed_steps):
            if ctx:
                success = await self._run_compensation(ctx, step)
            else:
                # Create a temporary context if none provided
                with self._tracer.start_as_current_span("compensate") as span:
                    temp_ctx = TraceContext(span)
                    success = await self._run_compensation(temp_ctx, step)

            results.append(
                StepResult(
                    name=step.name,
                    success=success,
                    compensated=True,
                )
            )

        self._status = WorkflowStatus.COMPENSATED
        if self._workflow_span:
            self._workflow_span.set_attribute("workflow.status", self._status.value)
            self._workflow_span.add_event("workflow.compensated")

        return results

    def compensate_sync(self, ctx: TraceContext | None = None) -> list[StepResult[Any]]:
        """Run compensations synchronously."""
        self._status = WorkflowStatus.COMPENSATING
        if self._workflow_span:
            self._workflow_span.set_attribute("workflow.status", self._status.value)
            self._workflow_span.add_event("workflow.compensating")

        results: list[StepResult[Any]] = []

        for step in reversed(self._completed_steps):
            if ctx:
                success = self._run_compensation_sync(ctx, step)
            else:
                with self._tracer.start_as_current_span("compensate") as span:
                    temp_ctx = TraceContext(span)
                    success = self._run_compensation_sync(temp_ctx, step)

            results.append(
                StepResult(
                    name=step.name,
                    success=success,
                    compensated=True,
                )
            )

        self._status = WorkflowStatus.COMPENSATED
        if self._workflow_span:
            self._workflow_span.set_attribute("workflow.status", self._status.value)
            self._workflow_span.add_event("workflow.compensated")

        return results

    async def step(
        self,
        name: str,
        handler: Callable[..., Any],
        *args: Any,
        compensation: Callable[..., Any] | None = None,
        timeout_ms: int | None = None,
        **kwargs: Any,
    ) -> Any:
        """Execute a workflow step (async).

        Args:
            name: Step name (used for span name).
            handler: Async function to execute.
            *args: Arguments passed to handler (after ctx).
            compensation: Optional compensation handler for saga rollback.
            timeout_ms: Optional timeout in milliseconds.
            **kwargs: Keyword arguments passed to handler.

        Returns:
            Result of the handler function.

        Raises:
            Exception: Any exception from the handler (after recording).
        """
        # Register step
        step_def = WorkflowStep(
            name=name,
            handler=handler,
            compensation=compensation,
            is_async=True,
            timeout_ms=timeout_ms,
        )
        self._steps.append(step_def)

        with self._tracer.start_as_current_span(
            f"{self.name}.{name}",
            kind=SpanKind.INTERNAL,
        ) as step_span:
            step_span.set_attribute("workflow.name", self.name)
            step_span.set_attribute("workflow.step.name", name)
            step_span.set_attribute("workflow.step.index", len(self._completed_steps))
            step_span.set_attribute("workflow.step.has_compensation", compensation is not None)

            ctx = TraceContext(step_span)

            try:
                if asyncio.iscoroutinefunction(handler):
                    result = await handler(ctx, *args, **kwargs)
                else:
                    result = handler(ctx, *args, **kwargs)

                self._completed_steps.append(
                    StepResult(name=name, success=True, result=result)
                )
                step_span.add_event("workflow.step.completed")
                return result

            except Exception as e:
                step_span.record_exception(e)
                step_span.set_status(StatusCode.ERROR, str(e))
                self._completed_steps.append(
                    StepResult(name=name, success=False, error=e)
                )
                raise

    def step_sync(
        self,
        name: str,
        handler: Callable[..., Any],
        *args: Any,
        compensation: Callable[..., Any] | None = None,
        **kwargs: Any,
    ) -> Any:
        """Execute a workflow step (sync).

        Args:
            name: Step name.
            handler: Sync function to execute.
            *args: Arguments passed to handler (after ctx).
            compensation: Optional compensation handler.
            **kwargs: Keyword arguments passed to handler.

        Returns:
            Result of the handler function.
        """
        step_def = WorkflowStep(
            name=name,
            handler=handler,
            compensation=compensation,
            is_async=False,
        )
        self._steps.append(step_def)

        with self._tracer.start_as_current_span(
            f"{self.name}.{name}",
            kind=SpanKind.INTERNAL,
        ) as step_span:
            step_span.set_attribute("workflow.name", self.name)
            step_span.set_attribute("workflow.step.name", name)
            step_span.set_attribute("workflow.step.index", len(self._completed_steps))
            step_span.set_attribute("workflow.step.has_compensation", compensation is not None)

            ctx = TraceContext(step_span)

            try:
                result = handler(ctx, *args, **kwargs)
                self._completed_steps.append(
                    StepResult(name=name, success=True, result=result)
                )
                step_span.add_event("workflow.step.completed")
                return result

            except Exception as e:
                step_span.record_exception(e)
                step_span.set_status(StatusCode.ERROR, str(e))
                self._completed_steps.append(
                    StepResult(name=name, success=False, error=e)
                )
                raise

    @asynccontextmanager
    async def run(self) -> AsyncIterator[TraceContext]:
        """Execute the workflow as an async context manager.

        Creates a parent span for the entire workflow. On exception,
        automatically runs compensations if auto_compensate=True.

        Yields:
            TraceContext for the workflow span.

        Example:
            >>> async with workflow.run() as ctx:
            ...     await workflow.step("step1", handler1)
            ...     await workflow.step("step2", handler2)
        """
        self._status = WorkflowStatus.RUNNING

        with (
            run_in_operation_context(self.name),
            self._tracer.start_as_current_span(
                self.name,
                kind=SpanKind.INTERNAL,
            ) as span,
        ):
            self._workflow_span = span
            self._set_workflow_attributes(span)
            span.add_event("workflow.started")

            ctx = TraceContext(span)

            try:
                yield ctx

                self._status = WorkflowStatus.COMPLETED
                span.set_attribute("workflow.status", self._status.value)
                span.add_event("workflow.completed")

            except Exception as e:
                self._status = WorkflowStatus.FAILED
                span.set_attribute("workflow.status", self._status.value)
                span.record_exception(e)
                span.set_status(StatusCode.ERROR, str(e))
                span.add_event("workflow.failed", {"error": str(e)})

                if self.auto_compensate and self._completed_steps:
                    await self.compensate(ctx)

                raise

    @contextmanager
    def run_sync(self) -> Iterator[TraceContext]:
        """Execute the workflow as a sync context manager.

        Yields:
            TraceContext for the workflow span.
        """
        self._status = WorkflowStatus.RUNNING

        with (
            run_in_operation_context(self.name),
            self._tracer.start_as_current_span(
                self.name,
                kind=SpanKind.INTERNAL,
            ) as span,
        ):
            self._workflow_span = span
            self._set_workflow_attributes(span)
            span.add_event("workflow.started")

            ctx = TraceContext(span)

            try:
                yield ctx

                self._status = WorkflowStatus.COMPLETED
                span.set_attribute("workflow.status", self._status.value)
                span.add_event("workflow.completed")

            except Exception as e:
                self._status = WorkflowStatus.FAILED
                span.set_attribute("workflow.status", self._status.value)
                span.record_exception(e)
                span.set_status(StatusCode.ERROR, str(e))
                span.add_event("workflow.failed", {"error": str(e)})

                if self.auto_compensate and self._completed_steps:
                    self.compensate_sync(ctx)

                raise


def trace_workflow(
    name: str,
    *,
    auto_compensate: bool = True,
    attributes: dict[str, Any] | None = None,
) -> Callable[[Callable[P, R]], Callable[P, R]]:
    """Decorator for workflow functions.

    Wraps a function in a workflow context, making the workflow available
    as the first argument.

    Args:
        name: Workflow name.
        auto_compensate: Run compensations on failure (default True).
        attributes: Additional workflow attributes.

    Returns:
        Decorated function.

    Example:
        >>> @trace_workflow("order-fulfillment")
        ... async def fulfill_order(wf: Workflow, order_id: str):
        ...     async with wf.run():
        ...         await wf.step("validate", validate_order, order_id)
        ...         await wf.step("charge", charge_card,
        ...                       compensation=refund_card)
        ...         await wf.step("ship", ship_order)
    """

    def decorator(func: Callable[P, R]) -> Callable[P, R]:
        if inspect.iscoroutinefunction(func):

            @functools.wraps(func)
            async def async_wrapper(*args: P.args, **kwargs: P.kwargs) -> R:
                wf = Workflow(name, auto_compensate=auto_compensate, attributes=attributes)
                return await func(wf, *args, **kwargs)  # type: ignore[arg-type, no-any-return]

            return async_wrapper  # type: ignore[return-value]
        else:

            @functools.wraps(func)
            def sync_wrapper(*args: P.args, **kwargs: P.kwargs) -> R:
                wf = Workflow(name, auto_compensate=auto_compensate, attributes=attributes)
                return func(wf, *args, **kwargs)  # type: ignore[arg-type]

            return sync_wrapper

    return decorator


@dataclass
class SagaStep:
    """A saga step with action and compensation."""

    name: str
    action: Callable[..., Any]
    compensation: Callable[..., Any]
    args: tuple[Any, ...] = field(default_factory=tuple)
    kwargs: dict[str, Any] = field(default_factory=dict)


class Saga:
    """Saga pattern implementation with guaranteed compensation.

    A saga is a sequence of steps where each step has a corresponding
    compensation action. If any step fails, all previous compensations
    are executed in reverse order.

    Example:
        >>> saga = Saga("order-saga")
        >>> saga.add_step("reserve", reserve_inventory, release_inventory, order_id=123)
        >>> saga.add_step("charge", charge_payment, refund_payment, amount=99.99)
        >>> saga.add_step("ship", ship_order, cancel_shipment)
        >>>
        >>> try:
        ...     await saga.execute()
        ... except SagaFailed as e:
        ...     print(f"Saga failed at step {e.failed_step}, compensations run: {e.compensated}")
    """

    def __init__(self, name: str, attributes: dict[str, Any] | None = None) -> None:
        """Initialize a saga.

        Args:
            name: Saga name (used for span name).
            attributes: Additional span attributes.
        """
        self.name = name
        self.attributes = attributes or {}
        self._steps: list[SagaStep] = []
        self._tracer = otel_trace.get_tracer(__name__)

    def add_step(
        self,
        name: str,
        action: Callable[..., Any],
        compensation: Callable[..., Any],
        *args: Any,
        **kwargs: Any,
    ) -> Saga:
        """Add a step to the saga.

        Args:
            name: Step name.
            action: Action to execute.
            compensation: Compensation to run if later steps fail.
            *args: Arguments for action/compensation.
            **kwargs: Keyword arguments for action/compensation.

        Returns:
            Self for chaining.
        """
        self._steps.append(
            SagaStep(
                name=name,
                action=action,
                compensation=compensation,
                args=args,
                kwargs=kwargs,
            )
        )
        return self

    async def execute(self) -> list[Any]:
        """Execute the saga asynchronously.

        Returns:
            List of results from each step.

        Raises:
            SagaFailed: If any step fails (after running compensations).
        """
        with (
            run_in_operation_context(self.name),
            self._tracer.start_as_current_span(
                self.name,
                kind=SpanKind.INTERNAL,
            ) as saga_span,
        ):
            saga_span.set_attribute("saga.name", self.name)
            saga_span.set_attribute("saga.step_count", len(self._steps))
            saga_span.add_event("saga.started")

            ctx = TraceContext(saga_span)
            completed: list[tuple[SagaStep, Any]] = []
            results: list[Any] = []

            try:
                for i, step in enumerate(self._steps):
                    with self._tracer.start_as_current_span(
                        f"{self.name}.{step.name}",
                        kind=SpanKind.INTERNAL,
                    ) as step_span:
                        step_span.set_attribute("saga.step.name", step.name)
                        step_span.set_attribute("saga.step.index", i)
                        step_ctx = TraceContext(step_span)

                        try:
                            if asyncio.iscoroutinefunction(step.action):
                                result = await step.action(step_ctx, *step.args, **step.kwargs)
                            else:
                                result = step.action(step_ctx, *step.args, **step.kwargs)

                            completed.append((step, result))
                            results.append(result)
                            step_span.add_event("saga.step.completed")

                        except Exception as e:
                            step_span.record_exception(e)
                            step_span.set_status(StatusCode.ERROR, str(e))

                            # Run compensations
                            await self._compensate(ctx, completed)

                            saga_span.set_attribute("saga.status", "failed")
                            saga_span.set_attribute("saga.failed_step", step.name)
                            saga_span.add_event(
                                "saga.failed",
                                {"step": step.name, "error": str(e)},
                            )
                            raise SagaFailed(step.name, e, len(completed)) from e

                saga_span.set_attribute("saga.status", "completed")
                saga_span.add_event("saga.completed")
                return results

            except SagaFailed:
                raise
            except Exception as e:
                saga_span.record_exception(e)
                saga_span.set_status(StatusCode.ERROR, str(e))
                raise

    def execute_sync(self) -> list[Any]:
        """Execute the saga synchronously.

        Returns:
            List of results from each step.
        """
        with (
            run_in_operation_context(self.name),
            self._tracer.start_as_current_span(
                self.name,
                kind=SpanKind.INTERNAL,
            ) as saga_span,
        ):
            saga_span.set_attribute("saga.name", self.name)
            saga_span.set_attribute("saga.step_count", len(self._steps))
            saga_span.add_event("saga.started")

            ctx = TraceContext(saga_span)
            completed: list[tuple[SagaStep, Any]] = []
            results: list[Any] = []

            try:
                for i, step in enumerate(self._steps):
                    with self._tracer.start_as_current_span(
                        f"{self.name}.{step.name}",
                        kind=SpanKind.INTERNAL,
                    ) as step_span:
                        step_span.set_attribute("saga.step.name", step.name)
                        step_span.set_attribute("saga.step.index", i)
                        step_ctx = TraceContext(step_span)

                        try:
                            result = step.action(step_ctx, *step.args, **step.kwargs)
                            completed.append((step, result))
                            results.append(result)
                            step_span.add_event("saga.step.completed")

                        except Exception as e:
                            step_span.record_exception(e)
                            step_span.set_status(StatusCode.ERROR, str(e))

                            self._compensate_sync(ctx, completed)

                            saga_span.set_attribute("saga.status", "failed")
                            saga_span.set_attribute("saga.failed_step", step.name)
                            saga_span.add_event(
                                "saga.failed",
                                {"step": step.name, "error": str(e)},
                            )
                            raise SagaFailed(step.name, e, len(completed)) from e

                saga_span.set_attribute("saga.status", "completed")
                saga_span.add_event("saga.completed")
                return results

            except SagaFailed:
                raise
            except Exception as e:
                saga_span.record_exception(e)
                saga_span.set_status(StatusCode.ERROR, str(e))
                raise

    async def _compensate(
        self,
        _ctx: TraceContext,
        completed: list[tuple[SagaStep, Any]],
    ) -> None:
        """Run compensations for completed steps in reverse order."""
        for step, _ in reversed(completed):
            with self._tracer.start_as_current_span(
                f"{self.name}.{step.name}.compensate",
                kind=SpanKind.INTERNAL,
            ) as comp_span:
                comp_span.set_attribute("saga.step.name", step.name)
                comp_span.set_attribute("saga.step.type", "compensation")
                comp_ctx = TraceContext(comp_span)

                try:
                    if asyncio.iscoroutinefunction(step.compensation):
                        await step.compensation(comp_ctx, *step.args, **step.kwargs)
                    else:
                        step.compensation(comp_ctx, *step.args, **step.kwargs)
                    comp_span.add_event("saga.compensation.completed")
                except Exception as e:
                    comp_span.record_exception(e)
                    comp_span.set_status(StatusCode.ERROR, f"Compensation failed: {e}")

    def _compensate_sync(
        self,
        _ctx: TraceContext,
        completed: list[tuple[SagaStep, Any]],
    ) -> None:
        """Run compensations synchronously."""
        for step, _ in reversed(completed):
            with self._tracer.start_as_current_span(
                f"{self.name}.{step.name}.compensate",
                kind=SpanKind.INTERNAL,
            ) as comp_span:
                comp_span.set_attribute("saga.step.name", step.name)
                comp_span.set_attribute("saga.step.type", "compensation")
                comp_ctx = TraceContext(comp_span)

                try:
                    step.compensation(comp_ctx, *step.args, **step.kwargs)
                    comp_span.add_event("saga.compensation.completed")
                except Exception as e:
                    comp_span.record_exception(e)
                    comp_span.set_status(StatusCode.ERROR, f"Compensation failed: {e}")


class SagaFailed(Exception):
    """Exception raised when a saga step fails.

    Attributes:
        failed_step: Name of the step that failed.
        original_error: The original exception.
        compensated: Number of steps that were compensated.
    """

    def __init__(
        self,
        failed_step: str,
        original_error: Exception,
        compensated: int,
    ) -> None:
        super().__init__(f"Saga failed at step '{failed_step}': {original_error}")
        self.failed_step = failed_step
        self.original_error = original_error
        self.compensated = compensated
