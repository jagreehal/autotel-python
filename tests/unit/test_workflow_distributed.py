"""Tests for distributed workflow tracing."""

from typing import Any

import pytest
from opentelemetry import context

from autotel import init
from autotel.exporters import InMemorySpanExporter
from autotel.processors import SimpleSpanProcessor
from autotel.workflow_distributed import (
    DistributedStepContext,
    DistributedWorkflowContext,
    WorkflowBaggage,
    WorkflowBaggageValues,
    create_workflow_headers,
    generate_workflow_id,
    get_workflow_progress,
    is_in_distributed_workflow,
    trace_distributed_step,
    trace_distributed_workflow,
)


@pytest.fixture
def exporter() -> Any:
    """Create in-memory exporter for testing."""
    exp = InMemorySpanExporter()
    init(service="test", span_processor=SimpleSpanProcessor(exp))
    return exp


def test_generate_workflow_id() -> None:
    """generate_workflow_id returns a non-empty string."""
    wf_id = generate_workflow_id()
    assert isinstance(wf_id, str)
    assert len(wf_id) > 0
    assert "-" in wf_id or wf_id.replace("-", "").replace(" ", "").isalnum()


def test_generate_workflow_id_with_prefix() -> None:
    """generate_workflow_id with prefix includes it."""
    wf_id = generate_workflow_id(prefix="order")
    assert wf_id.startswith("order-")


def test_create_workflow_headers(exporter: Any) -> None:
    """create_workflow_headers returns headers with baggage key."""
    headers = create_workflow_headers({
        "workflow_id": "wf-123",
        "workflow_name": "TestWorkflow",
        "step_index": 1,
    })
    assert "baggage" in headers
    assert "workflow.workflow_id" in headers["baggage"] or "wf-123" in headers["baggage"]


def test_is_in_distributed_workflow_false_without_baggage(exporter: Any) -> None:
    """is_in_distributed_workflow returns False when no workflow baggage."""
    assert is_in_distributed_workflow() is False


def test_get_workflow_progress_none_without_baggage(exporter: Any) -> None:
    """get_workflow_progress returns None when not in workflow."""
    assert get_workflow_progress() is None


def test_workflow_baggage_values_to_dict() -> None:
    """WorkflowBaggageValues.to_dict includes required fields."""
    v = WorkflowBaggageValues(workflow_id="id1", workflow_name="Test")
    d = v.to_dict()
    assert d["workflow_id"] == "id1"
    assert d["workflow_name"] == "Test"


def test_workflow_baggage_values_from_dict() -> None:
    """WorkflowBaggageValues.from_dict restores values."""
    d = {"workflow_id": "id1", "workflow_name": "Test", "step_index": 2}
    v = WorkflowBaggageValues.from_dict(d)
    assert v.workflow_id == "id1"
    assert v.workflow_name == "Test"
    assert v.step_index == 2


def test_trace_distributed_workflow_sync_no_ctx(exporter: Any) -> None:
    """trace_distributed_workflow sync without ctx creates workflow span."""

    @trace_distributed_workflow(
        name="SyncWorkflow",
        workflow_id_from=lambda x: str(x),
        version="1.0.0",
    )
    def run_workflow(value: int) -> int:
        return value + 1

    result = run_workflow(42)
    assert result == 43

    spans = exporter.get_finished_spans()
    assert len(spans) >= 1
    workflow_span = next((s for s in spans if s.name == "workflow.SyncWorkflow"), None)
    assert workflow_span is not None
    assert workflow_span.attributes.get("workflow.id") == "42"
    assert workflow_span.attributes.get("workflow.name") == "SyncWorkflow"
    assert workflow_span.attributes.get("workflow.version") == "1.0.0"


def test_trace_distributed_workflow_sync_with_ctx(exporter: Any) -> None:
    """trace_distributed_workflow sync with ctx passes DistributedWorkflowContext."""

    @trace_distributed_workflow(
        name="SyncCtxWorkflow",
        workflow_id_from=lambda order: order["id"],
    )
    def run_workflow(ctx: DistributedWorkflowContext, order: dict) -> str:
        assert ctx.workflow_id == order["id"]
        assert ctx.workflow_name == "SyncCtxWorkflow"
        ctx.record_step_progress("Validate", 0)
        return ctx.workflow_id

    result = run_workflow({"id": "ord-99"})
    assert result == "ord-99"

    spans = exporter.get_finished_spans()
    workflow_span = next((s for s in spans if s.name == "workflow.SyncCtxWorkflow"), None)
    assert workflow_span is not None
    assert workflow_span.attributes.get("workflow.id") == "ord-99"


@pytest.mark.asyncio
async def test_trace_distributed_workflow_async_with_ctx(exporter: Any) -> None:
    """trace_distributed_workflow async with ctx."""
    captured_ctx: DistributedWorkflowContext | None = None

    @trace_distributed_workflow(
        name="AsyncWorkflow",
        workflow_id_from=lambda x: x["id"],
        total_steps=2,
    )
    async def run_workflow(ctx: DistributedWorkflowContext, data: dict) -> str:
        nonlocal captured_ctx
        captured_ctx = ctx
        assert ctx.workflow_id == data["id"]
        return "ok"

    result = await run_workflow({"id": "async-1"})
    assert result == "ok"
    assert captured_ctx is not None
    assert captured_ctx.workflow_id == "async-1"
    assert captured_ctx.workflow_name == "AsyncWorkflow"

    spans = exporter.get_finished_spans()
    workflow_span = next((s for s in spans if s.name == "workflow.AsyncWorkflow"), None)
    assert workflow_span is not None
    assert workflow_span.attributes.get("workflow.total_steps") == 2


def test_trace_distributed_step_sync_no_baggage(exporter: Any) -> None:
    """trace_distributed_step without workflow baggage still creates step span."""

    @trace_distributed_step(name="StandaloneStep")
    def do_step() -> str:
        return "done"

    result = do_step()
    assert result == "done"

    spans = exporter.get_finished_spans()
    step_span = next((s for s in spans if s.name == "workflow.step.StandaloneStep"), None)
    assert step_span is not None
    assert step_span.attributes.get("workflow.step.name") == "StandaloneStep"


def test_trace_distributed_step_sync_with_ctx_no_baggage(exporter: Any) -> None:
    """trace_distributed_step with ctx when no baggage gives None workflow_id."""

    @trace_distributed_step(name="StepNoBaggage")
    def do_step(ctx: DistributedStepContext) -> str | None:
        assert ctx.workflow_id is None
        assert ctx.step_name == "StepNoBaggage"
        return ctx.workflow_id

    result = do_step()
    assert result is None


def test_trace_distributed_step_with_baggage(exporter: Any) -> None:
    """trace_distributed_step reads workflow baggage when set in context."""
    # Set workflow baggage in current context so the step sees it
    baggage_values = WorkflowBaggageValues(
        workflow_id="wf-bag-1",
        workflow_name="BaggageWorkflow",
        workflow_version="2.0",
        step_index=0,
        total_steps=3,
    )
    new_ctx = WorkflowBaggage.set(None, baggage_values.to_dict())
    token = context.attach(new_ctx)

    try:

        @trace_distributed_step(name="StepWithBaggage")
        def do_step(ctx: DistributedStepContext) -> str:
            assert ctx.workflow_id == "wf-bag-1"
            assert ctx.workflow_name == "BaggageWorkflow"
            assert ctx.step_name == "StepWithBaggage"
            return ctx.workflow_id or ""

        result = do_step()
        assert result == "wf-bag-1"

        spans = exporter.get_finished_spans()
        step_span = next((s for s in spans if s.name == "workflow.step.StepWithBaggage"), None)
        assert step_span is not None
        assert step_span.attributes.get("workflow.id") == "wf-bag-1"
        assert step_span.attributes.get("workflow.name") == "BaggageWorkflow"
    finally:
        context.detach(token)


@pytest.mark.asyncio
async def test_trace_distributed_step_async(exporter: Any) -> None:
    """trace_distributed_step works with async function."""

    @trace_distributed_step(name="AsyncStep")
    async def do_step() -> int:
        return 100

    result = await do_step()
    assert result == 100

    spans = exporter.get_finished_spans()
    step_span = next((s for s in spans if s.name == "workflow.step.AsyncStep"), None)
    assert step_span is not None


def test_trace_distributed_step_idempotent_and_compensation(exporter: Any) -> None:
    """trace_distributed_step sets idempotent and is_compensation attributes."""

    @trace_distributed_step(name="CompensationStep", idempotent=True, is_compensation=True)
    def compensate() -> None:
        pass

    compensate()

    spans = exporter.get_finished_spans()
    step_span = next((s for s in spans if s.name == "workflow.step.CompensationStep"), None)
    assert step_span is not None
    assert step_span.attributes.get("workflow.step.idempotent") is True
    assert step_span.attributes.get("workflow.step.is_compensation") is True


def test_trace_distributed_workflow_on_start_on_complete(exporter: Any) -> None:
    """trace_distributed_workflow calls on_start and on_complete."""
    started: list[str] = []
    completed: list[tuple[str, str]] = []

    @trace_distributed_workflow(
        name="CallbackWorkflow",
        workflow_id_from=lambda x: x,
        on_start=lambda ctx: started.append(ctx.workflow_id),
        on_complete=lambda ctx, result: completed.append((ctx.workflow_id, result)),
    )
    def run_wf(order_id: str) -> str:
        return f"done-{order_id}"

    result = run_wf("ord-cb")
    assert result == "done-ord-cb"
    assert started == ["ord-cb"]
    assert completed == [("ord-cb", "done-ord-cb")]


def test_trace_distributed_workflow_on_error(exporter: Any) -> None:
    """trace_distributed_workflow calls on_error when function raises."""
    errors: list[tuple[str, str]] = []

    @trace_distributed_workflow(
        name="ErrorWorkflow",
        workflow_id_from=lambda: "err-1",
        on_error=lambda ctx, e: errors.append((ctx.workflow_id, str(e))),
    )
    def run_wf() -> None:
        raise ValueError("step failed")

    with pytest.raises(ValueError, match="step failed"):
        run_wf()

    assert len(errors) == 1
    assert errors[0][0] == "err-1"
    assert "step failed" in errors[0][1]

    spans = exporter.get_finished_spans()
    workflow_span = next((s for s in spans if s.name == "workflow.ErrorWorkflow"), None)
    assert workflow_span is not None
    assert workflow_span.status.status_code.value == 2  # ERROR


# ---------------------------------------------------------------------------
# Characterization tests pinning behavior for the async/sync dedup refactor.
# These assert current behavior so the refactor cannot silently change it.
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_trace_distributed_workflow_async_on_error(exporter: Any) -> None:
    """Async path calls on_error and marks the span ERROR, like the sync path."""
    errors: list[tuple[str, str]] = []

    @trace_distributed_workflow(
        name="AsyncErrorWorkflow",
        workflow_id_from=lambda: "aerr-1",
        on_error=lambda ctx, e: errors.append((ctx.workflow_id, str(e))),
    )
    async def run_wf() -> None:
        raise ValueError("async boom")

    with pytest.raises(ValueError, match="async boom"):
        await run_wf()

    assert errors == [("aerr-1", "async boom")]
    spans = exporter.get_finished_spans()
    span = next((s for s in spans if s.name == "workflow.AsyncErrorWorkflow"), None)
    assert span is not None
    assert span.status.status_code.value == 2  # ERROR


def test_trace_distributed_workflow_on_error_fires_when_on_start_raises(
    exporter: Any,
) -> None:
    """If on_start raises, ctx is already created so on_error must still fire."""
    errors: list[str] = []
    called = []

    @trace_distributed_workflow(
        name="OnStartRaises",
        workflow_id_from=lambda: "osr-1",
        on_start=lambda ctx: (_ for _ in ()).throw(RuntimeError("start failed")),
        on_error=lambda ctx, e: errors.append(str(e)),
    )
    def run_wf() -> str:
        called.append(True)
        return "ok"

    with pytest.raises(RuntimeError, match="start failed"):
        run_wf()

    # Function body never runs; on_error receives the on_start failure.
    assert called == []
    assert errors == ["start failed"]


def test_trace_distributed_step_callbacks(exporter: Any) -> None:
    """Step decorator invokes on_start / on_complete / on_error callbacks."""
    started: list[Any] = []
    completed: list[Any] = []
    errored: list[str] = []

    @trace_distributed_step(
        name="OkStep",
        on_start=lambda ctx: started.append(ctx.step_name),
        on_complete=lambda ctx, result: completed.append(result),
    )
    def ok_step() -> int:
        return 42

    assert ok_step() == 42
    assert started == ["OkStep"]
    assert completed == [42]

    @trace_distributed_step(
        name="BadStep",
        on_error=lambda ctx, e: errored.append(str(e)),
    )
    def bad_step() -> None:
        raise ValueError("step boom")

    with pytest.raises(ValueError, match="step boom"):
        bad_step()
    assert errored == ["step boom"]


def test_trace_distributed_workflow_emits_lifecycle_events(exporter: Any) -> None:
    """Success path emits workflow.started and workflow.completed events."""

    @trace_distributed_workflow(name="EventWorkflow", workflow_id_from=lambda: "ev-1")
    def run_wf() -> str:
        return "done"

    assert run_wf() == "done"
    spans = exporter.get_finished_spans()
    span = next((s for s in spans if s.name == "workflow.EventWorkflow"), None)
    assert span is not None
    event_names = [e.name for e in span.events]
    assert "workflow.started" in event_names
    assert "workflow.completed" in event_names
