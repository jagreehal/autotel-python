"""Tests for workflow and saga tracing."""

from typing import Any

import pytest

from autotel import init
from autotel.context import TraceContext
from autotel.exporters import InMemorySpanExporter
from autotel.processors import SimpleSpanProcessor
from autotel.workflow import (
    Saga,
    SagaFailed,
    Workflow,
    WorkflowStatus,
    trace_workflow,
)


@pytest.fixture
def exporter() -> Any:
    """Create in-memory exporter for testing."""
    exp = InMemorySpanExporter()
    init(service="test", span_processor=SimpleSpanProcessor(exp))
    return exp


# --- Workflow Tests ---


class TestWorkflow:
    """Tests for Workflow class."""

    def test_workflow_basic_sync(self, exporter: Any) -> None:
        """Test basic sync workflow execution."""
        results: list[str] = []

        def step1(ctx: TraceContext) -> str:
            results.append("step1")
            return "result1"

        def step2(ctx: TraceContext) -> str:
            results.append("step2")
            return "result2"

        wf = Workflow("test-workflow")
        with wf.run_sync() as _ctx:
            wf.step_sync("step1", step1)
            wf.step_sync("step2", step2)

        assert results == ["step1", "step2"]
        assert wf.status == WorkflowStatus.COMPLETED
        assert len(wf.completed_steps) == 2

        spans = exporter.get_finished_spans()
        # Should have workflow span + 2 step spans
        assert len(spans) == 3

        # Find workflow span
        workflow_span = next(s for s in spans if s.name == "test-workflow")
        assert workflow_span.attributes["workflow.name"] == "test-workflow"
        assert workflow_span.attributes["workflow.status"] == "completed"

    @pytest.mark.asyncio
    async def test_workflow_basic_async(self, exporter: Any) -> None:
        """Test basic async workflow execution."""
        results: list[str] = []

        async def step1(ctx: TraceContext) -> str:
            results.append("step1")
            return "result1"

        async def step2(ctx: TraceContext) -> str:
            results.append("step2")
            return "result2"

        wf = Workflow("async-workflow")
        async with wf.run() as _ctx:
            await wf.step("step1", step1)
            await wf.step("step2", step2)

        assert results == ["step1", "step2"]
        assert wf.status == WorkflowStatus.COMPLETED

    def test_workflow_step_with_args(self, exporter: Any) -> None:
        """Test workflow step with arguments."""

        def process(ctx: TraceContext, value: int) -> int:
            return value * 2

        wf = Workflow("args-workflow")
        with wf.run_sync():
            result = wf.step_sync("multiply", process, 5)

        assert result == 10

    def test_workflow_failure_triggers_compensation(self, exporter: Any) -> None:
        """Test that workflow failure triggers compensation."""
        compensated: list[str] = []

        def step1(ctx: TraceContext) -> str:
            return "done"

        def step2(ctx: TraceContext) -> str:
            raise ValueError("Step 2 failed")

        def compensate1(ctx: TraceContext) -> None:
            compensated.append("step1")

        wf = Workflow("failing-workflow", auto_compensate=True)

        with pytest.raises(ValueError, match="Step 2 failed"), wf.run_sync():
            wf.step_sync("step1", step1, compensation=compensate1)
            wf.step_sync("step2", step2)

        assert wf.status == WorkflowStatus.COMPENSATED
        assert "step1" in compensated

    def test_workflow_no_auto_compensate(self, exporter: Any) -> None:
        """Test workflow with auto_compensate=False."""
        compensated: list[str] = []

        def step1(ctx: TraceContext) -> str:
            return "done"

        def failing_step(ctx: TraceContext) -> str:
            raise ValueError("Failed")

        def compensate1(ctx: TraceContext) -> None:
            compensated.append("step1")

        wf = Workflow("no-compensate", auto_compensate=False)

        with pytest.raises(ValueError), wf.run_sync():
            wf.step_sync("step1", step1, compensation=compensate1)
            wf.step_sync("failing", failing_step)

        # Should NOT have compensated
        assert wf.status == WorkflowStatus.FAILED
        assert compensated == []

    def test_workflow_step_result_tracking(self, exporter: Any) -> None:
        """Test that step results are tracked."""

        def step1(ctx: TraceContext) -> str:
            return "result1"

        def step2(ctx: TraceContext) -> int:
            return 42

        wf = Workflow("results-workflow")
        with wf.run_sync():
            wf.step_sync("step1", step1)
            wf.step_sync("step2", step2)

        assert len(wf.completed_steps) == 2
        assert wf.completed_steps[0].name == "step1"
        assert wf.completed_steps[0].result == "result1"
        assert wf.completed_steps[0].success is True
        assert wf.completed_steps[1].result == 42

    def test_workflow_attributes(self, exporter: Any) -> None:
        """Test workflow with custom attributes."""

        def step1(ctx: TraceContext) -> None:
            pass

        wf = Workflow(
            "attributed-workflow",
            attributes={"order_id": "ord-123", "customer": "acme"},
        )
        with wf.run_sync():
            wf.step_sync("step1", step1)

        spans = exporter.get_finished_spans()
        workflow_span = next(s for s in spans if s.name == "attributed-workflow")

        assert workflow_span.attributes["workflow.order_id"] == "ord-123"
        assert workflow_span.attributes["workflow.customer"] == "acme"


class TestTraceWorkflowDecorator:
    """Tests for @trace_workflow decorator."""

    def test_sync_decorator(self, exporter: Any) -> None:
        """Test sync workflow decorator."""

        @trace_workflow("decorated-workflow")
        def my_workflow(wf: Workflow) -> str:
            with wf.run_sync():
                wf.step_sync("step1", lambda ctx: "done")
            return "completed"

        result = my_workflow()
        assert result == "completed"

        spans = exporter.get_finished_spans()
        assert any(s.name == "decorated-workflow" for s in spans)

    @pytest.mark.asyncio
    async def test_async_decorator(self, exporter: Any) -> None:
        """Test async workflow decorator."""

        @trace_workflow("async-decorated")
        async def my_workflow(wf: Workflow) -> str:
            async with wf.run():
                await wf.step("step1", lambda ctx: "done")
            return "completed"

        result = await my_workflow()
        assert result == "completed"


# --- Saga Tests ---


class TestSaga:
    """Tests for Saga class."""

    def test_saga_basic_sync(self, exporter: Any) -> None:
        """Test basic sync saga execution."""
        results: list[str] = []

        def action1(ctx: TraceContext) -> str:
            results.append("action1")
            return "result1"

        def comp1(ctx: TraceContext) -> None:
            results.append("comp1")

        def action2(ctx: TraceContext) -> str:
            results.append("action2")
            return "result2"

        def comp2(ctx: TraceContext) -> None:
            results.append("comp2")

        saga = Saga("test-saga")
        saga.add_step("step1", action1, comp1)
        saga.add_step("step2", action2, comp2)

        outputs = saga.execute_sync()

        assert results == ["action1", "action2"]
        assert outputs == ["result1", "result2"]

    @pytest.mark.asyncio
    async def test_saga_basic_async(self, exporter: Any) -> None:
        """Test basic async saga execution."""
        results: list[str] = []

        async def action1(ctx: TraceContext) -> str:
            results.append("action1")
            return "result1"

        async def comp1(ctx: TraceContext) -> None:
            results.append("comp1")

        saga = Saga("async-saga")
        saga.add_step("step1", action1, comp1)

        outputs = await saga.execute()

        assert results == ["action1"]
        assert outputs == ["result1"]

    def test_saga_failure_triggers_compensation(self, exporter: Any) -> None:
        """Test that saga failure triggers compensation in reverse order."""
        actions: list[str] = []
        compensations: list[str] = []

        def action1(ctx: TraceContext) -> str:
            actions.append("action1")
            return "done"

        def comp1(ctx: TraceContext) -> None:
            compensations.append("comp1")

        def action2(ctx: TraceContext) -> str:
            actions.append("action2")
            return "done"

        def comp2(ctx: TraceContext) -> None:
            compensations.append("comp2")

        def action3(ctx: TraceContext) -> str:
            actions.append("action3")
            raise ValueError("Action 3 failed")

        def comp3(ctx: TraceContext) -> None:
            compensations.append("comp3")

        saga = Saga("failing-saga")
        saga.add_step("step1", action1, comp1)
        saga.add_step("step2", action2, comp2)
        saga.add_step("step3", action3, comp3)

        with pytest.raises(SagaFailed) as exc_info:
            saga.execute_sync()

        # Actions should have run up to the failing one
        assert actions == ["action1", "action2", "action3"]

        # Compensations should run in REVERSE order (not including failed step)
        assert compensations == ["comp2", "comp1"]

        # Check exception details
        assert exc_info.value.failed_step == "step3"
        assert exc_info.value.compensated == 2

    def test_saga_with_args(self, exporter: Any) -> None:
        """Test saga steps with arguments."""
        results: list[int] = []

        def reserve(ctx: TraceContext, item_id: int, quantity: int) -> dict[str, Any]:
            results.append(item_id * quantity)
            return {"reserved": True}

        def release(ctx: TraceContext, item_id: int, quantity: int) -> None:
            results.append(-(item_id * quantity))

        saga = Saga("args-saga")
        saga.add_step("reserve", reserve, release, 123, quantity=5)

        outputs = saga.execute_sync()

        assert results == [615]  # 123 * 5
        assert outputs == [{"reserved": True}]

    def test_saga_span_attributes(self, exporter: Any) -> None:
        """Test saga creates spans with correct attributes."""

        def action(ctx: TraceContext) -> None:
            pass

        def comp(ctx: TraceContext) -> None:
            pass

        saga = Saga("attributed-saga")
        saga.add_step("step1", action, comp)
        saga.add_step("step2", action, comp)

        saga.execute_sync()

        spans = exporter.get_finished_spans()

        # Find saga span
        saga_span = next(s for s in spans if s.name == "attributed-saga")
        assert saga_span.attributes["saga.name"] == "attributed-saga"
        assert saga_span.attributes["saga.step_count"] == 2
        assert saga_span.attributes["saga.status"] == "completed"

        # Find step spans
        step_spans = [s for s in spans if "step" in s.name and "attributed-saga" in s.name]
        assert len(step_spans) == 2

    def test_saga_chaining(self, exporter: Any) -> None:
        """Test saga method chaining."""

        def action(ctx: TraceContext) -> str:
            return "done"

        def comp(ctx: TraceContext) -> None:
            pass

        saga = (
            Saga("chained-saga")
            .add_step("step1", action, comp)
            .add_step("step2", action, comp)
            .add_step("step3", action, comp)
        )

        results = saga.execute_sync()
        assert len(results) == 3

    def test_saga_compensation_failure_continues(self, exporter: Any) -> None:
        """Test that compensation failures don't stop other compensations."""
        compensations: list[str] = []

        def action1(ctx: TraceContext) -> str:
            return "done"

        def comp1(ctx: TraceContext) -> None:
            compensations.append("comp1")

        def action2(ctx: TraceContext) -> str:
            return "done"

        def comp2(ctx: TraceContext) -> None:
            compensations.append("comp2_start")
            raise RuntimeError("Compensation 2 failed")

        def action3(ctx: TraceContext) -> str:
            raise ValueError("Action 3 failed")

        def comp3(ctx: TraceContext) -> None:
            compensations.append("comp3")

        saga = Saga("comp-failure-saga")
        saga.add_step("step1", action1, comp1)
        saga.add_step("step2", action2, comp2)
        saga.add_step("step3", action3, comp3)

        with pytest.raises(SagaFailed):
            saga.execute_sync()

        # All compensations should be attempted even if one fails
        assert "comp2_start" in compensations
        assert "comp1" in compensations


class TestSagaFailed:
    """Tests for SagaFailed exception."""

    def test_exception_attributes(self) -> None:
        """Test SagaFailed exception attributes."""
        original = ValueError("Original error")
        exc = SagaFailed("step2", original, 1)

        assert exc.failed_step == "step2"
        assert exc.original_error == original
        assert exc.compensated == 1
        assert "step2" in str(exc)
        assert "Original error" in str(exc)
