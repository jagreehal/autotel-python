"""Example: Distributed workflow tracing across services.

Workflow starts in one "service" (order), propagates via baggage to another
(inventory). Uses in-memory context for demo; in production use message headers.
"""

import asyncio
from typing import Any

from autotel import (
    ConsoleSpanExporter,
    SimpleSpanProcessor,
    init,
)
from autotel.workflow_distributed import (
    trace_distributed_step,
    trace_distributed_workflow,
)
from opentelemetry import context

from autotel.workflow_distributed import WorkflowBaggage, WorkflowBaggageValues

init(
    service="distributed-workflow-example",
    span_processor=SimpleSpanProcessor(ConsoleSpanExporter()),
)


# --- "Order service" ---

@trace_distributed_workflow(
    name="OrderFulfillment",
    workflow_id_from=lambda order: order["id"],
    version="1.0.0",
    total_steps=2,
)
async def create_order(ctx: Any, order: dict) -> dict:
    """Entry point: create order and "send" to inventory (we set baggage for demo)."""
    ctx.record_step_progress("ValidateOrder", 0)
    # Simulate validation
    ctx.record_step_progress("ReserveInventory", 1)
    # In production: publish to queue with ctx.get_workflow_headers() in headers
    # For demo, baggage is already in context so next step sees it
    return {"workflow_id": ctx.workflow_id, "status": "started"}


# --- "Inventory service" (same process for demo; normally another service) ---

@trace_distributed_step(name="ReserveInventory", idempotent=True)
async def reserve_inventory(ctx: Any, request: dict) -> str:
    """Downstream step: reserve inventory; sees workflow_id from baggage."""
    wf_id = getattr(ctx, "workflow_id", None)
    print(f"[Inventory] Part of workflow {wf_id}, step {ctx.step_name}")
    return "reserved"


async def main() -> None:
    order = {"id": "ord-001", "items": ["sku-a", "sku-b"]}
    result = await create_order(order)
    print(f"Workflow started: {result}")

    # Simulate inventory service receiving message: set workflow baggage then run step
    baggage = WorkflowBaggageValues(
        workflow_id=order["id"],
        workflow_name="OrderFulfillment",
        workflow_version="1.0.0",
        step_index=1,
        total_steps=2,
    )
    token = context.attach(WorkflowBaggage.set(None, baggage.to_dict()))
    try:
        await reserve_inventory({"items": order["items"]})
    finally:
        context.detach(token)


if __name__ == "__main__":
    asyncio.run(main())
