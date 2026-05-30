"""Example: Safe business baggage schema for context propagation.

Define allowed keys, optional hashing for PII, and use the schema to set/get
baggage that propagates with the trace.
"""

import asyncio
from typing import Any

from opentelemetry import context

from autotel import (
    ConsoleSpanExporter,
    SimpleSpanProcessor,
    init,
    trace,
)
from autotel.business_baggage import create_safe_baggage_schema

init(
    service="business-baggage-example",
    span_processor=SimpleSpanProcessor(ConsoleSpanExporter()),
)

# Schema: order and customer IDs, priority; customer_id is hashed (PII-safe)
OrderBaggage = create_safe_baggage_schema(
    {
        "order_id": {"type": "string", "max_length": 64},
        "customer_id": {"type": "string", "max_length": 64, "hash": True},
        "priority": {"type": "enum", "values": ["low", "normal", "high", "critical"]},
    },
    prefix="order",
)


@trace
async def create_order(ctx: Any, order_id: str, customer_id: str, priority: str) -> None:
    """Producer: set baggage that will propagate to downstream spans/calls."""
    ctx.set_attribute("order.id", order_id)
    new_ctx = OrderBaggage.set(None, {
        "order_id": order_id,
        "customer_id": customer_id,
        "priority": priority,
    })
    token = context.attach(new_ctx)
    try:
        await process_payment(order_id)
    finally:
        context.detach(token)


@trace
async def process_payment(ctx: Any, order_id: str) -> None:
    """Downstream: read baggage (e.g. after extract from headers)."""
    oid = OrderBaggage.get(None, "order_id")
    priority = OrderBaggage.get(None, "priority")
    # customer_id comes back hashed
    cust = OrderBaggage.get(None, "customer_id") or ""
    print(f"[process_payment] order_id={oid}, priority={priority}, customer_id={cust[:20] if len(cust) > 20 else cust}...")


def main() -> None:
    asyncio.run(create_order("ord-1", "cust-abc", "high"))


if __name__ == "__main__":
    main()
