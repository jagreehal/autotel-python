"""Example: Parking Lot pattern for webhook/callback tracing.

Park trace context when starting an async operation (e.g. payment), then
retrieve and link when the callback arrives (e.g. Stripe webhook).
"""

import asyncio
from typing import Any

from autotel import (
    ConsoleSpanExporter,
    SimpleSpanProcessor,
    init,
    trace,
)
from autotel.webhook import (
    CallbackContext,
    InMemoryTraceContextStore,
    create_correlation_key,
    create_parking_lot,
)

init(
    service="parking-lot-example",
    span_processor=SimpleSpanProcessor(ConsoleSpanExporter()),
)

store = InMemoryTraceContextStore(cleanup_interval_seconds=0)
parking_lot = create_parking_lot(
    store=store,
    default_ttl_seconds=3600,
    key_prefix="example:",
)


@trace
async def initiate_payment(ctx: Any, order_id: str, amount_cents: int) -> str:
    """Start a payment; park current trace context for the webhook."""
    ctx.set_attribute("order.id", order_id)
    ctx.set_attribute("payment.amount_cents", amount_cents)

    correlation_key = create_correlation_key("payment", order_id)
    await parking_lot.park(
        correlation_key,
        metadata={"order_id": order_id, "amount": str(amount_cents)},
    )
    # In production: await stripe.create_payment_intent(metadata={"order_id": order_id})
    print(f"[Initiate] Parked context for {correlation_key}")
    return correlation_key


@parking_lot.trace_callback(
    name="webhook.payment.succeeded",
    correlation_key_from=lambda event: create_correlation_key("payment", event["order_id"]),
)
async def handle_payment_webhook(ctx: CallbackContext, event: dict) -> None:
    """Handle payment success webhook; links to original trace."""
    if ctx.parked_context:
        print(f"[Webhook] Elapsed ms since payment: {ctx.elapsed_ms:.0f}")
        print(f"[Webhook] Original trace_id: {ctx.parked_context.trace_id}")
    else:
        print("[Webhook] No parked context (e.g. expired or wrong key)")
    # In production: await fulfill_order(event["order_id"])


async def main() -> None:
    # Simulate: service initiates payment
    key = await initiate_payment("ord-123", 9999)
    # Simulate: later, webhook arrives
    await handle_payment_webhook({"order_id": "ord-123"})
    # Miss: no parked context
    await handle_payment_webhook({"order_id": "ord-unknown"})


if __name__ == "__main__":
    asyncio.run(main())
