"""Complete example demonstrating all autotel features."""

import asyncio
from typing import Any

from autotel import (
    ConsoleSpanExporter,
    SimpleSpanProcessor,
    init,
    span,
    trace,
    track,
)

# Initialize autotel with console exporter for verification
init(
    service="example-app",
    span_processor=SimpleSpanProcessor(ConsoleSpanExporter()),
)


@trace
async def create_user(ctx, data: dict[str, Any]) -> None:
    """Create a user with tracing and events."""
    # Set span attributes
    ctx.set_attribute("user.email", data.get("email", ""))
    ctx.set_attribute("user.id", data.get("id", ""))

    # Simulate database operation
    with span("database.insert") as db_ctx:
        db_ctx.set_attribute("db.table", "users")
        db_ctx.set_attribute("db.operation", "INSERT")
        # Simulate DB call
        await asyncio.sleep(0.1)
        user = {"id": data.get("id", "123"), "email": data.get("email", "")}

    # Track events event (auto-enriched with trace context)
    track("user_created", {
        "user_id": user["id"],
        "email": user["email"],
    })

    return user


@trace
async def process_order(ctx, order_id: str, amount: float) -> None:
    """Process an order with nested spans."""
    ctx.set_attribute("order.id", order_id)
    ctx.set_attribute("order.amount", amount)

    # Payment processing span
    with span("payment.process") as payment_ctx:
        payment_ctx.set_attribute("payment.method", "credit_card")
        await asyncio.sleep(0.05)

    # Inventory update span
    with span("inventory.update") as inventory_ctx:
        inventory_ctx.set_attribute("inventory.action", "deduct")
        await asyncio.sleep(0.03)

    # Track events
    track("order_completed", {
        "order_id": order_id,
        "amount": amount,
    })

    return {"order_id": order_id, "status": "completed"}


async def main() -> None:
    """Main function demonstrating complete workflow."""
    print("=== autotel Complete Example ===\n")

    # Create user
    print("1. Creating user...")
    user = await create_user({"id": "user-123", "email": "user@example.com"})
    print(f"   Created: {user}\n")

    # Process order
    print("2. Processing order...")
    order = await process_order("order-456", 99.99)
    print(f"   Processed: {order}\n")

    print("Check console output above for trace spans!")


if __name__ == "__main__":
    asyncio.run(main())
