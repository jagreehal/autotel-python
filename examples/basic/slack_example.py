"""Example demonstrating Slack event subscriber."""

import asyncio
from typing import Any

from autotel import (
    ConsoleSpanExporter,
    SimpleSpanProcessor,
    init,
    trace,
    track,
)

# Mock Slack subscriber for demonstration (use real webhook URL in production)
from autotel.subscribers import SlackSubscriber


class MockSlackSubscriber(SlackSubscriber):
    """Mock Slack subscriber that prints messages instead of sending to Slack."""

    def __init__(self, channel: str = "#alerts", username: str = "autotel"):
        """Initialize mock subscriber."""
        # Don't call super().__init__ to avoid requiring webhook URL
        self.channel = channel
        self.username = username

    async def send(self, event: str, properties: dict | None = None) -> None:
        """Print Slack message instead of sending."""
        text = f"*{event}*"
        if properties:
            props_str = ", ".join(f"{k}={v}" for k, v in properties.items())
            print(f"[Slack #{self.channel}] {text} | {props_str}")
        else:
            print(f"[Slack #{self.channel}] {text}")

    async def shutdown(self) -> None:
        """No-op for mock."""


# Initialize autotel with Slack subscriber
init(
    service="slack-example",
    span_processor=SimpleSpanProcessor(ConsoleSpanExporter()),
    subscribers=[MockSlackSubscriber(channel="#alerts")],
)


@trace
async def create_user(ctx, data: dict[str, Any]) -> None:
    """Create a user and send alert to Slack."""
    # Simulate user creation
    user = {"id": "123", "email": data.get("email", "")}

    # Attach metadata to span
    ctx.set_attribute("user.id", user["id"])
    ctx.set_attribute("user.email", user["email"])

    # Track events event (will be sent to Slack)
    track("user_created", {
        "user_id": user["id"],
        "email": user["email"],
    })

    return user


@trace
async def process_order(ctx, order_id: str, amount: float) -> None:
    """Process an order and send alert to Slack."""
    # Simulate order processing
    order = {"id": order_id, "amount": amount, "status": "completed"}

    ctx.set_attribute("order.id", order_id)
    ctx.set_attribute("order.amount", amount)

    # Track events event (will be sent to Slack)
    track("order_completed", {
        "order_id": order_id,
        "amount": amount,
        "currency": "USD",
    })

    return order


async def main() -> None:
    """Main function demonstrating Slack integration."""
    print("=== Slack Analytics Example ===\n")

    # Create user (triggers Slack notification)
    print("1. Creating user...")
    user = await create_user({"email": "test@example.com"})
    print(f"   Created: {user}\n")

    # Process order (triggers Slack notification)
    print("2. Processing order...")
    order = await process_order("order-123", 99.99)
    print(f"   Processed: {order}\n")

    print("Check console output above for Slack messages and trace spans!")


if __name__ == "__main__":
    asyncio.run(main())
