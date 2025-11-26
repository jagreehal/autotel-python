"""Example demonstrating webhook event subscriber."""

import asyncio
from typing import Any

from autotel import (
    ConsoleSpanExporter,
    SimpleSpanProcessor,
    init,
    trace,
    track,
)

# Mock webhook subscriber for demonstration (use real webhook URL in production)
from autotel.subscribers import WebhookSubscriber


class MockWebhookSubscriber(WebhookSubscriber):
    """Mock webhook subscriber that prints payloads instead of sending HTTP requests."""

    def __init__(self, webhook_url: str = "https://hooks.example.com/webhook", headers: dict | None = None):
        """Initialize mock subscriber."""
        # Don't call super().__init__ to avoid requiring webhook URL
        self.webhook_url = webhook_url
        self.headers = headers or {}

    async def send(self, event: str, properties: dict | None = None) -> None:
        """Print webhook payload instead of sending."""
        payload = {
            "event": event,
            "properties": properties or {},
        }
        print(f"[Webhook {self.webhook_url}] POST {payload}")

    async def shutdown(self) -> None:
        """No-op for mock."""


# Initialize autotel with webhook subscriber
init(
    service="webhook-example",
    span_processor=SimpleSpanProcessor(ConsoleSpanExporter()),
    subscribers=[
        MockWebhookSubscriber(
            webhook_url="https://hooks.zapier.com/hooks/catch/...",
            headers={"Authorization": "Bearer token"},
        )
    ],
)


@trace
async def create_user(ctx, data: dict[str, Any]) -> None:
    """Create a user and send event to webhook."""
    # Simulate user creation
    user = {"id": "123", "email": data.get("email", "")}

    ctx.set_attribute("user.id", user["id"])
    ctx.set_attribute("user.email", user["email"])

    # Track events event (will be sent to webhook)
    track("user_created", {
        "user_id": user["id"],
        "email": user["email"],
    })

    return user


@trace
async def process_order(ctx, order_id: str, amount: float) -> None:
    """Process an order and send event to webhook."""
    # Simulate order processing
    order = {"id": order_id, "amount": amount, "status": "completed"}

    ctx.set_attribute("order.id", order_id)
    ctx.set_attribute("order.amount", amount)

    # Track events event (will be sent to webhook)
    track("order_completed", {
        "order_id": order_id,
        "amount": amount,
        "currency": "USD",
    })

    return order


async def main() -> None:
    """Main function demonstrating webhook integration."""
    print("=== Webhook Analytics Example ===\n")

    # Create user (triggers webhook)
    print("1. Creating user...")
    user = await create_user({"email": "test@example.com"})
    print(f"   Created: {user}\n")

    # Process order (triggers webhook)
    print("2. Processing order...")
    order = await process_order("order-123", 99.99)
    print(f"   Processed: {order}\n")

    print("Check console output above for webhook payloads and trace spans!")


if __name__ == "__main__":
    asyncio.run(main())
