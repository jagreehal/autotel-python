"""
Example demonstrating Event class usage with subscribers.

Event.trackEvent() sends product events to configured subscribers
(PostHog, Mixpanel, webhooks, etc.).

This is different from Metric.trackEvent() which sends to OTLP
for infrastructure monitoring.
"""

import asyncio
from typing import Any

from autotel import (
    ConsoleSpanExporter,
    EventSubscriber,
    SimpleSpanProcessor,
    init,
    trace,
    track,
)


class ConsoleSubscriber(EventSubscriber):
    """Simple subscriber that prints events to console."""

    async def send(self, event: str, properties: dict[str, Any]) -> None:
        """Print event to console."""
        print(f"\n📊 [Event Subscriber] {event}")
        print(f"   Properties: {properties}")

    async def shutdown(self) -> None:
        """No-op."""
        print("\n🔒 Event subscriber shutdown")


# Initialize autotel with Event subscribers
init(
    service="example-app",
    service_version="1.0.0",
    environment="development",
    span_processor=SimpleSpanProcessor(ConsoleSpanExporter()),
    subscribers=[ConsoleSubscriber()],
)


@trace
async def create_user(ctx, data: dict[str, Any]) -> None:
    """
    Create a user and track product event.

    Events are automatically enriched with:
    - trace_id, span_id, operation.name (from OpenTelemetry)
    - service.name, service.version, deployment.environment (from init)
    """
    ctx.set_attribute("user.email", data.get("email", ""))

    # Simulate user creation
    user = {"id": "user-123", "email": data.get("email", ""), "plan": "premium"}

    # Track product event - goes to subscribers (PostHog, Mixpanel, etc.)
    track("user.signup", {
        "user_id": user["id"],
        "email": user["email"],
        "plan": user["plan"],
    })

    return user


@trace
async def process_order(ctx, order_id: str, amount: float) -> None:
    """Process an order and track product events."""
    ctx.set_attribute("order.id", order_id)
    ctx.set_attribute("order.amount", amount)

    # Simulate order processing
    order = {"id": order_id, "amount": amount, "status": "completed"}

    # Track product event
    track("order.completed", {
        "order_id": order_id,
        "amount": amount,
        "currency": "USD",
        "payment_method": "credit_card",
    })

    return order


@trace
async def upgrade_subscription(ctx, user_id: str, from_plan: str, to_plan: str) -> None:
    """Track subscription upgrade event."""
    ctx.set_attribute("user.id", user_id)

    # Track product event for subscription change
    track("subscription.upgraded", {
        "user_id": user_id,
        "from_plan": from_plan,
        "to_plan": to_plan,
        "revenue_delta": 20.0,  # Additional revenue
    })

    return {"user_id": user_id, "plan": to_plan}


async def main() -> None:
    """Main function demonstrating event tracking."""
    print("=" * 60)
    print("Event Subscriber Example")
    print("=" * 60)
    print("\nDemonstrating Event.trackEvent() → Subscribers")
    print("(PostHog, Mixpanel, webhooks, etc.)\n")

    # Create user - triggers user.signup event
    user = await create_user({"email": "alice@example.com"})
    print(f"\n✓ Created user: {user['id']}")

    # Process order - triggers order.completed event
    order = await process_order("order-456", 99.99)
    print(f"\n✓ Processed order: {order['id']}")

    # Upgrade subscription - triggers subscription.upgraded event
    result = await upgrade_subscription("user-123", "basic", "premium")
    print(f"\n✓ Upgraded subscription to: {result['plan']}")

    print("\n" + "=" * 60)
    print("All events enriched with trace context automatically!")
    print("=" * 60)


if __name__ == "__main__":
    asyncio.run(main())
