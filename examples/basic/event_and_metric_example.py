"""
Comprehensive example showing Event vs Metric usage.

This demonstrates the key architectural separation:

📊 Event.trackEvent() → Subscribers (PostHog, Mixpanel, etc.)
   - Product analytics
   - User behavior tracking
   - Business outcomes
   - Funnels and conversion

📈 Metric.trackEvent() → OTLP (OpenTelemetry)
   - Infrastructure monitoring
   - Performance metrics
   - System health
   - SLOs and dashboards

Both auto-enrich with trace context for correlation!
"""

import asyncio
import time
from typing import Any

from autotel import (
    ConsoleSpanExporter,
    EventSubscriber,
    Metric,
    SimpleSpanProcessor,
    init,
    trace,
    track,
)


class ConsoleSubscriber(EventSubscriber):
    """Subscriber that prints product events."""

    async def send(self, event: str, properties: dict[str, Any]) -> None:
        print(f"  📊 [Product Event → Subscriber] {event}")
        # Show a few key properties
        if "user_id" in properties:
            print(f"     user_id: {properties['user_id']}")
        if "amount" in properties:
            print(f"     amount: {properties['amount']}")

    async def shutdown(self) -> None:
        pass


# Initialize with both Event subscribers AND Metric tracking
init(
    service="ecommerce-api",
    service_version="2.1.0",
    environment="production",
    span_processor=SimpleSpanProcessor(ConsoleSpanExporter()),
    # Event subscribers for product analytics
    subscribers=[ConsoleSubscriber()],
)

# Metrics for infrastructure monitoring (OTLP)
metrics = Metric(namespace="ecommerce", endpoint="http://localhost:4318")


@trace
async def process_checkout(ctx, user_id: str, cart_total: float) -> None:
    """
    Process checkout - demonstrates both Event and Metric usage.

    Event: Track business outcome (conversion, revenue) → PostHog
    Metric: Track infrastructure (latency, success rate) → OTLP
    """
    start_time = time.time()

    ctx.set_attribute("user.id", user_id)
    ctx.set_attribute("cart.total", cart_total)

    # Simulate payment processing
    await asyncio.sleep(0.1)
    success = True

    # ===================================================================
    # PRODUCT EVENT → Subscribers (PostHog, Mixpanel)
    # ===================================================================
    # Track business outcome for product analytics
    track("checkout.completed", {
        "user_id": user_id,
        "amount": cart_total,
        "currency": "USD",
        "payment_method": "credit_card",
        # This goes to PostHog for funnel analysis, user segmentation
    })

    # ===================================================================
    # INFRASTRUCTURE METRICS → OTLP (OpenTelemetry)
    # ===================================================================
    # Track system performance for SRE/DevOps
    metrics.trackEvent("checkout.success", {
        "payment_method": "credit_card",
        # This goes to Prometheus/Grafana for uptime monitoring
    })

    # Track checkout latency for SLO monitoring
    duration_ms = (time.time() - start_time) * 1000
    metrics.trackValue("checkout.duration", duration_ms, {
        "success": str(success).lower(),
    })

    # Track revenue for business metrics dashboard
    metrics.trackValue("revenue", cart_total, {
        "currency": "USD",
    })

    return {
        "success": success,
        "user_id": user_id,
        "amount": cart_total,
        "duration_ms": duration_ms,
    }


@trace
async def create_account(ctx, email: str, plan: str) -> None:
    """
    Create user account.

    Event: Track signup for growth analytics → PostHog
    Metric: Track signup rate for monitoring → OTLP
    """
    start_time = time.time()

    ctx.set_attribute("user.email", email)
    ctx.set_attribute("user.plan", plan)

    user_id = f"user-{hash(email) % 10000}"

    # Simulate account creation
    await asyncio.sleep(0.05)

    # ===================================================================
    # PRODUCT EVENT → Subscribers
    # ===================================================================
    # Track for product team to analyze growth, activation, retention
    track("user.signup", {
        "user_id": user_id,
        "email": email,
        "plan": plan,
        "acquisition_channel": "organic",
    })

    # ===================================================================
    # INFRASTRUCTURE METRICS → OTLP
    # ===================================================================
    # Track for SRE team to monitor signup service health
    metrics.trackEvent("signup.success", {
        "plan": plan,
    })

    duration_ms = (time.time() - start_time) * 1000
    metrics.trackValue("signup.duration", duration_ms)

    return {"user_id": user_id, "email": email, "plan": plan}


@trace
async def upgrade_plan(ctx, user_id: str, from_plan: str, to_plan: str) -> None:
    """
    Upgrade subscription plan.

    Event: Track conversion for revenue analysis → PostHog
    Metric: Track upgrade rate for business metrics → OTLP
    """
    ctx.set_attribute("user.id", user_id)

    # Calculate revenue impact
    plan_prices = {"free": 0, "basic": 9.99, "premium": 29.99, "enterprise": 99.99}
    revenue_delta = plan_prices[to_plan] - plan_prices[from_plan]

    # ===================================================================
    # PRODUCT EVENT → Subscribers
    # ===================================================================
    # Track for product/marketing team to analyze conversion paths
    track("subscription.upgraded", {
        "user_id": user_id,
        "from_plan": from_plan,
        "to_plan": to_plan,
        "mrr_delta": revenue_delta,
    })

    # ===================================================================
    # INFRASTRUCTURE METRICS → OTLP
    # ===================================================================
    # Track for finance dashboard
    metrics.trackEvent("plan.upgrade", {
        "from": from_plan,
        "to": to_plan,
    })

    metrics.trackValue("mrr.delta", revenue_delta)

    return {"user_id": user_id, "new_plan": to_plan, "mrr_delta": revenue_delta}


async def main() -> None:
    """Main function demonstrating Event vs Metric usage."""
    print("=" * 70)
    print("Event vs Metric: Comprehensive Example")
    print("=" * 70)
    print("\n🎯 Architecture:")
    print("   Event.trackEvent()  → Subscribers (PostHog, Mixpanel)")
    print("   Metric.trackEvent() → OTLP (Prometheus, Grafana)")
    print("\n" + "=" * 70)

    # Scenario 1: User Signup
    print("\n1️⃣  User Signup")
    print("-" * 70)
    user = await create_account("alice@example.com", "premium")
    print(f"   ✓ Created: {user['user_id']} ({user['plan']} plan)")

    # Scenario 2: Checkout Flow
    print("\n2️⃣  Checkout")
    print("-" * 70)
    checkout = await process_checkout(user["user_id"], 149.99)
    print(f"   ✓ Processed: ${checkout['amount']} in {checkout['duration_ms']:.2f}ms")

    # Scenario 3: Plan Upgrade
    print("\n3️⃣  Plan Upgrade")
    print("-" * 70)
    upgrade = await upgrade_plan(user["user_id"], "premium", "enterprise")
    print(f"   ✓ Upgraded: {upgrade['new_plan']} (+${upgrade['mrr_delta']:.2f} MRR)")

    # Summary
    print("\n" + "=" * 70)
    print("📊 Event Summary (Product Analytics → PostHog)")
    print("=" * 70)
    print("   • user.signup           → Growth analysis, activation tracking")
    print("   • checkout.completed    → Conversion funnels, revenue attribution")
    print("   • subscription.upgraded → Upsell analysis, customer lifetime value")
    print()
    print("   All enriched with: trace_id, span_id, service.name, etc.")

    print("\n" + "=" * 70)
    print("📈 Metric Summary (Infrastructure → OTLP)")
    print("=" * 70)
    print("   Counters:")
    print("   • ecommerce.signup.success    → Signup rate SLO")
    print("   • ecommerce.checkout.success  → Checkout reliability")
    print("   • ecommerce.plan.upgrade      → Upgrade rate")
    print()
    print("   Histograms:")
    print("   • ecommerce.signup.duration   → Signup latency (p50, p99)")
    print("   • ecommerce.checkout.duration → Checkout performance")
    print("   • ecommerce.revenue           → Revenue distribution")
    print("   • ecommerce.mrr.delta         → MRR changes")
    print()
    print("   Exported to: http://localhost:4318/v1/metrics")

    print("\n" + "=" * 70)
    print("🎯 Key Takeaway")
    print("=" * 70)
    print("""
   Use Event for:  Product decisions (What features drive growth?)
   Use Metric for: Operational health (Is our system performing well?)

   Both share trace context for end-to-end correlation!
    """)
    print("=" * 70)

    # Cleanup
    await metrics.shutdown()


if __name__ == "__main__":
    asyncio.run(main())
