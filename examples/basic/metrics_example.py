"""
Example demonstrating Metric class usage with OTLP.

Metric.trackEvent() sends OpenTelemetry counters to OTLP endpoint
Metric.trackValue() sends OpenTelemetry histograms to OTLP endpoint

This is different from Event.trackEvent() which sends to subscribers
(PostHog, Mixpanel, etc.) for product analytics.
"""

import asyncio
import time

from autotel import ConsoleSpanExporter, Metric, SimpleSpanProcessor, init, trace

# Initialize autotel with basic tracing
init(
    service="metrics-demo",
    service_version="1.0.0",
    environment="development",
    span_processor=SimpleSpanProcessor(ConsoleSpanExporter()),
)

# Create Metric instance for infrastructure monitoring
# This sends to OTLP endpoint (OpenTelemetry collector)
metrics = Metric(
    namespace="checkout",  # Metric namespace
    endpoint="http://localhost:4318",  # OTLP endpoint
    export_interval=60,  # Export every 60 seconds
)


@trace
async def process_payment(ctx, amount: float, currency: str) -> None:
    """Process payment and track infrastructure metrics."""
    start_time = time.time()

    ctx.set_attribute("payment.amount", amount)
    ctx.set_attribute("payment.currency", currency)

    # Simulate payment processing
    await asyncio.sleep(0.1)
    success = True  # Simulate success

    # Track event count as OpenTelemetry counter → OTLP
    if success:
        metrics.trackEvent("payment.success", {
            "currency": currency,
            "payment_method": "credit_card",
        })
    else:
        metrics.trackEvent("payment.failure", {
            "currency": currency,
            "error": "insufficient_funds",
        })

    # Track payment amount as OpenTelemetry histogram → OTLP
    metrics.trackValue("payment.amount", amount, {
        "currency": currency,
    })

    # Track processing duration as OpenTelemetry histogram → OTLP
    duration_ms = (time.time() - start_time) * 1000
    metrics.trackValue("payment.duration", duration_ms, {
        "success": str(success).lower(),
    })

    return {"success": success, "amount": amount, "currency": currency}


@trace
async def handle_api_request(ctx, endpoint: str) -> None:
    """Handle API request and track performance metrics."""
    start_time = time.time()

    ctx.set_attribute("http.endpoint", endpoint)

    # Simulate API processing
    await asyncio.sleep(0.05)

    # Track API request count as counter → OTLP
    metrics.trackEvent("api.request", {
        "endpoint": endpoint,
        "method": "POST",
        "status_code": "200",
    })

    # Track API latency as histogram → OTLP
    latency_ms = (time.time() - start_time) * 1000
    metrics.trackValue("api.latency", latency_ms, {
        "endpoint": endpoint,
    })

    return {"endpoint": endpoint, "latency_ms": latency_ms}


@trace
async def process_order(ctx, order_id: str, items_count: int) -> None:
    """Process order and track business metrics."""
    ctx.set_attribute("order.id", order_id)

    # Simulate order processing
    await asyncio.sleep(0.1)

    # Track order completion as counter → OTLP
    metrics.trackEvent("order.completed", {
        "order_id": order_id,
    })

    # Track order size as histogram → OTLP
    metrics.trackValue("order.items", float(items_count), {
        "order_type": "online",
    })

    # Track revenue as histogram → OTLP
    revenue = items_count * 29.99
    metrics.trackValue("revenue", revenue, {
        "currency": "USD",
    })

    return {"order_id": order_id, "items": items_count, "revenue": revenue}


async def main() -> None:
    """Main function demonstrating metric tracking."""
    print("=" * 60)
    print("Metric OTLP Example")
    print("=" * 60)
    print("\nDemonstrating Metric.trackEvent() → OTLP")
    print("(OpenTelemetry counters and histograms)")
    print("\nMetrics are exported to OTLP endpoint:")
    print("http://localhost:4318/v1/metrics\n")

    # Process payments
    print("\n📊 Processing payments...")
    for i, (amount, currency) in enumerate([
        (99.99, "USD"),
        (149.99, "USD"),
        (79.99, "EUR"),
    ], 1):
        result = await process_payment(amount, currency)
        print(f"  {i}. Payment {result['amount']} {result['currency']}: {'✓' if result['success'] else '✗'}")

    # Handle API requests
    print("\n📊 Handling API requests...")
    for i, endpoint in enumerate([
        "/api/users",
        "/api/orders",
        "/api/products",
    ], 1):
        result = await handle_api_request(endpoint)
        print(f"  {i}. {result['endpoint']}: {result['latency_ms']:.2f}ms")

    # Process orders
    print("\n📊 Processing orders...")
    for i, (order_id, items) in enumerate([
        ("order-1", 3),
        ("order-2", 5),
        ("order-3", 2),
    ], 1):
        result = await process_order(order_id, items)
        print(f"  {i}. {result['order_id']}: {result['items']} items, ${result['revenue']:.2f}")

    print("\n" + "=" * 60)
    print("Metrics Summary:")
    print("=" * 60)
    print("\nCounters (trackEvent):")
    print("  • checkout.payment.success")
    print("  • checkout.api.request")
    print("  • checkout.order.completed")
    print("\nHistograms (trackValue):")
    print("  • checkout.payment.amount")
    print("  • checkout.payment.duration")
    print("  • checkout.api.latency")
    print("  • checkout.order.items")
    print("  • checkout.revenue")
    print("\nAll metrics exported to OTLP with namespace 'checkout'")
    print("=" * 60)

    # Shutdown metrics
    await metrics.shutdown()
    print("\n✓ Metrics flushed and shutdown complete")


if __name__ == "__main__":
    asyncio.run(main())
