#!/usr/bin/env python3
"""Test script to verify all examples work correctly using in-memory exporters."""

import asyncio
import sys
from pathlib import Path
from typing import Any

# Get project root (parent of scripts directory)
PROJECT_ROOT = Path(__file__).parent.parent

# Add src to path
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from autotel import init, span, trace, track  # noqa: E402
from autotel.exporters import InMemorySpanExporter  # noqa: E402
from autotel.processors import SimpleSpanProcessor  # noqa: E402
from autotel.testing import (  # noqa: E402
    assert_no_errors,
    assert_trace_succeeded,
)


def test_basic_example() -> None:
    """Test basic example functionality."""
    print("Testing basic example...")

    exporter = InMemorySpanExporter()
    init(
        service="test-basic",
        span_processor=SimpleSpanProcessor(exporter),
    )

    @trace
    async def get_user(user_id: str):
        """Simple traced function."""
        return {"id": user_id, "name": "John Doe"}

    @trace
    async def create_user(ctx, data: dict[str, Any]):
        """Traced function with context parameter."""
        ctx.set_attribute("user.email", data.get("email", ""))
        ctx.set_attribute("user.id", data.get("id", ""))
        return {"id": data.get("id"), "email": data.get("email")}

    async def run_test():
        user = await get_user("123")
        new_user = await create_user({"id": "456", "email": "test@example.com"})
        return user, new_user

    asyncio.run(run_test())

    spans = exporter.get_finished_spans()
    assert len(spans) >= 2, f"Expected at least 2 spans, got {len(spans)}"
    assert_trace_succeeded(exporter, "get_user")
    assert_trace_succeeded(exporter, "create_user")
    assert_no_errors(exporter)
    print("✅ Basic example works!\n")


def test_functional_example() -> None:
    """Test functional API example."""
    print("Testing functional example...")

    exporter = InMemorySpanExporter()
    init(
        service="test-functional",
        span_processor=SimpleSpanProcessor(exporter),
    )

    from autotel import instrument, with_new_context

    @trace
    async def get_user(user_id: str):
        return {"id": user_id, "name": "John Doe"}

    def create_user_func(ctx, data):
        ctx.set_attribute("user.id", data.get("id", ""))
        return {"id": data.get("id"), "email": data.get("email")}

    user_service = instrument({
        "create": create_user_func,
        "get": lambda user_id: {"id": user_id, "name": "John Doe"},
    })

    async def complex_operation():
        with span("database.query") as ctx:
            ctx.set_attribute("query.type", "SELECT")
            results = [{"id": "1"}, {"id": "2"}]
        with span("processing") as ctx:
            ctx.set_attribute("items.count", len(results))
            return [r["id"] for r in results]

    def background_worker():
        with with_new_context(), span("background.job") as ctx:
            ctx.set_attribute("job.type", "scheduled")
            return "job completed"

    async def run_test():
        await get_user("123")
        user_service["create"]({"id": "456", "email": "test@example.com"})
        user_service["get"]("789")
        await complex_operation()
        background_worker()
        return True

    asyncio.run(run_test())

    spans = exporter.get_finished_spans()
    assert len(spans) >= 5, f"Expected at least 5 spans, got {len(spans)}"
    assert_trace_succeeded(exporter, "get_user")
    assert_trace_succeeded(exporter, "create")
    assert_trace_succeeded(exporter, "database.query")
    assert_trace_succeeded(exporter, "background.job")
    assert_no_errors(exporter)
    print("✅ Functional example works!\n")


def test_events_example() -> None:
    """Test events example."""
    print("Testing events example...")

    exporter = InMemorySpanExporter()

    from autotel.subscribers import EventSubscriber

    events_received = []

    class MockEventSubscriber(EventSubscriber):
        async def send(self, event: str, properties: dict[str, Any]) -> None:
            events_received.append((event, properties))

        async def shutdown(self) -> None:
            pass

    init(
        service="test-events",
        span_processor=SimpleSpanProcessor(exporter),
        subscribers=[MockEventSubscriber()],
    )

    @trace
    async def create_user(ctx, data: dict[str, Any]):
        ctx.set_attribute("user.email", data.get("email", ""))
        track("user_created", {
            "user_id": "123",
            "email": data.get("email", ""),
        })
        return {"id": "123", "email": data.get("email", "")}

    @trace
    async def process_order(ctx, order_id: str, amount: float):
        ctx.set_attribute("order.id", order_id)
        ctx.set_attribute("order.amount", amount)
        track("order_completed", {
            "order_id": order_id,
            "amount": amount,
            "currency": "USD",
        })
        return {"id": order_id, "amount": amount, "status": "completed"}

    async def run_test():
        await create_user({"email": "test@example.com"})
        await process_order("order-123", 99.99)
        # Give events time to process
        await asyncio.sleep(0.2)
        return True

    asyncio.run(run_test())

    spans = exporter.get_finished_spans()
    assert len(spans) >= 2, f"Expected at least 2 spans, got {len(spans)}"
    assert_trace_succeeded(exporter, "create_user")
    assert_trace_succeeded(exporter, "process_order")
    assert_no_errors(exporter)

    # Check events events were received
    assert len(events_received) >= 2, f"Expected at least 2 events events, got {len(events_received)}"
    event_names = [e[0] for e in events_received]
    assert "user_created" in event_names, "user_created event not found"
    assert "order_completed" in event_names, "order_completed event not found"
    print("✅ Analytics example works!\n")


def test_complete_example() -> None:
    """Test complete example."""
    print("Testing complete example...")

    exporter = InMemorySpanExporter()
    init(
        service="test-complete",
        span_processor=SimpleSpanProcessor(exporter),
    )

    @trace
    async def create_user(ctx, data: dict[str, Any]):
        ctx.set_attribute("user.email", data.get("email", ""))
        ctx.set_attribute("user.id", data.get("id", ""))

        with span("database.insert") as db_ctx:
            db_ctx.set_attribute("db.table", "users")
            db_ctx.set_attribute("db.operation", "INSERT")
            await asyncio.sleep(0.01)  # Simulate DB call
            user = {"id": data.get("id", "123"), "email": data.get("email", "")}

        track("user_created", {
            "user_id": user["id"],
            "email": user["email"],
        })
        return user

    @trace
    async def process_order(ctx, order_id: str, amount: float):
        ctx.set_attribute("order.id", order_id)
        ctx.set_attribute("order.amount", amount)

        with span("payment.process") as payment_ctx:
            payment_ctx.set_attribute("payment.method", "credit_card")
            await asyncio.sleep(0.01)

        with span("inventory.update") as inventory_ctx:
            inventory_ctx.set_attribute("inventory.action", "deduct")
            await asyncio.sleep(0.01)

        track("order_completed", {
            "order_id": order_id,
            "amount": amount,
        })
        return {"order_id": order_id, "status": "completed"}

    async def run_test():
        await create_user({"id": "user-123", "email": "user@example.com"})
        await process_order("order-456", 99.99)
        await asyncio.sleep(0.2)  # Give events time
        return True

    asyncio.run(run_test())

    spans = exporter.get_finished_spans()
    assert len(spans) >= 5, f"Expected at least 5 spans, got {len(spans)}"
    assert_trace_succeeded(exporter, "create_user")
    assert_trace_succeeded(exporter, "database.insert")
    assert_trace_succeeded(exporter, "process_order")
    assert_trace_succeeded(exporter, "payment.process")
    assert_trace_succeeded(exporter, "inventory.update")
    assert_no_errors(exporter)
    print("✅ Complete example works!\n")


def test_shutdown_example() -> None:
    """Test shutdown example."""
    print("Testing shutdown example...")

    exporter = InMemorySpanExporter()
    init(
        service="test-shutdown",
        span_processor=SimpleSpanProcessor(exporter),
    )

    @trace
    async def process_data(data: dict[str, Any]):
        await asyncio.sleep(0.01)
        track("data_processed", {"size": len(data)})
        return {"processed": True, **data}

    async def run_test():
        for i in range(3):
            await process_data({"index": i})
        await asyncio.sleep(0.2)  # Give events time
        from autotel import shutdown
        await shutdown(timeout=1.0)
        return True

    asyncio.run(run_test())

    spans = exporter.get_finished_spans()
    assert len(spans) >= 3, f"Expected at least 3 spans, got {len(spans)}"
    assert_trace_succeeded(exporter, "process_data")
    assert_no_errors(exporter)
    print("✅ Shutdown example works!\n")


def main() -> None:
    """Run all example tests."""
    print("=" * 60)
    print("Testing autotel examples")
    print("=" * 60 + "\n")

    tests = [
        test_basic_example,
        test_functional_example,
        test_events_example,
        test_complete_example,
        test_shutdown_example,
    ]

    passed = 0
    failed = 0

    for test in tests:
        try:
            test()
            passed += 1
        except Exception as e:
            print(f"❌ {test.__name__} failed: {e}")
            import traceback
            traceback.print_exc()
            failed += 1
            print()

    print("=" * 60)
    print(f"Results: {passed} passed, {failed} failed")
    print("=" * 60)

    if failed > 0:
        sys.exit(1)


if __name__ == "__main__":
    main()


