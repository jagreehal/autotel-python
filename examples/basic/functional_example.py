"""Example demonstrating functional API usage."""

from autotel import (
    ConsoleSpanExporter,
    SimpleSpanProcessor,
    init,
    instrument,
    span,
    trace,
    with_new_context,
)

# Initialize autotel with console exporter for verification
init(
    service="example-app",
    span_processor=SimpleSpanProcessor(ConsoleSpanExporter()),
)


# Pattern 1: Decorator (standard)
@trace
async def get_user(user_id: str) -> None:
    """Simple traced function."""
    return {"id": user_id, "name": "John Doe"}


# Pattern 2: Batch instrumentation
def create_user(ctx, data) -> None:
    ctx.set_attribute("user.id", data.get("id", ""))
    return {"id": data.get("id"), "email": data.get("email")}

def update_user(ctx, user_id, data) -> None:
    ctx.set_attribute("user.id", user_id)
    return {"id": user_id, **data}

user_service = instrument({
    "create": create_user,
    "get": lambda user_id: {"id": user_id, "name": "John Doe"},
    "update": update_user,
})


# Pattern 3: Manual span creation
async def complex_operation() -> None:
    """Demonstrate manual span creation."""
    with span("database.query") as ctx:
        ctx.set_attribute("query.type", "SELECT")
        # Simulate database query
        results = [{"id": "1"}, {"id": "2"}]

    with span("processing") as ctx:
        ctx.set_attribute("items.count", len(results))
        return [r["id"] for r in results]


# Pattern 4: Root context isolation
def background_worker() -> None:
    """Demonstrate root context isolation."""
    # This creates a new root trace, not child of current
    with with_new_context(), span("background.job") as ctx:
        ctx.set_attribute("job.type", "scheduled")
        return "job completed"


async def main() -> None:
    """Main function demonstrating functional API."""
    # Standard decorator
    user = await get_user("123")
    print(f"Got user: {user}")

    # Batch instrumentation
    new_user = user_service["create"]({"id": "456", "email": "test@example.com"})
    print(f"Created user: {new_user}")

    fetched_user = user_service["get"]("789")
    print(f"Fetched user: {fetched_user}")

    # Manual spans
    processed = await complex_operation()
    print(f"Processed: {processed}")

    # Root context
    result = background_worker()
    print(f"Background job: {result}")


if __name__ == "__main__":
    import asyncio

    asyncio.run(main())
