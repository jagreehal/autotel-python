"""Basic example of autotel usage."""

from typing import Any

from autotel import ConsoleSpanExporter, SimpleSpanProcessor, init, trace

# Initialize autotel with console exporter for verification
init(
    service="example-app",
    span_processor=SimpleSpanProcessor(ConsoleSpanExporter()),
)


@trace
async def get_user(user_id: str) -> None:
    """Simple traced function."""
    # Simulate database call
    return {"id": user_id, "name": "John Doe"}


@trace
async def create_user(ctx, data: dict[str, Any]) -> None:
    """Traced function with context parameter."""
    ctx.set_attribute("user.email", data.get("email", ""))
    ctx.set_attribute("user.id", data.get("id", ""))
    # Simulate database call
    return {"id": data.get("id"), "email": data.get("email")}


async def main() -> None:
    """Main function demonstrating autotel usage."""
    # Simple tracing
    user = await get_user("123")
    print(f"Got user: {user}")

    # Tracing with context
    new_user = await create_user({"id": "456", "email": "test@example.com"})
    print(f"Created user: {new_user}")


if __name__ == "__main__":
    import asyncio

    asyncio.run(main())
