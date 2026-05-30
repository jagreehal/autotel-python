"""FastAPI example with autotel - demonstrating @trace with TraceContext.

This example shows how to use the @trace decorator with FastAPI endpoints.
The ctx parameter is automatically hidden from FastAPI's signature inspection,
so it won't appear as a query parameter.
"""

from typing import Any

from fastapi import FastAPI
from pydantic import BaseModel

from autotel import trace
from autotel.integrations.fastapi import autotelMiddleware

app = FastAPI()

# Add autotel middleware for automatic request tracing
app.add_middleware(autotelMiddleware, service="fastapi-example")


class UserCreate(BaseModel):
    """User creation request body."""

    name: str
    email: str


@app.get("/")
def read_root() -> dict[str, str]:
    """Root endpoint."""
    return {"message": "Hello World"}


@app.get("/users/{user_id}")
@trace
async def get_user(ctx: Any, user_id: int) -> dict[str, Any]:
    """Get user by ID with custom tracing.

    The ctx parameter is automatically injected by @trace and is NOT
    exposed as a FastAPI query parameter thanks to signature rewriting.
    """
    ctx.set_attribute("user.id", user_id)
    ctx.add_event("user_lookup_started")

    user = {"user_id": user_id, "name": "John Doe", "email": "john@example.com"}

    ctx.add_event("user_lookup_completed", {"found": True})
    return user


@app.post("/users")
@trace(name="create_user")
async def create_user(ctx: Any, user_data: UserCreate) -> dict[str, Any]:
    """Create a new user with detailed tracing.

    Demonstrates custom span name and TraceContext usage with Pydantic models.
    """
    ctx.set_attribute("user.email_domain", user_data.email.split("@")[1])
    ctx.set_attribute("user.name_length", len(user_data.name))

    new_user = {"id": 123, "name": user_data.name, "email": user_data.email}

    ctx.add_event("user_created", {"user_id": 123})
    return new_user


@app.get("/items/{item_id}")
@trace
def get_item(item_id: int, q: str | None = None) -> dict[str, Any]:
    """Get item - example WITHOUT ctx parameter.

    Shows that @trace works seamlessly with endpoints that don't need custom tracing.
    """
    return {"item_id": item_id, "query": q}


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000)
