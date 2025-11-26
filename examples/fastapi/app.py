"""FastAPI example with autotel."""

from typing import Any

from fastapi import FastAPI

from autotel.integrations.fastapi import autotelMiddleware

app = FastAPI()

# Add autotel middleware
app.add_middleware(autotelMiddleware, service="fastapi-example")


@app.get("/")
def read_root() -> None:
    """Root endpoint."""
    return {"message": "Hello World"}


@app.get("/users/{user_id}")
def get_user(user_id: int) -> None:
    """Get user by ID."""
    return {"user_id": user_id, "name": "John Doe"}


@app.post("/users")
def create_user(user_data: dict[str, Any]) -> None:
    """Create a new user."""
    return {"id": 123, **user_data}


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000)
