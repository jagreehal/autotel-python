"""
Semantic Convention Helpers Example

This example demonstrates how to use autotel's semantic convention helpers
to automatically add OpenTelemetry semantic convention attributes for common
operation types: LLM calls, database operations, HTTP requests, and messaging.
"""

import asyncio
from typing import Any

from autotel import init, trace_db, trace_http, trace_llm, trace_messaging

# Initialize autotel with console exporter for demonstration
init(service="semantic-helpers-demo")


# ============================================================================
# LLM/AI Operations
# ============================================================================


@trace_llm(model="gpt-4-turbo", operation="chat", system="openai")
async def generate_chat_response(ctx, prompt: str) -> None:
    """
    Example LLM operation with automatic Gen AI semantic conventions.

    Automatically adds:
    - gen.ai.request.model: "gpt-4-turbo"
    - gen.ai.operation.name: "chat"
    - gen.ai.system: "openai"
    """
    # Simulate LLM API call
    await asyncio.sleep(0.1)

    # Add additional metrics (token usage, etc.)
    ctx.set_attribute("gen.ai.usage.prompt_tokens", 50)
    ctx.set_attribute("gen.ai.usage.completion_tokens", 120)
    ctx.set_attribute("gen.ai.usage.total_tokens", 170)
    ctx.set_attribute("gen.ai.request.temperature", 0.7)
    ctx.set_attribute("gen.ai.request.max_tokens", 500)
    ctx.set_attribute("gen.ai.prompt.length", len(prompt))

    return "This is a simulated response from GPT-4"


@trace_llm(model="text-embedding-3-small", operation="embedding", system="openai")
async def embed_text(ctx, text: str) -> None:
    """
    Example embedding operation.

    Automatically adds:
    - gen.ai.request.model: "text-embedding-3-small"
    - gen.ai.operation.name: "embedding"
    - gen.ai.system: "openai"
    """
    # Simulate embedding API call
    await asyncio.sleep(0.05)

    ctx.set_attribute("gen.ai.usage.total_tokens", 8)
    ctx.set_attribute("gen.ai.response.dimensions", 1536)
    ctx.set_attribute("gen.ai.request.input_length", len(text))

    return [0.1, 0.2, 0.3]  # Simulated embedding vector


@trace_llm(model="claude-3-opus-20240229", operation="chat", system="anthropic")
async def generate_with_claude(ctx, messages: list) -> None:
    """
    Example Anthropic Claude operation.

    Automatically adds semantic conventions for Claude API calls.
    """
    await asyncio.sleep(0.15)

    ctx.set_attribute("gen.ai.usage.input_tokens", 100)
    ctx.set_attribute("gen.ai.usage.output_tokens", 250)
    ctx.set_attribute("gen.ai.request.max_tokens", 1000)
    ctx.set_attribute("gen.ai.request.message_count", len(messages))

    return "This is a simulated response from Claude"


# ============================================================================
# Database Operations
# ============================================================================


@trace_db(system="postgresql", operation="SELECT", db_name="production")
async def get_user_by_id(ctx, user_id: str) -> None:
    """
    Example PostgreSQL query.

    Automatically adds:
    - db.system: "postgresql"
    - db.operation: "SELECT"
    - db.name: "production"
    """
    # Simulate database query
    await asyncio.sleep(0.02)

    # Add query details (sanitized, no PII!)
    ctx.set_attribute("db.statement", "SELECT * FROM users WHERE id = $1")
    ctx.set_attribute("db.collection.name", "users")
    ctx.set_attribute("db.query.duration_ms", 15)

    return {"id": user_id, "name": "John Doe", "email": "john@example.com"}


@trace_db(
    system="mongodb",
    operation="find",
    db_name="app_db",
    collection="orders"
)
async def find_user_orders(ctx, user_id: str) -> None:
    """
    Example MongoDB query.

    Automatically adds:
    - db.system: "mongodb"
    - db.operation: "find"
    - db.name: "app_db"
    - db.collection.name: "orders"
    """
    # Simulate MongoDB query
    await asyncio.sleep(0.03)

    ctx.set_attribute("db.mongodb.filter", f'{{"user_id": "{user_id}"}}')
    ctx.set_attribute("db.mongodb.results_count", 5)

    return [
        {"order_id": "1", "total": 99.99},
        {"order_id": "2", "total": 149.99},
    ]


@trace_db(system="redis", operation="get")
async def get_from_cache(ctx, key: str) -> None:
    """
    Example Redis cache operation.

    Automatically adds:
    - db.system: "redis"
    - db.operation: "get"
    """
    # Simulate Redis get
    await asyncio.sleep(0.001)

    ctx.set_attribute("db.redis.key", key)
    ctx.set_attribute("cache.hit", True)

    return "cached_value"


# ============================================================================
# HTTP Client Operations
# ============================================================================


@trace_http(method="GET", url="https://api.github.com/users/{username}")
async def get_github_user(ctx, username: str) -> None:
    """
    Example HTTP GET request.

    Automatically adds:
    - http.request.method: "GET"
    - url.full: "https://api.github.com/users/{username}"
    """
    # Simulate HTTP request
    await asyncio.sleep(0.05)

    # Add response details
    ctx.set_attribute("http.response.status_code", 200)
    ctx.set_attribute("http.response.body.size", 1024)
    ctx.set_attribute("http.request.header.user_agent", "autotel-demo/1.0")

    return {"login": username, "id": 12345, "type": "User"}


@trace_http(method="POST", url="https://api.stripe.com/v1/charges")
async def create_stripe_charge(ctx, amount: int, currency: str) -> None:
    """
    Example HTTP POST request.

    Automatically adds:
    - http.request.method: "POST"
    - url.full: "https://api.stripe.com/v1/charges"
    """
    # Simulate Stripe API call
    await asyncio.sleep(0.1)

    ctx.set_attribute("http.response.status_code", 201)
    ctx.set_attribute("http.request.body.size", 256)
    ctx.set_attribute("payment.amount", amount)
    ctx.set_attribute("payment.currency", currency)

    return {"id": "ch_123456", "amount": amount, "status": "succeeded"}


# ============================================================================
# Messaging Operations
# ============================================================================


@trace_messaging(
    system="kafka",
    operation="publish",
    destination="order-events"
)
async def publish_order_event(ctx, order_id: str, event_type: str, data: dict[str, Any]) -> None:
    """
    Example Kafka publish operation.

    Automatically adds:
    - messaging.system: "kafka"
    - messaging.operation: "publish"
    - messaging.destination.name: "order-events"
    """
    # Simulate Kafka publish
    await asyncio.sleep(0.02)

    ctx.set_attribute("messaging.message.id", order_id)
    ctx.set_attribute("messaging.kafka.partition", 2)
    ctx.set_attribute("messaging.message.payload_size_bytes", len(str(data)))
    ctx.set_attribute("event.type", event_type)

    return {"offset": 12345, "partition": 2}


@trace_messaging(
    system="rabbitmq",
    operation="receive",
    destination="notifications"
)
async def consume_notification(ctx, message_id: str) -> None:
    """
    Example RabbitMQ receive operation.

    Automatically adds:
    - messaging.system: "rabbitmq"
    - messaging.operation: "receive"
    - messaging.destination.name: "notifications"
    """
    # Simulate message processing
    await asyncio.sleep(0.015)

    ctx.set_attribute("messaging.message.id", message_id)
    ctx.set_attribute("messaging.rabbitmq.routing_key", "notification.email")
    ctx.set_attribute("messaging.message.conversation_id", "conv_123")

    return {"processed": True}


@trace_messaging(
    system="sqs",
    operation="process",
    destination="task-queue"
)
async def process_task(ctx, task_id: str, task_data: dict[str, Any]) -> None:
    """
    Example SQS message processing.

    Automatically adds:
    - messaging.system: "sqs"
    - messaging.operation: "process"
    - messaging.destination.name: "task-queue"
    """
    # Simulate task processing
    await asyncio.sleep(0.05)

    ctx.set_attribute("messaging.message.id", task_id)
    ctx.set_attribute("task.type", task_data.get("type", "unknown"))
    ctx.set_attribute("messaging.message.payload_size_bytes", len(str(task_data)))

    return {"status": "completed", "result": "success"}


# ============================================================================
# Complex Example: Combining Multiple Operations
# ============================================================================


@trace_llm(model="gpt-4-turbo", operation="chat", system="openai")
async def process_user_request(ctx, user_id: str, prompt: str) -> None:
    """
    Complex example combining multiple traced operations.

    This demonstrates how semantic helpers compose naturally with
    other traced functions.
    """
    # 1. Get user from database
    user = await get_user_by_id(user_id)
    ctx.set_attribute("user.tier", user.get("tier", "premium"))
    ctx.set_attribute("user.email", user.get("email", ""))

    # 2. Check cache for similar prompts
    cache_key = f"prompt:{hash(prompt)}"
    cached_response = await get_from_cache(cache_key)

    if cached_response:
        ctx.add_event("cache_hit", {"key": cache_key})
        return cached_response

    # 3. Generate LLM response (this is traced by the parent decorator)
    ctx.set_attribute("gen.ai.usage.prompt_tokens", len(prompt.split()))
    response = f"AI response for: {prompt}"

    # 4. Publish event
    await publish_order_event(
        "evt_123",
        "ai_completion",
        {"user_id": user_id, "prompt_length": len(prompt)}
    )

    return response


# ============================================================================
# Main Demo
# ============================================================================


async def main() -> None:
    """Run all examples to demonstrate semantic convention helpers."""

    print("🚀 Semantic Convention Helpers Demo\n")

    # LLM Operations
    print("📊 LLM Operations:")
    response = await generate_chat_response("What is the weather today?")
    print(f"  ✓ Chat response: {response[:50]}...")

    embedding = await embed_text("Hello, world!")
    print(f"  ✓ Embedding: {embedding}")

    claude_response = await generate_with_claude([{"role": "user", "content": "Hi"}])
    print(f"  ✓ Claude response: {claude_response[:50]}...\n")

    # Database Operations
    print("💾 Database Operations:")
    user = await get_user_by_id("user_123")
    print(f"  ✓ User: {user['name']}")

    orders = await find_user_orders("user_123")
    print(f"  ✓ Orders: {len(orders)} found")

    cached = await get_from_cache("user:123:profile")
    print(f"  ✓ Cache: {cached}\n")

    # HTTP Operations
    print("🌐 HTTP Operations:")
    github_user = await get_github_user("octocat")
    print(f"  ✓ GitHub user: {github_user['login']}")

    charge = await create_stripe_charge(5000, "usd")
    print(f"  ✓ Stripe charge: {charge['id']}\n")

    # Messaging Operations
    print("📨 Messaging Operations:")
    kafka_result = await publish_order_event(
        "order_456",
        "order.created",
        {"total": 99.99}
    )
    print(f"  ✓ Kafka publish: offset {kafka_result['offset']}")

    notification = await consume_notification("msg_789")
    print(f"  ✓ RabbitMQ consume: processed={notification['processed']}")

    task = await process_task("task_101", {"type": "email", "to": "user@example.com"})
    print(f"  ✓ SQS process: {task['status']}\n")

    # Complex Example
    print("🔗 Complex Operation (combining multiple traces):")
    final_response = await process_user_request("user_123", "Explain quantum computing")
    print(f"  ✓ Final response: {final_response[:50]}...\n")

    print("✅ Demo complete! Check your trace backend for spans with semantic conventions.")


if __name__ == "__main__":
    asyncio.run(main())
