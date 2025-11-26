"""Tests for semantic convention helpers."""

from typing import Any

import pytest
from opentelemetry.sdk.trace.export import SimpleSpanProcessor

from autotel import init, trace_db, trace_http, trace_llm, trace_messaging
from autotel.exporters import InMemorySpanExporter


@pytest.fixture
def setup_tracing() -> Any:
    """Setup test tracing."""
    exporter = InMemorySpanExporter()
    processor = SimpleSpanProcessor(exporter)

    init(service="test", span_processor=processor)
    yield exporter
    exporter.clear()


# ============================================================================
# trace_llm tests
# ============================================================================


def test_trace_llm_basic(setup_tracing: Any) -> None:
    """Test trace_llm adds semantic convention attributes."""
    exporter = setup_tracing

    @trace_llm(model="gpt-4-turbo", operation="chat", system="openai")
    def generate_response(prompt: str) -> str:
        return f"Response to: {prompt}"

    result = generate_response("test prompt")

    assert result == "Response to: test prompt"
    spans = exporter.get_finished_spans()
    assert len(spans) == 1

    attrs = spans[0].attributes
    assert attrs["gen.ai.request.model"] == "gpt-4-turbo"
    assert attrs["gen.ai.operation.name"] == "chat"
    assert attrs["gen.ai.system"] == "openai"


def test_trace_llm_with_ctx(setup_tracing: Any) -> None:
    """Test trace_llm with ctx parameter."""
    exporter = setup_tracing

    @trace_llm(model="claude-3-opus", operation="chat", system="anthropic")
    def generate_with_ctx(ctx: Any, prompt: str) -> str:
        ctx.set_attribute("gen.ai.usage.input_tokens", 100)
        ctx.set_attribute("gen.ai.usage.output_tokens", 250)
        ctx.set_attribute("gen.ai.prompt.length", len(prompt))
        return "Response"

    result = generate_with_ctx("test")  # type: ignore[call-arg]

    assert result == "Response"
    spans = exporter.get_finished_spans()
    assert len(spans) == 1

    attrs = spans[0].attributes
    assert attrs["gen.ai.request.model"] == "claude-3-opus"
    assert attrs["gen.ai.operation.name"] == "chat"
    assert attrs["gen.ai.system"] == "anthropic"
    assert attrs["gen.ai.usage.input_tokens"] == 100
    assert attrs["gen.ai.usage.output_tokens"] == 250


@pytest.mark.asyncio
async def test_trace_llm_async(setup_tracing: Any) -> None:
    """Test trace_llm with async functions."""
    exporter = setup_tracing

    @trace_llm(model="gpt-4-turbo", operation="embedding", system="openai")
    async def embed_text(ctx: Any, text: str) -> list[float]:
        ctx.set_attribute("gen.ai.response.dimensions", 1536)
        ctx.set_attribute("gen.ai.request.input_length", len(text))
        return [0.1, 0.2, 0.3]

    result = await embed_text("test")  # type: ignore[call-arg]

    assert result == [0.1, 0.2, 0.3]
    spans = exporter.get_finished_spans()
    assert len(spans) == 1

    attrs = spans[0].attributes
    assert attrs["gen.ai.request.model"] == "gpt-4-turbo"
    assert attrs["gen.ai.operation.name"] == "embedding"
    assert attrs["gen.ai.response.dimensions"] == 1536


def test_trace_llm_with_custom_attributes(setup_tracing: Any) -> None:
    """Test trace_llm with additional custom attributes."""
    exporter = setup_tracing

    @trace_llm(
        model="gpt-4-turbo",
        operation="chat",
        system="openai",
        attributes={"custom.key": "custom_value", "priority": 1},
    )
    def generate(prompt: str) -> Any:
        return f"Response to {prompt}"

    generate("test")

    spans = exporter.get_finished_spans()
    assert len(spans) == 1

    attrs = spans[0].attributes
    assert attrs["gen.ai.request.model"] == "gpt-4-turbo"
    assert attrs["custom.key"] == "custom_value"
    assert attrs["priority"] == 1


def test_trace_llm_without_system(setup_tracing: Any) -> None:
    """Test trace_llm without system parameter."""
    exporter = setup_tracing

    @trace_llm(model="custom-model", operation="completion")
    def generate(prompt: str) -> str:
        return f"Response to {prompt}"

    generate("test")

    spans = exporter.get_finished_spans()
    assert len(spans) == 1

    attrs = spans[0].attributes
    assert attrs["gen.ai.request.model"] == "custom-model"
    assert attrs["gen.ai.operation.name"] == "completion"
    assert "gen.ai.system" not in attrs  # Should not be set


def test_trace_llm_custom_name(setup_tracing: Any) -> None:
    """Test trace_llm with custom span name."""
    exporter = setup_tracing

    @trace_llm(model="gpt-4", operation="chat", name="custom.operation")
    def generate(prompt: str) -> str:
        return f"Response to {prompt}"

    generate("test")

    spans = exporter.get_finished_spans()
    assert len(spans) == 1
    assert spans[0].name == "custom.operation"


# ============================================================================
# trace_db tests
# ============================================================================


def test_trace_db_basic(setup_tracing: Any) -> None:
    """Test trace_db adds semantic convention attributes."""
    exporter = setup_tracing

    @trace_db(system="postgresql", operation="SELECT", db_name="production")
    def query_user(user_id: str) -> dict[str, str]:
        return {"id": user_id, "name": "John"}

    result = query_user("123")

    assert result is not None
    assert result.get("id") == "123"
    spans = exporter.get_finished_spans()
    assert len(spans) == 1

    attrs = spans[0].attributes
    assert attrs["db.system"] == "postgresql"
    assert attrs["db.operation"] == "SELECT"
    assert attrs["db.name"] == "production"


def test_trace_db_with_collection(setup_tracing: Any) -> None:
    """Test trace_db with collection parameter."""
    exporter = setup_tracing

    @trace_db(system="mongodb", operation="find", db_name="app_db", collection="users")
    def find_users(ctx: Any, query: dict[str, Any]) -> Any:
        ctx.set_attribute("db.mongodb.filter", str(query))
        return [{"id": "1"}]

    result = find_users({"active": True})  # type: ignore[call-arg]

    assert result is not None
    assert len(result) == 1
    spans = exporter.get_finished_spans()
    assert len(spans) == 1

    attrs = spans[0].attributes
    assert attrs["db.system"] == "mongodb"
    assert attrs["db.operation"] == "find"
    assert attrs["db.name"] == "app_db"
    assert attrs["db.collection.name"] == "users"


@pytest.mark.asyncio
async def test_trace_db_async(setup_tracing: Any) -> None:
    """Test trace_db with async functions."""
    exporter = setup_tracing

    @trace_db(system="redis", operation="get")
    async def get_cache(ctx: Any, key: str) -> str:
        ctx.set_attribute("db.redis.key", key)
        ctx.set_attribute("cache.hit", True)
        return "cached_value"

    result = await get_cache("user:123")  # type: ignore[call-arg]

    assert result == "cached_value"
    spans = exporter.get_finished_spans()
    assert len(spans) == 1
    attrs = spans[0].attributes
    assert attrs["db.system"] == "redis"
    assert attrs["db.operation"] == "get"
    assert attrs["db.redis.key"] == "user:123"
    assert attrs["cache.hit"] is True


def test_trace_db_minimal(setup_tracing: Any) -> None:
    """Test trace_db with minimal parameters."""
    exporter = setup_tracing

    @trace_db(system="sqlite")
    def query_data() -> list[Any]:
        return []

    query_data()

    spans = exporter.get_finished_spans()
    assert len(spans) == 1

    attrs = spans[0].attributes
    assert attrs["db.system"] == "sqlite"
    assert "db.operation" not in attrs
    assert "db.name" not in attrs


# ============================================================================
# trace_http tests
# ============================================================================


def test_trace_http_basic(setup_tracing: Any) -> None:
    """Test trace_http adds semantic convention attributes."""
    exporter = setup_tracing

    @trace_http(method="GET", url="https://api.example.com/users")
    def fetch_users() -> list[dict[str, str]]:
        return [{"id": "1"}]

    result = fetch_users()

    assert result is not None
    assert result is not None
    assert len(result) == 1
    spans = exporter.get_finished_spans()
    assert len(spans) == 1

    attrs = spans[0].attributes
    assert attrs["http.request.method"] == "GET"
    assert attrs["url.full"] == "https://api.example.com/users"


def test_trace_http_with_ctx(setup_tracing: Any) -> None:
    """Test trace_http with ctx parameter."""
    exporter = setup_tracing

    @trace_http(method="POST", url="https://api.example.com/users")
    def create_user(ctx: Any, data: dict[str, Any]) -> Any:
        ctx.set_attribute("http.response.status_code", 201)
        ctx.set_attribute("http.request.body.size", 256)
        ctx.set_attribute("user.name", data.get("name", ""))
        return {"id": "new_user"}

    result = create_user({"name": "John"})  # type: ignore[call-arg]

    assert result is not None
    assert result.get("id") == "new_user"
    spans = exporter.get_finished_spans()
    assert len(spans) == 1

    attrs = spans[0].attributes
    assert attrs["http.request.method"] == "POST"
    assert attrs["http.response.status_code"] == 201
    assert attrs["http.request.body.size"] == 256


@pytest.mark.asyncio
async def test_trace_http_async(setup_tracing: Any) -> None:
    """Test trace_http with async functions."""
    exporter = setup_tracing

    @trace_http(method="GET", url="https://api.github.com/users/{username}")
    async def get_github_user(ctx: Any, username: str) -> dict[str, str]:
        ctx.set_attribute("http.response.status_code", 200)
        return {"login": username}

    result = await get_github_user("octocat")  # type: ignore[call-arg]

    assert result is not None
    assert result.get("login") == "octocat"
    spans = exporter.get_finished_spans()
    assert len(spans) == 1

    attrs = spans[0].attributes
    assert attrs["http.request.method"] == "GET"
    assert attrs["url.full"] == "https://api.github.com/users/{username}"
    assert attrs["http.response.status_code"] == 200


def test_trace_http_partial(setup_tracing: Any) -> None:
    """Test trace_http with only method or url."""
    exporter = setup_tracing

    @trace_http(method="GET")
    def fetch_data() -> dict[str, Any]:
        return {}

    fetch_data()

    spans = exporter.get_finished_spans()
    assert len(spans) == 1

    attrs = spans[0].attributes
    assert attrs["http.request.method"] == "GET"
    assert "url.full" not in attrs


# ============================================================================
# trace_messaging tests
# ============================================================================


def test_trace_messaging_basic(setup_tracing: Any) -> None:
    """Test trace_messaging adds semantic convention attributes."""
    exporter = setup_tracing

    @trace_messaging(system="kafka", operation="publish", destination="events")
    def publish_event(event_id: str) -> dict[str, int | str]:
        return {"offset": 123, "event_id": event_id}

    result = publish_event("evt_1")

    assert result is not None
    assert result.get("offset") == 123
    spans = exporter.get_finished_spans()
    assert len(spans) == 1

    attrs = spans[0].attributes
    assert attrs["messaging.system"] == "kafka"
    assert attrs["messaging.operation"] == "publish"
    assert attrs["messaging.destination.name"] == "events"


def test_trace_messaging_with_ctx(setup_tracing: Any) -> None:
    """Test trace_messaging with ctx parameter."""
    exporter = setup_tracing

    @trace_messaging(system="rabbitmq", operation="receive", destination="notifications")
    def consume_message(ctx: Any, message_id: str) -> dict[str, bool]:
        ctx.set_attribute("messaging.message.id", message_id)
        ctx.set_attribute("messaging.rabbitmq.routing_key", "notification.email")
        return {"processed": True}

    result = consume_message("msg_123")  # type: ignore[call-arg]

    assert result is not None
    assert result.get("processed") is True
    spans = exporter.get_finished_spans()
    assert len(spans) == 1

    attrs = spans[0].attributes
    assert attrs["messaging.system"] == "rabbitmq"
    assert attrs["messaging.operation"] == "receive"
    assert attrs["messaging.destination.name"] == "notifications"
    assert attrs["messaging.message.id"] == "msg_123"


@pytest.mark.asyncio
async def test_trace_messaging_async(setup_tracing: Any) -> None:
    """Test trace_messaging with async functions."""
    exporter = setup_tracing

    @trace_messaging(system="sqs", operation="process", destination="tasks")
    async def process_task(ctx: Any, task_id: str) -> dict[str, str]:
        ctx.set_attribute("messaging.message.id", task_id)
        ctx.set_attribute("messaging.message.payload_size_bytes", 512)
        return {"status": "completed"}

    result = await process_task("task_456")  # type: ignore[call-arg]

    assert result is not None
    assert result.get("status") == "completed"
    spans = exporter.get_finished_spans()
    assert len(spans) == 1

    attrs = spans[0].attributes
    assert attrs["messaging.system"] == "sqs"
    assert attrs["messaging.operation"] == "process"
    assert attrs["messaging.message.id"] == "task_456"


def test_trace_messaging_minimal(setup_tracing: Any) -> None:
    """Test trace_messaging with minimal parameters."""
    exporter = setup_tracing

    @trace_messaging(system="redis_streams")
    def publish_event() -> Any:
        return {}

    publish_event()

    spans = exporter.get_finished_spans()
    assert len(spans) == 1

    attrs = spans[0].attributes
    assert attrs["messaging.system"] == "redis_streams"
    assert "messaging.operation" not in attrs
    assert "messaging.destination.name" not in attrs


# ============================================================================
# Error handling tests
# ============================================================================


def test_semantic_helper_exception_handling(setup_tracing: Any) -> None:
    """Test that exceptions are properly recorded."""
    exporter = setup_tracing

    @trace_llm(model="gpt-4", operation="chat", system="openai")
    def failing_function() -> None:
        raise ValueError("Test error")

    with pytest.raises(ValueError, match="Test error"):
        failing_function()

    spans = exporter.get_finished_spans()
    assert len(spans) == 1

    # Verify exception was recorded
    span = spans[0]
    assert span.status.status_code.name == "ERROR"
    assert len(span.events) >= 1
    assert span.events[0].name == "exception"
