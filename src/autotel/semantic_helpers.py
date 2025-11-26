"""Semantic convention helpers for common instrumentation patterns.

These helpers provide pre-configured decorators that automatically add
OpenTelemetry semantic convention attributes for common operation types.
"""

import functools
import inspect
from collections.abc import Callable
from typing import Any, ParamSpec, TypeVar

from opentelemetry import trace as otel_trace
from opentelemetry.trace import StatusCode

from .context import TraceContext
from .operation_context import run_in_operation_context

P = ParamSpec("P")
R = TypeVar("R")


def trace_llm(
    model: str,
    operation: str = "chat",
    system: str | None = None,
    attributes: dict[str, Any] | None = None,
    name: str | None = None,
) -> Callable[[Callable[P, R]], Callable[P, R]]:
    """
    Trace LLM/AI operations with Gen AI semantic conventions.

    Automatically adds OpenTelemetry semantic convention attributes for
    generative AI operations. Use this to instrument calls to LLM providers
    like OpenAI, Anthropic, Cohere, etc.

    Semantic conventions automatically added:
    - gen.ai.request.model: The model identifier (e.g., "gpt-4-turbo", "claude-3-opus")
    - gen.ai.operation.name: The operation type (e.g., "chat", "completion", "embedding")
    - gen.ai.system: The AI system name (e.g., "openai", "anthropic")

    Args:
        model: Model identifier (e.g., "gpt-4-turbo", "claude-3-opus")
        operation: Operation type (default: "chat"). Common values:
            - "chat": Chat completion
            - "completion": Text completion
            - "embedding": Text embedding
            - "streaming": Streaming response
        system: AI system name (e.g., "openai", "anthropic", "cohere")
        attributes: Additional custom attributes to add to the span
        name: Optional custom span name (defaults to function name)

    Returns:
        Decorator function that wraps the target function with LLM tracing

    Example:
        >>> from autotel import trace_llm
        >>> import openai
        >>>
        >>> @trace_llm(model="gpt-4-turbo", operation="chat", system="openai")
        >>> async def generate_response(ctx, prompt: str):
        ...     response = await openai.chat.completions.create(
        ...         model="gpt-4-turbo",
        ...         messages=[{"role": "user", "content": prompt}]
        ...     )
        ...     # Add token usage metrics
        ...     ctx.set_attribute("gen.ai.usage.completion_tokens",
        ...                       response.usage.completion_tokens)
        ...     ctx.set_attribute("gen.ai.usage.prompt_tokens",
        ...                       response.usage.prompt_tokens)
        ...     return response.choices[0].message.content

        >>> # Without ctx parameter (attributes set automatically, no manual additions)
        >>> @trace_llm(model="text-embedding-3-small", operation="embedding", system="openai")
        >>> async def embed_text(text: str):
        ...     return await openai.embeddings.create(
        ...         model="text-embedding-3-small",
        ...         input=text
        ...     )

    Best Practices:
        - Add token usage with gen.ai.usage.* attributes
        - Add request/response metadata with gen.ai.request.* and gen.ai.response.*
        - Add cost tracking with gen.ai.usage.cost if available
        - Consider adding gen.ai.request.temperature, gen.ai.request.max_tokens, etc.
    """

    def decorator(func: Callable[P, R]) -> Callable[P, R]:
        # Detect if function expects ctx parameter
        sig = inspect.signature(func)
        params = list(sig.parameters.keys())
        needs_ctx = len(params) > 0 and params[0] in ("ctx", "context", "tracecontext")

        # Infer span name
        span_name = name or func.__name__

        if inspect.iscoroutinefunction(func):

            @functools.wraps(func)
            async def async_wrapper(*args: P.args, **kwargs: P.kwargs) -> R:
                tracer = otel_trace.get_tracer(__name__)
                with (
                    run_in_operation_context(span_name),
                    tracer.start_as_current_span(span_name) as span,
                ):
                    try:
                        # Add semantic convention attributes
                        span.set_attribute("gen.ai.request.model", model)
                        span.set_attribute("gen.ai.operation.name", operation)
                        if system:
                            span.set_attribute("gen.ai.system", system)

                        # Add custom attributes if provided
                        if attributes:
                            for key, value in attributes.items():
                                if isinstance(value, str | bool | int | float):
                                    span.set_attribute(key, value)
                                else:
                                    span.set_attribute(key, str(value))

                        if needs_ctx:
                            ctx = TraceContext(span)
                            result = await func(ctx, *args, **kwargs)  # type: ignore[arg-type]
                            return result  # type: ignore[no-any-return]
                        result = await func(*args, **kwargs)
                        return result  # type: ignore[no-any-return]
                    except Exception as e:
                        span.record_exception(e)
                        span.set_status(StatusCode.ERROR, str(e))
                        raise

            return async_wrapper  # type: ignore[return-value]
        else:

            @functools.wraps(func)
            def sync_wrapper(*args: P.args, **kwargs: P.kwargs) -> R:
                tracer = otel_trace.get_tracer(__name__)
                with (
                    run_in_operation_context(span_name),
                    tracer.start_as_current_span(span_name) as span,
                ):
                    try:
                        # Add semantic convention attributes
                        span.set_attribute("gen.ai.request.model", model)
                        span.set_attribute("gen.ai.operation.name", operation)
                        if system:
                            span.set_attribute("gen.ai.system", system)

                        # Add custom attributes if provided
                        if attributes:
                            for key, value in attributes.items():
                                if isinstance(value, str | bool | int | float):
                                    span.set_attribute(key, value)
                                else:
                                    span.set_attribute(key, str(value))

                        if needs_ctx:
                            ctx = TraceContext(span)
                            result = func(ctx, *args, **kwargs)  # type: ignore[arg-type]
                            return result
                        result = func(*args, **kwargs)
                        return result
                    except Exception as e:
                        span.record_exception(e)
                        span.set_status(StatusCode.ERROR, str(e))
                        raise

            return sync_wrapper

    return decorator


def trace_db(
    system: str,
    operation: str | None = None,
    db_name: str | None = None,
    collection: str | None = None,
    attributes: dict[str, Any] | None = None,
    name: str | None = None,
) -> Callable[[Callable[P, R]], Callable[P, R]]:
    """
    Trace database operations with DB semantic conventions.

    Automatically adds OpenTelemetry semantic convention attributes for
    database operations. Use this to instrument SQL queries, NoSQL operations,
    cache access, and ORM calls.

    Semantic conventions automatically added:
    - db.system: Database system identifier
    - db.operation: Operation type (e.g., "SELECT", "INSERT", "find", "get")
    - db.name: Database name
    - db.collection.name: Collection/table name (for NoSQL databases)

    Args:
        system: Database system (e.g., "postgresql", "mongodb", "redis", "mysql")
        operation: Operation type (e.g., "SELECT", "INSERT", "find", "get")
        db_name: Database name
        collection: Collection or table name (primarily for NoSQL)
        attributes: Additional custom attributes to add to the span
        name: Optional custom span name (defaults to function name)

    Returns:
        Decorator function that wraps the target function with DB tracing

    Example:
        >>> from autotel import trace_db
        >>> import asyncpg
        >>>
        >>> @trace_db(system="postgresql", operation="SELECT", db_name="production")
        >>> async def get_user(ctx, user_id: str):
        ...     conn = await asyncpg.connect(DATABASE_URL)
        ...     # Add query details
        ...     ctx.set_attribute("db.statement", "SELECT * FROM users WHERE id = $1")
        ...     ctx.set_attribute("db.collection.name", "users")
        ...     result = await conn.fetchrow("SELECT * FROM users WHERE id = $1", user_id)
        ...     await conn.close()
        ...     return result

        >>> # NoSQL example
        >>> @trace_db(
        ...     system="mongodb",
        ...     operation="find",
        ...     db_name="app_db",
        ...     collection="orders"
        ... )
        >>> async def find_orders(ctx, customer_id: str):
        ...     return await db.orders.find({"customer_id": customer_id}).to_list()

        >>> # Redis example
        >>> @trace_db(system="redis", operation="get")
        >>> async def get_cache(ctx, key: str):
        ...     ctx.set_attribute("db.redis.key", key)
        ...     return await redis.get(key)

    Best Practices:
        - Add db.statement for SQL queries (sanitized, no PII)
        - Add db.collection.name for table/collection names
        - Add db.redis.key for Redis operations
        - Add execution time metrics if available
        - Sanitize SQL/queries to remove sensitive data
    """

    def decorator(func: Callable[P, R]) -> Callable[P, R]:
        # Detect if function expects ctx parameter
        sig = inspect.signature(func)
        params = list(sig.parameters.keys())
        needs_ctx = len(params) > 0 and params[0] in ("ctx", "context", "tracecontext")

        # Infer span name
        span_name = name or func.__name__

        if inspect.iscoroutinefunction(func):

            @functools.wraps(func)
            async def async_wrapper(*args: P.args, **kwargs: P.kwargs) -> R:
                tracer = otel_trace.get_tracer(__name__)
                with (
                    run_in_operation_context(span_name),
                    tracer.start_as_current_span(span_name) as span,
                ):
                    try:
                        # Add semantic convention attributes
                        span.set_attribute("db.system", system)
                        if operation:
                            span.set_attribute("db.operation", operation)
                        if db_name:
                            span.set_attribute("db.name", db_name)
                        if collection:
                            span.set_attribute("db.collection.name", collection)

                        # Add custom attributes if provided
                        if attributes:
                            for key, value in attributes.items():
                                if isinstance(value, str | bool | int | float):
                                    span.set_attribute(key, value)
                                else:
                                    span.set_attribute(key, str(value))

                        if needs_ctx:
                            ctx = TraceContext(span)
                            result = await func(ctx, *args, **kwargs)  # type: ignore[arg-type]
                        return result  # type: ignore[no-any-return]
                        result = await func(*args, **kwargs)
                        return result
                    except Exception as e:
                        span.record_exception(e)
                        span.set_status(StatusCode.ERROR, str(e))
                        raise

            return async_wrapper  # type: ignore[return-value]
        else:

            @functools.wraps(func)
            def sync_wrapper(*args: P.args, **kwargs: P.kwargs) -> R:
                tracer = otel_trace.get_tracer(__name__)
                with (
                    run_in_operation_context(span_name),
                    tracer.start_as_current_span(span_name) as span,
                ):
                    try:
                        # Add semantic convention attributes
                        span.set_attribute("db.system", system)
                        if operation:
                            span.set_attribute("db.operation", operation)
                        if db_name:
                            span.set_attribute("db.name", db_name)
                        if collection:
                            span.set_attribute("db.collection.name", collection)

                        # Add custom attributes if provided
                        if attributes:
                            for key, value in attributes.items():
                                if isinstance(value, str | bool | int | float):
                                    span.set_attribute(key, value)
                                else:
                                    span.set_attribute(key, str(value))

                        if needs_ctx:
                            ctx = TraceContext(span)
                            result = func(ctx, *args, **kwargs)  # type: ignore[arg-type]
                            return result
                        result = func(*args, **kwargs)
                        return result
                    except Exception as e:
                        span.record_exception(e)
                        span.set_status(StatusCode.ERROR, str(e))
                        raise

            return sync_wrapper

    return decorator


def trace_http(
    method: str | None = None,
    url: str | None = None,
    attributes: dict[str, Any] | None = None,
    name: str | None = None,
) -> Callable[[Callable[P, R]], Callable[P, R]]:
    """
    Trace HTTP client operations with HTTP semantic conventions.

    Automatically adds OpenTelemetry semantic convention attributes for
    HTTP client requests. Use this to instrument API calls, microservice
    communication, and external HTTP requests.

    Semantic conventions automatically added:
    - http.request.method: HTTP method (GET, POST, etc.)
    - url.full: Full URL being requested

    Args:
        method: HTTP method (e.g., "GET", "POST", "PUT", "DELETE")
        url: Full URL or URL template (e.g., "https://api.example.com/users/{id}")
        attributes: Additional custom attributes to add to the span
        name: Optional custom span name (defaults to function name)

    Returns:
        Decorator function that wraps the target function with HTTP client tracing

    Example:
        >>> from autotel import trace_http
        >>> import httpx
        >>>
        >>> @trace_http(method="GET", url="https://api.github.com/users/{username}")
        >>> async def get_github_user(ctx, username: str):
        ...     url = f"https://api.github.com/users/{username}"
        ...     async with httpx.AsyncClient() as client:
        ...         response = await client.get(url)
        ...         # Add response details
        ...         ctx.set_attribute("http.response.status_code", response.status_code)
        ...         ctx.set_attribute("http.response.body.size", len(response.content))
        ...         response.raise_for_status()
        ...         return response.json()

        >>> # POST example
        >>> @trace_http(method="POST", url="https://api.stripe.com/v1/charges")
        >>> async def create_charge(ctx, data: dict[str, Any]):
        ...     async with httpx.AsyncClient() as client:
        ...         response = await client.post(
        ...             "https://api.stripe.com/v1/charges",
        ...             json=data
        ...         )
        ...         ctx.set_attribute("http.response.status_code", response.status_code)
        ...         return response.json()

    Best Practices:
        - Add http.response.status_code for the response status
        - Add http.request.body.size and http.response.body.size for payload sizes
        - Add url.full with the actual URL (can use templates in decorator)
        - Add http.route for the route template (e.g., "/users/{id}")
        - Avoid logging sensitive data in URLs (credentials, tokens, etc.)
    """

    def decorator(func: Callable[P, R]) -> Callable[P, R]:
        # Detect if function expects ctx parameter
        sig = inspect.signature(func)
        params = list(sig.parameters.keys())
        needs_ctx = len(params) > 0 and params[0] in ("ctx", "context", "tracecontext")

        # Infer span name
        span_name = name or func.__name__

        if inspect.iscoroutinefunction(func):

            @functools.wraps(func)
            async def async_wrapper(*args: P.args, **kwargs: P.kwargs) -> R:
                tracer = otel_trace.get_tracer(__name__)
                with (
                    run_in_operation_context(span_name),
                    tracer.start_as_current_span(span_name) as span,
                ):
                    try:
                        # Add semantic convention attributes
                        if method:
                            span.set_attribute("http.request.method", method)
                        if url:
                            span.set_attribute("url.full", url)

                        # Add custom attributes if provided
                        if attributes:
                            for key, value in attributes.items():
                                if isinstance(value, str | bool | int | float):
                                    span.set_attribute(key, value)
                                else:
                                    span.set_attribute(key, str(value))

                        if needs_ctx:
                            ctx = TraceContext(span)
                            result = await func(ctx, *args, **kwargs)  # type: ignore[arg-type]
                        return result  # type: ignore[no-any-return]
                        result = await func(*args, **kwargs)
                        return result
                    except Exception as e:
                        span.record_exception(e)
                        span.set_status(StatusCode.ERROR, str(e))
                        raise

            return async_wrapper  # type: ignore[return-value]
        else:

            @functools.wraps(func)
            def sync_wrapper(*args: P.args, **kwargs: P.kwargs) -> R:
                tracer = otel_trace.get_tracer(__name__)
                with (
                    run_in_operation_context(span_name),
                    tracer.start_as_current_span(span_name) as span,
                ):
                    try:
                        # Add semantic convention attributes
                        if method:
                            span.set_attribute("http.request.method", method)
                        if url:
                            span.set_attribute("url.full", url)

                        # Add custom attributes if provided
                        if attributes:
                            for key, value in attributes.items():
                                if isinstance(value, str | bool | int | float):
                                    span.set_attribute(key, value)
                                else:
                                    span.set_attribute(key, str(value))

                        if needs_ctx:
                            ctx = TraceContext(span)
                            result = func(ctx, *args, **kwargs)  # type: ignore[arg-type]
                            return result
                        result = func(*args, **kwargs)
                        return result
                    except Exception as e:
                        span.record_exception(e)
                        span.set_status(StatusCode.ERROR, str(e))
                        raise

            return sync_wrapper

    return decorator


def trace_messaging(
    system: str,
    operation: str | None = None,
    destination: str | None = None,
    attributes: dict[str, Any] | None = None,
    name: str | None = None,
) -> Callable[[Callable[P, R]], Callable[P, R]]:
    """
    Trace messaging operations with Messaging semantic conventions.

    Automatically adds OpenTelemetry semantic convention attributes for
    messaging system operations. Use this to instrument Kafka, RabbitMQ,
    SQS, and other message queue operations.

    Semantic conventions automatically added:
    - messaging.system: Messaging system identifier
    - messaging.operation: Operation type (publish, receive, process)
    - messaging.destination.name: Queue/topic name

    Args:
        system: Messaging system (e.g., "kafka", "rabbitmq", "sqs", "redis_streams")
        operation: Operation type. Common values:
            - "publish": Publishing a message
            - "receive": Receiving a message
            - "process": Processing a received message
            - "create": Creating a queue/topic
        destination: Queue, topic, or channel name
        attributes: Additional custom attributes to add to the span
        name: Optional custom span name (defaults to function name)

    Returns:
        Decorator function that wraps the target function with messaging tracing

    Example:
        >>> from autotel import trace_messaging
        >>> from kafka import KafkaProducer
        >>>
        >>> @trace_messaging(
        ...     system="kafka",
        ...     operation="publish",
        ...     destination="order-events"
        ... )
        >>> async def publish_order_event(ctx, order_id: str, event_data: dict[str, Any]):
        ...     producer = KafkaProducer(bootstrap_servers=['localhost:9092'])
        ...     # Add message details
        ...     ctx.set_attribute("messaging.message.id", order_id)
        ...     ctx.set_attribute("messaging.message.payload_size_bytes", len(str(event_data)))
        ...     producer.send('order-events', value=event_data)
        ...     producer.flush()

        >>> # Consumer example
        >>> @trace_messaging(
        ...     system="rabbitmq",
        ...     operation="receive",
        ...     destination="notifications"
        ... )
        >>> async def consume_notification(ctx, message):
        ...     ctx.set_attribute("messaging.message.id", message.message_id)
        ...     ctx.set_attribute("messaging.rabbitmq.routing_key", message.routing_key)
        ...     await process_notification(message.body)

        >>> # SQS example
        >>> @trace_messaging(
        ...     system="sqs",
        ...     operation="process",
        ...     destination="task-queue"
        ... )
        >>> async def process_task(ctx, message):
        ...     ctx.set_attribute("messaging.message.id", message['MessageId'])
        ...     await execute_task(message['Body'])

    Best Practices:
        - Add messaging.message.id for message identifiers
        - Add messaging.message.payload_size_bytes for message size
        - Add messaging.batch.message_count for batch operations
        - Add system-specific attributes (e.g., messaging.kafka.partition)
        - Add messaging.message.conversation_id for correlated messages
    """

    def decorator(func: Callable[P, R]) -> Callable[P, R]:
        # Detect if function expects ctx parameter
        sig = inspect.signature(func)
        params = list(sig.parameters.keys())
        needs_ctx = len(params) > 0 and params[0] in ("ctx", "context", "tracecontext")

        # Infer span name
        span_name = name or func.__name__

        if inspect.iscoroutinefunction(func):

            @functools.wraps(func)
            async def async_wrapper(*args: P.args, **kwargs: P.kwargs) -> R:
                tracer = otel_trace.get_tracer(__name__)
                with (
                    run_in_operation_context(span_name),
                    tracer.start_as_current_span(span_name) as span,
                ):
                    try:
                        # Add semantic convention attributes
                        span.set_attribute("messaging.system", system)
                        if operation:
                            span.set_attribute("messaging.operation", operation)
                        if destination:
                            span.set_attribute("messaging.destination.name", destination)

                        # Add custom attributes if provided
                        if attributes:
                            for key, value in attributes.items():
                                if isinstance(value, str | bool | int | float):
                                    span.set_attribute(key, value)
                                else:
                                    span.set_attribute(key, str(value))

                        if needs_ctx:
                            ctx = TraceContext(span)
                            result = await func(ctx, *args, **kwargs)  # type: ignore[arg-type]
                        return result  # type: ignore[no-any-return]
                        result = await func(*args, **kwargs)
                        return result
                    except Exception as e:
                        span.record_exception(e)
                        span.set_status(StatusCode.ERROR, str(e))
                        raise

            return async_wrapper  # type: ignore[return-value]
        else:

            @functools.wraps(func)
            def sync_wrapper(*args: P.args, **kwargs: P.kwargs) -> R:
                tracer = otel_trace.get_tracer(__name__)
                with (
                    run_in_operation_context(span_name),
                    tracer.start_as_current_span(span_name) as span,
                ):
                    try:
                        # Add semantic convention attributes
                        span.set_attribute("messaging.system", system)
                        if operation:
                            span.set_attribute("messaging.operation", operation)
                        if destination:
                            span.set_attribute("messaging.destination.name", destination)

                        # Add custom attributes if provided
                        if attributes:
                            for key, value in attributes.items():
                                if isinstance(value, str | bool | int | float):
                                    span.set_attribute(key, value)
                                else:
                                    span.set_attribute(key, str(value))

                        if needs_ctx:
                            ctx = TraceContext(span)
                            result = func(ctx, *args, **kwargs)  # type: ignore[arg-type]
                            return result
                        result = func(*args, **kwargs)
                        return result
                    except Exception as e:
                        span.record_exception(e)
                        span.set_status(StatusCode.ERROR, str(e))
                        raise

            return sync_wrapper

    return decorator
