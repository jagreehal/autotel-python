# autotel

Write One, Observe Everywhere: OpenTelemetry for Python.

- One-line initialization with `init()` and `@trace` decorator
- OTLP-first design with subscribers for PostHog, Slack, Webhook, and custom destinations
- Production features: adaptive sampling, rate limiting, circuit breakers, PII redaction
- Automatic enrichment: service metadata and trace context flow into spans, metrics, logs, and events

OpenTelemetry requires significant boilerplate. autotel provides a simpler API while maintaining full control over your telemetry.

```bash
pip install autotel
# or
uv add autotel
```

## Quick Start

### 1. Initialize once at startup

```python
from autotel import init

init(service='checkout-api')
```

**Configuration options:**
- Environment variables: `OTEL_SERVICE_NAME`, `OTEL_EXPORTER_OTLP_ENDPOINT`, etc.
- Explicit parameters override env vars
- Defaults to `http://localhost:4318`

### 2. Instrument code with `@trace`

```python
from autotel import trace

@trace
async def create_user(ctx, data: dict):
    ctx.set_attribute('user.email', data['email'])
    user = await db.users.create(data)
    return user
```

- `ctx` parameter is auto-detected for span operations
- Errors are recorded automatically
- Works with sync and async functions

### 3. Track product events

```python
import os

from autotel import init, track
from autotel.subscribers import PostHogSubscriber

init(
    service='checkout-api',
    subscribers=[PostHogSubscriber(api_key=os.environ["POSTHOG_KEY"])]
)

@trace
async def process_order(ctx, order):
    track('order.completed', {'amount': order.total})
    return await charge(order)
```

Every span, metric, log, and event includes `traceId`, `spanId`, `operation.name`, `service.version`, and `deployment.environment` automatically.

## Why autotel

OpenTelemetry requires substantial boilerplate. Real-world examples show 60-90% of code is instrumentation, not business logic.

### Real Example: Flask + OTLP

**Without autotel** (80+ lines of setup + 25+ lines per endpoint):

```python
# 80+ lines of imports and setup
from opentelemetry import trace, metrics
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk.metrics import MeterProvider
# ... 15+ more imports

# Manual resource setup
resource = Resource.create({
    "service.name": "my-service",
    "deployment.environment": "production",
})

# Manual tracer setup
trace_provider = TracerProvider(resource=resource)
span_exporter = OTLPSpanExporter()
trace_provider.add_span_processor(BatchSpanProcessor(span_exporter))
trace.set_tracer_provider(trace_provider)
tracer = trace.get_tracer(__name__)

# Manual metrics setup
metric_exporter = OTLPMetricExporter()
metric_reader = PeriodicExportingMetricReader(metric_exporter)
meter_provider = MeterProvider(resource=resource, metric_readers=[metric_reader])
metrics.set_meter_provider(meter_provider)
meter = metrics.get_meter(__name__)

# Manual metric creation
request_counter = meter.create_counter("http.requests")
latency_histogram = meter.create_histogram("http.duration")
# ... more setup

@app.route("/api/orders", methods=["POST"])
def create_order():
    started = time.perf_counter()

    # Manual span creation (10+ lines per endpoint!)
    with tracer.start_as_current_span(
        "create_order",
        attributes={
            "http.method": "POST",
            "http.route": "/api/orders",
        }
    ) as span:
        data = request.get_json()

        # Manual attribute setting
        span.set_attribute("order.items", len(data["items"]))
        span.set_attribute("order.total", data["total"])

        # Nested span for database call
        with tracer.start_as_current_span(
            "db.insert",
            attributes={"db.system": "postgresql"}
        ) as db_span:
            order = db.orders.create(data)
            db_span.set_attribute("db.statement", "INSERT INTO orders")

        # Manual metric recording
        latency_ms = (time.perf_counter() - started) * 1000
        latency_histogram.record(latency_ms, {"route": "/api/orders"})
        request_counter.add(1, {"route": "/api/orders", "status": "200"})

        return {"order_id": order.id}
```

**With autotel** (3 lines of setup + 0 lines per endpoint):

```python
import autotel

# One-line setup replaces 80+ lines!
autotel.init(service="my-service", instrumentation=["flask"])

# Your endpoint - that's it! No manual instrumentation needed.
@app.route("/api/orders", methods=["POST"])
def create_order():
    data = request.get_json()
    order = db.orders.create(data)

    # Optional: add custom business attributes
    autotel.track("order.created", {
        "order_id": order.id,
        "total": data["total"]
    })

    return {"order_id": order.id}

autotel automatically:
- Creates spans for HTTP requests (via Flask instrumentation)
- Adds semantic attributes (http.method, http.route, http.status_code)
- Tracks latency as metrics
- Captures errors with stack traces
- Propagates context to database calls
- No manual span creation, attribute setting, or metric recording
```

Result: 95% less code (from 130+ lines to 6 lines)

### Context Propagation Nightmare → Zero Lines

**Without autotel** (135 lines for MCP context propagation):

```python
# Custom decorator to extract context from _meta (40 lines)
def with_otel_context_from_meta(func):
    @functools.wraps(func)
    async def wrapper(*args, **kwargs):
        meta = kwargs.get("_meta")
        ctx = extract_otel_context_from_meta(meta)
        token = context.attach(ctx)
        try:
            return await func(*args, **kwargs)
        finally:
            context.detach(token)
    # ... handle sync functions too
    return wrapper

# Custom wrapper class to inject context (35 lines)
class TracedMCPServer:
    def __init__(self, server):
        self._server = server

    async def call_tool(self, tool_name: str, arguments: dict):
        arguments["_meta"] = inject_otel_context_to_meta()
        return await self._server.call_tool(tool_name, arguments)
    # ... more plumbing

# Usage - still requires manual work!
@with_otel_context_from_meta  # Must decorate every tool
async def my_mcp_tool(query: str, _meta: dict = None):
    # Finally, your business logic
    return process(query)

traced_server = TracedMCPServer(server)  # Must wrap every server
```

**With autotel** (0 lines - automatic):

```python
autotel.init(service="mcp-server", instrumentation=["mcp"])

# Just write your tool - context propagates automatically!
async def my_mcp_tool(query: str):
    return process(query)

autotel automatically:
- Extracts context from _meta fields
- Injects context into outgoing calls
- Handles both sync and async
- Works across HTTP, stdio, SSE

Need lower-level access? Use the MCP helpers directly:
- `inject_otel_context_to_meta()` / `extract_otel_context_from_meta()` for manual propagation
- `instrument_mcp_client(client)` to auto-inject `_meta` on outbound calls
- `instrument_mcp_server(server)` to extract parent context for tool handlers

**fastmcp / agents.mcp (transport-agnostic)**

```python
from autotel import init

# Enable auto-patching for MCP clients/servers (stdio/HTTP/SSE)
init(service="search-server", instrumentation=["mcp"])

# Anywhere you construct a fastmcp/agents.mcp server:
async with MCPServerStdio(
    name="Search server",
    params={"command": "fastmcp", "args": ["run", "--no-banner", "./server.py"]},
) as server:
    # autotel patches MCPServer/MCPServerStdio so handlers are traced and
    # _meta carries W3C trace context automatically—no manual wrappers needed.
    ...
```
```

Result: 100% elimination (from 135 lines to 0 lines)

### The Numbers

Based on real production codebases:

| Pattern | Manual OTEL | autotel | Reduction |
|---------|-------------|-------------|-----------|
| OTEL setup | 80+ lines | 3 lines | 96% |
| Per-endpoint instrumentation | 25-30 lines | 0-2 lines | 93% |
| Context propagation | 135 lines | 0 lines | 100% |
| LLM call tracking | 40+ lines | 0 lines | 100% |
| Error handling | 30+ lines per error | 0 lines | 100% |

**See detailed examples:** [`examples/comparison/`](./examples/comparison/)

**Migrating from manual OTEL?** See [`MIGRATION.md`](./MIGRATION.md) for step-by-step guide.

## Why autotel

| Challenge | With autotel |
|-----------|-----------------|
| Raw OpenTelemetry requires dozens of lines for basic setup | One-line `init()` with sensible defaults |
| Vendor SDKs create lock-in | OTLP-native, works with any backend |
| Need both observability **and** product analytics | Ship technical telemetry and product events through the same API |
| Production needs sampling, rate limiting, PII redaction | Guardrails enabled by default |

## Core Building Blocks

### `@trace` decorator

```python
from autotel import trace

@trace
async def get_user(user_id: str):
    return await db.users.find(user_id)

@trace
async def create_user(ctx, data: dict):
    # ctx parameter gives you span operations
    ctx.set_attribute('user.email', data['email'])
    ctx.add_event('user.created')
    # Also available: ctx.get_baggage(), ctx.set_baggage(), ctx.trace_id, ctx.span_id
    return await db.users.create(data)

@trace(name="custom.operation")
def process_data(data):
    return transform(data)
```

### `span()` context manager

```python
from autotel import span

async def complex_operation():
    with span("database.query") as ctx:
        ctx.set_attribute("query.type", "SELECT")
        results = await db.query(...)

    with span("processing") as ctx:
        ctx.set_attribute("items.count", len(results))
        return process(results)
```

### Convenience Helpers

Simple functions for common operations without needing to get the span first:

```python
from autotel import (
    set_attribute,
    set_attributes,
    add_event,
    record_exception,
    get_trace_id,
    get_span_id,
    get_baggage,
)

def process_order(order_data):
    # Set single attribute
    set_attribute("order.type", "express")

    # Set multiple attributes at once
    set_attributes({
        "order.id": order_data["id"],
        "order.total": order_data["total"],
        "customer.tier": "premium",
    })

    # Add a span event
    add_event("order.validated", {"validator": "schema_v2"})

    # Get IDs for logging
    trace_id = get_trace_id()
    print(f"Processing order in trace: {trace_id}")

    try:
        process(order_data)
    except ValueError as e:
        # Record exception automatically (sets span status to ERROR)
        record_exception(e, {"order.id": order_data["id"]})
        raise

# Read baggage without needing TraceContext
tenant_id = get_baggage("tenant.id")
```

**Available helpers:**
- `set_attribute(key, value)` - Set single span attribute
- `set_attributes(dict)` - Set multiple span attributes
- `add_event(name, attributes)` - Add span event
- `record_exception(exception, attributes)` - Record exception and set error status
- `get_trace_id()` - Get current trace ID as hex string
- `get_span_id()` - Get current span ID as hex string
- `get_baggage(key)` - Get baggage value
- `get_all_baggage()` - Get all baggage as dict
- `set_baggage_value(key, value)` - Set baggage value

### Semantic Convention Helpers

Pre-configured decorators that automatically add [OpenTelemetry semantic conventions](https://opentelemetry.io/docs/specs/semconv/) for common operation types. These helpers work with **any** OpenTelemetry backend (Honeycomb, Datadog, New Relic, Jaeger, etc.) because they follow the standard OTel semantic conventions.

#### LLM/AI Operations (`@trace_llm`)

Automatically adds Gen AI semantic conventions for LLM operations:

```python
from autotel import trace_llm

@trace_llm(model="gpt-4-turbo", operation="chat", system="openai")
async def generate_response(ctx, prompt: str):
    response = await openai.chat.completions.create(
        model="gpt-4-turbo",
        messages=[{"role": "user", "content": prompt}]
    )

    # Add token usage metrics
    ctx.set_attribute("gen.ai.usage.completion_tokens", response.usage.completion_tokens)
    ctx.set_attribute("gen.ai.usage.prompt_tokens", response.usage.prompt_tokens)

    return response.choices[0].message.content
```

Automatically adds:
- `gen.ai.request.model` - Model identifier
- `gen.ai.operation.name` - Operation type (chat, completion, embedding)
- `gen.ai.system` - AI system name (openai, anthropic, cohere)

Common use cases:
- Chat completions with OpenAI, Anthropic Claude, Cohere
- Text embeddings
- Streaming LLM responses
- Multi-modal AI operations

#### Database Operations (`@trace_db`)

Automatically adds DB semantic conventions for database operations:

```python
from autotel import trace_db

@trace_db(system="postgresql", operation="SELECT", db_name="production")
async def get_user_by_id(ctx, user_id: str):
    result = await conn.fetchrow("SELECT * FROM users WHERE id = $1", user_id)

    # Add query details (sanitized, no PII!)
    ctx.set_attribute("db.statement", "SELECT * FROM users WHERE id = $1")
    ctx.set_attribute("db.collection.name", "users")

    return result

@trace_db(system="mongodb", operation="find", db_name="app_db", collection="orders")
async def find_user_orders(ctx, user_id: str):
    return await db.orders.find({"user_id": user_id}).to_list()

@trace_db(system="redis", operation="get")
async def get_from_cache(ctx, key: str):
    ctx.set_attribute("db.redis.key", key)
    return await redis.get(key)
```

Automatically adds:
- `db.system` - Database system (postgresql, mongodb, redis, mysql)
- `db.operation` - Operation type (SELECT, INSERT, find, get)
- `db.name` - Database name
- `db.collection.name` - Collection or table name

#### HTTP Client Operations (`@trace_http`)

Automatically adds HTTP semantic conventions for API calls:

```python
from autotel import trace_http

@trace_http(method="GET", url="https://api.github.com/users/{username}")
async def get_github_user(ctx, username: str):
    async with httpx.AsyncClient() as client:
        response = await client.get(f"https://api.github.com/users/{username}")

        # Add response details
        ctx.set_attribute("http.response.status_code", response.status_code)
        ctx.set_attribute("http.response.body.size", len(response.content))

        return response.json()

@trace_http(method="POST", url="https://api.stripe.com/v1/charges")
async def create_charge(ctx, amount: int):
    # HTTP request details automatically tracked
    return await stripe_client.post("/v1/charges", json={"amount": amount})
```

Automatically adds:
- `http.request.method` - HTTP method (GET, POST, PUT, DELETE)
- `url.full` - Full URL or URL template

#### Messaging Operations (`@trace_messaging`)

Automatically adds messaging semantic conventions for message queues:

```python
from autotel import trace_messaging

@trace_messaging(system="kafka", operation="publish", destination="order-events")
async def publish_order_event(ctx, order_id: str, event_data: dict):
    producer.send('order-events', value=event_data)

    # Add message details
    ctx.set_attribute("messaging.message.id", order_id)
    ctx.set_attribute("messaging.kafka.partition", 2)

    producer.flush()

@trace_messaging(system="rabbitmq", operation="receive", destination="notifications")
async def consume_notification(ctx, message):
    ctx.set_attribute("messaging.message.id", message.message_id)
    await process_notification(message.body)

@trace_messaging(system="sqs", operation="process", destination="tasks")
async def process_task(ctx, task_id: str, task_data: dict):
    # Automatic semantic conventions for SQS
    await execute_task(task_data)
```

Automatically adds:
- `messaging.system` - Messaging system (kafka, rabbitmq, sqs, redis_streams)
- `messaging.operation` - Operation type (publish, receive, process)
- `messaging.destination.name` - Queue, topic, or channel name

**Why semantic helpers?**
- Enforces OpenTelemetry standards automatically
- Ensures consistency across your codebase
- Works with any OTel backend (provider-agnostic)
- Reduces boilerplate for common patterns

See [`examples/basic/semantic_helpers_example.py`](./examples/basic/semantic_helpers_example.py) for complete examples.

### Batch instrumentation

```python
from autotel import instrument

# Instrument multiple functions at once
user_service = instrument({
    'create': lambda ctx, data: db.users.create(data),
    'get': lambda user_id: db.users.find(user_id),
    'update': lambda ctx, user_id, data: db.users.update(user_id, data),
})

user = user_service['create']({'id': '123', 'email': 'test@example.com'})
```

### Root context isolation

```python
from autotel import with_new_context

def background_worker():
    with with_new_context():
        # Creates a new root trace (not child of current)
        process_job()
```

### Baggage (Context Propagation)

Baggage allows you to propagate custom key-value pairs across distributed traces. Baggage is automatically included in HTTP headers when using `inject_trace_context()` from `autotel.http`.

**Basic usage:**

```python
from autotel import trace, with_baggage
from autotel.http import inject_trace_context

@trace
async def create_order(ctx, order):
    # Set baggage for downstream services
    with with_baggage({
        'tenant.id': order.tenant_id,
        'user.id': order.user_id,
    }):
        # Baggage is available to all child spans and HTTP calls
        tenant_id = ctx.get_baggage('tenant.id')
        ctx.set_attribute('tenant.id', tenant_id or 'unknown')
        
        # HTTP headers automatically include baggage
        headers = inject_trace_context()
        async with httpx.AsyncClient() as client:
            await client.post('/api/charge', headers=headers, json=order)
```

**TraceContext baggage methods:**

```python
@trace
async def process_order(ctx, order):
    # Get baggage entry
    tenant_id = ctx.get_baggage('tenant.id')
    
    # Set baggage entry (note: use with_baggage() for proper scoping)
    ctx.set_baggage('order.id', order.id)
    
    # Delete baggage entry
    ctx.delete_baggage('old.key')
    
    # Get all baggage entries
    all_baggage = ctx.get_all_baggage()
    # Returns: {'tenant.id': 't1', 'user.id': 'u1', ...}
```

**Automatic Baggage → Span Attributes:**

Enable baggage span attributes in `init()` to automatically copy all baggage entries to span attributes, making them visible in trace UIs (Jaeger, Grafana, DataDog, etc.) without manual `ctx.set_attribute()` calls:

```python
from autotel import init, trace, with_baggage

# Option 1: Default prefix 'baggage.'
init(
    service='my-app',
    baggage=True,  # Creates baggage.tenant.id, baggage.user.id
)

# Option 2: Custom prefix
init(
    service='my-app',
    baggage='ctx',  # Creates ctx.tenant.id, ctx.user.id
)

# Option 3: No prefix
init(
    service='my-app',
    baggage='',  # Creates tenant.id, user.id (no prefix)
)

# Option 4: Disabled (default)
init(
    service='my-app',
    baggage=False,  # or omit baggage parameter
    # Baggage won't be copied to span attributes
)
```

**Example usage:**

```python
init(service='my-app', baggage=True)

@trace
async def process_order(ctx, order):
    with with_baggage({
        'tenant.id': order.tenant_id,
        'user.id': order.user_id,
    }):
        # Span automatically has baggage.tenant.id and baggage.user.id attributes!
        # No need for: ctx.set_attribute('tenant.id', ctx.get_baggage('tenant.id'))
        await charge_customer(order)
```

**Key Points:**

- `baggage=True` in `init()` eliminates manual attribute setting for baggage
- Baggage values are strings (convert numbers/objects before setting)
- Never put PII in baggage - it propagates in HTTP headers across services!
- Use `with_baggage()` for proper scoping across async boundaries

## Business Metrics & Product Events

### OpenTelemetry Metrics (Metric class → OTLP)

```python
from autotel import Metric

metrics = Metric('checkout')

@trace
async def process_order(order):
    # Sends counter to OTLP
    metrics.trackEvent('order.completed', {
        'orderId': order.id,
        'amount': order.total,
    })

    # Sends histogram to OTLP
    metrics.trackValue('revenue', order.total, {'currency': order.currency})
```

- Emits OpenTelemetry counters/histograms via OTLP
- Infrastructure metrics enabled by default

### Product Events (Event class → Subscribers)

Track user behavior, funnels, and business outcomes alongside your OpenTelemetry traces.

**Recommended: Configure subscribers in `init()`, use global `track()` function:**

```python
from autotel import init, track
from autotel.subscribers import PostHogSubscriber

init(
    service='checkout',
    subscribers=[PostHogSubscriber(api_key='phc_...')]
)

@trace
async def signup(user):
    # All events use subscribers from init() automatically
    track('user.signup', {'userId': user.id, 'plan': user.plan})
```

**Event instance (inherits subscribers from `init()`):**

```python
from autotel import Event

# Uses subscribers configured in init()
events = Event()
events.trackEvent('order.completed', {'amount': 99.99})
```

**Override subscribers for specific Event instance:**

```python
from autotel import Event
from autotel.subscribers import WebhookSubscriber

# Override: use different subscribers (multi-tenant, A/B testing, etc.)
marketing_events = Event(
    subscribers=[WebhookSubscriber(url='https://api.example.com/events')]
)

marketing_events.trackEvent('campaign.viewed', {'campaignId': '123'})
```

**Subscriber resolution:**
- If `subscribers` passed to Event constructor → uses those (instance override)
- If no `subscribers` passed → falls back to `init()` subscribers (global config)
- If neither configured → events logged only (graceful degradation)

Auto-enrichment adds `traceId`, `spanId`, `operation.name`, `service.version`, and `deployment.environment` to every event automatically.

## Logging with Trace Context

**Bring your own logger** and autotel automatically instruments it to inject trace context.

### Using Python standard logging

```python
import logging
from autotel import init

logger = logging.getLogger(__name__)

init(service='user-service', logger=logger)

@trace
async def create_user(data):
    logger.info('Creating user', extra={'userId': data['id']})
    # Log now includes trace_id, span_id, operation_name automatically
    user = await db.users.create(data)
    logger.info('User created', extra={'userId': user.id})
    return user
```

### Using structlog

```python
import structlog
from autotel import init

logger = structlog.get_logger()

init(service='user-service', logger=logger)

# All logs now include trace context
logger.info('user.created', user_id='123')
```

What you get automatically:
- Logs include `trace_id`, `span_id`, `operation_name` for correlation
- Zero configuration: just pass your logger to `init()`
- Supports standard logging, structlog, and loguru

## Framework Integrations

### FastAPI

```python
from fastapi import FastAPI
from autotel.integrations.fastapi import autotelMiddleware

app = FastAPI()
app.add_middleware(autotelMiddleware, service="my-api")
```

### Django

```python
# settings.py
MIDDLEWARE = [
    'autotel.integrations.django.autotelMiddleware',
    # ... other middleware
]

autotel = {
    'SERVICE_NAME': 'my-django-app',
}
```

### Flask

```python
from flask import Flask
from autotel.integrations.flask import init_autotel

app = Flask(__name__)
init_autotel(app, service="my-flask-app")
```

## Configuration

### Environment Variables (Standard OTEL)

```bash
# Service name
export OTEL_SERVICE_NAME=my-app

# OTLP endpoint
export OTEL_EXPORTER_OTLP_ENDPOINT=http://localhost:4318

# Protocol (http or grpc)
export OTEL_EXPORTER_OTLP_PROTOCOL=http

# Headers (comma-separated key=value)
export OTEL_EXPORTER_OTLP_HEADERS=api-key=secret123

# Resource attributes (comma-separated key=value)
export OTEL_RESOURCE_ATTRIBUTES=service.version=1.0.0,deployment.environment=production
```

Then just call `init()` with no parameters:

```python
from autotel import init
init()  # Reads all config from environment variables!
```

### Adaptive Sampling

```python
from autotel import init, AdaptiveSampler

init(
    service="my-service",
    sampler=AdaptiveSampler(
        baseline_rate=0.1,      # 10% baseline
        error_rate=1.0,         # 100% errors
        slow_threshold_ms=1000, # >1s is slow
        slow_rate=1.0,          # 100% slow operations
    )
)
```

### Rate Limiting

```python
from autotel import RateLimiter

limiter = RateLimiter(max_spans_per_second=1000, burst_size=2000)
```

### Circuit Breaker

```python
from autotel import CircuitBreaker

breaker = CircuitBreaker(
    failure_threshold=5,   # Open after 5 failures
    recovery_timeout=60,   # Retry after 60s
    success_threshold=2,   # Close after 2 successes
)
```

### PII Redaction

```python
from autotel import PIIRedactor

redactor = PIIRedactor(
    redact_email=True,
    redact_phone=True,
    redact_ssn=True,
    redact_credit_card=True,
    allowlist_keys=["user_id", "request_id"],
)

safe_value = redactor.redact_attribute("email", "user@example.com")
# Returns: "[EMAIL_REDACTED]"
```

## HTTP Instrumentation

```python
from autotel.http import http_instrumented, trace_http_request, inject_trace_context

# Class decorator for auto-instrumentation
@http_instrumented(slow_threshold_ms=1000)
class ApiClient:
    async def get_user(self, user_id: str):
        res = await httpx.get(f'https://api.example.com/users/{user_id}')
        return res.json()

# Manual tracing with W3C Trace Context propagation
async def fetch_data(url: str):
    with trace_http_request("GET", url) as ctx:
        headers = inject_trace_context()  # W3C Trace Context
        res = await httpx.get(url, headers=headers)
        ctx.set_attribute("http.status_code", res.status_code)
        return res.json()
```

## Database Instrumentation

```python
from autotel.db import instrument_database, trace_db_query

# Runtime instrumentation
db = instrument_database(
    SQLAlchemy(...),
    db_system='postgresql',
    db_name='myapp',
    slow_threshold_ms=500,
)

# Manual tracing
with trace_db_query("SELECT", "users", "postgresql") as ctx:
    ctx.set_attribute("db.statement", query)
    result = await db.execute(query)
```

## Testing

```python
from autotel import init, span
from autotel.exporters import InMemorySpanExporter
from autotel.processors import SimpleSpanProcessor
from autotel.testing import (
    assert_trace_created,
    assert_trace_succeeded,
    assert_no_errors,
    get_trace_duration,
)

def test_my_function():
    exporter = InMemorySpanExporter()
    init(service="test", span_processor=SimpleSpanProcessor(exporter))

    with span("test.operation"):
        pass

    # Assertions
    assert_trace_created(exporter, "test.operation")
    assert_trace_succeeded(exporter, "test.operation")
    assert_no_errors(exporter)

    duration = get_trace_duration(exporter, "test.operation")
    assert duration < 500  # milliseconds
```

## Graceful Shutdown

```python
from autotel import shutdown, shutdown_sync

# Async shutdown (recommended)
await shutdown(timeout=5.0)

# Sync shutdown
shutdown_sync(timeout=5.0)
```

Shutdown ensures:
- Event queue is drained
- Pending spans are flushed
- Subscribers are properly closed
- No data loss

## Complete Feature List

### Core Features
- ✅ One-line initialization with environment variable support
- ✅ Ergonomic `@trace` decorator (sync & async)
- ✅ `TraceContext` for span operations
- ✅ Functional API (`instrument()`, `span()`, `with_new_context()`)
- ✅ Baggage support (`with_baggage()`, `ctx.get_baggage()`, automatic span attributes)

### Events & Metrics
- ✅ `Event` class → sends to subscribers (PostHog, Slack, Webhook, etc.)
- ✅ `Metric` class → sends to OTLP (OpenTelemetry counters/histograms)
- ✅ Global `track()` function
- ✅ Auto-enrichment with trace context
- ✅ Queue-based event system with circuit breaker protection

### Logging
- ✅ Bring your own logger (standard logging, structlog, loguru)
- ✅ Automatic trace context injection
- ✅ Zero configuration

### Production Features
- ✅ Adaptive sampling (10% baseline, 100% errors/slow)
- ✅ Rate limiting (token bucket)
- ✅ Circuit breaker (subscriber protection)
- ✅ PII redaction (email, phone, SSN, credit card, API keys)

### Framework Integrations
- ✅ FastAPI middleware
- ✅ Django middleware
- ✅ Flask integration

### Instrumentation Helpers
- ✅ HTTP instrumentation (`@http_instrumented`, `trace_http_request()`)
- ✅ Database instrumentation (`instrument_database()`, `trace_db_query()`)
- ✅ W3C Trace Context propagation

### Testing
- ✅ InMemorySpanExporter for unit tests
- ✅ Test helpers (`assert_trace_created()`, `assert_trace_succeeded()`, etc.)
- ✅ ConsoleSpanExporter for debugging

## Comparison with Raw OpenTelemetry

| Feature | Raw OpenTelemetry | autotel |
|---------|------------------|------------|
| Initialization | 20-30 lines | 1 line |
| Decorator API | `@tracer.start_as_current_span("name")` | `@trace` |
| Context access | `trace.get_current_span()` | `ctx` parameter |
| Env config | Manual parsing | Automatic (`OTEL_*` vars) |
| Adaptive sampling | ❌ (collector only) | ✅ Built-in |
| Rate limiting | ❌ | ✅ Built-in |
| PII redaction | ❌ | ✅ Built-in |
| Product events | ❌ | ✅ Built-in |
| Logging integration | Manual | ✅ Automatic |

## Status

Production ready. All core features implemented and tested.

**Version:** 0.1.0  
**Python:** 3.10+  
**License:** MIT

## License

MIT License - see [LICENSE](LICENSE) file for details.
