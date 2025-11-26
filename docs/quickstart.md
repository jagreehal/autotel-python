# Quick Start

Get started with autotel in under 5 minutes.

## Installation

```bash
pip install autotel
```

Or with optional dependencies:

```bash
# With FastAPI support
pip install autotel[fastapi]

# With all features
pip install autotel[all]
```

## Basic Usage

### Initialize Once

```python
from autotel import init

# In your main.py or app startup
init(service="my-app", endpoint="http://localhost:4318")
```

**Configuration options:**
- Environment variables: `OTEL_SERVICE_NAME`, `OTEL_EXPORTER_OTLP_ENDPOINT`, etc.
- Explicit parameters override env vars
- Defaults to `http://localhost:4318`

You can also use environment variables exclusively:

```bash
export OTEL_SERVICE_NAME=my-app
export OTEL_EXPORTER_OTLP_ENDPOINT=http://localhost:4318
export OTEL_EXPORTER_OTLP_PROTOCOL=http
```

```python
from autotel import init
init()  # Reads all config from environment variables!
```

### Custom Pipelines (traces/metrics/logs)

You can now send data to multiple backends with multiple processors/exporters.

```python
from opentelemetry.sdk._logs import LoggerProvider, LoggingHandler, LogRecordProcessor
from opentelemetry.sdk._logs.export import ConsoleLogExporter, BatchLogRecordProcessor
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import InMemoryMetricReader, PeriodicExportingMetricReader
from opentelemetry.sdk.trace.export import BatchSpanProcessor, SimpleSpanProcessor
from opentelemetry.exporter.otlp.proto.http.metric_exporter import OTLPMetricExporter
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter

from autotel import init
from autotel.exporters import InMemorySpanExporter

init(
    service="my-app",
    span_processors=[
        BatchSpanProcessor(OTLPSpanExporter(endpoint="https://otel.example.com/v1/traces")),
        SimpleSpanProcessor(InMemorySpanExporter()),  # debug side-channel
    ],
    metric_readers=[
        PeriodicExportingMetricReader(
            OTLPMetricExporter(endpoint="https://otel.example.com/v1/metrics")
        ),
        InMemoryMetricReader(),  # parallel sink
    ],
    log_record_processors=[
        BatchLogRecordProcessor(ConsoleLogExporter()),
    ],
)
```

### Trace Your Functions

```python
from autotel import trace

@trace
async def get_user(user_id: str):
    """Simple function tracing."""
    return await db.users.find(user_id)

@trace
async def create_user(ctx, data: dict):
    """With context parameter for span operations."""
    ctx.set_attribute('user.email', data['email'])
    ctx.set_attribute('user.id', data['id'])
    return await db.users.create(data)
```

### Manual Span Creation

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
```

### Track Product Events

Track user behavior and business events alongside your traces:

```python
import os
from autotel import init, track
from autotel.subscribers import PostHogSubscriber

init(
    service="checkout-api",
    subscribers=[PostHogSubscriber(api_key=os.environ["POSTHOG_KEY"])]
)

@trace
async def process_order(ctx, order):
    track("order.completed", {"amount": order.total})
    return await charge(order)
```

Events are automatically enriched with `traceId`, `spanId`, `operation.name`, `service.version`, and `deployment.environment`.

### Semantic Convention Helpers

Pre-configured decorators that automatically add OpenTelemetry semantic conventions:

```python
from autotel import trace_llm, trace_db, trace_http

# LLM operations
@trace_llm(model="gpt-4-turbo", operation="chat", system="openai")
async def generate_response(ctx, prompt: str):
    response = await openai.chat.completions.create(...)
    ctx.set_attribute("gen.ai.usage.prompt_tokens", response.usage.prompt_tokens)
    return response

# Database operations
@trace_db(system="postgresql", operation="SELECT", db_name="production")
async def get_user(ctx, user_id: str):
    return await conn.fetchrow("SELECT * FROM users WHERE id = $1", user_id)

# HTTP client operations
@trace_http(method="GET", url="https://api.github.com/users/{username}")
async def get_github_user(ctx, username: str):
    async with httpx.AsyncClient() as client:
        response = await client.get(f"https://api.github.com/users/{username}")
        ctx.set_attribute("http.response.status_code", response.status_code)
        return response.json()
```

### Baggage (Context Propagation)

Propagate custom key-value pairs across distributed traces:

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
        
        # HTTP headers automatically include baggage
        headers = inject_trace_context()
        async with httpx.AsyncClient() as client:
            await client.post('/api/charge', headers=headers, json=order)
```

Enable automatic baggage → span attributes in `init()`:

```python
init(service='my-app', baggage=True)  # Creates baggage.tenant.id, baggage.user.id
```

## Framework Integration

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
    'ENDPOINT': 'http://localhost:4318',
}
```

### Flask

```python
from flask import Flask
from autotel.integrations.flask import init_autotel

app = Flask(__name__)
init_autotel(app, service="my-flask-app")
```

## Next Steps

- Learn about [production configuration](production.md)
- Explore [framework integrations](integrations.md)
- Check out the [API reference](api/core.md)
