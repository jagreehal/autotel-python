# Migration Guide: From Manual OpenTelemetry to autotel

This guide helps teams migrate from manual OpenTelemetry instrumentation to autotel, typically reducing instrumentation code by 90-95%.

## Table of Contents

1. [Why Migrate?](#why-migrate)
2. [Before You Start](#before-you-start)
3. [Migration Strategy](#migration-strategy)
4. [Step-by-Step Migration](#step-by-step-migration)
5. [Pattern-by-Pattern Replacements](#pattern-by-pattern-replacements)
6. [Testing Your Migration](#testing-your-migration)
7. [Rollback Plan](#rollback-plan)
8. [Troubleshooting](#troubleshooting)

## Why Migrate?

### Benefits

- 90-95% less code: eliminate setup boilerplate and manual instrumentation
- Faster development: no manual span creation, attribute setting, or metric recording
- Fewer bugs: can't forget to instrument code paths
- Easier maintenance: framework updates don't break instrumentation
- Consistent telemetry: semantic conventions enforced automatically
- Clean separation between business logic and observability

### What You Keep

- Same backends: works with any OTLP-compatible backend (Jaeger, Tempo, DataDog, etc.)
- Same data: produces identical traces, metrics, and logs
- Same W3C standards: context propagation via W3C Trace Context
- Same flexibility: can still use raw OTEL APIs when needed

## Before You Start

### Assessment Checklist

- [ ] Identify all files with OTEL imports (`from opentelemetry import ...`)
- [ ] List all manual span creation locations (`tracer.start_as_current_span(...)`)
- [ ] Document custom instrumentation (decorators, wrappers, context managers)
- [ ] Note any custom exporters or processors
- [ ] Check for custom sampling logic
- [ ] Review environment variable usage (`OTEL_*` vars)

### Prerequisites

```bash
pip install autotel
# or
uv add autotel
```

**Python version**: 3.10+

## Migration Strategy

We recommend a **phased approach** to minimize risk:

### Phase 1: Parallel Run (Recommended)
Run autotel alongside existing OTEL code, compare outputs, then switch over.

### Phase 2: Incremental Migration
Migrate one service/module at a time, starting with least critical.

### Phase 3: Big Bang (Only for small codebases)
Replace all manual OTEL code at once. Only recommended for <5 files.

## Step-by-Step Migration

### Step 1: Replace OTEL Setup Code

**Before** (80+ lines):

```python
# Remove all of this setup code
from opentelemetry import trace, metrics
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader
from opentelemetry.exporter.otlp.proto.http.metric_exporter import OTLPMetricExporter

resource = Resource.create({
    "service.name": os.getenv("OTEL_SERVICE_NAME", "my-service"),
    "deployment.environment": os.getenv("ENVIRONMENT", "dev"),
})

trace_provider = TracerProvider(resource=resource)
span_exporter = OTLPSpanExporter()
trace_provider.add_span_processor(BatchSpanProcessor(span_exporter))
trace.set_tracer_provider(trace_provider)

metric_exporter = OTLPMetricExporter()
metric_reader = PeriodicExportingMetricReader(metric_exporter)
meter_provider = MeterProvider(resource=resource, metric_readers=[metric_reader])
metrics.set_meter_provider(meter_provider)

# ... 60+ more lines
```

**After** (3 lines):

```python
import autotel

autotel.init(
    service="my-service",  # Or use OTEL_SERVICE_NAME env var
    # All OTEL_* environment variables work automatically!
)
```

**Key points:**
- autotel reads all standard `OTEL_*` environment variables
- No need to manually parse env vars or create Resource objects
- OTLP exporter configured automatically

### Step 2: Remove Manual Span Creation

**Before**:

```python
tracer = trace.get_tracer(__name__)

@app.route("/api/users", methods=["POST"])
def create_user():
    with tracer.start_as_current_span(
        "create_user",
        attributes={
            "http.method": "POST",
            "http.route": "/api/users",
        }
    ) as span:
        data = request.get_json()
        span.set_attribute("user.email", data["email"])
        span.set_attribute("user.role", data["role"])

        user = db.users.create(data)
        span.set_attribute("user.id", user.id)

        return {"user_id": user.id}
```

**After** (automatic instrumentation):

```python
# Remove tracer = trace.get_tracer(__name__)

@app.route("/api/users", methods=["POST"])
def create_user():
    # Framework instrumentation handles spans automatically!
    data = request.get_json()
    user = db.users.create(data)

    # Optional: add custom business attributes
    span = autotel.get_active_span()
    if span:
        span.set_attribute("user.role", data["role"])

    return {"user_id": user.id}
```

**Enable framework instrumentation in `init()`:**

```python
autotel.init(
    service="my-service",
    instrumentation=["flask"],  # or "fastapi", "django"
)
```

### Step 3: Replace Custom Decorators

**Before** (custom tracing decorator):

```python
def traced_function(func):
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        tracer = trace.get_tracer(__name__)
        with tracer.start_as_current_span(func.__name__) as span:
            try:
                result = func(*args, **kwargs)
                return result
            except Exception as e:
                span.record_exception(e)
                span.set_status(StatusCode.ERROR)
                raise
    return wrapper

@traced_function
def process_data(data):
    return transform(data)
```

**After** (use `@trace` decorator):

```python
from autotel import trace

@trace
def process_data(data):
    return transform(data)

# Errors are automatically recorded - no manual exception handling needed!
```

### Step 4: Remove Context Propagation Code

**Before** (custom context propagation - often 100+ lines):

```python
# Remove custom extractors
def extract_otel_context_from_meta(meta: dict) -> Context:
    # ... 20 lines of manual extraction

# Remove custom injectors
def inject_otel_context_to_meta() -> dict:
    # ... 15 lines of manual injection

# Remove custom decorators
def with_otel_context_from_meta(func):
    # ... 40 lines of decorator logic

# Remove custom wrapper classes
class TracedMCPServer:
    # ... 35 lines of wrapper code
```

**After** (automatic):

```python
# Enable MCP instrumentation in init()
autotel.init(
    service="mcp-server",
    instrumentation=["mcp"],  # Context propagation is automatic!
)

# Your tools just work - no decorators or wrappers needed!
async def my_tool(query: str):
    return process(query)
```

### Step 5: Replace Manual Metrics

**Before**:

```python
meter = metrics.get_meter(__name__)

request_counter = meter.create_counter(
    "http.server.request.count",
    unit="1",
    description="Total requests"
)

latency_histogram = meter.create_histogram(
    "http.server.request.duration",
    unit="ms",
    description="Request duration"
)

@app.route("/api/orders")
def get_orders():
    started = time.perf_counter()

    # ... business logic

    # Manual metric recording
    latency_ms = (time.perf_counter() - started) * 1000
    latency_histogram.record(latency_ms, {"route": "/api/orders"})
    request_counter.add(1, {"route": "/api/orders", "status": "200"})
```

**After** (automatic):

```python
@app.route("/api/orders")
def get_orders():
    # Framework instrumentation records HTTP metrics automatically!
    # No manual timing or metric recording needed.

    # ... business logic

# autotel automatically tracks:
# - http.server.request.count (counter)
# - http.server.request.duration (histogram)
# - http.server.active_requests (up/down counter)
```

### Step 6: Simplify Error Handling

**Before**:

```python
with tracer.start_as_current_span("process_order") as span:
    try:
        order = process_order(data)
        span.set_attribute("order.id", order.id)
        return order
    except ValueError as e:
        span.set_attribute("error", True)
        span.set_attribute("error.type", "ValueError")
        span.set_attribute("error.message", str(e))
        span.set_status(StatusCode.ERROR, str(e))
        logger.error(f"Validation error: {e}")
        raise
    except Exception as e:
        span.record_exception(e)
        span.set_status(StatusCode.ERROR)
        logger.exception("Unexpected error")
        raise
```

**After** (automatic error capture):

```python
with autotel.span("process_order") as ctx:
    order = process_order(data)
    ctx.set_attribute("order.id", order.id)
    return order

# autotel automatically:
# - Records exceptions on span
# - Sets error=True attribute
# - Sets error.type and error.message
# - Captures stack traces
# - Updates span status to ERROR
```

### Step 7: Simplify Baggage Usage

**Before**:

```python
from opentelemetry import context
from opentelemetry.baggage import propagation

# Manual baggage setting
current_context = context.get_current()
new_context = propagation.set_baggage("user.id", "123", current_context)
new_context = propagation.set_baggage("tenant.id", "456", new_context)
token = context.attach(new_context)
try:
    make_downstream_call()
finally:
    context.detach(token)
```

**After**:

```python
from autotel import with_baggage

# Simple context manager
with with_baggage({"user.id": "123", "tenant.id": "456"}):
    make_downstream_call()
```

### Step 8: Update Shutdown/Cleanup

**Before**:

```python
import atexit
from opentelemetry.sdk.trace.export import BatchSpanProcessor

# Manual cleanup
def cleanup():
    if trace_provider:
        trace_provider.force_flush(timeout_millis=5000)
        trace_provider.shutdown()
    if meter_provider:
        meter_provider.force_flush(timeout_millis=5000)
        meter_provider.shutdown()

atexit.register(cleanup)
```

**After**:

```python
from autotel import shutdown

# Clean async shutdown
await shutdown(timeout=5.0)

# Or sync version
# shutdown_sync(timeout=5.0)
```

## Pattern-by-Pattern Replacements

### Pattern: Manual Span with Attributes

**Before**:
```python
with tracer.start_as_current_span("operation", attributes={"key": "value"}) as span:
    span.set_attribute("another", "attribute")
    result = do_work()
    span.set_attribute("result.size", len(result))
```

**After**:
```python
with autotel.span("operation") as ctx:
    ctx.set_attribute("key", "value")
    ctx.set_attribute("another", "attribute")
    result = do_work()
    ctx.set_attribute("result.size", len(result))
```

### Pattern: Nested Spans

**Before**:
```python
with tracer.start_as_current_span("parent") as parent:
    parent.set_attribute("level", "1")
    with tracer.start_as_current_span("child") as child:
        child.set_attribute("level", "2")
```

**After**:
```python
with autotel.span("parent") as ctx:
    ctx.set_attribute("level", "1")
    with autotel.span("child") as ctx:
        ctx.set_attribute("level", "2")
```

### Pattern: Span Events

**Before**:
```python
span = trace.get_current_span()
span.add_event("user.login", {"user_id": "123"})
```

**After**:
```python
span = autotel.get_active_span()
if span:
    span.add_event("user.login", {"user_id": "123"})
```

### Pattern: Getting Trace/Span IDs

**Before**:
```python
span = trace.get_current_span()
span_context = span.get_span_context()
trace_id = format(span_context.trace_id, "032x")
span_id = format(span_context.span_id, "016x")
```

**After**:
```python
with autotel.span("operation") as ctx:
    trace_id = ctx.trace_id  # Already formatted as hex string
    span_id = ctx.span_id    # Already formatted as hex string
```

### Pattern: Conditional Instrumentation

**Before**:
```python
if should_trace:
    with tracer.start_as_current_span("operation"):
        do_work()
else:
    do_work()
```

**After** (use sampling instead):
```python
# Configure sampling in init()
autotel.init(
    service="my-service",
    sampler=autotel.AdaptiveSampler(baseline_rate=0.1)
)

# Just write your code normally
do_work()
```

### Pattern: LLM/AI Operations with Semantic Conventions

**Before** (manual semantic conventions):
```python
with tracer.start_as_current_span("llm.chat") as span:
    # Manually add Gen AI semantic conventions
    span.set_attribute("gen.ai.request.model", "gpt-4-turbo")
    span.set_attribute("gen.ai.operation.name", "chat")
    span.set_attribute("gen.ai.system", "openai")

    response = await openai.chat.completions.create(
        model="gpt-4-turbo",
        messages=[{"role": "user", "content": prompt}]
    )

    # Manually add token usage
    span.set_attribute("gen.ai.usage.prompt_tokens", response.usage.prompt_tokens)
    span.set_attribute("gen.ai.usage.completion_tokens", response.usage.completion_tokens)
```

**After** (automatic semantic conventions):
```python
from autotel import trace_llm

@trace_llm(model="gpt-4-turbo", operation="chat", system="openai")
async def generate_response(ctx, prompt: str):
    # Semantic conventions added automatically!
    response = await openai.chat.completions.create(
        model="gpt-4-turbo",
        messages=[{"role": "user", "content": prompt}]
    )

    # Just add usage metrics
    ctx.set_attribute("gen.ai.usage.prompt_tokens", response.usage.prompt_tokens)
    ctx.set_attribute("gen.ai.usage.completion_tokens", response.usage.completion_tokens)

    return response.choices[0].message.content
```

### Pattern: Database Operations with Semantic Conventions

**Before** (manual DB semantic conventions):
```python
with tracer.start_as_current_span("db.query") as span:
    # Manually add DB semantic conventions
    span.set_attribute("db.system", "postgresql")
    span.set_attribute("db.operation", "SELECT")
    span.set_attribute("db.name", "production")
    span.set_attribute("db.collection.name", "users")
    span.set_attribute("db.statement", "SELECT * FROM users WHERE id = $1")

    result = await conn.fetchrow("SELECT * FROM users WHERE id = $1", user_id)
    return result
```

**After** (automatic semantic conventions):
```python
from autotel import trace_db

@trace_db(system="postgresql", operation="SELECT", db_name="production")
async def get_user(ctx, user_id: str):
    # DB semantic conventions added automatically!
    result = await conn.fetchrow("SELECT * FROM users WHERE id = $1", user_id)

    # Just add query-specific details
    ctx.set_attribute("db.statement", "SELECT * FROM users WHERE id = $1")
    ctx.set_attribute("db.collection.name", "users")

    return result
```

### Pattern: HTTP Client Operations with Semantic Conventions

**Before** (manual HTTP semantic conventions):
```python
with tracer.start_as_current_span("http.request") as span:
    # Manually add HTTP semantic conventions
    span.set_attribute("http.request.method", "GET")
    span.set_attribute("url.full", "https://api.github.com/users/octocat")

    async with httpx.AsyncClient() as client:
        response = await client.get("https://api.github.com/users/octocat")

        # Manually add response details
        span.set_attribute("http.response.status_code", response.status_code)
        span.set_attribute("http.response.body.size", len(response.content))

        return response.json()
```

**After** (automatic semantic conventions):
```python
from autotel import trace_http

@trace_http(method="GET", url="https://api.github.com/users/{username}")
async def get_github_user(ctx, username: str):
    # HTTP semantic conventions added automatically!
    async with httpx.AsyncClient() as client:
        response = await client.get(f"https://api.github.com/users/{username}")

        # Just add response details
        ctx.set_attribute("http.response.status_code", response.status_code)
        ctx.set_attribute("http.response.body.size", len(response.content))

        return response.json()
```

### Pattern: Messaging Operations with Semantic Conventions

**Before** (manual messaging semantic conventions):
```python
with tracer.start_as_current_span("kafka.publish") as span:
    # Manually add messaging semantic conventions
    span.set_attribute("messaging.system", "kafka")
    span.set_attribute("messaging.operation", "publish")
    span.set_attribute("messaging.destination.name", "order-events")
    span.set_attribute("messaging.message.id", order_id)
    span.set_attribute("messaging.kafka.partition", 2)

    producer.send('order-events', value=event_data)
    producer.flush()
```

**After** (automatic semantic conventions):
```python
from autotel import trace_messaging

@trace_messaging(system="kafka", operation="publish", destination="order-events")
async def publish_order_event(ctx, order_id: str, event_data: dict):
    # Messaging semantic conventions added automatically!
    producer.send('order-events', value=event_data)

    # Just add message-specific details
    ctx.set_attribute("messaging.message.id", order_id)
    ctx.set_attribute("messaging.kafka.partition", 2)

    producer.flush()
```

**Benefits of Semantic Convention Helpers:**
- Enforces OpenTelemetry standard semantic conventions automatically
- Reduces boilerplate by ~50-70% for instrumented operations
- Ensures consistency across your entire codebase
- Works with any OTel backend (provider-agnostic)
- Easy to add operation-specific attributes on top

## Testing Your Migration

### Unit Tests

**Before**:
```python
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor, InMemorySpanExporter

def test_tracing():
    exporter = InMemorySpanExporter()
    provider = TracerProvider()
    provider.add_span_processor(SimpleSpanProcessor(exporter))
    trace.set_tracer_provider(provider)

    # Test code
    my_function()

    # Assertions
    spans = exporter.get_finished_spans()
    assert len(spans) == 1
    assert spans[0].name == "my_function"
```

**After**:
```python
from autotel import init
from autotel.exporters import InMemorySpanExporter
from autotel.processors import SimpleSpanProcessor
from autotel.testing import assert_trace_created, assert_no_errors

def test_tracing():
    exporter = InMemorySpanExporter()
    init(service="test", span_processor=SimpleSpanProcessor(exporter))

    # Test code
    my_function()

    # Cleaner assertions
    assert_trace_created(exporter, "my_function")
    assert_no_errors(exporter)
```

### Integration Tests

1. **Compare trace structure**: Export traces from both implementations, compare span hierarchy
2. **Compare attributes**: Ensure same semantic attributes are present
3. **Compare metrics**: Verify metric names, labels, and values match
4. **Test error cases**: Ensure exceptions are captured correctly

### Validation Checklist

- [ ] All spans are created with correct names
- [ ] Semantic attributes follow conventions (http.method, db.statement, etc.)
- [ ] Parent-child relationships preserved
- [ ] Error spans marked correctly
- [ ] Metrics have expected labels
- [ ] Baggage propagates correctly
- [ ] Context propagates across service boundaries
- [ ] Performance is acceptable (autotel should be faster due to optimizations)

## Rollback Plan

### If Something Goes Wrong

1. **Keep old code in feature flag**:
```python
if os.getenv("USE_autotel", "false") == "true":
    import autotel
    autotel.init(service="my-service")
else:
    # Old OTEL setup
    setup_old_otel()
```

2. **Git branch strategy**:
   - Keep manual OTEL code in `main`
   - Migrate in `autotel-migration` branch
   - Merge after thorough testing

3. **Parallel run strategy**:
```python
# Run both for comparison
import autotel
autotel.init(service="my-service")

# Old OTEL still active, sending to different endpoint
setup_old_otel(endpoint="http://backup-collector:4318")
```

## Troubleshooting

### Issue: Missing spans after migration

**Cause**: Framework instrumentation not enabled

**Solution**:
```python
autotel.init(
    service="my-service",
    instrumentation=["flask", "requests", "httpx"]  # Add your frameworks
)
```

### Issue: Attributes not showing up

**Cause**: Using manual span creation instead of context manager

**Solution**:
```python
# Instead of:
span = autotel.get_active_span()
span.set_attribute("key", "value")

# Use:
with autotel.span("operation") as ctx:
    ctx.set_attribute("key", "value")
```

### Issue: Context not propagating

**Cause**: Missing HTTP header injection

**Solution**:
```python
from autotel.http import inject_trace_context

headers = inject_trace_context()
response = requests.post(url, headers=headers, json=data)
```

Or enable automatic HTTP instrumentation:
```python
autotel.init(
    service_name="my-service",
    instrumentation=["requests", "httpx"]  # Auto-injects headers
)
```

### Issue: Duplicate spans

**Cause**: Both manual and automatic instrumentation active

**Solution**: Remove manual span creation, rely on framework instrumentation

### Issue: Custom exporter not working

**Cause**: Trying to use raw OTEL exporter classes

**Solution**: Wrap in autotel's processor:
```python
from opentelemetry.exporter.custom import CustomExporter
from autotel.processors import BatchSpanProcessor

custom_exporter = CustomExporter(...)
processor = BatchSpanProcessor(custom_exporter)

autotel.init(
    service="my-service",
    span_processor=processor
)
```

### Issue: Performance degradation

**Cause**: Too many spans being created

**Solution**: Enable sampling:
```python
autotel.init(
    service="my-service",
    sampler=autotel.AdaptiveSampler(baseline_rate=0.1)  # 10% baseline
)
```

## Migration Checklist

Use this checklist to track your migration progress:

### Preparation
- [ ] Install autotel (`pip install autotel`)
- [ ] Document current OTEL setup
- [ ] Identify all manual instrumentation locations
- [ ] Create test plan

### Code Changes
- [ ] Replace OTEL setup with `autotel.init()`
- [ ] Remove manual span creation from endpoints
- [ ] Replace custom decorators with `@trace`
- [ ] Remove context propagation boilerplate
- [ ] Replace manual metrics with framework instrumentation
- [ ] Simplify error handling
- [ ] Update baggage usage to `with_baggage()`
- [ ] Replace cleanup code with `shutdown()`

### Testing
- [ ] Unit tests pass
- [ ] Integration tests pass
- [ ] Trace structure matches expected
- [ ] Semantic attributes correct
- [ ] Metrics reporting correctly
- [ ] Context propagation works
- [ ] Error handling works

### Deployment
- [ ] Deploy to staging
- [ ] Compare traces with production
- [ ] Monitor for errors/warnings
- [ ] Verify performance
- [ ] Deploy to production
- [ ] Monitor metrics

### Cleanup
- [ ] Remove old OTEL imports
- [ ] Remove custom instrumentation utilities
- [ ] Remove manual metric creation code
- [ ] Update documentation
- [ ] Archive old code for reference

## Getting Help

- **Documentation**: [README.md](./README.md)
- **Examples**: [examples/](./examples/)
- **Comparison examples**: [examples/comparison/](./examples/comparison/)
- **Issues**: [GitHub Issues](https://github.com/yourusername/autotel/issues)

## Results

After migration, teams typically report:

- 90-95% reduction in instrumentation code
- 3-5x faster to add new instrumented endpoints
- 50% fewer instrumentation bugs (can't forget to instrument)
- Easier onboarding (new developers productive in hours, not days)
- Better maintainability (framework updates don't break instrumentation)
