# Real-World Boilerplate Elimination Examples

This document shows exactly how autotel eliminates the boilerplate from two real production codebases.

## 📁 Source Examples

1. **MCP Context Propagation** (`otel_utils.py`)
   - Source: `langfuse-examples/applications/mcp-tracing/src/utils/otel_utils.py`
   - 135 lines of manual context propagation code
   - Custom decorators, extractors, injectors, wrapper classes

2. **Flask + Langfuse Instrumentation** (`app.py`)
   - Source: `langfuse-observability-demo/app/app.py`
   - 432 lines total, ~150 lines of setup + ~100 lines of manual instrumentation
   - Manual span creation, attribute setting, metric recording everywhere

## 🎯 Pattern 1: OpenTelemetry Setup Boilerplate

### ❌ Without autotel (80+ lines)

```python
from opentelemetry import trace, metrics
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader
from opentelemetry.exporter.otlp.proto.http.metric_exporter import OTLPMetricExporter
from opentelemetry.sdk._logs import LoggerProvider, LoggingHandler
from opentelemetry.sdk._logs.export import BatchLogRecordProcessor
from opentelemetry._logs import set_logger_provider
from opentelemetry.exporter.otlp.proto.http._log_exporter import OTLPLogExporter

# Manual resource setup
resource = Resource.create({
    "service.name": os.getenv("OTEL_SERVICE_NAME", "flask-api"),
    "service.namespace": "demo",
    "deployment.environment": os.getenv("APP_ENV", "dev"),
})

# Manual tracer setup
trace_provider = TracerProvider(resource=resource)
span_exporter = OTLPSpanExporter()
trace_provider.add_span_processor(BatchSpanProcessor(span_exporter))
trace.set_tracer_provider(trace_provider)

# Manual metrics setup
metric_exporter = OTLPMetricExporter()
metric_reader = PeriodicExportingMetricReader(metric_exporter)
meter_provider = MeterProvider(resource=resource, metric_readers=[metric_reader])
metrics.set_meter_provider(meter_provider)

# Manual logs setup
log_exporter = OTLPLogExporter()
log_provider = LoggerProvider(resource=resource)
log_provider.add_log_record_processor(BatchLogRecordProcessor(log_exporter))
set_logger_provider(log_provider)

# ... and more
```

### ✅ With autotel (3 lines)

```python
import autotel

autotel.init(
    service_name="flask-api",
    instrumentation=["flask", "openai"],  # Auto-instruments these frameworks
    subscribers=[
        autotel.subscribers.OTLPSubscriber(),  # OTLP export (traces, metrics, logs)
    ]
)
```

**Reduction: 95% (80+ lines → 3 lines)**

---

## 🎯 Pattern 2: Manual Span Creation & Instrumentation

### ❌ Without autotel (~25 lines per endpoint)

From `app.py:193-256`:

```python
@app.route("/askquestion", methods=["POST"])
def ask_question():
    started = time.perf_counter()

    # Manual metric recording
    request_counter.add(1, {"http.route": "/askquestion", "deployment.environment": _ENV})
    logger.info("request_received", extra={"request.id": request_id, "session.id": session_id})

    # Manual Langfuse span
    with lf.start_as_current_span(name="ask_question_request"):
        # Manual OTEL span with manual attributes
        with tracer.start_as_current_span(
            "ask_question_request",
            attributes={
                "lf.request_id": request_id,
                "lf.session_id": session_id,
                "http.route": "/askquestion",
            },
        ) as root_span:
            data = request.get_json(force=True) or {}
            question = data.get("question")

            # More manual span updates (30+ lines!)
            lf.update_current_span(
                name="ask_question_request",
                input={
                    "request_id": request_id,
                    "route": "/askquestion",
                    "client_ip": request.headers.get("X-Forwarded-For", request.remote_addr),
                    "headers": _scrub_headers(dict(request.headers)),
                    "userType": user_type,
                    "question": question,
                },
                metadata={"env": _ENV, "service": _SERVICE_NAME, "component": "ask_question"},
            )

            # Manual attribute setting on OTEL span
            root_span.set_attribute("lf.user_id", user_type)
            root_span.set_attribute("request.headers", str(_scrub_headers(dict(request.headers))))
            root_span.set_attribute("llm.input.userType", user_type)
            if question:
                root_span.set_attribute("llm.input.question", question)

            # ... business logic finally starts here
```

### ✅ With autotel (0 lines of instrumentation)

```python
@app.route("/askquestion", methods=["POST"])
def ask_question():
    # Flask auto-instrumentation creates spans automatically!
    # HTTP metrics recorded automatically!

    data = request.get_json(force=True) or {}
    question = data.get("question")

    # Optional: add custom attributes if needed
    span = autotel.get_active_span()
    if span:
        span.set_attribute("question.length", len(question))

    # ... business logic - that's it!
```

**Reduction: 100% (25+ lines → 0 lines per endpoint)**

---

## 🎯 Pattern 3: Nested Span Creation (LLM Calls)

### ❌ Without autotel (~40 lines)

From `app.py:258-360`:

```python
# Nested LLM span - another 10+ lines just to start it!
with tracer.start_as_current_span(
    "openai.chat.completions.create",
    attributes={
        "llm.vendor": "openai",
        "llm.model": DEFAULT_MODEL,
        "llm.input.role.system": f"You are answering as user type: {user_type}.",
        "lf.user_id": user_type,
        "lf.session_id": session_id,
    },
) as llm_span:
    # Langfuse observation (generation)
    with lf.start_as_current_observation(
        as_type="generation",
        name="openai-style-generation",
        model=DEFAULT_MODEL,
        input=[
            {"role": "system", "content": f"You are answering as user type: {user_type}."},
            {"role": "user", "content": question},
        ],
    ) as generation:
        completion = client.chat.completions.create(...)
        answer = completion.choices[0].message.content

        # Manual usage tracking (15+ lines!)
        usage = getattr(completion, "usage", None) or {}
        prompt_tokens = int(getattr(usage, "prompt_tokens", 0) or usage.get("prompt_tokens", 0) or 0)
        completion_tokens = int(getattr(usage, "completion_tokens", 0) or usage.get("completion_tokens", 0) or 0)

        # Manual metric recording
        token_counter.add(prompt_tokens, {"llm.token_type": "prompt", "llm.model": DEFAULT_MODEL})
        token_counter.add(completion_tokens, {"llm.token_type": "completion", "llm.model": DEFAULT_MODEL})

        # Manual span attribute setting (10+ lines!)
        llm_span.set_attribute("llm.usage.prompt_tokens", prompt_tokens)
        llm_span.set_attribute("llm.usage.completion_tokens", completion_tokens)
        llm_span.set_attribute("llm.usage.total_tokens", total_tokens)
        llm_span.set_attribute("llm.output.length", len(answer))

        # Manual Langfuse updates
        generation.update(
            output=answer,
            usage_details={...},
            cost_details={...},
        )

        # Propagate to parent span
        root_span.set_attribute("llm.usage.prompt_tokens", prompt_tokens)
        # ... more duplication
```

### ✅ With autotel (0-2 lines)

```python
# OpenAI is auto-instrumented via openllmetry integration!
completion = client.chat.completions.create(
    model="gpt-4",
    messages=[
        {"role": "system", "content": f"You are answering as user type: {user_type}."},
        {"role": "user", "content": question},
    ]
)
answer = completion.choices[0].message.content

# autotel automatically:
# - Creates "openai.chat.completions" span
# - Adds llm.vendor, llm.model attributes (semantic conventions)
# - Captures token usage (prompt_tokens, completion_tokens)
# - Records metrics for token usage
# - Propagates context to parent spans
# - Tracks errors if the call fails

# Optional: track as a business event
autotel.track("llm.completion", {
    "model": "gpt-4",
    "user_type": user_type,
    "tokens": completion.usage.total_tokens
})
```

**Reduction: 95% (40+ lines → 0-2 lines)**

---

## 🎯 Pattern 4: Context Propagation (MCP Servers)

### ❌ Without autotel (135 lines!)

From `otel_utils.py` - the entire file is boilerplate:

```python
# 20 lines: Extract context from _meta
def extract_otel_context_from_meta(meta: dict | None) -> Context:
    if not meta:
        return context.get_current()
    carrier = {}
    if "traceparent" in meta:
        carrier["traceparent"] = meta["traceparent"]
    if "tracestate" in meta:
        carrier["tracestate"] = meta["tracestate"]
    if "baggage" in meta:
        carrier["baggage"] = meta["baggage"]
    if carrier:
        propagator = get_global_textmap()
        return propagator.extract(carrier)
    return context.get_current()

# 10 lines: Inject context to _meta
def inject_otel_context_to_meta() -> dict:
    carrier = {}
    propagator = get_global_textmap()
    propagator.inject(carrier, context=context.get_current())
    return carrier

# 40 lines: Decorator for both sync and async
def with_otel_context_from_meta(func: F) -> F:
    @functools.wraps(func)
    def sync_wrapper(*args, **kwargs):
        meta = kwargs.get("_meta")
        ctx = extract_otel_context_from_meta(meta)
        token = context.attach(ctx)
        try:
            return func(*args, **kwargs)
        finally:
            context.detach(token)

    @functools.wraps(func)
    async def async_wrapper(*args, **kwargs):
        meta = kwargs.get("_meta")
        ctx = extract_otel_context_from_meta(meta)
        token = context.attach(ctx)
        try:
            return await func(*args, **kwargs)
        finally:
            context.detach(token)

    if asyncio.iscoroutinefunction(func):
        return async_wrapper
    else:
        return sync_wrapper

# 35 lines: Wrapper class
class TracedMCPServer:
    def __init__(self, server):
        self._server = server

    async def call_tool(self, tool_name: str, arguments: dict[str, Any] | None = None):
        if arguments is None:
            arguments = {}
        arguments["_meta"] = inject_otel_context_to_meta()
        return await self._server.call_tool(tool_name, arguments)

    def __getattr__(self, name: str) -> Any:
        return getattr(self._server, name)

# Usage - still requires manual work!
@with_otel_context_from_meta  # Must add this
async def my_tool(query: str, _meta: dict = None):  # Must add _meta param
    # ... finally your code
    pass

# Must manually wrap server
traced_server = TracedMCPServer(server)
```

### ✅ With autotel (0 lines!)

```python
import autotel

autotel.init(
    service_name="mcp-server",
    instrumentation=["mcp"],  # That's it!
)

# Your MCP tools just work - no decorators, no wrappers, no _meta parameters!
async def my_tool(query: str):
    # Context automatically:
    # - Extracted from _meta on incoming requests
    # - Injected into _meta on outgoing requests
    # - Propagated to child spans
    # - Works with HTTP, stdio, SSE transports
    result = f"Processed: {query}"
    return result
```

**Reduction: 100% (135 lines → 0 lines)**

---

## 🎯 Pattern 5: Error Handling & Metrics

### ❌ Without autotel (~30 lines per error type)

From `app.py:376-428`:

```python
except RateLimitError as e:
    status_code = 429
    logger.exception("openai_rate_limited", extra={"request.id": request_id})
    lf.update_current_span(
        output={"error": "rate_limit", "detail": str(e)},
        metadata={"status": "rate_limited", "http_status": status_code},
    )
    lf.update_current_trace(metadata={"error": True, "error.type": "RateLimitError"})

except AuthenticationError as e:
    status_code = 401
    logger.exception("openai_auth_error", extra={"request.id": request_id})
    lf.update_current_span(
        output={"error": "auth_error", "detail": "Invalid or missing API key."},
        metadata={"status": "auth_error", "http_status": status_code},
    )
    lf.update_current_trace(metadata={"error": True, "error.type": "AuthenticationError"})

# ... repeat for every error type

finally:
    if status_code != 200:
        latency_ms = int((time.perf_counter() - started) * 1000)
        latency_hist.record(latency_ms, {"http.route": "/askquestion", "http.status_code": status_code})
        logger.error("request_error", extra={"request.id": request_id, "http.status_code": status_code})
        span = trace.get_current_span()
        if span:
            span.set_attribute("error", True)
            span.set_attribute("error.status_code", status_code)
```

### ✅ With autotel (0 lines!)

```python
# Just write normal error handling - autotel captures everything!
try:
    completion = client.chat.completions.create(...)
except RateLimitError as e:
    # autotel automatically:
    # - Records exception on current span
    # - Sets error=True attribute
    # - Sets error.type=RateLimitError
    # - Captures stack trace
    # - Updates metrics (error count)
    # Your code just needs to handle the error
    return {"error": "Rate limited"}, 429
```

**Reduction: 100% (30+ lines → 0 lines)**

---

## 🎯 Pattern 6: Baggage Propagation

### ❌ Without autotel (Manual context management)

```python
from opentelemetry import context
from opentelemetry.baggage import propagation

# Manual baggage setting
current_context = context.get_current()
new_context = propagation.set_baggage("user.id", "123", current_context)
new_context = propagation.set_baggage("tenant.id", "456", new_context)
token = context.attach(new_context)
try:
    # Make calls
    make_downstream_call()
finally:
    context.detach(token)

# Manual baggage reading
current_context = context.get_current()
baggage = propagation.get_all(current_context)
user_id = baggage.get("user.id") if baggage else None
```

### ✅ With autotel (Simple context manager)

```python
# Set baggage with simple context manager
with autotel.with_baggage({
    "user.id": "123",
    "tenant.id": "456"
}):
    # Baggage automatically propagates through:
    # - HTTP headers
    # - MCP _meta fields
    # - gRPC metadata
    make_downstream_call()

# Read baggage with simple API
with autotel.span("handler") as ctx:
    user_id = ctx.get_baggage("user.id")
    all_baggage = ctx.get_all_baggage()  # Get everything
```

**Reduction: 90% (15+ lines → 2-3 lines)**

---

## 📊 Overall Impact Summary

| File | Before | After | Reduction |
|------|--------|-------|-----------|
| `otel_utils.py` | 135 lines | 0 lines | **100%** |
| `app.py` setup | 80 lines | 3 lines | **96%** |
| `app.py` per-endpoint | 25-30 lines | 0-2 lines | **93%** |
| **Total** | **~250 lines** | **~5 lines** | **~98%** |

## 🚀 Migration Path

### Step 1: Replace Setup Code

```python
# Remove ALL of this:
# - from opentelemetry import ...
# - resource = Resource.create(...)
# - trace_provider = TracerProvider(...)
# - meter_provider = MeterProvider(...)
# - log_provider = LoggerProvider(...)

# Replace with:
import autotel

autotel.init(
    service_name="your-service",
    instrumentation=["flask", "openai", "requests"],
    subscribers=[
        autotel.subscribers.OTLPSubscriber(),
    ]
)
```

### Step 2: Remove Manual Instrumentation

```python
# Remove manual span creation:
# - with tracer.start_as_current_span(...) as span:
# - span.set_attribute(...)
# - span.set_attribute(...)

# Framework instrumentation handles it automatically!
# Just write your business logic.
```

### Step 3: Remove Context Propagation Boilerplate

```python
# Remove:
# - extract_otel_context_from_meta()
# - inject_otel_context_to_meta()
# - with_otel_context_from_meta decorator
# - TracedMCPServer wrapper

# autotel handles propagation automatically!
```

### Step 4: Optional Custom Events

```python
# Only add custom tracking where it provides business value:
autotel.track("order.created", {
    "order_id": order.id,
    "amount": order.total,
    "user_id": user.id
})
```

## ✨ Key Benefits

1. **Developer Productivity**: 95%+ less boilerplate means faster development
2. **Maintainability**: Framework updates don't break instrumentation
3. **Consistency**: Semantic conventions enforced automatically
4. **Reliability**: Can't forget to instrument code paths
5. **Performance**: Optimized instrumentation, automatic batching
6. **Flexibility**: Easy to switch between OTLP, PostHog, webhooks, etc.

## 🎓 See Also

- [`before_autotel.py`](./before_autotel.py) - Full manual implementation
- [`after_autotel.py`](./after_autotel.py) - Same with autotel
- [`COMPARISON.md`](./COMPARISON.md) - Detailed comparison metrics
