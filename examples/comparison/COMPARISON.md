# autotel: Boilerplate Elimination Comparison

This directory contains real-world examples showing how autotel eliminates observability boilerplate.

## 📊 The Numbers

| Metric | Before (Manual OTEL) | After (autotel) | Reduction |
|--------|---------------------|---------------------|-----------|
| **Setup Lines** | 80-100 lines | 3-5 lines | **95%** |
| **Context Propagation** | 135 lines | 0 lines | **100%** |
| **Per-Endpoint Instrumentation** | 20-30 lines | 0-2 lines | **93%** |
| **Import Statements** | 15-20 imports | 1 import | **95%** |
| **Maintenance Burden** | High (manual attribute sync) | Low (automatic) | **~90%** |

## 🎯 What You Get

### Before: Manual OpenTelemetry (`before_autotel.py`)

**Problems:**
- **80+ lines** of setup code (providers, exporters, processors)
- **15+ import statements** just for observability
- **Manual span creation** everywhere (10+ lines per span)
- **Manual attribute setting** (5-10 `span.set_attribute()` calls per operation)
- **Manual metric recording** scattered throughout business logic
- **Manual error handling** in every try/catch block
- **Manual context propagation** requiring custom decorators

```python
# Just to start ONE span with attributes:
with tracer.start_as_current_span(
    "my_operation",
    attributes={
        "attr1": value1,
        "attr2": value2,
        # ... more attributes
    }
) as span:
    # Business logic
    span.set_attribute("result", ...)
    span.set_attribute("status", ...)
    # More manual work
```

### After: autotel (`after_autotel.py`)

**Benefits:**
- **3-5 lines** of setup
- **1 import statement**: `import autotel`
- **Automatic span creation** from framework instrumentation
- **Automatic attribute capture** based on semantic conventions
- **Automatic metric generation** from events
- **Automatic error tracking**
- **Automatic context propagation**

```python
# Same operation, zero manual instrumentation:
def my_operation():
    # Business logic - that's it!
    # autotel automatically:
    # - Creates spans from framework calls
    # - Adds semantic attributes
    # - Propagates context
    # - Tracks errors
    # - Records metrics
```

## 🔄 Context Propagation Example

### Before: Manual Context Propagation (`context_propagation_before.py`)

For distributed tracing across MCP servers, you need:

1. **Extract function** (20 lines) - manually extract context from metadata
2. **Inject function** (15 lines) - manually inject context into metadata
3. **Decorator** (40 lines) - handle both sync/async, attach/detach context
4. **Wrapper class** (35 lines) - wrap servers to inject context
5. **Manual application** - add decorators to every tool, wrap every server

**Total: 135 lines of boilerplate** 😱

### After: autotel (`context_propagation_after.py`)

```python
autotel.init(
    service_name="mcp-server",
    instrumentation=["mcp"],  # That's it!
)
```

**Total: 3 lines** 🎉

autotel automatically:
- Extracts context from incoming requests (_meta fields, HTTP headers, etc.)
- Propagates context to child operations
- Injects context into outgoing requests
- Handles sync and async functions
- Works across all transports (HTTP, stdio, SSE)

## 🏗️ Real-World Impact

### Example: Flask App with LLM Calls

| Aspect | Manual OTEL | autotel |
|--------|-------------|-------------|
| Total lines (example app) | 432 | ~80 |
| Setup code | 80 lines | 3 lines |
| Observability code per endpoint | 25-30 lines | 0-2 lines |
| Developer mental overhead | High | Low |
| Maintainability | Brittle (manual sync) | Robust (auto-sync) |

### Code-to-Instrumentation Ratio

**Before autotel:**
- Business logic: ~50 lines
- Observability code: ~150 lines
- **Ratio: 1:3** (more instrumentation than business logic!)

**After autotel:**
- Business logic: ~50 lines
- Observability code: ~5 lines
- **Ratio: 10:1** (focus on what matters!)

## 🎓 Key Patterns Eliminated

### 1. Manual Provider Setup
```python
# BEFORE: 40+ lines
resource = Resource.create({...})
trace_provider = TracerProvider(resource=resource)
span_exporter = OTLPHTTPSpanExporter()
trace_provider.add_span_processor(BatchSpanProcessor(span_exporter))
# ... repeat for metrics, logs

# AFTER: 1 line
autotel.init(service_name="my-service")
```

### 2. Manual Span Instrumentation
```python
# BEFORE: 10+ lines per operation
with tracer.start_as_current_span("operation", attributes={...}) as span:
    result = do_work()
    span.set_attribute("result.size", len(result))
    span.set_attribute("result.type", type(result))
    # More manual attribute setting...

# AFTER: 0 lines
result = do_work()  # autotel handles it via framework instrumentation
```

### 3. Manual Metric Recording
```python
# BEFORE: Create metric + record everywhere
request_counter = meter.create_counter("http.requests", ...)
# ... in handler:
request_counter.add(1, {"route": "/api", "status": 200})

# AFTER: Automatic
# autotel automatically creates and records HTTP metrics
```

### 4. Manual Context Propagation
```python
# BEFORE: Custom decorators, wrapper classes, manual extraction/injection
@with_otel_context_from_meta
def my_tool(..., _meta: dict = None):
    ...

# AFTER: Nothing
def my_tool(...):  # Context propagates automatically
    ...
```

### 5. Manual Error Tracking
```python
# BEFORE: Try/catch with manual span updates
try:
    result = do_work()
except Exception as e:
    span.set_attribute("error", True)
    span.set_attribute("error.type", type(e).__name__)
    span.set_attribute("error.message", str(e))
    raise

# AFTER: Automatic
result = do_work()  # Errors automatically captured
```

## 💡 Why This Matters

### Developer Experience
- **Onboarding**: Minutes instead of days to add observability
- **Maintenance**: Changes to business logic don't require updating instrumentation
- **Debugging**: Less instrumentation code means less to debug

### Code Quality
- **Separation of Concerns**: Business logic isn't polluted with observability code
- **DRY Principle**: No repetitive instrumentation patterns
- **Testability**: Test business logic without mocking observability libraries

### Production Reliability
- **Consistency**: Semantic conventions enforced automatically
- **Completeness**: Won't forget to instrument a code path
- **Performance**: Optimized instrumentation paths, automatic batching

## 🚀 Getting Started

```python
# Install
pip install autotel

# Add to your app
import autotel

autotel.init(
    service_name="my-service",
    instrumentation=["flask", "openai", "requests"],
    subscribers=[
        autotel.subscribers.OTLPSubscriber(),
    ]
)

# That's it! Your app is now fully instrumented.
```

## 📚 See Also

- [Basic Examples](../basic/) - Simple usage patterns
- [PydanticAI Examples](../pydantic_ai/) - AI framework integration
- [Documentation](../../README.md) - Full API reference
