# autotel Documentation

Ergonomic OpenTelemetry instrumentation for Python.

## Installation

```bash
pip install autotel
# or
uv add autotel
```

## Quick Start

**One-line initialization:**

```python
from autotel import init
init(service="my-service", endpoint="http://localhost:4318")
```

**Ergonomic decorator:**

```python
from autotel import trace

@trace
async def create_user(ctx, data):
    ctx.set_attribute('user.id', data['id'])
    return await db.users.create(data)
```

## Features

- ✅ One-line initialization with no boilerplate, reads standard OTEL env vars
- ✅ Ergonomic `@trace` decorator instead of verbose OTel syntax
- ✅ Auto-detected `ctx` parameter for span operations
- ✅ Convenience helpers: `set_attribute()`, `get_trace_id()`, etc. without needing spans
- ✅ Semantic helpers: `@trace_llm`, `@trace_db`, `@trace_http`, `@trace_messaging`
- ✅ Product events: `track()` function with auto-enriched trace context
- ✅ Production features: adaptive sampling, rate limiting, circuit breakers
- ✅ PII redaction: built-in PII detection and redaction
- ✅ Baggage support: context propagation with `with_baggage()`
- ✅ Framework integrations: FastAPI, Django, Flask middleware
- ✅ Serverless support: AWS Lambda, Google Cloud Functions, Azure Functions
- ✅ OpenLLMetry integration: auto-instrument LLM SDKs
- ✅ Validation utilities: event name and attribute validation

## Contents

```{toctree}
:maxdepth: 2
:caption: User Guide

quickstart
```

## Indices and tables

* {ref}`genindex`
* {ref}`modindex`
* {ref}`search`
