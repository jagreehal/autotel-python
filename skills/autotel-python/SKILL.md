---
name: autotel-python
description: >
  Instruments Python services with the autotel package: init, trace spans, FastAPI,
  Flask, Django and gRPC integrations, database and GenAI helpers, wide events, tail
  sampling and PII redaction. Use when adding OpenTelemetry to a Python service, calling
  autotel.init, choosing between autotel APIs, or testing emitted spans.
---

# autotel for Python

`autotel` wraps the OpenTelemetry SDK. Call `init` once, decorate work with `@trace`, and telemetry goes out over OTLP.

Add `autotel` as the only telemetry dependency, so skip `opentelemetry-sdk` and exporter setup code.

## Setup

```sh
pip install 'autotel[fastapi]'  # or flask, django, grpc, events, logging
```

```python
from autotel import AdaptiveSampler, init

init(
    service="orders",
    redact_pii=True,
    sampler=AdaptiveSampler.production(),
)
```

- `OTEL_*` variables beat arguments to `init`, except a preset's endpoint. Leave `endpoint` out of code and set `OTEL_EXPORTER_OTLP_ENDPOINT`.
- An endpoint turns on traces, metrics and logs. `metrics=False`, `logs=False` or `OTEL_{METRICS,LOGS}_EXPORTER=none` leave one out, which Jaeger needs because it takes only traces.
- `OTEL_{TRACES,METRICS,LOGS}_EXPORTER=none` silences one signal. An unknown `OTEL_EXPORTER_OTLP_PROTOCOL` fails `init`; Python has no `http/json` exporter.
- Workers and scripts should call `install_signal_handlers()`, because SIGTERM skips `atexit`. uvicorn and gunicorn handle it themselves.
- In a Lambda handler, call `flush()` at the end.

## Which API

| Need | API |
| --- | --- |
| Span around a function | `@trace`, with an optional first `ctx` parameter |
| Span around a block | `with span("name") as ctx:` |
| Attributes and events | `ctx.set_attribute(k, v)`, `ctx.add_event(name, attrs)` |
| Incoming HTTP on FastAPI or Starlette | `app.add_middleware(autotelMiddleware)` |
| Incoming HTTP on Flask | `init_autotel(app, skip_routes=["/health"])` |
| Incoming HTTP on Django | `autotel.integrations.django.autotelMiddleware` plus an `AUTOTEL` setting |
| Incoming gRPC | `grpc.server(pool, interceptors=[server_interceptor()])`, or `aio_server_interceptor()` |
| Skip health checks on FastAPI 0.142+ | `FastAPI(telemetry={"exclude": lambda scope: scope["path"] == "/health"})` |
| Outgoing HTTP | `await fetch("GET", url)`, or `trace_http_request` with `inject_trace_context()` |
| Incoming context by hand | `extract_trace_context(headers)` from `autotel.http` |
| Database calls | `trace_db_query`, `ctx.set_query_text(sql)`, `instrument_database` |
| One wide event per request | `get_request_logger().set({...})` |
| Keep a trace through sampling | `force_keep()`, `ctx.force_keep()`, `AdaptiveSampler.keep_when(...)` |
| Log files | `init(log_file=LogFile("logs"))` or `AUTOTEL_LOG_FILE_DIR` |
| JSON logs with `trace_id` | `init(json_logs=True)` |
| Business events | `init(subscribers=[...])`, then `track("name", {...})` |
| GenAI spans and metrics | `@trace_llm`, `invoke_agent`, `execute_tool`, `GenAiMetrics` |
| SLOs | `SloTracker("checkout", 0.999, 3600)` |
| Runtime metrics | `register_runtime_metrics()` after `init` |
| Assert on spans in tests | `TraceCollector("name")` from `autotel.testing` |

## Mistakes to avoid

- Don't put IDs in span names. Keep them in attributes, because names are a cardinality limit.
- Don't add `autotelMiddleware` and `opentelemetry-instrumentation-fastapi` together.
- Calling `init()` on FastAPI without the middleware? Pass `FastAPI(telemetry={"auto_configure": False})`, or releases that enable it, like 0.142, export a second, unredacted copy.
- `skip_routes` can't stop FastAPI's native span. Use `FastAPI(telemetry={"exclude": ...})`.
- `AdaptiveSampler` decides per process. For one decision across services, use the Collector's `tail_sampling` processor.
