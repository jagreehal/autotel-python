# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.9.1] - 2026-11-08

### Changed
- `RedactPiiProcessor` and `RedactPiiLogProcessor` work on providers you build yourself, with no `init()` call.
- Redacted spans are plain `ReadableSpan` copies, so `ConsoleSpanExporter` and other `to_json()` users print scrubbed values.
- `RedactPiiLogProcessor` scrubs dict and list log bodies key by key.
- gRPC handlers run with the caller's baggage, and it propagates to downstream calls.

## [0.9.0] - 2026-11-04

### Added
- `autotel.integrations.grpc`: `server_interceptor()` and `aio_server_interceptor()` record gRPC SERVER spans and `rpc.server.call.duration`, with the span open for the whole stream.
- `init(redact_pii=True)` scrubs span attributes, span events, exception text, logs and `track()` events at export. `RedactPiiProcessor` does the same for your own tracer provider.
- `init(log_file=LogFile("logs"))` and `AUTOTEL_LOG_FILE_{DIR,ROTATION,MAX_FILES}` write rolling log files. `init(json_logs=True)` prints JSON lines with `trace_id`.
- `force_keep()`, `ctx.force_keep()`, `ctx.debug_requested()`, `AdaptiveSampler.production()`, `.errors_only()` and `.keep_when()`. `autotel.debug` baggage keeps a trace.
- `http.server.request.duration`, `http.client.request.duration`, `db.client.operation.duration` and `rpc.server.call.duration`.
- `fetch()`, `sanitize_query()`, `ctx.set_query_text()`, `install_signal_handlers()`, `SloTracker`, `GenAiMetrics`, `record_evaluation()`, `register_runtime_metrics()`, and `TraceCollector` in `autotel.testing`.
- Server integrations gain `skip_routes`, `trace_response_header`, `trust_forwarded_for` and `raw_query`. `service` is optional once `init()` has run.
- `OTEL_{TRACES,METRICS,LOGS}_EXPORTER=none` silences one signal, and `init` logs where telemetry goes.
- `autotel[grpc]` extra, `SECURITY.md`, a `docker-compose.yml` with Jaeger, and the `autotel-python` agent skill.

### Changed
- **Breaking:** spans use stable semantic conventions. `http.method`, `http.url`, `http.status_code`, `db.system`, `db.name`, `db.operation`, `db.statement`, `db.sql.table` and `db.rows_affected` become `http.request.method`, `url.full`, `http.response.status_code`, `db.system.name`, `db.namespace`, `db.operation.name`, `db.query.text`, `db.collection.name` and `db.response.returned_rows`. `gen.ai.*` becomes `gen_ai.*`. Update dashboards and queries that read the old names.
- **Breaking:** the FastAPI, Flask and Django integrations record SERVER spans with `url.path` and `url.query` in place of `http.url`, and drop `http.duration_ms`, `http.status_text`, `http.slow_request` and the raw path as `http.route`. A request that matches no route is named after its method only.
- **Breaking:** HTTP client spans are named after the method (`GET`), as the conventions ask, and `url.full` has no credentials and hides query values.
- **Breaking:** `AdaptiveSampler` decides per trace, not per span, from a hash of the trace id. Spans wait in memory until their trace finishes.
- **Breaking:** an unknown `OTEL_EXPORTER_OTLP_PROTOCOL` now fails `init` rather than being ignored.
- **Breaking:** `OTEL_*` variables beat arguments to `init()`, so a deployment changes behaviour with no code change. A preset's endpoint and protocol still win, so a preset stays on its vendor.
- **Breaking:** setting an endpoint turns on traces, metrics and logs, and logs raise a root logger left at WARNING to INFO. Pass `metrics=False` or `logs=False`, or set `OTEL_METRICS_EXPORTER=none` and `OTEL_LOGS_EXPORTER=none` for trace-only backends like Jaeger. Presets decide for themselves.
- Semantic helpers set a span kind: CLIENT for HTTP, database and LLM calls, PRODUCER or CONSUMER for messaging.
- `autotel[fastapi]`, `[flask]` and `[django]` install the framework instead of `opentelemetry-instrumentation-*` packages that autotel never used.
- Django reads the `AUTOTEL` setting. The documented lowercase `autotel` setting never worked, because Django ignores lowercase settings.
- Dependencies upgraded to the latest releases (opentelemetry 1.45.2, pydantic 2.14.2, fastapi 0.162.0 and others), with `opentelemetry-*` and `pydantic` floors raised to match.
- CI runs the integration tests, builds the package, audits dependencies and runs weekly.

### Fixed
- FastAPI 0.142+ no longer gets a duplicate span per request. The middleware adds to FastAPI's native SERVER span.
- With the middleware, FastAPI's `auto_configure` (on by default in 0.142) no longer adds a second set of OTLP exporters, which sent everything twice and skipped `redact_pii` and tail sampling.
- `OTEL_EXPORTER_OTLP_{TRACES,METRICS,LOGS}_ENDPOINT` are used as is, as the OTel spec says, with no `/v1/traces` appended. The `traces_endpoint=` style parameters still append it.
- `OTEL_EXPORTER_OTLP_HEADERS` values are percent-decoded, so Grafana Cloud's `Authorization=Basic%20...` authenticates.
- Server spans join the caller's trace on Flask and Django, and spans created in a handler are their children. The FastAPI middleware no longer uses `BaseHTTPMiddleware`, and it ends the span after a streamed response.
- Flask and Django no longer end a span twice on an exception, and Flask no longer re-runs `init()` over the app's own setup.
- `@trace_http`, `@trace_db` and `@trace_messaging` no longer raise `UnboundLocalError` on async functions without a `ctx` parameter.
- `db.query.text` handles comments, double quotes, escapes and dollar quoting by dialect, and an unterminated string can't leak what follows it.
- A second `init()` replaces the meter provider, as it does for traces and logs.

## [0.8.1] - 2026-10-03

### Fixed
- MCP client instrumentation works with mcp 2.x (protocol 2026-07-28, stateless). Trace context goes in the SDK's `meta=` argument rather than `_meta=`, which raised `TypeError: call_tool() got an unexpected keyword argument '_meta'`, and it no longer leaks into tool `arguments`. Clients that accept no meta argument are left alone.

### Changed
- Lockfile upgraded to the latest dependencies (mcp 2.3.0, pydantic-ai 2.54.0, openai 3.24.0, fastapi 0.142.2, and others).

## [0.8.0] - 2026-10-03

### Added
- `get_request_logger()` and `RequestLogger` collect nested fields onto the active span as one wide event per request.
- `FileSubscriber` writes `track()` events to an NDJSON file, with or without an event loop.
- `autotel.http.extract_trace_context()` reads W3C trace context and baggage from incoming headers.
- `bucket()` and `compare_cohorts()` in `autotel.analysis` for comparing slow and normal requests.
- `trace("name", fn)` wraps an existing function.
- `AUTOTEL_DEBUG_BAGGAGE_KEY` constant (`"autotel.debug"`).

### Changed
- `init(metrics=True)` also sets up the `Metric` helper, so `create_counter()` and `create_histogram()` work straight away.
- `shutdown()` sends events still queued for async subscribers when no event loop was running.

## [0.7.0] - 2026-10-03

### Added
- `init(export_timeout=...)` sets the timeout for OTLP exporters and batch processors. `simple` mode defaults to 2 seconds.
- `langfuse_preset()` sends traces to Langfuse over OTLP.
- `invoke_agent()` and `execute_tool()` context managers for GenAI agent and tool spans.

### Changed
- `shutdown()` shuts down the tracer provider and works again after a re-`init()`.
- `shutdown_sync()` flushes and shuts down providers when called inside a running event loop.

## [0.6.0] - 2026-09-22

### Added
- **Log export over OTLP.** With `logs=True`, autotel exports stdlib logging records through the
  OTLP logs pipeline, linked to the active span. A structlog or loguru logger passed as `logger=`
  joins the same pipeline, including structlog loggers bound or cached before `init()`.
  Re-initializing replaces only autotel's handlers and leaves application handlers in place.
- **`flush()`** exports buffered spans, metrics and logs and keeps autotel running, for serverless
  handlers on warm containers.
- **`@trace("span.name")`** accepts the span name as the first argument.
- **`pydantic_ai=` mapping** passes settings to `InstrumentationSettings`,
  e.g. `{"include_content": False}`.
- **`datadog_preset()`** targets Datadog's OTLP intake and adds `version`, `enable_logs`,
  `llmobs` (routes `gen_ai.*` spans to Agent Observability) and local Agent mode
  (`use_agent`, `agent_host`, `agent_port`). A preset's `logs` setting applies when `init()`
  omits `logs`.

### Changed
- Dependency floors: `pydantic` >=2.13.5, `pydantic-ai` extra >=2.47.0.

## [0.5.0] - 2026-08-02

> Minimum supported dependency versions are raised, which is a compatibility break
> for downstreams, not a patch-level refresh.

### Changed
- **BREAKING: minimum supported dependency versions raised.** These are floors, not just
  lockfile pins — installs that previously resolved will now fail to. Runtime dependencies:
  `opentelemetry-{api,sdk,exporter-otlp}` >=1.38 → **>=1.44**, `pydantic` >=2.12 → **>=2.13.4**,
  `typing-extensions` >=4.12 → **>=4.16**. Optional extras: `opentelemetry-instrumentation-{fastapi,django,flask}`
  >=0.47b0 → **>=0.65b0**, `pydantic-ai` >=1.19 → **>=2.22** (a major version of that project),
  `structlog` >=25.5 → **>=26.1**, `ollama` >=0.6.0 → **>=0.6.2**, `httpx` >=0.28.0 → **>=0.28.1**.
  Consumers pinned below any of these must upgrade before taking this release.
  Every floor is set to the latest release published at the time of the bump.
- **Dev toolchain** — `pytest` 9.1.1, `pytest-asyncio` 1.4.0, `pytest-cov` 7.1.0, `ruff` 0.16.1,
  `mypy` 2.3.0, `fastapi` 0.141.1, `build` 1.5.0, `twine` 7.0.0. Dev-only; no consumer impact.
- **mypy hardening** — `warn_unused_ignores` is now `true`. The previous `false` existed to keep
  version-dependent ignores valid across the 1.38–1.42 SDK typing change; raising the
  `opentelemetry-sdk` floor to 1.44 puts the whole supported range past that change, so the
  suppression is no longer needed. Removed two now-dead `# type: ignore[no-untyped-call]`
  comments in `shutdown` / `init`, and gave `mcp._patch_mcp_class` an explicit `target: Any`
  boundary for the dynamically-patched third-party class instead of two new inline ignores.
  Note this coupling: reverting the SDK floor to <1.42 requires reverting `warn_unused_ignores` too.

### Internal
- CI now installs `.[dev,all]`, so the optional extras' version floors are proven to resolve on
  every supported Python version (3.10–3.13) rather than going unexercised.
- CI lints the whole tree (`ruff check .`) instead of just `src tests`; `examples/` and `scripts/`
  are exempted from `ARG001` for the same reason `tests/` already is — decorators supply
  `ctx`/payload arguments positionally, so demo signatures keep parameters their bodies ignore.

## [0.4.1] - 2026-05-30

### Documentation
- Added a grouped `init()` configuration reference (`docs/configuration.md`) and a "minimal vs advanced" Configuration section in the README, so advanced parameters (`devtools`, `span_processor_mode`, samplers, redactors, …) are discoverable without grepping the source.
- Framed the "value compounds" positioning: the one-line setup is the entry point; the same `init()` is where product events, enrichment, sampling, and PII redaction are opted into as you grow.
- Fixed a duplicate `## Why autotel` heading in the README.

## [0.4.0] - 2026-05-30

### Changed
- **Debug/console output is now opt-in.** Previously `init()` auto-enabled console span output in any non-production environment, so a plain `init()` in a notebook, script, or CLI printed spans the user never asked for. Console output is now off unless you explicitly pass `debug=True`.

  **Migration:** if you relied on automatic console spans during local development, pass `debug=True` explicitly:
  ```python
  init(service="my-app", debug=True)
  ```

### Changed
- CI: bumped `actions/checkout` to v5 and `actions/setup-python` to v6 so workflows run on Node 24 (Node 20 actions are deprecated). No package/runtime changes.

## [0.3.0] - 2026-05-30

### Fixed
- `sampling`: `AdaptiveSamplingProcessor` now subclasses `SpanProcessor`, fixing an `AttributeError: '_on_ending'` crash under opentelemetry-sdk >= 1.42; corrected its `on_start` signature to the SDK's `Context` type
- `workflow_distributed`: guarded a possibly-unbound `ctx` in decorator error paths so `on_error` can no longer raise `NameError` and mask the original exception
- `span_processors`: filter / attribute-redactor failures are now logged instead of silently swallowed (fail-open behavior preserved)
- `messaging`: corrected `extract_trace_context` return type and narrowed span access in `record_dlq` / `record_retry` / `record_consumer_lag`

### Changed
- De-duplicated the async/sync tracing-decorator bodies in `workflow_distributed`, `webhook`, and `messaging` into shared per-decorator helpers (behavior-preserving) and added characterization tests for previously-untested paths

### Added

#### Event-Driven Observability
- **Webhook/Parking Lot Pattern** (`webhook.py`) - "Park" trace context for async callbacks that arrive hours/days later (webhooks, payment callbacks, human approvals)
  - `ParkingLot` class with pluggable storage backends (`TraceContextStore` interface)
  - `InMemoryTraceContextStore` for development/testing
  - `@parking_lot.trace_callback` decorator for callback handlers
  - `CallbackContext` with elapsed time tracking and original trace linking
  - `create_correlation_key()` helper for consistent key generation

- **Distributed Workflow Tracing** (`workflow_distributed.py`) - Track workflows spanning multiple microservices via W3C baggage
  - `@trace_distributed_workflow` decorator for workflow entry points
  - `@trace_distributed_step` decorator for downstream service steps
  - `WorkflowBaggage` pre-built safe schema for workflow identity propagation
  - `DistributedWorkflowContext` and `DistributedStepContext` with workflow metadata
  - Automatic baggage propagation (workflow_id, step_name, step_index, priority)
  - `create_workflow_headers()`, `generate_workflow_id()`, `get_workflow_progress()` helpers

- **Messaging Adapters** (`messaging_adapters.py`) - Pre-built adapters for common messaging systems
  - `nats_adapter` - NATS JetStream with subject/stream attributes
  - `temporal_adapter` - Temporal workflow engine integration
  - `cloudflare_queues_adapter` - Cloudflare Queues support
  - `sqs_adapter` - AWS SQS with message attributes
  - `redis_streams_adapter` - Redis Streams with stream/group tracking
  - `MessagingAdapter`, `ProducerAdapter`, `ConsumerAdapter` base types

- **Context Extractors** - Parse trace context from non-W3C header formats
  - `datadog_context_extractor` - Datadog `x-datadog-*` headers
  - `b3_context_extractor` - Zipkin B3 headers (single and multi-header)
  - `xray_context_extractor` - AWS X-Ray `X-Amzn-Trace-Id` header
  - `jaeger_context_extractor` - Jaeger `uber-trace-id` header
  - `create_multi_format_extractor()` - Try multiple formats in order
  - `default_multi_format_extractor` - Pre-built extractor for common formats

- **Enhanced Messaging Context** - Rich context classes for message producers/consumers
  - `ProducerContext` with `inject_headers()` for automatic trace propagation
  - `ConsumerContext` with DLQ helpers (`should_dlq()`, `send_to_dlq()`, `record_dlq_decision()`)
  - `ConsumerContext` with retry helpers (`should_retry()`, `get_retry_delay()`)
  - `ConsumerContext` with ordering helpers (`is_duplicate()`, `get_out_of_order_info()`)
  - `DLQOptions`, `DLQReplayOptions` for dead-letter queue configuration
  - `OrderingConfig`, `OutOfOrderInfo` for message ordering validation
  - `ConsumerGroupState`, `ConsumerGroupTrackingConfig` for group coordination
  - `record_consumer_lag()`, `record_dlq()`, `record_retry()` metric helpers

- **Safe Baggage Schema** (`business_baggage.py`) - Type-safe baggage with guardrails
  - `create_safe_baggage_schema()` factory function
  - `SafeBaggageSchema` class with validation, PII detection, size limits
  - Support for string, number, boolean, enum field types
  - Automatic hashing of high-cardinality values
  - Configurable prefix for baggage keys

## [0.2.0] - 2025-12-03

### Added
- **Array attributes support** - `set_attribute()` now accepts homogeneous arrays (`list[str]`, `list[int]`, `list[float]`, `list[bool]`)
- **Batch attributes** - `set_attributes()` method for setting multiple attributes at once
- **Span links** - `add_link()` and `add_links()` methods for linking related spans
- **Dynamic span naming** - `update_name()` method for renaming spans after creation
- **Recording check** - `is_recording()` method to check if span is recording (useful for avoiding expensive computation)
- **PostHogSubscriber enhancements**:
  - `serverless=True` mode for AWS Lambda/Vercel (shorter timeout)
  - `filter_none_values=True` (default) removes None from properties
  - `on_error` callback for custom error handling

### Fixed
- Fixed name shadowing bug in `functional.py` where `trace` function shadowed `opentelemetry.trace` module

## [0.1.0] - 2025-11-26

### Initial Release

#### Added
- One-line initialization with `init()` supporting standard OTEL environment variables
- `@trace` decorator for sync and async functions
- `TraceContext` for span operations with auto-detected `ctx` parameter
- Convenience helpers: `set_attribute()`, `get_trace_id()`, `add_event()`, etc.
- Semantic convention helpers: `@trace_llm`, `@trace_db`, `@trace_http`, `@trace_messaging`
- Global `track()` function for product events with auto-enrichment
- `Event` class for sending events to subscribers (PostHog, Slack, Webhook)
- `Metric` class for OpenTelemetry metrics (counters, histograms)
- Baggage support with `with_baggage()` and automatic span attributes
- Production features: adaptive sampling, rate limiting, circuit breakers
- PII redaction for email, phone, SSN, credit card, and API keys
- Framework integrations: FastAPI, Django, Flask middleware
- HTTP instrumentation with W3C Trace Context propagation
- Database instrumentation helpers
- MCP (Model Context Protocol) instrumentation with auto-patching
- Logging integration (standard logging, structlog, loguru)
- Testing utilities: `InMemorySpanExporter`, assertion helpers
- Graceful shutdown with `shutdown()` and `shutdown_sync()`
- Comprehensive documentation and migration guide
- 189 passing tests with full coverage

#### Developer Experience
- Type hints throughout (mypy compliant)
- Comprehensive examples for all features
- Before/after comparison examples
- Migration guide from manual OpenTelemetry
- Clear error messages and validation

[0.2.0]: https://github.com/jagreehal/autotel-python/releases/tag/v0.2.0
[0.1.0]: https://github.com/jagreehal/autotel-python/releases/tag/v0.1.0
