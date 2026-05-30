# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

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
