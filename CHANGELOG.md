# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

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
