# Autotel Python — Feature Parity and Verification

This document is the single source of tasks for **feature parity** (with the Node autotel repo when available) and **verification** that the Python implementation is complete, tested, and documented.

---

## 1. Node parity

**Node repo path:** `/Users/jreehal/dev/node-examples/autotel` (hyphen: `node-examples`, not `node_examples`).

**Package:** `packages/autotel` — **version 2.18.0** (from package.json). Subpath exports include: main entry, `instrumentation`, `logger`, `sampling`, `event`, `event-subscriber`, `attributes`, `event-testing`, `metric`, `metric-testing`, `metric-helpers`, `testing`, `exporters`, `processors`, `config`, `tail-sampling-processor`, `filtering-span-processor`, `span-name-normalizer`, `attribute-redacting-processor`, `functional`, `http`, `db`, `trace-helpers`, `tracer-provider`, `semantic-helpers`, `decorators`, `register`, `auto`, `yaml`, **`messaging`**, **`messaging-testing`**, **`messaging/adapters`** (messaging-adapters), **`business-baggage`**, **`workflow`**, **`webhook`**, **`workflow-distributed`**, **`correlation-id`**.

### Discovery summary (re-check)

- **Apps** (`apps/`): awaitly-example, cloudflare-example, example-ai-agent, example-aws-lambda, example-basic, example-bunyan, example-canonical-logs, example-datadog, example-drizzle, example-http, example-mcp-client, example-mcp-observability, example-mcp-server, example-mongoose, example-nextjs, example-pg, example-prisma, example-subscribers (posthog, slack, webhook-server), example-tanstack-start, example-terminal, example-web-vanilla, example-winston. Each uses init, trace, subscribers, or framework-specific instrumentation.
- **Core tests** (`packages/autotel/src/`): workflow.test.ts, workflow-distributed.test.ts, webhook.test.ts, messaging.test.ts, messaging-adapters.test.ts, messaging-testing.test.ts, business-baggage.test.ts, decorators.test.ts, sampling.test.ts, event.test.ts, event-queue.test.ts, track.test.ts, init.*.test.ts, instrumentation.test.ts, functional.test.ts, trace-helpers*.test.ts, shutdown.test.ts, correlation-id.test.ts, config.test.ts, env-config.test.ts, validation.test.ts, attributes.test.ts, baggage-span-processor.test.ts, circuit-breaker.test.ts, rate-limiter.test.ts, db.test.ts, http.test.ts, logger.test.ts, autotel-logger.test.ts, metrics.test.ts, semantic-helpers.test.ts, tracer-provider.test.ts, variable-name-inference.test.ts, span-name-normalizer.test.ts, filtering-span-processor.test.ts, attribute-redacting-processor.test.ts, pretty-console-exporter.test.ts, tail-sampling-processor.test.ts, stub.integration.test.ts, workflow.async-safety.integration.test.ts, processors/canonical-log-line-processor.test.ts, yaml-config.test.ts.
- **Other packages with tests:** autotel-plugins (kafka, rabbitmq, bigquery), autotel-mcp, autotel-cli, autotel-subscribers (posthog, webhook, segment, amplitude, mixpanel), autotel-backends (datadog, honeycomb), autotel-aws (sqs, sns, eventbridge integration), autotel-tanstack, autotel-edge, autotel-cloudflare, autotel-terminal, autotel-web. **~95** `*.test.ts` files across the monorepo.
- **Git log (recent):** Version Packages (#54), Feature/eda-enhancements (#53), BigQuery/Kafka support (#51), codemod enhancements (#49), documentation and features (#48), events-trace-context (#46), canonical log lines (#32, #30). Themes: EDA, plugins, events, TanStack, messaging adapters, business baggage, workflow (local + distributed), webhook/parking lot, array attributes, YAML config.

Once you need a full Node → Python API checklist, follow the procedure below.

### 1.1 Discover Node feature set

- **Apps:** List and summarize every app under `apps/` (or equivalent): purpose, frameworks, and which autotel features each uses (init, trace, track, workflow, webhook, messaging, etc.).
- **Tests:** List every test file and test suite under `tests/`. Map each to a feature area (e.g. decorators, workflow, webhook, messaging, sampling).
- **Git log:** Run `git log --oneline -100` (or more) and note:
  - New features (e.g. parking lot, distributed workflow, adapters).
  - Bug fixes or behavior changes that might affect API or semantics.
  - Any deprecations or renames.

### 1.2 Build Node feature checklist

From the above, produce a checklist:

- **API surface:** Every public export/function/decorator in Node autotel. For each, mark: exists in Python / missing / different signature or behavior.
- **Examples/apps:** For each Node app, mark: equivalent Python example exists (under `examples/`) or add task "Add example: …".
- **Tests:** For each Node test area, mark: equivalent Python tests exist (under `tests/unit` or `tests/integration`) or add task "Add tests for …".

### 1.3 Node → Python parity (to fill when doing full comparison)

| Node feature / area | Python status | Notes |
|---------------------|---------------|--------|
| (fill after 1.1–1.2) | | |

**Missing in Python (to implement):**

- (List after comparison)

**Missing examples (Node app → Python example):**

- (List after comparison)

**Missing tests (Node test → Python test):**

- (List after comparison)

---

## 1.4 MCP semantic conventions (port from Node autotel-mcp)

**Spec:** [OpenTelemetry semantic conventions for MCP (Gen AI)](https://opentelemetry.io/docs/specs/semconv/gen-ai/mcp/). Context propagation via `params._meta` (traceparent, tracestate, baggage).

**Node reference:** `packages/autotel-mcp/` — separate package with `semantic-conventions.ts`, `client.ts`, `server.ts`, `context.ts`, `metrics.ts`, `types.ts`. Uses OTel attribute names (e.g. `mcp.method.name`, `gen_ai.tool.name`, `gen_ai.operation.name`, `error.type`, `gen_ai.tool.call.arguments`, `gen_ai.tool.call.result`), span names `{mcp.method.name} {target}`, SpanKind CLIENT/SERVER, and optional histograms `mcp.client.operation.duration` / `mcp.server.operation.duration` with spec buckets.

**Python today:** `src/autotel/mcp.py` — context inject/extract and client/server instrumentation exist but use **custom** attribute names (`mcp.client.operation`, `mcp.client.name`, `mcp.client.args`, `mcp.server.{kind}.name`, etc.) and span names (`mcp.client.{operation}.{name}`, `mcp.server.{kind}.{name}`). No `SpanKind.CLIENT`/`SERVER`, no MCP metrics, no OTel semconv attribute names.

**Tasks to port MCP semconv:**

1. **Add MCP semantic constants** — New module or section in `mcp.py`: `MCP_SEMCONV` (attribute keys: `mcp.method.name`, `gen_ai.tool.name`, `gen_ai.prompt.name`, `mcp.resource.uri`, `jsonrpc.request.id`, `rpc.response.status_code`, `gen_ai.operation.name`, `mcp.protocol.version`, `mcp.session.id`, `network.transport`, `server.address`, `server.port`, `client.address`, `client.port`, `gen_ai.tool.call.arguments`, `gen_ai.tool.call.result`, `error.type`), `MCP_METHODS` (e.g. `tools/call`, `tools/list`, `resources/read`, `prompts/get`, `initialize`, `ping`), `MCP_METRICS` (client/server operation duration, session duration), `MCP_DURATION_BUCKETS` (spec histogram buckets).
2. **Client spans (instrument_mcp_client)** — Use span name `{mcp.method.name} {target}` (e.g. `tools/call get_weather`; for resources use method only to avoid high cardinality). Set `SpanKind.CLIENT`. Set required `mcp.method.name`; conditionally `gen_ai.tool.name` / `gen_ai.prompt.name` / `mcp.resource.uri`; recommended `gen_ai.operation.name` = `execute_tool` for tool calls, `network.transport`, `mcp.session.id`; opt-in `gen_ai.tool.call.arguments`, `gen_ai.tool.call.result`; on error `error.type`. Keep injecting context into `params._meta`.
3. **Server spans (instrument_mcp_server)** — Use span name `{mcp.method.name} {target}`. Set `SpanKind.SERVER`. Same attribute set; when result has `isError` set `error.type` = `tool_error`. Keep extracting context from `_meta` and using as parent.
4. **Config alignment** — Add `network_transport`, `session_id`, `enable_metrics`, `capture_discovery_operations`. Support `capture_tool_args` / `capture_tool_results` (alias or rename from `capture_args` / `capture_results`). Default `capture_tool_args` = False, `capture_tool_results` = False per spec opt-in.
5. **Optional: discovery operations** — If config enables, wrap client `list_tools`, `list_resources`, `list_prompts`, `ping` with CLIENT spans and `mcp.method.name` = `tools/list`, `resources/list`, `prompts/list`, `ping`.
6. **Optional: MCP metrics** — When `enable_metrics` is True, record histograms `mcp.client.operation.duration`, `mcp.server.operation.duration` (seconds) with `MCP_DURATION_BUCKETS` and attributes `mcp.method.name`, `gen_ai.tool.name` / `mcp.resource.uri` / `gen_ai.prompt.name`, `error.type` as applicable.
7. **Tests** — Add or extend `tests/unit/test_mcp.py` to assert span names, span kind, and OTel attribute names (e.g. `mcp.method.name`, `gen_ai.tool.name`) match the spec; add semconv-compliance-style tests mirroring Node `semconv-compliance.test.ts`.
8. **Docs** — In README or docs, note that MCP instrumentation follows [OTel MCP semantic conventions](https://opentelemetry.io/docs/specs/semconv/gen-ai/mcp/) and document any Python-specific options.

---

## 2. Internal parity (Python repo)

### 2.1 Features vs CHANGELOG (Unreleased)

| CHANGELOG feature | Implemented | Exported | Documented | Unit tested | Example |
|-------------------|-------------|----------|------------|-------------|---------|
| Webhook/Parking Lot (`webhook.py`) | Yes | Yes | Yes (README, quickstart) | Yes (`test_webhook.py`) | Yes (`parking_lot_example.py`) |
| Distributed Workflow (`workflow_distributed.py`) | Yes | Yes | Yes | Yes (`test_workflow_distributed.py`) | Yes (`distributed_workflow_example.py`) |
| Messaging Adapters (`messaging_adapters.py`) | Yes | Yes | Docstrings | Yes (`test_messaging_adapters.py`) | Yes (`messaging_adapters_example.py`) |
| Context Extractors (Datadog, B3, X-Ray, Jaeger, multi) | Yes | Yes | Docstrings | Yes (in `test_messaging_adapters.py`) | No dedicated |
| Enhanced Messaging Context (ProducerContext, ConsumerContext, DLQ, retry, ordering) | Yes | Yes | Docstrings | Yes (`test_messaging.py`) | No dedicated |
| Safe Baggage Schema (`business_baggage.py`) | Yes | Yes | Docstrings | Yes (`test_business_baggage.py`) | Yes (`business_baggage_example.py`) |
| MCP client/server (`mcp.py`) | Yes | Yes | Docstrings | Yes (`test_mcp.py`) | (Node: example-mcp-client, example-mcp-server). **OTel semconv alignment:** see §1.4 and tasks 29–36. |

### 2.2 Unit tests — gaps

| Area | File to add / extend | What to cover |
|------|----------------------|----------------|
| `workflow_distributed` | `tests/unit/test_workflow_distributed.py` | `trace_distributed_workflow`, `trace_distributed_step`, `WorkflowBaggage`, `WorkflowBaggageValues`, `create_workflow_headers`, `generate_workflow_id`, `get_workflow_progress`, `is_in_distributed_workflow`, `DistributedWorkflowContext`, `DistributedStepContext` (sync and async, with/without ctx) |
| `webhook` (Parking Lot) | `tests/unit/test_webhook.py` | `ParkingLot`, `InMemoryTraceContextStore`, `TraceContextStore` contract, `@trace_callback`, `CallbackContext`, `create_correlation_key`, `create_parking_lot`, `to_span_context`, `StoredTraceContext`, park → callback link |
| `messaging_adapters` | `tests/unit/test_messaging_adapters.py` | Adapters: `nats_adapter`, `temporal_adapter`, `cloudflare_queues_adapter`, `sqs_adapter`, `redis_streams_adapter` (attributes/headers). Extractors: `datadog_context_extractor`, `b3_context_extractor`, `xray_context_extractor`, `jaeger_context_extractor`, `create_multi_format_extractor`, `default_multi_format_extractor` |
| Context span links | `tests/unit/test_context.py` (extend) | `TraceContext.add_link()`, `TraceContext.add_links()` (and that they are reflected on the span) |

### 2.3 Examples — gaps

| Feature | Current state | Task |
|---------|----------------|------|
| Parking Lot | `examples/basic/webhook_example.py` only shows WebhookSubscriber (HTTP events), not Parking Lot API | Add `examples/basic/parking_lot_example.py`: `create_parking_lot`, `InMemoryTraceContextStore`, `park()`, `@trace_callback`, `CallbackContext` |
| Distributed workflow | Documented in README/quickstart; no runnable example | Add `examples/basic/distributed_workflow_example.py`: two “services” using `trace_distributed_workflow` + producer and `trace_distributed_step` + consumer with in-memory propagation |
| Messaging adapters / context extractors | None | Add `examples/basic/messaging_adapters_example.py` (or similar) using at least one adapter and one context extractor with mock/in-memory messaging |
| Business baggage / SafeBaggageSchema | No dedicated example | Add `examples/basic/business_baggage_example.py`: `create_safe_baggage_schema`, `SafeBaggageSchema`, propagation |

### 2.4 Example verification scripts

| Script | Current behavior | Task |
|--------|------------------|------|
| `scripts/verify_examples.py` | Syntax check + old `events_adapters` import check; does not run examples | Ensure `examples_dir.rglob("*.py")` includes any new example dirs; optionally document that new examples should be added to the glob if new top-level dirs are added |
| `scripts/test_examples.py` | Runs only: basic, functional, events, complete, shutdown | Add test functions for: parking_lot, distributed_workflow, messaging_adapters (or one adapter), and optionally semantic_helpers/metrics so `make test-examples` covers new features |

### 2.5 Documentation

| Doc | Task |
|-----|------|
| `docs/quickstart.md` / `docs/index.md` | After new examples exist: add one-line links/references to `parking_lot_example.py`, `distributed_workflow_example.py`, messaging adapters and business baggage examples |
| `README.md` | Ensure "Webhook/Parking Lot" and "Distributed workflow" sections point to new examples and API |

### 2.6 CI and quality

| Item | Task |
|------|------|
| Unit tests | New test files must follow existing patterns: use `clean_otel` fixture, `InMemorySpanExporter` where appropriate |
| Integration tests | Only `tests/integration/test_fastapi_integration.py` exists. Optional: add "Add integration tests for Flask" and "Add integration tests for Django" to task list if Node apps matrix includes them |
| Makefile | Ensure `make quality` and `make test-examples` pass after all new tests and examples are added |

### 2.7 Release readiness

- When cutting a release: move "Unreleased" CHANGELOG content into a versioned section and tag.
- Pre-release: run full regression (unit + integration + test-examples + verify-examples) and update CHANGELOG/version.

---

## 3. Verification checklist (how to verify everything works)

Run these in order:

1. **Install:** `uv pip install -e ".[dev]"` (or `make install`).
2. **Unit tests:** `pytest tests/unit -v --cov=src --cov-report=term-missing`.
3. **Integration tests:** `pytest tests/integration -v`.
4. **Verify examples (syntax/imports):** `python scripts/verify_examples.py` (or `make verify-examples`).
5. **Test examples (run):** `python scripts/test_examples.py` (or `make test-examples`).
6. **Full quality:** `make quality` (lint, type-check, test, verify-examples).
7. **Optional:** Run each new example manually and confirm no errors (e.g. `python examples/basic/parking_lot_example.py`).
8. **When Node repo available:** Run Node test suite and compare behavior for shared features.

---

## 4. Task list (all tasks — leave no stone unturned)

### Node parity (after Node repo available)

1. Clone or locate Node autotel repo and set path (e.g. `node_examples/autotel`).
2. List all apps under `apps/` with purpose and features used.
3. List all test files under `tests/` and map to feature areas.
4. Run `git log --oneline -100` and note new features, fixes, deprecations.
5. Build API surface checklist: Node export → Python exists / missing / different.
6. Build examples checklist: Node app → Python example exists or add task.
7. Build tests checklist: Node test area → Python test exists or add task.
8. Fill "Node → Python parity" table and "Missing in Python", "Missing examples", "Missing tests" in this doc.

### Internal parity — unit tests

9. Add `tests/unit/test_workflow_distributed.py` covering `trace_distributed_workflow`, `trace_distributed_step`, `WorkflowBaggage`, `create_workflow_headers`, `generate_workflow_id`, `get_workflow_progress`, `is_in_distributed_workflow`, and context types (sync/async, with/without ctx).
10. Add `tests/unit/test_webhook.py` covering `ParkingLot`, `InMemoryTraceContextStore`, `TraceContextStore` contract, `@trace_callback`, `CallbackContext`, `create_correlation_key`, `create_parking_lot`, `to_span_context`, `StoredTraceContext`, park → callback link.
11. Add `tests/unit/test_messaging_adapters.py` covering all adapters (nats, temporal, cloudflare_queues, sqs, redis_streams) and all context extractors (datadog, b3, xray, jaeger, create_multi_format_extractor, default_multi_format_extractor).
12. Add tests in `tests/unit/test_context.py` for `TraceContext.add_link()` and `TraceContext.add_links()`.

### Internal parity — examples

13. Add `examples/basic/parking_lot_example.py` demonstrating Parking Lot API: `create_parking_lot`, `InMemoryTraceContextStore`, `park()`, `@trace_callback`, `CallbackContext`.
14. Add `examples/basic/distributed_workflow_example.py` with two logical “services” using `trace_distributed_workflow` + producer and `trace_distributed_step` + consumer (in-memory propagation).
15. Add `examples/basic/messaging_adapters_example.py` using at least one adapter and one context extractor with mock/in-memory messaging.
16. Add `examples/basic/business_baggage_example.py` demonstrating `create_safe_baggage_schema`, `SafeBaggageSchema`, and propagation.

### Internal parity — scripts

17. Confirm `scripts/verify_examples.py` glob (`examples_dir.rglob("*.py")`) covers all new example files; document if new top-level example dirs are added.
18. Add to `scripts/test_examples.py`: test function for parking_lot (or parking lot scenario).
19. Add to `scripts/test_examples.py`: test function for distributed_workflow.
20. Add to `scripts/test_examples.py`: test function for messaging_adapters (or one adapter); optionally add semantic_helpers/metrics.

### Internal parity — documentation

21. In `docs/quickstart.md` and/or `docs/index.md`, add references/links to `parking_lot_example.py`, `distributed_workflow_example.py`, messaging adapters example, business baggage example (after those examples exist).
22. In `README.md`, ensure Webhook/Parking Lot and Distributed workflow sections point to the new examples and public API.

### Internal parity — CI and quality

23. Ensure new unit test files use `clean_otel` fixture and `InMemorySpanExporter` where appropriate so CI passes.
24. Run `make quality` and fix any failures (lint, type-check, test, verify-examples).
25. Run `make test-examples` and fix any failures after adding new example tests.
26. Optional: add integration tests for Flask and Django in `tests/integration/` if targeting framework parity.

### MCP semantic conventions (port from Node autotel-mcp)

29. Add MCP semantic constants in `mcp.py` (or `mcp_semconv.py`): `MCP_SEMCONV`, `MCP_METHODS`, `MCP_METRICS`, `MCP_DURATION_BUCKETS` per [OTel MCP spec](https://opentelemetry.io/docs/specs/semconv/gen-ai/mcp/).
30. Update `instrument_mcp_client`: span name `{mcp.method.name} {target}`, `SpanKind.CLIENT`, set `mcp.method.name`, `gen_ai.tool.name` / `gen_ai.prompt.name` / `mcp.resource.uri`, `gen_ai.operation.name`, opt-in `gen_ai.tool.call.arguments` / `gen_ai.tool.call.result`, `error.type`.
31. Update `instrument_mcp_server`: span name `{mcp.method.name} {target}`, `SpanKind.SERVER`, same attributes; `error.type` = `tool_error` when result has `isError`.
32. Add config options: `network_transport`, `session_id`, `enable_metrics`, `capture_discovery_operations`; align `capture_tool_args` / `capture_tool_results`.
33. Optional: wrap client discovery (`list_tools`, `list_resources`, `list_prompts`, `ping`) when `capture_discovery_operations` is True.
34. Optional: record histograms `mcp.client.operation.duration`, `mcp.server.operation.duration` when `enable_metrics` is True.
35. Add or extend `tests/unit/test_mcp.py` for semconv: span names, span kind, OTel attribute names (mirror Node `semconv-compliance.test.ts`).
36. Document in README/docs that MCP instrumentation follows OTel MCP semantic conventions and list Python config options.

### Release readiness

37. Pre-release: run full regression (unit + integration + test-examples + verify-examples); update CHANGELOG and version when cutting release.
38. When releasing: move "Unreleased" CHANGELOG into versioned section and tag.
