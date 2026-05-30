# Configuration

`init()` has a large surface area, but **you rarely need most of it**. The
philosophy is: the simple path is one line, and every advanced knob is optional
and grouped by what it does. Start minimal; reach for a group below only when a
specific need shows up.

## Minimal

```python
from autotel import init

init(service="checkout-api")
```

That's the whole happy path. `init()` reads the standard OTEL environment
variables (`OTEL_SERVICE_NAME`, `OTEL_EXPORTER_OTLP_ENDPOINT`, …) and defaults
the endpoint to `http://localhost:4318`. Explicit parameters override env vars.

```python
# Point at a collector / backend
init(service="checkout-api", endpoint="https://otlp.example.com")
```

## Why the surface is large

The breadth isn't complexity for its own sake — it's the value compounding. A
one-line setup gets you traces. As you grow, the *same* `init()` call is where
you opt into product events, enrichment, sampling, PII redaction, and custom
export pipelines without rewiring anything. You pay for that breadth only when
you use it; defaults stay sensible and quiet.

## Advanced options, by group

### Service identity & resource

| Parameter | Default | Purpose |
|---|---|---|
| `service` | env `OTEL_SERVICE_NAME` | Service name (required, via arg or env). |
| `service_version` | — | Version attribute on all telemetry. |
| `environment` | — | Deployment environment (e.g. `production`). |
| `resource` | — | A fully-built OTel `Resource` (overrides the above). |
| `resource_attributes` | — | Extra resource attributes (`dict` or `key=val,…` string). |

### Export targets

| Parameter | Default | Purpose |
|---|---|---|
| `endpoint` | `http://localhost:4318` | OTLP endpoint for all signals. |
| `traces_endpoint` / `metrics_endpoint` / `logs_endpoint` | — | Per-signal endpoint overrides. |
| `protocol` | `http` | `"http"` or `"grpc"`. |
| `headers` | — | OTLP headers (`dict` or `key=val,…` string), e.g. auth. |
| `insecure` | `True` | Allow non-TLS export. |

### Signals

| Parameter | Default | Purpose |
|---|---|---|
| `metrics` | — | `True` creates an OTLP metric reader; or pass a config `dict`. |
| `logs` | — | `True` creates an OTLP log processor; `"auto"` for detection. |

### Local development

| Parameter | Default | Purpose |
|---|---|---|
| `devtools` | — | Local autotel-devtools shortcut; `True` ⇒ `http://127.0.0.1:4318` and enables metrics/logs. |
| `debug` | `False` | **Opt-in** console span output. Pass `debug=True` to print spans; off by default so notebooks/CLIs stay quiet. |

### Instrumentation

| Parameter | Default | Purpose |
|---|---|---|
| `instrumentation` | — | List of integrations to auto-instrument (e.g. FastAPI, Django, Flask). |
| `pydantic_ai` | `False` | Auto-instrument Pydantic AI agents. |
| `openllmetry` | — | OpenLLMetry config to auto-instrument LLM SDKs. |

### Events & enrichment

| Parameter | Default | Purpose |
|---|---|---|
| `subscribers` | — | Event subscribers (PostHog, Slack, Webhook, custom) for `track()`. |
| `logger` | — | Logger to bridge into OTel logs. |
| `baggage` | — | Enable baggage propagation (`True`, or a key/config). |

### Sampling

| Parameter | Default | Purpose |
|---|---|---|
| `sampler` | — | An `AdaptiveSampler` for tail/adaptive sampling. |

### Span transforms

| Parameter | Default | Purpose |
|---|---|---|
| `span_filter` | — | Predicate `(span) -> bool`; return `True` to keep a completed span. |
| `span_name_normalizer` | — | Normalize span names (callable or preset: `rest-api`, `graphql`, `minimal`). |
| `attribute_redactor` | — | PII/attribute redaction (config, preset `default`/`strict`/`pci-dss`, or callable). |

### Export pipeline (low-level)

Most users never touch these — `init()` builds a sensible pipeline. Override
only when integrating custom exporters/processors.

| Parameter | Default | Purpose |
|---|---|---|
| `span_processor_mode` | `"batch"` | `"batch"` or `"simple"` span processing. |
| `span_processor` | — | A single custom `SpanProcessor`. |
| `span_processors` | — | Multiple custom span processors. |
| `span_exporters` | — | Custom span exporters. |
| `metric_readers` | — | Custom metric readers (e.g. OTLP + Prometheus). |
| `log_record_processors` | — | Custom log record processors. |
| `batch_timeout` | `5000` | Batch processor flush interval (ms). |
| `max_queue_size` | `2048` | Max queued spans. |
| `max_export_batch_size` | `512` | Max spans per export batch. |

### Serverless & config helpers

| Parameter | Default | Purpose |
|---|---|---|
| `auto_flush` | `False` | Flush after each invocation (AWS Lambda, Cloud Functions, …). |
| `preset` | — | Preset config bundle from the `presets` module. |
| `validation` | — | Event-name / attribute validation config. |
