"""
BEFORE autotel: Manual OpenTelemetry instrumentation
This is what developers have to write WITHOUT autotel.
Based on real-world examples from langfuse-observability-demo.
"""

import contextlib
import logging
import os
import time

from flask import Flask, jsonify, request

# ==================== BOILERPLATE START ====================
# 40+ lines of imports just for observability!
from opentelemetry import metrics, trace
from opentelemetry._logs import set_logger_provider
from opentelemetry.exporter.otlp.proto.http._log_exporter import (
    OTLPLogExporter as OTLPHTTPLogExporter,
)
from opentelemetry.exporter.otlp.proto.http.metric_exporter import (
    OTLPMetricExporter as OTLPHTTPMetricExporter,
)
from opentelemetry.exporter.otlp.proto.http.trace_exporter import (
    OTLPSpanExporter as OTLPHTTPSpanExporter,
)
from opentelemetry.instrumentation.flask import FlaskInstrumentor
from opentelemetry.sdk._logs import LoggerProvider, LoggingHandler
from opentelemetry.sdk._logs.export import BatchLogRecordProcessor
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor

# Manual resource setup
resource = Resource.create({
    "service.name": os.getenv("OTEL_SERVICE_NAME", "flask-api"),
    "service.namespace": "demo",
    "deployment.environment": os.getenv("APP_ENV", "dev"),
})

# Manual tracer setup
trace_provider = TracerProvider(resource=resource)
span_exporter = OTLPHTTPSpanExporter()
trace_provider.add_span_processor(BatchSpanProcessor(span_exporter))
trace.set_tracer_provider(trace_provider)
tracer = trace.get_tracer(__name__)

# Manual metrics setup
metric_exporter = OTLPHTTPMetricExporter()
metric_reader = PeriodicExportingMetricReader(metric_exporter)
meter_provider = MeterProvider(resource=resource, metric_readers=[metric_reader])
metrics.set_meter_provider(meter_provider)
meter = metrics.get_meter(__name__)

# Manual metric creation - each one requires explicit setup
request_counter = meter.create_counter(
    "http.server.request.count", unit="1", description="Total requests"
)
latency_histogram = meter.create_histogram(
    "http.server.request.duration", unit="ms", description="Request duration"
)
token_counter = meter.create_counter(
    "llm.tokens", unit="1", description="LLM tokens"
)

# Manual logs setup
log_exporter = OTLPHTTPLogExporter()
log_provider = LoggerProvider(resource=resource)
log_provider.add_log_record_processor(BatchLogRecordProcessor(log_exporter))
set_logger_provider(log_provider)
otel_log_handler = LoggingHandler(level=logging.INFO, logger_provider=log_provider)
logging.basicConfig(handlers=[otel_log_handler], level=logging.INFO)
logger = logging.getLogger("flask-api")
# ==================== BOILERPLATE END ====================

app = Flask(__name__)

# More manual instrumentation
with contextlib.suppress(Exception):
    FlaskInstrumentor().instrument_app(app)

@app.route("/ask", methods=["POST"])
def ask_question() -> None:
    started = time.perf_counter()

    # Manual metric recording - scattered throughout code
    request_counter.add(1, {"http.route": "/ask", "environment": "dev"})

    # Manual span creation - 10+ lines just to start a span with attributes!
    with tracer.start_as_current_span(
        "ask_question_request",
        attributes={
            "http.route": "/ask",
            "http.method": "POST",
            # ... many more manual attributes
        }
    ) as root_span:
        try:
            data = request.get_json(force=True) or {}
            question = data.get("question")

            # More manual attribute setting
            root_span.set_attribute("user.question", question)
            root_span.set_attribute("request.has_question", bool(question))

            if not question:
                # Manual error handling in spans
                root_span.set_attribute("error", True)
                root_span.set_attribute("error.type", "bad_request")
                latency_ms = int((time.perf_counter() - started) * 1000)
                latency_histogram.record(latency_ms, {"http.route": "/ask", "http.status_code": 400})
                return jsonify({"error": "Missing question"}), 400

            # Nested span for LLM call - another 10+ lines!
            with tracer.start_as_current_span(
                "openai.chat.completions",
                attributes={
                    "llm.vendor": "openai",
                    "llm.model": "gpt-4",
                    "llm.input.role.system": "You are a helpful assistant",
                }
            ) as llm_span:
                # Simulate LLM call
                answer = f"Mock answer to: {question}"

                # Manual usage tracking - more attribute setting
                prompt_tokens = 100
                completion_tokens = 50

                llm_span.set_attribute("llm.usage.prompt_tokens", prompt_tokens)
                llm_span.set_attribute("llm.usage.completion_tokens", completion_tokens)
                llm_span.set_attribute("llm.usage.total_tokens", prompt_tokens + completion_tokens)
                llm_span.set_attribute("llm.output.length", len(answer))

                # Manual metric recording for tokens
                token_counter.add(prompt_tokens, {"llm.token_type": "prompt", "llm.model": "gpt-4"})
                token_counter.add(completion_tokens, {"llm.token_type": "completion", "llm.model": "gpt-4"})

                # Propagate attributes to parent span
                root_span.set_attribute("llm.usage.prompt_tokens", prompt_tokens)
                root_span.set_attribute("llm.usage.completion_tokens", completion_tokens)

            # Manual success recording
            latency_ms = int((time.perf_counter() - started) * 1000)
            root_span.set_attribute("http.status_code", 200)
            root_span.set_attribute("latency_ms", latency_ms)
            latency_histogram.record(latency_ms, {"http.route": "/ask", "http.status_code": 200})

            return jsonify({"answer": answer}), 200

        except Exception as e:
            # Manual error handling
            root_span.set_attribute("error", True)
            root_span.set_attribute("error.type", type(e).__name__)
            root_span.set_attribute("error.message", str(e))
            latency_ms = int((time.perf_counter() - started) * 1000)
            latency_histogram.record(latency_ms, {"http.route": "/ask", "http.status_code": 500})
            return jsonify({"error": "Server error"}), 500


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8080)
