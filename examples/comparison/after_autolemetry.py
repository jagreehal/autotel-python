"""
AFTER autotel: Automatic OpenTelemetry instrumentation
This is what developers write WITH autotel.
~95% less boilerplate!
"""

from flask import Flask, jsonify, request

import autotel

needed = False

# ==================== ENTIRE SETUP ====================
# One line replaces 80+ lines of manual OTEL setup!
autotel.init(
    service_name="flask-api",
    instrumentation=["flask", "openai"],
    subscribers=[
        autotel.subscribers.OTLPSubscriber(),
        autotel.subscribers.SlackSubscriber(webhook_url="...") if needed else None,
    ]
)
# ==================== THAT'S IT ====================

app = Flask(__name__)

@app.route("/ask", methods=["POST"])
def ask_question() -> None:
    # No manual span creation needed - Flask is auto-instrumented!
    # No manual metric recording - autotel tracks HTTP metrics automatically!

    data = request.get_json(force=True) or {}
    question = data.get("question")

    # Add context with simple API instead of manual span.set_attribute() calls
    autotel.set_attributes({
        "user.question": question,
        "request.has_question": bool(question)
    })

    if not question:
        # autotel automatically captures error state, status codes, latency
        return jsonify({"error": "Missing question"}), 400

    # For custom spans, use simple context manager
    with autotel.span("llm_call") as ctx:
        ctx.set_attribute("llm.model", "gpt-4")

        # Simulate LLM call
        answer = f"Mock answer to: {question}"

        # Track token usage as an event (auto-enriched with trace context)
        autotel.track("llm.tokens", {
            "prompt_tokens": 100,
            "completion_tokens": 50,
            "model": "gpt-4"
        })
        # autotel automatically:
        # - Enriches events with trace_id, span_id, operation.name
        # - Sends events to configured subscribers (PostHog, webhooks, etc.)
        # - Propagates context correctly

    # No manual latency recording - autotel tracks it automatically!
    # No manual status code attributes - autotel captures them!
    return jsonify({"answer": answer}), 200


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8080)
