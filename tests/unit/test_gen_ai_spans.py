"""GenAI invoke_agent / execute_tool span helpers."""

from autotel import execute_tool, init, invoke_agent
from autotel.exporters import InMemorySpanExporter
from autotel.processors import SimpleSpanProcessor


def test_invoke_agent_sets_gen_ai_attributes() -> None:
    exporter = InMemorySpanExporter()
    init(service="test", span_processor=SimpleSpanProcessor(exporter))

    with invoke_agent(agent_name="support", model="llama3.2", system="ollama") as ctx:
        ctx.set_attribute("demo.ok", True)

    spans = exporter.get_finished_spans()
    assert len(spans) == 1
    span = spans[0]
    assert span.name == "invoke_agent"
    assert span.attributes["gen_ai.operation.name"] == "invoke_agent"
    assert span.attributes["agent.name"] == "support"
    assert span.attributes["gen_ai.agent.name"] == "support"
    assert span.attributes["gen_ai.request.model"] == "llama3.2"
    assert span.attributes["gen_ai.system"] == "ollama"


def test_execute_tool_span_and_event() -> None:
    exporter = InMemorySpanExporter()
    init(service="test", span_processor=SimpleSpanProcessor(exporter))

    with execute_tool(
        "refund_lookup",
        tool_call_id="call-1",
        arguments='{"order_id":"ORD-1"}',
    ):
        pass

    spans = exporter.get_finished_spans()
    assert len(spans) == 1
    span = spans[0]
    assert span.name == "execute_tool"
    assert span.attributes["gen_ai.operation.name"] == "execute_tool"
    assert span.attributes["gen_ai.tool.name"] == "refund_lookup"
    assert span.attributes["gen_ai.tool.call.id"] == "call-1"
    event_names = [event.name for event in span.events]
    assert "gen_ai.tool.call" in event_names
