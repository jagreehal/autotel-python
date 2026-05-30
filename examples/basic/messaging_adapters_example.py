"""Example: Messaging adapters for consumer attributes and context extractors.

Shows trace_consumer with message headers (for trace linking) and using
adapter custom_attributes to add system-specific attributes (e.g. NATS).
Context extractors (Datadog, B3) are used when integrating with systems
that send non-W3C trace headers.
"""

import asyncio
from typing import Any

from autotel import (
    ConsoleSpanExporter,
    SimpleSpanProcessor,
    init,
)
from autotel.messaging import trace_consumer
from autotel.messaging_adapters import nats_adapter

init(
    service="messaging-adapters-example",
    span_processor=SimpleSpanProcessor(ConsoleSpanExporter()),
)


# Message with headers (for W3C link) and NATS-like shape for adapter
class MockStreamInfo:
    stream = "ORDERS"
    consumer = "consumer-1"


class MockNatsMessage:
    def __init__(self, subject: str, data: dict, headers: dict | None = None):
        self.subject = subject
        self.data = data
        self.headers = headers or {}
        self.info = MockStreamInfo()


@trace_consumer(
    system="nats",
    destination="orders.created",
    headers_key="headers",
)
async def process_order(ctx: Any, msg: MockNatsMessage) -> str:
    """Consumer: trace_consumer sets messaging.* attributes; add NATS-specific from adapter."""
    attrs = nats_adapter.consumer.custom_attributes(ctx, msg)
    if attrs:
        for key, value in attrs.items():
            ctx.set_attribute(key, value)
    return f"processed:{msg.subject}"


async def main() -> None:
    msg = MockNatsMessage(
        subject="orders.created",
        data={"order_id": "ord-1"},
        headers={"traceparent": "00-0abcdef0123456789012345678901234-0123456789abcdef-01"},
    )
    result = await process_order(msg)
    print(f"Result: {result}")


if __name__ == "__main__":
    asyncio.run(main())
