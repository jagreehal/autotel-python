"""Example demonstrating graceful shutdown."""

import asyncio
import signal
import sys
from typing import Any

from autotel import (
    ConsoleSpanExporter,
    SimpleSpanProcessor,
    init,
    shutdown,
    shutdown_sync,
    trace,
    track,
)

# Initialize autotel
init(
    service="shutdown-example",
    span_processor=SimpleSpanProcessor(ConsoleSpanExporter()),
)


@trace
async def process_data(data: dict[str, Any]) -> None:
    """Process some data."""
    await asyncio.sleep(0.1)
    track("data_processed", {"size": len(data)})
    return {"processed": True, **data}


async def main() -> None:
    """Main function with graceful shutdown."""
    print("=== autotel Shutdown Example ===\n")

    # Process some data
    for i in range(5):
        result = await process_data({"index": i})
        print(f"Processed: {result}")

    print("\nShutting down gracefully...")
    await shutdown(timeout=2.0)
    print("Shutdown complete!")


def signal_handler(_sig, _frame) -> None:
    """Handle shutdown signals."""
    print("\nReceived shutdown signal, shutting down gracefully...")
    shutdown_sync(timeout=2.0)
    sys.exit(0)


if __name__ == "__main__":
    # Register signal handlers for graceful shutdown
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    asyncio.run(main())
