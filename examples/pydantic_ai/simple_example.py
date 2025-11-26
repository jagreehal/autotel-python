"""Simple Pydantic AI example with autotel - minimal setup."""

import asyncio
import os

# Set OpenAI base URL to point to Ollama's OpenAI-compatible endpoint
# Ollama doesn't require an API key, but OpenAI client needs one set
os.environ.setdefault("OPENAI_BASE_URL", "http://localhost:11434/v1")
os.environ.setdefault("OPENAI_API_KEY", "ollama")  # Dummy key for Ollama

from pydantic_ai import Agent

from autotel import ConsoleSpanExporter, SimpleSpanProcessor, init, trace

# Initialize autotel (one line!)
init(
    service="pydantic-ai-simple",
    span_processor=SimpleSpanProcessor(ConsoleSpanExporter()),
)

# Create agent with Ollama via OpenAI-compatible API
agent = Agent("openai:llama3.2")


@trace
async def ask_ai(ctx, question: str) -> str:
    """
    Ask a question to the AI agent.

    Automatically traced by autotel!
    """
    ctx.set_attribute("ai.model", "llama3.2:latest")
    ctx.set_attribute("ai.provider", "ollama")
    ctx.set_attribute("user.question", question)

    result = await agent.run(question)

    # In pydantic-ai 1.19.0+, use result.output instead of result.data
    response = result.output if isinstance(result.output, str) else str(result.output)
    ctx.set_attribute("ai.response.length", len(response))
    ctx.add_event("ai.response.received")

    return response


async def main() -> None:
    """Simple example."""
    print("=== Simple Pydantic AI + autotel ===\n")

    # Ask a question
    response = await ask_ai("What is Python?")
    print(f"AI Response: {response}\n")

    print("Check console output for trace spans!")


if __name__ == "__main__":
    asyncio.run(main())
