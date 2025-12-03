"""Simple Pydantic AI example with autotel - minimal setup."""

import asyncio
import os
from pathlib import Path

# Load .env file if it exists (for OpenAI API key)
try:
    from dotenv import load_dotenv
    
    # Try to load from project root first, then from examples/pydantic_ai
    project_root = Path(__file__).parent.parent.parent
    env_file = project_root / ".env"
    if not env_file.exists():
        env_file = Path(__file__).parent / ".env"
    if env_file.exists():
        load_dotenv(env_file)
except ImportError:
    pass  # python-dotenv not installed, skip .env loading

# Set OpenAI base URL to point to Ollama's OpenAI-compatible endpoint
# If OPENAI_API_KEY is set in .env or environment, use OpenAI
# Otherwise, use Ollama (default)
if not os.getenv("OPENAI_API_KEY") or os.getenv("OPENAI_API_KEY") == "ollama":
    os.environ.setdefault("OPENAI_BASE_URL", "http://localhost:11434/v1")
    os.environ.setdefault("OPENAI_API_KEY", "ollama")  # Dummy key for Ollama

from pydantic_ai import Agent

from autotel import ConsoleSpanExporter, SimpleSpanProcessor, init, trace

# Initialize autotel (one line!)
init(
    service="pydantic-ai-simple",
    span_processor=SimpleSpanProcessor(ConsoleSpanExporter()),
)

# Create agent - uses OpenAI if OPENAI_API_KEY is set, otherwise Ollama
# For OpenAI, use: Agent("openai:gpt-4o-mini") or Agent("openai:gpt-4")
# For Ollama, use: Agent("openai:llama3.2") (requires Ollama running)
use_openai = os.getenv("OPENAI_API_KEY") and os.getenv("OPENAI_API_KEY") != "ollama"
if use_openai:
    agent = Agent("openai:gpt-4o-mini")  # Use OpenAI gpt-4o-mini
    print("Using OpenAI with gpt-4o-mini")
else:
    agent = Agent("openai:llama3.2")  # Use Ollama
    print("Using Ollama with llama3.2")


@trace
async def ask_ai(ctx, question: str) -> str:
    """
    Ask a question to the AI agent.

    Automatically traced by autotel!
    """
    # Detect provider from agent
    use_openai = os.getenv("OPENAI_API_KEY") and os.getenv("OPENAI_API_KEY") != "ollama"
    if use_openai:
        ctx.set_attribute("ai.model", "gpt-4o-mini")
        ctx.set_attribute("ai.provider", "openai")
    else:
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
