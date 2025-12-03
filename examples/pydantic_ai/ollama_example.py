"""Pydantic AI example with Ollama and Llama3.2, integrated with autotel."""

import asyncio
import os

from pydantic import BaseModel, Field
from pydantic_ai import Agent

from autotel import ConsoleSpanExporter, SimpleSpanProcessor, init, trace

# Initialize autotel
init(
    service="pydantic-ai-example",
    span_processor=SimpleSpanProcessor(ConsoleSpanExporter()),
)


# Define data models
class UserProfile(BaseModel):
    """User profile model."""

    name: str = Field(description="User's full name")
    age: int = Field(description="User's age")
    email: str = Field(description="User's email address")
    bio: str = Field(description="User's biography")


class UserSummary(BaseModel):
    """User summary model."""

    name: str
    summary: str = Field(description="Brief summary of the user")
    key_points: list[str] = Field(description="Key points about the user")


# Create agent with Ollama via OpenAI-compatible API
# Set OPENAI_BASE_URL to point to Ollama's OpenAI-compatible endpoint
# Ollama doesn't require an API key, but OpenAI client needs one set
os.environ.setdefault("OPENAI_BASE_URL", "http://localhost:11434/v1")
os.environ.setdefault("OPENAI_API_KEY", "ollama")  # Dummy key for Ollama
agent = Agent(
    "openai:llama3.2",
    output_type=UserProfile,  # Set default output type
    system_prompt="You are a helpful assistant that extracts and summarizes user information.",
)


@trace
async def extract_user_profile(ctx, text: str) -> UserProfile:
    """
    Extract user profile from text using Pydantic AI.

    This function is automatically traced by autotel.
    """
    ctx.set_attribute("ai.model", "llama3.2:latest")
    ctx.set_attribute("ai.provider", "ollama")
    ctx.set_attribute("ai.operation", "extract_profile")

    result = await agent.run(text)

    # Try to extract token usage if available
    try:
        if hasattr(result, "usage") and result.usage:
            ctx.set_attribute("ai.tokens.used", result.usage.get("total_tokens", 0))
    except Exception:
        pass  # Token usage not available

    ctx.add_event("ai.response.received", {"model": "llama3.2"})

    # In pydantic-ai 1.19.0+, use result.output instead of result.data
    return result.output


@trace
async def summarize_user(ctx, profile: UserProfile) -> UserSummary:
    """
    Summarize user profile using Pydantic AI.

    This function is automatically traced by autotel.
    """
    ctx.set_attribute("ai.model", "llama3.2:latest")
    ctx.set_attribute("ai.provider", "ollama")
    ctx.set_attribute("ai.operation", "summarize")

    prompt = f"Create a summary for this user: {profile.name}, age {profile.age}. Bio: {profile.bio}"

    # Create a temporary agent with UserSummary output type
    summary_agent = Agent(
        "openai:llama3.2",
        output_type=UserSummary,
        system_prompt="You are a helpful assistant that extracts and summarizes user information.",
    )
    result = await summary_agent.run(prompt)

    # Try to extract token usage if available
    try:
        if hasattr(result, "usage") and result.usage:
            ctx.set_attribute("ai.tokens.used", result.usage.get("total_tokens", 0))
    except Exception:
        pass  # Token usage not available

    ctx.add_event("ai.summary.completed")

    # In pydantic-ai 1.19.0+, use result.output instead of result.data
    return result.output


@trace
async def chat_with_agent(ctx, message: str) -> str:
    """
    Chat with the AI agent.

    This function is automatically traced by autotel.
    """
    ctx.set_attribute("ai.model", "llama3.2:latest")
    ctx.set_attribute("ai.provider", "ollama")
    ctx.set_attribute("ai.operation", "chat")
    ctx.set_attribute("user.message", message)

    result = await agent.run(message)

    # In pydantic-ai 1.19.0+, use result.output instead of result.data
    response_text = result.output if isinstance(result.output, str) else str(result.output)
    ctx.set_attribute("ai.response.length", len(response_text))
    ctx.add_event("ai.chat.completed")

    return response_text


async def main() -> None:
    """Main function demonstrating Pydantic AI with autotel."""
    print("=== Pydantic AI + autotel Example ===\n")

    # Example 1: Extract user profile
    print("1. Extracting user profile from text...")
    text = """
    Hi, I'm John Doe. I'm 30 years old and my email is john.doe@example.com.
    I'm a software engineer passionate about building scalable systems.
    I love Python and distributed systems.
    """
    try:
        profile = await extract_user_profile(text)
        print(f"   Extracted: {profile.name}, {profile.age}, {profile.email}")
        print(f"   Bio: {profile.bio}\n")
    except Exception as e:
        print(f"   Error: {e}\n")

    # Example 2: Summarize user
    if "profile" in locals():
        print("2. Summarizing user profile...")
        try:
            summary = await summarize_user(profile)
            print(f"   Summary: {summary.summary}")
            print(f"   Key points: {summary.key_points}\n")
        except Exception as e:
            print(f"   Error: {e}\n")

    # Example 3: Chat with agent
    print("3. Chatting with AI agent...")
    try:
        response = await chat_with_agent("What is Python?")
        print(f"   Response: {response[:100]}...\n")
    except Exception as e:
        print(f"   Error: {e}\n")

    print("Check console output above for trace spans!")


if __name__ == "__main__":
    asyncio.run(main())
