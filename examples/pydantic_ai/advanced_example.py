"""Advanced Pydantic AI example with autotel - multi-agent workflow."""

import asyncio
import os

# Set OpenAI base URL to point to Ollama's OpenAI-compatible endpoint
# Ollama doesn't require an API key, but OpenAI client needs one set
os.environ.setdefault("OPENAI_BASE_URL", "http://localhost:11434/v1")
os.environ.setdefault("OPENAI_API_KEY", "ollama")  # Dummy key for Ollama

from pydantic import BaseModel, Field
from pydantic_ai import Agent

from autotel import (
    ConsoleSpanExporter,
    SimpleSpanProcessor,
    init,
    span,
    trace,
    track,
)

# Initialize autotel
init(
    service="pydantic-ai-advanced",
    span_processor=SimpleSpanProcessor(ConsoleSpanExporter()),
)


# Data models
class CodeReview(BaseModel):
    """Code review model."""

    score: int = Field(description="Review score from 1-10", ge=1, le=10)
    issues: list[str] = Field(description="List of issues found")
    suggestions: list[str] = Field(description="Suggestions for improvement")
    summary: str = Field(description="Overall review summary")


class CodeExplanation(BaseModel):
    """Code explanation model."""

    explanation: str = Field(description="Explanation of what the code does")
    complexity: str = Field(description="Complexity level (low/medium/high)")
    improvements: list[str] = Field(description="Potential improvements")


# Create specialized agents with Ollama via OpenAI-compatible API
code_reviewer = Agent(
    "openai:llama3.2",
    output_type=CodeReview,
    system_prompt="You are an expert code reviewer. Analyze code and provide constructive feedback.",
)

code_explainer = Agent(
    "openai:llama3.2",
    output_type=CodeExplanation,
    system_prompt="You are a code explanation assistant. Explain code in simple terms.",
)


@trace
async def review_code(ctx, code: str) -> CodeReview:
    """
    Review code using AI agent.

    Automatically traced with autotel.
    """
    ctx.set_attribute("ai.model", "llama3.2")
    ctx.set_attribute("ai.provider", "ollama")
    ctx.set_attribute("ai.operation", "code_review")
    ctx.set_attribute("code.length", len(code))

    prompt = f"Review this code and provide feedback:\n\n```python\n{code}\n```"

    with span("ai.agent.run") as ai_span:
        result = await code_reviewer.run(prompt)
        ai_span.set_attribute("ai.response.received", True)

    # In pydantic-ai 1.19.0+, use result.output instead of result.data
    ctx.set_attribute("review.score", result.output.score)
    ctx.set_attribute("review.issues.count", len(result.output.issues))
    ctx.add_event("code.review.completed", {"score": result.output.score})

    # Track events (auto-enriched with trace context)
    track("code.reviewed", {
        "score": result.output.score,
        "issues_count": len(result.output.issues),
        "model": "llama3.2",
    })

    return result.output


@trace
async def explain_code(ctx, code: str) -> CodeExplanation:
    """
    Explain code using AI agent.

    Automatically traced with autotel.
    """
    ctx.set_attribute("ai.model", "llama3.2")
    ctx.set_attribute("ai.provider", "ollama")
    ctx.set_attribute("ai.operation", "code_explanation")
    ctx.set_attribute("code.length", len(code))

    prompt = f"Explain what this code does:\n\n```python\n{code}\n```"

    with span("ai.agent.run") as ai_span:
        result = await code_explainer.run(prompt)
        ai_span.set_attribute("ai.response.received", True)

    # In pydantic-ai 1.19.0+, use result.output instead of result.data
    ctx.set_attribute("explanation.complexity", result.output.complexity)
    ctx.add_event("code.explained", {"complexity": result.output.complexity})

    return result.output


@trace
async def analyze_codebase(ctx, code_snippets: list[str]) -> dict:
    """
    Analyze multiple code snippets.

    Demonstrates nested spans and batch processing.
    """
    ctx.set_attribute("codebase.snippets.count", len(code_snippets))

    reviews = []
    explanations = []

    for i, code in enumerate(code_snippets):
        with span(f"code_snippet.{i}") as snippet_span:
            snippet_span.set_attribute("snippet.index", i)
            snippet_span.set_attribute("snippet.length", len(code))

            # Review
            review = await review_code(code)
            reviews.append(review)

            # Explain
            explanation = await explain_code(code)
            explanations.append(explanation)

    # Aggregate results
    avg_score = sum(r.score for r in reviews) / len(reviews) if reviews else 0
    ctx.set_attribute("codebase.avg_score", avg_score)
    ctx.add_event("codebase.analysis.completed", {
        "snippets_analyzed": len(code_snippets),
        "avg_score": avg_score,
    })

    # Track events (auto-enriched with trace context)
    track("codebase.analyzed", {
        "snippets_count": len(code_snippets),
        "avg_score": avg_score,
        "model": "llama3.2",
    })

    return {
        "reviews": [r.model_dump() for r in reviews],
        "explanations": [e.model_dump() for e in explanations],
        "avg_score": avg_score,
    }


async def main() -> None:
    """Main function demonstrating advanced Pydantic AI usage."""
    print("=== Advanced Pydantic AI + autotel Example ===\n")

    # Example code snippets
    code1 = """
def fibonacci(n) -> None:
    if n <= 1:
        return n
    return fibonacci(n-1) + fibonacci(n-2)
"""

    code2 = """
def process_data(data) -> None:
    result = []
    for item in data:
        if item > 0:
            result.append(item * 2)
    return result
"""

    # Analyze codebase
    print("Analyzing codebase...")
    try:
        analysis = await analyze_codebase([code1, code2])
        print(f"   Analyzed {len(analysis['reviews'])} snippets")
        print(f"   Average score: {analysis['avg_score']:.1f}/10")
        print(f"   First review summary: {analysis['reviews'][0]['summary'][:100]}...\n")
    except Exception as e:
        print(f"   Error: {e}\n")
        import traceback
        traceback.print_exc()

    print("Check console output above for detailed trace spans!")


if __name__ == "__main__":
    asyncio.run(main())
