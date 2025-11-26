# Pydantic AI + autotel Example

This example demonstrates how to use Pydantic AI with Ollama and Llama3.2, integrated with autotel for automatic tracing.

## Prerequisites

1. **Install Ollama**: https://ollama.ai
2. **Pull Llama3.2 model**:
   ```bash
   ollama pull llama3.2
   ```

## Installation

```bash
# Install dependencies
pip install -r examples/pydantic_ai/requirements.txt

# Or install individually
pip install autotel pydantic-ai ollama
```

## Running the Examples

### Simple Example

```bash
# Make sure Ollama is running
ollama serve

# Run simple example
python examples/pydantic_ai/simple_example.py
```

### Basic Example

```bash
# Run basic example
python examples/pydantic_ai/ollama_example.py
```

### Advanced Example

```bash
# Run advanced example (multi-agent workflow)
python examples/pydantic_ai/advanced_example.py
```

## Examples Included

1. **simple_example.py** - Minimal setup, basic question/answer
2. **ollama_example.py** - Data extraction and summarization
3. **advanced_example.py** - Multi-agent workflow with code review

## Features Demonstrated

1. **Automatic Tracing** - All AI operations are automatically traced with `@trace`
2. **Structured Data Extraction** - Using Pydantic models with AI
3. **Multi-step Workflows** - Chaining AI operations with nested spans
4. **Error Handling** - Traced exceptions
5. **Analytics Integration** - Track AI usage with `track()`
6. **Span Attributes** - AI model, provider, operation type automatically recorded

## Example Output

The example will:
1. Extract user profile from text using AI
2. Summarize the extracted profile
3. Chat with the AI agent

All operations are automatically traced and you'll see spans in the console output.

## Integration Details

### Automatic Tracing

The `@trace` decorator automatically:
- Creates spans for each AI operation
- Records AI model and provider information
- Tracks token usage (if available)
- Records events for AI responses
- Captures exceptions

### Span Attributes

Each AI operation includes:
- `ai.model` - Model name (e.g., "llama3.2")
- `ai.provider` - Provider (e.g., "ollama")
- `ai.operation` - Operation type (extract, summarize, chat)
- `ai.tokens.used` - Token usage (if available)

### Events

- `ai.response.received` - When AI response is received
- `ai.summary.completed` - When summary is completed
- `ai.chat.completed` - When chat completes

## Customization

### Using Different Models

```python
agent = Agent(
    "ollama/llama3.1",  # Different model
    system_prompt="Your custom prompt",
)
```

### Using OTLP Exporter

```python
from autotel import init

init(
    service="pydantic-ai-app",
    endpoint="http://localhost:4318",  # OTLP endpoint
)
```

### Adding Analytics

```python
from autotel import init, track
from autotel.subscribers import PostHogSubscriber

init(
    service="pydantic-ai-app",
    subscribers=[
        PostHogSubscriber(api_key="phc_..."),
    ]
)

# Note: PostHogSubscriber is kept in subscribers submodule
# as it's an optional integration (requires httpx)

@trace
async def extract_user_profile(ctx, text: str):
    profile = await agent.run(text, response_model=UserProfile)
    
    # Track events event
    track("ai.profile.extracted", {
        "model": "llama3.2",
        "tokens": getattr(profile, "usage", {}).get("total_tokens", 0),
    })
    
    return profile
```

## Troubleshooting

### Ollama Not Running

```bash
# Start Ollama
ollama serve
```

### Model Not Found

```bash
# Pull the model
ollama pull llama3.2
```

### Connection Errors

Make sure Ollama is accessible at `http://localhost:11434` (default).
