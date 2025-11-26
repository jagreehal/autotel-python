# Basic Examples

This directory contains basic examples demonstrating autotel usage.

## Examples

### `main.py`
Basic usage example showing:
- One-line initialization
- Simple `@trace` decorator usage
- Context parameter usage

### `functional_example.py`
Demonstrates functional API patterns:
- Batch instrumentation with `instrument()`
- Manual span creation with `span()`
- Root context isolation with `with_new_context()`

### `events_example.py`
Shows events integration:
- Initializing with events adapters
- Using `track()` function
- Auto-enrichment with trace context

### `complete_example.py`
Complete example demonstrating:
- All major features together
- Nested spans
- Analytics tracking
- Multiple patterns

## Running Examples

```bash
# Basic example
python examples/basic/main.py

# Functional API
python examples/basic/functional_example.py

# Analytics
python examples/basic/events_example.py

# Complete example
python examples/basic/complete_example.py
```

Note: Some examples require an OTLP endpoint. For testing, you can use:
- Console exporter (prints to console)
- In-memory exporter (for testing)
- Local OTLP collector
