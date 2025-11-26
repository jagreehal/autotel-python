# FastAPI Example

Complete FastAPI application demonstrating autotel integration.

## Features Demonstrated

- FastAPI middleware integration
- Automatic request tracing
- HTTP attributes capture
- Status code tracking

## Running

```bash
# Install dependencies
pip install fastapi uvicorn

# Run the app
python examples/fastapi/app.py

# Or with uvicorn directly
uvicorn examples.fastapi.app:app --reload
```

## Testing

```bash
# Test endpoints
curl http://localhost:8000/
curl http://localhost:8000/users/123
curl -X POST http://localhost:8000/users -H "Content-Type: application/json" -d '{"name": "John"}'
```

Check your OTLP endpoint or console output for trace spans!
