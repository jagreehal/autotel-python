# Flask Example

Complete Flask application demonstrating autotel integration.

## Features Demonstrated

- Flask integration with `init_autotel()`
- Automatic request tracing
- HTTP attributes capture
- Status code tracking

## Running

```bash
# Install dependencies
pip install flask

# Run the app
python examples/flask/app.py
```

## Testing

```bash
# Test endpoints
curl http://localhost:5000/
curl http://localhost:5000/users/123
curl -X POST http://localhost:5000/users -H "Content-Type: application/json" -d '{"name": "John"}'
```

Check your OTLP endpoint or console output for trace spans!
