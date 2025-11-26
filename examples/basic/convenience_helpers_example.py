"""
Example demonstrating convenience helper functions.

These helpers make common operations easier without needing to manually
get the current span or context.
"""

import autotel

# Initialize autotel
autotel.init(
    service_name="convenience-helpers-demo",
    subscribers=[],  # No subscribers for this demo
)


def process_user_request(user_id: str, action: str) -> None:
    """Example using attribute helpers."""
    with autotel.span("process_request"):
        # Set single attribute (convenience helper)
        autotel.set_attribute("user.id", user_id)

        # Set multiple attributes at once (convenience helper)
        autotel.set_attributes({
            "action.type": action,
            "request.priority": "high",
            "retry.count": 0,
        })

        # Add a span event (convenience helper)
        autotel.add_event("request.started", {
            "timestamp": "2024-01-01T00:00:00Z",
            "user_id": user_id,
        })

        # Simulate work
        result = f"Processed {action} for user {user_id}"

        # Get trace and span IDs for logging (convenience helpers)
        trace_id = autotel.get_trace_id()
        span_id = autotel.get_span_id()
        print(f"Trace ID: {trace_id}")
        print(f"Span ID: {span_id}")

        return result


def handle_error_scenario() -> None:
    """Example using error recording helper."""
    with autotel.span("risky_operation"):
        try:
            # Simulate an error
            raise ValueError("Something went wrong")
        except ValueError as e:
            # Record exception with additional context (convenience helper)
            autotel.record_exception(e, {
                "error.severity": "high",
                "error.component": "data_validator",
            })
            # Handle or re-raise
            print(f"Caught error: {e}")


def process_with_baggage(tenant_id: str, user_id: str) -> None:
    """Example using baggage helpers."""
    # Set baggage using context manager (for proper scoping)
    with autotel.with_baggage({
        "tenant.id": tenant_id,
        "user.id": user_id,
    }), autotel.span("tenant_operation"):
        # Read single baggage value (convenience helper)
        current_tenant = autotel.get_baggage("tenant.id")
        print(f"Processing for tenant: {current_tenant}")

        # Read all baggage (convenience helper)
        all_baggage = autotel.get_all_baggage()
        print(f"Full context: {all_baggage}")

        # Add more baggage if needed (convenience helper)
        autotel.set_baggage_value("request.id", "req-12345")

        # Nested operation inherits baggage
        with autotel.span("nested_operation"):
            nested_tenant = autotel.get_baggage("tenant.id")
            assert nested_tenant == tenant_id


def compare_approaches() -> None:
    """Show the difference between manual and convenience approaches."""

    print("\n=== Manual Approach (verbose) ===")
    with autotel.span("manual_example"):
        # Manual: Get span, check if recording, set attribute
        span = autotel.get_active_span()
        if span and span.is_recording():
            span.set_attribute("method", "manual")
            span.set_attribute("status", "active")
            span.add_event("processing")

    print("\n=== Convenience Approach (concise) ===")
    with autotel.span("convenience_example"):
        # Convenience: Direct helpers, no manual checks
        autotel.set_attributes({
            "method": "convenience",
            "status": "active",
        })
        autotel.add_event("processing")


def main() -> None:
    """Run all examples."""
    print("=" * 60)
    print("Convenience Helpers Demo")
    print("=" * 60)

    print("\n1. Processing user request with attributes...")
    result = process_user_request("user-123", "purchase")
    print(f"Result: {result}")

    print("\n2. Handling error scenario...")
    handle_error_scenario()

    print("\n3. Processing with baggage context...")
    process_with_baggage("tenant-456", "user-789")

    print("\n4. Comparing approaches...")
    compare_approaches()

    print("\n" + "=" * 60)
    print("Demo complete!")
    print("=" * 60)

    # Show summary
    print("\n📊 Summary of Convenience Helpers:")
    print("  ✅ set_attribute()     - Set single attribute")
    print("  ✅ set_attributes()    - Set multiple attributes at once")
    print("  ✅ add_event()         - Add span event")
    print("  ✅ record_exception()  - Record exception with auto-status")
    print("  ✅ get_trace_id()      - Get current trace ID")
    print("  ✅ get_span_id()       - Get current span ID")
    print("  ✅ get_baggage()       - Get single baggage value")
    print("  ✅ get_all_baggage()   - Get all baggage entries")
    print("  ✅ set_baggage_value() - Set baggage value")


if __name__ == "__main__":
    main()
