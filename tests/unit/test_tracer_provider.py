"""Tests for isolated tracer provider support."""

from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor

from autotel.exporters import InMemorySpanExporter
from autotel.tracer_provider import (
    get_autotel_tracer,
    get_autotel_tracer_provider,
    set_autotel_tracer_provider,
)


def test_get_tracer_provider_default() -> None:
    """Test getting tracer provider when none is set."""
    # Clear any previous provider
    set_autotel_tracer_provider(None)

    provider = get_autotel_tracer_provider()
    assert provider is None


def test_set_and_get_tracer_provider() -> None:
    """Test setting and getting isolated tracer provider."""
    # Create a custom provider
    custom_provider = TracerProvider()

    # Set it
    set_autotel_tracer_provider(custom_provider)

    # Get it back
    provider = get_autotel_tracer_provider()
    assert provider is custom_provider

    # Clean up
    set_autotel_tracer_provider(None)


def test_get_autotel_tracer_without_provider() -> None:
    """Test getting tracer falls back to global when no provider is set."""
    # Clear any previous provider
    set_autotel_tracer_provider(None)

    # Should return global tracer
    tracer = get_autotel_tracer(__name__)
    assert isinstance(tracer, trace.Tracer)


def test_get_autotel_tracer_with_provider() -> None:
    """Test getting tracer from isolated provider."""
    # Create isolated provider with exporter
    exporter = InMemorySpanExporter()
    provider = TracerProvider()
    provider.add_span_processor(SimpleSpanProcessor(exporter))

    # Set it
    set_autotel_tracer_provider(provider)

    # Get tracer
    tracer = get_autotel_tracer(__name__)
    assert isinstance(tracer, trace.Tracer)

    # Create a span
    with tracer.start_as_current_span("test.operation") as span:
        span.set_attribute("test.key", "test_value")

    # Verify span was exported to isolated provider
    spans = exporter.get_finished_spans()
    assert len(spans) == 1
    assert spans[0].name == "test.operation"
    assert spans[0].attributes is not None
    assert spans[0].attributes.get("test.key") == "test_value"

    # Clean up
    exporter.clear()
    set_autotel_tracer_provider(None)


def test_get_autotel_tracer_with_version() -> None:
    """Test getting tracer with version and schema URL."""
    provider = TracerProvider()
    set_autotel_tracer_provider(provider)

    tracer = get_autotel_tracer(
        __name__, version="1.0.0", schema_url="https://opentelemetry.io/schemas/1.0.0"
    )
    assert isinstance(tracer, trace.Tracer)

    # Clean up
    set_autotel_tracer_provider(None)


def test_isolated_provider_independence() -> None:
    """Test that isolated provider doesn't affect global tracing."""
    # Create isolated provider with exporter
    isolated_exporter = InMemorySpanExporter()
    isolated_provider = TracerProvider()
    isolated_provider.add_span_processor(SimpleSpanProcessor(isolated_exporter))

    # Set isolated provider
    set_autotel_tracer_provider(isolated_provider)

    # Get tracer from isolated provider
    isolated_tracer = get_autotel_tracer("isolated")

    # Create span with isolated tracer
    with isolated_tracer.start_as_current_span("isolated.operation") as span:
        span.set_attribute("source", "isolated")

    # Verify span went to isolated exporter
    isolated_spans = isolated_exporter.get_finished_spans()
    assert len(isolated_spans) == 1
    assert isolated_spans[0].name == "isolated.operation"

    # Clean up
    isolated_exporter.clear()
    set_autotel_tracer_provider(None)


def test_provider_switching() -> None:
    """Test switching between providers."""
    # Create first provider
    exporter1 = InMemorySpanExporter()
    provider1 = TracerProvider()
    provider1.add_span_processor(SimpleSpanProcessor(exporter1))

    # Create second provider
    exporter2 = InMemorySpanExporter()
    provider2 = TracerProvider()
    provider2.add_span_processor(SimpleSpanProcessor(exporter2))

    # Use first provider
    set_autotel_tracer_provider(provider1)
    tracer = get_autotel_tracer(__name__)

    with tracer.start_as_current_span("span1"):
        pass

    assert len(exporter1.get_finished_spans()) == 1
    assert len(exporter2.get_finished_spans()) == 0

    # Switch to second provider
    set_autotel_tracer_provider(provider2)
    tracer = get_autotel_tracer(__name__)

    with tracer.start_as_current_span("span2"):
        pass

    # First exporter should still have 1 span
    assert len(exporter1.get_finished_spans()) == 1
    # Second exporter should now have 1 span
    assert len(exporter2.get_finished_spans()) == 1

    # Clean up
    exporter1.clear()
    exporter2.clear()
    set_autotel_tracer_provider(None)
