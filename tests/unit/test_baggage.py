"""Tests for baggage functionality."""

from typing import Any

import pytest
from opentelemetry import context
from opentelemetry.baggage import propagation

from autotel import init, span, with_baggage
from autotel.baggage_span_processor import BaggageSpanProcessor
from autotel.exporters import InMemorySpanExporter
from autotel.processors import SimpleSpanProcessor


def _set_baggage_dict(ctx: Any, baggage_dict: Any) -> Any:
    """Helper to set multiple baggage entries from a dict."""
    new_context = ctx
    for key, value in baggage_dict.items():
        new_context = propagation.set_baggage(key, value, new_context)
    return new_context


@pytest.fixture
def exporter() -> Any:
    """Create in-memory exporter for testing."""
    exp = InMemorySpanExporter()
    init(service="test", span_processor=SimpleSpanProcessor(exp))
    return exp


class TestTraceContextBaggage:
    """Tests for TraceContext baggage methods."""

    def test_get_baggage_entry_from_context(self: Any, exporter: Any) -> None:
        """Test getting baggage entry from context."""
        _ = exporter
        # Create context with baggage
        active_context = context.get_current()
        baggage_dict = {"tenant.id": "tenant-123"}
        context_with_baggage = _set_baggage_dict(active_context, baggage_dict)

        token = context.attach(context_with_baggage)
        try:
            with span("test.operation") as ctx:
                tenant_id = ctx.get_baggage("tenant.id")
                assert tenant_id == "tenant-123"
        finally:
            context.detach(token)

        spans = exporter.get_finished_spans()
        assert len(spans) == 1

    def test_get_baggage_returns_none_when_not_set(self: Any, exporter: Any) -> None:
        """Test getting baggage entry that doesn't exist."""
        _ = exporter
        with span("test.operation") as ctx:
            value = ctx.get_baggage("nonexistent.key")
            assert value is None

    def test_set_baggage(self: Any, exporter: Any) -> None:
        """Test setting baggage entry."""
        _ = exporter
        with span("test.operation") as ctx:
            ctx.set_baggage("test.key", "test.value")
            value = ctx.get_baggage("test.key")
            assert value == "test.value"

    def test_set_baggage_propagates_to_inject(self: Any, exporter: Any) -> None:
        """Test that baggage set via set_baggage() is available to inject()."""
        _ = exporter
        from autotel.http import inject_trace_context

        with span("test.operation") as ctx:
            # Set baggage after span is created
            ctx.set_baggage("tenant.id", "tenant-123")
            ctx.set_baggage("user.id", "user-456")

            # Verify baggage is available
            assert ctx.get_baggage("tenant.id") == "tenant-123"
            assert ctx.get_baggage("user.id") == "user-456"

            # Verify baggage is propagated to inject()
            headers = inject_trace_context()
            # Baggage should be in headers (check for baggage header or verify context is updated)
            # The exact header format depends on OpenTelemetry's baggage propagator
            assert isinstance(headers, dict)
            # Verify that the active context has the updated baggage
            active_context = context.get_current()
            baggage = propagation.get_all(active_context)
            assert baggage is not None
            assert baggage.get("tenant.id") == "tenant-123"
            assert baggage.get("user.id") == "user-456"

    def test_set_baggage_propagates_to_child_spans(self: Any) -> None:
        """Test that baggage set via set_baggage() is visible to child spans.

        Baggage is propagated via BaggageSpanProcessor.
        """
        # Create a new exporter for this test
        testexporter = InMemorySpanExporter()
        # Initialize with baggage processor enabled
        init(service="test", span_processor=SimpleSpanProcessor(testexporter), baggage=True)

        with span("parent.operation") as parent_ctx:
            # Set baggage after parent span is created
            parent_ctx.set_baggage("tenant.id", "tenant-789")
            parent_ctx.set_baggage("user.id", "user-012")

            # Verify baggage is available in parent context
            assert parent_ctx.get_baggage("tenant.id") == "tenant-789"
            assert parent_ctx.get_baggage("user.id") == "user-012"

            # Create a child span - should see the updated baggage via BaggageSpanProcessor
            with span("child.operation") as child_ctx:
                # Verify baggage is available in the child context
                assert child_ctx.get_baggage("tenant.id") == "tenant-789"
                assert child_ctx.get_baggage("user.id") == "user-012"

        # Verify that both spans have baggage attributes (set by BaggageSpanProcessor)
        spans = testexporter.get_finished_spans()
        assert len(spans) >= 2

        # Find parent and child spans
        parent_span = next((s for s in spans if s.name == "parent.operation"), None)
        child_span = next((s for s in spans if s.name == "child.operation"), None)

        assert parent_span is not None, "Parent span not found"
        assert child_span is not None, "Child span not found"

        # Both should have baggage attributes since BaggageSpanProcessor reads from active context
        # Note: The parent span was created BEFORE set_baggage was called,
        # so it might not have attributes
        # But the child span was created AFTER, so it should have them
        assert child_span.attributes is not None
        assert child_span.attributes.get("baggage.tenant.id") == "tenant-789"
        assert child_span.attributes.get("baggage.user.id") == "user-012"

    def test_delete_baggage(self: Any, exporter: Any) -> None:
        """Test deleting baggage entry."""
        _ = exporter
        # Set baggage first
        active_context = context.get_current()
        baggage_dict = {"test.key": "test.value"}
        context_with_baggage = _set_baggage_dict(active_context, baggage_dict)

        token = context.attach(context_with_baggage)
        try:
            with span("test.operation") as ctx:
                # Verify it exists
                assert ctx.get_baggage("test.key") == "test.value"
                # Delete it
                ctx.delete_baggage("test.key")
                # Verify it's gone
                assert ctx.get_baggage("test.key") is None
        finally:
            context.detach(token)

    def test_delete_baggage_propagates_to_inject(self: Any, exporter: Any) -> None:
        """Test that baggage deletion via delete_baggage() is reflected in inject()."""
        _ = exporter
        from autotel.http import inject_trace_context

        # Set initial baggage
        active_context = context.get_current()
        baggage_dict = {"test.key": "test.value", "keep.key": "keep.value"}
        context_with_baggage = _set_baggage_dict(active_context, baggage_dict)

        token = context.attach(context_with_baggage)
        try:
            with span("test.operation") as ctx:
                # Verify initial baggage exists
                assert ctx.get_baggage("test.key") == "test.value"
                assert ctx.get_baggage("keep.key") == "keep.value"

                # Delete one baggage entry
                ctx.delete_baggage("test.key")

                # Verify deletion
                assert ctx.get_baggage("test.key") is None
                assert ctx.get_baggage("keep.key") == "keep.value"

                # Verify that the active context reflects the deletion
                headers = inject_trace_context()
                assert isinstance(headers, dict)
                active_context_after = context.get_current()
                baggage_after = propagation.get_all(active_context_after)
                assert baggage_after is not None
                assert "test.key" not in baggage_after
                assert baggage_after.get("keep.key") == "keep.value"
        finally:
            context.detach(token)

    def test_get_all_baggage(self: Any, exporter: Any) -> None:
        """Test getting all baggage entries."""
        _ = exporter
        # Create context with multiple baggage entries
        active_context = context.get_current()
        baggage_dict = {"key1": "value1", "key2": "value2"}
        context_with_baggage = _set_baggage_dict(active_context, baggage_dict)

        token = context.attach(context_with_baggage)
        try:
            with span("test.operation") as ctx:
                all_baggage = ctx.get_all_baggage()
                assert len(all_baggage) >= 2
                assert all_baggage.get("key1") == "value1"
                assert all_baggage.get("key2") == "value2"
        finally:
            context.detach(token)


class TestWithBaggage:
    """Tests for with_baggage helper function."""

    def test_with_baggage_sets_baggage_for_child_spans(self: Any, exporter: Any) -> None:
        """Test that with_baggage sets baggage for child spans."""
        with (
            span("parent.operation") as parent_ctx,
            with_baggage({"tenant.id": "tenant-456", "user.id": "user-789"}),
        ):
            # Check baggage is available
            assert parent_ctx.get_baggage("tenant.id") == "tenant-456"
            assert parent_ctx.get_baggage("user.id") == "user-789"

            # Create child span - should inherit baggage
            with span("child.operation") as child_ctx:
                assert child_ctx.get_baggage("tenant.id") == "tenant-456"
                assert child_ctx.get_baggage("user.id") == "user-789"

        spans = exporter.get_finished_spans()
        assert len(spans) == 2

    def test_with_baggage_works_with_sync_functions(self: Any, exporter: Any) -> None:
        """Test that with_baggage works with sync functions."""
        _ = exporter
        captured_baggage = None

        with span("test.operation") as ctx, with_baggage({"key": "value"}):
            captured_baggage = ctx.get_baggage("key")

        assert captured_baggage == "value"

    def test_with_baggage_merges_with_existing_baggage(self: Any, exporter: Any) -> None:
        """Test that with_baggage merges with existing baggage."""
        _ = exporter
        # Set initial baggage
        active_context = context.get_current()
        initial_baggage = {"existing.key": "existing-value"}
        context_with_baggage = _set_baggage_dict(active_context, initial_baggage)

        token = context.attach(context_with_baggage)
        try:
            with span("test.operation") as ctx:
                # New baggage should not be available yet
                assert ctx.get_baggage("new.key") is None

                # Add new baggage
                with with_baggage({"new.key": "new-value"}):
                    # New baggage should be available
                    assert ctx.get_baggage("new.key") == "new-value"
                    # Existing baggage should still be available
                    assert ctx.get_baggage("existing.key") == "existing-value"
        finally:
            context.detach(token)

    @pytest.mark.asyncio
    async def test_with_baggage_works_with_async(self: Any, exporter: Any) -> None:
        """Test that with_baggage works with async code."""
        _ = exporter

        async def async_operation() -> str | None:
            with span("async.operation") as ctx, with_baggage({"async.key": "async-value"}):
                return ctx.get_baggage("async.key")  # type: ignore[no-any-return]

        result = await async_operation()
        assert result == "async-value"


class TestBaggageSpanProcessor:
    """Tests for BaggageSpanProcessor."""

    def test_baggage_span_processor_copies_baggage_to_attributes_default_prefix(self: Any) -> None:
        """Test that BaggageSpanProcessor copies baggage to span attributes with default prefix."""
        exporter = InMemorySpanExporter()
        baggage_processor = BaggageSpanProcessor()
        from opentelemetry.sdk.trace import TracerProvider

        provider = TracerProvider()
        provider.add_span_processor(baggage_processor)
        provider.add_span_processor(SimpleSpanProcessor(exporter))

        init(service="test", span_processor=SimpleSpanProcessor(exporter))

        tracer = provider.get_tracer("test")

        # Set baggage
        active_context = context.get_current()
        baggage_dict = {"tenant.id": "tenant-123", "user.id": "user-456"}
        context_with_baggage = _set_baggage_dict(active_context, baggage_dict)

        # Create span within baggage context
        token = context.attach(context_with_baggage)
        try:
            with tracer.start_as_current_span("test-span"):
                pass
        finally:
            context.detach(token)

        # Flush to ensure spans are exported
        provider.force_flush()

        spans = exporter.get_finished_spans()
        assert len(spans) == 1
        assert spans[0].attributes is not None
        assert spans[0].attributes.get("baggage.tenant.id") == "tenant-123"
        assert spans[0].attributes is not None
        assert spans[0].attributes.get("baggage.user.id") == "user-456"

    def test_baggage_span_processor_custom_prefix(self: Any) -> None:
        """Test that BaggageSpanProcessor uses custom prefix."""
        exporter = InMemorySpanExporter()
        baggage_processor = BaggageSpanProcessor(prefix="ctx.")
        from opentelemetry.sdk.trace import TracerProvider

        provider = TracerProvider()
        provider.add_span_processor(baggage_processor)
        provider.add_span_processor(SimpleSpanProcessor(exporter))

        tracer = provider.get_tracer("test")

        # Set baggage
        active_context = context.get_current()
        baggage_dict = {"tenant.id": "tenant-123"}
        context_with_baggage = _set_baggage_dict(active_context, baggage_dict)

        # Create span within baggage context
        token = context.attach(context_with_baggage)
        try:
            with tracer.start_as_current_span("test-span"):
                pass
        finally:
            context.detach(token)

        # Flush to ensure spans are exported
        provider.force_flush()

        spans = exporter.get_finished_spans()
        assert len(spans) == 1
        assert spans[0].attributes is not None
        assert spans[0].attributes.get("ctx.tenant.id") == "tenant-123"

    def test_baggage_span_processor_no_prefix(self: Any) -> None:
        """Test that BaggageSpanProcessor works with no prefix."""
        exporter = InMemorySpanExporter()
        baggage_processor = BaggageSpanProcessor(prefix="")
        from opentelemetry.sdk.trace import TracerProvider

        provider = TracerProvider()
        provider.add_span_processor(baggage_processor)
        provider.add_span_processor(SimpleSpanProcessor(exporter))

        tracer = provider.get_tracer("test")

        # Set baggage
        active_context = context.get_current()
        baggage_dict = {"tenant.id": "tenant-123"}
        context_with_baggage = _set_baggage_dict(active_context, baggage_dict)

        # Create span within baggage context
        token = context.attach(context_with_baggage)
        try:
            with tracer.start_as_current_span("test-span"):
                pass
        finally:
            context.detach(token)

        # Flush to ensure spans are exported
        provider.force_flush()

        spans = exporter.get_finished_spans()
        assert len(spans) == 1
        assert spans[0].attributes is not None
        assert spans[0].attributes.get("tenant.id") == "tenant-123"

    def test_baggage_span_processor_handles_no_baggage(self: Any) -> None:
        """Test that BaggageSpanProcessor handles spans with no baggage gracefully."""
        exporter = InMemorySpanExporter()
        baggage_processor = BaggageSpanProcessor()
        from opentelemetry.sdk.trace import TracerProvider

        provider = TracerProvider()
        provider.add_span_processor(baggage_processor)
        provider.add_span_processor(SimpleSpanProcessor(exporter))

        tracer = provider.get_tracer("test")

        # Create span without baggage
        with tracer.start_as_current_span("test-span"):
            pass

        # Flush to ensure spans are exported
        provider.force_flush()

        spans = exporter.get_finished_spans()
        assert len(spans) == 1
        # Should have no baggage attributes
        assert spans[0].attributes is not None
        baggage_keys = [k for k in spans[0].attributes if k.startswith("baggage.")]
        assert len(baggage_keys) == 0

    def test_baggage_span_processor_multiple_entries(self: Any) -> None:
        """Test that BaggageSpanProcessor copies multiple baggage entries."""
        exporter = InMemorySpanExporter()
        baggage_processor = BaggageSpanProcessor()
        from opentelemetry.sdk.trace import TracerProvider

        provider = TracerProvider()
        provider.add_span_processor(baggage_processor)
        provider.add_span_processor(SimpleSpanProcessor(exporter))

        tracer = provider.get_tracer("test")

        # Set multiple baggage entries
        active_context = context.get_current()
        baggage_dict = {"key1": "value1", "key2": "value2", "key3": "value3"}
        context_with_baggage = _set_baggage_dict(active_context, baggage_dict)

        # Create span within baggage context
        token = context.attach(context_with_baggage)
        try:
            with tracer.start_as_current_span("test-span"):
                pass
        finally:
            context.detach(token)

        # Flush to ensure spans are exported
        provider.force_flush()

        spans = exporter.get_finished_spans()
        assert len(spans) == 1
        attrs = spans[0].attributes
        assert attrs is not None
        assert attrs.get("baggage.key1") == "value1"
        assert attrs.get("baggage.key2") == "value2"
        assert attrs.get("baggage.key3") == "value3"
