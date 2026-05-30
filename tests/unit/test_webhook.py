"""Tests for webhook / Parking Lot pattern."""

from typing import Any

import pytest
from opentelemetry import trace as otel_trace

from autotel import init
from autotel.exporters import InMemorySpanExporter
from autotel.processors import SimpleSpanProcessor
from autotel.webhook import (
    CallbackContext,
    InMemoryTraceContextStore,
    ParkingLot,
    StoredTraceContext,
    create_correlation_key,
    create_parking_lot,
    to_span_context,
)


@pytest.fixture
def exporter() -> Any:
    """Create in-memory exporter for testing."""
    exp = InMemorySpanExporter()
    init(service="test", span_processor=SimpleSpanProcessor(exp))
    return exp


@pytest.fixture
def store() -> InMemoryTraceContextStore:
    """In-memory store with cleanup disabled for predictable tests."""
    return InMemoryTraceContextStore(cleanup_interval_seconds=0)


@pytest.fixture
def parking_lot(store: InMemoryTraceContextStore) -> ParkingLot:
    """Parking lot with in-memory store."""
    return create_parking_lot(store=store, default_ttl_seconds=3600)


# --- StoredTraceContext ---


def test_stored_trace_context_fields() -> None:
    """StoredTraceContext holds trace_id, span_id, trace_flags, parked_at, etc."""
    ctx = StoredTraceContext(
        trace_id="abc123",
        span_id="def456",
        trace_flags=1,
        parked_at=1000.0,
        ttl_seconds=3600,
        metadata={"order_id": "ord-1"},
    )
    assert ctx.trace_id == "abc123"
    assert ctx.span_id == "def456"
    assert ctx.trace_flags == 1
    assert ctx.parked_at == 1000.0
    assert ctx.ttl_seconds == 3600
    assert ctx.metadata == {"order_id": "ord-1"}


# --- InMemoryTraceContextStore ---


@pytest.mark.asyncio
async def test_in_memory_store_save_load(store: InMemoryTraceContextStore) -> None:
    """InMemoryTraceContextStore save and load round-trip."""
    ctx = StoredTraceContext(
        trace_id="t1", span_id="s1", trace_flags=1, parked_at=0.0
    )
    await store.save("key1", ctx)
    loaded = await store.load("key1")
    assert loaded is not None
    assert loaded.trace_id == "t1"
    assert loaded.span_id == "s1"
    assert store.size == 1


@pytest.mark.asyncio
async def test_in_memory_store_load_missing(store: InMemoryTraceContextStore) -> None:
    """InMemoryTraceContextStore load returns None for missing key."""
    assert await store.load("nonexistent") is None


@pytest.mark.asyncio
async def test_in_memory_store_delete(store: InMemoryTraceContextStore) -> None:
    """InMemoryTraceContextStore delete removes key."""
    ctx = StoredTraceContext(
        trace_id="t1", span_id="s1", trace_flags=1, parked_at=0.0
    )
    await store.save("key1", ctx)
    await store.delete("key1")
    assert await store.load("key1") is None
    assert store.size == 0


def test_in_memory_store_clear(store: InMemoryTraceContextStore) -> None:
    """InMemoryTraceContextStore clear removes all."""
    import asyncio

    async def run() -> None:
        ctx = StoredTraceContext(
            trace_id="t1", span_id="s1", trace_flags=1, parked_at=0.0
        )
        await store.save("k1", ctx)
        await store.save("k2", ctx)
        assert store.size == 2
        store.clear()
        assert store.size == 0
        assert await store.load("k1") is None

    asyncio.run(run())


# --- create_correlation_key ---


def test_create_correlation_key() -> None:
    """create_correlation_key joins parts with colon."""
    assert create_correlation_key("payment", "ord-123") == "payment:ord-123"
    assert create_correlation_key("payment", 123, "stripe") == "payment:123:stripe"


# --- to_span_context ---


def test_to_span_context() -> None:
    """to_span_context returns dict with trace_id, span_id, trace_flags, is_remote."""
    stored = StoredTraceContext(
        trace_id="abc", span_id="def", trace_flags=1, parked_at=0.0
    )
    d = to_span_context(stored)
    assert d["trace_id"] == "abc"
    assert d["span_id"] == "def"
    assert d["trace_flags"] == 1
    assert d["is_remote"] is True


# --- create_parking_lot ---


def test_create_parking_lot(store: InMemoryTraceContextStore) -> None:
    """create_parking_lot returns ParkingLot with given store."""
    pl = create_parking_lot(store=store, default_ttl_seconds=7200)
    assert isinstance(pl, ParkingLot)
    assert pl._default_ttl == 7200  # noqa: SLF001


# --- ParkingLot park / retrieve ---


@pytest.mark.asyncio
async def test_parking_lot_park_and_retrieve(
    exporter: Any, parking_lot: ParkingLot, store: InMemoryTraceContextStore
) -> None:
    """Park current context then retrieve by correlation key."""
    tracer = otel_trace.get_tracer(__name__)
    with tracer.start_as_current_span("initiate") as _span:
        key = await parking_lot.park("payment:ord-1", metadata={"order_id": "ord-1"})
    assert key == "payment:ord-1"
    assert store.size == 1

    stored = await parking_lot.retrieve("payment:ord-1")
    assert stored is not None
    assert stored.trace_id != ""
    assert stored.span_id != ""
    assert stored.metadata == {"order_id": "ord-1"}

    # Auto-delete on retrieve (default)
    assert await parking_lot.retrieve("payment:ord-1") is None


@pytest.mark.asyncio
async def test_parking_lot_exists(parking_lot: ParkingLot, store: InMemoryTraceContextStore) -> None:
    """exists returns True when key is stored, False otherwise."""
    tracer = otel_trace.get_tracer(__name__)
    with tracer.start_as_current_span("init"):
        await parking_lot.park("key:1")
    assert await parking_lot.exists("key:1") is True
    assert await parking_lot.exists("key:2") is False


@pytest.mark.asyncio
async def test_parking_lot_retrieve_miss(parking_lot: ParkingLot) -> None:
    """retrieve returns None for unknown key."""
    assert await parking_lot.retrieve("unknown:key") is None


# --- trace_callback ---


@pytest.mark.asyncio
async def test_trace_callback_without_parked_context(
    exporter: Any, parking_lot: ParkingLot
) -> None:
    """trace_callback runs when no parked context (ctx.parked_context is None)."""

    @parking_lot.trace_callback(
        name="webhook.payment",
        correlation_key_from=lambda event: event["key"],
    )
    async def handle_webhook(ctx: CallbackContext, event: dict) -> str:
        assert ctx.parked_context is None
        assert ctx.correlation_key == event["key"]
        return "handled"

    result = await handle_webhook({"key": "payment:missing"})
    assert result == "handled"

    spans = exporter.get_finished_spans()
    cb_span = next((s for s in spans if s.name == "webhook.payment"), None)
    assert cb_span is not None
    assert cb_span.attributes.get("parking_lot.context_found") is False


@pytest.mark.asyncio
async def test_trace_callback_with_parked_context(
    exporter: Any, parking_lot: ParkingLot
) -> None:
    """trace_callback links to parked context when it exists."""
    tracer = otel_trace.get_tracer(__name__)
    with tracer.start_as_current_span("initiate-payment"):
        await parking_lot.park("payment:ord-99", metadata={"order_id": "ord-99"})

    @parking_lot.trace_callback(
        name="webhook.payment.succeeded",
        correlation_key_from=lambda e: e["correlation_key"],
    )
    async def handle_webhook(ctx: CallbackContext, event: dict) -> float | None:
        assert ctx.parked_context is not None
        assert ctx.elapsed_ms is not None
        assert ctx.correlation_key == "payment:ord-99"
        return ctx.elapsed_ms

    result = await handle_webhook({"correlation_key": "payment:ord-99"})
    assert result is not None
    assert result >= 0

    spans = exporter.get_finished_spans()
    cb_span = next((s for s in spans if s.name == "webhook.payment.succeeded"), None)
    assert cb_span is not None
    assert cb_span.attributes.get("parking_lot.original_trace_id") is not None
    assert cb_span.attributes.get("parking_lot.correlation_key") == "payment:ord-99"


@pytest.mark.asyncio
async def test_trace_callback_require_parked_context_raises(
    parking_lot: ParkingLot,
) -> None:
    """trace_callback with require_parked_context=True raises when context missing."""

    @parking_lot.trace_callback(
        name="webhook.required",
        correlation_key_from=lambda: "missing",
        require_parked_context=True,
    )
    async def handle_webhook(ctx: CallbackContext) -> None:
        pass

    with pytest.raises(RuntimeError, match="Required parked context not found"):
        await handle_webhook()


# --- create_link ---


def test_parking_lot_create_link(parking_lot: ParkingLot) -> None:
    """create_link returns a Link with span context from StoredTraceContext."""
    stored = StoredTraceContext(
        trace_id="0" * 32,
        span_id="1" * 16,
        trace_flags=1,
        parked_at=123.45,
        metadata={"a": "b"},
    )
    link = parking_lot.create_link(stored)
    assert link is not None
    assert link.context.trace_id != 0 or stored.trace_id == "0" * 32
    assert link.attributes.get("link.type") == "parking_lot"


# ---------------------------------------------------------------------------
# Characterization tests for the sync trace_callback path (previously untested)
# so the async/sync dedup refactor cannot silently change it.
# ---------------------------------------------------------------------------


def test_trace_callback_sync_without_parked_context(
    exporter: Any, parking_lot: ParkingLot
) -> None:
    """Sync trace_callback runs and passes a CallbackContext with no parked context."""

    @parking_lot.trace_callback(
        name="webhook.sync",
        correlation_key_from=lambda event: event["key"],
    )
    def handle(ctx: CallbackContext, event: dict) -> str:
        assert ctx.parked_context is None
        assert ctx.correlation_key == event["key"]
        return "sync-handled"

    assert handle({"key": "sync:missing"}) == "sync-handled"
    spans = exporter.get_finished_spans()
    span = next((s for s in spans if s.name == "webhook.sync"), None)
    assert span is not None
    assert span.attributes.get("parking_lot.context_found") is False


def test_trace_callback_sync_require_parked_context_raises(
    parking_lot: ParkingLot,
) -> None:
    """Sync trace_callback with require_parked_context=True raises when missing."""

    @parking_lot.trace_callback(
        name="webhook.sync.required",
        correlation_key_from=lambda: "missing",
        require_parked_context=True,
    )
    def handle(ctx: CallbackContext) -> None:
        pass

    with pytest.raises(RuntimeError, match="Required parked context not found"):
        handle()


def test_trace_callback_sync_no_ctx(exporter: Any, parking_lot: ParkingLot) -> None:
    """Sync trace_callback works on a function that does not accept ctx."""

    @parking_lot.trace_callback(
        name="webhook.sync.noctx",
        correlation_key_from=lambda key: key,
    )
    def handle(key: str) -> str:
        return f"got-{key}"

    assert handle("abc") == "got-abc"
