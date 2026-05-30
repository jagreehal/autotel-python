"""Webhook and callback tracing with the "Parking Lot" pattern.

When initiating async operations that return hours/days later (webhooks,
payment callbacks, human approvals), you can't keep a span open. This module
provides utilities to "park" trace context and retrieve it when callbacks arrive.

Example:
    >>> from autotel.webhook import create_parking_lot, InMemoryTraceContextStore
    >>>
    >>> parking_lot = create_parking_lot(
    ...     store=InMemoryTraceContextStore(),
    ...     default_ttl_seconds=86400,  # 24 hours
    ... )
    >>>
    >>> # When initiating payment
    >>> @trace("initiate-payment")
    ... async def initiate_payment(ctx, order_id: str):
    ...     await parking_lot.park(f"payment:{order_id}", metadata={"order_id": order_id})
    ...     await stripe_client.create_payment_intent(metadata={"order_id": order_id})
    >>>
    >>> # When Stripe webhook arrives (hours later)
    >>> @parking_lot.trace_callback(
    ...     name="stripe.webhook.payment_intent.succeeded",
    ...     correlation_key_from=lambda event: f"payment:{event['data']['object']['metadata']['order_id']}",
    ... )
    ... async def handle_stripe_webhook(ctx, event: dict):
    ...     # ctx.parked_context contains the original trace context
    ...     # ctx.elapsed_ms shows time since payment was initiated
    ...     await fulfill_order(event["data"]["object"])
"""

from __future__ import annotations

import functools
import inspect
import threading
import time
from abc import ABC, abstractmethod
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any, ParamSpec, TypeVar

from opentelemetry import trace as otel_trace
from opentelemetry.trace import Link, Span, SpanKind, StatusCode

from .context import TraceContext
from .decorators import CTX_PARAM_NAMES, _rewrite_signature_without_ctx
from .operation_context import run_in_operation_context

P = ParamSpec("P")
R = TypeVar("R")


@dataclass
class StoredTraceContext:
    """Stored trace context for parking lot pattern."""

    trace_id: str
    """Trace ID from the original span."""

    span_id: str
    """Span ID from the original span."""

    trace_flags: int
    """Trace flags (sampling decision)."""

    parked_at: float
    """Unix timestamp when the context was parked."""

    ttl_seconds: float | None = None
    """Optional TTL in seconds."""

    metadata: dict[str, str] | None = None
    """User-provided metadata."""


class TraceContextStore(ABC):
    """Interface for trace context storage backends.

    Implement this interface to use different storage backends (Redis, DynamoDB, etc.)
    """

    @abstractmethod
    async def save(self, key: str, context: StoredTraceContext) -> None:
        """Save trace context with a correlation key.

        Args:
            key: Unique correlation key (e.g., "payment:order-123")
            context: The trace context to store
        """

    @abstractmethod
    async def load(self, key: str) -> StoredTraceContext | None:
        """Load trace context by correlation key.

        Args:
            key: The correlation key used when parking

        Returns:
            The stored context, or None if not found/expired
        """

    @abstractmethod
    async def delete(self, key: str) -> None:
        """Delete trace context by correlation key.

        Args:
            key: The correlation key to delete
        """


class InMemoryTraceContextStore(TraceContextStore):
    """In-memory trace context store.

    Useful for testing and development. For production, use a persistent
    store like Redis or DynamoDB.

    Example:
        >>> store = InMemoryTraceContextStore()
        >>> parking_lot = create_parking_lot(store=store)
    """

    def __init__(self, cleanup_interval_seconds: float = 60.0) -> None:
        """Initialize the in-memory store.

        Args:
            cleanup_interval_seconds: Interval for cleaning up expired entries.
                Set to 0 to disable automatic cleanup.
        """
        self._store: dict[str, StoredTraceContext] = {}
        self._lock = threading.Lock()
        self._cleanup_interval = cleanup_interval_seconds
        self._cleanup_timer: threading.Timer | None = None

        if cleanup_interval_seconds > 0:
            self._start_cleanup_timer()

    def _start_cleanup_timer(self) -> None:
        """Start the periodic cleanup timer."""
        self._cleanup_timer = threading.Timer(self._cleanup_interval, self._cleanup)
        self._cleanup_timer.daemon = True
        self._cleanup_timer.start()

    def _cleanup(self) -> None:
        """Remove expired entries."""
        now = time.time()
        with self._lock:
            expired = [
                key
                for key, ctx in self._store.items()
                if ctx.ttl_seconds and (now - ctx.parked_at) > ctx.ttl_seconds
            ]
            for key in expired:
                del self._store[key]

        # Reschedule cleanup
        if self._cleanup_interval > 0:
            self._start_cleanup_timer()

    async def save(self, key: str, context: StoredTraceContext) -> None:
        """Save trace context."""
        with self._lock:
            self._store[key] = context

    async def load(self, key: str) -> StoredTraceContext | None:
        """Load trace context."""
        with self._lock:
            context = self._store.get(key)
            if not context:
                return None

            # Check TTL expiration
            if context.ttl_seconds:
                age = time.time() - context.parked_at
                if age > context.ttl_seconds:
                    del self._store[key]
                    return None

            return context

    async def delete(self, key: str) -> None:
        """Delete trace context."""
        with self._lock:
            self._store.pop(key, None)

    @property
    def size(self) -> int:
        """Get number of stored contexts (for testing)."""
        with self._lock:
            return len(self._store)

    def clear(self) -> None:
        """Clear all stored contexts (for testing)."""
        with self._lock:
            self._store.clear()

    def destroy(self) -> None:
        """Stop the cleanup timer."""
        if self._cleanup_timer:
            self._cleanup_timer.cancel()
            self._cleanup_timer = None


class CallbackContext(TraceContext):
    """Extended context for callback handlers."""

    def __init__(
        self,
        span: otel_trace.Span,
        parked_context: StoredTraceContext | None,
        elapsed_ms: float | None,
        correlation_key: str,
    ) -> None:
        """Initialize callback context.

        Args:
            span: The span for this callback.
            parked_context: The retrieved parked context, if found.
            elapsed_ms: Time elapsed since context was parked (ms).
            correlation_key: The correlation key used for retrieval.
        """
        super().__init__(span)
        self.parked_context = parked_context
        self.elapsed_ms = elapsed_ms
        self.correlation_key = correlation_key


class ParkingLot:
    """Parking lot for trace context storage and retrieval.

    The parking lot pattern allows you to "park" trace context before
    initiating an async operation (webhook, payment, etc.) and retrieve
    it when the callback arrives.

    Example:
        >>> parking_lot = create_parking_lot(
        ...     store=InMemoryTraceContextStore(),
        ...     default_ttl_seconds=86400,
        ... )
        >>>
        >>> # Park context before initiating payment
        >>> async with trace("initiate-payment") as ctx:
        ...     await parking_lot.park(f"payment:{order_id}")
        ...     await stripe.create_payment(order_id)
        >>>
        >>> # Retrieve when callback arrives
        >>> context = await parking_lot.retrieve(f"payment:{order_id}")
    """

    def __init__(
        self,
        store: TraceContextStore,
        default_ttl_seconds: float = 86400,  # 24 hours
        key_prefix: str = "parkingLot:",
        auto_delete_on_retrieve: bool = True,
        on_miss: Callable[[str], None] | None = None,
    ) -> None:
        """Initialize the parking lot.

        Args:
            store: Storage backend for parked contexts.
            default_ttl_seconds: Default TTL in seconds (default: 24 hours).
            key_prefix: Prefix for all correlation keys.
            auto_delete_on_retrieve: Whether to auto-delete after retrieval.
            on_miss: Callback when context is not found.
        """
        self._store = store
        self._default_ttl = default_ttl_seconds
        self._key_prefix = key_prefix
        self._auto_delete = auto_delete_on_retrieve
        self._on_miss = on_miss

    def _prefix_key(self, key: str) -> str:
        """Apply key prefix."""
        return f"{self._key_prefix}{key}"

    async def park(
        self,
        correlation_key: str,
        metadata: dict[str, str] | None = None,
        ttl_seconds: float | None = None,
    ) -> str:
        """Park current trace context before initiating async operation.

        Call this before sending a webhook, initiating a payment, or starting
        any operation that will complete via callback.

        Args:
            correlation_key: Unique key to retrieve context later (e.g., "payment:order-123")
            metadata: Optional metadata to store with the context
            ttl_seconds: Optional TTL override (uses default if not provided)

        Returns:
            The correlation key (without prefix, for caller convenience)

        Example:
            >>> await parking_lot.park(f"payment:{order_id}", metadata={
            ...     "customer_id": customer.id,
            ...     "amount": str(payment.amount),
            ... })
        """
        span = otel_trace.get_current_span()
        span_context = span.get_span_context() if span else None

        full_key = self._prefix_key(correlation_key)

        stored_context = StoredTraceContext(
            trace_id=format(span_context.trace_id, "032x") if span_context else "",
            span_id=format(span_context.span_id, "016x") if span_context else "",
            trace_flags=span_context.trace_flags if span_context else 0,
            parked_at=time.time(),
            ttl_seconds=ttl_seconds or self._default_ttl,
            metadata=metadata,
        )

        await self._store.save(full_key, stored_context)

        # Add event to current span
        if span and span.is_recording():
            event_attrs: dict[str, Any] = {
                "parking_lot.correlation_key": correlation_key,
                "parking_lot.ttl_seconds": stored_context.ttl_seconds,
            }
            if metadata:
                for k, v in metadata.items():
                    event_attrs[f"parking_lot.metadata.{k}"] = v
            span.add_event("trace_context_parked", event_attrs)

        return correlation_key

    async def retrieve(self, correlation_key: str) -> StoredTraceContext | None:
        """Retrieve parked context when callback arrives.

        Args:
            correlation_key: The key used when parking

        Returns:
            The stored context, or None if not found/expired
        """
        full_key = self._prefix_key(correlation_key)
        stored_context = await self._store.load(full_key)

        if not stored_context:
            if self._on_miss:
                self._on_miss(correlation_key)
            return None

        if self._auto_delete:
            await self._store.delete(full_key)

        return stored_context

    async def exists(self, correlation_key: str) -> bool:
        """Check if a parked context exists (without retrieving/deleting it).

        Args:
            correlation_key: The key to check

        Returns:
            True if context exists and hasn't expired
        """
        full_key = self._prefix_key(correlation_key)
        context = await self._store.load(full_key)
        return context is not None

    def create_link(self, stored_context: StoredTraceContext) -> Link:
        """Create a span link from stored context.

        Args:
            stored_context: The stored trace context

        Returns:
            A span link that can be added to a span
        """
        from opentelemetry.trace import SpanContext, TraceFlags

        span_context = SpanContext(
            trace_id=int(stored_context.trace_id, 16) if stored_context.trace_id else 0,
            span_id=int(stored_context.span_id, 16) if stored_context.span_id else 0,
            is_remote=True,
            trace_flags=TraceFlags(stored_context.trace_flags),
        )

        link_attrs: dict[str, Any] = {
            "link.type": "parking_lot",
            "parking_lot.parked_at": stored_context.parked_at,
        }
        if stored_context.metadata:
            link_attrs["parking_lot.has_metadata"] = True

        return Link(context=span_context, attributes=link_attrs)

    def trace_callback(
        self,
        name: str,
        correlation_key_from: Callable[..., str],
        attributes: dict[str, Any] | None = None,
        require_parked_context: bool = False,
    ) -> Callable[[Callable[P, R]], Callable[P, R]]:
        """Decorator for callback handlers with automatic context retrieval.

        Creates a traced function that:
        1. Extracts correlation key from arguments
        2. Retrieves parked context from storage
        3. Creates a span link to the original trace
        4. Provides elapsed time since parking

        Args:
            name: Span name for the callback handler
            correlation_key_from: Function to extract correlation key from arguments
            attributes: Additional span attributes
            require_parked_context: If True, raise error when context not found

        Returns:
            Decorated function

        Example:
            >>> @parking_lot.trace_callback(
            ...     name="stripe.webhook.payment_intent.succeeded",
            ...     correlation_key_from=lambda event: f"payment:{event['data']['order_id']}",
            ... )
            ... async def handle_webhook(ctx, event: dict):
            ...     print(f"Payment completed after {ctx.elapsed_ms}ms")
            ...     await process_payment(event)
        """
        parking_lot = self

        def decorator(func: Callable[P, R]) -> Callable[P, R]:
            sig = inspect.signature(func)
            params = list(sig.parameters.keys())
            needs_ctx = len(params) > 0 and params[0] in CTX_PARAM_NAMES

            # Shared logic for the async and sync wrappers. Only the parked-context
            # retrieval (await vs event loop) and the func call (await vs direct)
            # differ; the span setup below is identical, so it lives in one place.
            def _elapsed(parked_context: StoredTraceContext | None) -> float | None:
                if parked_context is None:
                    return None
                return (time.time() - parked_context.parked_at) * 1000

            def _links(parked_context: StoredTraceContext | None) -> list[Link]:
                if parked_context is None:
                    return []
                return [parking_lot.create_link(parked_context)]

            def _setup_span(
                span: Span,
                correlation_key: str,
                parked_context: StoredTraceContext | None,
                elapsed_ms: float | None,
            ) -> None:
                span.set_attribute("parking_lot.correlation_key", correlation_key)

                if parked_context:
                    if elapsed_ms is not None:
                        span.set_attribute("parking_lot.elapsed_ms", elapsed_ms)
                    span.set_attribute("parking_lot.original_trace_id", parked_context.trace_id)
                    span.set_attribute("parking_lot.original_span_id", parked_context.span_id)

                    if parked_context.metadata:
                        for k, v in parked_context.metadata.items():
                            span.set_attribute(f"parking_lot.metadata.{k}", v)

                    event_attrs: dict[str, Any] = {
                        "parking_lot.correlation_key": correlation_key,
                        "parking_lot.original_trace_id": parked_context.trace_id,
                    }
                    if elapsed_ms is not None:
                        event_attrs["parking_lot.elapsed_ms"] = elapsed_ms
                    span.add_event("parked_context_retrieved", event_attrs)
                else:
                    span.set_attribute("parking_lot.context_found", False)

                    if require_parked_context:
                        error = RuntimeError(
                            f"Required parked context not found for key: {correlation_key}"
                        )
                        span.record_exception(error)
                        span.set_status(StatusCode.ERROR, str(error))
                        raise error

                if attributes:
                    for k, v in attributes.items():
                        if isinstance(v, str | bool | int | float):
                            span.set_attribute(k, v)

            def _make_ctx(
                span: Span,
                parked_context: StoredTraceContext | None,
                elapsed_ms: float | None,
                correlation_key: str,
            ) -> CallbackContext:
                return CallbackContext(
                    span=span,
                    parked_context=parked_context,
                    elapsed_ms=elapsed_ms,
                    correlation_key=correlation_key,
                )

            if inspect.iscoroutinefunction(func):

                @functools.wraps(func)
                async def async_wrapper(*args: P.args, **kwargs: P.kwargs) -> R:
                    tracer = otel_trace.get_tracer(__name__)
                    correlation_key = correlation_key_from(*args, **kwargs)
                    parked_context = await parking_lot.retrieve(correlation_key)
                    elapsed_ms = _elapsed(parked_context)

                    with (
                        run_in_operation_context(name),
                        tracer.start_as_current_span(
                            name, kind=SpanKind.SERVER, links=_links(parked_context)
                        ) as span,
                    ):
                        try:
                            _setup_span(span, correlation_key, parked_context, elapsed_ms)
                            if needs_ctx:
                                ctx = _make_ctx(span, parked_context, elapsed_ms, correlation_key)
                                return await func(ctx, *args, **kwargs)  # type: ignore[arg-type, no-any-return]
                            return await func(*args, **kwargs)  # type: ignore[no-any-return]
                        except Exception as e:
                            span.record_exception(e)
                            span.set_status(StatusCode.ERROR, str(e))
                            raise

                if needs_ctx:
                    _rewrite_signature_without_ctx(async_wrapper, func)
                return async_wrapper  # type: ignore[return-value]

            else:
                # Sync version - note: park/retrieve are still async
                @functools.wraps(func)
                def sync_wrapper(*args: P.args, **kwargs: P.kwargs) -> R:
                    import asyncio

                    correlation_key = correlation_key_from(*args, **kwargs)

                    try:
                        loop = asyncio.get_event_loop()
                    except RuntimeError:
                        loop = asyncio.new_event_loop()
                        asyncio.set_event_loop(loop)

                    parked_context = loop.run_until_complete(
                        parking_lot.retrieve(correlation_key)
                    )
                    elapsed_ms = _elapsed(parked_context)

                    tracer = otel_trace.get_tracer(__name__)
                    with (
                        run_in_operation_context(name),
                        tracer.start_as_current_span(
                            name, kind=SpanKind.SERVER, links=_links(parked_context)
                        ) as span,
                    ):
                        try:
                            _setup_span(span, correlation_key, parked_context, elapsed_ms)
                            if needs_ctx:
                                ctx = _make_ctx(span, parked_context, elapsed_ms, correlation_key)
                                return func(ctx, *args, **kwargs)  # type: ignore[arg-type]
                            return func(*args, **kwargs)
                        except Exception as e:
                            span.record_exception(e)
                            span.set_status(StatusCode.ERROR, str(e))
                            raise

                if needs_ctx:
                    _rewrite_signature_without_ctx(sync_wrapper, func)
                return sync_wrapper

        return decorator


def create_parking_lot(
    store: TraceContextStore,
    default_ttl_seconds: float = 86400,
    key_prefix: str = "parkingLot:",
    auto_delete_on_retrieve: bool = True,
    on_miss: Callable[[str], None] | None = None,
) -> ParkingLot:
    """Create a parking lot for trace context storage and retrieval.

    Args:
        store: Storage backend for parked contexts
        default_ttl_seconds: Default TTL in seconds (default: 24 hours)
        key_prefix: Prefix for all correlation keys
        auto_delete_on_retrieve: Whether to auto-delete after retrieval
        on_miss: Callback when context is not found

    Returns:
        A parking lot instance

    Example:
        >>> parking_lot = create_parking_lot(
        ...     store=InMemoryTraceContextStore(),
        ...     default_ttl_seconds=86400,  # 24 hours
        ... )
    """
    return ParkingLot(
        store=store,
        default_ttl_seconds=default_ttl_seconds,
        key_prefix=key_prefix,
        auto_delete_on_retrieve=auto_delete_on_retrieve,
        on_miss=on_miss,
    )


def create_correlation_key(*parts: str | int) -> str:
    """Create a correlation key from multiple parts.

    Args:
        parts: Key parts to join

    Returns:
        A correlation key string

    Example:
        >>> key = create_correlation_key("payment", order_id, "stripe")
        >>> # Returns: "payment:order-123:stripe"
    """
    return ":".join(str(p) for p in parts)


def to_span_context(stored_context: StoredTraceContext) -> dict[str, Any]:
    """Extract span context dict from stored context for manual linking.

    Args:
        stored_context: The stored trace context

    Returns:
        Dict with traceId, spanId, traceFlags suitable for creating SpanContext
    """
    return {
        "trace_id": stored_context.trace_id,
        "span_id": stored_context.span_id,
        "trace_flags": stored_context.trace_flags,
        "is_remote": True,
    }
