"""Wide-event request logger bound to the active span."""

from __future__ import annotations

import warnings
from collections.abc import Mapping, MutableMapping
from datetime import datetime, timezone
from typing import Any

from opentelemetry import trace as otel_trace

from .context import TraceContext

_POST_EMIT_HINT = (
    "For intentional background work tied to this request, gather fields before emit_now()."
)


def _merge_into(target: MutableMapping[str, Any], source: Mapping[str, Any]) -> None:
    for key, source_val in source.items():
        if source_val is None:
            continue
        target_val = target.get(key)
        if isinstance(source_val, Mapping) and isinstance(target_val, MutableMapping):
            _merge_into(target_val, source_val)
        elif isinstance(target_val, list) and isinstance(source_val, list):
            target[key] = [*target_val, *source_val]
        else:
            target[key] = source_val


def flatten_to_attributes(fields: Mapping[str, Any], *, prefix: str = "") -> dict[str, Any]:
    """Flatten nested mappings to dotted OpenTelemetry attribute keys."""
    flattened: dict[str, Any] = {}

    def _flatten(obj: Any, current: str) -> None:
        if obj is None:
            return
        if isinstance(obj, Mapping):
            for key, value in obj.items():
                next_key = f"{current}.{key}" if current else str(key)
                _flatten(value, next_key)
            return
        if isinstance(obj, bool | int | float | str):
            flattened[current] = obj
            return
        flattened[current] = str(obj)

    _flatten(fields, prefix)
    return flattened


class RequestLogger:
    """Accumulate fields onto one span as a single wide event per request."""

    def __init__(self, ctx: TraceContext) -> None:
        self._ctx = ctx
        self._state: dict[str, Any] = {}
        self._emitted = False

    def set(self, fields: Mapping[str, Any]) -> None:
        """Merge nested fields and set them as span attributes immediately."""
        if self._emitted:
            warnings.warn(
                f"[autotel] log.set() called after emit_now(). Keys dropped: "
                f"{', '.join(fields)}. {_POST_EMIT_HINT}",
                stacklevel=2,
            )
            return
        _merge_into(self._state, fields)
        self._ctx.set_attributes(flatten_to_attributes(fields))

    def get_context(self) -> dict[str, Any]:
        return dict(self._state)

    def emit_now(self, overrides: Mapping[str, Any] | None = None) -> dict[str, Any]:
        """Seal the wide event: merge overrides and stamp final attributes."""
        if self._emitted:
            warnings.warn(
                f"[autotel] log.emit_now() called twice. Ignoring duplicate emit. {_POST_EMIT_HINT}",
                stacklevel=2,
            )
        else:
            if overrides:
                _merge_into(self._state, overrides)
            self._ctx.set_attributes(flatten_to_attributes(self._state))
            self._emitted = True
        return {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "traceId": self._ctx.trace_id,
            "spanId": self._ctx.span_id,
            "context": dict(self._state),
        }


def get_request_logger(ctx: TraceContext | None = None) -> RequestLogger:
    """
    Return a request logger bound to ``ctx`` or the active span.

    Raises:
        RuntimeError: if there is no active span and no ``ctx`` was passed.
    """
    if ctx is not None:
        return RequestLogger(ctx)

    span = otel_trace.get_current_span()
    if not span.get_span_context().is_valid:
        raise RuntimeError(
            "[autotel] get_request_logger() requires an active span. "
            "Wrap your handler with @trace or pass a TraceContext."
        )
    return RequestLogger(TraceContext(span))
