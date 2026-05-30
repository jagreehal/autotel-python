"""Structured errors that serialize cleanly and enrich spans."""

from __future__ import annotations

import traceback
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any, Protocol

from opentelemetry.trace import StatusCode

from .trace_helpers import flatten_metadata


class StructuredErrorContext(Protocol):
    """TraceContext-like subset used by record_structured_error."""

    def record_exception(self, exception: Exception) -> None:
        """Record an exception on the active span."""

    def set_status(self, code: StatusCode, description: str | None = None) -> None:
        """Set span status."""

    def set_attributes(self, attributes: Mapping[str, str | int | float | bool]) -> None:
        """Set span attributes."""


@dataclass
class ParsedError:
    """Normalized shape for arbitrary error-like values."""

    message: str
    status: int
    raw: Any
    why: str | None = None
    fix: str | None = None
    link: str | None = None
    code: str | int | None = None
    details: dict[str, Any] | None = None


class StructuredError(Exception):
    """Exception with public guidance fields and private internal context."""

    def __init__(
        self,
        message: str,
        *,
        why: str | None = None,
        fix: str | None = None,
        link: str | None = None,
        code: str | int | None = None,
        status: int | None = None,
        details: dict[str, Any] | None = None,
        internal: dict[str, Any] | None = None,
        name: str = "StructuredError",
        cause: BaseException | None = None,
    ) -> None:
        super().__init__(message)
        self.name = name
        self.why = why
        self.fix = fix
        self.link = link
        self.code = code
        self.status = status
        self.details = details
        self.internal = internal
        if cause is not None:
            self.__cause__ = cause

    def __str__(self) -> str:
        lines = [f"{self.name}: {self.args[0]}"]
        if self.why:
            lines.append(f"  Why: {self.why}")
        if self.fix:
            lines.append(f"  Fix: {self.fix}")
        if self.link:
            lines.append(f"  Link: {self.link}")
        if self.code is not None:
            lines.append(f"  Code: {self.code}")
        if self.status is not None:
            lines.append(f"  Status: {self.status}")
        if self.__cause__:
            lines.append(f"  Caused by: {type(self.__cause__).__name__}: {self.__cause__}")
        return "\n".join(lines)


def create_structured_error(
    message: str,
    *,
    why: str | None = None,
    fix: str | None = None,
    link: str | None = None,
    code: str | int | None = None,
    status: int | None = None,
    details: dict[str, Any] | None = None,
    internal: dict[str, Any] | None = None,
    name: str = "StructuredError",
    cause: BaseException | None = None,
) -> StructuredError:
    """Create a StructuredError."""
    return StructuredError(
        message,
        why=why,
        fix=fix,
        link=link,
        code=code,
        status=status,
        details=details,
        internal=internal,
        name=name,
        cause=cause,
    )


def structured_error_to_json(error: StructuredError) -> dict[str, Any]:
    """Serialize a StructuredError without private internal context."""
    result: dict[str, Any] = {
        "name": error.name,
        "message": error.args[0],
    }
    if error.status is not None:
        result["status"] = error.status
    if error.why or error.fix or error.link:
        result["data"] = {
            key: value
            for key, value in {
                "why": error.why,
                "fix": error.fix,
                "link": error.link,
            }.items()
            if value
        }
    if error.code is not None:
        result["code"] = error.code
    if error.details:
        result["details"] = error.details
    if error.__cause__:
        result["cause"] = {
            "name": type(error.__cause__).__name__,
            "message": str(error.__cause__),
        }
    return result


def get_structured_error_attributes(error: Exception) -> dict[str, str | int | float | bool]:
    """Build OpenTelemetry attributes for a plain or structured exception."""
    name = getattr(error, "name", type(error).__name__)
    attrs: dict[str, str | int | float | bool] = {
        "error.type": str(name),
        "error.message": str(error.args[0] if error.args else error),
    }

    if error.__traceback__:
        attrs["error.stack"] = "".join(
            traceback.format_exception(type(error), error, error.__traceback__)
        )

    if isinstance(error, StructuredError):
        if error.why:
            attrs["error.why"] = error.why
        if error.fix:
            attrs["error.fix"] = error.fix
        if error.link:
            attrs["error.link"] = error.link
        if error.code is not None:
            attrs["error.code"] = str(error.code)
        if error.status is not None:
            attrs["error.status"] = error.status
        if error.details:
            attrs.update(flatten_metadata(error.details, prefix="error.details"))

    return attrs


def record_structured_error(ctx: StructuredErrorContext, error: Exception) -> None:
    """Record a structured error on a TraceContext-like object."""
    ctx.record_exception(error)
    ctx.set_status(StatusCode.ERROR, str(error.args[0] if error.args else error))
    ctx.set_attributes(get_structured_error_attributes(error))


def parse_error(error: Any) -> ParsedError:
    """Normalize arbitrary error-like values into a ParsedError."""
    if isinstance(error, StructuredError):
        return ParsedError(
            message=str(error.args[0] if error.args else error),
            status=_to_status(error.status) or 500,
            why=error.why,
            fix=error.fix,
            link=error.link,
            code=error.code,
            details=error.details,
            raw=error,
        )

    if isinstance(error, Exception):
        return ParsedError(
            message=str(error),
            status=_to_status(getattr(error, "status", None))
            or _to_status(getattr(error, "status_code", None))
            or 500,
            raw=error,
        )

    if isinstance(error, Mapping):
        data = error.get("data") if isinstance(error.get("data"), Mapping) else None
        nested = data.get("data") if isinstance(data, Mapping) and isinstance(data.get("data"), Mapping) else None
        payload = nested or data or error

        message = _pick_string(
            payload.get("statusText"),
            payload.get("statusMessage"),
            payload.get("message"),
            error.get("message"),
        ) or "An error occurred"

        return ParsedError(
            message=message,
            status=_to_status(payload.get("status"))
            or _to_status(payload.get("statusCode"))
            or _to_status(error.get("status"))
            or _to_status(error.get("statusCode"))
            or 500,
            why=_pick_string(payload.get("why"), error.get("why")),
            fix=_pick_string(payload.get("fix"), error.get("fix")),
            link=_pick_string(payload.get("link"), error.get("link")),
            code=_pick_code(payload.get("code")) or _pick_code(error.get("code")),
            details=_pick_details(payload.get("details")) or _pick_details(error.get("details")),
            raw=error,
        )

    return ParsedError(message=str(error), status=500, raw=error)


def _to_status(value: Any) -> int | None:
    if isinstance(value, int | float):
        return int(value)
    if isinstance(value, str):
        try:
            return int(value)
        except ValueError:
            return None
    return None


def _pick_string(*values: Any) -> str | None:
    for value in values:
        if isinstance(value, str) and value:
            return value
    return None


def _pick_code(value: Any) -> str | int | None:
    return value if isinstance(value, str | int) else None


def _pick_details(value: Any) -> dict[str, Any] | None:
    return dict(value) if isinstance(value, Mapping) else None
