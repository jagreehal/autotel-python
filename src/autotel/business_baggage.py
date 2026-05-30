"""Business Baggage: Safe context propagation for event-driven architectures.

This module provides a safe, validated approach to OpenTelemetry baggage propagation
that prevents common issues:
- Baggage explosion (unlimited keys)
- PII leakage (sensitive data in headers)
- Value overflow (unbounded string lengths)

Implements feature parity with Go's baggage.Business and Node's defineBusinessBaggage.

Example:
    >>> from autotel.business_baggage import create_safe_baggage_schema
    >>>
    >>> OrderBaggage = create_safe_baggage_schema({
    ...     "order_id": {"type": "string"},
    ...     "customer_id": {"type": "string", "hash": True},  # Auto-hash for privacy
    ...     "priority": {"type": "enum", "values": ["low", "normal", "high"]},
    ... })
    >>>
    >>> # Usage in traced function
    >>> OrderBaggage.set(ctx, {"order_id": "ord-123", "customer_id": "cust-456", "priority": "high"})
    >>> order_id, priority = OrderBaggage.get(ctx, "order_id"), OrderBaggage.get(ctx, "priority")
"""

from __future__ import annotations

import hashlib
import re
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Literal, TypedDict

from opentelemetry import context
from opentelemetry.baggage import propagation

if TYPE_CHECKING:
    from .context import TraceContext


@dataclass
class BusinessBaggageConfig:
    """Configuration for business baggage propagation.

    Attributes:
        allowed_keys: Set of keys allowed in baggage. Empty means no restrictions.
        hash_keys: Set of keys that should be hashed (for PII protection).
        max_value_length: Maximum length for any baggage value (default 256).
        hash_algorithm: Hash algorithm to use for PII keys (default "sha256").
        hash_prefix: Prefix for hashed values (default "hash:").
    """

    allowed_keys: set[str] = field(default_factory=set)
    hash_keys: set[str] = field(default_factory=set)
    max_value_length: int = 256
    hash_algorithm: str = "sha256"
    hash_prefix: str = "hash:"


class BusinessBaggage:
    """Safe baggage propagation with allowlists and PII protection.

    BusinessBaggage provides a controlled interface for OpenTelemetry baggage
    that prevents common production issues.

    Example:
        >>> bc = BusinessBaggage(
        ...     allowed_keys=["tenant_id", "correlation_id", "user_id"],
        ...     hash_keys=["user_id", "email"],  # PII gets hashed
        ...     max_value_length=128,
        ... )
        >>>
        >>> # In producer
        >>> ctx = bc.set(context.get_current(), "tenant_id", "acme-corp")
        >>> ctx = bc.set(ctx, "user_id", "user@email.com")  # Auto-hashed
        >>>
        >>> # In consumer
        >>> tenant = bc.get(ctx, "tenant_id")  # Returns "acme-corp"
        >>> user_hash = bc.get(ctx, "user_id")  # Returns "hash:abc123..."
    """

    def __init__(
        self,
        allowed_keys: set[str] | list[str] | None = None,
        hash_keys: set[str] | list[str] | None = None,
        max_value_length: int = 256,
        hash_algorithm: str = "sha256",
        hash_prefix: str = "hash:",
    ) -> None:
        """Initialize BusinessBaggage with safety configuration.

        Args:
            allowed_keys: Keys allowed in baggage. If empty/None, all keys allowed.
            hash_keys: Keys that should be hashed for PII protection.
            max_value_length: Maximum length for values (truncated if exceeded).
            hash_algorithm: Hash algorithm (sha256, sha1, md5).
            hash_prefix: Prefix for hashed values to identify them.
        """
        self.config = BusinessBaggageConfig(
            allowed_keys=set(allowed_keys) if allowed_keys else set(),
            hash_keys=set(hash_keys) if hash_keys else set(),
            max_value_length=max_value_length,
            hash_algorithm=hash_algorithm,
            hash_prefix=hash_prefix,
        )
        self._validate_config()

    def _validate_config(self) -> None:
        """Validate configuration parameters."""
        # Hash output is: prefix (6 chars for "hash:") + 16 hex chars = 22 chars
        min_hash_length = len(self.config.hash_prefix) + 16

        if self.config.max_value_length < 1:
            raise ValueError("max_value_length must be at least 1")

        # Ensure max_value_length can accommodate hashed values
        if self.config.hash_keys and self.config.max_value_length < min_hash_length:
            raise ValueError(
                f"max_value_length ({self.config.max_value_length}) is too small for "
                f"hashed values. Minimum required: {min_hash_length} "
                f"(prefix '{self.config.hash_prefix}' + 16 char hash)"
            )

        if self.config.hash_algorithm not in ("sha256", "sha1", "md5"):
            raise ValueError(f"Unsupported hash algorithm: {self.config.hash_algorithm}")

        # Validate hash_keys are subset of allowed_keys (if allowlist is set)
        if self.config.allowed_keys and self.config.hash_keys:
            invalid_hash_keys = self.config.hash_keys - self.config.allowed_keys
            if invalid_hash_keys:
                raise ValueError(
                    f"hash_keys must be subset of allowed_keys. "
                    f"Invalid keys: {invalid_hash_keys}"
                )

    def _hash_value(self, value: str) -> str:
        """Hash a value for PII protection."""
        h = hashlib.new(self.config.hash_algorithm)
        h.update(value.encode("utf-8"))
        return f"{self.config.hash_prefix}{h.hexdigest()[:16]}"

    def _sanitize_value(self, key: str, value: str) -> str:
        """Sanitize a value: hash if PII, truncate if too long."""
        # Hash PII keys
        if key in self.config.hash_keys:
            value = self._hash_value(value)

        # Truncate long values
        if len(value) > self.config.max_value_length:
            value = value[: self.config.max_value_length - 3] + "..."

        return value

    def _is_key_allowed(self, key: str) -> bool:
        """Check if a key is in the allowlist."""
        if not self.config.allowed_keys:
            return True
        return key in self.config.allowed_keys

    def set(
        self,
        ctx: context.Context | None,
        key: str,
        value: str,
    ) -> context.Context:
        """Set a baggage entry with validation and sanitization.

        Args:
            ctx: Context to set baggage in (None uses current context).
            key: Baggage key (must be in allowed_keys if set).
            value: Baggage value (will be sanitized).

        Returns:
            New context with baggage set.

        Raises:
            ValueError: If key is not in allowed_keys.

        Example:
            >>> bc = BusinessBaggage(allowed_keys=["tenant_id"])
            >>> ctx = bc.set(None, "tenant_id", "acme")
            >>> ctx = bc.set(ctx, "unknown", "value")  # Raises ValueError
        """
        if not self._is_key_allowed(key):
            raise ValueError(
                f"Key '{key}' is not in allowed_keys. "
                f"Allowed: {self.config.allowed_keys}"
            )

        sanitized = self._sanitize_value(key, value)
        base_ctx = ctx if ctx is not None else context.get_current()
        return propagation.set_baggage(key, sanitized, base_ctx)

    def set_many(
        self,
        ctx: context.Context | None,
        entries: Mapping[str, str],
    ) -> context.Context:
        """Set multiple baggage entries at once.

        Args:
            ctx: Context to set baggage in (None uses current context).
            entries: Dictionary of key-value pairs.

        Returns:
            New context with all baggage entries set.

        Raises:
            ValueError: If any key is not in allowed_keys.
        """
        result_ctx = ctx if ctx is not None else context.get_current()
        for key, value in entries.items():
            result_ctx = self.set(result_ctx, key, value)
        return result_ctx

    def get(self, ctx: context.Context | None, key: str) -> str | None:
        """Get a baggage entry.

        Args:
            ctx: Context to get baggage from (None uses current context).
            key: Baggage key.

        Returns:
            Baggage value or None if not set.
        """
        base_ctx = ctx if ctx is not None else context.get_current()
        baggage = propagation.get_all(base_ctx)
        if not baggage:
            return None
        value = baggage.get(key)
        return str(value) if value is not None else None

    def get_all(self, ctx: context.Context | None = None) -> dict[str, str]:
        """Get all baggage entries.

        Args:
            ctx: Context to get baggage from (None uses current context).

        Returns:
            Dictionary of all baggage entries.
        """
        base_ctx = ctx if ctx is not None else context.get_current()
        baggage = propagation.get_all(base_ctx)
        if not baggage:
            return {}
        return {k: str(v) for k, v in baggage.items()}

    def get_allowed_only(self, ctx: context.Context | None = None) -> dict[str, str]:
        """Get only baggage entries that are in the allowed list.

        Useful for filtering out any baggage that may have been set
        by other systems without the allowlist.

        Args:
            ctx: Context to get baggage from (None uses current context).

        Returns:
            Dictionary of allowed baggage entries only.
        """
        all_baggage = self.get_all(ctx)
        if not self.config.allowed_keys:
            return all_baggage
        return {k: v for k, v in all_baggage.items() if k in self.config.allowed_keys}

    def delete(self, ctx: context.Context | None, key: str) -> context.Context:
        """Delete a baggage entry.

        Args:
            ctx: Context to modify (None uses current context).
            key: Baggage key to delete.

        Returns:
            New context without the specified baggage entry.
        """
        base_ctx = ctx if ctx is not None else context.get_current()
        all_baggage = self.get_all(base_ctx)

        # Rebuild context without the key
        new_ctx = context.Context()
        for k, v in all_baggage.items():
            if k != key:
                new_ctx = propagation.set_baggage(k, v, new_ctx)

        return new_ctx

    def is_hashed(self, value: str) -> bool:
        """Check if a value is a hashed PII value.

        Args:
            value: Value to check.

        Returns:
            True if value appears to be hashed.
        """
        return value.startswith(self.config.hash_prefix)

    def attach(self, ctx: context.Context) -> object:
        """Attach context to make it current.

        Args:
            ctx: Context to attach.

        Returns:
            Token for detaching (use with context.detach).

        Example:
            >>> ctx = bc.set(None, "tenant_id", "acme")
            >>> token = bc.attach(ctx)
            >>> # ... do work with baggage in scope ...
            >>> context.detach(token)
        """
        return context.attach(ctx)


def define_business_baggage(
    schema: dict[str, dict[str, Any]],
    max_value_length: int = 256,
) -> BusinessBaggage:
    """Define a business baggage schema with validation rules.

    This is a schema-based approach similar to Node's defineBusinessBaggage.

    Args:
        schema: Dictionary where keys are baggage keys and values are config dicts:
            - type: "string" (only string supported for now)
            - pii: bool - if True, value will be hashed
            - propagate: bool - if True, key is allowed (default True)
        max_value_length: Maximum value length for all keys.

    Returns:
        Configured BusinessBaggage instance.

    Example:
        >>> bc = define_business_baggage({
        ...     "tenant_id": {"type": "string", "propagate": True},
        ...     "user_id": {"type": "string", "pii": True},
        ...     "correlation_id": {"type": "string"},
        ... })
        >>>
        >>> ctx = bc.set(None, "tenant_id", "acme-corp")
        >>> ctx = bc.set(ctx, "user_id", "user@example.com")  # Gets hashed
    """
    allowed_keys: set[str] = set()
    hash_keys: set[str] = set()

    for key, config in schema.items():
        # Default propagate to True
        if config.get("propagate", True):
            allowed_keys.add(key)

        if config.get("pii", False):
            hash_keys.add(key)

    return BusinessBaggage(
        allowed_keys=allowed_keys,
        hash_keys=hash_keys,
        max_value_length=max_value_length,
    )


# Convenience singleton for simple use cases
_default_baggage: BusinessBaggage | None = None


def configure_business_baggage(
    allowed_keys: set[str] | list[str] | None = None,
    hash_keys: set[str] | list[str] | None = None,
    max_value_length: int = 256,
) -> BusinessBaggage:
    """Configure the default business baggage instance.

    For simple use cases where you want a global configuration.

    Args:
        allowed_keys: Keys allowed in baggage.
        hash_keys: Keys that should be hashed.
        max_value_length: Maximum value length.

    Returns:
        Configured BusinessBaggage instance (also set as default).

    Example:
        >>> configure_business_baggage(
        ...     allowed_keys=["tenant_id", "user_id"],
        ...     hash_keys=["user_id"],
        ... )
        >>>
        >>> # Later, anywhere in your app:
        >>> ctx = get_business_baggage().set(None, "tenant_id", "acme")
    """
    global _default_baggage
    _default_baggage = BusinessBaggage(
        allowed_keys=allowed_keys,
        hash_keys=hash_keys,
        max_value_length=max_value_length,
    )
    return _default_baggage


def get_business_baggage() -> BusinessBaggage:
    """Get the default business baggage instance.

    Returns:
        Default BusinessBaggage (creates unrestricted one if not configured).
    """
    global _default_baggage
    if _default_baggage is None:
        _default_baggage = BusinessBaggage()
    return _default_baggage


# ============================================================================
# Safe Baggage Schema
# ============================================================================

# Type definitions for schema fields
BaggageFieldType = Literal["string", "number", "boolean", "enum"]


class BaggageFieldDefinition(TypedDict, total=False):
    """Field definition in a baggage schema."""

    type: BaggageFieldType
    """Field type: string, number, boolean, or enum."""

    max_length: int
    """Maximum length for string values (default: 256)."""

    hash: bool
    """Hash value before storing (for privacy)."""

    values: list[str]
    """Allowed values for enum type."""

    default_value: str | int | bool
    """Default value if not provided."""

    required: bool
    """Whether field is required."""


class BaggageError(TypedDict):
    """Baggage error details."""

    type: Literal["validation", "size", "pii", "key_length", "value_length"]
    key: str
    message: str
    value: Any


@dataclass
class SafeBaggageOptions:
    """Options for creating a safe baggage schema."""

    max_key_length: int = 64
    """Maximum key length (default: 64)."""

    max_value_length: int = 256
    """Maximum value length (default: 256)."""

    max_total_size: int = 8192
    """Maximum total baggage size in bytes (default: 8192)."""

    prefix: str = ""
    """Prefix for all keys (default: none)."""

    hash_high_cardinality: bool = False
    """Hash high-cardinality values automatically."""

    redact_pii: bool = False
    """Detect and redact PII patterns."""

    allowed_keys: list[str] | None = None
    """Allowed keys whitelist (others rejected)."""

    on_error: Callable[[BaggageError], None] | None = None
    """Custom error handler."""


# PII patterns to detect and redact
_PII_PATTERNS = [
    re.compile(r"\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b"),  # Email
    re.compile(r"\b\d{3}[-.]?\d{3}[-.]?\d{4}\b"),  # Phone (US)
    re.compile(r"\b\d{3}[-]?\d{2}[-]?\d{4}\b"),  # SSN
    re.compile(r"\b\d{16}\b"),  # Credit card (basic)
]

# High-cardinality value patterns
_HIGH_CARDINALITY_PATTERNS = [
    re.compile(r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$", re.I),  # UUID
    re.compile(r"^\d{13,}$"),  # Timestamps
    re.compile(r"^[A-Za-z0-9+/]{20,}={0,2}$"),  # Base64
]


class SafeBaggageSchema:
    """Safe baggage schema with validation and guardrails.

    Provides type-safe baggage operations with built-in protection against
    common pitfalls: high-cardinality values, PII leakage, and oversized payloads.

    Example:
        >>> schema = create_safe_baggage_schema({
        ...     "tenant_id": {"type": "string"},
        ...     "user_id": {"type": "string", "hash": True},
        ...     "priority": {"type": "enum", "values": ["low", "normal", "high"]},
        ... })
        >>> schema.set(ctx, {"tenant_id": "acme", "user_id": "user-123", "priority": "high"})
        >>> values = schema.get_all(ctx)
    """

    def __init__(
        self,
        schema: dict[str, BaggageFieldDefinition],
        options: SafeBaggageOptions | None = None,
    ) -> None:
        """Initialize the safe baggage schema.

        Args:
            schema: Field definitions mapping field names to definitions.
            options: Safety options for the schema.
        """
        self._schema = schema
        self._options = options or SafeBaggageOptions()

        # Validate schema keys against allowlist
        if self._options.allowed_keys:
            for key in schema:
                if key not in self._options.allowed_keys:
                    raise ValueError(f"Key '{key}' not in allowed_keys whitelist")

    def _prefix_key(self, key: str) -> str:
        """Apply prefix to a key."""
        return f"{self._options.prefix}.{key}" if self._options.prefix else key

    def _unprefix_key(self, full_key: str) -> str | None:
        """Remove prefix from a key, returning None if prefix doesn't match."""
        if self._options.prefix:
            prefix = f"{self._options.prefix}."
            if full_key.startswith(prefix):
                return full_key[len(prefix) :]
            return None
        return full_key

    def _hash_value(self, value: str) -> str:
        """Hash a value using FNV-1a for consistency with Node.js implementation."""
        # FNV-1a hash algorithm
        hash_val = 2166136261
        for char in value:
            hash_val ^= ord(char)
            hash_val = (hash_val * 16777619) & 0xFFFFFFFF
        return f"h_{hash_val:x}"

    def _contains_pii(self, value: str) -> bool:
        """Check if value contains PII patterns."""
        return any(pattern.search(value) for pattern in _PII_PATTERNS)

    def _is_high_cardinality(self, value: str) -> bool:
        """Check if value matches high-cardinality patterns."""
        return any(pattern.match(value) for pattern in _HIGH_CARDINALITY_PATTERNS)

    def _validate_and_transform(
        self,
        key: str,
        value: Any,
        field_def: BaggageFieldDefinition,
    ) -> str | None:
        """Validate and transform a single value."""
        full_key = self._prefix_key(key)
        on_error = self._options.on_error

        # Check key length
        if len(full_key) > self._options.max_key_length:
            if on_error:
                on_error({
                    "type": "key_length",
                    "key": key,
                    "message": f"Key '{key}' exceeds max length {self._options.max_key_length}",
                    "value": None,
                })
            return None

        # Handle undefined/null with default
        if value is None:
            if field_def.get("required"):
                if on_error:
                    on_error({
                        "type": "validation",
                        "key": key,
                        "message": f"Required field '{key}' is missing",
                        "value": None,
                    })
                return None
            default_val = field_def.get("default_value")
            if default_val is None:
                return None
            value = default_val

        # Type validation
        field_type = field_def.get("type", "string")
        string_value: str

        if field_type == "string":
            if not isinstance(value, str):
                if on_error:
                    on_error({
                        "type": "validation",
                        "key": key,
                        "message": f"Field '{key}' expected string, got {type(value).__name__}",
                        "value": value,
                    })
                return None
            string_value = value

        elif field_type == "number":
            if not isinstance(value, int | float):
                if on_error:
                    on_error({
                        "type": "validation",
                        "key": key,
                        "message": f"Field '{key}' expected number, got {type(value).__name__}",
                        "value": value,
                    })
                return None
            string_value = str(value)

        elif field_type == "boolean":
            if not isinstance(value, bool):
                if on_error:
                    on_error({
                        "type": "validation",
                        "key": key,
                        "message": f"Field '{key}' expected boolean, got {type(value).__name__}",
                        "value": value,
                    })
                return None
            string_value = str(value).lower()

        elif field_type == "enum":
            allowed_values = field_def.get("values", [])
            if str(value) not in allowed_values:
                if on_error:
                    on_error({
                        "type": "validation",
                        "key": key,
                        "message": f"Field '{key}' value '{value}' not in allowed values: {', '.join(allowed_values)}",
                        "value": value,
                    })
                return None
            string_value = str(value)

        else:
            string_value = str(value)

        # PII check
        if self._options.redact_pii and self._contains_pii(string_value):
            if on_error:
                on_error({
                    "type": "pii",
                    "key": key,
                    "message": f"Field '{key}' contains PII pattern",
                    "value": "[REDACTED]",
                })
            string_value = self._hash_value(string_value)

        # Hash if requested or high-cardinality
        if field_def.get("hash") or (
            self._options.hash_high_cardinality and self._is_high_cardinality(string_value)
        ):
            string_value = self._hash_value(string_value)

        # Length validation
        max_len = field_def.get("max_length", self._options.max_value_length)
        if len(string_value) > max_len:
            if on_error:
                on_error({
                    "type": "value_length",
                    "key": key,
                    "message": f"Field '{key}' value exceeds max length {max_len}",
                    "value": string_value,
                })
            string_value = string_value[:max_len]

        return string_value

    def _parse_value(self, key: str, string_value: str) -> Any:
        """Parse value back from baggage string."""
        field_def = self._schema.get(key)
        if not field_def:
            return string_value

        field_type = field_def.get("type", "string")

        if field_type == "number":
            try:
                if "." in string_value:
                    return float(string_value)
                return int(string_value)
            except ValueError:
                return string_value
        elif field_type == "boolean":
            return string_value.lower() == "true"
        else:
            return string_value

    def get(self, _ctx: TraceContext | None, key: str) -> Any:
        """Get a single baggage value.

        Args:
            _ctx: TraceContext (currently unused, uses active context).
            key: The baggage key to retrieve.

        Returns:
            The value, or None if not found.
        """
        all_baggage = propagation.get_all(context.get_current())
        if not all_baggage:
            return None

        full_key = self._prefix_key(key)
        value = all_baggage.get(full_key)

        if value is None:
            field_def = self._schema.get(key)
            if field_def:
                return field_def.get("default_value")
            return None

        return self._parse_value(key, str(value))

    def get_all(self, _ctx: TraceContext | None = None) -> dict[str, Any]:
        """Get all baggage values for this schema.

        Args:
            _ctx: TraceContext (currently unused, uses active context).

        Returns:
            Dictionary of all schema values.
        """
        result: dict[str, Any] = {}
        all_baggage = propagation.get_all(context.get_current())

        for key, field_def in self._schema.items():
            full_key = self._prefix_key(key)
            if all_baggage and full_key in all_baggage:
                result[key] = self._parse_value(key, str(all_baggage[full_key]))
            elif "default_value" in field_def:
                result[key] = field_def["default_value"]

        return result

    def set(self, _ctx: TraceContext | None, values: dict[str, Any]) -> context.Context:
        """Set baggage values.

        Args:
            _ctx: TraceContext (currently unused, uses active context).
            values: Dictionary of values to set.

        Returns:
            New context with baggage set.
        """
        base_ctx = context.get_current()
        total_size = 0

        # Calculate existing size
        existing = propagation.get_all(base_ctx)
        if existing:
            for k, v in existing.items():
                total_size += len(k) + len(str(v))

        result_ctx = base_ctx
        for key, value in values.items():
            field_def = self._schema.get(key)
            if not field_def:
                continue

            full_key = self._prefix_key(key)
            string_value = self._validate_and_transform(key, value, field_def)

            if string_value is not None:
                # Check total size
                entry_size = len(full_key) + len(string_value)
                if total_size + entry_size > self._options.max_total_size:
                    if self._options.on_error:
                        self._options.on_error({
                            "type": "size",
                            "key": key,
                            "message": f"Adding '{key}' would exceed max baggage size {self._options.max_total_size}",
                            "value": value,
                        })
                    continue

                result_ctx = propagation.set_baggage(full_key, string_value, result_ctx)
                total_size += entry_size

        return result_ctx

    def set_value(self, _ctx: TraceContext | None, key: str, value: Any) -> context.Context:
        """Set a single baggage value.

        Args:
            _ctx: TraceContext (currently unused, uses active context).
            key: The baggage key.
            value: The value to set.

        Returns:
            New context with baggage set.
        """
        return self.set(_ctx, {key: value})

    def clear(self, _ctx: TraceContext | None = None) -> context.Context:
        """Clear all schema baggage values.

        Args:
            _ctx: TraceContext (currently unused, uses active context).

        Returns:
            New context with schema baggage cleared.
        """
        base_ctx = context.get_current()
        existing = propagation.get_all(base_ctx)
        if not existing:
            return base_ctx

        # Rebuild context without schema keys
        result_ctx = context.Context()
        for k, v in existing.items():
            unprefixed = self._unprefix_key(k)
            if unprefixed is None or unprefixed not in self._schema:
                result_ctx = propagation.set_baggage(k, str(v), result_ctx)

        return result_ctx


def create_safe_baggage_schema(
    schema: dict[str, BaggageFieldDefinition],
    prefix: str = "",
    max_key_length: int = 64,
    max_value_length: int = 256,
    max_total_size: int = 8192,
    hash_high_cardinality: bool = False,
    redact_pii: bool = False,
    allowed_keys: list[str] | None = None,
    on_error: Callable[[BaggageError], None] | None = None,
) -> SafeBaggageSchema:
    """Create a safe baggage schema with validation and guardrails.

    Args:
        schema: Field definitions mapping field names to definitions.
        prefix: Prefix for all keys (default: none).
        max_key_length: Maximum key length (default: 64).
        max_value_length: Maximum value length (default: 256).
        max_total_size: Maximum total baggage size in bytes (default: 8192).
        hash_high_cardinality: Hash high-cardinality values automatically.
        redact_pii: Detect and redact PII patterns.
        allowed_keys: Allowed keys whitelist (others rejected).
        on_error: Custom error handler.

    Returns:
        Type-safe baggage schema.

    Example:
        >>> OrderBaggage = create_safe_baggage_schema({
        ...     "order_id": {"type": "string"},
        ...     "customer_id": {"type": "string", "hash": True},
        ...     "priority": {"type": "enum", "values": ["low", "normal", "high"]},
        ... }, prefix="order")
        >>>
        >>> ctx = OrderBaggage.set(None, {"order_id": "123", "priority": "high"})
        >>> order_id = OrderBaggage.get(None, "order_id")
    """
    options = SafeBaggageOptions(
        max_key_length=max_key_length,
        max_value_length=max_value_length,
        max_total_size=max_total_size,
        prefix=prefix,
        hash_high_cardinality=hash_high_cardinality,
        redact_pii=redact_pii,
        allowed_keys=allowed_keys,
        on_error=on_error,
    )
    return SafeBaggageSchema(schema, options)
