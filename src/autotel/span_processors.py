"""Span processors that make migration safer by default."""

from __future__ import annotations

import logging
import re
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from typing import Any, Literal, Protocol, cast

from opentelemetry import context as otel_context
from opentelemetry.sdk.trace import ReadableSpan, Span, SpanProcessor

logger = logging.getLogger(__name__)

AttributeValue = str | int | float | bool | list[str] | list[int] | list[float] | list[bool]
SpanFilter = Callable[[ReadableSpan], bool]
SpanNameNormalizer = Callable[[str], str]
SpanNameNormalizerPreset = Literal["rest-api", "graphql", "minimal"]
AttributeRedactorPreset = Literal["default", "strict", "pci-dss"]


class AttributeRedactor(Protocol):
    """Callable that redacts a single span attribute."""

    def __call__(self, key: str, value: Any) -> Any:
        """Return the redacted attribute value."""


@dataclass(frozen=True)
class ValuePattern:
    """Pattern for redacting sensitive content inside string values."""

    name: str
    pattern: re.Pattern[str]
    replacement: str | None = None
    mask: Callable[[re.Match[str]], str] | None = None


@dataclass(frozen=True)
class AttributeRedactorConfig:
    """Configuration for attribute redaction."""

    key_patterns: tuple[re.Pattern[str], ...] = ()
    value_patterns: tuple[ValuePattern, ...] = ()
    paths: tuple[str, ...] = ()
    patterns: tuple[re.Pattern[str], ...] = ()
    replacement: str = "[REDACTED]"
    redactor: AttributeRedactor | None = None


NORMALIZER_PATTERNS: dict[str, re.Pattern[str]] = {
    "numeric_id": re.compile(r"/\d+(?=/|$)"),
    "uuid": re.compile(
        r"/[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}(?=/|$)",
        re.IGNORECASE,
    ),
    "short_uuid": re.compile(r"/[0-9a-f]{32}(?=/|$)", re.IGNORECASE),
    "object_id": re.compile(r"/[0-9a-f]{24}(?=/|$)", re.IGNORECASE),
    "iso_date": re.compile(r"/\d{4}-\d{2}-\d{2}(?=/|$)"),
    "timestamp": re.compile(r"/1[0-9]{9}(?=/|$)"),
    "email": re.compile(r"/[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}(?=/|$)"),
}


def normalize_rest_api_span_name(name: str) -> str:
    """Normalize high-cardinality REST path segments."""
    replacements = [
        ("uuid", "/:uuid"),
        ("short_uuid", "/:uuid"),
        ("object_id", "/:objectId"),
        ("iso_date", "/:date"),
        ("timestamp", "/:timestamp"),
        ("email", "/:email"),
        ("numeric_id", "/:id"),
    ]
    normalized = name
    for pattern_name, replacement in replacements:
        normalized = NORMALIZER_PATTERNS[pattern_name].sub(replacement, normalized)
    return normalized


NORMALIZER_PRESETS: dict[SpanNameNormalizerPreset, SpanNameNormalizer] = {
    "rest-api": normalize_rest_api_span_name,
    "graphql": lambda name: NORMALIZER_PATTERNS["numeric_id"].sub(
        "/:id", NORMALIZER_PATTERNS["uuid"].sub("/:uuid", name)
    ),
    "minimal": lambda name: NORMALIZER_PATTERNS["numeric_id"].sub(
        "/:id", NORMALIZER_PATTERNS["uuid"].sub("/:uuid", name)
    ),
}


class SpanNameNormalizingProcessor(SpanProcessor):
    """Normalize span names on start to reduce backend cardinality."""

    def __init__(
        self,
        wrapped_processor: SpanProcessor,
        normalizer: SpanNameNormalizer | SpanNameNormalizerPreset,
    ) -> None:
        self.wrapped_processor = wrapped_processor
        self.normalizer = _resolve_normalizer(normalizer)

    def on_start(
        self,
        span: Span,
        parent_context: otel_context.Context | None = None,
    ) -> None:
        try:
            normalized_name = self.normalizer(span.name)
            if normalized_name != span.name:
                span.update_name(normalized_name)
        except Exception:
            pass
        self.wrapped_processor.on_start(span, parent_context)

    def on_end(self, span: ReadableSpan) -> None:
        self.wrapped_processor.on_end(span)

    def shutdown(self) -> None:
        self.wrapped_processor.shutdown()

    def force_flush(self, timeout_millis: int = 30000) -> bool:
        return self.wrapped_processor.force_flush(timeout_millis)


class FilteringSpanProcessor(SpanProcessor):
    """Drop completed spans when a predicate returns False."""

    def __init__(self, wrapped_processor: SpanProcessor, span_filter: SpanFilter) -> None:
        self.wrapped_processor = wrapped_processor
        self.span_filter = span_filter

    def on_start(
        self,
        span: Span,
        parent_context: otel_context.Context | None = None,
    ) -> None:
        self.wrapped_processor.on_start(span, parent_context)

    def on_end(self, span: ReadableSpan) -> None:
        try:
            if self.span_filter(span):
                self.wrapped_processor.on_end(span)
        except Exception:
            # Fail open: a broken filter must not drop spans. Surface it so the
            # filter bug is debuggable instead of silently swallowed.
            logger.warning(
                "span_filter raised; forwarding span unfiltered", exc_info=True
            )
            self.wrapped_processor.on_end(span)

    def shutdown(self) -> None:
        self.wrapped_processor.shutdown()

    def force_flush(self, timeout_millis: int = 30000) -> bool:
        return self.wrapped_processor.force_flush(timeout_millis)


class AttributeRedactingProcessor(SpanProcessor):
    """Redact span attributes before forwarding to the wrapped processor."""

    def __init__(
        self,
        wrapped_processor: SpanProcessor,
        redactor: AttributeRedactorConfig | AttributeRedactorPreset | AttributeRedactor,
    ) -> None:
        self.wrapped_processor = wrapped_processor
        self.redactor = create_attribute_redactor(redactor)

    def on_start(
        self,
        span: Span,
        parent_context: otel_context.Context | None = None,
    ) -> None:
        self.wrapped_processor.on_start(span, parent_context)

    def on_end(self, span: ReadableSpan) -> None:
        try:
            self.wrapped_processor.on_end(create_redacted_span(span, self.redactor))
        except Exception:
            # Fail open: redaction failure must not drop the span, but a span
            # forwarded unredacted may leak data, so make the failure visible.
            logger.warning(
                "attribute redaction failed; forwarding span unredacted",
                exc_info=True,
            )
            self.wrapped_processor.on_end(span)

    def shutdown(self) -> None:
        self.wrapped_processor.shutdown()

    def force_flush(self, timeout_millis: int = 30000) -> bool:
        return self.wrapped_processor.force_flush(timeout_millis)


class _RedactedSpanProxy:
    """Proxy for ReadableSpan with redacted attributes."""

    def __init__(self, span: ReadableSpan, attributes: Mapping[str, Any]) -> None:
        self._span = span
        self._attributes = attributes

    @property
    def attributes(self) -> Mapping[str, Any]:
        return self._attributes

    def __getattr__(self, name: str) -> Any:
        return getattr(self._span, name)


def create_redacted_span(span: ReadableSpan, redactor: AttributeRedactor) -> ReadableSpan:
    """Create a ReadableSpan proxy with redacted attributes."""
    return _RedactedSpanProxy(
        span,
        {key: redactor(key, value) for key, value in (span.attributes or {}).items()},
    )  # type: ignore[return-value]


def create_attribute_redactor(
    config: AttributeRedactorConfig | AttributeRedactorPreset | AttributeRedactor,
) -> AttributeRedactor:
    """Create an attribute redactor from a preset, config, or callable."""
    if callable(config) and not isinstance(config, str):
        return config

    resolved = _resolve_redactor_config(config)
    if resolved.redactor:
        return resolved.redactor

    key_patterns = resolved.key_patterns
    value_patterns = resolved.value_patterns
    path_set = set(resolved.paths)
    patterns = resolved.patterns
    replacement = resolved.replacement

    def redact(key: str, value: Any) -> Any:
        if isinstance(value, str):
            if key in path_set or any(pattern.search(key) for pattern in key_patterns):
                return replacement
            result = value
            for value_pattern in value_patterns:
                if value_pattern.mask:
                    result = value_pattern.pattern.sub(value_pattern.mask, result)
                else:
                    result = value_pattern.pattern.sub(
                        value_pattern.replacement or replacement,
                        result,
                    )
            for pattern in patterns:
                result = pattern.sub(replacement, result)
            return result

        if isinstance(value, tuple | list):
            return [redact(key, item) if isinstance(item, str) else item for item in value]

        return value

    return redact


def _resolve_normalizer(
    normalizer: SpanNameNormalizer | SpanNameNormalizerPreset,
) -> SpanNameNormalizer:
    if callable(normalizer):
        return normalizer
    try:
        return NORMALIZER_PRESETS[normalizer]
    except KeyError as exc:
        presets = ", ".join(NORMALIZER_PRESETS)
        raise ValueError(f"Unknown span name normalizer preset: {normalizer}. Use: {presets}") from exc


def _resolve_redactor_config(
    config: AttributeRedactorConfig | AttributeRedactorPreset | AttributeRedactor,
) -> AttributeRedactorConfig:
    if isinstance(config, AttributeRedactorConfig):
        return config
    if callable(config) and not isinstance(config, str):
        return AttributeRedactorConfig(redactor=config)
    preset = cast(AttributeRedactorPreset, config)
    try:
        return REDACTOR_PRESETS[preset]
    except KeyError as exc:
        presets = ", ".join(REDACTOR_PRESETS)
        raise ValueError(f"Unknown attribute redactor preset: {config}. Use: {presets}") from exc


def _mask_credit_card(match: re.Match[str]) -> str:
    digits = re.sub(r"[\s-]", "", match.group(0))
    return f"****{digits[-4:]}"


def _mask_email(match: re.Match[str]) -> str:
    value = match.group(0)
    at = value.find("@")
    if at < 1:
        return "***@***"
    tld = value[value.rfind(".") :]
    return f"{value[0]}***@***{tld}"


def _mask_phone(match: re.Match[str]) -> str:
    value = match.group(0)
    digits = re.sub(r"[^\d]", "", value)
    if value.startswith("+") and len(digits) > 4:
        country = re.match(r"^\+\d{1,3}", value)
        prefix = country.group(0) if country else "+"
        return f"{prefix}******{digits[-2:]}"
    if len(digits) > 2:
        return f"{'*' * (len(digits) - 2)}{digits[-2:]}"
    return "***"


def _mask_ipv4(match: re.Match[str]) -> str:
    return f"***.***.***.{match.group(0).split('.')[-1]}"


def _mask_iban(match: re.Match[str]) -> str:
    value = re.sub(r"[\s-]", "", match.group(0))
    return f"{value[:4]}****{value[-3:]}"


BUILTIN_VALUE_PATTERNS: dict[str, ValuePattern] = {
    "creditCard": ValuePattern(
        "creditCard",
        re.compile(r"\b\d{4}[\s-]?\d{4}[\s-]?\d{4}[\s-]?\d{4}\b"),
        mask=_mask_credit_card,
    ),
    "email": ValuePattern(
        "email",
        re.compile(r"[\w.+-]+@[\w-]+\.[\w.]+"),
        mask=_mask_email,
    ),
    "ipv4": ValuePattern(
        "ipv4",
        re.compile(r"\b(?!0\.0\.0\.0\b)(?!127\.0\.0\.1\b)\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}\b"),
        mask=_mask_ipv4,
    ),
    "phone": ValuePattern(
        "phone",
        re.compile(
            r"(?:\+\d{1,3}[\s.-]?\(?\d{1,4}\)?(?:[\s.-]?\d{2,4}){2,4}|\(\d{1,4}\)(?:[\s.-]?\d{2,4}){2,4}|\b\d{3}[-.]?\d{3}[-.]?\d{4}\b)"
        ),
        mask=_mask_phone,
    ),
    "jwt": ValuePattern("jwt", re.compile(r"\beyJ[\w-]*\.[\w-]*\.[\w-]*\b"), replacement="eyJ***.***"),
    "bearer": ValuePattern(
        "bearer",
        re.compile(r"\bBearer\s+[\w\-.~+/]{8,}=*", re.IGNORECASE),
        replacement="Bearer ***",
    ),
    "iban": ValuePattern(
        "iban",
        re.compile(r"\b[A-Z]{2}\d{2}[\s-]?[\dA-Z]{4}[\s-]?[\dA-Z]{4}[\s-]?[\dA-Z]{4}[\s-]?[\dA-Z]{0,4}[\s-]?[\dA-Z]{0,4}[\s-]?[\dA-Z]{0,4}\b"),
        mask=_mask_iban,
    ),
}


SENSITIVE_KEY_PATTERN = re.compile(
    r"^(password|passwd|pwd|secret|token|api[_-]?key|auth|credential|private[_-]?key|authorization)$",
    re.IGNORECASE,
)


REDACTOR_PRESETS: dict[AttributeRedactorPreset, AttributeRedactorConfig] = {
    "default": AttributeRedactorConfig(
        key_patterns=(SENSITIVE_KEY_PATTERN,),
        value_patterns=(
            BUILTIN_VALUE_PATTERNS["email"],
            BUILTIN_VALUE_PATTERNS["phone"],
            ValuePattern("ssn", re.compile(r"\b\d{3}[-]?\d{2}[-]?\d{4}\b")),
            BUILTIN_VALUE_PATTERNS["creditCard"],
        ),
    ),
    "strict": AttributeRedactorConfig(
        key_patterns=(SENSITIVE_KEY_PATTERN, re.compile("bearer", re.IGNORECASE), re.compile("jwt", re.IGNORECASE)),
        value_patterns=tuple(BUILTIN_VALUE_PATTERNS.values())
        + (ValuePattern("apiKeyInValue", re.compile(r"(?:api[_-]?key|apikey|api_secret)[=:][\s\"']*[A-Za-z0-9_-]+", re.IGNORECASE)),),
    ),
    "pci-dss": AttributeRedactorConfig(
        key_patterns=(
            re.compile("card", re.IGNORECASE),
            re.compile("cvv", re.IGNORECASE),
            re.compile("cvc", re.IGNORECASE),
            re.compile("pan", re.IGNORECASE),
            re.compile("expir", re.IGNORECASE),
            re.compile("ccn", re.IGNORECASE),
        ),
        value_patterns=(BUILTIN_VALUE_PATTERNS["creditCard"],),
    ),
}
