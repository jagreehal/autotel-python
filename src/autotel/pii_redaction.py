"""PII (Personally Identifiable Information) redaction for autotel."""

import re
from typing import Any


class PIIRedactor:
    """
    Redacts PII from span attributes and events.

    Detects common patterns like email, phone, SSN, credit card numbers, and API keys.
    """

    # Common PII patterns
    EMAIL_PATTERN = re.compile(r"\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b")
    PHONE_PATTERN = re.compile(r"\b\d{3}[-.\s]?\d{3}[-.\s]?\d{4}\b")
    SSN_PATTERN = re.compile(r"\b\d{3}-\d{2}-\d{4}\b")
    CREDIT_CARD_PATTERN = re.compile(r"\b\d{4}[-.\s]?\d{4}[-.\s]?\d{4}[-.\s]?\d{4}\b")
    API_KEY_PATTERN = re.compile(r"\b(sk_|pk_|api_|token_)[a-zA-Z0-9_-]{10,}\b", re.IGNORECASE)

    def __init__(
        self,
        redact_email: bool = True,
        redact_phone: bool = True,
        redact_ssn: bool = True,
        redact_credit_card: bool = True,
        redact_api_key: bool = True,
        custom_patterns: dict[str, str] | None = None,
        allowlist_keys: list[str] | None = None,
    ):
        """
        Initialize PII redactor.

        Args:
            redact_email: Redact email addresses
            redact_phone: Redact phone numbers
            redact_ssn: Redact SSNs
            redact_credit_card: Redact credit card numbers
            redact_api_key: Redact API keys (sk_*, pk_*, etc.)
            custom_patterns: Custom regex patterns {name: pattern}
            allowlist_keys: Keys that should never be redacted
        """
        self.redact_email = redact_email
        self.redact_phone = redact_phone
        self.redact_ssn = redact_ssn
        self.redact_credit_card = redact_credit_card
        self.redact_api_key = redact_api_key
        self.custom_patterns = custom_patterns or {}
        self.allowlist_keys = set(allowlist_keys or [])

        # Compile custom patterns
        self._compiled_custom_patterns = {
            name: re.compile(pattern) for name, pattern in self.custom_patterns.items()
        }

    def redact_attribute(
        self, key: str, value: str | int | float | bool
    ) -> str | int | float | bool:
        """
        Redact PII from an attribute value.

        Args:
            key: Attribute key
            value: Attribute value

        Returns:
            Redacted value (or original if not a string or in allowlist)
        """
        # Never redact allowlisted keys
        if key in self.allowlist_keys:
            return value

        # Only process string values
        if not isinstance(value, str):
            return value

        result = value

        # Apply redaction patterns
        if self.redact_email:
            result = self.EMAIL_PATTERN.sub("[EMAIL_REDACTED]", result)

        if self.redact_phone:
            result = self.PHONE_PATTERN.sub("[PHONE_REDACTED]", result)

        if self.redact_ssn:
            result = self.SSN_PATTERN.sub("[SSN_REDACTED]", result)

        if self.redact_credit_card:
            result = self.CREDIT_CARD_PATTERN.sub("[CC_REDACTED]", result)

        if self.redact_api_key:
            result = self.API_KEY_PATTERN.sub("[API_KEY_REDACTED]", result)

        # Apply custom patterns
        for name, pattern in self._compiled_custom_patterns.items():
            result = pattern.sub(f"[{name.upper()}_REDACTED]", result)

        return result

    def redact_dict(self, data: dict[str, Any]) -> dict[str, Any]:
        """
        Redact PII from all values in a dictionary.

        Args:
            data: Dictionary to redact

        Returns:
            Dictionary with redacted values
        """
        return {key: self.redact_attribute(key, value) for key, value in data.items()}
