"""Validation system for events and attributes."""

import logging
import re
from typing import Any

logger = logging.getLogger(__name__)


class ValidationConfig:
    """Configuration for validation rules."""

    def __init__(
        self,
        *,
        max_event_name_length: int = 100,
        max_attribute_length: int = 1000,
        max_nesting_depth: int = 5,
        max_array_size: int = 100,
        max_object_size: int = 100,
        sensitive_patterns: dict[str, str] | None = None,
        graceful_degradation: bool = True,
    ):
        """
        Initialize validation configuration.

        Args:
            max_event_name_length: Maximum length for event names
            max_attribute_length: Maximum length for string attributes
            max_nesting_depth: Maximum nesting depth for objects/arrays
            max_array_size: Maximum array size
            max_object_size: Maximum object size (number of keys)
            sensitive_patterns: Regex patterns for sensitive data detection
            graceful_degradation: If True, log warnings instead of raising exceptions
        """
        self.max_event_name_length = max_event_name_length
        self.max_attribute_length = max_attribute_length
        self.max_nesting_depth = max_nesting_depth
        self.max_array_size = max_array_size
        self.max_object_size = max_object_size
        self.sensitive_patterns = sensitive_patterns or {}
        self.graceful_degradation = graceful_degradation


class Validator:
    """Validates events and attributes according to configuration."""

    def __init__(self, config: ValidationConfig | None = None):
        """
        Initialize validator.

        Args:
            config: Validation configuration (uses defaults if None)
        """
        self.config = config or ValidationConfig()

    def validate_event_name(self, name: str) -> bool:
        """
        Validate event name.

        Args:
            name: Event name to validate

        Returns:
            True if valid, False otherwise
        """
        if not isinstance(name, str):
            self._warn_or_raise(f"Event name must be a string, got {type(name)}")
            return False

        if len(name) > self.config.max_event_name_length:
            self._warn_or_raise(
                f"Event name too long: {len(name)} > {self.config.max_event_name_length}"
            )
            return False

        # Check for invalid characters
        if not re.match(r"^[a-zA-Z0-9._-]+$", name):
            self._warn_or_raise(f"Event name contains invalid characters: {name}")
            return False

        return True

    def validate_attribute(self, key: str, value: Any, depth: int = 0) -> bool:
        """
        Validate attribute value.

        Args:
            key: Attribute key
            value: Attribute value
            depth: Current nesting depth

        Returns:
            True if valid, False otherwise
        """
        if depth > self.config.max_nesting_depth:
            self._warn_or_raise(
                f"Nesting depth exceeded: {depth} > {self.config.max_nesting_depth}"
            )
            return False

        # Check sensitive patterns
        if isinstance(value, str):
            for pattern_name, pattern in self.config.sensitive_patterns.items():
                if re.search(pattern, value, re.IGNORECASE):
                    self._warn_or_raise(
                        f"Sensitive data detected ({pattern_name}) in attribute: {key}"
                    )
                    return False

            if len(value) > self.config.max_attribute_length:
                self._warn_or_raise(
                    f"Attribute value too long: {len(value)} > {self.config.max_attribute_length}"
                )
                return False

        elif isinstance(value, list):
            if len(value) > self.config.max_array_size:
                self._warn_or_raise(f"Array too large: {len(value)} > {self.config.max_array_size}")
                return False

            # Validate each element
            for item in value:
                if not self.validate_attribute(f"{key}[]", item, depth + 1):
                    return False

        elif isinstance(value, dict):
            if len(value) > self.config.max_object_size:
                self._warn_or_raise(
                    f"Object too large: {len(value)} > {self.config.max_object_size}"
                )
                return False

            # Validate each value
            for k, v in value.items():
                if not self.validate_attribute(k, v, depth + 1):
                    return False

        return True

    def validate_properties(self, properties: dict[str, Any] | None) -> bool:
        """
        Validate event properties.

        Args:
            properties: Event properties dictionary

        Returns:
            True if valid, False otherwise
        """
        if properties is None:
            return True

        if not isinstance(properties, dict):
            self._warn_or_raise(f"Properties must be a dict, got {type(properties)}")
            return False

        return all(self.validate_attribute(key, value) for key, value in properties.items())

    def _warn_or_raise(self, message: str) -> None:
        """Log warning or raise exception based on graceful_degradation setting."""
        if self.config.graceful_degradation:
            logger.warning(f"Validation warning: {message}")
        else:
            raise ValueError(f"Validation error: {message}")


# Global validator instance
_global_validator: Validator | None = None


def set_validator(validator: Validator) -> None:
    """Set global validator instance."""
    global _global_validator
    _global_validator = validator


def get_validator() -> Validator | None:
    """Get global validator instance."""
    return _global_validator
