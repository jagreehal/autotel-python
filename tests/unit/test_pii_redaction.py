"""Tests for PII redaction."""

from autotel.pii_redaction import PIIRedactor


def test_redact_email() -> None:
    """Test email redaction."""
    redactor = PIIRedactor(redact_email=True)

    result = redactor.redact_attribute("email", "user@example.com")
    assert result == "[EMAIL_REDACTED]"

    # Should not redact non-email strings
    result = redactor.redact_attribute("name", "John Doe")
    assert result == "John Doe"


def test_redact_phone() -> None:
    """Test phone number redaction."""
    redactor = PIIRedactor(redact_phone=True)

    result = redactor.redact_attribute("phone", "555-123-4567")
    assert result == "[PHONE_REDACTED]"

    result = redactor.redact_attribute("phone", "555.123.4567")
    assert result == "[PHONE_REDACTED]"

    result = redactor.redact_attribute("phone", "555 123 4567")
    assert result == "[PHONE_REDACTED]"


def test_redact_ssn() -> None:
    """Test SSN redaction."""
    redactor = PIIRedactor(redact_ssn=True)

    result = redactor.redact_attribute("ssn", "123-45-6789")
    assert result == "[SSN_REDACTED]"


def test_redact_credit_card() -> None:
    """Test credit card redaction."""
    redactor = PIIRedactor(redact_credit_card=True)

    result = redactor.redact_attribute("card", "4111-1111-1111-1111")
    assert result == "[CC_REDACTED]"

    result = redactor.redact_attribute("card", "4111 1111 1111 1111")
    assert result == "[CC_REDACTED]"


def test_redact_api_key() -> None:
    """Test API key redaction."""
    redactor = PIIRedactor(redact_api_key=True)

    result = redactor.redact_attribute("key", "sk_test_1234567890abcdefghijklmnopqrstuvwxyz")
    assert result == "[API_KEY_REDACTED]"

    result = redactor.redact_attribute("key", "pk_test_xyz789")
    assert result == "[API_KEY_REDACTED]"


def test_allowlist_keys() -> None:
    """Test that allowlisted keys are never redacted."""
    redactor = PIIRedactor(
        redact_email=True,
        allowlist_keys=["user_id", "request_id"],
    )

    # Should redact normally
    result = redactor.redact_attribute("email", "user@example.com")
    assert result == "[EMAIL_REDACTED]"

    # Should not redact allowlisted keys
    result = redactor.redact_attribute("user_id", "user@example.com")
    assert result == "user@example.com"

    result = redactor.redact_attribute("request_id", "123-45-6789")
    assert result == "123-45-6789"


def test_custom_patterns() -> None:
    """Test custom redaction patterns."""
    redactor = PIIRedactor(
        custom_patterns={
            "secret": r"secret_[a-zA-Z0-9]+",
        }
    )

    result = redactor.redact_attribute("token", "secret_abc123")
    assert result == "[SECRET_REDACTED]"


def test_redact_dict() -> None:
    """Test redacting entire dictionary."""
    redactor = PIIRedactor(redact_email=True, redact_phone=True)

    data = {
        "name": "John Doe",
        "email": "john@example.com",
        "phone": "555-123-4567",
        "age": 30,
    }

    redacted = redactor.redact_dict(data)
    assert redacted["name"] == "John Doe"
    assert redacted["email"] == "[EMAIL_REDACTED]"
    assert redacted["phone"] == "[PHONE_REDACTED]"
    assert redacted["age"] == 30  # Non-string values unchanged


def test_non_string_values() -> None:
    """Test that non-string values are not redacted."""
    redactor = PIIRedactor(redact_email=True)

    assert redactor.redact_attribute("count", 42) == 42
    assert redactor.redact_attribute("active", True) is True
    assert redactor.redact_attribute("price", 99.99) == 99.99
