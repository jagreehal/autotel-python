"""Tests for business baggage safe context propagation."""

import pytest
from opentelemetry import context

from autotel.business_baggage import (
    BusinessBaggage,
    configure_business_baggage,
    define_business_baggage,
    get_business_baggage,
)


class TestBusinessBaggage:
    """Tests for BusinessBaggage class."""

    def test_set_and_get_allowed_key(self) -> None:
        """Test setting and getting an allowed key."""
        bc = BusinessBaggage(allowed_keys=["tenant_id", "correlation_id"])

        ctx = bc.set(None, "tenant_id", "acme-corp")
        value = bc.get(ctx, "tenant_id")

        assert value == "acme-corp"

    def test_set_disallowed_key_raises(self) -> None:
        """Test that setting a disallowed key raises ValueError."""
        bc = BusinessBaggage(allowed_keys=["tenant_id"])

        with pytest.raises(ValueError, match="not in allowed_keys"):
            bc.set(None, "unknown_key", "value")

    def test_no_allowlist_allows_all_keys(self) -> None:
        """Test that empty allowlist allows all keys."""
        bc = BusinessBaggage()  # No allowlist

        ctx = bc.set(None, "any_key", "any_value")
        ctx = bc.set(ctx, "another_key", "another_value")

        assert bc.get(ctx, "any_key") == "any_value"
        assert bc.get(ctx, "another_key") == "another_value"

    def test_hash_keys_are_hashed(self) -> None:
        """Test that PII keys are automatically hashed."""
        bc = BusinessBaggage(
            allowed_keys=["tenant_id", "user_email"],
            hash_keys=["user_email"],
        )

        ctx = bc.set(None, "user_email", "user@example.com")
        value = bc.get(ctx, "user_email")

        assert value is not None
        assert value.startswith("hash:")
        assert "user@example.com" not in value

    def test_is_hashed_helper(self) -> None:
        """Test is_hashed helper method."""
        bc = BusinessBaggage(hash_keys=["email"])

        ctx = bc.set(None, "email", "test@example.com")
        value = bc.get(ctx, "email")

        assert value is not None
        assert bc.is_hashed(value) is True
        assert bc.is_hashed("normal_value") is False

    def test_value_truncation(self) -> None:
        """Test that long values are truncated."""
        bc = BusinessBaggage(max_value_length=20)

        long_value = "a" * 100
        ctx = bc.set(None, "key", long_value)
        value = bc.get(ctx, "key")

        assert value is not None
        assert len(value) == 20
        assert value.endswith("...")

    def test_set_many(self) -> None:
        """Test setting multiple entries at once."""
        bc = BusinessBaggage(allowed_keys=["a", "b", "c"])

        ctx = bc.set_many(None, {"a": "1", "b": "2", "c": "3"})

        assert bc.get(ctx, "a") == "1"
        assert bc.get(ctx, "b") == "2"
        assert bc.get(ctx, "c") == "3"

    def test_get_all(self) -> None:
        """Test getting all baggage entries."""
        bc = BusinessBaggage()

        ctx = bc.set(None, "key1", "value1")
        ctx = bc.set(ctx, "key2", "value2")
        all_baggage = bc.get_all(ctx)

        assert "key1" in all_baggage
        assert "key2" in all_baggage

    def test_get_allowed_only(self) -> None:
        """Test filtering to only allowed keys."""
        bc = BusinessBaggage(allowed_keys=["allowed"])

        # Set allowed key through business baggage
        ctx = bc.set(None, "allowed", "yes")

        # Get only allowed
        filtered = bc.get_allowed_only(ctx)

        assert "allowed" in filtered
        assert filtered["allowed"] == "yes"

    def test_delete(self) -> None:
        """Test deleting a baggage entry."""
        bc = BusinessBaggage()

        ctx = bc.set(None, "to_delete", "value")
        assert bc.get(ctx, "to_delete") == "value"

        ctx = bc.delete(ctx, "to_delete")
        assert bc.get(ctx, "to_delete") is None

    def test_validation_max_value_length(self) -> None:
        """Test validation of max_value_length."""
        with pytest.raises(ValueError, match="max_value_length must be at least 1"):
            BusinessBaggage(max_value_length=0)

    def test_validation_hash_algorithm(self) -> None:
        """Test validation of hash algorithm."""
        with pytest.raises(ValueError, match="Unsupported hash algorithm"):
            BusinessBaggage(hash_algorithm="invalid")

    def test_validation_hash_keys_subset(self) -> None:
        """Test that hash_keys must be subset of allowed_keys."""
        with pytest.raises(ValueError, match="hash_keys must be subset"):
            BusinessBaggage(
                allowed_keys=["key1"],
                hash_keys=["key2"],  # Not in allowed_keys
            )

    def test_validation_max_value_length_too_small_for_hash(self) -> None:
        """Test that max_value_length must accommodate hash output."""
        # Hash output is "hash:" (5 chars) + 16 hex chars = 21 chars minimum
        with pytest.raises(ValueError, match="too small for hashed values"):
            BusinessBaggage(
                hash_keys=["pii_field"],
                max_value_length=10,  # Too small for hash output
            )

        # Should work with sufficient length
        bc = BusinessBaggage(
            hash_keys=["pii_field"],
            max_value_length=25,  # Enough for hash
        )
        assert bc is not None

    def test_attach_context(self) -> None:
        """Test attaching context to make it current."""
        bc = BusinessBaggage()

        ctx = bc.set(None, "tenant", "acme")
        token = bc.attach(ctx)

        # After attach, current context should have the baggage
        current_baggage = bc.get_all()
        assert "tenant" in current_baggage

        context.detach(token)

    def test_different_hash_algorithms(self) -> None:
        """Test different hash algorithms produce different results."""
        bc_sha256 = BusinessBaggage(hash_keys=["key"], hash_algorithm="sha256")
        bc_sha1 = BusinessBaggage(hash_keys=["key"], hash_algorithm="sha1")
        bc_md5 = BusinessBaggage(hash_keys=["key"], hash_algorithm="md5")

        ctx_256 = bc_sha256.set(None, "key", "test")
        ctx_1 = bc_sha1.set(None, "key", "test")
        ctx_md5 = bc_md5.set(None, "key", "test")

        hash_256 = bc_sha256.get(ctx_256, "key")
        hash_1 = bc_sha1.get(ctx_1, "key")
        hash_md5 = bc_md5.get(ctx_md5, "key")

        # All should be hashed but with different values
        assert hash_256 is not None and hash_256.startswith("hash:")
        assert hash_1 is not None and hash_1.startswith("hash:")
        assert hash_md5 is not None and hash_md5.startswith("hash:")

        # Different algorithms should produce different hashes
        assert hash_256 != hash_1
        assert hash_256 != hash_md5


class TestDefineBusinessBaggage:
    """Tests for schema-based business baggage definition."""

    def test_basic_schema(self) -> None:
        """Test basic schema definition."""
        bc = define_business_baggage({
            "tenant_id": {"type": "string"},
            "correlation_id": {"type": "string"},
        })

        ctx = bc.set(None, "tenant_id", "acme")
        assert bc.get(ctx, "tenant_id") == "acme"

    def test_pii_flag(self) -> None:
        """Test PII flag causes hashing."""
        bc = define_business_baggage({
            "user_id": {"type": "string", "pii": True},
            "tenant_id": {"type": "string"},
        })

        ctx = bc.set(None, "user_id", "user@example.com")
        ctx = bc.set(ctx, "tenant_id", "acme")

        # user_id should be hashed
        user_val = bc.get(ctx, "user_id")
        assert user_val is not None
        assert user_val.startswith("hash:")

        # tenant_id should not be hashed
        assert bc.get(ctx, "tenant_id") == "acme"

    def test_propagate_false_excludes_key(self) -> None:
        """Test propagate: false excludes key from allowlist."""
        bc = define_business_baggage({
            "allowed": {"type": "string", "propagate": True},
            "not_allowed": {"type": "string", "propagate": False},
        })

        ctx = bc.set(None, "allowed", "yes")

        with pytest.raises(ValueError):
            bc.set(ctx, "not_allowed", "no")


class TestGlobalConfiguration:
    """Tests for global business baggage configuration."""

    def test_configure_and_get(self) -> None:
        """Test global configuration."""
        configure_business_baggage(
            allowed_keys=["global_key"],
        )

        bc = get_business_baggage()
        ctx = bc.set(None, "global_key", "value")

        assert bc.get(ctx, "global_key") == "value"

    def test_get_creates_default(self) -> None:
        """Test get_business_baggage creates default if not configured."""
        # Reset global state by importing fresh
        import autotel.business_baggage as bb
        bb._default_baggage = None

        bc = get_business_baggage()
        assert bc is not None

        # Should allow any key (no allowlist)
        ctx = bc.set(None, "any", "value")
        assert bc.get(ctx, "any") == "value"
