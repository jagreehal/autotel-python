"""Tests for configuration validation."""

import pytest

from autotel.config import autotelConfig


def test_valid_config() -> None:
    """Test valid configuration."""
    config = autotelConfig(service="test-service")
    assert config.service == "test-service"
    assert config.endpoint == "http://localhost:4318"
    assert config.protocol == "http"


def test_invalid_endpoint() -> None:
    """Test invalid endpoint validation."""
    with pytest.raises(ValueError, match="endpoint must start with"):
        autotelConfig(service="test", endpoint="invalid-endpoint")


def test_invalid_service() -> None:
    """Test invalid service name validation."""
    with pytest.raises(ValueError, match="service name cannot be empty"):
        autotelConfig(service="")


def test_batch_timeout_validation() -> None:
    """Test batch timeout validation."""
    # Valid
    config = autotelConfig(service="test", batch_timeout=1000)
    assert config.batch_timeout == 1000

    # Too low
    with pytest.raises(ValueError):
        autotelConfig(service="test", batch_timeout=50)

    # Too high
    with pytest.raises(ValueError):
        autotelConfig(service="test", batch_timeout=100000)


def test_queue_size_validation() -> None:
    """Test queue size validation."""
    # Valid
    config = autotelConfig(service="test", max_queue_size=1000)
    assert config.max_queue_size == 1000

    # Too low
    with pytest.raises(ValueError):
        autotelConfig(service="test", max_queue_size=0)
