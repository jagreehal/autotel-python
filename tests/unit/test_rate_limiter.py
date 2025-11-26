"""Tests for rate limiter."""

import time

import pytest

from autotel.rate_limiter import RateLimiter


def test_rate_limiter_allows_within_limit() -> None:
    """Test that rate limiter allows spans within limit."""
    limiter = RateLimiter(max_spans_per_second=10, burst_size=10)

    # Should allow 10 spans immediately (burst)
    for _ in range(10):
        assert limiter.allow_span() is True

    # Next one should be rate limited
    assert limiter.allow_span() is False


def test_rate_limiter_refills_over_time() -> None:
    """Test that tokens refill over time."""
    limiter = RateLimiter(max_spans_per_second=10, burst_size=10)

    # Use all tokens
    for _ in range(10):
        assert limiter.allow_span() is True

    # Wait for refill (0.2 seconds should give us 2 tokens at 10/sec)
    time.sleep(0.2)
    assert limiter.allow_span() is True
    assert limiter.allow_span() is True
    assert limiter.allow_span() is False  # No more tokens yet


def test_rate_limiter_burst_size() -> None:
    """Test custom burst size."""
    limiter = RateLimiter(max_spans_per_second=10, burst_size=5)

    # Should allow 5 spans (burst size)
    for _ in range(5):
        assert limiter.allow_span() is True

    # Next one should be rate limited
    assert limiter.allow_span() is False


def test_rate_limiter_reset() -> None:
    """Test resetting the rate limiter."""
    limiter = RateLimiter(max_spans_per_second=10, burst_size=10)

    # Use all tokens
    for _ in range(10):
        limiter.allow_span()

    # Reset
    limiter.reset()

    # Should allow spans again
    assert limiter.allow_span() is True


def test_rate_limiter_validation() -> None:
    """Test parameter validation."""
    # Valid
    limiter = RateLimiter(max_spans_per_second=10)
    assert limiter.rate == 10

    # Invalid
    with pytest.raises(ValueError):
        RateLimiter(max_spans_per_second=0)
    with pytest.raises(ValueError):
        RateLimiter(max_spans_per_second=10, burst_size=0)
    with pytest.raises(ValueError):
        RateLimiter(max_spans_per_second=-1)
