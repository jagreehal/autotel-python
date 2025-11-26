"""Tests for circuit breaker."""

import time

import pytest

from autotel.circuit_breaker import CircuitBreaker, CircuitState


def test_circuit_breaker_closed_state() -> None:
    """Test circuit breaker in closed (normal) state."""
    breaker = CircuitBreaker(failure_threshold=5)

    assert breaker.state == CircuitState.CLOSED
    assert breaker.is_open() is False


def test_circuit_breaker_opens_after_threshold() -> None:
    """Test circuit breaker opens after threshold failures."""
    breaker = CircuitBreaker(failure_threshold=3)

    # Record failures
    breaker.record_failure()
    assert breaker.state == CircuitState.CLOSED

    breaker.record_failure()
    assert breaker.state == CircuitState.CLOSED

    breaker.record_failure()
    # Should open after 3 failures
    assert breaker.state == CircuitState.OPEN  # type: ignore[comparison-overlap]
    assert breaker.is_open() is True


def test_circuit_breaker_half_open_recovery() -> None:
    """Test circuit breaker transitions to half-open for recovery."""
    breaker = CircuitBreaker(failure_threshold=2, recovery_timeout=0.1)

    # Open the circuit
    breaker.record_failure()
    breaker.record_failure()
    assert breaker.state == CircuitState.OPEN

    # Wait for recovery timeout
    time.sleep(0.15)

    # Should transition to half-open
    assert breaker.is_open() is False
    assert breaker.state == CircuitState.HALF_OPEN  # type: ignore[comparison-overlap]


def test_circuit_breaker_closes_on_success() -> None:
    """Test circuit breaker closes after successful recovery."""
    breaker = CircuitBreaker(failure_threshold=2, recovery_timeout=0.1, success_threshold=2)

    # Open the circuit
    breaker.record_failure()
    breaker.record_failure()
    assert breaker.state == CircuitState.OPEN

    # Wait for recovery
    time.sleep(0.15)
    # Call is_open() to trigger state transition from OPEN to HALF_OPEN
    breaker.is_open()  # This transitions OPEN -> HALF_OPEN if timeout passed
    assert breaker.state == CircuitState.HALF_OPEN  # type: ignore[comparison-overlap]

    # Record successes
    breaker.record_success()
    assert breaker.state == CircuitState.HALF_OPEN

    breaker.record_success()
    # Should close after 2 successes
    assert breaker.state == CircuitState.CLOSED
    assert breaker.is_open() is False


def test_circuit_breaker_reopens_on_failure_during_recovery() -> None:
    """Test circuit breaker reopens if failure occurs during recovery."""
    breaker = CircuitBreaker(failure_threshold=2, recovery_timeout=0.1)

    # Open and wait for recovery
    breaker.record_failure()
    breaker.record_failure()
    time.sleep(0.15)
    # Call is_open() to trigger state transition from OPEN to HALF_OPEN
    breaker.is_open()  # This transitions OPEN -> HALF_OPEN if timeout passed
    assert breaker.state == CircuitState.HALF_OPEN

    # Failure during recovery should reopen
    breaker.record_failure()
    assert breaker.state == CircuitState.OPEN  # type: ignore[comparison-overlap]


def test_circuit_breaker_reset() -> None:
    """Test resetting the circuit breaker."""
    breaker = CircuitBreaker(failure_threshold=2)

    # Open the circuit
    breaker.record_failure()
    breaker.record_failure()
    assert breaker.state == CircuitState.OPEN

    # Reset
    breaker.reset()
    assert breaker.state == CircuitState.CLOSED  # type: ignore[comparison-overlap]
    assert breaker.failure_count == 0


def test_circuit_breaker_validation() -> None:
    """Test parameter validation."""
    # Valid
    breaker = CircuitBreaker(failure_threshold=5)
    assert breaker.failure_threshold == 5

    # Invalid
    with pytest.raises(ValueError):
        CircuitBreaker(failure_threshold=0)
    with pytest.raises(ValueError):
        CircuitBreaker(failure_threshold=5, recovery_timeout=0)
    with pytest.raises(ValueError):
        CircuitBreaker(failure_threshold=5, success_threshold=0)
