"""Circuit breaker for exporter failure protection."""

import time
from enum import Enum
from threading import Lock


class CircuitState(Enum):
    """Circuit breaker states."""

    CLOSED = "closed"  # Normal operation
    OPEN = "open"  # Too many failures, stop exporting
    HALF_OPEN = "half_open"  # Testing if exporter recovered


class CircuitBreaker:
    """
    Circuit breaker pattern for exporter failure protection.

    Prevents cascading failures by stopping export attempts after threshold failures.
    """

    def __init__(
        self,
        failure_threshold: int = 5,
        recovery_timeout: float = 60.0,
        success_threshold: int = 2,
    ):
        """
        Initialize circuit breaker.

        Args:
            failure_threshold: Number of failures before opening circuit
            recovery_timeout: Seconds to wait before attempting recovery (half-open)
            success_threshold: Number of successes needed to close circuit from half-open
        """
        if failure_threshold <= 0:
            raise ValueError("failure_threshold must be positive")
        if recovery_timeout <= 0:
            raise ValueError("recovery_timeout must be positive")
        if success_threshold <= 0:
            raise ValueError("success_threshold must be positive")

        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self.success_threshold = success_threshold

        self.state = CircuitState.CLOSED
        self.failure_count = 0
        self.success_count = 0
        self.last_failure_time: float | None = None
        self.lock = Lock()

    def record_success(self) -> None:
        """Record a successful export."""
        with self.lock:
            if self.state == CircuitState.HALF_OPEN:
                self.success_count += 1
                if self.success_count >= self.success_threshold:
                    # Circuit recovered - close it
                    self.state = CircuitState.CLOSED
                    self.failure_count = 0
                    self.success_count = 0
            elif self.state == CircuitState.CLOSED:
                # Reset failure count on success
                self.failure_count = 0

    def record_failure(self) -> None:
        """Record a failed export."""
        with self.lock:
            self.failure_count += 1
            self.last_failure_time = time.time()

            if self.state == CircuitState.HALF_OPEN:
                # Failed during recovery - open circuit again
                self.state = CircuitState.OPEN
                self.success_count = 0
            elif self.state == CircuitState.CLOSED:
                if self.failure_count >= self.failure_threshold:
                    # Too many failures - open circuit
                    self.state = CircuitState.OPEN

    def is_open(self) -> bool:
        """
        Check if circuit is open (should skip export).

        Returns:
            True if circuit is open, False if export should proceed
        """
        with self.lock:
            # Check if we should transition from OPEN to HALF_OPEN
            if self.state == CircuitState.OPEN:
                if self.last_failure_time is not None:
                    elapsed = time.time() - self.last_failure_time
                    if elapsed >= self.recovery_timeout:
                        # Try recovery
                        self.state = CircuitState.HALF_OPEN
                        self.success_count = 0
                        return False  # Allow one attempt
                return True  # Circuit is open

            return False  # Circuit is closed or half-open

    def reset(self) -> None:
        """Reset circuit breaker (useful for testing)."""
        with self.lock:
            self.state = CircuitState.CLOSED
            self.failure_count = 0
            self.success_count = 0
            self.last_failure_time = None
