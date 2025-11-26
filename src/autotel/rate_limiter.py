"""Rate limiting for autotel spans."""

import time
from threading import Lock


class RateLimiter:
    """
    Token bucket rate limiter for span creation.

    Prevents span flooding by limiting the number of spans created per second.
    """

    def __init__(self, max_spans_per_second: int, burst_size: int | None = None):
        """
        Initialize rate limiter.

        Args:
            max_spans_per_second: Maximum number of spans allowed per second
            burst_size: Maximum burst size (defaults to max_spans_per_second)
        """
        if max_spans_per_second <= 0:
            raise ValueError("max_spans_per_second must be positive")
        if burst_size is not None and burst_size <= 0:
            raise ValueError("burst_size must be positive")

        self.rate = max_spans_per_second
        self.burst = burst_size if burst_size is not None else max_spans_per_second
        self.tokens = float(self.burst)
        self.last_update = time.time()
        self.lock = Lock()

    def allow_span(self) -> bool:
        """
        Check if a span should be created (rate limiting).

        Returns:
            True if span should be created, False if rate limited
        """
        with self.lock:
            now = time.time()
            elapsed = now - self.last_update

            # Refill tokens based on elapsed time
            self.tokens = min(self.burst, self.tokens + elapsed * self.rate)
            self.last_update = now

            # Check if we have tokens available
            if self.tokens >= 1.0:
                self.tokens -= 1.0
                return True
            return False

    def reset(self) -> None:
        """Reset the rate limiter (useful for testing)."""
        with self.lock:
            self.tokens = float(self.burst)
            self.last_update = time.time()
