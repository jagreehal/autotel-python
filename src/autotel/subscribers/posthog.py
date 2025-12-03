"""PostHog event subscriber."""

import logging
from collections.abc import Callable
from typing import Any

try:
    import httpx
except ImportError:
    httpx = None  # type: ignore[assignment]

from .base import EventSubscriber

logger = logging.getLogger(__name__)


class PostHogSubscriber(EventSubscriber):
    """
    PostHog event subscriber.

    Example:
        >>> from autotel.subscribers import PostHogSubscriber
        >>> subscriber = PostHogSubscriber(api_key="phc_...", host="https://app.posthog.com")

    Serverless mode (AWS Lambda, Vercel, etc.):
        >>> subscriber = PostHogSubscriber(
        ...     api_key="phc_...",
        ...     serverless=True,  # Short timeout, no retries
        ... )

    With error handling:
        >>> subscriber = PostHogSubscriber(
        ...     api_key="phc_...",
        ...     on_error=lambda e: print(f"PostHog error: {e}"),
        ... )
    """

    def __init__(
        self,
        api_key: str,
        host: str = "https://app.posthog.com",
        project_api_key: str | None = None,  # Legacy parameter
        *,
        serverless: bool = False,
        timeout: float | None = None,
        filter_none_values: bool = True,
        on_error: Callable[[Exception], None] | None = None,
    ):
        """
        Initialize PostHog subscriber.

        Args:
            api_key: PostHog API key (phc_...)
            host: PostHog host URL
            project_api_key: Legacy parameter (use api_key instead)
            serverless: Enable serverless mode (shorter timeout for Lambda/Vercel)
            timeout: Request timeout in seconds (default: 5.0, serverless: 3.0)
            filter_none_values: Remove None values from properties (default: True)
            on_error: Callback for error handling (receives Exception)
        """
        if httpx is None:
            raise ImportError(
                "httpx is required for PostHogSubscriber. Install with: pip install httpx"
            )

        self.api_key = api_key or project_api_key
        if not self.api_key:
            raise ValueError("api_key is required")

        self.host = host.rstrip("/")
        self.filter_none_values = filter_none_values
        self.on_error = on_error

        # Serverless mode: shorter timeout for Lambda/Vercel cold starts
        request_timeout = timeout if timeout is not None else (3.0 if serverless else 5.0)
        self.client: Any = httpx.AsyncClient(timeout=request_timeout)

    async def send(self, event: str, properties: dict[str, Any] | None = None) -> None:
        """
        Send event to PostHog.

        Args:
            event: Event name
            properties: Event properties (None values filtered if filter_none_values=True)
        """
        if not self.client:
            return

        # Filter out None values if enabled (improves DX with optional properties)
        props = properties or {}
        if self.filter_none_values:
            props = {k: v for k, v in props.items() if v is not None}

        url = f"{self.host}/capture/"
        payload = {
            "api_key": self.api_key,
            "event": event,
            "properties": props,
        }

        try:
            response = await self.client.post(url, json=payload)
            response.raise_for_status()
        except Exception as e:
            logger.error(f"PostHog send failed: {e}", exc_info=True)
            if self.on_error:
                self.on_error(e)
            raise

    async def shutdown(self) -> None:
        """Shutdown subscriber and close HTTP client."""
        if self.client:
            await self.client.aclose()
            self.client = None
