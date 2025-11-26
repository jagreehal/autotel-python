"""PostHog event subscriber."""

import logging
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
    """

    def __init__(
        self,
        api_key: str,
        host: str = "https://app.posthog.com",
        project_api_key: str | None = None,  # Legacy parameter
    ):
        """
        Initialize PostHog subscriber.

        Args:
            api_key: PostHog API key (phc_...)
            host: PostHog host URL
            project_api_key: Legacy parameter (use api_key instead)
        """
        if httpx is None:
            raise ImportError(
                "httpx is required for PostHogSubscriber. Install with: pip install httpx"
            )

        self.api_key = api_key or project_api_key
        if not self.api_key:
            raise ValueError("api_key is required")

        self.host = host.rstrip("/")
        self.client: Any = httpx.AsyncClient(timeout=5.0)

    async def send(self, event: str, properties: dict[str, Any] | None = None) -> None:
        """
        Send event to PostHog.

        Args:
            event: Event name
            properties: Event properties
        """
        if not self.client:
            return

        url = f"{self.host}/capture/"
        payload = {
            "api_key": self.api_key,
            "event": event,
            "properties": properties or {},
        }

        try:
            response = await self.client.post(url, json=payload)
            response.raise_for_status()
        except Exception as e:
            logger.error(f"PostHog send failed: {e}", exc_info=True)
            raise

    async def shutdown(self) -> None:
        """Shutdown subscriber and close HTTP client."""
        if self.client:
            await self.client.aclose()
            self.client = None
