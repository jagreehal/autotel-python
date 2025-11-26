"""Webhook event subscriber for sending events to custom webhooks."""

import logging
from typing import Any

try:
    import httpx
except ImportError:
    httpx = None  # type: ignore[assignment]

from .base import EventSubscriber

logger = logging.getLogger(__name__)


class WebhookSubscriber(EventSubscriber):
    """
    Webhook event subscriber for sending events to custom webhooks.

    Useful for integrating with Zapier, Make.com, custom APIs, or any webhook endpoint.

    Example:
        >>> from autotel.subscribers import WebhookSubscriber
        >>> subscriber = WebhookSubscriber(
        ...     webhook_url="https://hooks.example.com/webhook",
        ...     headers={"Authorization": "Bearer token"}
        ... )
    """

    def __init__(
        self,
        webhook_url: str,
        headers: dict[str, str] | None = None,
        timeout: float = 5.0,
    ):
        """
        Initialize webhook subscriber.

        Args:
            webhook_url: Webhook endpoint URL
            headers: Optional HTTP headers (e.g., Authorization)
            timeout: Request timeout in seconds
        """
        if httpx is None:
            raise ImportError(
                "httpx is required for WebhookSubscriber. Install with: pip install httpx"
            )

        if not webhook_url:
            raise ValueError("webhook_url is required")

        self.webhook_url = webhook_url.rstrip("/")
        self.headers = headers or {}
        self.timeout = timeout
        self.client: Any = httpx.AsyncClient(timeout=timeout)

    async def send(self, event: str, properties: dict[str, Any] | None = None) -> None:
        """
        Send event to webhook endpoint.

        Args:
            event: Event name
            properties: Event properties
        """
        if not self.client:
            return

        payload = {
            "event": event,
            "properties": properties or {},
        }

        try:
            response = await self.client.post(
                self.webhook_url,
                json=payload,
                headers=self.headers,
            )
            response.raise_for_status()
        except Exception as e:
            logger.error(f"Webhook send failed: {e}", exc_info=True)
            raise

    async def shutdown(self) -> None:
        """Shutdown subscriber and close HTTP client."""
        if self.client:
            await self.client.aclose()
            self.client = None
