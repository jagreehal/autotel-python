"""Slack event subscriber for sending events to Slack channels."""

import logging
from typing import Any

try:
    import httpx
except ImportError:
    httpx = None  # type: ignore[assignment]

from .base import EventSubscriber

logger = logging.getLogger(__name__)


class SlackSubscriber(EventSubscriber):
    """
    Slack event subscriber for sending events to Slack channels.

    Formats events as Slack messages and posts them to a Slack webhook URL.

    Example:
        >>> from autotel.subscribers import SlackSubscriber
        >>> subscriber = SlackSubscriber(
        ...     webhook_url="https://hooks.slack.com/services/...",
        ...     channel="#alerts"
        ... )
    """

    def __init__(
        self,
        webhook_url: str,
        channel: str | None = None,
        username: str = "autotel",
        icon_emoji: str = ":chart_with_upwards_trend:",
    ):
        """
        Initialize Slack subscriber.

        Args:
            webhook_url: Slack webhook URL (from Slack app settings)
            channel: Optional channel override (e.g., "#alerts")
            username: Bot username (default: "autotel")
            icon_emoji: Bot icon emoji (default: ":chart_with_upwards_trend:")
        """
        if httpx is None:
            raise ImportError(
                "httpx is required for SlackSubscriber. Install with: pip install httpx"
            )

        if not webhook_url:
            raise ValueError("webhook_url is required")

        self.webhook_url = webhook_url
        self.channel = channel
        self.username = username
        self.icon_emoji = icon_emoji
        self.client: Any = httpx.AsyncClient(timeout=5.0)

    async def send(self, event: str, properties: dict[str, Any] | None = None) -> None:
        """
        Send event to Slack as a formatted message.

        Args:
            event: Event name
            properties: Event properties
        """
        if not self.client:
            return

        # Format event as Slack message
        text = f"*{event}*"
        if properties:
            # Format properties as Slack fields
            fields = []
            for key, value in properties.items():
                # Truncate long values
                value_str = str(value)
                if len(value_str) > 200:
                    value_str = value_str[:200] + "..."
                fields.append(
                    {
                        "title": key,
                        "value": value_str,
                        "short": len(value_str) < 50,
                    }
                )

            # Build Slack message payload
            payload = {
                "text": text,
                "username": self.username,
                "icon_emoji": self.icon_emoji,
                "attachments": [
                    {
                        "color": "good",  # Green color for events
                        "fields": fields,
                    }
                ],
            }
        else:
            payload = {
                "text": text,
                "username": self.username,
                "icon_emoji": self.icon_emoji,
            }

        # Add channel override if specified
        if self.channel:
            payload["channel"] = self.channel

        try:
            response = await self.client.post(
                self.webhook_url,
                json=payload,
            )
            response.raise_for_status()
        except Exception as e:
            logger.error(f"Slack send failed: {e}", exc_info=True)
            raise

    async def shutdown(self) -> None:
        """Shutdown subscriber and close HTTP client."""
        if self.client:
            await self.client.aclose()
            self.client = None
