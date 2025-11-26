"""Base classes for event subscribers."""

from abc import ABC, abstractmethod
from typing import Any


class EventSubscriber(ABC):
    """
    Base class for event subscribers.

    Event subscribers receive product events and forward them to external platforms
    like PostHog, Mixpanel, Amplitude, webhooks, or custom destinations.
    """

    @abstractmethod
    async def send(self, event: str, properties: dict[str, Any]) -> None:
        """
        Send event to destination.

        Args:
            event: Event name
            properties: Event properties
        """
        pass

    async def shutdown(self) -> None:  # noqa: B027
        """Shutdown subscriber (override if needed)."""
        pass
