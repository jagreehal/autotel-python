"""Base class for streaming event subscribers (Kafka, Kinesis, Pub/Sub)."""

import logging
from abc import ABC, abstractmethod
from typing import Any

from .base import EventSubscriber

logger = logging.getLogger(__name__)


class StreamingEventSubscriber(EventSubscriber, ABC):
    """
    Base class for streaming event subscribers.

    Streaming subscribers send events in batches to message queues/streams like:
    - Apache Kafka
    - AWS Kinesis
    - Google Cloud Pub/Sub
    - Azure Event Hubs

    Example:
        >>> from autotel.subscribers import StreamingEventSubscriber
        >>>
        >>> class KafkaSubscriber(StreamingEventSubscriber):
        ...     async def send_batch(self, events: List[Dict[str, Any]]) -> None:
        ...         await self.producer.send_batch(events)
    """

    async def send(self, event: str, properties: dict[str, Any] | None = None) -> None:
        """
        Send single event (default implementation batches).

        For streaming subscribers, it's more efficient to batch events.
        This default implementation creates a single-event batch.
        Override `send_batch()` for better performance.
        """
        await self.send_batch(
            [
                {
                    "name": event,
                    "properties": properties or {},
                }
            ]
        )

    @abstractmethod
    async def send_batch(self, events: list[dict[str, Any]]) -> None:
        """
        Send batch of events to streaming platform.

        Args:
            events: List of event dictionaries, each with 'name' and 'properties' keys

        Example:
            >>> events = [
            ...     {"name": "user_created", "properties": {"user_id": "123"}},
            ...     {"name": "order_placed", "properties": {"order_id": "456"}},
            ... ]
            >>> await subscriber.send_batch(events)
        """
        pass

    async def shutdown(self) -> None:
        """Shutdown subscriber (override if needed for cleanup)."""
        pass
