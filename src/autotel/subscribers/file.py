"""Append tracked events to a newline-delimited JSON file."""

from __future__ import annotations

import json
import threading
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .base import EventSubscriber


class FileSubscriber(EventSubscriber):
    """
    Write each ``track()`` event as one NDJSON line.

    Useful for local demos, notebooks, and agent tooling without a hosted sink.
    """

    def __init__(
        self,
        path: str | Path,
        *,
        enabled: bool = True,
    ) -> None:
        self._path = Path(path)
        self._enabled = enabled
        self._lock = threading.Lock()

    def send_sync(self, event: str, properties: dict[str, Any]) -> None:
        """Write one NDJSON line. Works without an event loop."""
        if not self._enabled:
            return
        payload = {
            "event": event,
            "properties": properties,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }
        line = json.dumps(payload, separators=(",", ":"), default=str)
        with self._lock:
            self._path.parent.mkdir(parents=True, exist_ok=True)
            with self._path.open("a", encoding="utf-8") as handle:
                handle.write(line + "\n")

    async def send(self, event: str, properties: dict[str, Any]) -> None:
        self.send_sync(event, properties)
