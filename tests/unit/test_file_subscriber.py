"""FileSubscriber NDJSON sink."""

from pathlib import Path

from autotel import FileSubscriber, init, track
from autotel.exporters import InMemorySpanExporter
from autotel.processors import SimpleSpanProcessor


def test_file_subscriber_writes_ndjson(tmp_path: Path) -> None:
    path = tmp_path / "events.ndjson"
    init(
        service="test-file-sub",
        span_processor=SimpleSpanProcessor(InMemorySpanExporter()),
        metrics=False,
        logs=False,
        subscribers=[FileSubscriber(path)],
    )
    track("atm.cash_withdrawn", {"atm.branch": "bridge-street", "amount": 40})
    text = path.read_text(encoding="utf-8").strip()
    assert "atm.cash_withdrawn" in text
    assert "bridge-street" in text


def test_no_loop_keeps_async_subscribers_and_writes_file_once(tmp_path: Path) -> None:
    from typing import Any

    from autotel import EventSubscriberBase, shutdown_sync

    class Recording(EventSubscriberBase):
        def __init__(self) -> None:
            self.events: list[str] = []

        async def send(self, event: str, properties: dict[str, Any]) -> None:
            self.events.append(event)

    path = tmp_path / "events.ndjson"
    recording = Recording()
    init(
        service="test-file-sub",
        span_processor=SimpleSpanProcessor(InMemorySpanExporter()),
        subscribers=[FileSubscriber(path), recording],
    )
    track("order.placed", {"id": 1})
    shutdown_sync(timeout=1.0)

    assert recording.events == ["order.placed"]
    assert len(path.read_text(encoding="utf-8").splitlines()) == 1
