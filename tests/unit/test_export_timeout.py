"""export_timeout wiring on init()."""

import sys
from typing import Any
from unittest.mock import MagicMock, patch

from autotel import init

# `autotel.init` the function shadows the submodule on the package
init_module = sys.modules["autotel.init"]


def _init_capturing_exporter_kwargs(**kwargs: Any) -> dict[str, Any]:
    captured: dict[str, Any] = {}

    class FakeHTTPExporter:
        def __init__(self, *args: Any, **exporter_kwargs: Any) -> None:
            captured.update(exporter_kwargs)

        def export(self, spans: Any) -> int:
            return 0

        def shutdown(self) -> None:
            return None

    with patch.object(init_module, "HTTPExporter", FakeHTTPExporter):
        init(
            service="timeout-test",
            endpoint="http://localhost:4318",
            metrics=False,
            logs=False,
            **kwargs,
        )
    return captured


def test_simple_mode_defaults_export_timeout_to_two_seconds() -> None:
    captured = _init_capturing_exporter_kwargs(span_processor_mode="simple")
    assert captured.get("timeout") == 2.0


def test_explicit_export_timeout_is_passed() -> None:
    captured = _init_capturing_exporter_kwargs(span_processor_mode="batch", export_timeout=1.5)
    assert captured.get("timeout") == 1.5


def test_batch_mode_without_export_timeout_leaves_sdk_default() -> None:
    fake_bsp = MagicMock()
    with patch.object(init_module, "BatchSpanProcessor", fake_bsp):
        captured = _init_capturing_exporter_kwargs(span_processor_mode="batch")

    assert "timeout" not in captured
    _, kwargs = fake_bsp.call_args
    assert "export_timeout_millis" not in kwargs
