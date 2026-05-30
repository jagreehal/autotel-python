"""Tests for debug-mode resolution.

Debug/console output is opt-in: a plain ``init()`` must not print spans to the
console. Users enable it explicitly with ``debug=True``.
"""

import autotel.debug as debug_mod
from autotel.debug import should_enable_debug


def test_debug_off_by_default() -> None:
    """debug=None (the default) must NOT enable console output."""
    assert should_enable_debug(None) is False


def test_debug_explicit_true_enables() -> None:
    assert should_enable_debug(True) is True


def test_debug_explicit_false_disables() -> None:
    assert should_enable_debug(False) is False


def test_debug_default_independent_of_environment(monkeypatch) -> None:
    """Auto-on-in-non-prod is gone: the environment must not flip the default."""
    monkeypatch.delenv("ENVIRONMENT", raising=False)
    assert should_enable_debug(None) is False
    monkeypatch.setenv("ENVIRONMENT", "development")
    assert should_enable_debug(None) is False
    monkeypatch.setenv("ENVIRONMENT", "production")
    assert should_enable_debug(None) is False


def test_no_debug_printer_after_plain_init() -> None:
    """A default init() must not leave an enabled debug printer."""
    debug_mod.set_debug_printer(None)  # reset any global state
    from autotel import init

    init(service="test-no-debug")
    printer = debug_mod.get_debug_printer()
    assert printer is None or printer.enabled is False
