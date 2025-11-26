"""Operation context for auto-capturing operation names in events events."""

import logging
from contextlib import contextmanager
from contextvars import ContextVar
from typing import Any

logger = logging.getLogger(__name__)

# Internal: ContextVar for operation name
_operation_context: ContextVar[str | None] = ContextVar("operation", default=None)


@contextmanager
def run_in_operation_context(operation_name: str) -> Any:
    """
    Store operation name in async context.

    Automatically captured in events events as 'operation.name'.

    Example:
        >>> with run_in_operation_context("user.create"):
        ...     track("user_created", {"user_id": "123"})
        ...     # Analytics event will include operation.name="user.create"
    """
    token = _operation_context.set(operation_name)
    try:
        yield
    finally:
        _operation_context.reset(token)


def get_operation_context() -> str | None:
    """
    Get current operation name from context.

    Returns:
        Operation name if set, None otherwise

    Example:
        >>> operation = get_operation_context()
        >>> if operation:
        ...     print(f"Current operation: {operation}")
    """
    return _operation_context.get()


def set_operation_context(operation_name: str) -> None:
    """
    Set operation name in context (internal use).

    Used by @trace decorator to auto-set operation context.
    """
    _operation_context.set(operation_name)
