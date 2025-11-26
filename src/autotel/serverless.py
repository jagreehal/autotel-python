"""Auto-flush for serverless environments."""

import atexit
import logging
import os
from collections.abc import Callable

logger: logging.Logger | None = None


def _get_logger() -> logging.Logger:
    """Lazy import logger to avoid circular imports."""
    global logger
    if logger is None:
        logger = logging.getLogger(__name__)
    return logger


def is_serverless() -> bool:
    """
    Detect if running in a serverless environment.

    Checks for:
    - AWS Lambda (AWS_LAMBDA_FUNCTION_NAME)
    - Google Cloud Functions (FUNCTION_NAME)
    - Azure Functions (AZURE_FUNCTIONS_ENVIRONMENT)

    Returns:
        True if serverless environment detected
    """
    return bool(
        os.environ.get("AWS_LAMBDA_FUNCTION_NAME")
        or os.environ.get("FUNCTION_NAME")
        or os.environ.get("AZURE_FUNCTIONS_ENVIRONMENT")
    )


def register_auto_flush(flush_func: Callable[[], None]) -> None:
    """
    Register a flush function to be called on exit.

    Useful for serverless environments where we need to flush
    spans/metrics/events before the function exits.

    Args:
        flush_func: Function to call on exit

    Example:
        >>> register_auto_flush(lambda: shutdown_sync())
    """
    atexit.register(flush_func)
    _get_logger().info("Registered auto-flush for serverless environment")


def auto_flush_if_serverless(flush_func: Callable[[], None]) -> None:
    """
    Auto-register flush function if in serverless environment.

    Args:
        flush_func: Function to call on exit

    Example:
        >>> auto_flush_if_serverless(lambda: shutdown_sync())
    """
    if is_serverless():
        register_auto_flush(flush_func)
        _get_logger().info("Auto-detected serverless environment, registered flush")
