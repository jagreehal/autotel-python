"""
Bring Your Own Logger - Automatic trace context injection.

autotel automatically instruments your logger to inject trace context
(trace_id, span_id, operation.name) into log records.

Supported loggers:
- Python standard logging
- structlog
- loguru (coming soon)
"""

import logging
from typing import Any, Protocol

from opentelemetry import trace


class Logger(Protocol):
    """
    Logger protocol for type hints.

    Any logger with these methods can be passed to init().
    """

    def info(self, msg: str, *args: Any, **kwargs: Any) -> None:
        """Log info message."""
        ...

    def warning(self, msg: str, *args: Any, **kwargs: Any) -> None:
        """Log warning message."""
        ...

    def error(self, msg: str, *args: Any, **kwargs: Any) -> None:
        """Log error message."""
        ...

    def debug(self, msg: str, *args: Any, **kwargs: Any) -> None:
        """Log debug message."""
        ...


def instrument_logger(logger: Any) -> None:
    """
    Instrument a logger to automatically inject trace context.

    Detects logger type and applies appropriate instrumentation.
    Supports standard logging and structlog.

    Args:
        logger: Logger instance to instrument

    Example:
        >>> import logging
        >>> logger = logging.getLogger(__name__)
        >>> from autotel import init
        >>> init(service="my-app", logger=logger)
        >>> # Logger now automatically includes trace_id and span_id
    """
    # Detect structlog
    if _is_structlog(logger):
        _instrument_structlog()
        return

    # Detect loguru
    if _is_loguru(logger):
        _instrument_loguru(logger)
        return

    # Default to standard logging
    _instrument_standard_logging()


def _is_structlog(logger: Any) -> bool:
    """Check if logger is structlog."""
    # structlog logger has these specific attributes
    return hasattr(logger, "_context") and hasattr(logger, "_processors")


def _is_loguru(logger: Any) -> bool:
    """Check if logger is loguru."""
    # loguru logger has 'add' and 'remove' methods and is module-level
    return hasattr(logger, "add") and hasattr(logger, "remove") and hasattr(logger, "configure")


def _instrument_standard_logging() -> None:
    """Instrument standard Python logging to inject trace context."""
    old_factory = logging.getLogRecordFactory()

    def record_factory(*args: Any, **kwargs: Any) -> logging.LogRecord:
        record = old_factory(*args, **kwargs)

        # Inject trace context (dynamic attributes for logging)
        span = trace.get_current_span()
        if span and span.is_recording():
            span_context = span.get_span_context()
            record.trace_id = format(span_context.trace_id, "032x")
            record.span_id = format(span_context.span_id, "016x")

            # Add operation name if available
            try:
                if hasattr(span, "name"):
                    record.operation_name = span.name
            except Exception:
                pass
        else:
            record.trace_id = None
            record.span_id = None
            record.operation_name = None

        return record

    logging.setLogRecordFactory(record_factory)


def _instrument_structlog() -> None:
    """Instrument structlog to inject trace context."""
    try:
        import structlog
    except ImportError as e:
        raise ImportError(
            "structlog is required for structlog integration. "
            "Install with: pip install autotel[logging]"
        ) from e

    def add_trace_context(
        _logger: Any, _method_name: str, event_dict: dict[str, Any]
    ) -> dict[str, Any]:
        """Add trace context to structlog event dict."""
        span = trace.get_current_span()
        if span and span.is_recording():
            span_context = span.get_span_context()
            event_dict["trace_id"] = format(span_context.trace_id, "032x")
            event_dict["span_id"] = format(span_context.span_id, "016x")

            # Add operation name if available
            try:
                if hasattr(span, "name"):
                    event_dict["operation.name"] = span.name
            except Exception:
                pass  # Graceful degradation

        return event_dict

    # Get existing processors or use defaults
    current_config = structlog.get_config()
    existing_processors = list(current_config.get("processors", []))

    # Insert trace context processor after contextvars merge if it exists
    insert_index = 0
    for i, processor in enumerate(existing_processors):
        processor_name = (
            processor.__class__.__name__ if hasattr(processor, "__class__") else str(processor)
        )
        if "contextvars" in processor_name.lower():
            insert_index = i + 1
            break

    # Add our processor if not already present
    if add_trace_context not in existing_processors:
        existing_processors.insert(insert_index, add_trace_context)

    # Reconfigure with trace context processor
    structlog.configure(processors=existing_processors)


def _instrument_loguru(logger: Any) -> None:
    """Instrument loguru to inject trace context."""
    # Loguru uses a global logger instance with custom formatting
    # We patch the logger's record with trace context

    def trace_context_patcher(record: dict[str, Any]) -> None:
        """Patch loguru record with trace context."""
        span = trace.get_current_span()
        if span and span.is_recording():
            span_context = span.get_span_context()
            record["extra"]["trace_id"] = format(span_context.trace_id, "032x")
            record["extra"]["span_id"] = format(span_context.span_id, "016x")

            try:
                if hasattr(span, "name"):
                    record["extra"]["operation_name"] = span.name
            except Exception:
                pass

    # Add patcher to existing handlers
    logger.configure(patcher=trace_context_patcher)
