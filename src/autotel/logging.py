"""
Bring Your Own Logger - Automatic trace context injection.

autotel instruments your logger to inject trace context (trace_id, span_id,
operation.name) into log records, and with ``logs=True`` exports those records
over OTLP.

Supported loggers:
- Python standard logging
- structlog
- loguru
"""

import json
import logging
import logging.handlers
import os
import sys
from collections.abc import Callable
from contextlib import suppress
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal, Protocol

from opentelemetry import trace

from .redaction import redact_pii_enabled, scrub_text


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


LogRotation = Literal["minutely", "hourly", "daily", "never"]
_ROTATION_WHEN = {"minutely": "M", "hourly": "H", "daily": "midnight"}


@dataclass(frozen=True)
class LogFile:
    """
    Rolling log files in ``directory``. Give each service its own directory.

    Writes happen on the logging thread. For a slow disk, add a QueueHandler.

    Args:
        directory: Created if missing.
        prefix: File name stem. Defaults to the service name.
        rotation: ``minutely``, ``hourly``, ``daily`` or ``never``.
        max_files: Files to keep: 2 or more, or 0 for all.
        json: Write JSON lines with ``trace_id`` and ``span_id``.
    """

    directory: str | Path
    prefix: str | None = None
    rotation: LogRotation = "daily"
    max_files: int = 7
    json: bool = False

    def __post_init__(self) -> None:
        if self.rotation != "never" and self.rotation not in _ROTATION_WHEN:
            raise ValueError(f"Unknown log rotation {self.rotation!r}")
        if self.max_files == 1 or self.max_files < 0:
            raise ValueError("max_files must be 2 or more, or 0 to keep all files")

    def handler(self, service: str) -> logging.Handler:
        directory = Path(self.directory)
        directory.mkdir(parents=True, exist_ok=True)
        path = directory / f"{self.prefix or service}.log"
        if self.rotation == "never":
            return logging.FileHandler(path, encoding="utf-8")
        return logging.handlers.TimedRotatingFileHandler(
            path,
            when=_ROTATION_WHEN[self.rotation],
            backupCount=max(self.max_files - 1, 0),
            encoding="utf-8",
        )


def log_file_from_env() -> LogFile | None:
    """Build a LogFile from ``AUTOTEL_LOG_FILE_{DIR,ROTATION,MAX_FILES}``."""
    directory = os.getenv("AUTOTEL_LOG_FILE_DIR")
    if not directory:
        return None
    rotation: Any = os.getenv("AUTOTEL_LOG_FILE_ROTATION", "daily").lower()
    return LogFile(
        directory, rotation=rotation, max_files=int(os.getenv("AUTOTEL_LOG_FILE_MAX_FILES", "7"))
    )


class JsonWithTraceIds(logging.Formatter):
    """One JSON object per record, with the active ``trace_id`` and ``span_id``."""

    def format(self, record: logging.LogRecord) -> str:
        entry: dict[str, Any] = {
            "timestamp": self.formatTime(record, "%Y-%m-%dT%H:%M:%S%z"),
            "level": record.levelname,
            "logger": record.name,
            "message": _message(record),
        }
        context = trace.get_current_span().get_span_context()
        if context.is_valid:
            entry["trace_id"] = format(context.trace_id, "032x")
            entry["span_id"] = format(context.span_id, "016x")
        if record.exc_info:
            entry["exception"] = _scrubbed(self.formatException(record.exc_info))
        return json.dumps(entry, default=str)


class _ScrubbingFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        return _scrubbed(super().format(record))


def _scrubbed(text: str) -> str:
    return scrub_text(text) if redact_pii_enabled() else text


def _message(record: logging.LogRecord) -> str:
    return _scrubbed(record.getMessage())


_output_handlers: list[logging.Handler] = []


def configure_log_output(service: str, log_file: LogFile | None, json_logs: bool) -> None:
    """Attach file and JSON stdout handlers to the root logger, replacing a previous init's."""
    root = logging.getLogger()
    while _output_handlers:
        handler = _output_handlers.pop()
        root.removeHandler(handler)
        handler.close()

    log_file = log_file or log_file_from_env()
    if log_file is not None:
        file_handler = log_file.handler(service)
        file_handler.setFormatter(
            JsonWithTraceIds()
            if log_file.json
            else _ScrubbingFormatter("%(asctime)s %(levelname)s %(name)s: %(message)s")
        )
        _output_handlers.append(file_handler)
    if json_logs:
        stdout_handler = logging.StreamHandler(sys.stdout)
        stdout_handler.setFormatter(JsonWithTraceIds())
        _output_handlers.append(stdout_handler)

    for handler in _output_handlers:
        root.addHandler(handler)
    # The root default of WARNING would drop info records before any handler sees them.
    if _output_handlers and root.level > logging.INFO:
        root.setLevel(logging.INFO)


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


# Undo callbacks for whatever the last forward_logging_to_otlp() installed, so a
# re-init touches only autotel's own handlers and never the application's.
_undo_forwarding: list[Callable[[], None]] = []


def forward_logging_to_otlp(logger_provider: Any, logger: Any | None = None) -> None:
    """
    Send log records to the OTLP logs pipeline.

    Attaches an OTel ``LoggingHandler`` so records written with plain
    ``logging`` reach the configured logger provider — and so Datadog Logs —
    already correlated with the active span. Applied to ``logger`` when it is a
    stdlib logger, otherwise to the root logger.

    structlog (default config) and loguru don't route through stdlib, so when
    one of those is passed it is bridged to the same handler.
    """
    from opentelemetry.sdk._logs import LoggingHandler

    # Replace what the previous init() installed; its provider is shut down.
    while _undo_forwarding:
        _undo_forwarding.pop()()

    handler = LoggingHandler(logger_provider=logger_provider)
    target = logger if isinstance(logger, logging.Logger) else logging.getLogger()
    target.addHandler(handler)
    _undo_forwarding.append(lambda: target.removeHandler(handler))
    # A logger left at WARNING (the root default) drops info records before any
    # handler sees them.
    if target.level == logging.NOTSET or target.level > logging.INFO:
        target.setLevel(logging.INFO)

    if logger is not None and _is_structlog(logger):
        _bridge_structlog(logger, handler)
    elif logger is not None and _is_loguru(logger):
        _bridge_loguru(logger, handler)


# The handler structlog events go to. The bridge processor reads it on every
# event, so loggers structlog cached or bound before a re-init follow the
# current provider.
_structlog_handler: logging.Handler | None = None


def _forward_structlog_event(
    logger: Any, method_name: str, event_dict: dict[str, Any]
) -> dict[str, Any]:
    handler = _structlog_handler
    if handler is None:
        return event_dict
    level = logging.getLevelName("ERROR" if method_name == "exception" else method_name.upper())
    record = logging.LogRecord(
        name=getattr(logger, "name", None) or "structlog",
        level=level if isinstance(level, int) else logging.INFO,
        pathname="",
        lineno=0,
        msg=event_dict.get("event", ""),
        args=None,
        exc_info=None,
    )
    for key, value in event_dict.items():
        if key != "event" and not hasattr(record, key):
            setattr(record, key, value)
    handler.handle(record)
    return event_dict


def _with_bridge(processors: Any) -> Any:
    """Return ``processors`` with the bridge just before the renderer, while the
    event is still a dict. Lists are changed in place - loggers already bound
    hold that same list - anything else is copied."""
    if _forward_structlog_event in processors:
        return processors
    if not isinstance(processors, list):
        processors = list(processors)
    processors.insert(max(len(processors) - 1, 0), _forward_structlog_event)
    return processors


def _bridge_structlog(logger: Any, handler: logging.Handler) -> None:
    """Hand structlog events to ``handler`` unless they already reach stdlib."""
    global _structlog_handler
    import structlog

    config = structlog.get_config()
    global_is_stdlib = isinstance(config["logger_factory"], structlog.stdlib.LoggerFactory)
    wrapped = getattr(logger, "_logger", None)  # None: a proxy that uses the factory
    if isinstance(wrapped, logging.Logger) or (wrapped is None and global_is_stdlib):
        return  # already lands on stdlib loggers, where the handler sees it

    # The global chain, for loggers created or bound from it...
    if not global_is_stdlib:
        processors = _with_bridge(config["processors"])
        if processors is not config["processors"]:
            structlog.configure(processors=processors)
    # ...and the supplied logger's own chain: a bound logger or one from
    # wrap_logger(processors=...) no longer reads the global one.
    own = getattr(logger, "_processors", None)
    if own is not None:
        processors = _with_bridge(own)
        if processors is not own:
            logger._processors = processors

    _structlog_handler = handler

    def undo() -> None:
        global _structlog_handler
        _structlog_handler = None

    _undo_forwarding.append(undo)


def _bridge_loguru(logger: Any, handler: logging.Handler) -> None:
    """Add ``handler`` as a loguru sink - loguru accepts stdlib handlers."""
    handler_id = logger.add(handler, format="{message}")

    def undo() -> None:
        with suppress(ValueError):  # the app may have called logger.remove()
            logger.remove(handler_id)

    _undo_forwarding.append(undo)
