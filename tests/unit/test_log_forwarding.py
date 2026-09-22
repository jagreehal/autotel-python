"""forward_logging_to_otlp: bridges non-stdlib loggers, leaves app handlers alone."""

import logging

import pytest
import structlog
from opentelemetry.sdk._logs import LoggerProvider, LoggingHandler
from opentelemetry.sdk._logs.export import InMemoryLogExporter, SimpleLogRecordProcessor

from autotel.logging import forward_logging_to_otlp


def _provider() -> tuple[LoggerProvider, InMemoryLogExporter]:
    exporter = InMemoryLogExporter()
    provider = LoggerProvider()
    provider.add_log_record_processor(SimpleLogRecordProcessor(exporter))
    return provider, exporter


def _bodies(exporter: InMemoryLogExporter) -> list:
    return [d.log_record.body for d in exporter.get_finished_logs()]


@pytest.fixture(autouse=True)
def _reset():
    root = logging.getLogger()
    handlers, level = list(root.handlers), root.level
    yield
    root.handlers[:] = handlers
    root.setLevel(level)
    structlog.reset_defaults()


def test_default_structlog_events_are_forwarded():
    provider, exporter = _provider()
    log = structlog.get_logger()
    forward_logging_to_otlp(provider, log)

    log.info("order.placed", order_id=42)

    [data] = exporter.get_finished_logs()
    assert data.log_record.body == "order.placed"
    assert data.log_record.attributes["order_id"] == 42


def test_logger_bound_before_forwarding_is_forwarded():
    provider, exporter = _provider()
    log = structlog.get_logger().bind(request_id="r1")
    forward_logging_to_otlp(provider, log)

    log.info("bound early")

    assert _bodies(exporter) == ["bound early"]


def test_bound_logger_with_processor_tuple_is_forwarded():
    structlog.configure(processors=tuple(structlog.get_config()["processors"]))
    provider, exporter = _provider()
    log = structlog.get_logger().bind(request_id="r1")
    forward_logging_to_otlp(provider, log)

    log.info("tuple chain")

    assert _bodies(exporter) == ["tuple chain"]


def test_wrap_logger_with_its_own_processors_is_forwarded():
    provider, exporter = _provider()
    log = structlog.wrap_logger(
        structlog.PrintLogger(), processors=[structlog.processors.KeyValueRenderer()]
    )
    forward_logging_to_otlp(provider, log)

    log.info("own chain")

    assert _bodies(exporter) == ["own chain"]


def test_structlog_on_stdlib_is_not_forwarded_twice():
    provider, exporter = _provider()
    log = structlog.wrap_logger(logging.getLogger("svc"), processors=[lambda _l, _m, e: e["event"]])
    forward_logging_to_otlp(provider, log)

    log.info("once")

    assert _bodies(exporter) == ["once"]


def test_cached_logger_follows_reinit_to_the_new_provider():
    structlog.configure(cache_logger_on_first_use=True)
    first, first_exporter = _provider()
    second, second_exporter = _provider()
    log = structlog.get_logger()
    forward_logging_to_otlp(first, log)
    log.info("one")  # caches the assembled logger

    forward_logging_to_otlp(second, log)
    log.info("two")

    assert _bodies(first_exporter) == ["one"]
    assert _bodies(second_exporter) == ["two"]


def test_reinit_replaces_only_autotels_handlers():
    app_provider, app_exporter = _provider()
    app_handler = LoggingHandler(logger_provider=app_provider)
    logging.getLogger().addHandler(app_handler)

    first, first_exporter = _provider()
    second, second_exporter = _provider()
    log = structlog.get_logger()
    forward_logging_to_otlp(first, log)
    forward_logging_to_otlp(second, log)

    logging.getLogger("x").info("stdlib")
    log.info("structlog")

    assert app_handler in logging.getLogger().handlers
    assert _bodies(app_exporter) == ["stdlib"]
    assert _bodies(first_exporter) == []
    assert _bodies(second_exporter) == ["stdlib", "structlog"]  # no duplicates


def test_loguru_records_are_forwarded():
    loguru = pytest.importorskip("loguru")
    provider, exporter = _provider()
    forward_logging_to_otlp(provider, loguru.logger)
    try:
        loguru.logger.info("from loguru")
    finally:
        forward_logging_to_otlp(provider)  # removes the loguru sink

    assert "from loguru" in _bodies(exporter)
