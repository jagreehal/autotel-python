"""Graceful shutdown for autotel."""

import asyncio
import logging

from opentelemetry.sdk._logs import LoggerProvider
from opentelemetry.sdk.metrics import MeterProvider

from .events import Event
from .metrics import Metric

logger = logging.getLogger(__name__)

_event_instance = None
_metrics_instance = None
_meter_provider = None
_logger_provider = None
_shutdown_complete = False


def set_event_for_shutdown(event: Event) -> None:
    """Set event instance for shutdown (internal use)."""
    global _event_instance
    _event_instance = event


def set_metrics_for_shutdown(metrics: Metric) -> None:
    """Set metrics instance for shutdown (internal use)."""
    global _metrics_instance
    _metrics_instance = metrics


def set_meter_provider_for_shutdown(provider: MeterProvider) -> None:
    """Set meter provider for shutdown (internal use)."""
    global _meter_provider
    _meter_provider = provider


def set_logger_provider_for_shutdown(provider: LoggerProvider) -> None:
    """Set logger provider for shutdown (internal use)."""
    global _logger_provider
    _logger_provider = provider


async def shutdown(timeout: float = 5.0) -> None:
    """
    Gracefully shutdown autotel.

    This function:
    1. Stops accepting new events
    2. Drains event queue (waits for pending sends)
    3. Shuts down event subscribers
    4. Flushes spans to exporter
    5. Cleans up resources

    Args:
        timeout: Maximum time to wait for shutdown in seconds
    """
    global _shutdown_complete
    if _shutdown_complete:
        logger.info("autotel shutdown skipped (already completed)")
        return

    from opentelemetry import trace
    from opentelemetry.sdk.trace import TracerProvider

    # Shutdown events if initialized
    if _event_instance:
        try:
            await asyncio.wait_for(_event_instance.shutdown(), timeout=timeout)
        except asyncio.TimeoutError:
            logger.warning(f"Event shutdown timed out after {timeout}s")
        except Exception as e:
            logger.error(f"Error shutting down events: {e}", exc_info=True)

    # Shutdown metrics if initialized
    if _metrics_instance:
        try:
            await asyncio.wait_for(_metrics_instance.shutdown(), timeout=timeout)
        except asyncio.TimeoutError:
            logger.warning(f"Metrics shutdown timed out after {timeout}s")
        except Exception as e:
            logger.error(f"Error shutting down metrics: {e}", exc_info=True)

    if _meter_provider:
        try:
            _meter_provider.force_flush(timeout_millis=int(timeout * 1000))
            _meter_provider.shutdown()
        except Exception as e:
            logger.error(f"Error shutting down meter provider: {e}", exc_info=True)

    if _logger_provider:
        try:
            _logger_provider.force_flush(timeout_millis=int(timeout * 1000))
            _logger_provider.shutdown()  # type: ignore[no-untyped-call]
        except Exception as e:
            logger.error(f"Error shutting down logger provider: {e}", exc_info=True)

    # Flush spans
    provider = trace.get_tracer_provider()
    if isinstance(provider, TracerProvider):
        try:
            provider.force_flush(timeout_millis=int(timeout * 1000))
        except Exception as e:
            logger.error(f"Error flushing spans: {e}", exc_info=True)

    _shutdown_complete = True
    logger.info("autotel shutdown complete")


def shutdown_sync(timeout: float = 5.0) -> None:
    """
    Synchronous version of shutdown.

    Args:
        timeout: Maximum time to wait for shutdown in seconds
    """
    if _shutdown_complete:
        logger.info("autotel shutdown skipped (already completed)")
        return

    try:
        loop = asyncio.get_event_loop()
        if loop.is_running():
            # If loop is running, schedule shutdown
            asyncio.create_task(shutdown(timeout))
        else:
            # If no loop, run shutdown
            loop.run_until_complete(shutdown(timeout))
    except RuntimeError:
        # No event loop - create one
        asyncio.run(shutdown(timeout))
