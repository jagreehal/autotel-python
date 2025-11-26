"""Enhanced metrics module with helpers and auto-export."""

import logging
from collections.abc import Callable
from typing import Any

_OTEL_METRICS_AVAILABLE = False
try:
    from opentelemetry import metrics as otel_metrics
    from opentelemetry.exporter.otlp.proto.http.metric_exporter import OTLPMetricExporter
    from opentelemetry.metrics import (
        CallbackOptions,
        Counter,
        Histogram,
        Meter,
        ObservableGauge,
        Observation,
        UpDownCounter,
    )
    from opentelemetry.sdk.metrics import MeterProvider
    from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader

    _OTEL_METRICS_AVAILABLE = True
except ImportError:
    # Fallback type definitions for when OpenTelemetry is not installed
    otel_metrics = None  # type: ignore[assignment]
    Counter = type("Counter", (), {})  # type: ignore[assignment, misc]
    Histogram = type("Histogram", (), {})  # type: ignore[assignment, misc]
    Meter = type("Meter", (), {})  # type: ignore[assignment, misc]
    ObservableGauge = type("ObservableGauge", (), {})  # type: ignore[assignment, misc]
    UpDownCounter = type("UpDownCounter", (), {})  # type: ignore[assignment, misc]
    MeterProvider = type("MeterProvider", (), {})  # type: ignore[assignment, misc]
    PeriodicExportingMetricReader = type("PeriodicExportingMetricReader", (), {})  # type: ignore[assignment, misc]
    OTLPMetricExporter = type("OTLPMetricExporter", (), {})  # type: ignore[assignment, misc]
    CallbackOptions = type("CallbackOptions", (), {})  # type: ignore[assignment, misc]
    Observation = type("Observation", (), {})  # type: ignore[assignment, misc]

logger = logging.getLogger(__name__)


class Metric:
    """
    Metric class for OpenTelemetry metrics with auto-export to OTLP.

    Note: In the Node.js version, there are two separate classes:
    - `Metric.trackEvent()` → sends to OTLP (OpenTelemetry metrics)
    - `Event.trackEvent()` → sends to subscribers (PostHog, etc.)

    This Python class handles the Metric side (OTLP).
    For product events to subscribers, use the Event class from events.py
    """

    def __init__(
        self,
        namespace: str | None = None,
        *,
        endpoint: str = "http://localhost:4318",
        export_interval: int = 60,
        headers: dict[str, str] | None = None,
    ):
        """
        Initialize metrics system.

        Args:
            namespace: Metric namespace (e.g., 'checkout', 'user-service')
            endpoint: OTLP endpoint for metrics export
            export_interval: Export interval in seconds
            headers: Optional HTTP headers
        """
        if otel_metrics is None:
            raise ImportError(
                "OpenTelemetry metrics SDK not available. "
                "Install with: pip install opentelemetry-sdk"
            )

        self.namespace = namespace or "default"
        self.endpoint = endpoint
        self.export_interval = export_interval
        self.headers = headers or {}
        self._meter_provider: MeterProvider | None = None
        self._meter: Meter | None = None
        self._counters: dict[str, Counter] = {}
        self._histograms: dict[str, Histogram] = {}

    def _setup(self) -> None:
        """Set up meter provider and meter."""
        if self._meter_provider is not None:
            return

        current_provider = otel_metrics.get_meter_provider()
        if _OTEL_METRICS_AVAILABLE and isinstance(current_provider, MeterProvider):
            # Reuse existing provider if one has already been configured
            self._meter_provider = current_provider
            self._meter = otel_metrics.get_meter(__name__)
            return

        # Create OTLP exporter
        exporter = OTLPMetricExporter(
            endpoint=f"{self.endpoint}/v1/metrics",
            headers=self.headers,
        )

        # Create metric reader with periodic export
        reader = PeriodicExportingMetricReader(
            exporter,
            export_interval_millis=self.export_interval * 1000,
        )

        # Create meter provider
        self._meter_provider = MeterProvider(metric_readers=[reader])
        otel_metrics.set_meter_provider(self._meter_provider)

        # Get meter
        self._meter = otel_metrics.get_meter(__name__)

    def get_meter(self, name: str | None = None) -> Meter:  # noqa: ARG002
        """
        Get meter instance.

        Args:
            name: Meter name (defaults to __name__)

        Returns:
            Meter instance
        """
        self._setup()
        if self._meter is None:
            raise RuntimeError("Metrics not initialized")
        return self._meter

    def trackEvent(self, event: str, properties: dict[str, Any] | None = None) -> None:
        """
        Track an event as an OpenTelemetry counter (sends to OTLP).

        This is different from Event.trackEvent() which sends to subscribers.
        This method emits OpenTelemetry metrics via OTLP.

        Args:
            event: Event name (e.g., 'order.completed')
            properties: Event properties to use as metric attributes

        Example:
            >>> metric = Metric('checkout')
            >>> metric.trackEvent('order.completed', {'orderId': '123', 'amount': 99.99})
        """
        self._setup()

        # Create or get counter for this event
        counter_name = f"{self.namespace}.{event}"
        if counter_name not in self._counters:
            meter = self.get_meter()
            self._counters[counter_name] = meter.create_counter(
                counter_name,
                description=f"Count of {event} events",
            )

        # Record the event
        counter = self._counters[counter_name]
        counter.add(1, attributes=properties or {})

    def trackValue(
        self, metric_name: str, value: float, properties: dict[str, Any] | None = None
    ) -> None:
        """
        Track a numeric value as an OpenTelemetry histogram (sends to OTLP).

        Args:
            metric_name: Metric name (e.g., 'revenue', 'response_time')
            value: Numeric value to record
            properties: Additional attributes

        Example:
            >>> metric = Metric('checkout')
            >>> metric.trackValue('revenue', 99.99, {'currency': 'USD'})
        """
        self._setup()

        # Create or get histogram for this metric
        histogram_name = f"{self.namespace}.{metric_name}"
        if histogram_name not in self._histograms:
            meter = self.get_meter()
            self._histograms[histogram_name] = meter.create_histogram(
                histogram_name,
                description=f"Distribution of {metric_name}",
            )

        # Record the value
        histogram = self._histograms[histogram_name]
        histogram.record(value, attributes=properties or {})

    async def shutdown(self) -> None:
        """Shutdown metrics system and flush pending metrics."""
        if self._meter_provider:
            self._meter_provider.shutdown()
            self._meter_provider = None
            self._meter = None


# Global metrics instance
_global_metrics: Metric | None = None


def set_metrics(metrics: Metric | None) -> None:
    """Set global metrics instance."""
    global _global_metrics
    _global_metrics = metrics


def get_metrics() -> Metric | None:
    """Get global metrics instance."""
    return _global_metrics


def create_counter(
    name: str,
    *,
    description: str | None = None,
    unit: str = "",
) -> Counter:
    """
    Create a counter metric.

    Args:
        name: Metric name
        description: Metric description
        unit: Metric unit

    Returns:
        Counter instance

    Example:
        >>> counter = create_counter("http.requests", description="Total HTTP requests")
        >>> counter.add(1, {"method": "GET"})
    """
    if _global_metrics is None:
        raise RuntimeError("Metrics not initialized. Call init() with metrics parameter.")
    meter = _global_metrics.get_meter()
    return meter.create_counter(name, description=description or "", unit=unit)


def create_histogram(
    name: str,
    *,
    description: str | None = None,
    unit: str = "",
) -> Histogram:
    """
    Create a histogram metric.

    Args:
        name: Metric name
        description: Metric description
        unit: Metric unit

    Returns:
        Histogram instance

    Example:
        >>> histogram = create_histogram("http.duration", description="Request duration", unit="ms")
        >>> histogram.record(150.5, {"method": "GET"})
    """
    if _global_metrics is None:
        raise RuntimeError("Metrics not initialized. Call init() with metrics parameter.")
    meter = _global_metrics.get_meter()
    return meter.create_histogram(name, description=description or "", unit=unit)


def create_up_down_counter(
    name: str,
    *,
    description: str | None = None,
    unit: str = "",
) -> UpDownCounter:
    """
    Create an up-down counter metric.

    Args:
        name: Metric name
        description: Metric description
        unit: Metric unit

    Returns:
        UpDownCounter instance

    Example:
        >>> counter = create_up_down_counter("active.connections", description="Active connections")
        >>> counter.add(1)
        >>> counter.add(-1)
    """
    if _global_metrics is None:
        raise RuntimeError("Metrics not initialized. Call init() with metrics parameter.")
    meter = _global_metrics.get_meter()
    return meter.create_up_down_counter(name, description=description or "", unit=unit)


def create_observable_gauge(
    name: str,
    *,
    description: str | None = None,
    unit: str = "",
    callback: Callable[[], float] | None = None,
) -> ObservableGauge:
    """
    Create an observable gauge metric.

    Args:
        name: Metric name
        description: Metric description
        unit: Metric unit
        callback: Callback function to get current value

    Returns:
        ObservableGauge instance

    Example:
        >>> def get_memory_usage():
        ...     return psutil.virtual_memory().used
        >>> gauge = create_observable_gauge("memory.used", callback=get_memory_usage)
    """
    if _global_metrics is None:
        raise RuntimeError("Metrics not initialized. Call init() with metrics parameter.")
    meter = _global_metrics.get_meter()

    def callback_wrapper(options: CallbackOptions) -> list[Observation]:  # noqa: ARG001
        if callback:
            return [Observation(value=callback())]
        return []

    return meter.create_observable_gauge(
        name,
        description=description or "",
        unit=unit,
        callbacks=[callback_wrapper] if callback else [],
    )


class MetricsCollector:
    """
    Metrics collector for testing.

    Captures metrics for assertions in tests.
    """

    def __init__(self) -> None:
        """Initialize metrics collector."""
        self._counters: dict[str, list[dict[str, Any]]] = {}
        self._histograms: dict[str, list[dict[str, Any]]] = {}
        self._up_down_counters: dict[str, list[dict[str, Any]]] = {}

    def record_counter(
        self, name: str, value: float, attributes: dict[str, Any] | None = None
    ) -> None:
        """Record a counter increment."""
        if name not in self._counters:
            self._counters[name] = []
        record: dict[str, Any] = {"value": value, "attributes": attributes or {}}
        self._counters[name].append(record)

    def record_histogram(
        self, name: str, value: float, attributes: dict[str, Any] | None = None
    ) -> None:
        """Record a histogram value."""
        if name not in self._histograms:
            self._histograms[name] = []
        record: dict[str, Any] = {"value": value, "attributes": attributes or {}}
        self._histograms[name].append(record)

    def get_counter_total(self, name: str) -> float:
        """Get total counter value."""
        if name not in self._counters:
            return 0.0
        total: float = sum(float(record["value"]) for record in self._counters[name])
        return total

    def get_histogram_values(self, name: str) -> list[float]:
        """Get all histogram values."""
        if name not in self._histograms:
            return []
        return [record["value"] for record in self._histograms[name]]
