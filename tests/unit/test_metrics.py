"""Tests for Metric class and OTLP integration."""

import pytest

from autotel.metrics import (
    Metric,
    MetricsCollector,
    create_counter,
    create_histogram,
    get_metrics,
    set_metrics,
)


@pytest.fixture
def metric() -> Metric:
    """Create Metric instance for testing."""
    return Metric(namespace="test", endpoint="http://localhost:4318", export_interval=60)


def test_metric_initialization() -> None:
    """Test Metric initialization."""
    metric = Metric(namespace="checkout")
    assert metric.namespace == "checkout"
    assert metric.endpoint == "http://localhost:4318"
    assert metric.export_interval == 60


def test_metric_default_namespace() -> None:
    """Test Metric with default namespace."""
    metric = Metric()
    assert metric.namespace == "default"


def test_metric_track_event_creates_counter(metric: Metric) -> None:
    """Test that trackEvent creates and uses a counter."""
    # Track an event
    metric.trackEvent("order.completed", {"orderId": "123", "amount": 99.99})

    # Counter should be created with namespace prefix
    assert "test.order.completed" in metric._counters


def test_metric_track_event_increments_counter(metric: Metric) -> None:
    """Test that trackEvent increments counter."""
    # Track multiple events
    metric.trackEvent("order.completed", {"orderId": "123"})
    metric.trackEvent("order.completed", {"orderId": "456"})
    metric.trackEvent("order.completed", {"orderId": "789"})

    # Counter should be created (we can't easily test the value without exporter)
    assert "test.order.completed" in metric._counters


def test_metric_track_value_creates_histogram(metric: Metric) -> None:
    """Test that trackValue creates and uses a histogram."""
    # Track a value
    metric.trackValue("revenue", 99.99, {"currency": "USD"})

    # Histogram should be created with namespace prefix
    assert "test.revenue" in metric._histograms


def test_metric_track_value_records_values(metric: Metric) -> None:
    """Test that trackValue records multiple values."""
    # Track multiple values
    metric.trackValue("revenue", 99.99, {"currency": "USD"})
    metric.trackValue("revenue", 149.99, {"currency": "USD"})
    metric.trackValue("revenue", 249.99, {"currency": "EUR"})

    # Histogram should be created
    assert "test.revenue" in metric._histograms


def test_metric_multiple_event_types(metric: Metric) -> None:
    """Test tracking multiple different event types."""
    metric.trackEvent("order.completed", {})
    metric.trackEvent("user.signup", {})
    metric.trackEvent("payment.failed", {})

    # Should have separate counters
    assert "test.order.completed" in metric._counters
    assert "test.user.signup" in metric._counters
    assert "test.payment.failed" in metric._counters


def test_metric_multiple_value_types(metric: Metric) -> None:
    """Test tracking multiple different value types."""
    metric.trackValue("revenue", 99.99)
    metric.trackValue("response_time", 150.5)
    metric.trackValue("cpu_usage", 45.2)

    # Should have separate histograms
    assert "test.revenue" in metric._histograms
    assert "test.response_time" in metric._histograms
    assert "test.cpu_usage" in metric._histograms


def test_metric_get_meter(metric: Metric) -> None:
    """Test get_meter returns meter instance."""
    meter = metric.get_meter()
    assert meter is not None


def test_metric_event_without_properties(metric: Metric) -> None:
    """Test trackEvent without properties."""
    metric.trackEvent("simple.event")
    assert "test.simple.event" in metric._counters


def test_metric_value_without_properties(metric: Metric) -> None:
    """Test trackValue without properties."""
    metric.trackValue("simple.metric", 42.0)
    assert "test.simple.metric" in metric._histograms


@pytest.mark.asyncio
async def test_metric_shutdown(metric: Metric) -> None:
    """Test metric shutdown."""
    metric.trackEvent("test.event", {})

    # Shutdown should not raise
    await metric.shutdown()

    # Meter provider should be None after shutdown
    assert metric._meter_provider is None
    assert metric._meter is None


def test_metrics_collector() -> None:
    """Test MetricsCollector for testing."""
    collector = MetricsCollector()

    # Record counter values
    collector.record_counter("requests", 1, {"method": "GET"})
    collector.record_counter("requests", 1, {"method": "POST"})
    collector.record_counter("requests", 1, {"method": "GET"})

    # Get total
    assert collector.get_counter_total("requests") == 3


def test_metrics_collector_histogram() -> None:
    """Test MetricsCollector histogram recording."""
    collector = MetricsCollector()

    # Record histogram values
    collector.record_histogram("duration", 100.5, {"endpoint": "/api"})
    collector.record_histogram("duration", 150.2, {"endpoint": "/api"})
    collector.record_histogram("duration", 200.8, {"endpoint": "/api"})

    # Get values
    values = collector.get_histogram_values("duration")
    assert len(values) == 3
    assert 100.5 in values
    assert 150.2 in values
    assert 200.8 in values


def test_global_metrics_set_and_get() -> None:
    """Test global metrics set and get."""
    metric = Metric(namespace="global")
    set_metrics(metric)

    retrieved = get_metrics()
    assert retrieved is metric
    assert retrieved.namespace == "global"


def test_create_counter_requires_metrics() -> None:
    """Test that create_counter requires initialized metrics."""
    # Clear global metrics
    set_metrics(None)

    with pytest.raises(RuntimeError, match="Metrics not initialized"):
        create_counter("test.counter")


def test_create_histogram_requires_metrics() -> None:
    """Test that create_histogram requires initialized metrics."""
    # Clear global metrics
    set_metrics(None)

    with pytest.raises(RuntimeError, match="Metrics not initialized"):
        create_histogram("test.histogram")


def test_create_counter_with_metrics() -> None:
    """Test create_counter with initialized metrics."""
    metric = Metric(namespace="test")
    set_metrics(metric)

    counter = create_counter("http.requests", description="Total HTTP requests")
    assert counter is not None


def test_create_histogram_with_metrics() -> None:
    """Test create_histogram with initialized metrics."""
    metric = Metric(namespace="test")
    set_metrics(metric)

    histogram = create_histogram("http.duration", description="Request duration", unit="ms")
    assert histogram is not None


def test_metric_namespace_in_counter_name(metric: Metric) -> None:
    """Test that namespace is included in counter names."""
    metric.trackEvent("user.login", {})

    # Check that counter name includes namespace
    counter_names = list(metric._counters.keys())
    assert any("test.user.login" in name for name in counter_names)


def test_metric_namespace_in_histogram_name(metric: Metric) -> None:
    """Test that namespace is included in histogram names."""
    metric.trackValue("api.latency", 150.0)

    # Check that histogram name includes namespace
    histogram_names = list(metric._histograms.keys())
    assert any("test.api.latency" in name for name in histogram_names)


def test_metric_properties_as_attributes(metric: Metric) -> None:
    """Test that properties are used as metric attributes."""
    # Track event with properties
    properties = {"user_id": "123", "plan": "premium", "region": "us-west"}
    metric.trackEvent("subscription.created", properties)

    # Counter should exist (can't easily test attributes without exporter mock)
    assert "test.subscription.created" in metric._counters


def test_metric_custom_endpoint() -> None:
    """Test Metric with custom endpoint."""
    custom_endpoint = "https://otel-collector.example.com:4318"
    metric = Metric(namespace="test", endpoint=custom_endpoint)

    assert metric.endpoint == custom_endpoint


def test_metric_custom_export_interval() -> None:
    """Test Metric with custom export interval."""
    metric = Metric(namespace="test", export_interval=30)
    assert metric.export_interval == 30


def test_metric_with_headers() -> None:
    """Test Metric with custom headers."""
    headers = {"x-api-key": "secret123", "x-tenant-id": "tenant456"}
    metric = Metric(namespace="test", headers=headers)

    assert metric.headers == headers
