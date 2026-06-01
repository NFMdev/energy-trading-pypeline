import pytest
from prometheus_client import CollectorRegistry, generate_latest

from energy_trading_pypeline.observability.prometheus.producer_metrics import (
    NoOpProducerMetrics,
    PrometheusProducerMetrics,
    create_producer_metrics,
)

pytestmark = pytest.mark.unit


def test_noop_producer_metrics_does_nothing() -> None:
    metrics = NoOpProducerMetrics()

    metrics.record_publish_success(
        market_area="DK1",
        duration_seconds=0.123,
        published_at_seconds=1_700_000_000.0,
    )
    metrics.record_publish_failure(error_type="KafkaException")


def test_create_producer_metrics_returns_noop_when_disabled() -> None:
    metrics = create_producer_metrics(enabled=False)

    assert isinstance(metrics, NoOpProducerMetrics)


def test_prometheus_producer_metrics_records_publish_success() -> None:
    registry = CollectorRegistry()
    metrics = PrometheusProducerMetrics.create(registry=registry)

    metrics.record_publish_success(
        market_area="DK1",
        duration_seconds=0.123,
        published_at_seconds=1_700_000_000.0,
    )

    output = generate_latest(registry).decode("utf-8")

    assert (
        'energy_trading_pypeline_producer_events_published_total{market_area="DK1"} 1.0' in output
    )
    assert "energy_trading_pypeline_producer_publish_duration_seconds_count 1.0" in output
    assert "energy_trading_pypeline_producer_publish_duration_seconds_sum 0.123" in output
    assert (
        "energy_trading_pypeline_producer_last_successful_publish_timestamp_seconds "
        "1.7e+09" in output
    )


def test_prometheus_producer_metrics_records_publish_failure() -> None:
    registry = CollectorRegistry()
    metrics = PrometheusProducerMetrics.create(registry=registry)

    metrics.record_publish_failure(error_type="KafkaException")

    output = generate_latest(registry).decode("utf-8")

    assert (
        'energy_trading_pypeline_producer_publish_failures_total{error_type="KafkaException"} 1.0'
        in output
    )
