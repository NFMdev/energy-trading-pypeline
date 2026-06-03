import pytest
from prometheus_client import CollectorRegistry, generate_latest

from energy_trading_pypeline.observability import prometheus
from energy_trading_pypeline.observability.prometheus.consumer_metrics import (
    NoOpConsumerMetrics,
    PrometheusConsumerMetrics,
    create_consumer_metrics,
)

pytestmark = pytest.mark.unit


def test_noop_consumer_metrics_does_nothing() -> None:
    metrics = NoOpConsumerMetrics()

    metrics.record_message_processed(
        outcome="valid",
        duration_seconds=0.123,
    )
    metrics.record_invalid_event_persisted()
    metrics.record_alerts_generated(count=3)


def test_create_consumer_metrics_returns_noop_when_disabled() -> None:
    metrics = create_consumer_metrics(enabled=False)

    assert isinstance(metrics, NoOpConsumerMetrics)


def test_create_consumer_metrics_registers_metrics_with_default_registry(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    registry = CollectorRegistry()
    monkeypatch.setattr(prometheus.consumer_metrics, "REGISTRY", registry)

    metrics = create_consumer_metrics(enabled=True)
    metrics.record_message_processed(
        outcome="valid",
        duration_seconds=0.123,
    )

    output = generate_latest(registry).decode("utf-8")

    assert (
        'energy_trading_pypeline_consumer_processed_messages_total{outcome="valid"} 1.0' in output
    )


def test_prometheus_consumer_metrics_records_valid_message() -> None:
    registry = CollectorRegistry()
    metrics = PrometheusConsumerMetrics.create(registry=registry)

    metrics.record_message_processed(
        outcome="valid",
        duration_seconds=0.123,
    )

    output = generate_latest(registry).decode("utf-8")

    assert (
        'energy_trading_pypeline_consumer_processed_messages_total{outcome="valid"} 1.0' in output
    )
    assert (
        'energy_trading_pypeline_consumer_processing_duration_seconds_count{outcome="valid"} 1.0'
        in output
    )
    assert (
        'energy_trading_pypeline_consumer_processing_duration_seconds_sum{outcome="valid"} 0.123'
        in output
    )


def test_prometheus_consumer_metrics_records_invalid_event_persisted() -> None:
    registry = CollectorRegistry()
    metrics = PrometheusConsumerMetrics.create(registry=registry)

    metrics.record_message_processed(
        outcome="invalid",
        duration_seconds=0.05,
    )
    metrics.record_invalid_event_persisted()

    output = generate_latest(registry).decode("utf-8")

    assert (
        'energy_trading_pypeline_consumer_processed_messages_total{outcome="invalid"} 1.0' in output
    )
    assert "energy_trading_pypeline_consumer_invalid_events_persisted_total 1.0" in output
