from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Protocol, Self

from prometheus_client import CollectorRegistry, Counter, Gauge, Histogram

METRICS_NAMESPACE = "energy_trading_pypeline"
PRODUCER_METRICS_SYBSYSTEM = "producer"


class ProducerMetrics(Protocol):
    def record_publish_success(
        self,
        *,
        market_area: str,
        duration_seconds: float,
        published_at_seconds: float | None = None,
    ) -> None:
        """Record a successfully published producer event."""

    def record_publish_failure(self, *, error_type: str) -> None:
        """Record a producer publish failure."""


@dataclass(frozen=True)
class NoOpProducerMetrics:
    def record_publish_success(
        self,
        *,
        market_area: str,
        duration_seconds: float,
        published_at_seconds: float | None = None,
    ) -> None:
        return None

    def record_publish_failure(self, *, error_type: str) -> None:
        return None


@dataclass(frozen=True)
class PrometheusProducerMetrics:
    _events_published: Counter
    _publish_failures: Counter
    _publish_duration: Histogram
    _last_successful_publish_timestamp: Gauge

    @classmethod
    def create(
        cls,
        *,
        registry: CollectorRegistry | None = None,
    ) -> Self:
        return cls(
            _events_published=Counter(
                name="events_published",
                documentation="Total number of energy market events published by the producer.",
                labelnames=("market_area",),
                namespace=METRICS_NAMESPACE,
                subsystem=PRODUCER_METRICS_SYBSYSTEM,
                registry=registry,
            ),
            _publish_failures=Counter(
                name="publish_failures",
                documentation="Total number of producer publish failures.",
                labelnames=("error_type",),
                namespace=METRICS_NAMESPACE,
                subsystem=PRODUCER_METRICS_SYBSYSTEM,
                registry=registry,
            ),
            _publish_duration=Histogram(
                name="publish_duration_seconds",
                documentation="Duration of producer publish operations in seconds",
                namespace=METRICS_NAMESPACE,
                subsystem=PRODUCER_METRICS_SYBSYSTEM,
                registry=registry,
                buckets=(0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0),
            ),
            _last_successful_publish_timestamp=Gauge(
                name="last_successful_publish_timestamp_seconds",
                documentation="Unix timestamp of the last successfully published producer event.",
                namespace=METRICS_NAMESPACE,
                subsystem=PRODUCER_METRICS_SYBSYSTEM,
                registry=registry,
            ),
        )

    def record_publish_success(
        self,
        *,
        market_area: str,
        duration_seconds: float,
        published_at_seconds: float | None = None,
    ) -> None:
        timestamp = time.time() if published_at_seconds is None else published_at_seconds

        self._events_published.labels(market_area=market_area).inc()
        self._publish_duration.observe(duration_seconds)
        self._last_successful_publish_timestamp.set(timestamp)

    def record_publish_failure(self, *, error_type: str) -> None:
        self._publish_failures.labels(error_type=error_type).inc()


def create_producer_metrics(*, enabled: bool) -> ProducerMetrics | NoOpProducerMetrics:
    if not enabled:
        return NoOpProducerMetrics()

    return PrometheusProducerMetrics.create()
