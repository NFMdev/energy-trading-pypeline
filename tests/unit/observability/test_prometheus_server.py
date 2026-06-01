import pytest

from energy_trading_pypeline.observability.prometheus.prometheus_server import (
    PrometheusMetricsServer,
)

pytestmark = pytest.mark.unit


def test_disabled_prometheus_metrics_server_does_not_start() -> None:
    server = PrometheusMetricsServer.start(
        enabled=False,
        host="127.0.0.1",
        port=9101,
    )

    assert server.server is None
    assert server.thread is None

    server.stop()
