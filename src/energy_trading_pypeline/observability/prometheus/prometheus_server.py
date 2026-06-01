import logging
from dataclasses import dataclass
from threading import Thread
from typing import Protocol, Self, cast

from prometheus_client import start_http_server

logger = logging.Logger(__name__)


class _StoppableServer(Protocol):
    def shutdown(self) -> None:
        """Stop the server loop."""

    def server_close(self) -> None:
        """Close the server socket."""


@dataclass(frozen=True)
class PrometheusMetricsServer:
    server: _StoppableServer | None
    thread: Thread | None

    @classmethod
    def disabled(cls) -> Self:
        return cls(server=None, thread=None)

    @classmethod
    def start(cls, *, enabled: bool, host: str, port: int) -> Self:
        if not enabled:
            logger.info("Prometheus metrics server disabled")
            return cls.disabled()

        raw_server, thread = start_http_server(port=port, addr=host)

        server = cast(_StoppableServer, raw_server)

        logger.info(
            "Prometheus metrics server started",
            extra={
                "metrics_host": host,
                "metrics_port": port,
            },
        )

        return cls(server=server, thread=thread)

    def stop(self) -> None:
        if self.server is None or self.thread is None:
            return

        logger.info("Stopping Prometheus metrics server")

        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=5)
