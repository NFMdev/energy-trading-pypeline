import logging
import signal
import time
from types import FrameType

from energy_trading_pypeline.config import get_settings
from energy_trading_pypeline.generator.event_generator import (
    EventGenerator,
)
from energy_trading_pypeline.messaging.producer import (
    EnergyMarketEventProducer,
    KafkaProducerConfig,
)
from energy_trading_pypeline.messaging.topic_admin import KafkaTopicConfig, check_topic_exists
from energy_trading_pypeline.observability.logging import configure_logging
from energy_trading_pypeline.observability.periodic_summary import EventCountSummaryReporter
from energy_trading_pypeline.observability.prometheus_server import PrometheusMetricsServer
from energy_trading_pypeline.observability.runtime_stats import ProducerRuntimeStats

logger = logging.getLogger(__name__)
stats = ProducerRuntimeStats()
_running = True


def _request_shutdown(signum: int, frame: FrameType | None) -> None:
    global _running
    _running = False

    logger.info(
        "Shutdown requested for continuous producer",
        extra={"signal": signum},
    )


def main() -> None:
    settings = get_settings()
    configure_logging(settings.log_level)
    metrics_server = PrometheusMetricsServer.start(
        enabled=settings.metrics_enabled,
        host=settings.metrics_host,
        port=settings.producer_metrics_port,
    )

    summary_reporter = EventCountSummaryReporter(
        interval_events=settings.operational_summary_interval_events
    )

    signal.signal(signal.SIGINT, _request_shutdown)
    signal.signal(signal.SIGTERM, _request_shutdown)

    topic_created = check_topic_exists(
        bootstrap_servers=settings.kafka_bootstrap_servers,
        topic_config=KafkaTopicConfig(
            name=settings.kafka_raw_topic,
            partitions=3,
            replication_factor=1,
        ),
    )

    if topic_created:
        logger.info("Created Kafka topic", extra={"topic": settings.kafka_raw_topic})

    generator = EventGenerator(market_areas=settings.market_areas)
    producer = EnergyMarketEventProducer(
        KafkaProducerConfig(
            bootstrap_servers=settings.kafka_bootstrap_servers, topic=settings.kafka_raw_topic
        )
    )

    try:
        while _running:
            if (
                settings.producer_max_events is not None
                and stats.produced_events >= settings.producer_max_events
            ):
                logger.info(
                    "Producer reached configured max events.",
                    extra={"produced_events": stats.produced_events},
                )
                break

            try:
                event = generator.generate_energy_market_event()
                producer.produce(event)
                stats.record_produced_event()

                logger.info(
                    "Energy market event produced",
                    extra={
                        "event_id": str(event.event_id),
                        "market_area": event.market_area,
                        "timestamp": event.timestamp.isoformat(),
                        "produced_events": stats.produced_events,
                    },
                )

                if summary_reporter.should_report(stats.produced_events):
                    logger.info(
                        "Producer operational summary",
                        extra={
                            "produced_events": stats.produced_events,
                            "publish_failures": stats.publish_failures,
                        },
                    )

                time.sleep(settings.producer_interval_seconds)

            except Exception:
                stats.record_publish_failure()
                logger.exception("Failed to publish energy market event.")

    finally:
        producer.flush()
        metrics_server.stop()
        logger.info(
            "Energy market producer stopped.",
            extra={
                "produced_events": stats.produced_events,
                "publish_failures": stats.publish_failures,
            },
        )


if __name__ == "__main__":
    main()
