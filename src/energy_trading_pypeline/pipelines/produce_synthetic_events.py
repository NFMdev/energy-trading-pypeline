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

logger = logging.getLogger(__name__)
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

    produced_events = 0

    try:
        while _running:
            if (
                settings.producer_max_events is not None
                and produced_events >= settings.producer_max_events
            ):
                logger.info(
                    "Producer reached configured max events.",
                    extra={"produced_events": produced_events},
                )
                break

            event = generator.generate_energy_market_event()
            producer.produce(event)

            produced_events += 1

            logger.info(
                "Energy market event produced",
                extra={
                    "event_id": str(event.event_id),
                    "market_area": event.market_area,
                    "timestamp": event.timestamp.isoformat(),
                    "produced_events": produced_events,
                },
            )

            time.sleep(settings.producer_interval_seconds)

    finally:
        producer.flush()
        logger.info("Energy market producer stopped.", extra={"produced_events": produced_events})


if __name__ == "__main__":
    main()
