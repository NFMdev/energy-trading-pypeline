import argparse
import logging
from json import JSONDecodeError

from pydantic import ValidationError
from sqlalchemy.exc import SQLAlchemyError

from energy_trading_pypeline.config import get_settings
from energy_trading_pypeline.messaging.consumer import (
    EnergyMarketEventConsumer,
    KafkaConsumerConfig,
)
from energy_trading_pypeline.observability.logging import configure_logging
from energy_trading_pypeline.persistence.db import SessionLocal
from energy_trading_pypeline.pipelines.core.energy_market_event_processor import (
    EnergyMarketEventProcessor,
)
from energy_trading_pypeline.pipelines.core.invalid_energy_market_event_processor import (
    InvalidEnergyMarketEventProcessor,
)

logger = logging.getLogger(__name__)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Consume energy market events from Kafka/Redpanda "
        "and persist them to PostgreSQL."
    )

    parser.add_argument(
        "--max-messages",
        type=int,
        default=0,
        help="Maximum number of messages to consume. Use 0 to run continously.",
    )

    parser.add_argument(
        "--poll-timeout-seconds",
        type=float,
        default=1.0,
        help="Kafka poll timeout in seconds",
    )

    return parser.parse_args()


def main() -> None:
    args = parse_args()
    settings = get_settings()
    configure_logging(settings.log_level)

    consumer = EnergyMarketEventConsumer(
        KafkaConsumerConfig(
            bootstrap_servers=settings.kafka_bootstrap_servers,
            topic=settings.kafka_raw_topic,
            group_id=settings.kafka_consumer_group,
        )
    )
    event_processor = EnergyMarketEventProcessor(SessionLocal)
    invalid_event_processor = InvalidEnergyMarketEventProcessor(
        session_factory=SessionLocal, consumer_group=settings.kafka_consumer_group
    )

    consumer.subscribe()
    consumed_messages = 0

    logger.info(
        "Starting raw energy market consumer",
        extra={"topic": settings.kafka_raw_topic, "group_id": settings.kafka_consumer_group},
    )

    try:
        while True:
            if args.max_messages > 0 and consumed_messages >= args.max_messages:
                break

            message = consumer.poll(timeout_seconds=args.poll_timeout_seconds)

            if message is None:
                continue

            try:
                event = consumer.parse_message(message)
                result = event_processor.process(event)

                if result.duplicate_event:
                    logger.info(
                        "Duplicate energy market event skipped",
                        extra={
                            "event_id": str(event.event_id),
                            "market_area": event.market_area,
                            "timestamp": event.timestamp.isoformat(),
                        },
                    )
                elif result.stale_event:
                    logger.info(
                        "Stale energy market event persisted without snapshot update",
                        extra={
                            "event_id": str(event.event_id),
                            "market_area": event.market_area,
                            "timestamp": event.timestamp.isoformat(),
                        },
                    )
                else:
                    logger.info(
                        "Energy market event processed",
                        extra={
                            "event_id": str(event.event_id),
                            "market_area": event.market_area,
                            "timestamp": event.timestamp.isoformat(),
                            "inserted_alerts": result.inserted_alerts,
                        },
                    )

                consumer.commit(message)
                consumed_messages += 1

                logger.info(
                    "Kafka message committed",
                    extra={
                        "topic": message.topic(),
                        "partition": message.partition(),
                        "offset": message.offset(),
                    },
                )

            except (ValidationError, ValueError, JSONDecodeError) as exc:
                logger.warning(
                    "Invalid Kafka message received",
                    extra={
                        "topic": message.topic(),
                        "partition": message.partition(),
                        "offset": message.offset(),
                        "error_type": type(exc).__name__,
                    },
                )

                inserted = invalid_event_processor.process(message, exc)

                logger.info(
                    "Invalid Kafka message persisted",
                    extra={
                        "topic": message.topic(),
                        "partition": message.partition(),
                        "offset": message.offset(),
                        "inserted": inserted,
                        "error_type": type(exc).__name__,
                    },
                )

                consumer.commit(message)

                logger.info(
                    "Kafka message committed",
                    extra={
                        "topic": message.topic(),
                        "partition": message.partition(),
                        "offset": message.offset(),
                    },
                )

            except SQLAlchemyError as exc:
                logger.exception(
                    "Database error while processing message.",
                    extra={
                        "topic": message.topic(),
                        "partition": message.partition(),
                        "offset": message.offset(),
                        "error": exc,
                    },
                )

                raise

            except Exception as exc:
                logger.exception(
                    "Unexpected error while processing Kafka message.",
                    extra={
                        "topic": message.topic(),
                        "partition": message.partition(),
                        "offset": message.offset(),
                        "error": exc,
                    },
                )

                raise

    except KeyboardInterrupt:
        logger.info("Stopping consumer...")

    finally:
        consumer.close()
        logger.info(f"Consumer stopped. consumed_messages={consumed_messages}")


if __name__ == "__main__":
    main()
