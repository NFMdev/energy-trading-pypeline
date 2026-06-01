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
from energy_trading_pypeline.observability.periodic_summary import EventCountSummaryReporter
from energy_trading_pypeline.observability.prometheus.prometheus_server import (
    PrometheusMetricsServer,
)
from energy_trading_pypeline.observability.runtime_stats import ConsumerRuntimeStats
from energy_trading_pypeline.persistence.db import SessionLocal
from energy_trading_pypeline.pipelines.core.energy_market_event_processor import (
    EnergyMarketEventProcessor,
)
from energy_trading_pypeline.pipelines.core.invalid_energy_market_event_processor import (
    InvalidEnergyMarketEventProcessor,
)

logger = logging.getLogger(__name__)
stats = ConsumerRuntimeStats()


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
    metrics_server = PrometheusMetricsServer.start(
        enabled=settings.metrics_enabled,
        host=settings.metrics_host,
        port=settings.consumer_metrics_port,
    )

    summary_reporter = EventCountSummaryReporter(
        interval_events=settings.operational_summary_interval_events
    )

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
    logger.info(
        "Starting raw energy market consumer",
        extra={"topic": settings.kafka_raw_topic, "group_id": settings.kafka_consumer_group},
    )

    try:
        while True:
            if args.max_messages > 0 and stats.committed_messages >= args.max_messages:
                break

            message = consumer.poll(timeout_seconds=args.poll_timeout_seconds)

            if message is None:
                continue

            try:
                event = consumer.parse_message(message)
                result = event_processor.process(event)

                if result.duplicate_event:
                    stats.record_duplicate_event()
                    logger.info(
                        "Duplicate energy market event skipped",
                        extra={
                            "event_id": str(event.event_id),
                            "market_area": event.market_area,
                            "timestamp": event.timestamp.isoformat(),
                        },
                    )
                elif result.stale_event:
                    stats.record_stale_event()
                    logger.info(
                        "Stale energy market event persisted without snapshot update",
                        extra={
                            "event_id": str(event.event_id),
                            "market_area": event.market_area,
                            "timestamp": event.timestamp.isoformat(),
                        },
                    )
                else:
                    stats.record_valid_event_processed(inserted_alerts=result.inserted_alerts)
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
                stats.record_committed_messages()

                logger.info(
                    "Kafka message committed",
                    extra={
                        "topic": message.topic(),
                        "partition": message.partition(),
                        "offset": message.offset(),
                    },
                )
                _log_consumer_summary_if_needed(summary_reporter)

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

                if inserted:
                    stats.record_invalid_event_persisted()

                    logger.warning(
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
                stats.record_committed_messages()

                logger.info(
                    "Kafka message committed",
                    extra={
                        "topic": message.topic(),
                        "partition": message.partition(),
                        "offset": message.offset(),
                    },
                )
                _log_consumer_summary_if_needed(summary_reporter)

            except SQLAlchemyError as exc:
                stats.record_processing_failure()
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
                stats.record_processing_failure()
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
        metrics_server.stop()
        logger.info(
            "Energy market consumer stopped.",
            extra={
                "valid_events_processed": stats.valid_events_processed,
                "duplicate_events": stats.duplicate_events,
                "stale_events": stats.stale_events,
                "invalid_events_persisted": stats.invalid_events_persisted,
                "generated_alerts": stats.generated_alerts,
                "processing_failures": stats.processing_failures,
                "committed_messages": stats.committed_messages,
            },
        )


def _log_consumer_summary_if_needed(summary_reporter: EventCountSummaryReporter) -> None:
    if not summary_reporter.should_report(stats.committed_messages):
        return

    logger.info(
        "Consumer operational summary",
        extra={
            "valid_events_processed": stats.valid_events_processed,
            "duplicate_events": stats.duplicate_events,
            "stale_events": stats.stale_events,
            "invalid_events_persisted": stats.invalid_events_persisted,
            "generated_alerts": stats.generated_alerts,
            "processing_failures": stats.processing_failures,
            "committed_messages": stats.committed_messages,
        },
    )


if __name__ == "__main__":
    main()
