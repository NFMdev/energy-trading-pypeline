import argparse
from json import JSONDecodeError

from pydantic import ValidationError
from sqlalchemy.exc import SQLAlchemyError

from energy_trading_pypeline.config import get_settings
from energy_trading_pypeline.messaging.consumer import (
    EnergyMarketEventConsumer,
    KafkaConsumerConfig,
)
from energy_trading_pypeline.persistence.db import SessionLocal
from energy_trading_pypeline.pipelines.core.energy_market_event_processor import (
    EnergyMarketEventProcessor,
)
from energy_trading_pypeline.pipelines.core.invalid_energy_market_event_processor import (
    InvalidEnergyMarketEventProcessor,
)


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

    print(
        "Started consumer "
        f"topic={settings.kafka_raw_topic} "
        f"group_id={settings.kafka_consumer_group}"
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

                consumer.commit(message)
                consumed_messages += 1

                if result.raw_event_inserted:
                    print(
                        "Persisted event "
                        f"event_id={event.event_id} "
                        f"market_area={event.market_area} "
                        f"snapshot_updated={result.snapshot_updated} "
                        f"inserted_alerts={result.inserted_alerts} "
                        f"partition={message.partition()} "
                        f"offset={message.offset()}"
                    )
                else:
                    print(
                        "Skipped duplicate event "
                        f"event_id={event.event_id} "
                        f"market_area={event.market_area} "
                        f"partition={message.partition()} "
                        f"offset={message.offset()}"
                    )

            except (ValidationError, ValueError, JSONDecodeError) as exc:
                invalid_event_processor.process(message, exc)
                consumer.commit(message)

            except SQLAlchemyError as exc:
                print(
                    "Database error while processing message. "
                    f"partition={message.partition()} "
                    f"offset={message.offset()} "
                    f"error={exc}"
                )

                raise

    except KeyboardInterrupt:
        print("Stopping consumer...")

    finally:
        consumer.close()
        print(f"Consumer stopped. consumed_messages={consumed_messages}")


if __name__ == "__main__":
    main()
