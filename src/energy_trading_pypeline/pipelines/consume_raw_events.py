import argparse

from pydantic import ValidationError
from sqlalchemy.exc import SQLAlchemyError

from energy_trading_pypeline.config import get_settings
from energy_trading_pypeline.domain.alerts import evaluate_alerts
from energy_trading_pypeline.domain.market_snapshot import calculate_snapshot
from energy_trading_pypeline.messaging.consumer import (
    EnergyMarketEventConsumer,
    KafkaConsumerConfig,
)
from energy_trading_pypeline.persistence.db import SessionLocal
from energy_trading_pypeline.persistence.repositories import (
    MarketAlertRepository,
    MarketSnapshotRepository,
    RawEnergyMarketEventRepository,
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
        help="Maximum number of messages to consume. Use 0 to run continously."
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
    consumer.subscribe()
    consumed_messages = 0

    print(
        "Started consumer "
        f"topic={settings.kafka_raw_topic}"
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

                with SessionLocal.begin() as session:
                    repository = RawEnergyMarketEventRepository(session)
                    snapshot_repository = MarketSnapshotRepository(session)
                    alert_repository = MarketAlertRepository(session)

                    inserted = repository.save_valid_event(event)
                
                    if inserted:
                        snapshot = calculate_snapshot(event)
                        snapshot_updated = snapshot_repository.upsert_snapshot(snapshot)

                        if snapshot_updated:
                            alerts = evaluate_alerts(snapshot)
                            inserted_alerts = alert_repository.save_alerts(alerts)
                        else:
                            inserted_alerts = 0
                    else:
                        snapshot_updated = False
                        inserted_alerts = 0

                consumer.commit(message)
                consumed_messages += 1

                if inserted:
                    print(
                        "Persisted event "
                        f"event_id={event.event_id} "
                        f"market_area={event.market_area} "
                        f"snapshot_updated={snapshot_updated} "
                        f"inserted_alerts={inserted_alerts} "
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
                
            except ValidationError as exc:
                print(
                    "Invalid event payload. "
                    f"partition={message.partition()} "
                    f"offset={message.offset()} "
                    f"error={exc}"
                )

                # MVP decision:
                # Commit invalid messages to avoid blocking the consumer forever.
                # TODO: send invalid payloads to a dead-letter topic/table.
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