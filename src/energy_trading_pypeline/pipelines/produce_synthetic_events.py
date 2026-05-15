import argparse
import time

from energy_trading_pypeline.config import get_settings
from energy_trading_pypeline.generator.event_generator import generate_energy_market_event
from energy_trading_pypeline.messaging.producer import (
    EnergyMarketEventProducer,
    KafkaProducerConfig,
)
from energy_trading_pypeline.messaging.topic_admin import KafkaTopicConfig, check_topic_exists


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Produce synthethic energy market events to Kafka/Redpanda."
    )

    parser.add_argument(
        "--count",
        type=int,
        default=10,
        help="Number of events to produce.",
    )

    parser.add_argument(
        "--delay-seconds",
        type=float,
        default=0.5,
        help="Delay between produced events.",
    )

    return parser.parse_args()

def main() -> None:
    args = parse_args()
    settings = get_settings()
    topic_created = check_topic_exists(
        bootstrap_servers=settings.kafka_bootstrap_servers,
        topic_config=KafkaTopicConfig(
            name=settings.kafka_raw_topic,
            partitions=3,
            replication_factor=1,
        ),
    )

    if topic_created:
        print(f"Created Kafka topic: {settings.kafka_raw_topic}")
    else:
        print(f"Kafka topic already exists: {settings.kafka_raw_topic}")

    producer = EnergyMarketEventProducer(
        KafkaProducerConfig(
            bootstrap_servers=settings.kafka_bootstrap_servers,
            topic=settings.kafka_raw_topic
        )
    )

    for index in range(args.count):
        event = generate_energy_market_event()
        producer.produce(event)

        print (
            f"Queued event {index + 1}/{args.count}: "
            f"event_id={event.event_id} "
            f"market_area={event.market_area} "
            f"timestamp={event.timestamp.isoformat()}"
        )

        if args.delay_seconds > 0:
            time.sleep(args.delay_seconds)
        
    producer.flush()
    print(f"Produced {args.count} event(s) to topic {settings.kafka_raw_topic}")

if __name__ == "__main__":
    main()