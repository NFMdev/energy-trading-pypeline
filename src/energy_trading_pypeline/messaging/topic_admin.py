from dataclasses import dataclass

from confluent_kafka.admin import AdminClient
from confluent_kafka.cimpl import NewTopic


@dataclass(frozen=True)
class KafkaTopicConfig:
    name: str
    partitions: int = 3
    replication_factor: int = 1


def check_topic_exists(
    *, bootstrap_servers: str, topic_config: KafkaTopicConfig, timeout_seconds: float = 10.0
) -> bool:
    admin_client = AdminClient({"bootstrap.servers": bootstrap_servers})

    metadata = admin_client.list_topics(timeout=timeout_seconds)

    if topic_config.name in metadata.topics:
        return False

    topic = NewTopic(
        topic=topic_config.name,
        num_partitions=topic_config.partitions,
        replication_factor=topic_config.replication_factor,
    )

    futures = admin_client.create_topics([topic])

    future = futures[topic_config.name]
    future.result(timeout=timeout_seconds)

    return True
