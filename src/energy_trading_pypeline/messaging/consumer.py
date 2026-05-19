from dataclasses import dataclass
from typing import Any

from confluent_kafka import Consumer, KafkaError, KafkaException

from energy_trading_pypeline.domain.energy_market_event import EnergyMarketEvent
from energy_trading_pypeline.messaging.serialization import deserialize_energy_market_event


@dataclass(frozen=True)
class KafkaConsumerConfig:
    bootstrap_servers: str
    topic: str
    group_id: str
    client_id: str = "energy-market-consumer"
    auto_offset_reset: str = "earliest"


class EnergyMarketEventConsumer:
    def __init__(self, config: KafkaConsumerConfig) -> None:
        self._config = config
        self._consumer = Consumer(
            {
                "bootstrap.servers": config.bootstrap_servers,
                "group.id": config.group_id,
                "client.id": config.client_id,
                "auto.offset.reset": config.auto_offset_reset,
                "enable.auto.commit": False,
            }
        )

    def subscribe(self) -> None:
        self._consumer.subscribe([self._config.topic])

    def poll(self, timeout_seconds: float = 1.0) -> Any | None:
        message = self._consumer.poll(timeout_seconds)

        if message is None:
            return None

        error = message.error()

        if error is None:
            return message

        if error.code() == KafkaError._PARTITION_EOF:
            return None

        raise KafkaException(error)

    def parse_message(self, message: Any) -> EnergyMarketEvent:
        payload = message.value()

        if payload is None:
            raise ValueError("Kafka message payload cannot be null")

        return deserialize_energy_market_event(payload)

    def commit(self, message: Any) -> None:
        self._consumer.commit(message=message, asynchronous=False)

    def close(self) -> None:
        self._consumer.close()
