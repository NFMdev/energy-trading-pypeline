from dataclasses import dataclass
from typing import Any

from confluent_kafka import KafkaException, Producer

from energy_trading_pypeline.domain.energy_market_event import EnergyMarketEvent
from energy_trading_pypeline.messaging.serialization import serialize_energy_market_event


@dataclass(frozen=True)
class KafkaProducerConfig:
    bootstrap_servers: str
    topic: str
    client_id: str = "energy-market-producer"

class EnergyMarketEventProducer:
    def __init__(self, config: KafkaProducerConfig) -> None:
        self.config = config
        self._producer = Producer(
            {
                "bootstrap.servers": config.bootstrap_servers,
                "client.id": config.client_id,
                "acks": "all",
                "enable.idempotence": True
            }
        )

    def produce(self, event: EnergyMarketEvent) -> None:
        key = event.market_area.encode("utf-8")
        value = serialize_energy_market_event(event)

        try:
            self._producer.produce(
                topic=self.config.topic,
                key=key,
                value=value,
                on_delivery=self._on_delivery,
            )
            self._producer.poll(0)
        except BufferError as exc:
            raise RuntimeError("Kafka producer queue is full") from exc
        except KafkaException as exc:
            raise RuntimeError("Failed to produce event to Kafka") from exc
        
    def flush(self, timeout_seconds: float = 10.0) -> None:
        remaining_messages = self._producer.flush(timeout_seconds)

        if remaining_messages > 0:
            raise RuntimeError(f"Failed to flush {remaining_messages} Kafka message(s)")

    @staticmethod
    def _on_delivery(error: Any, message: Any) -> None:
        if error is not None:
            raise RuntimeError(f"Kafka delivery failed: {error}")

        print(
            "Produced event to Kafka "
            f"topic={message.topic()} "
            f"partition={message.partition()} "
            f"offset={message.offset()}"
        )