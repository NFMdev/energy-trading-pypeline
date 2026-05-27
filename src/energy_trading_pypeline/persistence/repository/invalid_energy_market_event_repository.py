from sqlalchemy import text
from sqlalchemy.orm import Session

from energy_trading_pypeline.domain.invalid_energy_market_event import InvalidEnergyMarketEvent


class InvalidEnergyMarketEventRepository:
    def __init__(self, session: Session) -> None:
        self._session = session

    def save(self, invalid_event: InvalidEnergyMarketEvent) -> bool:
        statement = text(
            """
            INSERT INTO invalid_energy_market_events (
                topic,
                kafka_partition,
                kafka_offset,
                kafka_key,
                payload,
                payload_text,
                error_type,
                error_message,
                consumer_group
            ) VALUES (
                :topic,
                :kafka_partition,
                :kafka_offset,
                :kafka_key,
                :payload,
                :payload_text,
                :error_type,
                :error_message,
                :consumer_group
            ) ON CONFLICT (topic, kafka_partition, kafka_offset) DO NOTHING
            RETURNING id
            """
        )

        result = self._session.execute(
            statement,
            {
                "topic": invalid_event.topic,
                "kafka_partition": invalid_event.kafka_partition,
                "kafka_offset": invalid_event.kafka_offset,
                "kafka_key": invalid_event.kafka_key,
                "payload": invalid_event.payload,
                "payload_text": invalid_event.payload_text,
                "error_type": invalid_event.error_type,
                "error_message": invalid_event.error_message,
                "consumer_group": invalid_event.consumer_group,
            },
        )

        inserted_id = result.scalar_one_or_none()
        return inserted_id is not None
