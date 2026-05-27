from dataclasses import dataclass

import pytest
from sqlalchemy import Engine, text
from sqlalchemy.orm import Session, sessionmaker

from energy_trading_pypeline.pipelines.core.invalid_energy_market_event_processor import (
    InvalidEnergyMarketEventProcessor,
)

pytestmark = pytest.mark.integration


@dataclass(frozen=True)
class FakeKafkaMessage:
    topic_value: str
    partition_value: int
    offset_value: int
    key_value: bytes | None
    payload_value: bytes | None

    def topic(self) -> str:
        return self.topic_value

    def partition(self) -> int:
        return self.partition_value

    def offset(self) -> int:
        return self.offset_value

    def key(self) -> bytes | None:
        return self.key_value

    def value(self) -> bytes | None:
        return self.payload_value


def _create_session_facatory(postgres_engine: Engine) -> sessionmaker[Session]:
    return sessionmaker(
        bind=postgres_engine,
        autoflush=False,
        expire_on_commit=False,
    )


def test_process_invalid_message_persists_kafka_metadata_payload_and_error(
    postgres_engine: Engine,
    session: Session,
) -> None:
    processor = InvalidEnergyMarketEventProcessor(
        session_factory=_create_session_facatory(postgres_engine),
        consumer_group="energy-market-ingestion-v1",
    )

    message = FakeKafkaMessage(
        topic_value="energy.market.raw.v1",
        partition_value=0,
        offset_value=123,
        key_value=b"DK1",
        payload_value=b'{"market_area": "DK1", "electricity_price_dkk_mwh": "invalid"}',
    )

    inserted = processor.process(message, ValueError("Invalid decimal value"))
    session.commit()

    assert inserted is True
    row = (
        session.execute(
            text(
                """
            SELECT
                topic,
                kafka_partition,
                kafka_offset,
                kafka_key,
                payload,
                payload_text,
                error_type,
                error_message,
                consumer_group
            FROM invalid_energy_market_events
            WHERE topic = :topic
                AND kafka_partition = :kafka_partition
                AND kafka_offset = :kafka_offset
            """
            ),
            {
                "topic": message.topic(),
                "kafka_partition": message.partition(),
                "kafka_offset": message.offset(),
            },
        )
        .mappings()
        .one()
    )

    assert row["topic"] == "energy.market.raw.v1"
    assert row["kafka_partition"] == 0
    assert row["kafka_offset"] == 123
    assert row["kafka_key"] == "DK1"
    assert bytes(row["payload"]) == message.payload_value
    assert row["payload_text"] == '{"market_area": "DK1", "electricity_price_dkk_mwh": "invalid"}'
    assert row["error_type"] == "ValueError"
    assert row["error_message"] == "Invalid decimal value"
    assert row["consumer_group"] == "energy-market-ingestion-v1"


def test_process_invalid_message_is_idempotent_by_kafka_position(postgres_engine: Engine) -> None:
    processor = InvalidEnergyMarketEventProcessor(
        session_factory=_create_session_facatory(postgres_engine),
        consumer_group="energy-market-ingestion-v1",
    )

    message = FakeKafkaMessage(
        topic_value="energy.market.raw.v1",
        partition_value=1,
        offset_value=456,
        key_value=None,
        payload_value=b"not-json",
    )

    first_insert = processor.process(message, ValueError("Invalid JSON payload"))
    second_insert = processor.process(message, ValueError("Invalid JSON payload"))

    assert first_insert is True
    assert second_insert is False


def test_process_invalid_message_with_null_payload_persists_empty_payload(
    postgres_engine: Engine,
) -> None:
    processor = InvalidEnergyMarketEventProcessor(
        session_factory=_create_session_facatory(postgres_engine),
        consumer_group="energy-market-ingestion-v1",
    )

    message = FakeKafkaMessage(
        topic_value="energy.market.raw.v1",
        partition_value=0,
        offset_value=999,
        key_value=b"DK1",
        payload_value=None,
    )

    inserted = processor.process(
        message,
        ValueError("Kafka message payload cannot be null"),
    )

    assert inserted is True

    with processor._session_factory() as session:
        row = (
            session.execute(
                text(
                    """
                SELECT
                    payload,
                    payload_text,
                    error_type,
                    error_message
                FROM invalid_energy_market_events
                WHERE topic = :topic
                  AND kafka_partition = :kafka_partition
                  AND kafka_offset = :kafka_offset
                """
                ),
                {
                    "topic": "energy.market.raw.v1",
                    "kafka_partition": 0,
                    "kafka_offset": 999,
                },
            )
            .mappings()
            .one()
        )

    assert bytes(row["payload"]) == b""
    assert row["payload_text"] is None
    assert row["error_type"] == "ValueError"
    assert row["error_message"] == "Kafka message payload cannot be null"
