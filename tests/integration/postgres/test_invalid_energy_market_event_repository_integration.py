from typing import Any, cast

import pytest
from sqlalchemy import CursorResult, text
from sqlalchemy.orm import Session

from energy_trading_pypeline.domain.invalid_energy_market_event import InvalidEnergyMarketEvent
from energy_trading_pypeline.persistence.repository.invalid_energy_market_event_repository import (
    InvalidEnergyMarketEventRepository,
)

pytestmark = pytest.mark.integration


def test_save_invalid_message_inserts_row(session: Session) -> None:
    repository = InvalidEnergyMarketEventRepository(session)

    event = InvalidEnergyMarketEvent(
        topic="energy.market.raw.v1",
        kafka_partition=0,
        kafka_offset=42,
        kafka_key="DK1",
        payload=b'{"market_area": "DK1", "electricity_price_dkk_mwh": "invalid"}',
        payload_text='{"market_area": "DK1", "electricity_price_dkk_mwh": "invalid"}',
        error_type="ValidationError",
        error_message="Invalid decimal value",
        consumer_group="energy-market-ingestion-v1",
    )

    inserted = repository.save(event)
    session.commit()

    assert inserted is True

    result = cast(
        CursorResult[Any],
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
                "topic": event.topic,
                "kafka_partition": event.kafka_partition,
                "kafka_offset": event.kafka_offset,
            },
        ),
    )

    row = result.mappings().one()

    assert row["topic"] == "energy.market.raw.v1"
    assert row["kafka_partition"] == 0
    assert row["kafka_offset"] == 42
    assert row["kafka_key"] == "DK1"
    assert bytes(row["payload"]) == event.payload
    assert row["payload_text"] == event.payload_text
    assert row["error_type"] == "ValidationError"
    assert row["error_message"] == "Invalid decimal value"
    assert row["consumer_group"] == "energy-market-ingestion-v1"


def test_save_invalid_message_is_idempotent_by_kafka_position(session: Session) -> None:
    repository = InvalidEnergyMarketEventRepository(session)

    event = InvalidEnergyMarketEvent(
        topic="energy.market.raw.v1",
        kafka_partition=1,
        kafka_offset=100,
        kafka_key=None,
        payload=b"not-json",
        payload_text="not-json",
        error_type="ValidationError",
        error_message="Invalid JSON payload",
        consumer_group="energy-market-ingestion-v1",
    )

    first_insert = repository.save(event)
    second_insert = repository.save(event)

    session.commit()

    assert first_insert is True
    assert second_insert is False

    total = cast(
        CursorResult[Any],
        session.execute(
            text(
                """
                SELECT
                    COUNT(*) as total
                FROM invalid_energy_market_events
                WHERE topic = :topic
                    AND kafka_partition = :kafka_partition
                    AND kafka_offset = :kafka_offset
                """
            ),
            {
                "topic": event.topic,
                "kafka_partition": event.kafka_partition,
                "kafka_offset": event.kafka_offset,
            },
        ),
    ).scalar_one()

    assert total == 1
