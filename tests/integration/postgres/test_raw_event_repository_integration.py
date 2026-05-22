from datetime import UTC, datetime
from decimal import Decimal
from typing import Any
from uuid import UUID, uuid4

import pytest
from sqlalchemy import text
from sqlalchemy.orm import Session

from energy_trading_pypeline.domain.energy_market_event import EnergyMarketEvent, QualityFlag
from energy_trading_pypeline.persistence.repositories import RawEnergyMarketEventRepository

pytestmark = pytest.mark.integration


def _create_energy_market_event(
    *,
    event_id: UUID | None = None,
    market_area: str = "DK1",
    timestamp: datetime | None = None,
    created_at: datetime | None = None,
    electricity_price_dkk_mwh: Decimal = Decimal("850.25"),
    forecast_wind_mw: Decimal = Decimal("1200.00"),
    actual_wind_mw: Decimal = Decimal("1300.00"),
    forecast_solar_mw: Decimal = Decimal("300.00"),
    actual_solar_mw: Decimal = Decimal(280.00),
    load_mw: Decimal = Decimal("5000.00"),
    imbalance_price_dkk_mwh: Decimal = Decimal("975.00"),
    source: str = "integration-test",
    quality_flag: QualityFlag = "OK",
) -> EnergyMarketEvent:
    event_timestamp = timestamp or datetime(2026, 1, 1, 12, 0, tzinfo=UTC)
    event_created_at = created_at or datetime(2026, 1, 1, 12, 0, 5, tzinfo=UTC)

    return EnergyMarketEvent(
        event_id=event_id or uuid4(),
        schema_version="1.0",
        market_area=market_area,
        timestamp=event_timestamp,
        created_at=event_created_at,
        electricity_price_dkk_mwh=electricity_price_dkk_mwh,
        forecast_wind_mw=forecast_wind_mw,
        actual_wind_mw=actual_wind_mw,
        forecast_solar_mw=forecast_solar_mw,
        actual_solar_mw=actual_solar_mw,
        load_mw=load_mw,
        imbalance_price_dkk_mwh=imbalance_price_dkk_mwh,
        source=source,
        quality_flag=quality_flag,
    )


def _fetch_raw_event_by_event_id(session: Session, event_id: UUID) -> dict[str, Any]:
    row = (
        session.execute(
            text("SELECT * FROM raw_energy_market_events WHERE event_id = :event_id"),
            {"event_id": event_id},
        )
        .mappings()
        .one()
    )

    return dict(row)


def _count_raw_events_by_event_id(
    session: Session,
    event_id: UUID,
) -> int:
    result = session.execute(
        text("SELECT COUNT(*) FROM raw_energy_market_events WHERE event_id = :event_id"),
        {"event_id": event_id},
    ).scalar_one()

    return int(result)


def test_insert_raw_event_persists_event_in_postgres(session: Session) -> None:
    repository = RawEnergyMarketEventRepository(session)
    event = _create_energy_market_event()

    inserted = repository.save_valid_event(event)

    assert inserted is True

    row = _fetch_raw_event_by_event_id(session, event.event_id)

    assert row["event_id"] == event.event_id
    assert row["schema_version"] == event.schema_version
    assert row["market_area"] == event.market_area
    assert row["electricity_price_dkk_mwh"] == event.electricity_price_dkk_mwh
    assert row["forecast_wind_mw"] == event.forecast_wind_mw
    assert row["actual_wind_mw"] == event.actual_wind_mw
    assert row["forecast_solar_mw"] == event.forecast_solar_mw
    assert row["actual_solar_mw"] == event.actual_solar_mw
    assert row["load_mw"] == event.load_mw
    assert row["imbalance_price_dkk_mwh"] == event.imbalance_price_dkk_mwh
    assert row["source"] == event.source
    assert row["quality_flag"] == event.quality_flag
    assert row["validation_status"] == "VALID"
    assert row["validation_error"] is None
    assert row["ingested_at"] is not None

    payload = row["payload"]
    assert isinstance(payload, dict)
    assert payload["event_id"] == str(event.event_id)
    assert payload["market_area"] == event.market_area
    assert payload["quality_flag"] == event.quality_flag


def test_insert_raw_event_is_idempotent_for_duplicate_event_id(
    session: Session,
) -> None:
    repository = RawEnergyMarketEventRepository(session)
    event = _create_energy_market_event()

    first_insert = repository.save_valid_event(event)
    second_insert = repository.save_valid_event(event)

    assert first_insert is True
    assert second_insert is False
    assert _count_raw_events_by_event_id(session, event.event_id) == 1


def test_duplicate_event_id_does_not_overwrite_existing_raw_event(session: Session) -> None:
    repository = RawEnergyMarketEventRepository(session)
    event_id = uuid4()

    original_event = _create_energy_market_event(
        event_id=event_id, market_area="DK1", source="original_source"
    )
    duplicate_event = _create_energy_market_event(
        event_id=event_id,
        market_area="DK2",
        electricity_price_dkk_mwh=Decimal("9999.99"),
        source="duplicate_source",
    )

    first_insert = repository.save_valid_event(original_event)
    second_insert = repository.save_valid_event(duplicate_event)

    assert first_insert is True
    assert second_insert is False

    row = _fetch_raw_event_by_event_id(session, event_id)

    assert row["market_area"] == "DK1"
    assert row["electricity_price_dkk_mwh"] == Decimal("850.25")
    assert row["source"] == "original_source"
    assert _count_raw_events_by_event_id(session, event_id) == 1
