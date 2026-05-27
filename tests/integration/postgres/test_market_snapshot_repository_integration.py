from datetime import UTC, datetime
from decimal import Decimal
from typing import Any
from uuid import UUID, uuid4

import pytest
from sqlalchemy import text
from sqlalchemy.orm import Session

from energy_trading_pypeline.domain.energy_market_event import EnergyMarketEvent, QualityFlag
from energy_trading_pypeline.domain.market_snapshot import MarketSnapshot, calculate_snapshot
from energy_trading_pypeline.persistence.repository.repositories import MarketSnapshotRepository

pytestmark = pytest.mark.integration


def _create_energy_market_event(
    *,
    event_id: UUID | None = None,
    market_area: str = "DK1",
    timestamp: datetime | None = None,
    created_at: datetime | None = None,
    electricity_price_dkk_mwh: Decimal = Decimal("850.00"),
    forecast_wind_mw: Decimal = Decimal("1200.00"),
    actual_wind_mw: Decimal = Decimal("1500.00"),
    forecast_solar_mw: Decimal = Decimal("300.00"),
    actual_solar_mw: Decimal = Decimal("250.00"),
    load_mw: Decimal = Decimal("5000.00"),
    imbalance_price_dkk_mwh: Decimal = Decimal("1000.00"),
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


def _create_snapshot_from_event(event: EnergyMarketEvent) -> MarketSnapshot:
    return calculate_snapshot(event)


def _fetch_snapshot(session: Session, market_area: str) -> dict[str, Any]:
    row = (
        session.execute(
            text("SELECT * FROM market_snapshot WHERE market_area = :market_area"),
            {"market_area": market_area},
        )
        .mappings()
        .one()
    )

    return dict(row)


def test_upsert_snapshot_inserts_new_market_area(session: Session) -> None:
    repository = MarketSnapshotRepository(session)

    event = _create_energy_market_event()
    snapshot = _create_snapshot_from_event(event)

    updated = repository.upsert_snapshot(snapshot)

    assert updated is True
    row = _fetch_snapshot(session, "DK1")

    assert row["market_area"] == "DK1"
    assert row["last_event_id"] == event.event_id
    assert row["last_event_timestamp"] == event.timestamp
    assert row["electricity_price_dkk_mwh"] == event.electricity_price_dkk_mwh
    assert row["imbalance_price_dkk_mwh"] == event.imbalance_price_dkk_mwh
    assert row["wind_forecast_error_mw"] == Decimal("300.00")
    assert row["solar_forecast_error_mw"] == Decimal("-50.00")
    assert row["renewable_actual_mw"] == Decimal("1750.00")
    assert row["net_load_mw"] == Decimal("3250.00")
    assert row["imbalance_spread_dkk_mwh"] == Decimal("150.00")
    assert row["quality_flag"] == "OK"
    assert row["updated_at"] is not None


def test_upsert_snapshot_updates_when_incoming_timestamp_is_newer(session: Session) -> None:
    repository = MarketSnapshotRepository(session)

    old_event = _create_energy_market_event()
    new_event = _create_energy_market_event(
        timestamp=datetime(2026, 1, 1, 12, 5, tzinfo=UTC),
        electricity_price_dkk_mwh=Decimal("900.00"),
    )

    first_update = repository.upsert_snapshot(_create_snapshot_from_event(old_event))
    second_update = repository.upsert_snapshot(_create_snapshot_from_event(new_event))

    assert first_update is True
    assert second_update is True

    row = _fetch_snapshot(session, "DK1")

    assert row["last_event_id"] == new_event.event_id
    assert row["last_event_timestamp"] == new_event.timestamp
    assert row["electricity_price_dkk_mwh"] == Decimal("900.00")


def test_upsert_snapshot_updates_when_incoming_timestamp_is_equal(session: Session) -> None:
    repository = MarketSnapshotRepository(session)

    first_event = _create_energy_market_event()
    second_event = _create_energy_market_event(electricity_price_dkk_mwh=Decimal("900.00"))

    first_update = repository.upsert_snapshot(_create_snapshot_from_event(first_event))
    second_update = repository.upsert_snapshot(_create_snapshot_from_event(second_event))

    assert first_update is True
    assert second_update is True

    row = _fetch_snapshot(session, "DK1")

    assert row["last_event_id"] == second_event.event_id
    assert row["last_event_timestamp"] == second_event.timestamp
    assert row["electricity_price_dkk_mwh"] == Decimal("900.00")


def test_upsert_snapshot_does_not_update_when_timestamp_is_older(session: Session) -> None:
    repository = MarketSnapshotRepository(session)

    newer_event = _create_energy_market_event(timestamp=datetime(2026, 1, 1, 12, 10, tzinfo=UTC))
    older_event = _create_energy_market_event(
        timestamp=datetime(2026, 1, 1, 12, 5, tzinfo=UTC),
        electricity_price_dkk_mwh=Decimal("500.00"),
    )

    first_update = repository.upsert_snapshot(_create_snapshot_from_event(newer_event))
    second_update = repository.upsert_snapshot(_create_snapshot_from_event(older_event))

    assert first_update is True
    assert second_update is False

    row = _fetch_snapshot(session, "DK1")

    assert row["last_event_id"] == newer_event.event_id
    assert row["last_event_timestamp"] == newer_event.timestamp
    assert row["electricity_price_dkk_mwh"] == Decimal("850.00")
