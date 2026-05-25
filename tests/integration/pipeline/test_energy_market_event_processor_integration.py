from datetime import UTC, datetime
from decimal import Decimal
from typing import cast
from uuid import UUID, uuid4

import pytest
from sqlalchemy import Engine, text
from sqlalchemy.orm import Session, sessionmaker

from energy_trading_pypeline.domain.energy_market_event import EnergyMarketEvent, QualityFlag
from energy_trading_pypeline.pipelines.enery_market_event_processor import (
    EnergyMarketEventProcessor,
)

pytestmark = pytest.mark.integration


def _create_session_facatory(postgres_engine: Engine) -> sessionmaker[Session]:
    return sessionmaker(
        bind=postgres_engine,
        autoflush=False,
        expire_on_commit=False,
    )


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


def _count_rows(session: Session, table_name: str) -> int:
    result = session.execute(text(f"SELECT COUNT(*) FROM {table_name}")).scalar_one()
    return int(result)


def _fetch_snapshot_event_id(session: Session, market_area: str) -> UUID:
    result = session.execute(
        text(
            """
            SELECT last_event_id
            FROM market_snapshot
            WHERE market_area = :market_area
            """
        ),
        {"market_area": market_area},
    ).scalar_one()

    return cast(UUID, result)


def test_process_valid_event_persists_raw_event_and_updates_snapshot(
    postgres_engine: Engine, session: Session
) -> None:
    processor = EnergyMarketEventProcessor(_create_session_facatory(postgres_engine))
    event = _create_energy_market_event()

    result = processor.process(event)

    assert result.raw_event_inserted is True
    assert result.snapshot_updated is True
    assert result.duplicate_event is False
    assert result.stale_event is False

    assert _count_rows(session, "raw_energy_market_events") == 1
    assert _count_rows(session, "market_snapshot") == 1
    assert _fetch_snapshot_event_id(session, "DK1") == event.event_id


def test_process_event_generates_alerts_only_when_snapshot_is_updated(
    postgres_engine: Engine, session: Session
) -> None:
    processor = EnergyMarketEventProcessor(_create_session_facatory(postgres_engine))

    event = _create_energy_market_event(
        electricity_price_dkk_mwh=Decimal("100.00"),
        imbalance_price_dkk_mwh=Decimal("800.00"),
    )

    result = processor.process(event)

    assert result.raw_event_inserted is True
    assert result.snapshot_updated is True
    assert result.inserted_alerts >= 1

    assert _count_rows(session, "market_alerts") >= 1


def test_process_duplicate_event_does_not_reprocess_derived_state(
    postgres_engine: Engine, session: Session
) -> None:
    processor = EnergyMarketEventProcessor(_create_session_facatory(postgres_engine))

    event = _create_energy_market_event(
        electricity_price_dkk_mwh=Decimal("100.00"),
        imbalance_price_dkk_mwh=Decimal("800.00"),
    )

    first_result = processor.process(event)
    second_result = processor.process(event)

    assert first_result.raw_event_inserted is True
    assert first_result.snapshot_updated is True

    assert second_result.raw_event_inserted is False
    assert second_result.snapshot_updated is False
    assert second_result.inserted_alerts == 0
    assert second_result.duplicate_event is True

    assert _count_rows(session, "raw_energy_market_events") == 1
    assert _count_rows(session, "market_snapshot") == 1


def test_process_stale_event_persists_raw_event_but_does_not_update_snapshot_or_alerts(
    postgres_engine: Engine, session: Session
) -> None:
    processor = EnergyMarketEventProcessor(_create_session_facatory(postgres_engine))

    newer_event = _create_energy_market_event(
        timestamp=datetime(2026, 1, 1, 12, 10, tzinfo=UTC),
        electricity_price_dkk_mwh=Decimal("950.00"),
        imbalance_price_dkk_mwh=Decimal("1000.00"),
    )
    older_event = _create_energy_market_event(
        timestamp=datetime(2026, 1, 1, 12, 5, tzinfo=UTC),
        electricity_price_dkk_mwh=Decimal("100.00"),
        imbalance_price_dkk_mwh=Decimal("800.00"),
    )

    first_result = processor.process(newer_event)
    second_result = processor.process(older_event)

    assert first_result.raw_event_inserted is True
    assert first_result.snapshot_updated is True

    assert second_result.raw_event_inserted is True
    assert second_result.snapshot_updated is False
    assert second_result.inserted_alerts == 0
    assert second_result.stale_event is True

    assert _count_rows(session, "raw_energy_market_events") == 2
    assert _count_rows(session, "market_snapshot") == 1
    assert _fetch_snapshot_event_id(session, "DK1") == newer_event.event_id
