from datetime import UTC, datetime
from decimal import Decimal
from unittest.mock import Mock
from uuid import uuid4

from energy_trading_pypeline.domain.market_snapshot import MarketSnapshot
from energy_trading_pypeline.persistence.repositories import MarketSnapshotRepository


def create_snapshot() -> MarketSnapshot:
    return MarketSnapshot(
        market_area="DK1",
        last_event_id=uuid4(),
        last_event_timestamp=datetime.now(UTC),
        eletricity_price_dkk_mwh=Decimal("800.00"),
        imbalance_price_dkk_mwh=Decimal("950.00"),
        wind_forecast_error_mw=Decimal("-100.00"),
        solar_forecast_error_mw=Decimal("50.00"),
        renewable_actual_mw=Decimal("1450.00"),
        net_load_mw=Decimal("1550.00"),
        imbalance_spread_dkk_mwh=Decimal("150.00"),
        quality_flag="OK",
    )

def test_upsert_snapshot_return_true_when_inserted_or_updated() -> None:
    session = Mock()
    session.execute.return_value.scalar_one_or_none.return_value = "DK1"

    repository = MarketSnapshotRepository(session)
    updated = repository.upsert_snapshot(create_snapshot())

    assert updated is True
    session.execute.assert_called_once()

def test_upsert_snapshot_returns_false_when_event_is_stale() -> None:
    session = Mock()
    session.execute.return_value.scalar_one_or_none.return_value = None

    repository = MarketSnapshotRepository(session)
    updated = repository.upsert_snapshot(create_snapshot())

    assert updated is False
    session.execute.assert_called_once()