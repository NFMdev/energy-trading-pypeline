from datetime import UTC, datetime
from decimal import Decimal
from unittest.mock import Mock

from energy_trading_pypeline.domain.energy_market_event import EnergyMarketEvent
from energy_trading_pypeline.persistence.repositories import RawEnergyMarketEventRepository


def create_event() -> EnergyMarketEvent:
    return EnergyMarketEvent(
        market_area="DK1",
        timestamp=datetime.now(UTC),
        electricity_price_dkk_mwh=Decimal("750.00"),
        forecast_wind_mw=Decimal("1200.00"),
        actual_wind_mw=Decimal("1100.00"),
        forecast_solar_mw=Decimal("300.00"),
        actual_solar_mw=Decimal("280.00"),
        load_mw=Decimal("4500.00"),
        imbalance_price_dkk_mwh=Decimal("900.00"),
        source="test-generator",
        quality_flag="OK",
    )

def test_save_valid_event_returns_true_when_inserted() -> None:
    session = Mock()
    session.execute.return_value.scalar_one_or_none.return_value = 1

    repository = RawEnergyMarketEventRepository(session)

    inserted = repository.save_valid_event(create_event())
    assert inserted is True
    session.execute.assert_called_once()

def test_save_valid_event_returns_false_when_duplicate() -> None:
    session = Mock()

    session.execute.return_value.scalar_one_or_none.return_value = None

    repository = RawEnergyMarketEventRepository(session)

    inserted = repository.save_valid_event(create_event())
    assert inserted is False
    session.execute.assert_called_once()