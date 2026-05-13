from datetime import UTC, datetime
from decimal import Decimal

import pytest
from pydantic import ValidationError

from energy_trading_pypeline.domain.energy_market_event import EnergyMarketEvent


def test_energy_market_event_accepts_valid_event() -> None:
    event = EnergyMarketEvent(
        market_area="dk1",
        timestamp=datetime.now(UTC),
        electricity_price_dkk_mwh=Decimal("715.00"),
        forecast_wind_mw=Decimal("1200"),
        actual_wind_mw=Decimal("1100"),
        forecast_solar_mw=Decimal("300"),
        actual_solar_mw=Decimal("280"),
        load_mw=Decimal("4500"),
        imbalance_price_dkk_mwh=Decimal("120.00"),
        source="test-generator",
        quality_flag="OK",
    )

    assert event.market_area == "DK1"


def test_energy_market_event_rejects_negative_load() -> None:
    with pytest.raises(ValidationError):
        EnergyMarketEvent(
            market_area="DK1",
            timestamp=datetime.now(UTC),
            electricity_price_dkk_mwh=Decimal("715.00"),
            forecast_wind_mw=Decimal("1200"),
            actual_wind_mw=Decimal("1100"),
            forecast_solar_mw=Decimal("300"),
            actual_solar_mw=Decimal("280"),
            load_mw=Decimal("-1"),
            imbalance_price_dkk_mwh=Decimal("900.00"),
            source="test-generator",
            quality_flag="OK",
        )


def test_energy_market_event_rejects_naive_datetime() -> None:
    with pytest.raises(ValidationError):
        EnergyMarketEvent(
            market_area="DK1",
            timestamp=datetime.now(),
            electricity_price_dkk_mwh=Decimal("715.00"),
            forecast_wind_mw=Decimal("1200"),
            actual_wind_mw=Decimal("1100"),
            forecast_solar_mw=Decimal("300"),
            actual_solar_mw=Decimal("280"),
            load_mw=Decimal("4500"),
            imbalance_price_dkk_mwh=Decimal("900.00"),
            source="test-generator",
            quality_flag="OK",
        )