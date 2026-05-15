from datetime import UTC, datetime
from decimal import Decimal

from energy_trading_pypeline.domain.energy_market_event import EnergyMarketEvent
from energy_trading_pypeline.messaging.serialization import (
    deserialize_energy_market_event,
    serialize_energy_market_event,
)


def test_serializes_and_deserializes_energy_market_event() -> None:
    event = EnergyMarketEvent(
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

    payload = serialize_energy_market_event(event)
    restored_event = deserialize_energy_market_event(payload)

    assert isinstance(payload, bytes)
    assert restored_event == event