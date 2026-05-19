from datetime import UTC, datetime
from decimal import Decimal

from energy_trading_pypeline.domain.energy_market_event import EnergyMarketEvent
from energy_trading_pypeline.domain.market_snapshot import calculate_snapshot


def test_calculate_snapshot_from_market_event() -> None:
    event = EnergyMarketEvent(
        market_area="DK1",
        timestamp=datetime.now(UTC),
        electricity_price_dkk_mwh=Decimal("800.00"),
        forecast_wind_mw=Decimal("1000.00"),
        actual_wind_mw=Decimal("900.00"),
        forecast_solar_mw=Decimal("500.00"),
        actual_solar_mw=Decimal("550.00"),
        load_mw=Decimal("3000.00"),
        imbalance_price_dkk_mwh=Decimal("900.00"),
        source="test-generator",
        quality_flag="OK",
    )

    snapshot = calculate_snapshot(event)

    assert snapshot.market_area == "DK1"
    assert snapshot.last_event_id == event.event_id
    assert snapshot.last_event_timestamp == event.timestamp
    assert snapshot.wind_forecast_error_mw == Decimal("-100.00")
    assert snapshot.solar_forecast_error_mw == Decimal("50.00")
    assert snapshot.renewable_actual_mw == Decimal("1450.00")
    assert snapshot.net_load_mw == Decimal("1550.00")
    assert snapshot.imbalance_spread_dkk_mwh == Decimal("100.00")
