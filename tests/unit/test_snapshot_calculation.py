from datetime import datetime, timezone
from decimal import Decimal

from energy_trading_pypeline.domain.energy_market_event import EnergyMarketEvent
from energy_trading_pypeline.domain.market_snapshot import calculate_snapshot


def test_calculate_snapshot_from_market_event() -> None:
    event = EnergyMarketEvent(
        market_area="DK1",
        timestamp=datetime.now(timezone.utc),
        electricity_price_eur_mwh=Decimal("100"),
        forecast_wind_mw=Decimal("1000"),
        actual_wind_mw=Decimal("900"),
        forecast_solar_mw=Decimal("500"),
        actual_solar_mw=Decimal("550"),
        load_mw=Decimal("3000"),
        imbalance_price_eur_mwh=Decimal("130"),
        source="test-generator",
        quality_flag="OK",
    )

    snapshot = calculate_snapshot(event)

    assert snapshot.market_area == "DK1"
    assert snapshot.wind_forecast_error_mw == Decimal("-100")
    assert snapshot.solar_forecast_error_mw == Decimal("50")
    assert snapshot.renewable_actual_mw == Decimal("1450")
    assert snapshot.net_load_mw == Decimal("1550")
    assert snapshot.imbalance_spread_eur_mwh == Decimal("30")