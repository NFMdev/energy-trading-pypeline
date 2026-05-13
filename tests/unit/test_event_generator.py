from decimal import Decimal

from energy_trading_pypeline.generator.event_generator import generate_energy_market_event


def test_generate_energy_market_event_returns_valid_event() -> None:
    event = generate_energy_market_event()

    assert event.market_area in {"DK1", "DK2", "DE", "SE3", "NO2"}
    assert event.source == "energy-generator"
    assert event.quality_flag == "OK"

    assert event.electricity_price_dkk_mwh >= Decimal("-3000")
    assert event.forecast_wind_mw >= Decimal("0")
    assert event.actual_wind_mw >= Decimal("0")
    assert event.forecast_solar_mw >= Decimal("0")
    assert event.actual_solar_mw >= Decimal("0")
    assert event.load_mw >= Decimal("0")