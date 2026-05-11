from datetime import datetime, timezone
from decimal import Decimal
from random import uniform

from energy_trading_pypeline.domain.energy_market_event import EnergyMarketEvent


MARKET_AREAS = ["DK1", "DK2", "DE", "SE3", "NO2"];

def decimal_from_float(value: float) -> Decimal:
    return Decimal(str(round(value, 2)))

def generate_energy_market_event() -> EnergyMarketEvent:
    market_area = choice(MARKET_AREAS)

    forecast_wind = uniform(3000, 27500)
    actual_wind = forecast_wind + uniform(-3000, 3000)

    forecast_solar = uniform(0, 15000)
    actual_solar = forecast_solar + uniform(-2000, 2000)

    load = uniform(15000, 45000)

    electricity_price = uniform(150, 1750)
    imbalance_price = electricity_price + uniform(-600, 1000)

    return EnergyMarketEvent(
        market_area=market_area,
        timestamp=datetime.now(timezone.utc),
        elecricity_price_dkk_mwh=decimal_from_float(electricity_price),
        forecast_wind_mw=decimal_from_float(forecast_wind),
        actual_wind_mw=actual_wind,
        forecast_solar_mw=forecast_solar,
        actual_solar_mw=actual_solar,
        load_mw=load,
        source="event-generator",
        quality_flag="OK"
    )

def main() -> None:
    event = generate_energy_market_event()
    print(event.model_dump_json(indent=2))

if __name__ == "__main__":
    main()