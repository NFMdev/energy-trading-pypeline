from datetime import UTC, datetime
from decimal import ROUND_HALF_UP, Decimal
from random import choice, uniform

from energy_trading_pypeline.domain.energy_market_event import EnergyMarketEvent

MARKET_AREAS = ["DK1", "DK2", "DE", "SE3", "NO2"]
SOURCE_NAME = "energy-generator"

def decimal_from_float(value: float) -> Decimal:
    return Decimal(str(value)).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)

def generate_energy_market_event() -> EnergyMarketEvent:
    market_area = choice(MARKET_AREAS)

    forecast_wind_mw = uniform(500, 4_500)
    actual_wind_mw = max(0, forecast_wind_mw + uniform(-600, 600))

    forecast_solar_mw = uniform(0, 2_500)
    actual_solar_mw = max(0, forecast_solar_mw + uniform(-350, 350))

    load_mw = uniform(2_500, 8_500)

    electricity_price = uniform(150, 1750)
    imbalance_price = electricity_price + uniform(-600, 1000)

    return EnergyMarketEvent(
        market_area=market_area,
        timestamp=datetime.now(UTC),
        electricity_price_dkk_mwh=decimal_from_float(electricity_price),
        forecast_wind_mw=decimal_from_float(forecast_wind_mw),
        actual_wind_mw=decimal_from_float(actual_wind_mw),
        forecast_solar_mw=decimal_from_float(forecast_solar_mw),
        actual_solar_mw=decimal_from_float(actual_solar_mw),
        load_mw=decimal_from_float(load_mw),
        imbalance_price_dkk_mwh=decimal_from_float(imbalance_price),
        source=SOURCE_NAME,
        quality_flag="OK"
    )

def main() -> None:
    event = generate_energy_market_event()
    print(event.model_dump_json(indent=2))

if __name__ == "__main__":
    main()