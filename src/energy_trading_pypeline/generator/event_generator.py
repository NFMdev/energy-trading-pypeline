from datetime import UTC, datetime
from decimal import ROUND_HALF_UP, Decimal
from random import Random

from energy_trading_pypeline.domain.energy_market_event import EnergyMarketEvent

MARKET_AREAS = ["DK1", "DK2", "DE", "SE3", "NO2"]
SOURCE_NAME = "energy-generator"


class EventGenerator:
    def __init__(
        self,
        *,
        market_areas: list[str] = MARKET_AREAS,
        source: str = SOURCE_NAME,
        seed: int | None = None,
    ) -> None:
        self._market_areas = market_areas
        self._source = source
        self._random = Random(seed)

    def generate_energy_market_event(self) -> EnergyMarketEvent:
        market_area = self._random.choice(self._market_areas)

        forecast_wind_mw = self._random.uniform(500, 4_500)
        actual_wind_mw = max(0, forecast_wind_mw + self._random.uniform(-600, 600))

        forecast_solar_mw = self._random.uniform(0, 2_500)
        actual_solar_mw = max(0, forecast_solar_mw + self._random.uniform(-350, 350))

        load_mw = self._random.uniform(2_500, 8_500)

        electricity_price = self._random.uniform(150, 1750)
        imbalance_price = electricity_price + self._random.uniform(-600, 1000)

        return EnergyMarketEvent(
            market_area=market_area,
            timestamp=datetime.now(UTC),
            electricity_price_dkk_mwh=self._decimal_from_float(electricity_price),
            forecast_wind_mw=self._decimal_from_float(forecast_wind_mw),
            actual_wind_mw=self._decimal_from_float(actual_wind_mw),
            forecast_solar_mw=self._decimal_from_float(forecast_solar_mw),
            actual_solar_mw=self._decimal_from_float(actual_solar_mw),
            load_mw=self._decimal_from_float(load_mw),
            imbalance_price_dkk_mwh=self._decimal_from_float(imbalance_price),
            source=self._source,
            quality_flag="OK",
        )

    @staticmethod
    def _decimal_from_float(value: float) -> Decimal:
        return Decimal(str(value)).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
