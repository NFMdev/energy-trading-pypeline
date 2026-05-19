from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from uuid import UUID

from energy_trading_pypeline.domain.energy_market_event import EnergyMarketEvent, QualityFlag


@dataclass(frozen=True)
class MarketSnapshot:
    market_area: str
    last_event_id: UUID
    last_event_timestamp: datetime
    electricity_price_dkk_mwh: Decimal
    imbalance_price_dkk_mwh: Decimal
    wind_forecast_error_mw: Decimal
    solar_forecast_error_mw: Decimal
    renewable_actual_mw: Decimal
    net_load_mw: Decimal
    imbalance_spread_dkk_mwh: Decimal
    quality_flag: QualityFlag


def calculate_snapshot(event: EnergyMarketEvent) -> MarketSnapshot:
    wind_forecast_error = event.actual_wind_mw - event.forecast_wind_mw
    solar_forecast_error = event.actual_solar_mw - event.forecast_solar_mw

    renewable_actual = event.actual_wind_mw + event.actual_solar_mw
    net_load = event.load_mw - renewable_actual

    imbalance_spread = event.imbalance_price_dkk_mwh - event.electricity_price_dkk_mwh

    return MarketSnapshot(
        market_area=event.market_area,
        last_event_id=event.event_id,
        last_event_timestamp=event.timestamp,
        electricity_price_dkk_mwh=event.electricity_price_dkk_mwh,
        imbalance_price_dkk_mwh=event.imbalance_price_dkk_mwh,
        wind_forecast_error_mw=wind_forecast_error,
        solar_forecast_error_mw=solar_forecast_error,
        renewable_actual_mw=renewable_actual,
        net_load_mw=net_load,
        imbalance_spread_dkk_mwh=imbalance_spread,
        quality_flag=event.quality_flag,
    )
