
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import Session

from energy_trading_pypeline.domain.energy_market_event import EnergyMarketEvent
from energy_trading_pypeline.persistence.schema import raw_energy_market_events


class RawEnergyMarketEventRepository:
    def __init__(self, session: Session) -> None:
        self._session = session
    
    def save_valid_event(self, event: EnergyMarketEvent) -> bool:
        statement = (
            insert(raw_energy_market_events)
            .values(
                event_id=event.event_id,
                schema_version=event.schema_version,
                market_area=event.market_area,
                event_timestamp=event.timestamp,
                created_at=event.created_at,
                electricity_price_dkk_mwh=event.electricity_price_dkk_mwh,
                forecast_wind_mw=event.forecast_wind_mw,
                actual_wind_mw=event.actual_wind_mw,
                forecast_solar_mw=event.forecast_solar_mw,
                actual_solar_mw=event.actual_solar_mw,
                load_mw=event.load_mw,
                imbalance_price_dkk_mwh=event.imbalance_price_dkk_mwh,
                source=event.source,
                quality_flag=event.quality_flag,
                payload=event.model_dump(mode="json"),
                validation_status="Valid",
                validation_error=None,
            )
            .on_conflict_do_nothing(
                index_elements=[raw_energy_market_events.c.event_id],
            )
            .returning(raw_energy_market_events.c.event_id)
        )

        inserted_id = self._session.execute(statement).scalar_one_or_none()

        return inserted_id is not None