from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import Session

from energy_trading_pypeline.domain.alerts import MarketAlert
from energy_trading_pypeline.domain.energy_market_event import EnergyMarketEvent
from energy_trading_pypeline.domain.market_snapshot import MarketSnapshot
from energy_trading_pypeline.persistence.schema import (
    market_alerts,
    market_snapshot,
    raw_energy_market_events,
)


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
    
class MarketSnapshotRepository:
    def __init__(self, session: Session) -> None:
        self._session = session

    def upsert_snapshot(self, snapshot: MarketSnapshot) -> bool:
        insert_statement = insert(market_snapshot).values(
            market_area=snapshot.market_area,
            last_event_id=snapshot.last_event_id,
            last_event_timestamp=snapshot.last_event_timestamp,
            electricity_price_dkk_mwh=snapshot.electricity_price_dkk_mwh,
            imbalance_price_dkk_mwh=snapshot.imbalance_price_dkk_mwh,
            wind_forecast_error_mw=snapshot.wind_forecast_error_mw,
            solar_forecast_error_mw=snapshot.solar_forecast_error_mw,
            renewable_actual_mw=snapshot.renewable_actual_mw,
            net_load_mw=snapshot.net_load_mw,
            imbalance_spread_dkk_mwh=snapshot.imbalance_spread_dkk_mwh,
            quality_flag=snapshot.quality_flag,
        )

        upsert_statement = (
            insert_statement.on_conflict_do_update(
                index_elements=[market_snapshot.c.market_area],
                set_={
                    "last_event_id": insert_statement.excluded.last_event_id,
                    "last_event_timestamp": insert_statement.excluded.last_event_timestamp,
                    "electricity_price_dkk_mwh": 
                    insert_statement.excluded.electricity_price_dkk_mwh,
                    "imbalance_price_dkk_mwh": insert_statement.excluded.imbalance_price_dkk_mwh,
                    "wind_forecast_error_mw": insert_statement.excluded.wind_forecast_error_mw,
                    "solar_forecast_error_mw": insert_statement.excluded.solar_forecast_error_mw,
                    "renewable_actual_mw": insert_statement.excluded.renewable_actual_mw,
                    "net_load_mw": insert_statement.excluded.net_load_mw,
                    "imbalance_spread_dkk_mwh": insert_statement.excluded.imbalance_spread_dkk_mwh,
                    "quality_flag": insert_statement.excluded.quality_flag,
                },
                where=(
                    insert_statement.excluded.last_event_timestamp
                    >= market_snapshot.c.last_event_timestamp
                ),
            ).returning(market_snapshot.c.market_area)
        )

        updated_market_area = self._session.execute(upsert_statement).scalar_one_or_none()
        return updated_market_area is not None
    
class MarketAlertRepository:
    def __init__(self, session: Session) -> None:
        self._session = session

    def save_alerts(self, alerts: list[MarketAlert]) -> int:
        if not alerts:
            return 0
        
        values = [
            {
                "alert_id": alert.alert_id,
                "market_area": alert.market_area,
                "alert_type": alert.alert_type,
                "severity": alert.severity,
                "message": alert.message,
                "observed_value": alert.observed_value,
                "threshold_value": alert.threshold_value,
                "last_event_id": alert.last_event_id,
                "event_timestamp": alert.event_timestamp,
                "created_at": alert.created_at,
            }
            for alert in alerts
        ]

        statement = (
            insert(market_alerts)
            .values(values)
            .on_conflict_do_nothing(
                index_elements=[market_alerts.c.alert_id],
            )
            .returning(market_alerts.c.id)
        )

        inserted_ids = self._session.execute(statement).scalars().all()

        return len(inserted_ids)