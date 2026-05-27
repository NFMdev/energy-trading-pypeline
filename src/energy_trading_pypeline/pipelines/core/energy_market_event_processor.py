from dataclasses import dataclass

from sqlalchemy.orm import Session, sessionmaker

from energy_trading_pypeline.domain.alerts import evaluate_alerts
from energy_trading_pypeline.domain.energy_market_event import EnergyMarketEvent
from energy_trading_pypeline.domain.market_snapshot import calculate_snapshot
from energy_trading_pypeline.persistence.repository.repositories import (
    MarketAlertRepository,
    MarketSnapshotRepository,
    RawEnergyMarketEventRepository,
)


@dataclass(frozen=True)
class EnergyMarketEventProcessingResult:
    raw_event_inserted: bool
    snapshot_updated: bool
    inserted_alerts: int

    @property
    def duplicate_event(self) -> bool:
        return not self.raw_event_inserted

    @property
    def stale_event(self) -> bool:
        return self.raw_event_inserted and not self.snapshot_updated


class EnergyMarketEventProcessor:
    def __init__(self, session_factory: sessionmaker[Session]) -> None:
        self._session_factory = session_factory

    def process(self, event: EnergyMarketEvent) -> EnergyMarketEventProcessingResult:
        with self._session_factory.begin() as session:
            raw_event_repository = RawEnergyMarketEventRepository(session)
            snapshot_repository = MarketSnapshotRepository(session)
            alert_repository = MarketAlertRepository(session)

            raw_event_inserted = raw_event_repository.save_valid_event(event)

            if not raw_event_inserted:
                return EnergyMarketEventProcessingResult(
                    raw_event_inserted=False,
                    snapshot_updated=False,
                    inserted_alerts=0,
                )

            snapshot = calculate_snapshot(event)
            snapshot_updated = snapshot_repository.upsert_snapshot(snapshot)

            if not snapshot_updated:
                return EnergyMarketEventProcessingResult(
                    raw_event_inserted=True,
                    snapshot_updated=False,
                    inserted_alerts=0,
                )

            alerts = evaluate_alerts(snapshot)
            inserted_alerts = alert_repository.save_alerts(alerts)

            return EnergyMarketEventProcessingResult(
                raw_event_inserted=True,
                snapshot_updated=True,
                inserted_alerts=inserted_alerts,
            )
