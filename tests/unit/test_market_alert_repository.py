from datetime import UTC, datetime
from decimal import Decimal
from unittest.mock import Mock
from uuid import uuid4

from energy_trading_pypeline.domain.alerts import MarketAlert
from energy_trading_pypeline.persistence.repositories import MarketAlertRepository


def create_alert() -> MarketAlert:
    return MarketAlert(
        alert_id=uuid4(),
        market_area="DK1",
        alert_type="HIGH_IMBALANCE_SPREAD",
        severity="CRITICAL",
        message="High imbalance spread detected",
        observed_value=Decimal("750.00"),
        threshold_value=Decimal("500.00"),
        last_event_id=uuid4(),
        event_timestamp=datetime.now(UTC),
    )

def test_save_alerts_returns_zero_when_alert_list_is_empty() -> None:
    session = Mock()

    repository = MarketAlertRepository(session)

    inserted_alerts = repository.save_alerts([])

    assert inserted_alerts == 0
    session.execute.assert_not_called()

def test_save_alerts_returns_inserted_count() -> None:
    session = Mock()
    session.execute.return_value.scalars.return_value.all.return_value = [1, 2]

    repository = MarketAlertRepository(session)

    inserted_alerts = repository.save_alerts([create_alert(), create_alert()])

    assert inserted_alerts == 2
    session.execute.assert_called_once()