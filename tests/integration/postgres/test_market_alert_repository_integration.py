from datetime import UTC, datetime
from decimal import Decimal
from typing import Any
from uuid import UUID, uuid4

import pytest
from sqlalchemy import text
from sqlalchemy.orm import Session

from energy_trading_pypeline.domain.alerts import MarketAlert
from energy_trading_pypeline.persistence.repositories import MarketAlertRepository

pytestmark = pytest.mark.integration


def _create_market_alert(
    alert_id: UUID | None = None,
    market_area: str = "DK1",
    alert_type: str = "HIGH_IMBALANCE_SPREAD",
    severity: str = "CRITICAL",
    message: str = "High imbalance spread detected for DK1",
    observed_value: Decimal = Decimal("750.00"),
    threshold_value: Decimal = Decimal("500.00"),
    last_event_id: UUID | None = None,
    event_timestamp: datetime | None = None,
    created_at: datetime | None = None,
) -> MarketAlert:
    timestamp = event_timestamp or datetime(2026, 1, 1, 12, 0, tzinfo=UTC)

    return MarketAlert(
        alert_id=alert_id or uuid4(),
        market_area=market_area,
        alert_type=alert_type,
        severity=severity,
        message=message,
        observed_value=observed_value,
        threshold_value=threshold_value,
        last_event_id=last_event_id or uuid4(),
        event_timestamp=timestamp,
        created_at=created_at or timestamp,
    )


def _fetch_market_alert(session: Session, alert_id: UUID) -> dict[str, Any]:
    row = (
        session.execute(
            text("SELECT * FROM market_alerts WHERE alert_id = :alert_id"),
            {"alert_id": alert_id},
        )
        .mappings()
        .one()
    )

    return dict(row)


def _count_alerts_by_alert_id(session: Session, alert_id: UUID) -> int:
    result = session.execute(
        text("SELECT COUNT(*) FROM market_alerts WHERE alert_id = :alert_id"),
        {"alert_id": alert_id},
    ).scalar_one()

    return int(result)


def _count_alerts_by_market_area(session: Session, market_area: str) -> int:
    result = session.execute(
        text("SELECT COUNT(*) FROM market_alerts WHERE market_area = :market_area"),
        {"market_area": market_area},
    ).scalar_one()

    return int(result)


def test_insert_market_alert_persist_alert(session: Session) -> None:
    repository = MarketAlertRepository(session)
    alert = _create_market_alert()

    inserted = repository.save_alerts([alert])

    assert inserted == 1

    row = _fetch_market_alert(session, alert.alert_id)

    assert row["alert_id"] == alert.alert_id
    assert row["market_area"] == alert.market_area
    assert row["alert_type"] == alert.alert_type
    assert row["severity"] == alert.severity
    assert row["message"] == alert.message
    assert row["observed_value"] == alert.observed_value
    assert row["threshold_value"] == alert.threshold_value
    assert row["last_event_id"] == alert.last_event_id
    assert row["event_timestamp"] == alert.event_timestamp
    assert row["created_at"] == alert.created_at


def test_insert_market_alert_is_idempotent_for_duplicate_alert_id(session: Session) -> None:
    repository = MarketAlertRepository(session)
    alert = _create_market_alert()

    inserted = repository.save_alerts(alerts=[alert, alert])

    assert inserted == 1
    assert _count_alerts_by_alert_id(session, alert.alert_id) == 1


def test_insert_market_alert_id_does_not_overwrite_existing_alert(session: Session) -> None:
    repository = MarketAlertRepository(session)
    alert_id = uuid4()

    original_alert = _create_market_alert(
        alert_id=alert_id,
        message="Original alert",
    )
    duplicate_alert = _create_market_alert(
        alert_id=alert_id,
        market_area="DK2",
        alert_type="HIGH_NET_LOAD",
        severity="WARNING",
        message="Duplicate alert should not overwrite original",
        observed_value=Decimal("9000.00"),
        threshold_value=Decimal("6500.00"),
    )

    inserted = repository.save_alerts([original_alert, duplicate_alert])

    assert inserted == 1

    row = _fetch_market_alert(session, alert_id)

    assert row["market_area"] == "DK1"
    assert row["alert_type"] == "HIGH_IMBALANCE_SPREAD"
    assert row["severity"] == "CRITICAL"
    assert row["message"] == "Original alert"
    assert row["observed_value"] == Decimal("750.00")
    assert row["threshold_value"] == Decimal("500.00")
    assert _count_alerts_by_alert_id(session, alert_id)


def test_insert_multiple_alerts(session: Session) -> None:
    repository = MarketAlertRepository(session)

    first_alert = _create_market_alert()
    second_alert = _create_market_alert(
        alert_type="NEGATIVE_PRICE",
        severity="INFO",
        observed_value=Decimal("-25.00"),
        threshold_value=Decimal("0.00"),
    )
    third_alert = _create_market_alert(
        alert_type="SUSPECT_QUALITY_FLAG",
        severity="WARNING",
        observed_value=Decimal("1.00"),
        threshold_value=Decimal("1.00"),
    )

    inserted = repository.save_alerts([first_alert, second_alert, third_alert])

    assert inserted == 3
    assert _count_alerts_by_market_area(session, "DK1") == 3
