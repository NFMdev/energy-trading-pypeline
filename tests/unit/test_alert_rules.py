from datetime import UTC, datetime
from decimal import Decimal
from uuid import UUID, uuid4

from energy_trading_pypeline.domain.alerts import AlertRuleConfig, evaluate_alerts
from energy_trading_pypeline.domain.energy_market_event import QualityFlag
from energy_trading_pypeline.domain.market_snapshot import MarketSnapshot


def create_snapshot(
    *,
    market_area: str = "DK1",
    last_event_id: UUID | None = None,
    last_event_timestamp: datetime | None = None,
    electricity_price_dkk_mwh: Decimal = Decimal("800.00"),
    imbalance_price_dkk_mwh: Decimal = Decimal("900.00"),
    wind_forecast_error_mw: Decimal = Decimal("100.00"),
    solar_forecast_error_mw: Decimal = Decimal("50.00"),
    renewable_actual_mw: Decimal = Decimal("3000.00"),
    net_load_mw: Decimal = Decimal("3500.00"),
    imbalance_spread_dkk_mwh: Decimal = Decimal("100.00"),
    quality_flag: QualityFlag = "OK",
) -> MarketSnapshot:
    return MarketSnapshot(
        market_area=market_area,
        last_event_id=last_event_id or uuid4(),
        last_event_timestamp=last_event_timestamp or datetime.now(UTC),
        electricity_price_dkk_mwh=electricity_price_dkk_mwh,
        imbalance_price_dkk_mwh=imbalance_price_dkk_mwh,
        wind_forecast_error_mw=wind_forecast_error_mw,
        solar_forecast_error_mw=solar_forecast_error_mw,
        renewable_actual_mw=renewable_actual_mw,
        net_load_mw=net_load_mw,
        imbalance_spread_dkk_mwh=imbalance_spread_dkk_mwh,
        quality_flag=quality_flag,
    )

def test_returns_no_alerts_when_snapshot_is_normal() -> None:
    snapshot = create_snapshot()

    alerts = evaluate_alerts(snapshot)

    assert alerts == []

def test_detects_high_imbalance_spread() -> None:
    snapshot = create_snapshot(
        imbalance_spread_dkk_mwh=Decimal("750.00"),
    )

    alerts = evaluate_alerts(snapshot)

    assert len(alerts) == 1
    assert alerts[0].alert_type == "HIGH_IMBALANCE_SPREAD"

def test_detects_high_wind_forecast_error_using_absolute_value() -> None:
    snapshot = create_snapshot(
        wind_forecast_error_mw=Decimal("-900.00"),
    )

    alerts = evaluate_alerts(snapshot)

    assert len(alerts) == 1
    assert alerts[0].alert_type == "HIGH_WIND_FORECAST_ERROR"
    assert alerts[0].observed_value == Decimal("900.00")

def test_detects_negative_price() -> None:
    snapshot = create_snapshot(
        electricity_price_dkk_mwh=Decimal("-50.00"),
    )

    alerts = evaluate_alerts(snapshot)

    assert len(alerts) == 1
    assert alerts[0].alert_type == "NEGATIVE_PRICE"

def test_detects_multiple_alerts() -> None:
    snapshot = create_snapshot(
        electricity_price_dkk_mwh=Decimal("-50.00"),
        imbalance_spread_dkk_mwh=Decimal("900.00"),
        net_load_mw=Decimal("7000.00"),
    )

    alerts = evaluate_alerts(snapshot)

    alert_types = {alert.alert_type for alert in alerts}

    assert alert_types == {
        "HIGH_IMBALANCE_SPREAD",
        "HIGH_NET_LOAD",
        "NEGATIVE_PRICE",
    }

def test_allows_custom_tresholds() -> None:
    snapshot = create_snapshot(
        imbalance_spread_dkk_mwh=Decimal("250.00"),
    )

    config = AlertRuleConfig(
        high_imbalance_spread_dkk_mwh=Decimal("200.00"),
    )

    alerts = evaluate_alerts(snapshot, config)

    assert len(alerts) == 1
    assert alerts[0].alert_type == "HIGH_IMBALANCE_SPREAD"