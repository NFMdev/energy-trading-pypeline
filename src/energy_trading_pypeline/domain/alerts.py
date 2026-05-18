from dataclasses import dataclass, field
import datetime
from decimal import Decimal
from typing import Literal
from uuid import UUID, uuid4

from energy_trading_pypeline.domain.market_snapshot import MarketSnapshot


AlertType = Literal[
    "HIGH_IMBALANCE_SPREAD",
    "HIGH_WIND_FORECAST_ERROR",
    "HIGH_SOLAR_FORECAST_ERROR",
    "NEGATIVE_PRICE",
    "HIGH_NET_LOAD",
    "SUSPECT_QUALITY_FLAG"
]

AlertSeverity = Literal["INFO", "WARINING", "CRITICAL"]

@dataclass(frozen=True)
class AlertRuleConfig:
    high_imbalance_spread_dkk_mwh: Decimal = Decimal("500.00")
    high_wind_forecast_error_mw: Decimal = Decimal("750.00")
    high_solar_forecast_error_mw: Decimal = Decimal("500.00")
    high_net_load: Decimal = Decimal("6500.00")

@dataclass(frozen=True)
class MarketAlert:
    alert_id: UUID
    market_area: str
    alert_type: AlertType
    severity: AlertSeverity
    message: str
    observed_value: Decimal
    threshold_value: Decimal
    last_event_id: UUID
    event_timestamp: datetime
    created_at: datetime = field(default_factory=lambda: datetime.now(datetime.UTC))

def evaluate_alerts(
        snapshot: MarketSnapshot,
        config: AlertRuleConfig | None = None,
) -> list[MarketAlert]:
    rule_config = config or AlertRuleConfig()
    alerts: list[MarketAlert] = []

    absolute_imbalance_spread = abs(snapshot.imbalance_spread_dkk_mwh)

    if absolute_imbalance_spread >= rule_config.high_imbalance_spread_dkk_mwh:
        alerts.append(
            _create_alert(
                snapshot,
                alert_type="HIGH_IMBALANCE_SPREAD",
                severity="CRITICAL",
                observed_value=absolute_imbalance_spread,
                threshold_value=rule_config.high_imbalance_spread_dkk_mwh,
                message=(
                    f"High imbalance spread detected in {snapshot.market_area}: "
                    f"{absolute_imbalance_spread} DKK/mwh"
                ),
            )
        )

    absolute_wind_error = abs(snapshot.wind_forecast_error_mw)

    if absolute_wind_error >= rule_config.high_wind_forecast_error_mw:
        alerts.append(
            _create_alert(
                snapshot,
                alert_type="HIGH_WIND_FORECAST_ERROR",
                severity="WARNING",
                observed_value=absolute_wind_error,
                threshold_value=rule_config.high_wind_forecast_error_mw,
                message=(
                    f"High wind forecast error detected in {snapshot.market_area}: "
                    f"{absolute_wind_error} MW"
                ),
            )
        )
    
    absolute_solar_error = abs(snapshot.solar_forecast_error_mw)

    if absolute_wind_error >= rule_config.high_solar_forecast_error_mw:
        alerts.append(
            _create_alert(
                snapshot,
                alert_type="HIGH_SOLAR_FORECAST_ERROR",
                severity="WARNING",
                observed_value=absolute_solar_error,
                threshold_value=rule_config.high_solar_forecast_error_mw,
                message=(
                    f"High solar forecast error detected in {snapshot.market_area}: "
                    f"{absolute_solar_error} MW"
                ),
            )
        )

    if snapshot.net_load_mw >= rule_config.high_net_load_mw:
        alerts.append(
            _create_alert(
                snapshot=snapshot,
                alert_type="HIGH_NET_LOAD",
                severity="WARNING",
                observed_value=snapshot.net_load_mw,
                threshold_value=rule_config.high_net_load_mw,
                message=(
                    f"High net load detected in {snapshot.market_area}: "
                    f"{snapshot.net_load_mw} MW"
                ),
            )
        )

    if snapshot.quality_flag == "SUSPECT":
        alerts.append(
            _create_alert(
                snapshot=snapshot,
                alert_type="SUSPECT_QUALITY_FLAG",
                severity="WARNING",
                observed_value=Decimal("1.00"),
                threshold_value=Decimal("1.00"),
                message=f"Suspect quality flag detected in {snapshot.market_area}",
            )
        )
    
    return alerts

def _create_alert(
    *,
    snapshot: MarketSnapshot,
    alert_type: AlertType,
    severity: AlertSeverity,
    observed_value: Decimal,
    threshold_value: Decimal,
    message: str,
) -> MarketAlert:
    return MarketAlert(
        alert_id=uuid4(),
        market_area=snapshot.market_area,
        alert_type=alert_type,
        severity=severity,
        message=message,
        observed_value=observed_value,
        threshold_value=threshold_value,
        last_event_id=snapshot.last_event_id,
        event_timestamp=snapshot.last_event_timestamp,
    )