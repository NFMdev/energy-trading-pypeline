from datetime import UTC, datetime
from decimal import Decimal
from typing import ClassVar, Literal
from uuid import UUID, uuid4

from pydantic import BaseModel, ConfigDict, Field, field_validator

QualityFlag = Literal["OK", "ESTIMATED", "MISSING", "SUSPECT"]


class EnergyMarketEvent(BaseModel):
    """
    Canonical event contract for synthetic energy market data.
    The event represents one observation for a market area at a specific point in time.
    """

    model_config = ConfigDict(frozen=True, extra="forbid")

    SUPPORTED_MARKET_AREAS: ClassVar[set[str]] = {"DK1", "DK2", "DE", "SE3", "NO2"}

    event_id: UUID = Field(default_factory=uuid4)
    schema_version: str = "energy.market.v1"

    market_area: str = Field(min_length=2, max_length=16)
    timestamp: datetime
    created_at: datetime = Field(default_factory=lambda: datetime.now(UTC))

    electricity_price_dkk_mwh: Decimal = Field(ge=Decimal("-3000"), le=Decimal("30000"))
    forecast_wind_mw: Decimal = Field(ge=Decimal("0"))
    actual_wind_mw: Decimal = Field(ge=Decimal("0"))
    forecast_solar_mw: Decimal = Field(ge=Decimal("0"))
    actual_solar_mw: Decimal = Field(ge=Decimal("0"))
    load_mw: Decimal = Field(ge=Decimal("0"))
    imbalance_price_dkk_mwh: Decimal = Field(ge=Decimal("-3000"), le=Decimal("30000"))

    source: str = Field(min_length=3, max_length=64)
    quality_flag: QualityFlag = "OK"

    @field_validator("timestamp", "created_at")
    @classmethod
    def require_timezone(cls, value: datetime) -> datetime:
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("datetime fields must be timezone-aware")
        return value

    @field_validator("market_area")
    @classmethod
    def normalize_market_area(cls, value: str) -> str:
        normalized_value: str = value.strip().upper()
        if normalized_value not in cls.SUPPORTED_MARKET_AREAS:
            supported_values = ", ".join(sorted(cls.SUPPORTED_MARKET_AREAS))
            raise ValueError(
                f"Unsupported market_area '{value}'. Supported values: {supported_values}"
            )

        return normalized_value
