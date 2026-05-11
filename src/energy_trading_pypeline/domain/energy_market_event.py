from datetime import datetime, timezone
from decimal import Decimal
from typing import Literal
from uuid import UUID, uuid4

from pydantic import BaseModel, Field, field_validator


QualityFlag = Literal["OK", "ESTIMATED", "MISSING", "SUSPECT"]

class EnergyMarketEvent(BaseModel):
    event_id: UUID = Field(default_factory=uuid4)
    schema_version: str = "energy.market.v1"

    market_area: str = Field(min_length=2, max_length=16)
    timestamp: datetime
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))

    elecricity_price_dkk_mwh: Decimal = Field(ge=Decimal("-3000"), le=Decimal("30000"))
    forecast_wind_mw: Decimal = Field(ge=Decimal("0"))
    actual_wind_mw: Decimal = Field(ge=Decimal("0"))
    forecast_solar_mw: Decimal = Field(ge=Decimal("0"))
    actual_solar_mw: Decimal = Field(ge=Decimal("0"))
    load_mw: Decimal = Field(ge=Decimal("0"))
    imbalance_price_dkk_mwh = Field(ge=Decimal("-3000"), le=Decimal("30000"))

    source: str = Field(min_length=3, max_length=64)
    quality_flag: QualityFlag = "0K"

    @field_validator("timestamp", "created_at")
    @classmethod
    def require_timezone(cls, value: datetime) -> datetime:
        if value.tzinfo is None:
            raise ValueError("datetime fields must be timezone-aware")
        return value
    
    @field_validator("market_area")
    @classmethod
    def normalize_market_area(cls, value: str) -> str:
        return value.strip().upper