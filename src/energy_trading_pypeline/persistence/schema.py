from sqlalchemy import (
    UUID,
    BigInteger,
    Column,
    DateTime,
    MetaData,
    Numeric,
    String,
    Table,
    Text,
    func,
)
from sqlalchemy.dialects.postgresql import JSONB

metadata = MetaData()

raw_energy_market_events = Table(
    "raw_energy_market_events",
    metadata,
    Column("id", BigInteger, primary_key=True),
    Column("event_id", UUID(as_uuid=True), nullable=False, unique=True),
    Column("schema_version", Text, nullable=False),
    Column("market_area", String(length=16), nullable=False),
    Column("event_timestamp", DateTime(timezone=True), nullable=False),
    Column("created_at", DateTime(timezone=True), nullable=False),
    Column("electricity_price_dkk_mwh", Numeric(12, 2), nullable=False),
    Column("forecast_wind_mw", Numeric(12, 2), nullable=False),
    Column("actual_wind_mw", Numeric(12, 2), nullable=False),
    Column("forecast_solar_mw", Numeric(12, 2), nullable=False),
    Column("actual_solar_mw", Numeric(12, 2), nullable=False),
    Column("load_mw", Numeric(12, 2), nullable=False),
    Column("imbalance_price_dkk_mwh", Numeric(12, 2), nullable=False),
    Column("source", String(length=64), nullable=False),
    Column("quality_flag", String(length=16), nullable=False),
    Column("payload", JSONB, nullable=False),
    Column("validation_status", String(length=16), nullable=False),
    Column("validation_error", Text, nullable=False),
    Column("ingested_at", DateTime(timezone=True), nullable=False, server_default=func.now()),
)