-- migrations/001_create_energy_market_tables.sql

CREATE TABLE IF NOT EXISTS raw_energy_market_events (
    id BIGSERIAL PRIMARY KEY,

    event_id UUID NOT NULL UNIQUE,
    schema_version TEXT NOT NULL,

    market_area TEXT NOT NULL,
    event_timestamp TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,

    electricity_price_dkk_mwh NUMERIC(12, 2) NOT NULL,
    forecast_wind_mw NUMERIC(12, 2) NOT NULL,
    actual_wind_mw NUMERIC(12, 2) NOT NULL,
    forecast_solar_mw NUMERIC(12, 2) NOT NULL,
    actual_solar_mw NUMERIC(12, 2) NOT NULL,
    load_mw NUMERIC(12, 2) NOT NULL,
    imbalance_price_dkk_mwh NUMERIC(12, 2) NOT NULL,

    source TEXT NOT NULL,
    quality_flag TEXT NOT NULL,

    payload JSONB NOT NULL,

    validation_status TEXT NOT NULL DEFAULT 'VALID',
    validation_error TEXT NULL,

    ingested_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_raw_energy_market_events_market_area_timestamp
ON raw_energy_market_events (market_area, event_timestamp DESC);

CREATE INDEX IF NOT EXISTS idx_raw_energy_market_events_event_timestamp
ON raw_energy_market_events (event_timestamp DESC);

CREATE TABLE IF NOT EXISTS market_snapshot (
    market_area TEXT PRIMARY KEY,

    last_event_id UUID NOT NULL,
    last_event_timestamp TIMESTAMPTZ NOT NULL,

    electricity_price_dkk_mwh NUMERIC(12, 2) NOT NULL,
    imbalance_price_dkk_mwh NUMERIC(12, 2) NOT NULL,

    wind_forecast_error_mw NUMERIC(12, 2) NOT NULL,
    solar_forecast_error_mw NUMERIC(12, 2) NOT NULL,
    renewable_actual_mw NUMERIC(12, 2) NOT NULL,
    net_load_mw NUMERIC(12, 2) NOT NULL,
    imbalance_spread_dkk_mwh NUMERIC(12, 2) NOT NULL,

    quality_flag TEXT NOT NULL,

    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);