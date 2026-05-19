-- migrations/002_create_market_alerts.sql

CREATE TABLE IF NOT EXISTS market_alerts (
    id BIGSERIAL PRIMARY KEY,

    alert_id UUID NOT NULL UNIQUE,

    market_area TEXT NOT NULL,
    alert_type TEXT NOT NULL,
    severity TEXT NOT NULL,

    message TEXT NOT NULL,
    observed_value NUMERIC(14, 2) NOT NULL,
    threshold_value NUMERIC(14, 2) NOT NULL,

    last_event_id UUID NOT NULL,
    event_timestamp TIMESTAMPTZ NOT NULL,

    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_market_alerts_market_area_created_at
ON market_alerts (market_area, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_market_alerts_alert_type_created_at
ON market_alerts (alert_type, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_market_alerts_severity_created_at
ON market_alerts (severity, created_at DESC);
