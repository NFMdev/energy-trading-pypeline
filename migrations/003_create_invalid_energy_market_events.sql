-- 003_create_invalid_energy_market_events.sql

CREATE TABLE IF NOT EXISTS INVALID_ENERGY_MARKET_EVENTS(
    id BIGSERIAL PRIMARY KEY,

    topic TEXT NOT NULL,
    kafka_partition INTEGER NOT NULL,
    kafka_offset BIGINT NOT NULL,
    kafka_key TEXT NULL,

    payload BYTEA NOT NULL,
    payload_text TEXT NULL,

    error_type TEXT NOT NULL,
    error_message TEXT NOT NULL,

    consumer_group TEXT NOT NULL,

    received_at TIMESTAMP NOT NULL DEFAULT now(),

    CONSTRAINT uq_invalid_energy_market_events_kafka_position
        UNIQUE (topic, kafka_partition, kafka_offset)
);

CREATE INDEX IF NOT EXISTS idx_invalid_energy_market_event_received_at
    ON invalid_energy_market_events (received_at);

CREATE INDEX IF NOT EXISTS idx_invalid_energy_market_event_consumer_group
    ON invalid_energy_market_events (consumer_group);

CREATE INDEX IF NOT EXISTS idx_invalid_energy_market_event_topi_partition_offset
    ON invalid_energy_market_events (topic, kafka_partition, kafka_offset);
