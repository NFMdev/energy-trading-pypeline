# Releases

## v0.1.0 — Core Pipeline MVP

Initial MVP release of the Energy Trading Pipeline.

### Included

- Synthetic energy market event generation
- Pydantic validation
- Kafka-compatible producer using Redpanda locally
- Kafka-compatible consumer
- Manual offset commits
- PostgreSQL raw event persistence
- Timestamp-aware market snapshot upsert
- Derived market alerts
- Unit tests for domain models, repositories, serialization, snapshots and alert rules

### Processing model

The MVP uses:

```text
at-least-once processing
+
idempotent persistence
```

Offsets are committed only after the PostgreSQL transaction succeeds.

## v0.2.0 — Project hardening and developer workflow

Developer workflow and maintainability release.

### Included

- Makefile with standard development commands
- documented local configuration
- pre-commit hooks
- GitHub Actions CI
- local and remote quality gates

## v0.3 — Real Event processing Integration Testing

Release focused on infrastructure-backed integration testing.

### Included

- Separation between unit and integration test workflows
- Pytest markers for `unit` and `integration`
- Dedicated Makefile commands for:
  - `make test-unit`
  - `make test-integration`
  - `make test-all`
  - `make check`
  - `make check-all`
- PostgreSQL integration test infrastructure using Testcontainers
- Real SQL migrations applied during integration tests
- PostgreSQL-backed repository integration tests for:
  - `raw_energy_market_events`
  - `market_snapshot`
  - `market_alerts`
- Transactional event processor extracted from the Kafka consumer script
- PostgreSQL-backed integration tests for the event processor
- GitHub Actions job for PostgreSQL integration tests

### Validated behavior

The integration test suite validates that:

- raw energy market events are persisted idempotently
- duplicate `event_id` values do not overwrite existing raw events
- market snapshots are inserted for new market areas
- market snapshots update only when incoming events are newer or equal in timestamp
- stale events do not overwrite newer operational state
- market alerts are persisted as a derived event log
- duplicate `alert_id` values are idempotently ignored
- a valid event is processed transactionally into raw event, snapshot and alerts
- duplicate events do not reprocess derived state
- stale events are stored as raw history but do not update snapshots or generate alerts

## v0.4.0 — Dead-letter handling & invalid event persistence

### Included

- Added `invalid_energy_market_events` table.
- Added invalid event persistence for malformed or schema-invalid Kafka messages.
- Added `InvalidEnergyMarketEvent`.
- Added `InvalidEnergyMarketEventRepository`.
- Added `InvalidEnergyMarketEventProcessor`.
- Added integration tests for invalid event persistence.
- Added handling for invalid payloads before Kafka offset commit.

### Changed

- Invalid Kafka messages are no longer only logged and committed.
- Invalid messages are persisted before committing the Kafka offset.
- `consume_raw_events` now routes validation/deserialization failures to invalid event persistence.

## v0.5.0 — Operational Observability MVP / Continuous Local Runtime

### Added

- Added centralized logging configuration.
- Refactored synthetic producer to run continuously.
- Added runtime stats for producer and consumer.
- Added periodic operational summaries.
- Added configurable producer interval and max event count.
- Added configurable operational summary interval.

### Changed

- Producer and consumer can now be run continuously in local development.
- Runtime logs now expose produced, processed, duplicate, stale, invalid, alert and commit outcomes.
