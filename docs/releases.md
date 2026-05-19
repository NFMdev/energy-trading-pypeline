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
