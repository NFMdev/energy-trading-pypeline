# Energy Trading Pypeline

Energy Trading Pypeline is a Python-based portfolio project that simulates a small internal data platform for energy market data.

The project is designed to practice professional Python in a realistic data engineering / energy trading context: typed data contracts, event ingestion, Kafka-compatible messaging, PostgreSQL persistence, derived operational state, alert logic, testing and technical documentation.

## Context

This project simulates a pipeline for synthetic energy market events.

### Current flow:

```text
Synthetic energy market data
        ↓
Pydantic validation
        ↓
Redpanda / Kafka-compatible topic
        ↓
Python consumer
        ↓
PostgreSQL raw event storage
        ↓
Market snapshot derivation
        ↓
Basic alert evaluation
```
### Goals

The project focuses on:

- Python project structure
- Pydantic data contracts
- Ingestion using Redpanda
- PostgreSQL storage
- Derived market snapshots
- Basic alert logic
- Type checking with mypy
- Testing with pytest
- Reproducible local infrastructure with Docker Compose

## Tech Stack

- Python 3.12
- Pydantic
- pydantic-settings
- pytest
- mypy
- ruff
- SQLAlchemy Core
- psycopg
- PostgreSQL
- Redpanda
- Docker Compose

## Main Concepts

### Raw Events

```text
Raw events are stored in raw_energy_market_events.

This table acts as an immutable event log for auditability, debugging and future replay.
```
### Market Snapshots

```text
The market_snapshot table stores the latest operational state per market_area.

A snapshot is only updated if the incoming event is at least as recent as the current stored state.
```

### Alerts

```text
The market_alerts table stores derived alert events based on market conditions such as:

- High imbalance spread
- High forecast error
- Negative electricity price
- High net load
- Suspect quality flag
```

## Getting Started

### 1. Install dependencies
```console
uv sync --dev
```
### 2. Start local infrastructure
```console
docker compose up -d