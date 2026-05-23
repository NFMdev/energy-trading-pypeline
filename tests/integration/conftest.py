from collections.abc import Generator
from pathlib import Path

import pytest
from sqlalchemy import Engine, create_engine, text
from sqlalchemy.orm import Session, sessionmaker
from testcontainers.postgres import PostgresContainer

pytestmark = pytest.mark.integration

PROJECT_ROOT = Path(__file__).resolve().parents[2]

MIGRATIONS_DIR = PROJECT_ROOT / "migrations"

TABLES_TO_TRUNCATE = (
    "market_alerts",
    "market_snapshot",
    "raw_energy_market_events",
)


def _to_sql_alchemy_psycopg_url(postgres_url: str) -> str:
    """
    Testcontainers can return a plain postgresql:// URL when driver=None is used.

    The application uses SQLAlchemy with psycopg, so integration tests should use
    the same SQLAlchemy driver as the application instead of falling back to psycopg2.
    """
    if postgres_url.startswith("postgresql+psycopg://"):
        return postgres_url

    if postgres_url.startswith("postgresql://"):
        return postgres_url.replace("postgresql://", "postgresql+psycopg://", 1)

    msg = f"Unsopported PostgreSQL URL format: {postgres_url}"
    raise ValueError(msg)


def _apply_migration(engine: Engine) -> None:
    migration_files = sorted(MIGRATIONS_DIR.glob("*.sql"))

    if not migration_files:
        msg = f"No SQL migrations found in {MIGRATIONS_DIR}"
        raise RuntimeError(msg)

    with engine.begin() as connection:
        for migration_file in migration_files:
            sql = migration_file.read_text(encoding="utf-8")
            connection.exec_driver_sql(sql)


def _truncate_tables(session: Session) -> None:
    tables = ", ".join(TABLES_TO_TRUNCATE)
    session.execute(text(f"Truncate TABLE {tables} RESTART IDENTITY CASCADE"))
    session.commit()


@pytest.fixture(scope="session")
def postgres_database_url() -> Generator[str, None, None]:
    with PostgresContainer("postgres:18", driver=None) as postgres:
        raw_url = postgres.get_connection_url()
        yield _to_sql_alchemy_psycopg_url(raw_url)


@pytest.fixture(scope="session")
def postgres_engine(postgres_database_url: str) -> Generator[Engine, None, None]:
    engine = create_engine(postgres_database_url, pool_pre_ping=True)
    _apply_migration(engine)

    try:
        yield engine
    finally:
        engine.dispose()


@pytest.fixture()
def session(postgres_engine: Engine) -> Generator[Session, None, None]:
    session_factory = sessionmaker(
        bind=postgres_engine,
        autoflush=False,
        expire_on_commit=False,
    )

    session = session_factory()

    try:
        _truncate_tables(session)
        yield session
        session.rollback()
    finally:
        session.close()
