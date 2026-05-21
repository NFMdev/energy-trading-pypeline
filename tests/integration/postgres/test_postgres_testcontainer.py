import pytest
from sqlalchemy import text
from sqlalchemy.orm import Session

pytestmark = pytest.mark.integration


def test_postgres_container_is_available(db_session: Session) -> None:
    result = db_session.execute(text("SELECT 1")).scalar_one()

    assert result == 1


def test_migrations_create_expected_tables(db_session: Session) -> None:
    table_names = (
        db_session.execute(
            text(
                """
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = 'public'
            ORDER BY table_name
            """
            )
        )
        .scalars()
        .all()
    )

    assert "raw_energy_market_events" in table_names
    assert "market_snapshot" in table_names
    assert "market_alerts" in table_names
