"""Tests for physical AI-research artifact immutability triggers."""

import importlib
from collections.abc import Iterator
from types import SimpleNamespace
from typing import cast

import pytest
from sqlalchemy import create_engine
from sqlalchemy import text
from sqlalchemy.engine import Connection
from sqlalchemy.exc import DBAPIError

from snapper.data import models
from snapper.data.ai_research_triggers import drop_ai_research_immutability_triggers
from snapper.data.ai_research_triggers import install_ai_research_immutability_triggers
from snapper.data.models import MarketView
from snapper.data.models import MarketViewSource

_MIGRATION_MODULE = "snapper.data.migrations.versions.0036_ai_research_persistence"
_TRIGGER_QUERY = (
    "SELECT name FROM sqlite_master WHERE type='trigger' "
    "AND tbl_name IN ('market_views', 'market_view_sources') ORDER BY name"
)


class _RecordingConnection:
    """Record DDL emitted for a selected dialect without a live server."""

    def __init__(self, dialect_name: str) -> None:
        """Initialize the reported dialect and empty statement log."""
        self.dialect = SimpleNamespace(name=dialect_name)
        self.statements: list[str] = []

    def execute(self, clause: object) -> None:
        """Append normalized SQL text to the statement log."""
        self.statements.append(" ".join(str(clause).split()))


@pytest.fixture
def sqlite_artifact_connection() -> Iterator[Connection]:
    """Create both artifact tables through their ORM DDL event path."""
    engine = create_engine("sqlite://")
    MarketView.__table__.create(engine)
    MarketViewSource.__table__.create(engine)
    connection = engine.connect()
    try:
        yield connection
    finally:
        connection.close()
        engine.dispose()


def _trigger_names(connection: Connection) -> list[str]:
    """Return all SQLite trigger names for the two artifact tables."""
    return [str(row[0]) for row in connection.execute(text(_TRIGGER_QUERY))]


def _insert_artifact(connection: Connection) -> tuple[str, str]:
    """Insert one valid view and source directly for mutation probes."""
    view_id = "019bf000-0000-7000-8000-000000000001"
    source_id = "019bf000-0000-7000-8000-000000000002"
    connection.execute(
        text(
            "INSERT INTO market_views "
            "(public_id, research_round_public_id, trigger, status, as_of, valid_until, "
            "regime, bias, confidence, horizon_hours, key_risks, next_events, rationale) "
            "VALUES (:public_id, :round_id, 'periodic', 'completed', :as_of, :valid_until, "
            "'neutral', 'longs_ok', 0.75, 2, '[]', '[]', 'Balanced conditions.')"
        ),
        {
            "public_id": view_id,
            "round_id": "019bf000-0000-7000-8000-000000000003",
            "as_of": "2026-07-21 08:00:00+00:00",
            "valid_until": "2026-07-21 10:00:00+00:00",
        },
    )
    connection.execute(
        text(
            "INSERT INTO market_view_sources "
            "(public_id, market_view_public_id, ordinal, url, title, retrieved_at) "
            "VALUES (:public_id, :view_id, 0, 'https://example.com/source', "
            "'Source', :retrieved_at)"
        ),
        {
            "public_id": source_id,
            "view_id": view_id,
            "retrieved_at": "2026-07-21 07:55:00+00:00",
        },
    )
    connection.commit()
    return view_id, source_id


def test_orm_creation_installs_all_sqlite_triggers(
    sqlite_artifact_connection: Connection,
) -> None:
    """Each ORM-created artifact table receives update and delete guards."""
    assert _trigger_names(sqlite_artifact_connection) == [
        "market_view_sources_reject_delete",
        "market_view_sources_reject_update",
        "market_views_reject_delete",
        "market_views_reject_update",
    ]


def test_sqlite_install_drop_and_reinstall_are_idempotent(
    sqlite_artifact_connection: Connection,
) -> None:
    """The shared installer safely supports repeated install and removal."""
    install_ai_research_immutability_triggers(sqlite_artifact_connection)
    install_ai_research_immutability_triggers(sqlite_artifact_connection)
    assert len(_trigger_names(sqlite_artifact_connection)) == 4
    drop_ai_research_immutability_triggers(sqlite_artifact_connection)
    drop_ai_research_immutability_triggers(sqlite_artifact_connection)
    assert _trigger_names(sqlite_artifact_connection) == []
    install_ai_research_immutability_triggers(sqlite_artifact_connection)
    assert len(_trigger_names(sqlite_artifact_connection)) == 4


def test_sqlite_physically_rejects_update_and_delete_on_both_tables(
    sqlite_artifact_connection: Connection,
) -> None:
    """Raw SQL cannot revise or remove committed views or citations."""
    view_id, source_id = _insert_artifact(sqlite_artifact_connection)
    mutations = (
        "UPDATE market_views SET rationale = 'changed'",
        "DELETE FROM market_views",
        "UPDATE market_view_sources SET title = 'changed'",
        "DELETE FROM market_view_sources",
    )
    for statement in mutations:
        with pytest.raises(DBAPIError, match="insert-only"):
            sqlite_artifact_connection.execute(text(statement))
        sqlite_artifact_connection.rollback()

    assert (
        sqlite_artifact_connection.execute(text("SELECT public_id FROM market_views")).scalar_one()
        == view_id
    )
    assert (
        sqlite_artifact_connection.execute(
            text("SELECT public_id FROM market_view_sources")
        ).scalar_one()
        == source_id
    )


def test_postgresql_install_emits_all_row_and_truncate_guards() -> None:
    """PostgreSQL receives fail-closed guards for both artifact tables."""
    recorder = _RecordingConnection("postgresql")
    install_ai_research_immutability_triggers(cast(Connection, recorder))
    statements = recorder.statements
    rendered = "\n".join(statements)
    assert len(statements) == 13
    assert statements[0].startswith(
        "CREATE OR REPLACE FUNCTION ai_research_reject_market_artifact_mutation()"
    )
    for table_name in ("market_views", "market_view_sources"):
        assert f"BEFORE UPDATE OR DELETE ON {table_name}" in rendered
        assert f"BEFORE TRUNCATE ON {table_name}" in rendered
        assert (
            f"ALTER TABLE {table_name} ENABLE ALWAYS TRIGGER {table_name}_reject_row_mutation"
        ) in statements
        assert (
            f"ALTER TABLE {table_name} ENABLE ALWAYS TRIGGER {table_name}_reject_truncate"
        ) in statements


def test_postgresql_drop_removes_table_triggers_before_function() -> None:
    """PostgreSQL drops all four triggers before the shared function."""
    recorder = _RecordingConnection("postgresql")
    drop_ai_research_immutability_triggers(cast(Connection, recorder))
    assert len(recorder.statements) == 5
    assert all(
        statement.startswith("DROP TRIGGER IF EXISTS") for statement in recorder.statements[:4]
    )
    assert recorder.statements[-1] == (
        "DROP FUNCTION IF EXISTS ai_research_reject_market_artifact_mutation()"
    )


def test_models_and_migration_share_the_trigger_installer() -> None:
    """ORM and Alembic creation paths resolve to one DDL authority."""
    migration = importlib.import_module(_MIGRATION_MODULE)
    assert (
        models.install_ai_research_immutability_triggers
        is install_ai_research_immutability_triggers
    )
    assert (
        migration.install_ai_research_immutability_triggers
        is install_ai_research_immutability_triggers
    )
    assert (
        migration.drop_ai_research_immutability_triggers is drop_ai_research_immutability_triggers
    )
