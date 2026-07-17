"""Install-integrity tests for the executions append-only trigger DDL.

:mod:`snapper.data.ledger_triggers` is the single source of truth for the
physical append-only triggers: the ``after_create`` DDL event on
``Execution.__table__`` and migration 0030 both call
``install_execution_immutability_triggers`` on their bind, so the emitted
DDL is byte-identical by construction. These tests pin that single
authority (both callers resolve to the SAME function object), exercise the
SQLite install/drop lifecycle on a real connection, and pin the
PostgreSQL statement protocol through a recording connection so the PG
branch is covered without a live server (the live-dialect rejection itself
is proven in the adversarial modules).
"""

import importlib
from collections.abc import Iterator
from types import SimpleNamespace

import pytest
from sqlalchemy import create_engine
from sqlalchemy import text
from sqlalchemy.engine import Connection

from snapper.data import models
from snapper.data.ledger_triggers import drop_execution_immutability_triggers
from snapper.data.ledger_triggers import install_execution_immutability_triggers
from snapper.data.models import Execution

_MIGRATION_MODULE = "snapper.data.migrations.versions.0030_executions_immutability_triggers"
_SQLITE_TRIGGER_QUERY = (
    "SELECT name FROM sqlite_master WHERE type='trigger' AND tbl_name='executions' ORDER BY name"
)


class _RecordingConnection:
    """A stand-in connection that records executed statement text.

    Only ``dialect.name`` and ``execute`` are consulted by the installer,
    so this reproduces the PostgreSQL branch without a live server and lets
    the tests pin the exact statement protocol and order.
    """

    def __init__(self, dialect_name: str) -> None:
        """Record the reported dialect and start an empty statement log."""
        self.dialect = SimpleNamespace(name=dialect_name)
        self.statements: list[str] = []

    def execute(self, clause: object) -> None:
        """Append the rendered statement text to the log."""
        self.statements.append(" ".join(str(clause).split()))


@pytest.fixture()
def sqlite_executions_connection() -> Iterator[Connection]:
    """Create the ``executions`` table on a real in-memory SQLite connection.

    Creating the table fires the ``after_create`` event, which installs the
    triggers through the shared helper, so the fixture yields a connection
    whose ``executions`` table already carries them.
    """
    engine = create_engine("sqlite://")
    Execution.__table__.create(engine)
    connection = engine.connect()
    try:
        yield connection
    finally:
        connection.close()
        engine.dispose()


def _sqlite_trigger_names(connection: Connection) -> list[str]:
    """Return the executions triggers present on a SQLite connection."""
    return [str(row[0]) for row in connection.execute(text(_SQLITE_TRIGGER_QUERY))]


def test_after_create_event_installs_both_sqlite_triggers(
    sqlite_executions_connection: Connection,
) -> None:
    """Creating the table installs the update and delete rejection triggers.

    Given: A fresh in-memory SQLite database whose ``executions`` table was
        just created (firing ``after_create``).
    When: The trigger catalog is read.
    Then: Both ``executions_reject_update`` and ``executions_reject_delete``
        are present — the ``create_all`` path installed them without any
        migration.
    """
    assert _sqlite_trigger_names(sqlite_executions_connection) == [
        "executions_reject_delete",
        "executions_reject_update",
    ]


def test_sqlite_install_is_idempotent(sqlite_executions_connection: Connection) -> None:
    """Re-installing over existing triggers is a safe no-op.

    Given: A SQLite ``executions`` table already carrying the triggers.
    When: The install helper runs again on the same connection.
    Then: It raises nothing and the two triggers remain present (the
        ``IF NOT EXISTS`` guards make a double-install harmless).
    """
    install_execution_immutability_triggers(sqlite_executions_connection)
    install_execution_immutability_triggers(sqlite_executions_connection)
    assert _sqlite_trigger_names(sqlite_executions_connection) == [
        "executions_reject_delete",
        "executions_reject_update",
    ]


def test_sqlite_drop_removes_both_triggers_and_reinstall_restores_them(
    sqlite_executions_connection: Connection,
) -> None:
    """Drop clears the triggers and a subsequent install restores them.

    Given: A SQLite ``executions`` table carrying the triggers.
    When: The drop helper runs, then the install helper runs again.
    Then: The catalog is empty after the drop (both drops are
        ``IF EXISTS``) and both triggers are present again after reinstall.
    """
    drop_execution_immutability_triggers(sqlite_executions_connection)
    assert _sqlite_trigger_names(sqlite_executions_connection) == []
    install_execution_immutability_triggers(sqlite_executions_connection)
    assert _sqlite_trigger_names(sqlite_executions_connection) == [
        "executions_reject_delete",
        "executions_reject_update",
    ]


def test_postgres_install_emits_function_row_and_truncate_triggers() -> None:
    """The PostgreSQL install emits the function, both triggers, and ENABLE ALWAYS.

    Given: A recording connection reporting the ``postgresql`` dialect.
    When: The install helper runs.
    Then: It emits seven statements — the ``CREATE OR REPLACE FUNCTION``
        first, then a drop+create+``ENABLE ALWAYS`` for the ``BEFORE UPDATE
        OR DELETE`` row trigger and a drop+create+``ENABLE ALWAYS`` for the
        ``BEFORE TRUNCATE`` statement trigger; the function's ``RAISE``
        carries the ``append-only`` substring, both triggers reuse the
        single rejection function, and each is promoted to ``ENABLE ALWAYS``
        so it fires even under ``session_replication_role = replica``.
    """
    connection = _RecordingConnection("postgresql")
    install_execution_immutability_triggers(connection)
    statements = connection.statements
    assert len(statements) == 7
    assert statements[0].startswith("CREATE OR REPLACE FUNCTION executions_reject_mutation()")
    assert "append-only" in statements[0]
    assert statements[1].startswith("DROP TRIGGER IF EXISTS executions_reject_row_mutation")
    assert "BEFORE UPDATE OR DELETE ON executions" in statements[2]
    assert "FOR EACH ROW EXECUTE FUNCTION executions_reject_mutation()" in statements[2]
    assert (
        statements[3]
        == "ALTER TABLE executions ENABLE ALWAYS TRIGGER executions_reject_row_mutation"
    )
    assert statements[4].startswith("DROP TRIGGER IF EXISTS executions_reject_truncate")
    assert "BEFORE TRUNCATE ON executions" in statements[5]
    assert "FOR EACH STATEMENT EXECUTE FUNCTION executions_reject_mutation()" in statements[5]
    assert (
        statements[6] == "ALTER TABLE executions ENABLE ALWAYS TRIGGER executions_reject_truncate"
    )


def test_postgres_drop_emits_both_trigger_drops_then_function_drop() -> None:
    """The PostgreSQL drop removes both triggers and then the shared function.

    Given: A recording connection reporting the ``postgresql`` dialect.
    When: The drop helper runs.
    Then: It emits the two ``DROP TRIGGER IF EXISTS`` statements followed by
        the ``DROP FUNCTION IF EXISTS`` — the function is dropped last
        because the triggers depend on it.
    """
    connection = _RecordingConnection("postgresql")
    drop_execution_immutability_triggers(connection)
    statements = connection.statements
    assert len(statements) == 3
    assert statements[0].startswith("DROP TRIGGER IF EXISTS executions_reject_row_mutation")
    assert statements[1].startswith("DROP TRIGGER IF EXISTS executions_reject_truncate")
    assert statements[2].startswith("DROP FUNCTION IF EXISTS executions_reject_mutation()")


def test_event_and_migration_share_one_install_authority() -> None:
    """The ``after_create`` event and migration 0030 resolve to one installer.

    Given: The models module (host of the ``after_create`` listener) and
        migration 0030 (the production-table installer).
    When: Both are inspected for the installer symbol they call.
    Then: Both are the SAME ``install_execution_immutability_triggers``
        function object, so the two install paths emit byte-identical DDL
        by construction rather than by convention.
    """
    migration = importlib.import_module(_MIGRATION_MODULE)
    assert (
        migration.install_execution_immutability_triggers is install_execution_immutability_triggers
    )
    assert models.install_execution_immutability_triggers is install_execution_immutability_triggers
    assert migration.drop_execution_immutability_triggers is drop_execution_immutability_triggers
