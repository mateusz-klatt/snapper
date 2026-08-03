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
from snapper.data.ledger_triggers import drop_execution_annulment_immutability_triggers
from snapper.data.ledger_triggers import drop_execution_annulment_visibility_immutability_triggers
from snapper.data.ledger_triggers import drop_execution_immutability_triggers
from snapper.data.ledger_triggers import install_execution_annulment_immutability_triggers
from snapper.data.ledger_triggers import (
    install_execution_annulment_visibility_immutability_triggers,
)
from snapper.data.ledger_triggers import install_execution_immutability_triggers
from snapper.data.models import Execution
from snapper.data.models import ExecutionAnnulment

_MIGRATION_MODULE = "snapper.data.migrations.versions.0030_executions_immutability_triggers"
_ANNULMENT_MIGRATION_MODULE = "snapper.data.migrations.versions.0037_execution_annulments"
_SQLITE_TRIGGER_QUERY = (
    "SELECT name FROM sqlite_master WHERE type='trigger' AND tbl_name='executions' ORDER BY name"
)
_SQLITE_ANNULMENT_TRIGGER_QUERY = (
    "SELECT name FROM sqlite_master WHERE type='trigger' "
    "AND tbl_name='execution_annulments' ORDER BY name"
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


@pytest.fixture
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


@pytest.fixture
def sqlite_annulment_connection() -> Iterator[Connection]:
    """Create ``execution_annulments`` on a real in-memory SQLite connection.

    Creating the table fires its ``after_create`` event, which installs the
    manifest triggers through the shared helper, so the fixture yields a
    connection whose table already carries them.
    """
    engine = create_engine("sqlite://")
    ExecutionAnnulment.__table__.create(engine)
    connection = engine.connect()
    try:
        yield connection
    finally:
        connection.close()
        engine.dispose()


def _sqlite_annulment_trigger_names(connection: Connection) -> list[str]:
    """Return the manifest triggers present on a SQLite connection."""
    return [str(row[0]) for row in connection.execute(text(_SQLITE_ANNULMENT_TRIGGER_QUERY))]


def test_after_create_event_installs_both_sqlite_annulment_triggers(
    sqlite_annulment_connection: Connection,
) -> None:
    """Creating the manifest installs the update and delete rejection triggers.

    Given: A fresh in-memory SQLite database whose ``execution_annulments``
        table was just created (firing ``after_create``).
    When: The trigger catalog is read.
    Then: Both ``execution_annulments_reject_update`` and
        ``execution_annulments_reject_delete`` are present — the ``create_all``
        path installs them without any migration, exactly as it does for the
        executions ledger.
    """
    assert _sqlite_annulment_trigger_names(sqlite_annulment_connection) == [
        "execution_annulments_reject_delete",
        "execution_annulments_reject_update",
    ]


def test_sqlite_annulment_install_and_drop_round_trip(
    sqlite_annulment_connection: Connection,
) -> None:
    """Manifest install is idempotent and drop/reinstall restores both triggers.

    Given: A SQLite ``execution_annulments`` table already carrying the
        triggers.
    When: The install helper runs twice, then the drop helper runs, then the
        install helper runs once more.
    Then: The double install raises nothing and leaves both triggers (the
        ``IF NOT EXISTS`` guards), the drop empties the catalog (every drop is
        ``IF EXISTS``), and the reinstall restores both.
    """
    install_execution_annulment_immutability_triggers(sqlite_annulment_connection)
    install_execution_annulment_immutability_triggers(sqlite_annulment_connection)
    assert _sqlite_annulment_trigger_names(sqlite_annulment_connection) == [
        "execution_annulments_reject_delete",
        "execution_annulments_reject_update",
    ]
    drop_execution_annulment_immutability_triggers(sqlite_annulment_connection)
    assert _sqlite_annulment_trigger_names(sqlite_annulment_connection) == []
    install_execution_annulment_immutability_triggers(sqlite_annulment_connection)
    assert _sqlite_annulment_trigger_names(sqlite_annulment_connection) == [
        "execution_annulments_reject_delete",
        "execution_annulments_reject_update",
    ]


def test_postgres_annulment_install_emits_function_row_and_truncate_triggers() -> None:
    """The PostgreSQL manifest install mirrors the executions protocol exactly.

    Given: A recording connection reporting the ``postgresql`` dialect.
    When: The manifest install helper runs.
    Then: It emits the same seven-statement protocol the executions installer
        does — ``CREATE OR REPLACE FUNCTION`` first, then drop+create+``ENABLE
        ALWAYS`` for the ``BEFORE UPDATE OR DELETE`` row trigger and the same
        for the ``BEFORE TRUNCATE`` statement trigger — so TRUNCATE is refused
        and ``session_replication_role = replica`` cannot disable either
        trigger.
    """
    connection = _RecordingConnection("postgresql")
    install_execution_annulment_immutability_triggers(connection)
    statements = connection.statements
    assert len(statements) == 7
    assert statements[0].startswith(
        "CREATE OR REPLACE FUNCTION execution_annulments_reject_mutation()"
    )
    assert "append-only" in statements[0]
    assert statements[1].startswith(
        "DROP TRIGGER IF EXISTS execution_annulments_reject_row_mutation"
    )
    assert "BEFORE UPDATE OR DELETE ON execution_annulments" in statements[2]
    assert "FOR EACH ROW EXECUTE FUNCTION execution_annulments_reject_mutation()" in statements[2]
    assert statements[3] == (
        "ALTER TABLE execution_annulments "
        "ENABLE ALWAYS TRIGGER execution_annulments_reject_row_mutation"
    )
    assert statements[4].startswith("DROP TRIGGER IF EXISTS execution_annulments_reject_truncate")
    assert "BEFORE TRUNCATE ON execution_annulments" in statements[5]
    assert (
        "FOR EACH STATEMENT EXECUTE FUNCTION execution_annulments_reject_mutation()"
        in statements[5]
    )
    assert statements[6] == (
        "ALTER TABLE execution_annulments "
        "ENABLE ALWAYS TRIGGER execution_annulments_reject_truncate"
    )


def test_postgres_annulment_drop_emits_both_trigger_drops_then_function_drop() -> None:
    """The PostgreSQL manifest drop removes both triggers then the function.

    Given: A recording connection reporting the ``postgresql`` dialect.
    When: The manifest drop helper runs.
    Then: It emits the two ``DROP TRIGGER IF EXISTS`` statements followed by the
        ``DROP FUNCTION IF EXISTS`` — the function is dropped last because the
        triggers depend on it, and it is dropped at all because no
        ``after_create`` path ever removes it.
    """
    connection = _RecordingConnection("postgresql")
    drop_execution_annulment_immutability_triggers(connection)
    statements = connection.statements
    assert len(statements) == 3
    assert statements[0].startswith(
        "DROP TRIGGER IF EXISTS execution_annulments_reject_row_mutation"
    )
    assert statements[1].startswith("DROP TRIGGER IF EXISTS execution_annulments_reject_truncate")
    assert statements[2].startswith(
        "DROP FUNCTION IF EXISTS execution_annulments_reject_mutation()"
    )


def test_annulment_event_and_migration_share_one_install_authority() -> None:
    """The ``after_create`` event and migration 0037 resolve to one installer.

    Given: The models module (host of the manifest ``after_create`` listener)
        and migration 0037 (the production-table installer).
    When: Both are inspected for the installer symbols they call.
    Then: Both are the SAME function objects, so the two install paths emit
        byte-identical DDL by construction rather than by convention — the same
        guarantee the executions ledger has had since 0030.
    """
    migration = importlib.import_module(_ANNULMENT_MIGRATION_MODULE)
    assert (
        migration.install_execution_annulment_immutability_triggers
        is install_execution_annulment_immutability_triggers
    )
    assert (
        models.install_execution_annulment_immutability_triggers
        is install_execution_annulment_immutability_triggers
    )
    assert (
        migration.drop_execution_annulment_immutability_triggers
        is drop_execution_annulment_immutability_triggers
    )


def test_postgres_visibility_install_emits_function_row_and_truncate_triggers() -> None:
    """The PostgreSQL observation install mirrors the same protocol exactly.

    Given: A recording connection reporting the ``postgresql`` dialect.
    When: The visibility install helper runs.
    Then: It emits the same seven-statement protocol the other two installers
        do — ``CREATE OR REPLACE FUNCTION`` first, then drop+create+``ENABLE
        ALWAYS`` for the ``BEFORE UPDATE OR DELETE`` row trigger and the same
        for the ``BEFORE TRUNCATE`` statement trigger. The observation plane
        needs every one of them: a movable ``observed_at`` would forge a
        correction's durability proof, and TRUNCATE or a replica-role bypass
        would erase it wholesale.
    """
    connection = _RecordingConnection("postgresql")
    install_execution_annulment_visibility_immutability_triggers(connection)
    statements = connection.statements
    assert len(statements) == 7
    assert statements[0].startswith(
        "CREATE OR REPLACE FUNCTION execution_annulment_visibility_reject_mutation()"
    )
    assert "append-only" in statements[0]
    assert statements[1].startswith(
        "DROP TRIGGER IF EXISTS execution_annulment_visibility_reject_row_mutation"
    )
    assert "BEFORE UPDATE OR DELETE ON execution_annulment_visibility" in statements[2]
    assert (
        "FOR EACH ROW EXECUTE FUNCTION execution_annulment_visibility_reject_mutation()"
        in statements[2]
    )
    assert statements[3] == (
        "ALTER TABLE execution_annulment_visibility "
        "ENABLE ALWAYS TRIGGER execution_annulment_visibility_reject_row_mutation"
    )
    assert statements[4].startswith(
        "DROP TRIGGER IF EXISTS execution_annulment_visibility_reject_truncate"
    )
    assert "BEFORE TRUNCATE ON execution_annulment_visibility" in statements[5]
    assert (
        "FOR EACH STATEMENT EXECUTE FUNCTION "
        "execution_annulment_visibility_reject_mutation()" in statements[5]
    )
    assert statements[6] == (
        "ALTER TABLE execution_annulment_visibility "
        "ENABLE ALWAYS TRIGGER execution_annulment_visibility_reject_truncate"
    )


def test_postgres_visibility_drop_emits_both_trigger_drops_then_function_drop() -> None:
    """The PostgreSQL observation drop removes both triggers then the function.

    Given: A recording connection reporting the ``postgresql`` dialect.
    When: The visibility drop helper runs.
    Then: It emits the two ``DROP TRIGGER IF EXISTS`` statements followed by the
        ``DROP FUNCTION IF EXISTS`` — the function last because the triggers
        depend on it, and at all because no ``after_create`` path removes it.
    """
    connection = _RecordingConnection("postgresql")
    drop_execution_annulment_visibility_immutability_triggers(connection)
    statements = connection.statements
    assert len(statements) == 3
    assert statements[0].startswith(
        "DROP TRIGGER IF EXISTS execution_annulment_visibility_reject_row_mutation"
    )
    assert statements[1].startswith(
        "DROP TRIGGER IF EXISTS execution_annulment_visibility_reject_truncate"
    )
    assert statements[2].startswith(
        "DROP FUNCTION IF EXISTS execution_annulment_visibility_reject_mutation()"
    )
