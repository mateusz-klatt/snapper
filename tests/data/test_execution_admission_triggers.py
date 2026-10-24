"""Physical append-only trigger witnesses shared by migration and create-all evidence stores."""

from typing import cast
from unittest.mock import MagicMock

import pytest
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import dialect as postgres_dialect
from sqlalchemy.exc import IntegrityError

from snapper.data.execution_admission_triggers import ADMISSION_EVIDENCE_TABLES
from snapper.data.execution_admission_triggers import AdmissionEvidenceTable
from snapper.data.execution_admission_triggers import drop_execution_admission_triggers
from snapper.data.execution_admission_triggers import install_execution_admission_table_triggers
from snapper.data.execution_admission_triggers import install_execution_admission_triggers


@pytest.mark.parametrize("table", ADMISSION_EVIDENCE_TABLES)
@pytest.mark.parametrize("operation", ("UPDATE", "DELETE", "REPLACE"))
def test_sqlite_installed_guards_reject_actual_mutation_and_replacement(
    table: AdmissionEvidenceTable, operation: str
) -> None:
    """The shared helper installs physical guards independently of ORM mutation hooks.

    Given: Two real SQLite evidence tables, recursive triggers and one immutable row,
    When: Raw SQL attempts UPDATE, DELETE or replacement after repeated guard installation,
    Then: The physical guard rejects it and leaves the original row and payload intact.
    """
    engine = sa.create_engine("sqlite://")
    try:
        with engine.begin() as connection:
            connection.exec_driver_sql("PRAGMA recursive_triggers=ON")
            for name in ADMISSION_EVIDENCE_TABLES:
                connection.exec_driver_sql(
                    f"CREATE TABLE {name} (id INTEGER PRIMARY KEY, payload TEXT NOT NULL)"
                )
            install_execution_admission_triggers(connection)
            install_execution_admission_triggers(connection)
            connection.exec_driver_sql(f"INSERT INTO {table} VALUES (1, 'original')")
            statements = {
                "UPDATE": f"UPDATE {table} SET payload='changed'",
                "DELETE": f"DELETE FROM {table}",
                "REPLACE": f"INSERT OR REPLACE INTO {table} VALUES (1, 'changed')",
            }
            with pytest.raises(IntegrityError, match="append-only"):
                connection.exec_driver_sql(statements[operation])
            assert connection.scalar(sa.text(f"SELECT payload FROM {table}")) == "original"
            drop_execution_admission_triggers(connection)
            drop_execution_admission_triggers(connection)
            connection.exec_driver_sql(f"UPDATE {table} SET payload='explicitly-unprotected'")
            connection.exec_driver_sql(f"DELETE FROM {table}")
            assert connection.scalar(sa.text(f"SELECT COUNT(*) FROM {table}")) == 0
    finally:
        engine.dispose()


@pytest.mark.parametrize("table", ADMISSION_EVIDENCE_TABLES)
def test_postgresql_installs_always_row_and_statement_guards(
    table: AdmissionEvidenceTable,
) -> None:
    """PostgreSQL row and TRUNCATE guards use ALWAYS activation for replica sessions.

    Given: A PostgreSQL-dialect connection and one fixed internal evidence table,
    When: Its physical guards are installed and all evidence guards are dropped,
    Then: Both mutation levels are guarded, enabled ALWAYS and later explicitly removed.
    """
    connection = MagicMock(dialect=postgres_dialect())
    install_execution_admission_table_triggers(connection, table)
    statements = [str(call.args[0]) for call in connection.execute.call_args_list]
    assert f"RAISE EXCEPTION '{table} is append-only: % forbidden'" in statements[0]
    assert "USING ERRCODE = 'raise_exception'" in statements[0]
    assert any(
        f"BEFORE UPDATE OR DELETE ON {table} FOR EACH ROW" in statement for statement in statements
    )
    assert any(
        f"BEFORE TRUNCATE ON {table} FOR EACH STATEMENT" in statement for statement in statements
    )
    assert (
        sum(f"ALTER TABLE {table} ENABLE ALWAYS TRIGGER" in statement for statement in statements)
        == 2
    )
    connection.execute.reset_mock()
    drop_execution_admission_triggers(connection)
    drops = [str(call.args[0]) for call in connection.execute.call_args_list]
    for name in ADMISSION_EVIDENCE_TABLES:
        assert f"DROP TRIGGER IF EXISTS {name}_reject_row_mutation ON {name}" in drops
        assert f"DROP TRIGGER IF EXISTS {name}_reject_truncate ON {name}" in drops
        assert f"DROP FUNCTION IF EXISTS {name}_reject_mutation()" in drops


def test_unknown_table_refuses_before_interpolating_any_sql_identifier() -> None:
    """Only the two fixed internal table identifiers are valid trigger targets.

    Given: An arbitrary unsupported evidence-table name,
    When: A caller requests physical guard installation,
    Then: The helper refuses before any SQL executes.
    """
    connection = MagicMock()
    with pytest.raises(ValueError, match="Unsupported execution admission evidence table"):
        install_execution_admission_table_triggers(
            connection, cast(AdmissionEvidenceTable, "arbitrary")
        )
    connection.execute.assert_not_called()


def test_unsupported_backend_cannot_silently_skip_guard_creation_or_removal() -> None:
    """Unsupported backends fail closed at both physical guard boundaries.

    Given: An online connection for an unsupported SQL dialect,
    When: Guard installation or removal is requested,
    Then: Both operations refuse without emitting partial DDL.
    """
    connection = MagicMock()
    connection.dialect.name = "unsupported"
    with pytest.raises(NotImplementedError):
        install_execution_admission_triggers(connection)
    with pytest.raises(NotImplementedError):
        drop_execution_admission_triggers(connection)
    connection.execute.assert_not_called()
