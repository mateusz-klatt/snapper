"""Dual-dialect DDL and immutable-target witnesses for the isolated 0056 draft."""

from importlib import import_module
from types import SimpleNamespace
from unittest.mock import MagicMock
from unittest.mock import patch

import pytest
import sqlalchemy as sa
from alembic.migration import MigrationContext
from alembic.operations import Operations
from sqlalchemy.dialects.postgresql import dialect as postgres_dialect
from sqlalchemy.dialects.sqlite import dialect as sqlite_dialect
from sqlalchemy.exc import IntegrityError
from sqlalchemy.schema import CreateTable

from snapper.data.execution_unit_snapshot_triggers import drop_execution_unit_snapshot_triggers
from snapper.data.execution_unit_snapshot_triggers import install_execution_unit_snapshot_triggers
from snapper.data.models import ExecutionUnitSnapshot

_MIGRATION = "snapper.data.migrations.versions.0056_execution_unit_snapshots"
_UUID = "00000000-0000-7000-8000-000000000001"
_OTHER_UUID = "00000000-0000-7000-8000-000000000002"
_INSERT = sa.text(
    "INSERT INTO execution_unit_snapshots "
    "(public_id, session_id, sequence_id, timestamp, known_to, phase, wallet_public_id, exchange, "
    "mode, instrument_public_id, command_public_id, execution_public_id, spec_public_id, "
    "spec_timestamp, spec_source, spec_version, content_digest, canonical_payload) "
    "VALUES (:public_id, :public_id, 1, '2026-10-22 23:00:00.000000', :known_to, :phase, "
    ":public_id, 'kraken_futures', 'live', :public_id, :command_id, :execution_id, :public_id, "
    "'2026-10-22 22:00:00.000000', 'source', 'version', :digest, '{}')"
)


def _values() -> dict[str, str | None]:
    """Build a structurally valid migration row without claiming typed economic authority."""
    return {
        "public_id": _UUID,
        "known_to": "9999-12-31 23:59:59.000000",
        "phase": "fill",
        "command_id": None,
        "execution_id": _UUID,
        "digest": "a" * 64,
    }


def test_sqlite_upgrade_is_empty_and_populated_downgrade_refuses(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Migration 0056 never manufactures historical snapshots.

    Given: An online SQLite migration context without a snapshot table,
    When: The migration upgrades, losslessly downgrades empty storage and then gains evidence,
    Then: Empty storage is reversible and populated storage is retained.
    """
    migration = import_module(_MIGRATION)
    engine = sa.create_engine("sqlite://")
    try:
        with engine.begin() as connection:
            monkeypatch.setattr(migration, "op", Operations(MigrationContext.configure(connection)))
            migration.upgrade()
            assert connection.scalar(sa.text("SELECT COUNT(*) FROM execution_unit_snapshots")) == 0
            migration.downgrade()
            assert not sa.inspect(connection).has_table("execution_unit_snapshots")
            migration.upgrade()
            connection.execute(_INSERT, _values())
            with pytest.raises(RuntimeError, match="prevents a lossless downgrade"):
                migration.downgrade()
            assert connection.scalar(sa.text("SELECT COUNT(*) FROM execution_unit_snapshots")) == 1
    finally:
        engine.dispose()


@pytest.mark.parametrize(
    "changes",
    (
        {"public_id": _OTHER_UUID, "digest": "b" * 64},
        {"public_id": _UUID, "execution_id": _OTHER_UUID},
        {
            "public_id": _OTHER_UUID,
            "execution_id": _OTHER_UUID,
            "known_to": "2026-10-22 23:00:00.000000",
        },
        {"public_id": _OTHER_UUID, "execution_id": _OTHER_UUID, "phase": "submission"},
        {"public_id": _OTHER_UUID, "command_id": _OTHER_UUID, "execution_id": _OTHER_UUID},
    ),
)
def test_sqlite_target_uniqueness_excludes_digest_and_known_to_is_permanent(
    monkeypatch: pytest.MonkeyPatch, changes: dict[str, str | None]
) -> None:
    """A digest change cannot create a second snapshot for one execution.

    Given: One snapshot and strict target/global-identity/active-sentinel constraints,
    When: Raw SQL tries a duplicate target, duplicate UUID or closed/non-discriminated row,
    Then: The database refuses each invalid shape independently of application checks.
    """
    migration = import_module(_MIGRATION)
    engine = sa.create_engine("sqlite://")
    try:
        with engine.begin() as connection:
            monkeypatch.setattr(migration, "op", Operations(MigrationContext.configure(connection)))
            migration.upgrade()
            connection.execute(_INSERT, _values())
            with pytest.raises(IntegrityError):
                connection.execute(_INSERT, {**_values(), **changes})
    finally:
        engine.dispose()


@pytest.mark.parametrize(
    "statement",
    (
        "UPDATE execution_unit_snapshots SET content_digest = 'changed'",
        "DELETE FROM execution_unit_snapshots",
        "INSERT OR REPLACE INTO execution_unit_snapshots SELECT * FROM execution_unit_snapshots",
    ),
)
def test_sqlite_migration_guards_update_delete_and_replace(
    monkeypatch: pytest.MonkeyPatch, statement: str
) -> None:
    """The migration installs physical guards rather than relying on ORM discipline.

    Given: A populated migration-built SQLite store with repository-equivalent recursive triggers,
    When: Raw SQL attempts UPDATE, DELETE or replacement of an immutable row,
    Then: The physical trigger aborts the mutation and preserves original content.
    """
    migration = import_module(_MIGRATION)
    engine = sa.create_engine("sqlite://")
    try:
        with engine.begin() as connection:
            connection.execute(sa.text("PRAGMA recursive_triggers=ON"))
            monkeypatch.setattr(migration, "op", Operations(MigrationContext.configure(connection)))
            migration.upgrade()
            connection.execute(_INSERT, _values())
            with pytest.raises(IntegrityError):
                connection.execute(sa.text(statement))
            assert (
                connection.scalar(sa.text("SELECT content_digest FROM execution_unit_snapshots"))
                == "a" * 64
            )
    finally:
        engine.dispose()


def test_postgresql_schema_and_immutable_trigger_statements_have_matching_contract() -> None:
    """Mocked online PostgreSQL operations expose the same storage and trigger theorem.

    Given: PostgreSQL-dialect migration operations and the actual ORM table,
    When: Upgrade and empty downgrade operations are captured without a live server,
    Then: UUID columns, MAX sentinel, target uniqueness and ALWAYS/TRUNCATE guards agree.
    """
    migration = import_module(_MIGRATION)
    connection = MagicMock(
        dialect=postgres_dialect(),
        scalar=MagicMock(return_value=0),
        get_isolation_level=MagicMock(return_value="READ COMMITTED"),
    )
    operations = MagicMock(get_bind=MagicMock(return_value=connection))
    operations.get_context.return_value.as_sql = False
    with patch.object(migration, "op", operations):
        migration.upgrade()
        migration.downgrade()
    create_args = operations.create_table.call_args.args
    migrated = sa.Table(create_args[0], sa.MetaData(), *create_args[1:])
    ddl = str(CreateTable(migrated).compile(dialect=postgres_dialect()))
    model_ddl = str(
        CreateTable(ExecutionUnitSnapshot.__table__).compile(dialect=postgres_dialect())
    )
    assert "UUID" in ddl
    assert "TIMESTAMPTZ '9999-12-31 23:59:59+00:00'" in ddl
    assert "TIMESTAMPTZ '9999-12-31 23:59:59+00:00'" in model_ddl
    index_calls = {call.args[0]: call for call in operations.create_index.call_args_list}
    assert index_calls["uq_unit_snapshot_execution"].args[2] == ["execution_public_id"]
    assert index_calls["uq_unit_snapshot_command"].args[2] == ["command_public_id"]
    statements = "\n".join(str(call.args[0]) for call in connection.execute.call_args_list)
    assert "BEFORE UPDATE OR DELETE" in statements
    assert "BEFORE TRUNCATE" in statements
    assert statements.count("ENABLE ALWAYS TRIGGER") == 2
    assert "DROP FUNCTION IF EXISTS execution_unit_snapshots_reject_mutation()" in statements


@pytest.mark.parametrize("operation", ("upgrade", "downgrade"))
def test_offline_migration_refuses_before_any_schema_mutation(operation: str) -> None:
    """Offline SQL cannot silently omit physical immutability guards.

    Given: An offline migration context without a real dialect bind,
    When: Either migration direction is requested,
    Then: The migration refuses before rendering a table mutation.
    """
    migration = import_module(_MIGRATION)
    operations = MagicMock()
    operations.get_context.return_value.as_sql = True
    with (
        patch.object(migration, "op", operations),
        pytest.raises(RuntimeError, match="online connection"),
    ):
        getattr(migration, operation)()
    operations.create_table.assert_not_called()
    operations.drop_table.assert_not_called()


def test_unknown_dialects_never_skip_immutable_trigger_installation() -> None:
    """Unsupported database dialects fail closed instead of creating mutable evidence.

    Given: A connection for an unsupported backend,
    When: Snapshot guards are installed or dropped,
    Then: Both operations refuse without executing DDL.
    """
    connection = MagicMock()
    connection.dialect.name = "unknown"
    for operation in (
        install_execution_unit_snapshot_triggers,
        drop_execution_unit_snapshot_triggers,
    ):
        with pytest.raises(NotImplementedError):
            operation(connection)
    connection.execute.assert_not_called()


@pytest.mark.parametrize("operation", ("upgrade", "downgrade"))
def test_unsupported_online_backend_refuses_before_rendering_evidence_storage(
    operation: str,
) -> None:
    """An online connection alone cannot certify an unsupported database's immutable storage.

    Given: An online context whose actual backend is unsupported,
    When: Either public migration direction is requested,
    Then: No table mutation, trigger statement or evidence query is attempted.
    """
    migration = import_module(_MIGRATION)
    connection = MagicMock()
    connection.dialect.name = "unsupported"
    operations = MagicMock(get_bind=MagicMock(return_value=connection))
    operations.get_context.return_value.as_sql = False
    with patch.object(migration, "op", operations), pytest.raises(NotImplementedError):
        getattr(migration, operation)()
    operations.create_table.assert_not_called()
    operations.drop_table.assert_not_called()
    connection.execute.assert_not_called()
    connection.scalar.assert_not_called()


@pytest.mark.parametrize("isolation", ("REPEATABLE READ", "SERIALIZABLE", "AUTOCOMMIT", None))
def test_postgresql_snapshot_requires_proven_fresh_statement_visibility(
    isolation: str | None,
) -> None:
    """A stale or unavailable isolation profile cannot justify an empty-evidence verdict.

    Given: A supported PostgreSQL adapter without proven READ COMMITTED isolation,
    When: Public downgrade is requested,
    Then: It refuses before acquiring a lock, reading evidence or dropping storage.
    """
    migration = import_module(_MIGRATION)
    connection = MagicMock(
        dialect=postgres_dialect(), get_isolation_level=MagicMock(return_value=isolation)
    )
    operations = MagicMock(get_bind=MagicMock(return_value=connection))
    operations.get_context.return_value.as_sql = False
    with (
        patch.object(migration, "op", operations),
        pytest.raises(RuntimeError, match="READ COMMITTED"),
    ):
        migration.downgrade()
    connection.execute.assert_not_called()
    connection.scalar.assert_not_called()
    operations.drop_table.assert_not_called()


def test_backend_change_cannot_skip_the_evidence_write_fence() -> None:
    """A contradictory adapter must revalidate its backend at the serialization boundary.

    Given: An adapter that changes backend identity after the public online check,
    When: Downgrade reaches the write-reservation boundary,
    Then: It refuses rather than treating an unsupported backend as unfenced success.
    """
    migration = import_module(_MIGRATION)
    original = MagicMock(dialect=sqlite_dialect())
    changed = MagicMock()
    changed.dialect.name = "unsupported"
    operations = MagicMock(get_bind=MagicMock(side_effect=(original, changed)))
    operations.get_context.return_value.as_sql = False
    with patch.object(migration, "op", operations), pytest.raises(NotImplementedError):
        migration.downgrade()
    changed.execute.assert_not_called()
    changed.scalar.assert_not_called()
    operations.drop_table.assert_not_called()


def test_real_sqlite_autocommit_cannot_discard_evidence_transaction_ownership(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A real autocommit driver cannot promise caller-owned protection through schema removal.

    Given: Actual SQLite storage on a Python autocommit connection,
    When: Public downgrade attempts to reserve evidence writes,
    Then: It refuses and leaves the real empty evidence table intact.
    """
    migration = import_module(_MIGRATION)
    engine = sa.create_engine("sqlite://", connect_args={"autocommit": True})
    try:
        with engine.begin() as connection:
            monkeypatch.setattr(migration, "op", Operations(MigrationContext.configure(connection)))
            migration.upgrade()
            with pytest.raises(RuntimeError, match="caller-controlled transaction"):
                migration.downgrade()
            assert sa.inspect(connection).has_table("execution_unit_snapshots")
            assert connection.scalar(sa.text("SELECT COUNT(*) FROM execution_unit_snapshots")) == 0
    finally:
        engine.dispose()


@pytest.mark.parametrize("state", (None, "unknown"))
def test_sqlite_missing_physical_state_refuses_before_guessing_empty_storage(state: object) -> None:
    """SQLAlchemy's logical transaction cannot substitute for missing physical driver evidence.

    Given: A supported SQLite adapter with unavailable or malformed physical state,
    When: Public downgrade requests a serialization guarantee,
    Then: It refuses without a guessed BEGIN, evidence count or destructive DDL.
    """
    migration = import_module(_MIGRATION)
    connection = MagicMock(dialect=sqlite_dialect())
    connection.connection.driver_connection = SimpleNamespace(
        autocommit=False, in_transaction=state
    )
    operations = MagicMock(get_bind=MagicMock(return_value=connection))
    operations.get_context.return_value.as_sql = False
    with (
        patch.object(migration, "op", operations),
        pytest.raises(RuntimeError, match="driver transaction state"),
    ):
        migration.downgrade()
    connection.exec_driver_sql.assert_not_called()
    connection.scalar.assert_not_called()
    operations.drop_table.assert_not_called()


def test_sqlite_ignored_begin_cannot_turn_into_an_unprotected_empty_count() -> None:
    """An adapter that ignores BEGIN must not claim a retained write reservation.

    Given: A SQLite driver that reports no physical transaction after BEGIN IMMEDIATE,
    When: Public downgrade verifies the reservation,
    Then: It refuses before counting or dropping evidence instead of assuming success.
    """
    migration = import_module(_MIGRATION)
    connection = MagicMock(dialect=sqlite_dialect())
    connection.connection.driver_connection = SimpleNamespace(
        autocommit=False, in_transaction=False
    )
    operations = MagicMock(get_bind=MagicMock(return_value=connection))
    operations.get_context.return_value.as_sql = False
    with (
        patch.object(migration, "op", operations),
        pytest.raises(RuntimeError, match="did not retain"),
    ):
        migration.downgrade()
    connection.exec_driver_sql.assert_called_once_with("BEGIN IMMEDIATE")
    connection.scalar.assert_not_called()
    operations.drop_table.assert_not_called()
