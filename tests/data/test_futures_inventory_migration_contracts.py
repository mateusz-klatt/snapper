"""Check inventory physical declaration parity and fail-closed migration boundaries.

Rendered PostgreSQL statements are declaration checks, not live catalog or
concurrency evidence. The separate isolated PostgreSQL witness supplies those
proofs. Real SQLite driver checks preserve caller-controlled completion.
"""

from collections.abc import Callable
from importlib import import_module
from pathlib import Path
from sqlite3 import Connection as SQLiteConnection
from types import ModuleType
from types import SimpleNamespace
from unittest.mock import MagicMock
from unittest.mock import patch

import pytest
import sqlalchemy as sa
from alembic.migration import MigrationContext
from alembic.operations import Operations
from sqlalchemy.dialects.postgresql import dialect as postgres_dialect
from sqlalchemy.dialects.sqlite import dialect as sqlite_dialect
from sqlalchemy.exc import OperationalError
from sqlalchemy.schema import CreateTable

from snapper.data.futures_inventory_read_triggers import drop_futures_inventory_read_triggers
from snapper.data.futures_inventory_read_triggers import install_futures_inventory_read_triggers
from snapper.data.models import AccountingRawReceipt
from snapper.data.models import FuturesInventoryReadAttempt
from snapper.data.models import FuturesInventoryReadOutcome
from snapper.data.models import NativeAccountIdentityProof
from snapper.data.models import NativeAccountReadAttempt
from snapper.data.models import NativeAccountReadOutcome

_MIGRATIONS = {
    "0066": import_module("snapper.data.migrations.versions.0066_futures_inventory_read_lifecycle"),
}
_TABLES = {
    "0066": (FuturesInventoryReadAttempt.__table__, FuturesInventoryReadOutcome.__table__),
}
_GUARDS: dict[str, tuple[Callable[[sa.Connection, str], None], ...]] = {
    "0066": (install_futures_inventory_read_triggers, drop_futures_inventory_read_triggers),
}


def _contract(table: sa.Table) -> tuple[dict[str, tuple[str, bool]], set[str]]:
    """Compare concrete PostgreSQL types/nullability and every emitted named constraint."""
    columns = {
        column.name: (str(column.type.compile(dialect=postgres_dialect())), column.nullable)
        for column in table.columns
    }
    ddl = str(CreateTable(table).compile(dialect=postgres_dialect()))
    constraints = {
        line.strip().removesuffix(",")
        for line in ddl.splitlines()
        if line.strip().startswith("CONSTRAINT ")
    }
    return columns, constraints


def _operations(connection: sa.Connection | MagicMock) -> MagicMock:
    """Keep a captured online Alembic boundary without pretending that SQL reached PostgreSQL."""
    operations = MagicMock(get_bind=MagicMock(return_value=connection))
    operations.get_context.return_value.as_sql = False
    return operations


def _invoke(connection: sa.Connection, migration: ModuleType, operation: str) -> None:
    """Invoke the real revision under its actual caller-controlled SQLite transaction."""
    with patch.object(migration, "op", Operations(MigrationContext.configure(connection))):
        getattr(migration, operation)()


@pytest.mark.parametrize("revision", ("0066",))
def test_rendered_postgresql_matches_models_and_preserves_guarded_drop_order(
    revision: str,
) -> None:
    """Model and migration declarations retain identical PostgreSQL physical contracts.

    Given: Actual ORM declarations and the exact online revision operations,
    When: PostgreSQL upgrade and a serialized empty downgrade render their statements,
    Then: Types, constraints and indexes match and exclusive locking precedes guard removal.
    """
    connection = MagicMock(
        dialect=postgres_dialect(),
        scalar=MagicMock(return_value=0),
        get_isolation_level=MagicMock(return_value="READ COMMITTED"),
    )
    operations = _operations(connection)
    migration = _MIGRATIONS[revision]
    with patch.object(migration, "op", operations):
        migration.upgrade()
        migration.downgrade()
    metadata = sa.MetaData()
    for model in (
        AccountingRawReceipt,
        NativeAccountReadAttempt,
        NativeAccountReadOutcome,
        NativeAccountIdentityProof,
    ):
        model.__table__.to_metadata(metadata)
    models = {table.name: table for table in _TABLES[revision]}
    captured = [
        sa.Table(call.args[0], metadata, *call.args[1:])
        for call in operations.create_table.call_args_list
    ]
    assert set(models) == {table.name for table in captured}
    for table in captured:
        assert _contract(table) == _contract(models[table.name])
        ddl = str(CreateTable(table).compile(dialect=postgres_dialect()))
        names = [
            line.split()[1] for line in ddl.splitlines() if line.strip().startswith("CONSTRAINT ")
        ]
        assert len(names) == len(set(names))
    assert {
        call.args[0]: (tuple(call.args[2]), bool(call.kwargs.get("unique", False)))
        for call in operations.create_index.call_args_list
    } == {
        index.name: (tuple(column.name for column in index.columns), bool(index.unique))
        for table in models.values()
        for index in table.indexes
    }
    statements = [str(call.args[0]) for call in connection.execute.call_args_list]
    lock = "LOCK TABLE " + ", ".join(models) + " IN ACCESS EXCLUSIVE MODE"
    assert lock in statements
    assert sum("ENABLE ALWAYS TRIGGER" in item for item in statements) == len(models) * 2
    guard_removal = next(
        i
        for i, item in enumerate(statements)
        if "DROP TRIGGER IF EXISTS" in item and i > statements.index(lock)
    )
    assert statements.index(lock) < guard_removal
    function = "futures_inventory_read"
    assert statements[-1] == f"DROP FUNCTION IF EXISTS {function}_reject_mutation()"
    assert [call.args[0] for call in operations.drop_table.call_args_list] == list(reversed(models))


@pytest.mark.parametrize("revision", ("0066",))
@pytest.mark.parametrize("operation", ("upgrade", "downgrade"))
@pytest.mark.parametrize("profile", ("offline", "unsupported"))
def test_unproven_database_profile_refuses_before_any_schema_change(
    revision: str, operation: str, profile: str
) -> None:
    """An offline rendering mode or unsupported backend cannot silently omit protection.

    Given: A SQL-only context or unsupported database adapter,
    When: Either direction of the new inventory revision is invoked,
    Then: It refuses before reading evidence or issuing destructive DDL.
    """
    connection = MagicMock()
    connection.dialect.name = "unsupported"
    operations = _operations(connection)
    operations.get_context.return_value.as_sql = profile == "offline"
    expected = RuntimeError if profile == "offline" else NotImplementedError
    with patch.object(_MIGRATIONS[revision], "op", operations), pytest.raises(expected):
        getattr(_MIGRATIONS[revision], operation)()
    operations.create_table.assert_not_called()
    operations.drop_table.assert_not_called()
    connection.execute.assert_not_called()
    connection.scalar.assert_not_called()


@pytest.mark.parametrize("revision", ("0066",))
@pytest.mark.parametrize("isolation", ("REPEATABLE READ", "SERIALIZABLE", "AUTOCOMMIT", None))
def test_stale_postgresql_visibility_is_refused_before_downgrade_lock(
    revision: str, isolation: str | None
) -> None:
    """An exclusive relation lock cannot renew an older transaction snapshot.

    Given: A PostgreSQL connection without proven READ COMMITTED isolation,
    When: The public downgrade reaches its actual serialization boundary,
    Then: It refuses before locking, counting or removing retained evidence.
    """
    connection = MagicMock(
        dialect=postgres_dialect(), get_isolation_level=MagicMock(return_value=isolation)
    )
    operations = _operations(connection)
    with (
        patch.object(_MIGRATIONS[revision], "op", operations),
        pytest.raises(RuntimeError, match="READ COMMITTED"),
    ):
        _MIGRATIONS[revision].downgrade()
    connection.execute.assert_not_called()
    connection.scalar.assert_not_called()
    operations.drop_table.assert_not_called()


@pytest.mark.parametrize("revision", ("0066",))
def test_real_sqlite_autocommit_refuses_loss_of_caller_control(revision: str) -> None:
    """A physical autocommit driver cannot retain the fence until a caller decision.

    Given: Empty real revision tables and an actual SQLite autocommit driver,
    When: Downgrade requests caller-controlled completion,
    Then: It refuses before removing any original physical relation.
    """
    engine = sa.create_engine("sqlite://", connect_args={"autocommit": True})
    try:
        with engine.begin() as connection:
            _invoke(connection, _MIGRATIONS[revision], "upgrade")
            with pytest.raises(RuntimeError, match="caller-controlled completion"):
                _invoke(connection, _MIGRATIONS[revision], "downgrade")
            assert all(sa.inspect(connection).has_table(table.name) for table in _TABLES[revision])
    finally:
        engine.dispose()


@pytest.mark.parametrize("revision", ("0066",))
@pytest.mark.parametrize("state", (None, "unknown", False))
def test_missing_or_ignored_sqlite_driver_state_refuses_before_count(
    revision: str, state: object
) -> None:
    """Logical transaction state cannot replace a retained physical write reservation.

    Given: A missing driver flag, malformed flag or ignored BEGIN IMMEDIATE,
    When: Downgrade verifies that its write fence survived,
    Then: It refuses before reading counts or changing evidence storage.
    """
    connection = MagicMock(dialect=sqlite_dialect())
    connection.connection.driver_connection = SimpleNamespace(
        autocommit=False, in_transaction=state
    )
    operations = _operations(connection)
    with patch.object(_MIGRATIONS[revision], "op", operations), pytest.raises(RuntimeError):
        _MIGRATIONS[revision].downgrade()
    connection.scalar.assert_not_called()
    operations.drop_table.assert_not_called()
    if state is False:
        connection.exec_driver_sql.assert_called_once_with("BEGIN IMMEDIATE")
    else:
        connection.exec_driver_sql.assert_not_called()


@pytest.mark.parametrize("revision", ("0066",))
@pytest.mark.parametrize("operation", (0, 1))
@pytest.mark.parametrize("profile", ("unknown_table", "unsupported_backend"))
def test_guard_ddl_rejects_unproven_identifiers_and_adapters(
    revision: str, operation: int, profile: str
) -> None:
    """Guard helpers cannot interpolate arbitrary tables or silently ignore an adapter.

    Given: An unknown table name or unsupported physical backend,
    When: Either guard installation or removal is requested,
    Then: It refuses before executing any statement.
    """
    connection = MagicMock()
    connection.dialect.name = "unsupported"
    table = "unknown_table" if profile == "unknown_table" else _TABLES[revision][0].name
    expected = ValueError if profile == "unknown_table" else NotImplementedError
    with pytest.raises(expected):
        _GUARDS[revision][operation](connection, table)
    connection.execute.assert_not_called()


@pytest.mark.parametrize("revision", ("0066",))
def test_adapter_change_is_rejected_at_the_actual_fence(revision: str) -> None:
    """A prior supported label cannot authorize a later unsupported downgrade fence.

    Given: Initial SQLite profile validation followed by an unsupported actual bind,
    When: Public downgrade requests its evidence write reservation,
    Then: It refuses before counting or dropping any relation.
    """
    changed = MagicMock()
    changed.dialect.name = "unsupported"
    operations = _operations(changed)
    operations.get_bind.side_effect = (MagicMock(dialect=sqlite_dialect()), changed)
    with patch.object(_MIGRATIONS[revision], "op", operations), pytest.raises(NotImplementedError):
        _MIGRATIONS[revision].downgrade()
    changed.execute.assert_not_called()
    changed.scalar.assert_not_called()
    operations.drop_table.assert_not_called()


@pytest.mark.parametrize("revision", ("0066",))
@pytest.mark.parametrize("completion", ("commit", "rollback"))
def test_real_empty_sqlite_downgrade_preserves_caller_completion(
    tmp_path: Path, revision: str, completion: str
) -> None:
    """Empty evidence removal and earlier caller work share one physical completion decision.

    Given: Empty real revision tables and an actual uncommitted caller write,
    When: Downgrade drops only its empty tables while another connection attempts a write,
    Then: The writer remains fenced and caller commit or rollback controls both DDL and earlier work.
    """
    engine = sa.create_engine(f"sqlite:///{tmp_path / 'caller.db'}", connect_args={"timeout": 0})
    try:
        with engine.begin() as connection:
            connection.exec_driver_sql("CREATE TABLE caller_work (id INTEGER PRIMARY KEY)")
            _invoke(connection, _MIGRATIONS[revision], "upgrade")
        with engine.connect() as first, engine.connect() as writer:
            first.exec_driver_sql("INSERT INTO caller_work VALUES (77)")
            _invoke(first, _MIGRATIONS[revision], "downgrade")
            assert first.in_transaction()
            with pytest.raises(OperationalError, match="database is locked"):
                writer.exec_driver_sql("INSERT INTO caller_work VALUES (88)")
            writer.rollback()
            if completion == "commit":
                first.commit()
            else:
                first.rollback()
            assert writer.scalar(sa.text("SELECT COUNT(*) FROM caller_work")) == (
                completion == "commit"
            )
            assert all(
                sa.inspect(writer).has_table(table.name) == (completion == "rollback")
                for table in _TABLES[revision]
            )
            writer.exec_driver_sql("INSERT INTO caller_work VALUES (99)")
            writer.commit()
    finally:
        engine.dispose()


@pytest.mark.parametrize("revision", ("0066",))
def test_real_new_sqlite_fence_is_retained_after_empty_ddl(tmp_path: Path, revision: str) -> None:
    """A fresh physical transaction begins before any empty evidence verdict or table removal.

    Given: Empty actual revision tables and no prior physical transaction on the caller,
    When: Public downgrade begins its write reservation and removes the empty tables,
    Then: The reservation blocks an independent writer until rollback restores those same tables.
    """
    engine = sa.create_engine(f"sqlite:///{tmp_path / 'fresh.db'}", connect_args={"timeout": 0})
    try:
        with engine.begin() as connection:
            connection.exec_driver_sql("CREATE TABLE caller_work (id INTEGER PRIMARY KEY)")
            _invoke(connection, _MIGRATIONS[revision], "upgrade")
        with engine.connect() as first, engine.connect() as writer:
            driver = first.connection.driver_connection
            assert isinstance(driver, SQLiteConnection)
            assert driver.in_transaction is False
            _invoke(first, _MIGRATIONS[revision], "downgrade")
            assert driver.in_transaction is True
            with pytest.raises(OperationalError, match="database is locked"):
                writer.exec_driver_sql("INSERT INTO caller_work VALUES (88)")
            writer.rollback()
            first.rollback()
            assert all(sa.inspect(writer).has_table(table.name) for table in _TABLES[revision])
            writer.exec_driver_sql("INSERT INTO caller_work VALUES (99)")
            writer.commit()
    finally:
        engine.dispose()
