"""Real two-connection downgrade fencing and explicit unsupported transaction witnesses."""

import sqlite3
from collections.abc import Callable
from dataclasses import dataclass
from dataclasses import field
from importlib import import_module
from pathlib import Path
from types import ModuleType
from typing import Literal
from typing import cast
from unittest.mock import MagicMock

import pytest
import sqlalchemy as sa
from alembic.migration import MigrationContext
from alembic.operations import Operations
from sqlalchemy.dialects.postgresql import dialect as postgres_dialect
from sqlalchemy.dialects.sqlite import dialect as sqlite_dialect
from sqlalchemy.engine import Dialect
from sqlalchemy.engine import Engine
from sqlalchemy.exc import OperationalError
from sqlalchemy.sql import Executable

_MODULE = "snapper.data.migrations.versions.0055_derivative_payoff_metadata"
_COUNT = "SELECT COUNT(*) FROM instrument_specs"


def _driver(connection: sa.Connection) -> sqlite3.Connection:
    """Expose the actual synchronous SQLite transaction state, never SQLAlchemy autobegin.

    Args:
        connection: Real engine connection using the configured synchronous SQLite driver.

    Returns:
        The actual sqlite3 driver owning the transaction.
    """
    return cast(sqlite3.Connection, connection.connection.driver_connection)


def _engine(path: Path, *, autocommit: bool = False, wal: bool = False) -> Engine:
    """Create a file-backed two-connection database with bounded lock contention.

    Args:
        path: Isolated test database under pytest's temporary directory.
        autocommit: Whether to request the explicitly unsupported Python autocommit mode.
        wal: Whether the stale-reader witness requires WAL journaling.

    Returns:
        A real engine whose writer waits at most ten milliseconds.
    """
    options: dict[str, object] = {"timeout": 0.01}
    if autocommit:
        options["autocommit"] = True
    engine = sa.create_engine(f"sqlite:///{path}", connect_args=options)
    if wal:
        with engine.connect() as connection:
            assert connection.exec_driver_sql("PRAGMA journal_mode=WAL").scalar_one() == "wal"
            connection.commit()
    return engine


def _prepare(engine: Engine, migration: ModuleType, monkeypatch: pytest.MonkeyPatch) -> None:
    """Upgrade real legacy rows and commit the initial setup before contention tests.

    Args:
        engine: Isolated file-backed SQLite engine.
        migration: Actual candidate migration module.
        monkeypatch: Test-owned operation binding replacement.
    """
    with engine.begin() as connection:
        connection.exec_driver_sql(
            "CREATE TABLE instrument_specs (id INTEGER PRIMARY KEY, known_to TEXT NOT NULL, "
            "unit_certified BOOLEAN NOT NULL DEFAULT false)"
        )
        connection.exec_driver_sql(
            "INSERT INTO instrument_specs (id, known_to, unit_certified) "
            "VALUES (1, '9999-12-31', false)"
        )
        connection.exec_driver_sql("CREATE TABLE caller_work (id INTEGER PRIMARY KEY)")
        _bind(connection, migration, monkeypatch)
        migration.upgrade()


def _bind(
    connection: sa.Connection, migration: ModuleType, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Bind public migration operations to one real caller-owned connection.

    Args:
        connection: Connection whose transaction remains owned by the test caller.
        migration: Actual candidate migration module.
        monkeypatch: Test-owned operation binding replacement.
    """
    monkeypatch.setattr(migration, "op", Operations(MigrationContext.configure(connection)))


def _guarded_count(
    first: sa.Connection, second: sa.Connection
) -> tuple[Callable[[Executable], object], list[bool]]:
    """Attempt an independent evidence write exactly when the protected count begins.

    Args:
        first: Caller connection executing the actual downgrade.
        second: Independent physical connection attempting to add evidence.

    Returns:
        A count-boundary observer and its independently blocked-writer witnesses.
    """
    original = first.scalar
    blocked: list[bool] = []

    def observe(statement: Executable) -> object:
        """Preserve the real count while witnessing exclusive writer ownership.

        Args:
            statement: Actual statement passed to the real connection scalar boundary.

        Returns:
            The original scalar result after the concurrency witness.
        """
        if str(statement).startswith(_COUNT):
            with pytest.raises(OperationalError, match="locked"):
                second.exec_driver_sql(
                    "UPDATE instrument_specs SET native_order_quantity_unit='contract_count' "
                    "WHERE id=1"
                )
            second.rollback()
            blocked.append(True)
        return original(statement)

    return observe, blocked


@pytest.mark.parametrize("existing_dml", (False, True))
@pytest.mark.parametrize("completion", ("commit", "rollback"))
def test_count_and_schema_change_hold_one_caller_owned_write_transaction(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    existing_dml: bool,
    completion: Literal["commit", "rollback"],
) -> None:
    """The evidence guard drains and blocks writers without committing earlier caller work.

    Given: Two physical legacy SQLite connections and optional uncommitted caller DML,
    When: An evidence write attempts to race the count and the caller later commits or rolls back,
    Then: The race is blocked, transaction ownership survives and release permits the next writer.
    """
    migration = import_module(_MODULE)
    engine = _engine(tmp_path / "fenced.sqlite")
    try:
        _prepare(engine, migration, monkeypatch)
        with engine.connect() as first, engine.connect() as second:
            transaction = first.begin()
            try:
                assert _driver(first).autocommit == sqlite3.LEGACY_TRANSACTION_CONTROL
                assert _driver(first).in_transaction is False
                if existing_dml:
                    first.exec_driver_sql("INSERT INTO caller_work (id) VALUES (77)")
                    assert _driver(first).in_transaction is True
                _bind(first, migration, monkeypatch)
                observe, blocked = _guarded_count(first, second)
                monkeypatch.setattr(first, "scalar", observe)
                migration.downgrade()
                assert blocked == [True]
                assert _driver(first).in_transaction is True
                with pytest.raises(OperationalError, match="locked"):
                    second.exec_driver_sql("INSERT INTO caller_work (id) VALUES (99)")
                second.rollback()
                if completion == "commit":
                    transaction.commit()
                else:
                    transaction.rollback()
                assert _driver(first).in_transaction is False
                second.exec_driver_sql("INSERT INTO caller_work (id) VALUES (99)")
                second.commit()
                ids = (
                    second.exec_driver_sql("SELECT id FROM caller_work ORDER BY id").scalars().all()
                )
                assert ids == ([77, 99] if existing_dml and completion == "commit" else [99])
                columns = {
                    column["name"] for column in sa.inspect(second).get_columns("instrument_specs")
                }
                assert ("native_order_quantity_unit" in columns) == (completion == "rollback")
            finally:
                if transaction.is_active:
                    transaction.rollback()
    finally:
        engine.dispose()


def test_populated_refusal_preserves_evidence_and_releases_only_on_caller_rollback(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A lossy downgrade refusal retains both financial metadata and caller transaction ownership.

    Given: Committed native-unit evidence and an independent second writer,
    When: The public downgrade checks the populated table under its write reservation,
    Then: Evidence remains, the other writer stays blocked and only caller rollback releases it.
    """
    migration = import_module(_MODULE)
    engine = _engine(tmp_path / "populated.sqlite")
    try:
        _prepare(engine, migration, monkeypatch)
        with engine.begin() as seed:
            seed.exec_driver_sql(
                "UPDATE instrument_specs SET native_order_quantity_unit='contract_count' WHERE id=1"
            )
        with engine.connect() as first, engine.connect() as second:
            transaction = first.begin()
            try:
                _bind(first, migration, monkeypatch)
                observe, blocked = _guarded_count(first, second)
                monkeypatch.setattr(first, "scalar", observe)
                with pytest.raises(RuntimeError, match="prevents a lossless downgrade"):
                    migration.downgrade()
                assert blocked == [True]
                assert _driver(first).in_transaction is True
                assert (
                    first.exec_driver_sql(
                        "SELECT native_order_quantity_unit FROM instrument_specs WHERE id=1"
                    ).scalar_one()
                    == "contract_count"
                )
                transaction.rollback()
                second.exec_driver_sql("INSERT INTO caller_work (id) VALUES (99)")
                second.commit()
            finally:
                if transaction.is_active:
                    transaction.rollback()
    finally:
        engine.dispose()


def test_stale_wal_reader_refuses_promotion_without_discarding_new_evidence(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A stale reader cannot promote and drop interpretation committed by another connection.

    Given: A real WAL reader snapshot preceding another connection's committed evidence,
    When: Downgrade tries to promote that existing read transaction to a writer,
    Then: SQLite aborts with BUSY_SNAPSHOT and the caller can roll back to see intact evidence.
    """
    migration = import_module(_MODULE)
    engine = _engine(tmp_path / "stale.sqlite", wal=True)
    try:
        _prepare(engine, migration, monkeypatch)
        with engine.connect() as first, engine.connect() as second:
            transaction = first.begin()
            try:
                first.exec_driver_sql("BEGIN DEFERRED")
                assert (
                    first.exec_driver_sql(
                        "SELECT native_order_quantity_unit FROM instrument_specs WHERE id=1"
                    ).scalar_one()
                    is None
                )
                second.exec_driver_sql(
                    "UPDATE instrument_specs SET native_order_quantity_unit='contract_count' WHERE id=1"
                )
                second.commit()
                _bind(first, migration, monkeypatch)
                with pytest.raises(OperationalError) as caught:
                    migration.downgrade()
                assert isinstance(caught.value.orig, sqlite3.OperationalError)
                assert caught.value.orig.sqlite_errorcode == sqlite3.SQLITE_BUSY_SNAPSHOT
                assert _driver(first).in_transaction is True
                transaction.rollback()
                assert (
                    first.exec_driver_sql(
                        "SELECT native_order_quantity_unit FROM instrument_specs WHERE id=1"
                    ).scalar_one()
                    == "contract_count"
                )
            finally:
                if transaction.is_active:
                    transaction.rollback()
    finally:
        engine.dispose()


def test_python_autocommit_true_is_refused_before_any_schema_removal(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Explicit Python autocommit cannot masquerade as caller-owned transaction completion.

    Given: A real Python SQLite connection whose commit and rollback methods have no effect,
    When: The evidence-sensitive downgrade is requested,
    Then: The unsupported completion policy is refused and all evidence columns remain.
    """
    migration = import_module(_MODULE)
    engine = _engine(tmp_path / "autocommit.sqlite", autocommit=True)
    try:
        _prepare(engine, migration, monkeypatch)
        with engine.connect() as connection:
            assert _driver(connection).autocommit is True
            _bind(connection, migration, monkeypatch)
            with pytest.raises(RuntimeError, match="caller-controlled transaction completion"):
                migration.downgrade()
            assert "native_order_quantity_unit" in {
                column["name"] for column in sa.inspect(connection).get_columns("instrument_specs")
            }
    finally:
        engine.dispose()


def test_postgresql_mock_orders_lock_count_then_ddl_without_claiming_live_concurrency(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The PostgreSQL statement contract locks under verified fresh-snapshot isolation first.

    Given: A typed PostgreSQL boundary mock reporting READ COMMITTED and empty evidence,
    When: The public migration runs,
    Then: Lock precedes count and every destructive DDL call follows the successful count.
    """
    migration = import_module(_MODULE)
    events: list[str] = []

    def record_lock(statement: Executable) -> None:
        """Record the exact lock statement at the mocked database boundary.

        Args:
            statement: SQL statement supplied to the mock connection.
        """
        events.append(str(statement))

    def record_count(statement: Executable) -> int:
        """Return empty evidence after recording the actual guarded count.

        Args:
            statement: Evidence count statement supplied to the mock connection.

        Returns:
            Zero retained evidence rows for this mocked DDL-order witness.
        """
        events.append(str(statement))
        return 0

    def record_drop(table: str, column: str) -> None:
        """Record destructive column removal after the protected evidence check.

        Args:
            table: Actual migration target table.
            column: Column whose removal the public migration requested.
        """
        events.append(f"DROP {table}.{column}")

    connection = MagicMock(
        dialect=postgres_dialect(),
        get_isolation_level=MagicMock(return_value="READ COMMITTED"),
        execute=MagicMock(side_effect=record_lock),
        scalar=MagicMock(side_effect=record_count),
    )
    operations = MagicMock(
        get_bind=MagicMock(return_value=connection),
        drop_column=MagicMock(side_effect=record_drop),
    )
    monkeypatch.setattr(migration, "op", operations)
    migration.downgrade()
    assert events[0] == "LOCK TABLE instrument_specs IN ACCESS EXCLUSIVE MODE"
    assert events[1].startswith(_COUNT)
    assert len(events) == 6
    assert all(event.startswith("DROP instrument_specs.") for event in events[2:])
    connection.commit.assert_not_called()
    connection.rollback.assert_not_called()


@pytest.mark.parametrize("isolation", ("REPEATABLE READ", "SERIALIZABLE", "READ UNCOMMITTED", ""))
def test_postgresql_without_proven_read_committed_refuses_before_lock_or_count(
    monkeypatch: pytest.MonkeyPatch, isolation: str
) -> None:
    """An old PostgreSQL transaction snapshot cannot be relabeled fresh after a late lock.

    Given: A connection reporting an unsupported or unavailable isolation profile,
    When: A caller invokes the public downgrade outside the standard environment,
    Then: The guard refuses before counting or attempting any destructive operation.
    """
    migration = import_module(_MODULE)
    connection = MagicMock(
        dialect=postgres_dialect(), get_isolation_level=MagicMock(return_value=isolation)
    )
    operations = MagicMock(get_bind=MagicMock(return_value=connection))
    monkeypatch.setattr(migration, "op", operations)
    with pytest.raises(RuntimeError, match="READ COMMITTED"):
        migration.downgrade()
    connection.execute.assert_not_called()
    connection.scalar.assert_not_called()
    operations.drop_column.assert_not_called()


@dataclass
class _BrokenDriver:
    """An unsupported adapter reports unavailable or unretained physical transaction state."""

    in_transaction: object
    autocommit: bool = False


@dataclass
class _DriverProxy:
    """Model only the explicit SQLAlchemy raw-driver access boundary."""

    driver_connection: _BrokenDriver


@dataclass
class _UnreliableSQLiteConnection:
    """Accept transaction commands without actually retaining them, modeling a broken adapter."""

    connection: _DriverProxy
    dialect: Dialect = field(default_factory=sqlite_dialect)
    statements: list[str] = field(default_factory=list)
    scalar_calls: int = 0

    def exec_driver_sql(self, statement: str) -> None:
        """Expose the adapter fault without pretending a real SQLite engine behaves this way.

        Args:
            statement: Transaction command the adapter accepts but does not honor.
        """
        self.statements.append(statement)

    def scalar(self, statement: Executable) -> int:
        """Record an erroneous count if the public guard ever lets unsafe state through.

        Args:
            statement: Evidence query the broken adapter must never receive.

        Returns:
            Zero only to expose an incorrectly permitted destructive continuation.
        """
        self.scalar_calls += 1
        return 0


@pytest.mark.parametrize("state", (None, "unavailable", 0, False))
def test_unavailable_or_unretained_driver_transaction_state_fails_closed(
    monkeypatch: pytest.MonkeyPatch, state: object
) -> None:
    """A nonconforming DBAPI adapter cannot silently remove the transaction boundary.

    Given: An honestly labeled fake adapter with unknown state or an ignored BEGIN,
    When: The public downgrade attempts to prove its physical transaction,
    Then: It refuses before count or DDL and never claims a real SQLite runtime theorem.
    """
    migration = import_module(_MODULE)
    connection = _UnreliableSQLiteConnection(_DriverProxy(_BrokenDriver(state)))
    operations = MagicMock(get_bind=MagicMock(return_value=connection))
    monkeypatch.setattr(migration, "op", operations)
    expected = "did not retain" if state is False else "driver transaction state"
    with pytest.raises(RuntimeError, match=expected):
        migration.downgrade()
    assert connection.scalar_calls == 0
    assert connection.statements == (["BEGIN IMMEDIATE"] if state is False else [])
    operations.drop_column.assert_not_called()


def test_unsupported_dialect_cannot_skip_evidence_serialization(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """An unsupported backend cannot fall through to an unfenced destructive operation.

    Given: A connection outside SQLite and PostgreSQL,
    When: The public evidence-sensitive downgrade is requested,
    Then: It refuses before count or DDL instead of silently omitting serialization.
    """
    migration = import_module(_MODULE)
    connection = MagicMock()
    connection.dialect.name = "unsupported"
    operations = MagicMock(get_bind=MagicMock(return_value=connection))
    monkeypatch.setattr(migration, "op", operations)
    with pytest.raises(NotImplementedError, match="only SQLite and PostgreSQL"):
        migration.downgrade()
    connection.scalar.assert_not_called()
    operations.drop_column.assert_not_called()
