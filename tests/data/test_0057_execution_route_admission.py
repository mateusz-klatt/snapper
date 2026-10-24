"""Actual SQLite evidence, transaction fencing and dual-dialect migration 0057 contracts."""

from collections.abc import Iterator
from importlib import import_module
from pathlib import Path
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
from sqlalchemy.exc import OperationalError
from sqlalchemy.schema import CreateTable

from snapper.data.execution_admission_triggers import ADMISSION_EVIDENCE_TABLES
from snapper.data.execution_admission_triggers import AdmissionEvidenceTable
from snapper.data.models import ExecutionRouteAuthority
from snapper.data.models import SubmissionUnitAdmission

_MIGRATION = import_module("snapper.data.migrations.versions.0057_execution_route_admission")
_UUID = "00000000-0000-7000-8000-000000000001"
_OTHER_UUID = "00000000-0000-7000-8000-000000000002"
_TIMESTAMP = "2026-10-23 00:00:00.000000"
type _Value = str | int | None


def _values(table: AdmissionEvidenceTable) -> dict[str, _Value]:
    """Build valid raw physical evidence without asserting application-level canonical meaning."""
    result: dict[str, _Value] = {
        "public_id": _UUID,
        "session_id": _UUID,
        "sequence_id": 1,
        "timestamp": _TIMESTAMP,
        "known_to": "9999-12-31 23:59:59.000000",
        "wallet_public_id": _UUID,
        "exchange": "paper",
        "mode": "paper",
        "account_scope_digest": "a" * 64,
        "content_digest": "b" * 64,
        "canonical_payload": "{}",
    }
    if table == "execution_route_authorities":
        result.update(
            environment="paper",
            margin_account="cash",
            credential_public_id=None,
            credential_timestamp=None,
            venue_account_id=None,
            account_observation_public_id=_UUID,
            account_observation_timestamp=_TIMESTAMP,
            observed_at=_TIMESTAMP,
            valid_until="2026-10-24 00:00:00.000000",
            route_revision="c" * 64,
        )
    else:
        result.update(
            attempt_public_id=_UUID,
            instrument_public_id=_UUID,
            command_public_id=_UUID,
            client_order_id="physical-witness",
            authority_public_id=_UUID,
            authority_content_digest="c" * 64,
            command_snapshot_public_id=_UUID,
            command_snapshot_content_digest="d" * 64,
            source_exchange="kraken",
            source_instrument_public_id=_UUID,
            current_spec_public_id=_UUID,
            current_spec_timestamp=_TIMESTAMP,
            source_spec_public_id=_UUID,
            source_spec_timestamp=_TIMESTAMP,
            economic_context_digest="e" * 64,
            request_digest="f" * 64,
            interpreted_at=_TIMESTAMP,
        )
    return result


def _insert(
    connection: sa.Connection, table: AdmissionEvidenceTable, values: dict[str, _Value]
) -> None:
    """Insert a fixed evidence-table shape through real physical constraints."""
    columns = ", ".join(values)
    parameters = ", ".join(f":{column}" for column in values)
    connection.execute(sa.text(f"INSERT INTO {table} ({columns}) VALUES ({parameters})"), values)


def _invoke(connection: sa.Connection, operation: str) -> None:
    """Run the exact public migration without controlling caller completion."""
    with patch.object(_MIGRATION, "op", Operations(MigrationContext.configure(connection))):
        getattr(_MIGRATION, operation)()


def _postgresql_contract(table: sa.Table) -> tuple[dict[str, tuple[str, bool]], set[str]]:
    """Compare actual rendered types, nullability and named constraint expressions."""
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


@pytest.fixture
def database(tmp_path: Path) -> Iterator[sa.Engine]:
    """Create committed migration DDL in a private file with immediate lock contention feedback."""
    engine = sa.create_engine(f"sqlite:///{tmp_path / '0057.db'}", connect_args={"timeout": 0})
    try:
        with engine.begin() as connection:
            connection.exec_driver_sql("PRAGMA recursive_triggers=ON")
            connection.exec_driver_sql("CREATE TABLE caller_work (id INTEGER PRIMARY KEY)")
            _invoke(connection, "upgrade")
        yield engine
    finally:
        engine.dispose()


def test_sqlite_empty_roundtrip_never_backfills_historical_authority(
    database: sa.Engine,
) -> None:
    """An empty reversible migration never guesses route or admission authority.

    Given: Actual upgraded SQLite storage with no route or admission witnesses,
    When: Empty downgrade and upgrade execute on a real connection,
    Then: Both tables disappear and return empty with all five scope indexes.
    """
    with database.begin() as connection:
        assert all(
            connection.scalar(sa.text(f"SELECT COUNT(*) FROM {table}")) == 0
            for table in ADMISSION_EVIDENCE_TABLES
        )
        _invoke(connection, "downgrade")
        assert all(
            not sa.inspect(connection).has_table(table) for table in ADMISSION_EVIDENCE_TABLES
        )
        _invoke(connection, "upgrade")
        assert all(
            connection.scalar(sa.text(f"SELECT COUNT(*) FROM {table}")) == 0
            for table in ADMISSION_EVIDENCE_TABLES
        )
        indexes = [
            index
            for table in ADMISSION_EVIDENCE_TABLES
            for index in sa.inspect(connection).get_indexes(table)
        ]
        assert len(indexes) == 5


@pytest.mark.parametrize("table", ADMISSION_EVIDENCE_TABLES)
def test_each_populated_table_refuses_before_any_guard_or_table_removal(
    database: sa.Engine, table: AdmissionEvidenceTable
) -> None:
    """Either evidence family independently makes downgrade lossy.

    Given: A physical witness in exactly one of the two evidence tables,
    When: Downgrade inspects both tables under its write reservation,
    Then: It refuses, keeps both tables and leaves their physical deletion guards installed.
    """
    with database.begin() as connection:
        _insert(connection, table, _values(table))
        with pytest.raises(RuntimeError, match="prevents a lossless downgrade"):
            _invoke(connection, "downgrade")
        assert connection.in_transaction()
        assert all(sa.inspect(connection).has_table(name) for name in ADMISSION_EVIDENCE_TABLES)
        with pytest.raises(IntegrityError, match="append-only"):
            connection.execute(sa.text(f"DELETE FROM {table}"))
        assert connection.scalar(sa.text(f"SELECT COUNT(*) FROM {table}")) == 1


@pytest.mark.parametrize(
    "changes",
    (
        {"public_id": _UUID, "content_digest": "x" * 64},
        {"content_digest": "b" * 64},
        {"exchange": "Paper"},
        {"exchange": " "},
        {"mode": "unknown"},
        {"environment": "unknown"},
        {"environment": "production"},
        {"margin_account": " "},
        {"credential_public_id": _UUID},
        {"credential_timestamp": _TIMESTAMP},
        {"exchange": "kraken", "mode": "live", "environment": "production"},
        {"account_scope_digest": "short"},
        {"route_revision": "short"},
        {"content_digest": "short"},
        {"canonical_payload": ""},
        {"valid_until": _TIMESTAMP},
        {"known_to": _TIMESTAMP},
    ),
)
def test_route_constraints_reject_raw_invalid_or_duplicate_evidence(
    database: sa.Engine, changes: dict[str, _Value]
) -> None:
    """Route identities and account bounds are enforced below application validation.

    Given: One valid route authority and an otherwise independent raw candidate,
    When: SQL violates identity, digest, account, credential, validity or permanent-active constraints,
    Then: SQLite rejects that candidate and retains the single original witness.
    """
    table = "execution_route_authorities"
    with database.begin() as connection:
        _insert(connection, table, _values(table))
        candidate = {
            **_values(table),
            "public_id": _OTHER_UUID,
            "content_digest": "x" * 64,
            **changes,
        }
        with pytest.raises(IntegrityError):
            _insert(connection, table, candidate)
        assert connection.scalar(sa.text(f"SELECT COUNT(*) FROM {table}")) == 1


@pytest.mark.parametrize(
    "changes",
    (
        {"public_id": _UUID},
        {"attempt_public_id": _UUID},
        {"exchange": "Paper"},
        {"exchange": " "},
        {"source_exchange": "Kraken"},
        {"source_exchange": " "},
        {"mode": "unknown"},
        {"account_scope_digest": "short"},
        {"authority_content_digest": "short"},
        {"command_snapshot_content_digest": "short"},
        {"economic_context_digest": "short"},
        {"request_digest": "short"},
        {"content_digest": "short"},
        {"canonical_payload": ""},
        {"known_to": _TIMESTAMP},
    ),
)
def test_admission_constraints_preserve_global_attempt_identity(
    database: sa.Engine, changes: dict[str, _Value]
) -> None:
    """A changed scope or content cannot admit a second witness for one attempt.

    Given: One valid admission and an otherwise independent raw candidate,
    When: SQL reuses its public or attempt identity or violates physical evidence bounds,
    Then: SQLite rejects the candidate without replacing or changing the original admission.
    """
    table = "submission_unit_admissions"
    with database.begin() as connection:
        _insert(connection, table, _values(table))
        candidate = {
            **_values(table),
            "public_id": _OTHER_UUID,
            "attempt_public_id": _OTHER_UUID,
            "content_digest": "x" * 64,
            **changes,
        }
        with pytest.raises(IntegrityError):
            _insert(connection, table, candidate)
        assert connection.scalar(sa.text(f"SELECT COUNT(*) FROM {table}")) == 1


@pytest.mark.parametrize("table", ADMISSION_EVIDENCE_TABLES)
@pytest.mark.parametrize("completion", ("commit", "rollback"))
def test_empty_downgrade_fences_writers_and_preserves_caller_transaction(
    database: sa.Engine, table: AdmissionEvidenceTable, completion: str
) -> None:
    """Actual SQLite DDL and earlier caller DML retain the same transaction owner.

    Given: Empty evidence tables and caller work in a real write transaction,
    When: Downgrade removes both tables and another connection tries to insert,
    Then: The writer remains fenced until caller completion determines DDL and earlier DML together.
    """
    with database.connect() as first, database.connect() as writer:
        transaction = first.begin()
        try:
            first.exec_driver_sql("INSERT INTO caller_work VALUES (77)")
            _invoke(first, "downgrade")
            assert first.in_transaction()
            with pytest.raises(OperationalError, match="database is locked"):
                _insert(writer, table, _values(table))
            writer.rollback()
            if completion == "commit":
                transaction.commit()
                with pytest.raises(OperationalError, match="no such table"):
                    _insert(writer, table, _values(table))
                writer.rollback()
            else:
                transaction.rollback()
                _insert(writer, table, _values(table))
                writer.commit()
                assert writer.scalar(sa.text(f"SELECT COUNT(*) FROM {table}")) == 1
            assert writer.scalar(sa.text("SELECT COUNT(*) FROM caller_work")) == (
                completion == "commit"
            )
            assert all(
                sa.inspect(writer).has_table(name) == (completion == "rollback")
                for name in ADMISSION_EVIDENCE_TABLES
            )
        finally:
            if transaction.is_active:
                transaction.rollback()


@pytest.mark.parametrize("table", ADMISSION_EVIDENCE_TABLES)
def test_prior_sqlite_writer_prevents_unprotected_empty_verdict(
    database: sa.Engine, table: AdmissionEvidenceTable
) -> None:
    """A real earlier writer prevents downgrade from counting an unsafe empty snapshot.

    Given: A prior empty observation and another connection's uncommitted evidence,
    When: Downgrade attempts its write fence before and after the writer commits,
    Then: It refuses committed evidence, retains its fence and leaves completion to the caller.
    """
    with database.connect() as first, database.connect() as writer:
        assert first.scalar(sa.text(f"SELECT COUNT(*) FROM {table}")) == 0
        _insert(writer, table, _values(table))
        with pytest.raises(OperationalError, match="database is locked"):
            _invoke(first, "downgrade")
        writer.commit()
        first.exec_driver_sql("INSERT INTO caller_work VALUES (77)")
        with pytest.raises(RuntimeError, match="prevents a lossless downgrade"):
            _invoke(first, "downgrade")
        assert first.in_transaction()
        assert first.scalar(sa.text(f"SELECT COUNT(*) FROM {table}")) == 1
        assert all(sa.inspect(first).has_table(name) for name in ADMISSION_EVIDENCE_TABLES)
        other = {**_values(table), "public_id": _OTHER_UUID, "content_digest": "x" * 64}
        if table == "submission_unit_admissions":
            other["attempt_public_id"] = _OTHER_UUID
        with pytest.raises(OperationalError, match="database is locked"):
            _insert(writer, table, other)
        writer.rollback()
        first.rollback()
        _insert(writer, table, other)
        writer.commit()
        assert writer.scalar(sa.text(f"SELECT COUNT(*) FROM {table}")) == 2
        assert writer.scalar(sa.text("SELECT COUNT(*) FROM caller_work")) == 0


def test_postgresql_migration_and_models_agree_on_schema_and_guard_removal() -> None:
    """The PostgreSQL-rendered physical schema agrees with both actual ORM table contracts.

    Given: Captured online PostgreSQL migration operations and the two ORM tables,
    When: Upgrade and serialized empty downgrade render their actual statements,
    Then: UUIDs, permanent sentinels, unique identities, indexes and both-table lock precede removal.
    """
    connection = MagicMock(
        dialect=postgres_dialect(),
        scalar=MagicMock(return_value=False),
        get_isolation_level=MagicMock(return_value="READ COMMITTED"),
    )
    operations = MagicMock(get_bind=MagicMock(return_value=connection))
    operations.get_context.return_value.as_sql = False
    with patch.object(_MIGRATION, "op", operations):
        _MIGRATION.upgrade()
        _MIGRATION.downgrade()
    models = {
        model.__table__.name: model.__table__
        for model in (ExecutionRouteAuthority, SubmissionUnitAdmission)
    }
    for call in operations.create_table.call_args_list:
        table = sa.Table(call.args[0], sa.MetaData(), *call.args[1:])
        ddl = str(CreateTable(table).compile(dialect=postgres_dialect()))
        model_ddl = str(CreateTable(models[table.name]).compile(dialect=postgres_dialect()))
        assert "UUID" in ddl
        assert "TIMESTAMPTZ '9999-12-31 23:59:59+00:00'" in ddl
        assert "TIMESTAMPTZ '9999-12-31 23:59:59+00:00'" in model_ddl
        assert _postgresql_contract(table) == _postgresql_contract(models[table.name])
    assert {
        call.args[0]: tuple(call.args[2]) for call in operations.create_index.call_args_list
    } == {
        index.name: tuple(column.name for column in index.columns)
        for table in models.values()
        for index in table.indexes
    }
    statements = [str(call.args[0]) for call in connection.execute.call_args_list]
    assert (
        "LOCK TABLE execution_route_authorities, submission_unit_admissions IN ACCESS EXCLUSIVE MODE"
        in statements
    )
    assert sum("ENABLE ALWAYS TRIGGER" in statement for statement in statements) == 4
    assert sum("DROP FUNCTION IF EXISTS" in statement for statement in statements) == 2
    assert [call.args[0] for call in operations.drop_table.call_args_list] == [
        "submission_unit_admissions",
        "execution_route_authorities",
    ]


@pytest.mark.parametrize("operation", ("upgrade", "downgrade"))
@pytest.mark.parametrize("profile", ("offline", "unsupported"))
def test_unproven_migration_profile_refuses_before_mutation(operation: str, profile: str) -> None:
    """An unsupported or offline context cannot silently omit physical evidence guards.

    Given: An offline context or unsupported actual database backend,
    When: Either public migration direction is requested,
    Then: No table mutation, evidence query or guard DDL occurs.
    """
    connection = MagicMock()
    connection.dialect.name = "unsupported"
    operations = MagicMock(get_bind=MagicMock(return_value=connection))
    operations.get_context.return_value.as_sql = profile == "offline"
    expected = RuntimeError if profile == "offline" else NotImplementedError
    with patch.object(_MIGRATION, "op", operations), pytest.raises(expected):
        getattr(_MIGRATION, operation)()
    operations.create_table.assert_not_called()
    operations.drop_table.assert_not_called()
    connection.execute.assert_not_called()
    connection.scalar.assert_not_called()


@pytest.mark.parametrize("isolation", ("REPEATABLE READ", "SERIALIZABLE", "AUTOCOMMIT", None))
def test_postgresql_downgrade_requires_read_committed_before_locking(
    isolation: str | None,
) -> None:
    """Only fresh statement visibility can prove an empty post-lock evidence store.

    Given: PostgreSQL without proven READ COMMITTED isolation,
    When: The public downgrade requests evidence serialization,
    Then: It refuses before locking, counting or destroying storage.
    """
    connection = MagicMock(
        dialect=postgres_dialect(),
        get_isolation_level=MagicMock(return_value=isolation),
    )
    operations = MagicMock(get_bind=MagicMock(return_value=connection))
    operations.get_context.return_value.as_sql = False
    with (
        patch.object(_MIGRATION, "op", operations),
        pytest.raises(RuntimeError, match="READ COMMITTED"),
    ):
        _MIGRATION.downgrade()
    connection.execute.assert_not_called()
    connection.scalar.assert_not_called()
    operations.drop_table.assert_not_called()


def test_backend_change_refuses_at_the_write_fence() -> None:
    """A changing adapter identity cannot bypass supported-backend serialization.

    Given: An adapter that first reports SQLite and then an unsupported backend,
    When: Public downgrade reaches its separate write-fence boundary,
    Then: It refuses before reading or mutating evidence.
    """
    changed = MagicMock()
    changed.dialect.name = "unsupported"
    operations = MagicMock(
        get_bind=MagicMock(side_effect=(MagicMock(dialect=sqlite_dialect()), changed))
    )
    operations.get_context.return_value.as_sql = False
    with patch.object(_MIGRATION, "op", operations), pytest.raises(NotImplementedError):
        _MIGRATION.downgrade()
    changed.execute.assert_not_called()
    changed.scalar.assert_not_called()
    operations.drop_table.assert_not_called()


def test_real_sqlite_autocommit_refuses_without_losing_evidence_tables() -> None:
    """A real autocommit driver cannot retain caller ownership through guard removal.

    Given: Actual empty evidence tables on a Python autocommit SQLite connection,
    When: Public downgrade requests caller-owned evidence serialization,
    Then: It refuses and both physical tables remain intact.
    """
    engine = sa.create_engine("sqlite://", connect_args={"autocommit": True})
    try:
        with engine.begin() as connection:
            _invoke(connection, "upgrade")
            with pytest.raises(RuntimeError, match="caller transaction completion"):
                _invoke(connection, "downgrade")
            assert all(
                sa.inspect(connection).has_table(table) for table in ADMISSION_EVIDENCE_TABLES
            )
    finally:
        engine.dispose()


@pytest.mark.parametrize("state", (None, "unknown"))
def test_missing_sqlite_transaction_state_refuses_before_guessing(
    state: object,
) -> None:
    """Missing physical driver evidence cannot be replaced by a logical SQLAlchemy transaction.

    Given: A SQLite driver with missing or malformed physical transaction state,
    When: Downgrade tries to establish its evidence write fence,
    Then: It refuses before issuing BEGIN, counting evidence or dropping tables.
    """
    connection = MagicMock(dialect=sqlite_dialect())
    connection.connection.driver_connection = SimpleNamespace(
        autocommit=False, in_transaction=state
    )
    operations = MagicMock(get_bind=MagicMock(return_value=connection))
    operations.get_context.return_value.as_sql = False
    with (
        patch.object(_MIGRATION, "op", operations),
        pytest.raises(RuntimeError, match="SQLite transaction state"),
    ):
        _MIGRATION.downgrade()
    connection.exec_driver_sql.assert_not_called()
    connection.scalar.assert_not_called()
    operations.drop_table.assert_not_called()


def test_ignored_begin_refuses_before_any_unprotected_empty_count() -> None:
    """An adapter that ignores BEGIN cannot claim a retained physical write reservation.

    Given: A SQLite driver still outside a physical transaction after BEGIN IMMEDIATE,
    When: Downgrade verifies that the write fence was retained,
    Then: It refuses without counting or destroying evidence.
    """
    connection = MagicMock(dialect=sqlite_dialect())
    connection.connection.driver_connection = SimpleNamespace(
        autocommit=False, in_transaction=False
    )
    operations = MagicMock(get_bind=MagicMock(return_value=connection))
    operations.get_context.return_value.as_sql = False
    with (
        patch.object(_MIGRATION, "op", operations),
        pytest.raises(RuntimeError, match="lost its SQLite write transaction"),
    ):
        _MIGRATION.downgrade()
    connection.exec_driver_sql.assert_called_once_with("BEGIN IMMEDIATE")
    connection.scalar.assert_not_called()
    operations.drop_table.assert_not_called()
