"""Physical native account authority graph, nonrenewable reads and caller-owned downgrade fences."""

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

from snapper.data.models import AccountingRawReceipt
from snapper.data.models import NativeAccountAuthority
from snapper.data.models import NativeAccountIdentityProof
from snapper.data.models import NativeAccountReadAttempt
from snapper.data.models import NativeAccountReadOutcome
from snapper.data.native_account_authority_triggers import NATIVE_ACCOUNT_AUTHORITY_TABLES
from snapper.data.native_account_authority_triggers import drop_native_account_authority_triggers
from snapper.data.native_account_authority_triggers import install_native_account_authority_triggers

_MIGRATION = import_module("snapper.data.migrations.versions.0063_native_account_authorities")
_IDENTITY_MIGRATION = import_module(
    "snapper.data.migrations.versions.0062_native_account_identity_proofs"
)
_READ_MIGRATION = import_module(
    "snapper.data.migrations.versions.0061_native_account_read_lifecycle"
)
_RAW_MIGRATION = import_module("snapper.data.migrations.versions.0058_contract_accounting_evidence")
_UUID = "00000000-0000-7000-8000-000000000001"
_OTHER_UUID = "00000000-0000-7000-8000-000000000004"
_TIMESTAMP = "2026-10-23 00:00:00.000000"
_AUTHORITIES = "native_account_authorities"
type _Value = str | int | None


_RAW = "accounting_raw_receipts"
_ATTEMPTS = "native_account_read_attempts"
_OUTCOMES = "native_account_read_outcomes"
_PROOFS = "native_account_identity_proofs"
_SOURCE_TIME = "2026-10-23 00:00:00.000000"
_OBSERVED = "2026-10-23 00:00:01.000000"


def _values(table: str = _AUTHORITIES) -> dict[str, _Value]:
    """Describe physical authority columns without claiming actual authenticated source bindings."""
    assert table == _AUTHORITIES
    return {
        "public_id": _UUID,
        "session_id": _UUID,
        "sequence_id": 1,
        "timestamp": "2026-10-23 00:00:02.000000",
        "known_to": "9999-12-31 23:59:59.000000",
        "wallet_public_id": _UUID,
        "exchange": "kraken_futures",
        "mode": "live",
        "environment": "production",
        "margin_account": "flex",
        "account_scope_digest": "a" * 64,
        "credential_public_id": "00000000-0000-7000-8000-000000000002",
        "credential_timestamp": _SOURCE_TIME,
        "public_api_origin": "https://futures.kraken.com",
        "venue_account_id": "00000000-0000-7000-8000-000000000003",
        "owner_proof_public_id": "00000000-0000-7000-8000-000000000004",
        "read_attempt_public_id": "00000000-0000-7000-8000-000000000005",
        "read_outcome_public_id": "00000000-0000-7000-8000-000000000006",
        "raw_receipt_public_id": "00000000-0000-7000-8000-000000000007",
        "observed_at": _OBSERVED,
        "valid_until": "2026-10-23 00:00:03.000000",
        "route_revision": "c" * 64,
        "content_digest": "b" * 64,
        "canonical_payload": "{}",
    }


def _provenance(values: dict[str, _Value], public_id: _Value, stamp: str) -> dict[str, _Value]:
    """Use actual physical parent rows while excluding any financial or authenticated HTTP claim."""
    return {
        "public_id": public_id,
        "session_id": _UUID,
        "sequence_id": 1,
        "timestamp": stamp,
        "known_to": "9999-12-31 23:59:59.000000",
        "wallet_public_id": values["wallet_public_id"],
        "exchange": "kraken_futures",
        "mode": "live",
        "environment": values["environment"],
        "margin_account": None,
        "content_digest": "8" * 64,
        "canonical_payload": "{}",
    }


def _exists(connection: sa.Connection, table: str, public_id: _Value) -> bool:
    """Locate an actual immutable parent through a statically bounded physical table."""
    assert table in (_RAW, _ATTEMPTS, _OUTCOMES, _PROOFS)
    return bool(
        connection.scalar(
            sa.text(f"SELECT COUNT(*) FROM {table} WHERE public_id=:id"), {"id": public_id}
        )
    )


def _dependency_insert(connection: sa.Connection, table: str, values: dict[str, _Value]) -> None:
    """Insert only the actual fixed parent tables used by the public migrations."""
    assert table in (_RAW, _ATTEMPTS, _OUTCOMES, _PROOFS)
    columns = ", ".join(values)
    params = ", ".join(f":{column}" for column in values)
    connection.execute(sa.text(f"INSERT INTO {table} ({columns}) VALUES ({params})"), values)


def _retain_read(connection: sa.Connection, values: dict[str, _Value]) -> int:
    """Allocate an actual physical request ordinal rather than manufacturing an execution prefix."""
    public_id = values["read_attempt_public_id"]
    if _exists(connection, _ATTEMPTS, public_id):
        sequence = connection.scalar(
            sa.text("SELECT family_sequence FROM native_account_read_attempts WHERE public_id=:id"),
            {"id": public_id},
        )
    else:
        sequence = connection.scalar(
            sa.text(
                "SELECT COALESCE(MAX(family_sequence), 0) + 1 FROM native_account_read_attempts WHERE scope_digest=:scope"
            ),
            {"scope": "d" * 64},
        )
        _dependency_insert(
            connection,
            _ATTEMPTS,
            {
                **_provenance(values, public_id, _SOURCE_TIME),
                "scope_digest": "d" * 64,
                "family_sequence": sequence,
                "client_selection_digest": "e" * 64,
                "started_at": _SOURCE_TIME,
            },
        )
    assert isinstance(sequence, int)
    return sequence


def _retain_owner(connection: sa.Connection, values: dict[str, _Value], sequence: int) -> None:
    """Retain a real initial or later public proof referencing its actual parent attempt."""
    public_id = values["owner_proof_public_id"]
    if _exists(connection, _PROOFS, public_id):
        return
    root = connection.execute(
        sa.text(
            "SELECT public_id, content_digest, account_uid FROM native_account_identity_proofs WHERE scope_digest=:scope AND owner_root_public_id IS NULL"
        ),
        {"scope": "d" * 64},
    ).one_or_none()
    _dependency_insert(
        connection,
        _PROOFS,
        {
            **_provenance(values, public_id, _SOURCE_TIME),
            "scope_digest": "d" * 64,
            "family_sequence": sequence,
            "client_selection_digest": "e" * 64,
            "attempt_public_id": values["read_attempt_public_id"],
            "attempt_content_digest": "8" * 64,
            "owner_root_public_id": None if root is None else root[0],
            "owner_root_content_digest": None if root is None else root[1],
            "owner_root_account_uid": None if root is None else root[2],
            "source_policy": "kraken_futures_key_check_redacted_v1",
            "account_uid": values["venue_account_id"],
            "general_access": "READ_ONLY",
            "transfer_access": "NO_ACCESS",
            "request_started_at": _SOURCE_TIME,
            "observed_at": _SOURCE_TIME,
        },
    )


def _retain_graph(connection: sa.Connection, values: dict[str, _Value]) -> None:
    """Create the actual 0058/0061/0062 parents required for a positive physical authority."""
    connection.exec_driver_sql("PRAGMA foreign_keys=ON")
    connection.exec_driver_sql("PRAGMA recursive_triggers=ON")
    assert connection.scalar(sa.text("PRAGMA foreign_keys")) == 1
    raw_id = values["raw_receipt_public_id"]
    if not _exists(connection, _RAW, raw_id):
        _dependency_insert(
            connection,
            _RAW,
            {
                **_provenance(values, raw_id, _OBSERVED),
                "claimed_public_id": raw_id,
                "scope_digest": "d" * 64,
                "route_authority_public_id": None,
                "kind": "native_account_state",
                "observed_at": _OBSERVED,
            },
        )
    sequence = _retain_read(connection, values)
    _retain_owner(connection, values, sequence)
    outcome_id = values["read_outcome_public_id"]
    if not _exists(connection, _OUTCOMES, outcome_id):
        _dependency_insert(
            connection,
            _OUTCOMES,
            {
                **_provenance(values, outcome_id, _OBSERVED),
                "scope_digest": "d" * 64,
                "family_sequence": sequence,
                "attempt_public_id": values["read_attempt_public_id"],
                "attempt_content_digest": "8" * 64,
                "completed_at": _OBSERVED,
                "result_kind": "captured",
                "raw_receipt_public_id": raw_id,
            },
        )


def _insert_valid(connection: sa.Connection, table: str, values: dict[str, _Value]) -> None:
    """Keep all four actual FK parents present before checking a positive authority row."""
    _retain_graph(connection, values)
    _insert(connection, table, values)


def _insert(connection: sa.Connection, table: str, values: dict[str, _Value]) -> None:
    """Insert a statically selected evidence shape through the real physical schema."""
    assert table in NATIVE_ACCOUNT_AUTHORITY_TABLES
    columns = ", ".join(values)
    parameters = ", ".join(f":{column}" for column in values)
    connection.execute(sa.text(f"INSERT INTO {table} ({columns}) VALUES ({parameters})"), values)


def _invoke(connection: sa.Connection, operation: str) -> None:
    """Call the exact public migration while retaining caller-controlled completion."""
    with patch.object(_MIGRATION, "op", Operations(MigrationContext.configure(connection))):
        getattr(_MIGRATION, operation)()


def _postgresql_contract(table: sa.Table) -> tuple[dict[str, tuple[str, bool]], set[str]]:
    """Compare actual PostgreSQL column types/nullability and named constraint expressions."""
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
    """Prepare real isolated file-backed DDL with recursive guards and immediate lock feedback."""
    engine = sa.create_engine(f"sqlite:///{tmp_path / '0063.db'}", connect_args={"timeout": 0})
    try:
        with engine.begin() as connection:
            connection.exec_driver_sql("PRAGMA recursive_triggers=ON")
            connection.exec_driver_sql("PRAGMA foreign_keys=ON")
            connection.exec_driver_sql("CREATE TABLE caller_work (id INTEGER PRIMARY KEY)")
            with patch.object(
                _RAW_MIGRATION, "op", Operations(MigrationContext.configure(connection))
            ):
                _RAW_MIGRATION.upgrade()
            with patch.object(
                _READ_MIGRATION, "op", Operations(MigrationContext.configure(connection))
            ):
                _READ_MIGRATION.upgrade()
            with patch.object(
                _IDENTITY_MIGRATION, "op", Operations(MigrationContext.configure(connection))
            ):
                _IDENTITY_MIGRATION.upgrade()
            _invoke(connection, "upgrade")
        yield engine
    finally:
        engine.dispose()


def test_empty_roundtrip_creates_no_financial_backfill(database: sa.Engine) -> None:
    """An empty native authority migration never invents a prior read, profile or validity interval.

    Given: The authority table was created by the real upgrade,
    When: Empty downgrade and upgrade execute on the same caller connection,
    Then: The authority table disappears and returns empty with its scope index.
    """
    with database.begin() as connection:
        assert all(
            connection.scalar(sa.text(f"SELECT COUNT(*) FROM {table}")) == 0
            for table in NATIVE_ACCOUNT_AUTHORITY_TABLES
        )
        _invoke(connection, "downgrade")
        assert all(
            not sa.inspect(connection).has_table(table) for table in NATIVE_ACCOUNT_AUTHORITY_TABLES
        )
        _invoke(connection, "upgrade")
        assert all(
            connection.scalar(sa.text(f"SELECT COUNT(*) FROM {table}")) == 0
            for table in NATIVE_ACCOUNT_AUTHORITY_TABLES
        )
        indexes = [
            index
            for table in NATIVE_ACCOUNT_AUTHORITY_TABLES
            for index in sa.inspect(connection).get_indexes(table)
        ]
        assert len(indexes) == 1
        assert not any(index["unique"] for index in indexes)


@pytest.mark.parametrize("table", NATIVE_ACCOUNT_AUTHORITY_TABLES)
@pytest.mark.parametrize("mutation", ("update", "delete", "close", "replace"))
def test_every_evidence_family_physically_refuses_row_mutation(
    database: sa.Engine, table: str, mutation: str
) -> None:
    """Direct SQL cannot rewrite retained source evidence or hide it through a temporal close.

    Given: One row in an actual append-only evidence table,
    When: SQL updates, deletes, temporally closes or replaces that identity,
    Then: A physical guard rejects the mutation and preserves the original row.
    """
    statements = {
        "update": f"UPDATE {table} SET sequence_id = sequence_id + 1",
        "delete": f"DELETE FROM {table}",
        "close": f"UPDATE {table} SET known_to = '2026-10-24 00:00:00.000000'",
    }
    with database.begin() as connection:
        values = _values(table)
        _insert_valid(connection, table, values)
        with pytest.raises(IntegrityError, match="append-only"):
            if mutation == "replace":
                columns = ", ".join(values)
                parameters = ", ".join(f":{column}" for column in values)
                connection.execute(
                    sa.text(f"INSERT OR REPLACE INTO {table} ({columns}) VALUES ({parameters})"),
                    {**values, "sequence_id": 2},
                )
            else:
                connection.execute(sa.text(statements[mutation]))
        assert connection.scalar(sa.text(f"SELECT COUNT(*) FROM {table}")) == 1
        assert connection.scalar(sa.text(f"SELECT sequence_id FROM {table}")) == 1


@pytest.mark.parametrize("table", NATIVE_ACCOUNT_AUTHORITY_TABLES)
@pytest.mark.parametrize(
    "changes",
    (
        {"mode": "unknown"},
        {"environment": "unknown"},
        {"environment": "paper"},
        {"mode": "paper"},
        {"exchange": "paper"},
        {"exchange": "KRAKEN_FUTURES"},
        {"exchange": " "},
        {"known_to": _TIMESTAMP},
    ),
)
def test_invalid_physical_scope_never_enters_any_evidence_family(
    database: sa.Engine, table: str, changes: dict[str, _Value]
) -> None:
    """Unknown or inconsistent account environment cannot become retained physical evidence.

    Given: One otherwise valid row for an actual evidence table,
    When: Its scope or permanent-known-to sentinel is inconsistent,
    Then: The table rejects insertion and remains empty.
    """
    with database.begin() as connection:
        _retain_graph(connection, _values(table))
        with pytest.raises(IntegrityError):
            _insert(connection, table, {**_values(table), **changes})
        assert connection.scalar(sa.text(f"SELECT COUNT(*) FROM {table}")) == 0


@pytest.mark.parametrize("table", NATIVE_ACCOUNT_AUTHORITY_TABLES)
def test_same_public_identity_cannot_replace_retained_evidence(
    database: sa.Engine, table: str
) -> None:
    """Public immutable evidence identity is independently unique in every family.

    Given: One retained row,
    When: A second insert reuses its public identity,
    Then: The unique constraint keeps the original single row.
    """
    with database.begin() as connection:
        _insert_valid(connection, table, _values(table))
        with pytest.raises(IntegrityError):
            _insert_valid(connection, table, _values(table))
        assert connection.scalar(sa.text(f"SELECT COUNT(*) FROM {table}")) == 1


@pytest.mark.parametrize("table", NATIVE_ACCOUNT_AUTHORITY_TABLES)
@pytest.mark.parametrize("margin_account", ("", "   "))
def test_blank_account_labels_cannot_become_physical_evidence(
    database: sa.Engine, table: str, margin_account: str
) -> None:
    """Empty account identity cannot become a native authority namespace.

    Given: A physical source row with an explicitly supplied account label,
    When: That label is empty or whitespace only,
    Then: Its scope constraint refuses the evidence.
    """
    with database.begin() as connection:
        with pytest.raises(IntegrityError):
            _insert_valid(connection, table, {**_values(table), "margin_account": margin_account})
        assert connection.scalar(sa.text(f"SELECT COUNT(*) FROM {table}")) == 0


@pytest.mark.parametrize("table", NATIVE_ACCOUNT_AUTHORITY_TABLES)
def test_any_retained_evidence_refuses_downgrade_before_guard_removal(
    database: sa.Engine, table: str
) -> None:
    """Even an expired authority prevents lossy removal of its original source history.

    Given: One physical immutable authority and its actual source parents,
    When: The real downgrade evaluates evidence under its write reservation,
    Then: All tables and their mutation guards remain and completion stays with the caller.
    """
    with database.begin() as connection:
        _insert_valid(connection, table, _values(table))
        with pytest.raises(RuntimeError, match="prevents a lossless downgrade"):
            _invoke(connection, "downgrade")
        assert connection.in_transaction()
        assert all(
            sa.inspect(connection).has_table(name) for name in NATIVE_ACCOUNT_AUTHORITY_TABLES
        )
        with pytest.raises(IntegrityError, match="append-only"):
            connection.execute(sa.text(f"DELETE FROM {table}"))
        assert connection.scalar(sa.text(f"SELECT COUNT(*) FROM {table}")) == 1


@pytest.mark.parametrize("completion", ("commit", "rollback"))
@pytest.mark.parametrize("table", NATIVE_ACCOUNT_AUTHORITY_TABLES)
def test_empty_downgrade_fences_writers_until_actual_caller_completion(
    database: sa.Engine, table: str, completion: str
) -> None:
    """DDL and prior caller work share one physical completion decision.

    Given: Empty evidence tables and existing caller DML in a real transaction,
    When: Downgrade removes storage and another connection tries to add evidence,
    Then: The writer stays fenced and caller commit/rollback controls both DDL and earlier DML.
    """
    with database.connect() as first, database.connect() as writer:
        transaction = first.begin()
        try:
            first.exec_driver_sql("INSERT INTO caller_work VALUES (77)")
            _invoke(first, "downgrade")
            assert first.in_transaction()
            with pytest.raises(OperationalError, match="database is locked"):
                _insert_valid(writer, table, _values(table))
            writer.rollback()
            if completion == "commit":
                transaction.commit()
                with pytest.raises(OperationalError, match="no such table"):
                    _insert_valid(writer, table, _values(table))
                writer.rollback()
            else:
                transaction.rollback()
                _insert_valid(writer, table, _values(table))
                writer.commit()
                assert writer.scalar(sa.text(f"SELECT COUNT(*) FROM {table}")) == 1
            assert writer.scalar(sa.text("SELECT COUNT(*) FROM caller_work")) == (
                completion == "commit"
            )
            assert all(
                sa.inspect(writer).has_table(name) == (completion == "rollback")
                for name in NATIVE_ACCOUNT_AUTHORITY_TABLES
            )
        finally:
            if transaction.is_active:
                transaction.rollback()


@pytest.mark.parametrize("table", NATIVE_ACCOUNT_AUTHORITY_TABLES)
def test_prior_uncommitted_writer_prevents_an_unprotected_empty_verdict(
    database: sa.Engine, table: str
) -> None:
    """A concurrent source insertion must precede a safe downgrade verdict.

    Given: An empty read followed by another connection's uncommitted evidence,
    When: Downgrade attempts its write fence before and after that writer commits,
    Then: Lock contention first refuses and committed evidence then prevents destruction.
    """
    with database.connect() as first, database.connect() as writer:
        assert first.scalar(sa.text(f"SELECT COUNT(*) FROM {table}")) == 0
        _insert_valid(writer, table, _values(table))
        with pytest.raises(OperationalError, match="database is locked"):
            _invoke(first, "downgrade")
        writer.commit()
        with pytest.raises(RuntimeError, match="prevents a lossless downgrade"):
            _invoke(first, "downgrade")
        assert first.in_transaction()
        assert first.scalar(sa.text(f"SELECT COUNT(*) FROM {table}")) == 1
        assert all(sa.inspect(first).has_table(name) for name in NATIVE_ACCOUNT_AUTHORITY_TABLES)
        first.rollback()


@pytest.mark.parametrize("table", NATIVE_ACCOUNT_AUTHORITY_TABLES)
def test_postgresql_constraint_names_are_unique_within_the_authority_table(table: str) -> None:
    """Different physical checks cannot share a PostgreSQL constraint identity.

    Given: Actual model declarations with backend-conditional sentinel checks,
    When: Every named constraint emitted to PostgreSQL is grouped within the authority table,
    Then: Every name is unique so PostgreSQL can retain every declared check.
    """
    tables = {model.__table__.name: model.__table__ for model in (NativeAccountAuthority,)}
    ddl = str(CreateTable(tables[table]).compile(dialect=postgres_dialect()))
    names = [line.split()[1] for line in ddl.splitlines() if line.strip().startswith("CONSTRAINT ")]
    assert len(names) == len(set(names))


def test_postgresql_rendered_models_and_migration_match_without_claiming_live_evidence() -> None:
    """The model and migration declare identical PostgreSQL types, constraints and indexes.

    Given: Captured actual migration operations and the ORM authority-table contract,
    When: PostgreSQL upgrade and serialized empty downgrade render their statements,
    Then: UUID/sentinel/source constraints and all-table locking precede guard/table removal.
    """
    connection = MagicMock(
        dialect=postgres_dialect(),
        scalar=MagicMock(return_value=0),
        get_isolation_level=MagicMock(return_value="READ COMMITTED"),
    )
    operations = MagicMock(get_bind=MagicMock(return_value=connection))
    operations.get_context.return_value.as_sql = False
    with patch.object(_MIGRATION, "op", operations):
        _MIGRATION.upgrade()
        _MIGRATION.downgrade()
    models = {model.__table__.name: model.__table__ for model in (NativeAccountAuthority,)}
    metadata = sa.MetaData()
    AccountingRawReceipt.__table__.to_metadata(metadata)
    NativeAccountReadAttempt.__table__.to_metadata(metadata)
    NativeAccountReadOutcome.__table__.to_metadata(metadata)
    NativeAccountIdentityProof.__table__.to_metadata(metadata)
    captured = [
        sa.Table(call.args[0], metadata, *call.args[1:])
        for call in operations.create_table.call_args_list
    ]
    for table in captured:
        assert _postgresql_contract(table) == _postgresql_contract(models[table.name])
    assert {
        call.args[0]: (
            tuple(call.args[2]),
            bool(call.kwargs.get("unique", False)),
            str(call.kwargs.get("postgresql_where")),
        )
        for call in operations.create_index.call_args_list
    } == {
        index.name: (
            tuple(column.name for column in index.columns),
            bool(index.unique),
            str(index.dialect_options["postgresql"].get("where")),
        )
        for table in models.values()
        for index in table.indexes
    }
    statements = [str(call.args[0]) for call in connection.execute.call_args_list]
    lock = "LOCK TABLE " + ", ".join(NATIVE_ACCOUNT_AUTHORITY_TABLES) + " IN ACCESS EXCLUSIVE MODE"
    assert lock in statements
    assert sum("ENABLE ALWAYS TRIGGER" in statement for statement in statements) == 2
    assert statements[-1] == "DROP FUNCTION IF EXISTS native_account_authority_reject_mutation()"
    assert [call.args[0] for call in operations.drop_table.call_args_list] == list(
        reversed(NATIVE_ACCOUNT_AUTHORITY_TABLES)
    )


@pytest.mark.parametrize("operation", ("upgrade", "downgrade"))
@pytest.mark.parametrize("profile", ("offline", "unsupported"))
def test_unproven_database_profile_refuses_before_evidence_mutation(
    operation: str, profile: str
) -> None:
    """Offline or unsupported migration profiles cannot omit physical evidence protection.

    Given: A SQL-only context or an unsupported actual database adapter,
    When: Either public migration direction executes,
    Then: It refuses before evidence reads or destructive DDL.
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
def test_postgresql_requires_fresh_read_committed_visibility_before_lock(
    isolation: str | None,
) -> None:
    """An old transaction snapshot cannot establish an empty post-lock evidence store.

    Given: A PostgreSQL caller without proven READ COMMITTED isolation,
    When: Downgrade attempts its evidence fence,
    Then: It refuses before locking, querying or removing storage.
    """
    connection = MagicMock(
        dialect=postgres_dialect(), get_isolation_level=MagicMock(return_value=isolation)
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


def test_real_sqlite_autocommit_cannot_hide_loss_of_caller_completion() -> None:
    """An actual autocommit driver cannot own the evidence fence until caller completion.

    Given: Empty real authority storage and a SQLite autocommit driver,
    When: Public downgrade requests caller-controlled completion,
    Then: It refuses without dropping any table.
    """
    engine = sa.create_engine("sqlite://", connect_args={"autocommit": True})
    try:
        with engine.begin() as connection:
            _invoke(connection, "upgrade")
            with pytest.raises(RuntimeError, match="caller-controlled completion"):
                _invoke(connection, "downgrade")
            assert all(
                sa.inspect(connection).has_table(table) for table in NATIVE_ACCOUNT_AUTHORITY_TABLES
            )
    finally:
        engine.dispose()


@pytest.mark.parametrize("state", (None, "unknown", False))
def test_unproven_or_ignored_sqlite_driver_transaction_refuses_before_counting(
    state: object,
) -> None:
    """Logical transaction objects cannot replace proof of a retained physical write fence.

    Given: Missing/malformed driver state or an ignored BEGIN IMMEDIATE,
    When: Public downgrade verifies its actual write reservation,
    Then: It refuses before reading or removing evidence.
    """
    connection = MagicMock(dialect=sqlite_dialect())
    connection.connection.driver_connection = SimpleNamespace(
        autocommit=False, in_transaction=state
    )
    operations = MagicMock(get_bind=MagicMock(return_value=connection))
    operations.get_context.return_value.as_sql = False
    with patch.object(_MIGRATION, "op", operations), pytest.raises(RuntimeError):
        _MIGRATION.downgrade()
    connection.scalar.assert_not_called()
    operations.drop_table.assert_not_called()
    if state is False:
        connection.exec_driver_sql.assert_called_once_with("BEGIN IMMEDIATE")
    else:
        connection.exec_driver_sql.assert_not_called()


@pytest.mark.parametrize("operation", ("install", "drop"))
@pytest.mark.parametrize("profile", ("unknown_table", "unsupported_backend"))
def test_physical_guard_helpers_refuse_unproven_tables_or_adapters(
    operation: str, profile: str
) -> None:
    """Guard installation cannot interpolate arbitrary identifiers or omit backend protection.

    Given: An unknown evidence table or unsupported database adapter,
    When: Either physical guard direction is requested,
    Then: It refuses before executing any DDL.
    """
    connection = MagicMock()
    connection.dialect.name = "unsupported"
    table = "unknown_table" if profile == "unknown_table" else _AUTHORITIES
    function = (
        install_native_account_authority_triggers
        if operation == "install"
        else drop_native_account_authority_triggers
    )
    expected = ValueError if profile == "unknown_table" else NotImplementedError
    with pytest.raises(expected):
        function(connection, table)
    connection.execute.assert_not_called()


def test_changed_adapter_refuses_at_the_actual_downgrade_fence() -> None:
    """Separate write-fence validation does not trust a previously supported adapter identity.

    Given: An adapter initially labeled SQLite and then unsupported at the fence,
    When: The public downgrade reaches its actual serialization operation,
    Then: It refuses before counting or changing any evidence.
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


def _independent_graph() -> dict[str, _Value]:
    """Allocate distinct actual parent identities so uniqueness checks prove one boundary at a time."""
    return {
        **_values(),
        "public_id": "00000000-0000-7000-8000-000000000011",
        "owner_proof_public_id": "00000000-0000-7000-8000-000000000014",
        "read_attempt_public_id": "00000000-0000-7000-8000-000000000015",
        "read_outcome_public_id": "00000000-0000-7000-8000-000000000016",
        "raw_receipt_public_id": "00000000-0000-7000-8000-000000000017",
        "content_digest": "f" * 64,
    }


@pytest.mark.parametrize(
    "field",
    (
        "owner_proof_public_id",
        "read_attempt_public_id",
        "read_outcome_public_id",
        "raw_receipt_public_id",
    ),
)
def test_each_source_pointer_requires_its_actual_retained_parent(
    database: sa.Engine, field: str
) -> None:
    """Every authority source pointer is protected by a real immediate foreign key.

    Given: A complete actual 0058/0061/0062 parent graph without an authority,
    When: One authority pointer names an absent row while all other parents remain valid,
    Then: Direct SQL fails its foreign key and retains no authority.
    """
    with database.begin() as connection:
        _retain_graph(connection, _values())
        with pytest.raises(IntegrityError, match="FOREIGN KEY"):
            _insert(
                connection,
                _AUTHORITIES,
                {
                    **_values(),
                    field: "00000000-0000-7000-8000-000000000099",
                },
            )
        assert connection.scalar(sa.text("SELECT COUNT(*) FROM native_account_authorities")) == 0


@pytest.mark.parametrize(
    "changes",
    (
        {"valid_until": "2026-10-23 01:00:03.000000"},
        {"route_revision": "e" * 64},
        {"account_scope_digest": "f" * 64},
        {"credential_public_id": "00000000-0000-7000-8000-000000000088"},
    ),
)
def test_same_actual_read_and_profile_cannot_receive_a_new_authority(
    database: sa.Engine,
    changes: dict[str, _Value],
) -> None:
    """A prior read cannot be renewed or reissued by changing indexed or credential metadata.

    Given: One authority for an actual retained read and its literal native margin profile,
    When: Another public identity and content digest reuse that read/profile with changed metadata,
    Then: The independent read/profile uniqueness rule preserves only the original issuance.
    """
    with database.begin() as connection:
        _insert_valid(connection, _AUTHORITIES, _values())
        with pytest.raises(IntegrityError, match="read_attempt_public_id.*margin_account"):
            _insert(
                connection,
                _AUTHORITIES,
                {
                    **_values(),
                    "public_id": _OTHER_UUID,
                    "content_digest": "e" * 64,
                    **changes,
                },
            )
        row = connection.execute(
            sa.text("SELECT valid_until, content_digest FROM native_account_authorities")
        ).one()
        assert tuple(row) == (_values()["valid_until"], _values()["content_digest"])


def test_distinct_literal_profiles_can_reference_the_same_actual_read(database: sa.Engine) -> None:
    """Physical read/profile uniqueness retains each distinct native profile once.

    Given: An authority for the flex profile of one actual retained read,
    When: Another authority references that same read with a different literal profile,
    Then: Both physical rows remain without certifying their placeholder payloads as authenticated profiles.
    """
    with database.begin() as connection:
        _insert_valid(connection, _AUTHORITIES, _values())
        _insert(
            connection,
            _AUTHORITIES,
            {
                **_values(),
                "public_id": _OTHER_UUID,
                "content_digest": "e" * 64,
                "margin_account": "cash",
                "account_scope_digest": "f" * 64,
            },
        )
        assert connection.scalar(sa.text("SELECT COUNT(*) FROM native_account_authorities")) == 2


@pytest.mark.parametrize("field", ("content_digest", "public_id"))
def test_content_and_public_identity_uniqueness_are_independent(
    database: sa.Engine, field: str
) -> None:
    """Equal content and equal public identity each collide independently of source-profile identity.

    Given: One retained authority and another complete graph with a distinct actual read,
    When: The second authority reuses only one of the original independent unique fields,
    Then: Its specific unique constraint rejects insertion while both actual source graphs remain.
    """
    with database.begin() as connection:
        _insert_valid(connection, _AUTHORITIES, _values())
        second = {**_independent_graph(), field: _values()[field]}
        _retain_graph(connection, second)
        with pytest.raises(IntegrityError, match=field):
            _insert(connection, _AUTHORITIES, second)
        assert connection.scalar(sa.text("SELECT COUNT(*) FROM native_account_read_attempts")) == 2
        assert connection.scalar(sa.text("SELECT COUNT(*) FROM native_account_authorities")) == 1


@pytest.mark.parametrize(
    "changes",
    (
        {"margin_account": None},
        {"account_scope_digest": "short"},
        {"route_revision": "short"},
        {"content_digest": "short"},
        {"canonical_payload": "  "},
        {"public_api_origin": "https://demo-futures.kraken.com"},
        {"public_api_origin": "https://futures.kraken.com/"},
        {"public_api_origin": "https://example.invalid"},
        {"environment": "demo"},
        {"observed_at": "2026-10-23 00:00:02.000001"},
        {"valid_until": "2026-10-23 00:00:02.000000"},
        {"valid_until": "2026-10-23 00:00:01.000000"},
        {"credential_timestamp": "2026-10-23 00:00:01.000001"},
    ),
)
def test_authority_requires_exact_origin_scope_and_original_validity(
    database: sa.Engine,
    changes: dict[str, _Value],
) -> None:
    """Malformed native authority metadata cannot pass a physical insert.

    Given: A valid actual parent graph and otherwise valid authority columns,
    When: Its literal profile, digest, payload, API origin or issuance chronology is invalid,
    Then: The physical constraint rejects that authority without changing retained parents.
    """
    with database.begin() as connection:
        _retain_graph(connection, _values())
        with pytest.raises(IntegrityError):
            _insert(connection, _AUTHORITIES, {**_values(), **changes})
        assert connection.scalar(sa.text("SELECT COUNT(*) FROM native_account_authorities")) == 0
        assert connection.scalar(sa.text("SELECT COUNT(*) FROM native_account_read_attempts")) == 1


def test_demo_authority_keeps_the_actual_demo_origin(database: sa.Engine) -> None:
    """The demo namespace has a positive exact-origin physical witness.

    Given: Actual parent rows carrying the demo environment,
    When: An otherwise valid authority uses the literal demo API origin,
    Then: Its distinct environment and origin remain stored together.
    """
    with database.begin() as connection:
        _insert_valid(
            connection,
            _AUTHORITIES,
            {
                **_values(),
                "environment": "demo",
                "public_api_origin": "https://demo-futures.kraken.com",
            },
        )
        row = connection.execute(
            sa.text("SELECT environment, public_api_origin FROM native_account_authorities")
        ).one()
        assert tuple(row) == ("demo", "https://demo-futures.kraken.com")
