"""Physical request/outcome ownership, foreign keys and caller-owned downgrade fences."""

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
from snapper.data.models import NativeAccountReadAttempt
from snapper.data.models import NativeAccountReadOutcome
from snapper.data.native_account_read_triggers import NATIVE_ACCOUNT_READ_TABLES
from snapper.data.native_account_read_triggers import drop_native_account_read_triggers
from snapper.data.native_account_read_triggers import install_native_account_read_triggers

_MIGRATION = import_module("snapper.data.migrations.versions.0061_native_account_read_lifecycle")
_RAW_MIGRATION = import_module("snapper.data.migrations.versions.0058_contract_accounting_evidence")
_UUID = "00000000-0000-7000-8000-000000000001"
_OTHER_UUID = "00000000-0000-7000-8000-000000000002"
_TIMESTAMP = "2026-10-23 00:00:00.000000"
_ATTEMPTS = "native_account_read_attempts"
_OUTCOMES = "native_account_read_outcomes"
_RAW = "accounting_raw_receipts"
type _Value = str | int | None


def _values(table: str) -> dict[str, _Value]:
    """Provide physical request/outcome columns without claiming an actual HTTP read."""
    values: dict[str, _Value] = {
        "public_id": _UUID,
        "session_id": _UUID,
        "sequence_id": 1,
        "timestamp": _TIMESTAMP,
        "known_to": "9999-12-31 23:59:59.000000",
        "wallet_public_id": _UUID,
        "exchange": "kraken_futures",
        "mode": "live",
        "environment": "production",
        "margin_account": None,
        "scope_digest": "a" * 64,
        "family_sequence": 1,
        "content_digest": "b" * 64,
        "canonical_payload": "{}",
    }
    if table == _ATTEMPTS:
        values.update(client_selection_digest="c" * 64, started_at=_TIMESTAMP)
    else:
        values.update(
            attempt_public_id=_UUID,
            attempt_content_digest="b" * 64,
            completed_at=_TIMESTAMP,
            result_kind="captured",
            raw_receipt_public_id=_OTHER_UUID,
        )
    return values


def _insert_valid(connection: sa.Connection, table: str, values: dict[str, _Value]) -> None:
    """Create an actual parent row when the physical outcome scenario requires one."""
    if table == _OUTCOMES:
        _retain_raw(connection, values["raw_receipt_public_id"])
        parent_id = values["attempt_public_id"]
        exists = connection.scalar(
            sa.text("SELECT COUNT(*) FROM native_account_read_attempts WHERE public_id=:id"),
            {"id": parent_id},
        )
        if not exists:
            parent = {
                **_values(_ATTEMPTS),
                "public_id": parent_id,
                "family_sequence": values["family_sequence"],
            }
            _insert(connection, _ATTEMPTS, parent)
    _insert(connection, table, values)


def _retain_raw(connection: sa.Connection, public_id: _Value) -> None:
    """Retain an actual physical raw parent without certifying HTTP content or authority."""
    if public_id is None:
        return
    exists = connection.scalar(
        sa.text("SELECT COUNT(*) FROM accounting_raw_receipts WHERE public_id=:id"),
        {"id": public_id},
    )
    if exists:
        return
    _insert(
        connection,
        _RAW,
        {
            "public_id": public_id,
            "session_id": _UUID,
            "sequence_id": 1,
            "timestamp": _TIMESTAMP,
            "known_to": "9999-12-31 23:59:59.000000",
            "wallet_public_id": _UUID,
            "exchange": "kraken_futures",
            "mode": "live",
            "environment": "production",
            "margin_account": None,
            "scope_digest": "a" * 64,
            "claimed_public_id": public_id,
            "route_authority_public_id": None,
            "kind": "native_account_state",
            "observed_at": _TIMESTAMP,
            "content_digest": "b" * 64,
            "canonical_payload": "{}",
        },
    )


def _insert(connection: sa.Connection, table: str, values: dict[str, _Value]) -> None:
    """Insert a statically selected evidence shape through the real physical schema."""
    assert table in (*NATIVE_ACCOUNT_READ_TABLES, _RAW)
    columns = ", ".join(values)
    parameters = ", ".join(f":{column}" for column in values)
    connection.execute(sa.text(f"INSERT INTO {table} ({columns}) VALUES ({parameters})"), values)


def _invoke(connection: sa.Connection, operation: str) -> None:
    """Call the exact public migration while retaining caller-controlled completion."""
    with patch.object(_MIGRATION, "op", Operations(MigrationContext.configure(connection))):
        getattr(_MIGRATION, operation)()


@pytest.mark.parametrize(
    "changes",
    (
        {"scope_digest": "c" * 64},
        {"client_selection_digest": "d" * 64},
        {"scope_digest": "c" * 64, "client_selection_digest": "d" * 64},
    ),
)
def test_attempt_order_cannot_fork_by_digest_or_selected_credential(
    database: sa.Engine, changes: dict[str, _Value]
) -> None:
    """One actual family owns each request ordinal regardless of indexed client/digest copies.

    Given: A retained first attempt in an actual wallet, venue, mode and environment,
    When: Another UUID repeats its ordinal but changes digest or selected credential,
    Then: The direct family ordering key refuses the duplicate request coordinate.
    """
    with database.begin() as connection:
        _insert(connection, _ATTEMPTS, _values(_ATTEMPTS))
        with pytest.raises(IntegrityError):
            _insert(
                connection, _ATTEMPTS, {**_values(_ATTEMPTS), "public_id": _OTHER_UUID, **changes}
            )
        assert connection.scalar(sa.text("SELECT COUNT(*) FROM native_account_read_attempts")) == 1


def test_outcome_requires_an_actual_parent_attempt(database: sa.Engine) -> None:
    """An otherwise complete response claim cannot manufacture an absent request.

    Given: Actual migrated tables with foreign-key enforcement enabled,
    When: An outcome refers to an attempt UUID that has never been stored,
    Then: The foreign key refuses it and both read histories remain empty.
    """
    with database.begin() as connection:
        assert connection.scalar(sa.text("PRAGMA foreign_keys")) == 1
        _retain_raw(connection, _OTHER_UUID)
        with pytest.raises(IntegrityError, match="FOREIGN KEY"):
            _insert(connection, _OUTCOMES, _values(_OUTCOMES))
        assert connection.scalar(sa.text("SELECT COUNT(*) FROM native_account_read_attempts")) == 0
        assert connection.scalar(sa.text("SELECT COUNT(*) FROM native_account_read_outcomes")) == 0


@pytest.mark.parametrize("result_kind", ("captured", "failed"))
def test_nonnull_outcome_source_requires_an_actual_retained_raw_receipt(
    database: sa.Engine, result_kind: str
) -> None:
    """A claimed raw source cannot refer to bytes absent from durable receipt history.

    Given: A real migrated raw schema and retained attempt but no matching raw receipt,
    When: A captured or failed outcome claims a nonnull raw UUID,
    Then: The raw foreign key refuses it while preserving the attempt and empty outcome history.
    """
    with database.begin() as connection:
        assert connection.scalar(sa.text("PRAGMA foreign_keys")) == 1
        _insert(connection, _ATTEMPTS, _values(_ATTEMPTS))
        with pytest.raises(IntegrityError, match="FOREIGN KEY"):
            _insert(connection, _OUTCOMES, {**_values(_OUTCOMES), "result_kind": result_kind})
        assert connection.scalar(sa.text("SELECT COUNT(*) FROM accounting_raw_receipts")) == 0
        assert connection.scalar(sa.text("SELECT COUNT(*) FROM native_account_read_attempts")) == 1
        assert connection.scalar(sa.text("SELECT COUNT(*) FROM native_account_read_outcomes")) == 0


@pytest.mark.parametrize("table", NATIVE_ACCOUNT_READ_TABLES)
def test_public_identity_is_unique_independently_of_order_attempt_and_raw_keys(
    database: sa.Engine, table: str
) -> None:
    """Public immutable identity cannot be borrowed while every other logical key changes.

    Given: A retained attempt or outcome and independently allocated alternate logical keys,
    When: Another row keeps only the original public UUID,
    Then: Public identity uniqueness alone rejects the candidate.
    """
    with database.begin() as connection:
        _insert_valid(connection, table, _values(table))
        other = {**_values(table), "family_sequence": 2}
        if table == _OUTCOMES:
            other.update(
                attempt_public_id="00000000-0000-7000-8000-000000000003",
                raw_receipt_public_id="00000000-0000-7000-8000-000000000004",
            )
        with pytest.raises(IntegrityError):
            _insert_valid(connection, table, other)
        assert connection.scalar(sa.text(f"SELECT COUNT(*) FROM {table}")) == 1


def test_coarse_digest_ordinal_key_is_not_only_a_direct_family_key(database: sa.Engine) -> None:
    """One retained family digest and ordinal cannot be reattached to different indexed scope.

    Given: An actual stored first attempt,
    When: A candidate changes its wallet but reuses the old family digest and ordinal,
    Then: The digest-order unique key refuses the contradictory indexed copy.
    """
    with database.begin() as connection:
        _insert(connection, _ATTEMPTS, _values(_ATTEMPTS))
        with pytest.raises(IntegrityError):
            _insert(
                connection,
                _ATTEMPTS,
                {**_values(_ATTEMPTS), "public_id": _OTHER_UUID, "wallet_public_id": _OTHER_UUID},
            )
        assert connection.scalar(sa.text("SELECT COUNT(*) FROM native_account_read_attempts")) == 1


def test_one_attempt_cannot_be_replaced_with_a_second_outcome(database: sa.Engine) -> None:
    """A completed refusal cannot later become a different response for the same request.

    Given: One actual attempt and a retained failed outcome without a raw response,
    When: Another outcome UUID tries to claim that same attempt as captured,
    Then: Physical attempt uniqueness retains the original failed result.
    """
    with database.begin() as connection:
        _insert_valid(
            connection,
            _OUTCOMES,
            {**_values(_OUTCOMES), "result_kind": "failed", "raw_receipt_public_id": None},
        )
        _retain_raw(connection, _OTHER_UUID)
        with pytest.raises(IntegrityError):
            _insert(connection, _OUTCOMES, {**_values(_OUTCOMES), "public_id": _OTHER_UUID})
        assert (
            connection.scalar(sa.text("SELECT result_kind FROM native_account_read_outcomes"))
            == "failed"
        )


def test_one_actual_response_uuid_cannot_supply_two_attempts(database: sa.Engine) -> None:
    """Two requests cannot borrow one local HTTP response identity.

    Given: Distinct actual parent attempts and a captured response owned by the first,
    When: A second outcome reuses the same raw response UUID,
    Then: The shared raw-reference unique key refuses it without deleting either request.
    """
    with database.begin() as connection:
        _insert_valid(connection, _OUTCOMES, _values(_OUTCOMES))
        other = {
            **_values(_OUTCOMES),
            "public_id": _OTHER_UUID,
            "attempt_public_id": _OTHER_UUID,
            "family_sequence": 2,
        }
        with pytest.raises(IntegrityError):
            _insert_valid(connection, _OUTCOMES, other)
        assert connection.scalar(sa.text("SELECT COUNT(*) FROM native_account_read_attempts")) == 2
        assert connection.scalar(sa.text("SELECT COUNT(*) FROM native_account_read_outcomes")) == 1


@pytest.mark.parametrize("failed", (False, True))
def test_independent_responses_or_multiple_failed_nulls_do_not_collapse(
    database: sa.Engine, failed: bool
) -> None:
    """Equal payload material does not collapse independent actual requests or absent responses.

    Given: Two distinct parent attempts with equal physical payload material,
    When: Their outcomes reference distinct raw UUIDs or both explicitly failed without raw data,
    Then: Both outcomes remain rather than deduplicating by bytes or treating NULL as one response.
    """
    with database.begin() as connection:
        first = {
            **_values(_OUTCOMES),
            "result_kind": "failed" if failed else "captured",
            "raw_receipt_public_id": None if failed else _UUID,
        }
        second = {
            **first,
            "public_id": _OTHER_UUID,
            "attempt_public_id": _OTHER_UUID,
            "family_sequence": 2,
            "raw_receipt_public_id": None if failed else _OTHER_UUID,
        }
        _insert_valid(connection, _OUTCOMES, first)
        _insert_valid(connection, _OUTCOMES, second)
        assert connection.scalar(sa.text("SELECT COUNT(*) FROM native_account_read_outcomes")) == 2


@pytest.mark.parametrize(
    ("table", "changes"),
    (
        (_ATTEMPTS, {"client_selection_digest": "short"}),
        (_ATTEMPTS, {"started_at": "2026-10-23 00:00:01.000000"}),
        (_OUTCOMES, {"attempt_content_digest": "short"}),
        (_OUTCOMES, {"completed_at": "2026-10-23 00:00:01.000000"}),
        (_OUTCOMES, {"result_kind": "assumed_success"}),
        (_OUTCOMES, {"raw_receipt_public_id": None}),
    )
    + tuple(
        (table, changes)
        for table in NATIVE_ACCOUNT_READ_TABLES
        for changes in (
            {"scope_digest": "short"},
            {"family_sequence": 0},
            {"family_sequence": -1},
            {"margin_account": "futures.flex"},
            {"content_digest": "short"},
            {"canonical_payload": " "},
        )
    ),
)
def test_unproven_read_metadata_cannot_enter_either_history(
    database: sa.Engine, table: str, changes: dict[str, _Value]
) -> None:
    """Read lifecycle storage requires honest source dimensions, chronology and outcome claims.

    Given: An actual valid parent when the scenario concerns an outcome,
    When: Required digests, ordering, coarse scope or result/source clocks are inconsistent,
    Then: Physical checks refuse the malformed request or response row.
    """
    with database.begin() as connection:
        if table == _OUTCOMES:
            _insert(connection, _ATTEMPTS, _values(_ATTEMPTS))
            _retain_raw(connection, _OTHER_UUID)
        with pytest.raises(IntegrityError):
            _insert(connection, table, {**_values(table), **changes})
        assert connection.scalar(sa.text(f"SELECT COUNT(*) FROM {table}")) == 0


def test_orphaned_restore_still_prevents_lossy_downgrade(database: sa.Engine) -> None:
    """Even an externally damaged foreign-key history cannot be erased as empty.

    Given: An outcome restored while foreign-key enforcement was explicitly disabled,
    When: Foreign keys are re-enabled and the real downgrade evaluates both tables,
    Then: Its second-table check refuses the orphaned outcome and preserves both physical tables.
    """
    with database.connect() as connection:
        connection.exec_driver_sql("PRAGMA foreign_keys=OFF")
        connection.commit()
        _insert(connection, _OUTCOMES, _values(_OUTCOMES))
        connection.commit()
        connection.exec_driver_sql("PRAGMA foreign_keys=ON")
        connection.commit()
        assert connection.scalar(sa.text("PRAGMA foreign_keys")) == 1
        assert connection.scalar(sa.text("SELECT COUNT(*) FROM native_account_read_attempts")) == 0
        with pytest.raises(RuntimeError, match="prevents a lossless downgrade"):
            _invoke(connection, "downgrade")
        assert connection.scalar(sa.text("SELECT COUNT(*) FROM native_account_read_outcomes")) == 1
        assert all(sa.inspect(connection).has_table(name) for name in NATIVE_ACCOUNT_READ_TABLES)
        connection.rollback()


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
    engine = sa.create_engine(f"sqlite:///{tmp_path / '0061.db'}", connect_args={"timeout": 0})
    try:
        with engine.begin() as connection:
            connection.exec_driver_sql("PRAGMA recursive_triggers=ON")
            connection.exec_driver_sql("PRAGMA foreign_keys=ON")
            connection.exec_driver_sql("CREATE TABLE caller_work (id INTEGER PRIMARY KEY)")
            with patch.object(
                _RAW_MIGRATION, "op", Operations(MigrationContext.configure(connection))
            ):
                _RAW_MIGRATION.upgrade()
            _invoke(connection, "upgrade")
        yield engine
    finally:
        engine.dispose()


def test_empty_roundtrip_creates_no_financial_backfill(database: sa.Engine) -> None:
    """An actual empty migration never invents historical balance or execution prefixes.

    Given: Both read-history families were created by the real upgrade,
    When: Empty downgrade and upgrade execute on the same caller connection,
    Then: Tables disappear and return empty with both intended indexes.
    """
    with database.begin() as connection:
        assert all(
            connection.scalar(sa.text(f"SELECT COUNT(*) FROM {table}")) == 0
            for table in NATIVE_ACCOUNT_READ_TABLES
        )
        _invoke(connection, "downgrade")
        assert all(
            not sa.inspect(connection).has_table(table) for table in NATIVE_ACCOUNT_READ_TABLES
        )
        _invoke(connection, "upgrade")
        assert all(
            connection.scalar(sa.text(f"SELECT COUNT(*) FROM {table}")) == 0
            for table in NATIVE_ACCOUNT_READ_TABLES
        )
        indexes = [
            index
            for table in NATIVE_ACCOUNT_READ_TABLES
            for index in sa.inspect(connection).get_indexes(table)
        ]
        assert len(indexes) == 2


@pytest.mark.parametrize("table", NATIVE_ACCOUNT_READ_TABLES)
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


@pytest.mark.parametrize("table", NATIVE_ACCOUNT_READ_TABLES)
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
        with pytest.raises(IntegrityError):
            _insert_valid(connection, table, {**_values(table), **changes})
        assert connection.scalar(sa.text(f"SELECT COUNT(*) FROM {table}")) == 0


@pytest.mark.parametrize("table", NATIVE_ACCOUNT_READ_TABLES)
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


@pytest.mark.parametrize("table", NATIVE_ACCOUNT_READ_TABLES)
@pytest.mark.parametrize("margin_account", ("", "   "))
def test_blank_account_labels_cannot_become_physical_evidence(
    database: sa.Engine, table: str, margin_account: str
) -> None:
    """Empty account identity cannot become a financial publication namespace.

    Given: A physical source row with an explicitly supplied account label,
    When: That label is empty or whitespace only,
    Then: Its scope constraint refuses the evidence.
    """
    with database.begin() as connection:
        with pytest.raises(IntegrityError):
            _insert_valid(connection, table, {**_values(table), "margin_account": margin_account})
        assert connection.scalar(sa.text(f"SELECT COUNT(*) FROM {table}")) == 0


@pytest.mark.parametrize("table", NATIVE_ACCOUNT_READ_TABLES)
def test_any_retained_evidence_refuses_downgrade_before_guard_removal(
    database: sa.Engine, table: str
) -> None:
    """Even a successful read outcome prevents lossy removal of read history.

    Given: One physical row in exactly one of the two publication families,
    When: The real downgrade evaluates evidence under its write reservation,
    Then: All tables and their mutation guards remain and completion stays with the caller.
    """
    with database.begin() as connection:
        _insert_valid(connection, table, _values(table))
        with pytest.raises(RuntimeError, match="prevents a lossless downgrade"):
            _invoke(connection, "downgrade")
        assert connection.in_transaction()
        assert all(sa.inspect(connection).has_table(name) for name in NATIVE_ACCOUNT_READ_TABLES)
        with pytest.raises(IntegrityError, match="append-only"):
            connection.execute(sa.text(f"DELETE FROM {table}"))
        assert connection.scalar(sa.text(f"SELECT COUNT(*) FROM {table}")) == 1


@pytest.mark.parametrize("completion", ("commit", "rollback"))
@pytest.mark.parametrize("table", NATIVE_ACCOUNT_READ_TABLES)
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
                for name in NATIVE_ACCOUNT_READ_TABLES
            )
        finally:
            if transaction.is_active:
                transaction.rollback()


@pytest.mark.parametrize("table", NATIVE_ACCOUNT_READ_TABLES)
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
        assert all(sa.inspect(first).has_table(name) for name in NATIVE_ACCOUNT_READ_TABLES)
        first.rollback()


@pytest.mark.parametrize("table", NATIVE_ACCOUNT_READ_TABLES)
def test_postgresql_constraint_names_are_unique_within_each_read_table(table: str) -> None:
    """Different physical checks cannot share a PostgreSQL constraint identity.

    Given: Actual model declarations with backend-conditional sentinel checks,
    When: Every named constraint emitted to PostgreSQL is grouped within either table,
    Then: Every name is unique so PostgreSQL can retain every declared check.
    """
    tables = {
        model.__table__.name: model.__table__
        for model in (NativeAccountReadAttempt, NativeAccountReadOutcome)
    }
    ddl = str(CreateTable(tables[table]).compile(dialect=postgres_dialect()))
    names = [line.split()[1] for line in ddl.splitlines() if line.strip().startswith("CONSTRAINT ")]
    assert len(names) == len(set(names))


def test_postgresql_rendered_models_and_migration_match_without_claiming_live_evidence() -> None:
    """The model and migration declare identical PostgreSQL types, constraints and indexes.

    Given: Captured actual migration operations and both ORM table contracts,
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
    models = {
        model.__table__.name: model.__table__
        for model in (
            NativeAccountReadAttempt,
            NativeAccountReadOutcome,
        )
    }
    metadata = sa.MetaData()
    AccountingRawReceipt.__table__.to_metadata(metadata)
    captured = [
        sa.Table(call.args[0], metadata, *call.args[1:])
        for call in operations.create_table.call_args_list
    ]
    for table in captured:
        assert _postgresql_contract(table) == _postgresql_contract(models[table.name])
    assert {
        call.args[0]: tuple(call.args[2]) for call in operations.create_index.call_args_list
    } == {
        index.name: tuple(column.name for column in index.columns)
        for table in models.values()
        for index in table.indexes
    }
    statements = [str(call.args[0]) for call in connection.execute.call_args_list]
    lock = "LOCK TABLE " + ", ".join(NATIVE_ACCOUNT_READ_TABLES) + " IN ACCESS EXCLUSIVE MODE"
    assert lock in statements
    assert sum("ENABLE ALWAYS TRIGGER" in statement for statement in statements) == 4
    assert statements[-1] == "DROP FUNCTION IF EXISTS native_account_read_reject_mutation()"
    assert [call.args[0] for call in operations.drop_table.call_args_list] == list(
        reversed(NATIVE_ACCOUNT_READ_TABLES)
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

    Given: Empty real accounting evidence tables and a SQLite autocommit driver,
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
                sa.inspect(connection).has_table(table) for table in NATIVE_ACCOUNT_READ_TABLES
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
    table = "unknown_table" if profile == "unknown_table" else _ATTEMPTS
    function = (
        install_native_account_read_triggers
        if operation == "install"
        else drop_native_account_read_triggers
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
