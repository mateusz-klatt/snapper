"""Physical native source identity ownership and real SQLite downgrade fences."""

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

from snapper.data.accounting_native_source_triggers import ACCOUNTING_NATIVE_SOURCE_TABLE
from snapper.data.accounting_native_source_triggers import drop_accounting_native_source_triggers
from snapper.data.accounting_native_source_triggers import install_accounting_native_source_triggers
from snapper.data.models import AccountingNativeSourceClaim

_MIGRATION = import_module("snapper.data.migrations.versions.0060_accounting_native_source_claims")
_UUID = "00000000-0000-7000-8000-000000000001"
_OTHER_UUID = "00000000-0000-7000-8000-000000000002"
_TIMESTAMP = "2026-10-23 00:00:00.000000"
_TABLES = (ACCOUNTING_NATIVE_SOURCE_TABLE,)
type _Value = str | int | None


def _values(table: str) -> dict[str, _Value]:
    """Provide actual physical claim columns without claiming application source verification."""
    assert table == ACCOUNTING_NATIVE_SOURCE_TABLE
    return {
        "public_id": _UUID,
        "session_id": _UUID,
        "sequence_id": 1,
        "timestamp": _TIMESTAMP,
        "known_to": "9999-12-31 23:59:59.000000",
        "wallet_public_id": _UUID,
        "exchange": "kraken_futures",
        "mode": "live",
        "environment": "production",
        "margin_account": "futures.flex",
        "scope_digest": "a" * 64,
        "account_scope_digest": "b" * 64,
        "source_domain": "kraken_futures.account_log",
        "source_log_id": "17",
        "source_identity_digest": "c" * 64,
        "semantic_digest": "d" * 64,
        "accounting_event_public_id": _UUID,
        "raw_receipt_public_id": _UUID,
        "route_authority_public_id": _UUID,
        "kind": "contract_position_fill",
        "booking_uid": "booking-paired-position-and-cash",
        "content_digest": "e" * 64,
        "canonical_payload": "{}",
    }


def _insert(connection: sa.Connection, table: str, values: dict[str, _Value]) -> None:
    """Insert a statically selected evidence shape through the real physical schema."""
    assert table in _TABLES
    columns = ", ".join(values)
    parameters = ", ".join(f":{column}" for column in values)
    connection.execute(sa.text(f"INSERT INTO {table} ({columns}) VALUES ({parameters})"), values)


def _next_claim(**changes: _Value) -> dict[str, _Value]:
    """Allocate another physical event and receipt while retaining the same native identity.

    Args:
        **changes: Explicit indexed fields replaced by the physical boundary scenario.

    Returns:
        Independent local identities with the original selected native log unless overridden.
    """
    return {
        **_values(ACCOUNTING_NATIVE_SOURCE_TABLE),
        "public_id": _OTHER_UUID,
        "accounting_event_public_id": _OTHER_UUID,
        "raw_receipt_public_id": _OTHER_UUID,
        **changes,
    }


@pytest.mark.parametrize("identity", ("public_id", "accounting_event_public_id"))
def test_one_local_event_or_claim_cannot_acquire_two_native_owners(
    database: sa.Engine, identity: str
) -> None:
    """Each retained claim and event has one native source owner.

    Given: A retained owner.
    When: Another native ID reuses its claim/event.
    Then: Uniqueness refuses.
    """
    first = _values(ACCOUNTING_NATIVE_SOURCE_TABLE)
    second = _next_claim(source_log_id="18", source_identity_digest="f" * 64)
    second[identity] = first[identity]
    with database.begin() as connection:
        _insert(connection, ACCOUNTING_NATIVE_SOURCE_TABLE, first)
        with pytest.raises(IntegrityError):
            _insert(connection, ACCOUNTING_NATIVE_SOURCE_TABLE, second)
        assert (
            connection.scalar(sa.text(f"SELECT COUNT(*) FROM {ACCOUNTING_NATIVE_SOURCE_TABLE}"))
            == 1
        )


@pytest.mark.parametrize(
    "changes",
    (
        {"kind": "native_cash"},
        {"margin_account": "second-profile", "account_scope_digest": "f" * 64},
        {
            "kind": "native_cash",
            "margin_account": "second-profile",
            "scope_digest": "f" * 64,
            "account_scope_digest": "0" * 64,
            "source_identity_digest": "1" * 64,
            "semantic_digest": "2" * 64,
        },
    ),
)
def test_native_id_cannot_fork_through_kind_margin_or_changed_indexed_digests(
    database: sa.Engine, changes: dict[str, _Value]
) -> None:
    """A contradictory accounting projection cannot create another native source namespace.

    Given: A claimed native log in one actual wallet, venue, mode and environment,
    When: Another claim changes kind, margin or even its indexed family/identity digests,
    Then: Physical uniqueness refuses the same actual native log and retains its first owner.
    """
    with database.begin() as connection:
        _insert(connection, ACCOUNTING_NATIVE_SOURCE_TABLE, _values(ACCOUNTING_NATIVE_SOURCE_TABLE))
        with pytest.raises(IntegrityError):
            _insert(connection, ACCOUNTING_NATIVE_SOURCE_TABLE, _next_claim(**changes))
        assert (
            connection.scalar(sa.text(f"SELECT kind FROM {ACCOUNTING_NATIVE_SOURCE_TABLE}"))
            == "contract_position_fill"
        )
        assert (
            connection.scalar(sa.text(f"SELECT COUNT(*) FROM {ACCOUNTING_NATIVE_SOURCE_TABLE}"))
            == 1
        )


def test_identity_digest_cannot_be_rebound_to_a_different_native_text_id(
    database: sa.Engine,
) -> None:
    """An existing source identity digest cannot be assigned to different native text.

    Given: A retained identity digest.
    When: Another log text reuses it.
    Then: The family key refuses.
    """
    with database.begin() as connection:
        _insert(connection, ACCOUNTING_NATIVE_SOURCE_TABLE, _values(ACCOUNTING_NATIVE_SOURCE_TABLE))
        with pytest.raises(IntegrityError):
            _insert(connection, ACCOUNTING_NATIVE_SOURCE_TABLE, _next_claim(source_log_id="18"))
        assert (
            connection.scalar(
                sa.text(f"SELECT source_log_id FROM {ACCOUNTING_NATIVE_SOURCE_TABLE}")
            )
            == "17"
        )


def test_one_booking_can_retain_distinct_cash_and_position_native_entries(
    database: sa.Engine,
) -> None:
    """Distinct native cash and position entries may share one booking group.

    Given: Paired booking entries.
    When: Their native IDs differ.
    Then: Each retains its own owner.
    """
    with database.begin() as connection:
        _insert(connection, ACCOUNTING_NATIVE_SOURCE_TABLE, _values(ACCOUNTING_NATIVE_SOURCE_TABLE))
        _insert(
            connection,
            ACCOUNTING_NATIVE_SOURCE_TABLE,
            _next_claim(source_log_id="18", source_identity_digest="f" * 64, kind="native_cash"),
        )
        assert (
            connection.scalar(sa.text(f"SELECT COUNT(*) FROM {ACCOUNTING_NATIVE_SOURCE_TABLE}"))
            == 2
        )
        assert (
            connection.scalar(
                sa.text(f"SELECT COUNT(DISTINCT booking_uid) FROM {ACCOUNTING_NATIVE_SOURCE_TABLE}")
            )
            == 1
        )


@pytest.mark.parametrize(
    "changes",
    (
        {"wallet_public_id": _OTHER_UUID, "scope_digest": "f" * 64},
        {"environment": "demo", "scope_digest": "f" * 64},
    ),
)
def test_independent_local_account_families_can_retain_the_same_native_id(
    database: sa.Engine, changes: dict[str, _Value]
) -> None:
    """Separate local account families retain independent native ID namespaces.

    Given: Independent local families.
    When: Their native IDs match.
    Then: Physical scopes stay separate.
    """
    with database.begin() as connection:
        _insert(connection, ACCOUNTING_NATIVE_SOURCE_TABLE, _values(ACCOUNTING_NATIVE_SOURCE_TABLE))
        _insert(connection, ACCOUNTING_NATIVE_SOURCE_TABLE, _next_claim(**changes))
        assert (
            connection.scalar(sa.text(f"SELECT COUNT(*) FROM {ACCOUNTING_NATIVE_SOURCE_TABLE}"))
            == 2
        )


@pytest.mark.parametrize("source_log_id", ("9223372036854775808", "9" * 100))
def test_native_positive_integer_ids_have_no_invented_signed_64_bit_bound(
    database: sa.Engine, source_log_id: str
) -> None:
    """Canonical positive native IDs preserve digits beyond signed 64-bit.

    Given: A canonical large source ID.
    When: Retained as text.
    Then: Storage preserves every digit.
    """
    with database.begin() as connection:
        _insert(
            connection,
            ACCOUNTING_NATIVE_SOURCE_TABLE,
            {**_values(ACCOUNTING_NATIVE_SOURCE_TABLE), "source_log_id": source_log_id},
        )
        assert (
            connection.scalar(
                sa.text(f"SELECT source_log_id FROM {ACCOUNTING_NATIVE_SOURCE_TABLE}")
            )
            == source_log_id
        )


@pytest.mark.parametrize(
    "source_log_id", ("", "0", "01", "-1", "+1", "1.0", "1e2", " 1", "1 ", "1\n", "1\x00extra", "١")
)
def test_native_id_aliases_cannot_create_additional_identity_namespaces(
    database: sa.Engine, source_log_id: str
) -> None:
    """Noncanonical numeric spellings cannot create new native source identities.

    Given: A noncanonical ID alias.
    When: Raw SQL inserts it.
    Then: The physical boundary refuses.
    """
    with database.begin() as connection:
        with pytest.raises(IntegrityError):
            _insert(
                connection,
                ACCOUNTING_NATIVE_SOURCE_TABLE,
                {**_values(ACCOUNTING_NATIVE_SOURCE_TABLE), "source_log_id": source_log_id},
            )
        assert (
            connection.scalar(sa.text(f"SELECT COUNT(*) FROM {ACCOUNTING_NATIVE_SOURCE_TABLE}"))
            == 0
        )


@pytest.mark.parametrize(
    "changes",
    (
        {"scope_digest": "short"},
        {"account_scope_digest": "short"},
        {"source_identity_digest": "short"},
        {"semantic_digest": "short"},
        {"content_digest": "short"},
        {"canonical_payload": " "},
        {"booking_uid": " "},
        {"source_domain": "kraken_futures.cash"},
        {"kind": "guessed_balance"},
        {"margin_account": None},
        {"source_log_id": None},
        {"raw_receipt_public_id": None},
        {"accounting_event_public_id": None},
        {"route_authority_public_id": None},
    ),
)
def test_incomplete_or_invented_native_ownership_metadata_is_physically_refused(
    database: sa.Engine, changes: dict[str, _Value]
) -> None:
    """Claims require actual source references and valid ownership metadata.

    Given: Missing source or actual ownership metadata.
    When: Inserted.
    Then: Storage retains no claim.
    """
    with database.begin() as connection:
        with pytest.raises(IntegrityError):
            _insert(
                connection,
                ACCOUNTING_NATIVE_SOURCE_TABLE,
                {**_values(ACCOUNTING_NATIVE_SOURCE_TABLE), **changes},
            )
        assert (
            connection.scalar(sa.text(f"SELECT COUNT(*) FROM {ACCOUNTING_NATIVE_SOURCE_TABLE}"))
            == 0
        )


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
    engine = sa.create_engine(f"sqlite:///{tmp_path / '0060.db'}", connect_args={"timeout": 0})
    try:
        with engine.begin() as connection:
            connection.exec_driver_sql("PRAGMA recursive_triggers=ON")
            connection.exec_driver_sql("CREATE TABLE caller_work (id INTEGER PRIMARY KEY)")
            _invoke(connection, "upgrade")
        yield engine
    finally:
        engine.dispose()


def test_empty_roundtrip_creates_no_financial_backfill(database: sa.Engine) -> None:
    """An actual empty migration never invents historical balance or execution prefixes.

    Given: The native claim table were created by the real upgrade,
    When: Empty downgrade and upgrade execute on the same caller connection,
    Then: Tables disappear and return empty with all one intended index.
    """
    with database.begin() as connection:
        assert all(
            connection.scalar(sa.text(f"SELECT COUNT(*) FROM {table}")) == 0 for table in _TABLES
        )
        _invoke(connection, "downgrade")
        assert all(not sa.inspect(connection).has_table(table) for table in _TABLES)
        _invoke(connection, "upgrade")
        assert all(
            connection.scalar(sa.text(f"SELECT COUNT(*) FROM {table}")) == 0 for table in _TABLES
        )
        indexes = [
            index for table in _TABLES for index in sa.inspect(connection).get_indexes(table)
        ]
        assert len(indexes) == 1


@pytest.mark.parametrize("table", _TABLES)
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
        _insert(connection, table, values)
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


@pytest.mark.parametrize("table", _TABLES)
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
            _insert(connection, table, {**_values(table), **changes})
        assert connection.scalar(sa.text(f"SELECT COUNT(*) FROM {table}")) == 0


@pytest.mark.parametrize("table", _TABLES)
def test_same_public_identity_cannot_replace_retained_evidence(
    database: sa.Engine, table: str
) -> None:
    """Public immutable evidence identity is independently unique in every family.

    Given: One retained row,
    When: A second insert reuses its public identity,
    Then: The unique constraint keeps the original single row.
    """
    with database.begin() as connection:
        _insert(connection, table, _values(table))
        with pytest.raises(IntegrityError):
            _insert(connection, table, _values(table))
        assert connection.scalar(sa.text(f"SELECT COUNT(*) FROM {table}")) == 1


@pytest.mark.parametrize("table", _TABLES)
@pytest.mark.parametrize("margin_account", ("", "   "))
def test_blank_account_labels_cannot_become_physical_evidence(
    database: sa.Engine, table: str, margin_account: str
) -> None:
    """Empty account identity cannot become a native source ownership namespace.

    Given: A physical source row with an explicitly supplied account label,
    When: That label is empty or whitespace only,
    Then: Its scope constraint refuses the evidence.
    """
    with database.begin() as connection:
        with pytest.raises(IntegrityError):
            _insert(connection, table, {**_values(table), "margin_account": margin_account})
        assert connection.scalar(sa.text(f"SELECT COUNT(*) FROM {table}")) == 0


@pytest.mark.parametrize("table", _TABLES)
def test_any_retained_evidence_refuses_downgrade_before_guard_removal(
    database: sa.Engine, table: str
) -> None:
    """Even a retained source claim prevents lossy removal of native source ownership history.

    Given: One physical row in exactly the native claim table,
    When: The real downgrade evaluates evidence under its write reservation,
    Then: All tables and their mutation guards remain and completion stays with the caller.
    """
    with database.begin() as connection:
        _insert(connection, table, _values(table))
        with pytest.raises(RuntimeError, match="prevents a lossless downgrade"):
            _invoke(connection, "downgrade")
        assert connection.in_transaction()
        assert all(sa.inspect(connection).has_table(name) for name in _TABLES)
        with pytest.raises(IntegrityError, match="append-only"):
            connection.execute(sa.text(f"DELETE FROM {table}"))
        assert connection.scalar(sa.text(f"SELECT COUNT(*) FROM {table}")) == 1


@pytest.mark.parametrize("completion", ("commit", "rollback"))
@pytest.mark.parametrize("table", _TABLES)
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
                sa.inspect(writer).has_table(name) == (completion == "rollback") for name in _TABLES
            )
        finally:
            if transaction.is_active:
                transaction.rollback()


@pytest.mark.parametrize("table", _TABLES)
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
        _insert(writer, table, _values(table))
        with pytest.raises(OperationalError, match="database is locked"):
            _invoke(first, "downgrade")
        writer.commit()
        with pytest.raises(RuntimeError, match="prevents a lossless downgrade"):
            _invoke(first, "downgrade")
        assert first.in_transaction()
        assert first.scalar(sa.text(f"SELECT COUNT(*) FROM {table}")) == 1
        assert all(sa.inspect(first).has_table(name) for name in _TABLES)
        first.rollback()


def test_postgresql_rendered_models_and_migration_match_without_claiming_live_evidence() -> None:
    """The model and migration declare identical PostgreSQL types, constraints and indexes.

    Given: Captured actual migration operations and the ORM table contract,
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
    models = {model.__table__.name: model.__table__ for model in (AccountingNativeSourceClaim,)}
    for call in operations.create_table.call_args_list:
        table = sa.Table(call.args[0], sa.MetaData(), *call.args[1:])
        assert _postgresql_contract(table) == _postgresql_contract(models[table.name])
    assert {
        call.args[0]: tuple(call.args[2]) for call in operations.create_index.call_args_list
    } == {
        index.name: tuple(column.name for column in index.columns)
        for table in models.values()
        for index in table.indexes
    }
    statements = [str(call.args[0]) for call in connection.execute.call_args_list]
    lock = "LOCK TABLE " + ", ".join(_TABLES) + " IN ACCESS EXCLUSIVE MODE"
    assert lock in statements
    assert sum("ENABLE ALWAYS TRIGGER" in statement for statement in statements) == 2
    assert statements[-1] == "DROP FUNCTION IF EXISTS accounting_native_source_reject_mutation()"
    assert [call.args[0] for call in operations.drop_table.call_args_list] == list(
        reversed(_TABLES)
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
            assert all(sa.inspect(connection).has_table(table) for table in _TABLES)
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
    table = "unknown_table" if profile == "unknown_table" else ACCOUNTING_NATIVE_SOURCE_TABLE
    function = (
        install_accounting_native_source_triggers
        if operation == "install"
        else drop_accounting_native_source_triggers
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
