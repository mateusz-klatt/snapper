"""Physical redacted identity, original-owner guards and caller-owned downgrade fences."""

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

from snapper.data.models import NativeAccountIdentityProof
from snapper.data.models import NativeAccountReadAttempt
from snapper.data.native_account_identity_triggers import NATIVE_ACCOUNT_IDENTITY_TABLES
from snapper.data.native_account_identity_triggers import drop_native_account_identity_triggers
from snapper.data.native_account_identity_triggers import install_native_account_identity_triggers

_MIGRATION = import_module("snapper.data.migrations.versions.0062_native_account_identity_proofs")
_READ_MIGRATION = import_module(
    "snapper.data.migrations.versions.0061_native_account_read_lifecycle"
)
_RAW_MIGRATION = import_module("snapper.data.migrations.versions.0058_contract_accounting_evidence")
_UUID = "00000000-0000-7000-8000-000000000001"
_OTHER_UUID = "00000000-0000-7000-8000-000000000004"
_TIMESTAMP = "2026-10-23 00:00:00.000000"
_PROOFS = "native_account_identity_proofs"
type _Value = str | int | None


def _values(table: str = _PROOFS) -> dict[str, _Value]:
    """Provide physical public-proof values without claiming an actual authenticated key check."""
    assert table == _PROOFS
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
        "margin_account": None,
        "scope_digest": "a" * 64,
        "family_sequence": 1,
        "client_selection_digest": "c" * 64,
        "attempt_public_id": "00000000-0000-7000-8000-000000000002",
        "attempt_content_digest": "b" * 64,
        "owner_root_public_id": None,
        "owner_root_content_digest": None,
        "owner_root_account_uid": None,
        "source_policy": "kraken_futures_key_check_redacted_v1",
        "account_uid": "00000000-0000-7000-8000-000000000003",
        "general_access": "READ_ONLY",
        "transfer_access": "NO_ACCESS",
        "request_started_at": _TIMESTAMP,
        "observed_at": _TIMESTAMP,
        "content_digest": "b" * 64,
        "canonical_payload": "{}",
    }


def _retain_attempt(connection: sa.Connection, values: dict[str, _Value]) -> None:
    """Retain a real 0061 parent row while leaving authenticated source semantics to application tests."""
    if connection.scalar(
        sa.text("SELECT COUNT(*) FROM native_account_read_attempts WHERE public_id=:id"),
        {"id": values["attempt_public_id"]},
    ):
        return
    parent = {
        "public_id": values["attempt_public_id"],
        "session_id": _UUID,
        "sequence_id": 1,
        "timestamp": _TIMESTAMP,
        "known_to": "9999-12-31 23:59:59.000000",
        "wallet_public_id": values["wallet_public_id"],
        "exchange": "kraken_futures",
        "mode": "live",
        "environment": values["environment"],
        "margin_account": None,
        "scope_digest": values["scope_digest"],
        "family_sequence": values["family_sequence"],
        "client_selection_digest": values["client_selection_digest"],
        "started_at": _TIMESTAMP,
        "content_digest": "b" * 64,
        "canonical_payload": "{}",
    }
    columns = ", ".join(parent)
    params = ", ".join(f":{name}" for name in parent)
    connection.execute(
        sa.text(f"INSERT INTO native_account_read_attempts ({columns}) VALUES ({params})"), parent
    )


def _insert_valid(connection: sa.Connection, table: str, values: dict[str, _Value]) -> None:
    """Prepare the actual required attempt before exercising a positive physical proof row."""
    _retain_attempt(connection, values)
    _insert(connection, table, values)


def _rotation(root: dict[str, _Value]) -> dict[str, _Value]:
    """Use another actual attempt while preserving the original owner's full reference tuple."""
    return {
        **root,
        "public_id": _OTHER_UUID,
        "family_sequence": 2,
        "attempt_public_id": "00000000-0000-7000-8000-000000000005",
        "client_selection_digest": "d" * 64,
        "owner_root_public_id": root["public_id"],
        "owner_root_content_digest": root["content_digest"],
        "owner_root_account_uid": root["account_uid"],
    }


@pytest.mark.parametrize(
    "changes",
    (
        {"scope_digest": "d" * 64},
        {"scope_digest": "d" * 64, "client_selection_digest": "e" * 64},
        {"scope_digest": "d" * 64, "account_uid": _OTHER_UUID},
    ),
)
def test_direct_initial_owner_cannot_fork_on_digest_credential_or_observed_uid(
    database: sa.Engine, changes: dict[str, _Value]
) -> None:
    """An actual account family keeps one initial owner despite altered copied metadata.

    Given: A retained initial proof and a separately allocated actual attempt,
    When: Another initial UUID repeats the direct family with changed digest, credential or UID,
    Then: The direct partial unique key preserves the first owner and both actual attempts.
    """
    with database.begin() as connection:
        first = _values()
        _insert_valid(connection, _PROOFS, first)
        candidate = {
            **first,
            "public_id": _OTHER_UUID,
            "family_sequence": 2,
            "attempt_public_id": "00000000-0000-7000-8000-000000000005",
            **changes,
        }
        _retain_attempt(connection, candidate)
        with pytest.raises(IntegrityError):
            _insert(connection, _PROOFS, candidate)
        assert (
            connection.scalar(sa.text("SELECT COUNT(*) FROM native_account_identity_proofs")) == 1
        )
        assert connection.scalar(sa.text("SELECT COUNT(*) FROM native_account_read_attempts")) == 2


def test_coarse_initial_owner_key_cannot_fork_by_indexed_wallet(database: sa.Engine) -> None:
    """The family digest independently protects its original root against copied-index drift.

    Given: One actual initial proof and another valid attempt for a different indexed wallet,
    When: Another initial proof copies the original digest despite changing the wallet,
    Then: The digest partial unique key refuses the contradictory root.
    """
    with database.begin() as connection:
        first = _values()
        _insert_valid(connection, _PROOFS, first)
        other = {
            **first,
            "public_id": _OTHER_UUID,
            "wallet_public_id": _OTHER_UUID,
            "family_sequence": 2,
            "attempt_public_id": "00000000-0000-7000-8000-000000000005",
        }
        _retain_attempt(connection, other)
        with pytest.raises(IntegrityError):
            _insert(connection, _PROOFS, other)
        assert (
            connection.scalar(sa.text("SELECT COUNT(*) FROM native_account_identity_proofs")) == 1
        )


@pytest.mark.parametrize("owner_conflict", (False, True))
def test_rotation_or_conflicting_uid_retains_original_owner_tuple(
    database: sa.Engine, owner_conflict: bool
) -> None:
    """Later observations may report a conflict while the first owner remains immutable.

    Given: A retained initial owner and a different actual read attempt,
    When: A later proof records credential rotation or a contradictory observed account UID,
    Then: Both proofs remain and the original root and complete owner tuple stay unchanged.
    """
    with database.begin() as connection:
        root = _values()
        _insert_valid(connection, _PROOFS, root)
        child = _rotation(root)
        if owner_conflict:
            child["account_uid"] = _OTHER_UUID
        _insert_valid(connection, _PROOFS, child)
        assert (
            connection.scalar(sa.text("SELECT COUNT(*) FROM native_account_identity_proofs")) == 2
        )
        retained = connection.execute(
            sa.text(
                "SELECT account_uid, owner_root_public_id, owner_root_content_digest, "
                "owner_root_account_uid FROM native_account_identity_proofs WHERE public_id=:id"
            ),
            {"id": child["public_id"]},
        ).one()
        assert tuple(retained) == (
            child["account_uid"],
            root["public_id"],
            root["content_digest"],
            root["account_uid"],
        )
        assert (
            connection.scalar(
                sa.text(
                    "SELECT account_uid FROM native_account_identity_proofs WHERE owner_root_public_id IS NULL"
                )
            )
            == root["account_uid"]
        )


def test_read_only_roots_do_not_invent_global_external_account_ownership(
    database: sa.Engine,
) -> None:
    """Separate local read families may observe the same UID without claiming financial allocation.

    Given: One root and another actual wallet family with a different coarse digest,
    When: Both observe the same external account UID,
    Then: Both read-only roots coexist without a fabricated global ownership constraint.
    """
    with database.begin() as connection:
        first = _values()
        second = {
            **first,
            "public_id": _OTHER_UUID,
            "wallet_public_id": _OTHER_UUID,
            "scope_digest": "d" * 64,
            "attempt_public_id": "00000000-0000-7000-8000-000000000005",
        }
        _insert_valid(connection, _PROOFS, first)
        _insert_valid(connection, _PROOFS, second)
        assert (
            connection.scalar(sa.text("SELECT COUNT(*) FROM native_account_identity_proofs")) == 2
        )


@pytest.mark.parametrize("missing", ("attempt", "owner_root"))
def test_proof_cannot_invent_an_actual_attempt_or_original_root(
    database: sa.Engine, missing: str
) -> None:
    """Canonical-looking indexed UUIDs cannot substitute for actual retained parent evidence.

    Given: Real migrated tables with foreign keys enabled,
    When: A proof refers to an absent actual attempt or absent original root,
    Then: The foreign key refuses the proof and retains no invented identity history.
    """
    with database.begin() as connection:
        assert connection.scalar(sa.text("PRAGMA foreign_keys")) == 1
        values = _values() if missing == "attempt" else _rotation(_values())
        if missing == "owner_root":
            _retain_attempt(connection, values)
        with pytest.raises(IntegrityError, match="FOREIGN KEY"):
            _insert(connection, _PROOFS, values)
        assert (
            connection.scalar(sa.text("SELECT COUNT(*) FROM native_account_identity_proofs")) == 0
        )


def test_one_actual_attempt_cannot_receive_another_public_proof(database: sa.Engine) -> None:
    """A changed public UUID cannot create a second identity result for one actual request.

    Given: A retained root and another otherwise complete proof referencing that actual attempt,
    When: The candidate has a distinct public UUID and valid existing owner root,
    Then: Attempt uniqueness refuses the extra proof independently of initial-root uniqueness.
    """
    with database.begin() as connection:
        root = _values()
        _insert_valid(connection, _PROOFS, root)
        child = {**_rotation(root), "attempt_public_id": root["attempt_public_id"]}
        with pytest.raises(IntegrityError):
            _insert(connection, _PROOFS, child)
        assert (
            connection.scalar(sa.text("SELECT COUNT(*) FROM native_account_identity_proofs")) == 1
        )


def test_public_identity_cannot_be_reused_by_an_independent_initial_family(
    database: sa.Engine,
) -> None:
    """Public proof identity remains independently unique after other logical coordinates change.

    Given: A retained root and an independent actual attempt in a different family,
    When: The otherwise valid new initial family copies only the root's public proof UUID,
    Then: Public uniqueness refuses the candidate without conflating its attempt identity.
    """
    with database.begin() as connection:
        root = _values()
        _insert_valid(connection, _PROOFS, root)
        child = _rotation(root)
        child["public_id"] = root["public_id"]
        child["owner_root_public_id"] = None
        child["owner_root_content_digest"] = None
        child["owner_root_account_uid"] = None
        child["wallet_public_id"] = _OTHER_UUID
        child["scope_digest"] = "d" * 64
        child["family_sequence"] = 1
        _retain_attempt(connection, child)
        with pytest.raises(IntegrityError):
            _insert(connection, _PROOFS, child)
        assert (
            connection.scalar(sa.text("SELECT COUNT(*) FROM native_account_identity_proofs")) == 1
        )


@pytest.mark.parametrize(
    "presence",
    (
        (True, False, False),
        (False, True, False),
        (False, False, True),
        (True, True, False),
        (True, False, True),
        (False, True, True),
    ),
)
def test_partial_owner_root_reference_cannot_enter_history(
    database: sa.Engine, presence: tuple[bool, bool, bool]
) -> None:
    """An original root reference is either explicitly absent or retained as one complete tuple.

    Given: An actual root and another valid attempt with all non-root fields complete,
    When: Any proper nonempty subset of root UUID, digest and account UID is provided,
    Then: The tuple check rejects that incomplete ownership claim.
    """
    with database.begin() as connection:
        root = _values()
        _insert_valid(connection, _PROOFS, root)
        child = _rotation(root)
        _retain_attempt(connection, child)
        for field, present in zip(
            ("owner_root_public_id", "owner_root_content_digest", "owner_root_account_uid"),
            presence,
            strict=True,
        ):
            if not present:
                child[field] = None
        with pytest.raises(IntegrityError, match="ck_native_identity_root_tuple"):
            _insert(connection, _PROOFS, child)
        assert (
            connection.scalar(sa.text("SELECT COUNT(*) FROM native_account_identity_proofs")) == 1
        )


def test_proof_cannot_name_itself_as_its_original_owner(database: sa.Engine) -> None:
    """A self-referencing child cannot manufacture the absence of an actual initial owner.

    Given: A real actual attempt and no existing proof,
    When: A proof supplies a complete owner tuple whose UUID is its own new identity,
    Then: The self-root check refuses it before a cyclic owner can be retained.
    """
    with database.begin() as connection:
        values = _values()
        _retain_attempt(connection, values)
        values.update(
            owner_root_public_id=values["public_id"],
            owner_root_content_digest=values["content_digest"],
            owner_root_account_uid=values["account_uid"],
        )
        with pytest.raises(IntegrityError, match="ck_native_identity_root_not_self"):
            _insert(connection, _PROOFS, values)
        assert (
            connection.scalar(sa.text("SELECT COUNT(*) FROM native_account_identity_proofs")) == 0
        )


@pytest.mark.parametrize(
    "changes",
    (
        {"scope_digest": "short"},
        {"family_sequence": 0},
        {"family_sequence": -1},
        {"client_selection_digest": "short"},
        {"attempt_content_digest": "short"},
        {"content_digest": "short"},
        {"canonical_payload": " "},
        {"margin_account": "futures.flex"},
        {"exchange": "kraken"},
        {"source_policy": "kraken_futures_unredacted_key_response"},
        {"general_access": "read_only"},
        {"general_access": "TRADE_ONLY"},
        {"transfer_access": "WITHDRAW_ONLY"},
        {"transfer_access": ""},
        {"request_started_at": "2026-10-23 00:00:01.000000"},
        {"observed_at": "2026-10-23 00:00:01.000000"},
        {"timestamp": "2026-10-22 23:59:59.000000"},
    ),
)
def test_inconsistent_public_proof_material_is_physically_refused(
    database: sa.Engine, changes: dict[str, _Value]
) -> None:
    """Incorrect public source policy, permissions or knowledge cannot be retained as identity proof.

    Given: One valid actual attempt with foreign-key requirements already satisfied,
    When: A candidate changes required scope, digests, public enums or source clocks,
    Then: Physical checks reject that row while leaving identity history empty.
    """
    with database.begin() as connection:
        values = _values()
        _retain_attempt(connection, values)
        with pytest.raises(IntegrityError):
            _insert(connection, _PROOFS, {**values, **changes})
        assert (
            connection.scalar(sa.text("SELECT COUNT(*) FROM native_account_identity_proofs")) == 0
        )


def test_retained_root_digest_requires_complete_material(database: sa.Engine) -> None:
    """An existing root UUID cannot turn a truncated public digest into a complete root reference.

    Given: A real initial owner, another actual attempt and an otherwise complete root tuple,
    When: The child's original-root digest is too short,
    Then: Its digest check rejects the candidate while preserving the first proof.
    """
    with database.begin() as connection:
        root = _values()
        _insert_valid(connection, _PROOFS, root)
        child = _rotation(root)
        _retain_attempt(connection, child)
        with pytest.raises(IntegrityError, match="ck_native_identity_root_digest"):
            _insert(connection, _PROOFS, {**child, "owner_root_content_digest": "short"})
        assert (
            connection.scalar(sa.text("SELECT COUNT(*) FROM native_account_identity_proofs")) == 1
        )


@pytest.mark.parametrize("general_access", ("NO_ACCESS", "READ_ONLY", "FULL_ACCESS"))
@pytest.mark.parametrize("transfer_access", ("NO_ACCESS", "READ_ONLY", "FULL_ACCESS"))
def test_all_observed_permission_enums_remain_public_facts_only(
    database: sa.Engine, general_access: str, transfer_access: str
) -> None:
    """Observed permission values remain evidence rather than an implicit trading instruction.

    Given: An actual attempt and one otherwise valid initial public proof,
    When: Any documented pair of general and transfer permission enums is retained,
    Then: Both exact source values remain without manufacturing an order or collateral amount.
    """
    with database.begin() as connection:
        _insert_valid(
            connection,
            _PROOFS,
            {
                **_values(),
                "general_access": general_access,
                "transfer_access": transfer_access,
            },
        )
        assert tuple(
            connection.execute(
                sa.text(
                    "SELECT general_access, transfer_access FROM native_account_identity_proofs"
                )
            ).one()
        ) == (general_access, transfer_access)


def test_explicitly_orphaned_restore_still_prevents_lossy_downgrade(database: sa.Engine) -> None:
    """Externally damaged original-owner linkage cannot make retained proof history disposable.

    Given: A child restored only while foreign keys were explicitly disabled,
    When: Foreign keys resume and actual downgrade checks proof history under its write fence,
    Then: It refuses loss, preserves the proof table and keeps mutation guards active.
    """
    with database.connect() as connection:
        child = _rotation(_values())
        _retain_attempt(connection, child)
        connection.commit()
        connection.exec_driver_sql("PRAGMA foreign_keys=OFF")
        connection.commit()
        _insert(connection, _PROOFS, child)
        connection.commit()
        connection.exec_driver_sql("PRAGMA foreign_keys=ON")
        connection.commit()
        assert connection.scalar(sa.text("PRAGMA foreign_keys")) == 1
        with pytest.raises(RuntimeError, match="prevents a lossless downgrade"):
            _invoke(connection, "downgrade")
        assert (
            connection.scalar(sa.text("SELECT COUNT(*) FROM native_account_identity_proofs")) == 1
        )
        with pytest.raises(IntegrityError, match="append-only"):
            connection.exec_driver_sql("DELETE FROM native_account_identity_proofs")
        connection.rollback()


def _insert(connection: sa.Connection, table: str, values: dict[str, _Value]) -> None:
    """Insert a statically selected evidence shape through the real physical schema."""
    assert table in NATIVE_ACCOUNT_IDENTITY_TABLES
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
    engine = sa.create_engine(f"sqlite:///{tmp_path / '0062.db'}", connect_args={"timeout": 0})
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
            _invoke(connection, "upgrade")
        yield engine
    finally:
        engine.dispose()


def test_empty_roundtrip_creates_no_financial_backfill(database: sa.Engine) -> None:
    """An empty identity migration never invents an account owner or selected-key proof.

    Given: The identity-history families were created by the real upgrade,
    When: Empty downgrade and upgrade execute on the same caller connection,
    Then: The proof table disappears and returns empty with all intended indexes.
    """
    with database.begin() as connection:
        assert all(
            connection.scalar(sa.text(f"SELECT COUNT(*) FROM {table}")) == 0
            for table in NATIVE_ACCOUNT_IDENTITY_TABLES
        )
        _invoke(connection, "downgrade")
        assert all(
            not sa.inspect(connection).has_table(table) for table in NATIVE_ACCOUNT_IDENTITY_TABLES
        )
        _invoke(connection, "upgrade")
        assert all(
            connection.scalar(sa.text(f"SELECT COUNT(*) FROM {table}")) == 0
            for table in NATIVE_ACCOUNT_IDENTITY_TABLES
        )
        indexes = [
            index
            for table in NATIVE_ACCOUNT_IDENTITY_TABLES
            for index in sa.inspect(connection).get_indexes(table)
        ]
        assert len(indexes) == 3
        assert sum(bool(index["unique"]) for index in indexes) == 2


@pytest.mark.parametrize("table", NATIVE_ACCOUNT_IDENTITY_TABLES)
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


@pytest.mark.parametrize("table", NATIVE_ACCOUNT_IDENTITY_TABLES)
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
        _retain_attempt(connection, _values(table))
        with pytest.raises(IntegrityError):
            _insert(connection, table, {**_values(table), **changes})
        assert connection.scalar(sa.text(f"SELECT COUNT(*) FROM {table}")) == 0


@pytest.mark.parametrize("table", NATIVE_ACCOUNT_IDENTITY_TABLES)
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


@pytest.mark.parametrize("table", NATIVE_ACCOUNT_IDENTITY_TABLES)
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


@pytest.mark.parametrize("table", NATIVE_ACCOUNT_IDENTITY_TABLES)
def test_any_retained_evidence_refuses_downgrade_before_guard_removal(
    database: sa.Engine, table: str
) -> None:
    """Even a successful read outcome prevents lossy removal of read history.

    Given: One physical immutable proof and its actual attempt parent,
    When: The real downgrade evaluates evidence under its write reservation,
    Then: All tables and their mutation guards remain and completion stays with the caller.
    """
    with database.begin() as connection:
        _insert_valid(connection, table, _values(table))
        with pytest.raises(RuntimeError, match="prevents a lossless downgrade"):
            _invoke(connection, "downgrade")
        assert connection.in_transaction()
        assert all(
            sa.inspect(connection).has_table(name) for name in NATIVE_ACCOUNT_IDENTITY_TABLES
        )
        with pytest.raises(IntegrityError, match="append-only"):
            connection.execute(sa.text(f"DELETE FROM {table}"))
        assert connection.scalar(sa.text(f"SELECT COUNT(*) FROM {table}")) == 1


@pytest.mark.parametrize("completion", ("commit", "rollback"))
@pytest.mark.parametrize("table", NATIVE_ACCOUNT_IDENTITY_TABLES)
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
                for name in NATIVE_ACCOUNT_IDENTITY_TABLES
            )
        finally:
            if transaction.is_active:
                transaction.rollback()


@pytest.mark.parametrize("table", NATIVE_ACCOUNT_IDENTITY_TABLES)
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
        assert all(sa.inspect(first).has_table(name) for name in NATIVE_ACCOUNT_IDENTITY_TABLES)
        first.rollback()


@pytest.mark.parametrize("table", NATIVE_ACCOUNT_IDENTITY_TABLES)
def test_postgresql_constraint_names_are_unique_within_each_read_table(table: str) -> None:
    """Different physical checks cannot share a PostgreSQL constraint identity.

    Given: Actual model declarations with backend-conditional sentinel checks,
    When: Every named constraint emitted to PostgreSQL is grouped within either table,
    Then: Every name is unique so PostgreSQL can retain every declared check.
    """
    tables = {model.__table__.name: model.__table__ for model in (NativeAccountIdentityProof,)}
    ddl = str(CreateTable(tables[table]).compile(dialect=postgres_dialect()))
    names = [line.split()[1] for line in ddl.splitlines() if line.strip().startswith("CONSTRAINT ")]
    assert len(names) == len(set(names))


def test_postgresql_rendered_models_and_migration_match_without_claiming_live_evidence() -> None:
    """The model and migration declare identical PostgreSQL types, constraints and indexes.

    Given: Captured actual migration operations and the ORM proof-table contract,
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
    models = {model.__table__.name: model.__table__ for model in (NativeAccountIdentityProof,)}
    metadata = sa.MetaData()
    NativeAccountReadAttempt.__table__.to_metadata(metadata)
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
    lock = "LOCK TABLE " + ", ".join(NATIVE_ACCOUNT_IDENTITY_TABLES) + " IN ACCESS EXCLUSIVE MODE"
    assert lock in statements
    assert sum("ENABLE ALWAYS TRIGGER" in statement for statement in statements) == 2
    assert statements[-1] == "DROP FUNCTION IF EXISTS native_account_identity_reject_mutation()"
    assert [call.args[0] for call in operations.drop_table.call_args_list] == list(
        reversed(NATIVE_ACCOUNT_IDENTITY_TABLES)
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
                sa.inspect(connection).has_table(table) for table in NATIVE_ACCOUNT_IDENTITY_TABLES
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
    table = "unknown_table" if profile == "unknown_table" else _PROOFS
    function = (
        install_native_account_identity_triggers
        if operation == "install"
        else drop_native_account_identity_triggers
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
