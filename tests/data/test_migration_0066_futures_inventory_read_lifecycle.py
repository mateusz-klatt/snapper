"""Physical inventory-purpose ordering, actual proof/attempt foreign keys and append-only guards.

The isolated database installs real 0058/0061/0062 parent declarations and the
actual new 0066 migration. Physical parent rows claim no authenticated HTTP or
permission. Application tests separately bind the closed transport and sources.
"""

from collections.abc import Iterator
from importlib import import_module
from pathlib import Path
from types import ModuleType
from unittest.mock import patch

import pytest
import sqlalchemy as sa
from alembic.migration import MigrationContext
from alembic.operations import Operations
from sqlalchemy.exc import IntegrityError

from snapper.data.futures_inventory_read_triggers import FUTURES_INVENTORY_READ_TABLES
from tests.data.test_migration_0062_native_account_identity_proofs import (
    _insert_valid as retain_proof,
)
from tests.data.test_migration_0062_native_account_identity_proofs import _values as proof_values

_MIGRATION = import_module("snapper.data.migrations.versions.0066_futures_inventory_read_lifecycle")
_PARENTS = (
    import_module("snapper.data.migrations.versions.0058_contract_accounting_evidence"),
    import_module("snapper.data.migrations.versions.0061_native_account_read_lifecycle"),
    import_module("snapper.data.migrations.versions.0062_native_account_identity_proofs"),
)
_ATTEMPTS, _OUTCOMES = FUTURES_INVENTORY_READ_TABLES
_UUID = "00000000-0000-7000-8000-000000000001"
_OTHER = "00000000-0000-7000-8000-000000000099"
_TIMESTAMP = "2026-10-23 00:00:00.000000"
type _Value = str | int | None


def _values(table: str) -> dict[str, _Value]:
    """Provide complete physical columns without claiming an actual captured inventory entity."""
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
        "purpose": "kraken_futures.open_orders",
        "family_sequence": 1,
        "content_digest": "b" * 64,
        "canonical_payload": "{}",
    }
    if table == _ATTEMPTS:
        values.update(
            client_selection_digest="c" * 64,
            key_proof_public_id=_UUID,
            key_proof_content_digest="b" * 64,
            started_at=_TIMESTAMP,
        )
    else:
        assert table == _OUTCOMES
        values.update(
            attempt_public_id=_UUID,
            attempt_content_digest="b" * 64,
            completed_at=_TIMESTAMP,
            result_kind="captured",
        )
    return values


def _insert(connection: sa.Connection, table: str, values: dict[str, _Value]) -> None:
    """Insert one allowlisted real physical shape through its actual constraints and triggers."""
    assert table in FUTURES_INVENTORY_READ_TABLES
    assert connection.scalar(sa.text("PRAGMA foreign_keys")) == 1
    assert connection.scalar(sa.text("PRAGMA recursive_triggers")) == 1
    columns = ", ".join(values)
    parameters = ", ".join(f":{name}" for name in values)
    connection.execute(sa.text(f"INSERT INTO {table} ({columns}) VALUES ({parameters})"), values)


def _retain_valid(connection: sa.Connection, table: str) -> None:
    """Retain real physical parents without claiming closed producer provenance."""
    retain_proof(connection, "native_account_identity_proofs", proof_values())
    _insert(connection, _ATTEMPTS, _values(_ATTEMPTS))
    if table == _OUTCOMES:
        _insert(connection, table, _values(table))


def _invoke(connection: sa.Connection, module: ModuleType, operation: str) -> None:
    """Invoke the genuine migration under the actual caller-owned SQLite transaction."""
    with patch.object(module, "op", Operations(MigrationContext.configure(connection))):
        getattr(module, operation)()


@pytest.fixture
def database(tmp_path: Path) -> Iterator[sa.Engine]:
    """Build real parent and inventory DDL with physical foreign keys and recursive trigger checks."""
    engine = sa.create_engine(f"sqlite:///{tmp_path / 'inventory.db'}")
    try:
        with engine.begin() as connection:
            connection.exec_driver_sql("PRAGMA foreign_keys=ON")
            connection.exec_driver_sql("PRAGMA recursive_triggers=ON")
            for migration in (*_PARENTS, _MIGRATION):
                _invoke(connection, migration, "upgrade")
        yield engine
    finally:
        engine.dispose()


@pytest.mark.parametrize("table", FUTURES_INVENTORY_READ_TABLES)
def test_inventory_rows_have_no_financial_raw_or_body_column(
    database: sa.Engine, table: str
) -> None:
    """Inventory source bytes belong only to the strict immutable outcome payload.

    Given: Actual 0066 tables installed against their genuine physical proof/attempt parents,
    When: The stored inventory lifecycle columns are inspected,
    Then: Neither table can reuse a financial raw UUID or a separately mutable body column.
    """
    columns = {item["name"] for item in sa.inspect(database).get_columns(table)}
    assert "raw_receipt_public_id" not in columns and "body" not in columns
    assert {"content_digest", "canonical_payload"} <= columns


@pytest.mark.parametrize(
    "changes",
    (
        {"scope_digest": "d" * 64},
        {"client_selection_digest": "e" * 64},
        {"key_proof_content_digest": "f" * 64},
    ),
)
def test_inventory_family_ordinal_cannot_fork_by_credential_or_digest(
    database: sa.Engine, changes: dict[str, _Value]
) -> None:
    """Fixed inventory-purpose ordering remains one stream across credential and copied digest changes.

    Given: One retained inventory attempt with an actual physical key proof parent,
    When: A different UUID repeats its direct family ordinal while changing indexed source copies,
    Then: The direct family unique constraint preserves the first request coordinate.
    """
    with database.begin() as connection:
        _retain_valid(connection, _ATTEMPTS)
        with pytest.raises(IntegrityError):
            _insert(connection, _ATTEMPTS, {**_values(_ATTEMPTS), "public_id": _OTHER, **changes})


@pytest.mark.parametrize("table", FUTURES_INVENTORY_READ_TABLES)
def test_inventory_lifecycle_refuses_missing_actual_parent(database: sa.Engine, table: str) -> None:
    """A inventory row cannot manufacture a selected-key proof or a prior inventory request.

    Given: Real empty inventory tables and enforced physical foreign keys,
    When: A complete attempt or outcome names an absent proof or inventory attempt,
    Then: The actual foreign key rejects the dangling relation.
    """
    with database.begin() as connection, pytest.raises(IntegrityError, match="FOREIGN KEY"):
        _insert(connection, table, _values(table))


def test_inventory_outcome_is_unique_per_actual_attempt(database: sa.Engine) -> None:
    """A later result cannot supersede an earlier immutable outcome of the same request.

    Given: A real retained inventory attempt and its first physical captured result,
    When: A different result UUID attempts to reuse that original request,
    Then: The outcome-attempt unique key refuses replacement or a second result.
    """
    with database.begin() as connection:
        _retain_valid(connection, _OUTCOMES)
        with pytest.raises(IntegrityError):
            _insert(
                connection,
                _OUTCOMES,
                {**_values(_OUTCOMES), "public_id": _OTHER, "result_kind": "failed"},
            )


@pytest.mark.parametrize("table", FUTURES_INVENTORY_READ_TABLES)
@pytest.mark.parametrize("operation", ("UPDATE", "DELETE"))
def test_inventory_lifecycle_is_physically_append_only(
    database: sa.Engine, table: str, operation: str
) -> None:
    """Actual trigger protection applies independently of the application writer.

    Given: One real inventory lifecycle row with its required actual parent rows,
    When: Direct SQL attempts a content update or deletion,
    Then: The installed append-only trigger refuses mutation and the original row remains.
    """
    with database.begin() as connection:
        _retain_valid(connection, table)
        statement = (
            f"UPDATE {table} SET canonical_payload='changed'"
            if operation == "UPDATE"
            else f"DELETE FROM {table}"
        )
        with pytest.raises(IntegrityError, match="append-only"):
            connection.exec_driver_sql(statement)
        assert connection.scalar(sa.text(f"SELECT canonical_payload FROM {table}")) == "{}"


@pytest.mark.parametrize("table", FUTURES_INVENTORY_READ_TABLES)
def test_inventory_evidence_blocks_lossless_downgrade(database: sa.Engine, table: str) -> None:
    """Pending requests and historical results cannot be destroyed by a schema rollback.

    Given: An actual nonempty inventory lifecycle table protected by real triggers,
    When: The genuine 0066 downgrade acquires its physical write fence,
    Then: It refuses evidence loss and leaves both inventory tables and mutation guards intact.
    """
    with database.begin() as connection:
        _retain_valid(connection, table)
    with database.connect() as connection:
        with pytest.raises(RuntimeError, match="lossless downgrade"):
            _invoke(connection, _MIGRATION, "downgrade")
        with pytest.raises(IntegrityError, match="append-only"):
            connection.exec_driver_sql(f"DELETE FROM {table}")
        connection.rollback()


def test_empty_inventory_history_can_downgrade_without_touching_identity_parents(
    database: sa.Engine,
) -> None:
    """An empty inventory history is independently removable without erasing original account identity.

    Given: Empty inventory tables and an actual separately retained physical identity proof,
    When: The genuine inventory downgrade completes under its retained caller transaction,
    Then: Only the two inventory tables disappear and the actual identity parent remains unchanged.
    """
    with database.begin() as connection:
        retain_proof(connection, "native_account_identity_proofs", proof_values())
    with database.connect() as connection:
        _invoke(connection, _MIGRATION, "downgrade")
        connection.commit()
        assert not set(FUTURES_INVENTORY_READ_TABLES) & set(
            sa.inspect(connection).get_table_names()
        )
        assert (
            connection.scalar(sa.text("SELECT COUNT(*) FROM native_account_identity_proofs")) == 1
        )


def test_inventory_purposes_have_independent_request_ordinals(database: sa.Engine) -> None:
    """Both endpoint families retain independent latest coordinates in one account scope.

    Given: A real physical proof and an open-orders attempt at ordinal one,
    When: Another UUID records open-positions ordinal one under its distinct digest,
    Then: Both original attempts persist with their explicit purposes and first ordinal.
    """
    with database.begin() as connection:
        _retain_valid(connection, _ATTEMPTS)
        _insert(
            connection,
            _ATTEMPTS,
            {
                **_values(_ATTEMPTS),
                "public_id": _OTHER,
                "purpose": "kraken_futures.open_positions",
                "scope_digest": "d" * 64,
            },
        )
        rows = connection.execute(
            sa.text(f"SELECT purpose, family_sequence FROM {_ATTEMPTS} ORDER BY purpose")
        ).all()
        assert [tuple(row) for row in rows] == [
            ("kraken_futures.open_orders", 1),
            ("kraken_futures.open_positions", 1),
        ]


def test_inventory_digest_ordinal_cannot_fork_into_another_family(database: sa.Engine) -> None:
    """The indexed digest coordinate independently rejects contradictory direct family copies.

    Given: One actual inventory attempt and its purpose-scoped digest ordinal,
    When: Another direct wallet family reuses that exact digest coordinate,
    Then: The actual digest-ordinal unique constraint refuses the second UUID.
    """
    with database.begin() as connection:
        _retain_valid(connection, _ATTEMPTS)
        with pytest.raises(IntegrityError, match="UNIQUE"):
            _insert(
                connection,
                _ATTEMPTS,
                {**_values(_ATTEMPTS), "public_id": _OTHER, "wallet_public_id": _OTHER},
            )


def test_inventory_outcome_cannot_change_its_actual_attempt_purpose(
    database: sa.Engine,
) -> None:
    """Actual parent existence cannot bind a result to a different endpoint.

    Given: A stored open-orders attempt with its valid physical key-proof parent,
    When: An outcome uses that actual UUID while claiming open-positions purpose,
    Then: The composite attempt-purpose foreign key refuses the endpoint mismatch.
    """
    with database.begin() as connection:
        _retain_valid(connection, _ATTEMPTS)
        assert connection.scalar(sa.text(f"SELECT COUNT(*) FROM {_ATTEMPTS}")) == 1
        with pytest.raises(IntegrityError, match="FOREIGN KEY"):
            _insert(
                connection,
                _OUTCOMES,
                {**_values(_OUTCOMES), "purpose": "kraken_futures.open_positions"},
            )
        assert connection.scalar(sa.text(f"SELECT COUNT(*) FROM {_OUTCOMES}")) == 0


@pytest.mark.parametrize("table", FUTURES_INVENTORY_READ_TABLES)
def test_inventory_lifecycle_rejects_unknown_purpose(database: sa.Engine, table: str) -> None:
    """A copied scope digest never widens the two fixed endpoint namespaces.

    Given: Real physical proof and attempt parents with supported endpoint purpose,
    When: A new UUID supplies an unknown third endpoint purpose,
    Then: The actual purpose CHECK refuses the nominal result or request.
    """
    with database.begin() as connection:
        if table == _ATTEMPTS:
            retain_proof(connection, "native_account_identity_proofs", proof_values())
        else:
            _retain_valid(connection, _ATTEMPTS)
        with pytest.raises(IntegrityError, match="CHECK"):
            _insert(
                connection,
                table,
                {**_values(table), "public_id": _OTHER, "purpose": "kraken_futures.unknown"},
            )


@pytest.mark.parametrize(
    "changes",
    (
        {"attempt_content_digest": "d" * 64},
        {"scope_digest": "e" * 64},
        {"family_sequence": 2},
        {"wallet_public_id": _OTHER},
        {"environment": "demo"},
    ),
)
def test_inventory_outcome_rebinds_every_nonnull_actual_parent_copy(
    database: sa.Engine, changes: dict[str, _Value]
) -> None:
    """An existing attempt UUID cannot excuse altered physical request identity copies.

    Given: An actual retained attempt with all non-NULL binding fields intact,
    When: Its outcome changes one otherwise valid digest, ordinal or account-family copy,
    Then: The actual composite foreign key rejects that contradictory parent relation.
    """
    with database.begin() as connection:
        _retain_valid(connection, _ATTEMPTS)
        with pytest.raises(IntegrityError, match="FOREIGN KEY"):
            _insert(connection, _OUTCOMES, {**_values(_OUTCOMES), **changes})
        assert connection.scalar(sa.text(f"SELECT COUNT(*) FROM {_OUTCOMES}")) == 0


def test_inventory_parent_fk_has_all_repeated_nonnull_operands(database: sa.Engine) -> None:
    """The retained parent relation includes every repeated non-NULL family operand.

    Given: The actual migrated inventory outcome table and its declared parent constraint,
    When: The real SQLite foreign-key operands are inspected,
    Then: They include the complete request identity and exclude always-NULL margin labels.
    """
    foreign_keys = sa.inspect(database).get_foreign_keys(_OUTCOMES)
    parent = next(item for item in foreign_keys if item["referred_table"] == _ATTEMPTS)
    expected = [
        "attempt_public_id",
        "attempt_content_digest",
        "purpose",
        "scope_digest",
        "family_sequence",
        "wallet_public_id",
        "exchange",
        "mode",
        "environment",
    ]
    assert parent["constrained_columns"] == expected
    assert parent["referred_columns"] == ["public_id", "content_digest", *expected[2:]]
    assert "margin_account" not in parent["constrained_columns"]
