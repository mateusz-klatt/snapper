"""Physical product-purpose ordering, actual proof/attempt foreign keys and append-only guards.

The isolated database installs real 0058/0061/0062 parent declarations and the
actual new 0065 migration. Physical parent rows claim no authenticated HTTP or
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

from snapper.data.futures_product_read_triggers import FUTURES_PRODUCT_READ_TABLES
from tests.data.test_migration_0062_native_account_identity_proofs import (
    _insert_valid as retain_proof,
)
from tests.data.test_migration_0062_native_account_identity_proofs import _values as proof_values

_MIGRATION = import_module("snapper.data.migrations.versions.0065_futures_product_read_lifecycle")
_PARENTS = (
    import_module("snapper.data.migrations.versions.0058_contract_accounting_evidence"),
    import_module("snapper.data.migrations.versions.0061_native_account_read_lifecycle"),
    import_module("snapper.data.migrations.versions.0062_native_account_identity_proofs"),
)
_ATTEMPTS, _OUTCOMES = FUTURES_PRODUCT_READ_TABLES
_UUID = "00000000-0000-7000-8000-000000000001"
_OTHER = "00000000-0000-7000-8000-000000000099"
_TIMESTAMP = "2026-10-23 00:00:00.000000"
type _Value = str | int | None


def _values(table: str) -> dict[str, _Value]:
    """Provide complete physical columns without claiming an actual captured product entity."""
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
    assert table in FUTURES_PRODUCT_READ_TABLES
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
    """Build real parent and product DDL with physical foreign keys and recursive trigger checks."""
    engine = sa.create_engine(f"sqlite:///{tmp_path / 'products.db'}")
    try:
        with engine.begin() as connection:
            connection.exec_driver_sql("PRAGMA foreign_keys=ON")
            connection.exec_driver_sql("PRAGMA recursive_triggers=ON")
            for migration in (*_PARENTS, _MIGRATION):
                _invoke(connection, migration, "upgrade")
        yield engine
    finally:
        engine.dispose()


@pytest.mark.parametrize("table", FUTURES_PRODUCT_READ_TABLES)
def test_product_rows_have_no_financial_raw_or_body_column(database: sa.Engine, table: str) -> None:
    """Product source bytes belong only to the strict immutable outcome payload.

    Given: Actual 0065 tables installed against their genuine physical proof/attempt parents,
    When: The stored product lifecycle columns are inspected,
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
def test_product_family_ordinal_cannot_fork_by_credential_or_digest(
    database: sa.Engine, changes: dict[str, _Value]
) -> None:
    """Fixed product-purpose ordering remains one stream across credential and copied digest changes.

    Given: One retained product attempt with an actual physical key proof parent,
    When: A different UUID repeats its direct family ordinal while changing indexed source copies,
    Then: The direct family unique constraint preserves the first request coordinate.
    """
    with database.begin() as connection:
        _retain_valid(connection, _ATTEMPTS)
        with pytest.raises(IntegrityError):
            _insert(connection, _ATTEMPTS, {**_values(_ATTEMPTS), "public_id": _OTHER, **changes})


@pytest.mark.parametrize("table", FUTURES_PRODUCT_READ_TABLES)
def test_product_lifecycle_refuses_missing_actual_parent(database: sa.Engine, table: str) -> None:
    """A product row cannot manufacture a selected-key proof or a prior product request.

    Given: Real empty product tables and enforced physical foreign keys,
    When: A complete attempt or outcome names an absent proof or product attempt,
    Then: The actual foreign key rejects the dangling relation.
    """
    with database.begin() as connection, pytest.raises(IntegrityError, match="FOREIGN KEY"):
        _insert(connection, table, _values(table))


def test_product_outcome_is_unique_per_actual_attempt(database: sa.Engine) -> None:
    """A later result cannot supersede an earlier immutable outcome of the same request.

    Given: A real retained product attempt and its first physical captured result,
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


@pytest.mark.parametrize("table", FUTURES_PRODUCT_READ_TABLES)
@pytest.mark.parametrize("operation", ("UPDATE", "DELETE"))
def test_product_lifecycle_is_physically_append_only(
    database: sa.Engine, table: str, operation: str
) -> None:
    """Actual trigger protection applies independently of the application writer.

    Given: One real product lifecycle row with its required actual parent rows,
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


@pytest.mark.parametrize("table", FUTURES_PRODUCT_READ_TABLES)
def test_product_evidence_blocks_lossless_downgrade(database: sa.Engine, table: str) -> None:
    """Pending requests and historical results cannot be destroyed by a schema rollback.

    Given: An actual nonempty product lifecycle table protected by real triggers,
    When: The genuine 0065 downgrade acquires its physical write fence,
    Then: It refuses evidence loss and leaves both product tables and mutation guards intact.
    """
    with database.begin() as connection:
        _retain_valid(connection, table)
    with database.connect() as connection:
        with pytest.raises(RuntimeError, match="lossless downgrade"):
            _invoke(connection, _MIGRATION, "downgrade")
        with pytest.raises(IntegrityError, match="append-only"):
            connection.exec_driver_sql(f"DELETE FROM {table}")
        connection.rollback()


def test_empty_product_history_can_downgrade_without_touching_identity_parents(
    database: sa.Engine,
) -> None:
    """An empty product history is independently removable without erasing original account identity.

    Given: Empty product tables and an actual separately retained physical identity proof,
    When: The genuine product downgrade completes under its retained caller transaction,
    Then: Only the two product tables disappear and the actual identity parent remains unchanged.
    """
    with database.begin() as connection:
        retain_proof(connection, "native_account_identity_proofs", proof_values())
    with database.connect() as connection:
        _invoke(connection, _MIGRATION, "downgrade")
        connection.commit()
        assert not set(FUTURES_PRODUCT_READ_TABLES) & set(sa.inspect(connection).get_table_names())
        assert (
            connection.scalar(sa.text("SELECT COUNT(*) FROM native_account_identity_proofs")) == 1
        )
