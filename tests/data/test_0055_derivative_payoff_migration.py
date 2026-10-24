"""Dual-dialect witnesses for atomic and retained derivative monetary metadata."""

from importlib import import_module
from io import StringIO
from unittest.mock import MagicMock
from unittest.mock import patch

import pytest
import sqlalchemy as sa
from alembic.migration import MigrationContext
from alembic.operations import Operations
from sqlalchemy.dialects.postgresql import dialect
from sqlalchemy.exc import IntegrityError

from snapper.data.models import InstrumentSpec


def _legacy_table(connection: sa.Connection) -> None:
    """Create two historical specification rows predating monetary evidence."""
    connection.execute(
        sa.text(
            "CREATE TABLE instrument_specs (id INTEGER PRIMARY KEY, "
            "known_to TEXT NOT NULL, unit_certified BOOLEAN NOT NULL DEFAULT false)"
        )
    )
    connection.execute(
        sa.text(
            "INSERT INTO instrument_specs (id, known_to, unit_certified) "
            "VALUES (1, '2026-10-22', true), (2, '9999-12-31', false)"
        )
    )


def test_upgrade_preserves_unknown_history_and_allows_empty_downgrade(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Legacy versions remain unknown without changing quantity certification or history.

    Given: Historical specifications created before explicit derivative interpretations,
    When: The migration upgrades and losslessly downgrades empty evidence fields,
    Then: Original rows remain unchanged and all new interpretations stay null.
    """
    migration = import_module("snapper.data.migrations.versions.0055_derivative_payoff_metadata")
    engine = sa.create_engine("sqlite://")
    try:
        with engine.begin() as connection:
            _legacy_table(connection)
            monkeypatch.setattr(migration, "op", Operations(MigrationContext.configure(connection)))
            migration.upgrade()
            assert connection.execute(
                sa.text(
                    "SELECT id, known_to, unit_certified, contract_payoff_model, "
                    "contract_value_currency, settlement_currency, native_order_quantity_unit "
                    "FROM instrument_specs ORDER BY id"
                )
            ).all() == [
                (1, "2026-10-22", 1, None, None, None, None),
                (2, "9999-12-31", 0, None, None, None, None),
            ]
            migration.downgrade()
            assert {
                column["name"] for column in sa.inspect(connection).get_columns("instrument_specs")
            } == {"id", "known_to", "unit_certified"}
            assert connection.execute(
                sa.text("SELECT * FROM instrument_specs ORDER BY id")
            ).all() == [(1, "2026-10-22", 1), (2, "9999-12-31", 0)]
            migration.upgrade()
            assert connection.execute(
                sa.text("SELECT contract_payoff_model FROM instrument_specs ORDER BY id")
            ).all() == [(None,), (None,)]
    finally:
        engine.dispose()


def test_sqlite_rejects_partial_evidence_and_retains_populated_history(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Every incomplete tuple is rejected and any recorded payoff blocks lossy downgrade.

    Given: An upgraded store with nullable payoff evidence and atomicity constraints,
    When: Writers attempt incomplete tuples and then persist valid interpretations,
    Then: Invalid tuples are rejected and populated history prevents downgrade.
    """
    migration = import_module("snapper.data.migrations.versions.0055_derivative_payoff_metadata")
    engine = sa.create_engine("sqlite://")
    invalid: tuple[tuple[str | None, str | None, str | None], ...] = (
        ("quanto", "BTC", "USD"),
        ("linear", None, None),
        (None, "BTC", None),
        (None, None, "USD"),
        ("linear", "BTC", None),
        ("linear", None, "USD"),
        (None, "BTC", "USD"),
        ("linear", "", "USD"),
        ("linear", "BTC", " "),
    )
    insert = sa.text(
        "INSERT INTO instrument_specs (id, known_to, contract_payoff_model, "
        "contract_value_currency, settlement_currency) "
        "VALUES (:id, '2026-10-22', :payoff, :value, :settlement)"
    )
    try:
        with engine.begin() as connection:
            _legacy_table(connection)
            monkeypatch.setattr(migration, "op", Operations(MigrationContext.configure(connection)))
            migration.upgrade()
            for number, (payoff, value, settlement) in enumerate(invalid, start=3):
                with pytest.raises(IntegrityError):
                    connection.execute(
                        insert,
                        {"id": number, "payoff": payoff, "value": value, "settlement": settlement},
                    )
            for number, (payoff, value, settlement) in enumerate(
                (("linear", "BTC", "USD"), ("inverse", "USD", "BTC")), start=20
            ):
                connection.execute(
                    insert,
                    {"id": number, "payoff": payoff, "value": value, "settlement": settlement},
                )
            with pytest.raises(RuntimeError, match="prevents a lossless downgrade"):
                migration.downgrade()
            assert connection.execute(
                sa.text(
                    "SELECT contract_payoff_model, contract_value_currency, settlement_currency "
                    "FROM instrument_specs WHERE id >= 20 ORDER BY id"
                )
            ).all() == [("linear", "BTC", "USD"), ("inverse", "USD", "BTC")]
            assert "contract_payoff_model" in {
                column["name"] for column in sa.inspect(connection).get_columns("instrument_specs")
            }
    finally:
        engine.dispose()


def test_postgresql_ddl_and_model_share_the_nullable_atomic_contract() -> None:
    """PostgreSQL DDL exposes the same optional fields and checks as the ORM model.

    Given: Offline PostgreSQL migration operations and the production ORM schema,
    When: Empty-evidence upgrade and downgrade statements are generated,
    Then: Native units and payoff columns have matching lengths, nullability and checks.
    """
    migration = import_module("snapper.data.migrations.versions.0055_derivative_payoff_metadata")
    output = StringIO()
    context = MigrationContext.configure(
        dialect_name="postgresql", opts={"as_sql": True, "output_buffer": output}
    )
    operations = Operations(context)
    connection = MagicMock(
        dialect=dialect(),
        scalar=MagicMock(return_value=0),
        get_isolation_level=MagicMock(return_value="READ COMMITTED"),
    )
    with (
        patch.object(migration, "op", operations),
        patch.object(operations, "get_bind", return_value=connection),
    ):
        migration.upgrade()
        migration.downgrade()
    ddl = output.getvalue()
    assert migration.revision == "0055"
    assert migration.down_revision == "0054"
    for name, length in (
        ("contract_payoff_model", 16),
        ("contract_value_currency", 32),
        ("settlement_currency", 32),
        ("native_order_quantity_unit", 32),
    ):
        assert f"{name} VARCHAR({length})" in ddl
        assert f"DROP COLUMN {name}" in ddl
        column = InstrumentSpec.__table__.c[name]
        assert isinstance(column.type, sa.String)
        assert column.type.length == length
        assert column.nullable is True
    assert "ck_instrument_specs_payoff_model" in ddl
    assert "ck_instrument_specs_payoff_evidence" in ddl
    assert "ck_instrument_specs_native_order_quantity_unit" in ddl


def test_sqlite_native_order_units_are_checked_and_block_lossy_downgrade(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Retain native-unit evidence even when no monetary interpretation was recorded.

    Given: Historical specifications upgraded with a nullable native order unit,
    When: A writer supplies an unknown label or records known units without payoff fields,
    Then: Invalid labels are rejected and retained unit evidence blocks downgrade.
    """
    migration = import_module("snapper.data.migrations.versions.0055_derivative_payoff_metadata")
    engine = sa.create_engine("sqlite://")
    insert = sa.text(
        "INSERT INTO instrument_specs (id, known_to, native_order_quantity_unit) "
        "VALUES (:id, '2026-10-22', :unit)"
    )
    try:
        with engine.begin() as connection:
            _legacy_table(connection)
            monkeypatch.setattr(migration, "op", Operations(MigrationContext.configure(connection)))
            migration.upgrade()
            with pytest.raises(IntegrityError):
                connection.execute(insert, {"id": 3, "unit": "shares"})
            for number, unit in enumerate(("base_asset", "contract_count"), start=3):
                connection.execute(insert, {"id": number, "unit": unit})
            with pytest.raises(RuntimeError, match="prevents a lossless downgrade"):
                migration.downgrade()
            assert connection.execute(
                sa.text(
                    "SELECT native_order_quantity_unit, contract_payoff_model "
                    "FROM instrument_specs WHERE id >= 3 ORDER BY id"
                )
            ).all() == [("base_asset", None), ("contract_count", None)]
    finally:
        engine.dispose()
