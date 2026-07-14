"""Tests for the dual-dialect instrument metadata migration."""

import importlib
from decimal import Decimal
from io import StringIO
from pathlib import Path
from unittest.mock import patch

import pytest
import sqlalchemy as sa
from alembic import command
from alembic.config import Config
from alembic.migration import MigrationContext
from alembic.operations import Operations
from sqlalchemy.exc import IntegrityError

from snapper.data.models import InstrumentSpec
from snapper.data.models import TZDateTime

ALEMBIC_INI = Path(__file__).resolve().parents[2] / "alembic.ini"
_ACTIVE = "9999-12-31 23:59:59.000000"
_TS = "2026-07-14 08:00:00.000000"


def _config(db_url: str) -> Config:
    """Build an Alembic configuration for one throwaway database."""
    config = Config(str(ALEMBIC_INI))
    config.set_main_option("sqlalchemy.url", db_url)
    return config


def _insert_spec(
    engine: sa.Engine,
    suffix: str,
    contract_size: Decimal | None,
    quantity_unit: str | None,
    spec_source: str | None,
    spec_version: str | None,
    spec_observed_at: str | None,
    unit_certified: bool,
) -> None:
    """Insert one post-0023 instrument specification through literal SQL."""
    with engine.begin() as connection:
        connection.execute(
            sa.text(
                "INSERT INTO instrument_specs "
                "(public_id, instrument_public_id, session_id, sequence_id, timestamp, known_to, "
                "contract_size, quantity_unit, spec_source, spec_version, spec_observed_at, "
                "unit_certified) VALUES "
                "(:public_id, :instrument_public_id, :session_id, 1, :timestamp, :known_to, "
                ":contract_size, :quantity_unit, :spec_source, :spec_version, :spec_observed_at, "
                ":unit_certified)"
            ),
            {
                "public_id": f"spec-{suffix}",
                "instrument_public_id": f"inst-{suffix}",
                "session_id": f"session-{suffix}",
                "timestamp": _TS,
                "known_to": _ACTIVE,
                "contract_size": str(contract_size) if contract_size is not None else None,
                "quantity_unit": quantity_unit,
                "spec_source": spec_source,
                "spec_version": spec_version,
                "spec_observed_at": spec_observed_at,
                "unit_certified": unit_certified,
            },
        )


def test_0023_sqlite_upgrade_checks_downgrade_and_reupgrade(tmp_path: Path) -> None:
    """SQLite preserves rows and enforces every new fail-closed invariant."""
    db_url = f"sqlite:///{tmp_path / 'instrument_metadata.db'}"
    config = _config(db_url)
    command.upgrade(config, "0022")
    engine = sa.create_engine(db_url)
    with engine.begin() as connection:
        connection.execute(
            sa.text(
                "INSERT INTO instrument_specs "
                "(public_id, instrument_public_id, session_id, sequence_id, timestamp, known_to) "
                "VALUES ('old-spec', 'old-inst', 'old-session', 1, :timestamp, :known_to)"
            ),
            {"timestamp": _TS, "known_to": _ACTIVE},
        )
    command.upgrade(config, "0023")
    columns = {
        column["name"]: column for column in sa.inspect(engine).get_columns("instrument_specs")
    }
    assert columns["contract_size"]["type"].precision == 38
    assert columns["contract_size"]["type"].scale == 18
    assert columns["quantity_unit"]["type"].length == 32
    assert columns["spec_source"]["type"].length == 64
    assert columns["spec_version"]["type"].length == 96
    assert columns["spec_observed_at"]["nullable"] is True
    assert columns["unit_certified"]["nullable"] is False
    with engine.begin() as connection:
        old = connection.execute(
            sa.text(
                "SELECT contract_size, quantity_unit, spec_source, spec_version, "
                "spec_observed_at, unit_certified FROM instrument_specs "
                "WHERE public_id = 'old-spec'"
            )
        ).one()
    assert old == (None, None, None, None, None, 0)
    _insert_spec(engine, "null", None, None, None, None, None, False)
    _insert_spec(
        engine,
        "certified",
        Decimal("7.125000000000000001"),
        "contract_count",
        "kraken_futures:rest.get_instruments",
        "s2a-v1:abc",
        _TS,
        True,
    )
    invalid_rows = (
        ("zero", Decimal("0"), None, None, None, None, False),
        ("negative", Decimal("-1"), None, None, None, None, False),
        ("unit", None, "lots", None, None, None, False),
        ("partial-source", None, "base_asset", "kraken:ccxt.load_markets", None, None, False),
        ("partial-version", None, "base_asset", None, "s2a-v1:x", None, False),
        ("partial-time", None, "base_asset", None, None, _TS, False),
        (
            "cert-no-contract",
            None,
            "contract_count",
            "kraken_futures:rest.get_instruments",
            "s2a-v1:x",
            _TS,
            True,
        ),
        (
            "cert-base",
            Decimal("1"),
            "base_asset",
            "kraken_futures:rest.get_instruments",
            "s2a-v1:x",
            _TS,
            True,
        ),
        (
            "cert-null-unit",
            Decimal("1"),
            None,
            "kraken_futures:rest.get_instruments",
            "s2a-v1:x",
            _TS,
            True,
        ),
    )
    for values in invalid_rows:
        with pytest.raises(IntegrityError):
            _insert_spec(engine, *values)
    command.downgrade(config, "0022")
    downgraded = {column["name"] for column in sa.inspect(engine).get_columns("instrument_specs")}
    assert "contract_size" not in downgraded
    assert "unit_certified" not in downgraded
    command.upgrade(config, "0023")
    reupgraded = {column["name"] for column in sa.inspect(engine).get_columns("instrument_specs")}
    assert "contract_size" in reupgraded
    assert "unit_certified" in reupgraded
    engine.dispose()


def test_0023_postgresql_compile_and_model_signature() -> None:
    """PostgreSQL DDL and ORM metadata expose the same six-column contract."""
    migration = importlib.import_module("snapper.data.migrations.versions.0023_instrument_metadata")
    output = StringIO()
    context = MigrationContext.configure(
        dialect_name="postgresql",
        opts={"as_sql": True, "output_buffer": output},
    )
    operations = Operations(context)
    with patch.object(migration, "op", operations):
        migration.upgrade()
        migration.downgrade()
    ddl = output.getvalue()
    assert migration.revision == "0023"
    assert migration.down_revision == "0022"
    assert "contract_size NUMERIC(38, 18)" in ddl
    assert "quantity_unit VARCHAR(32)" in ddl
    assert "spec_observed_at TIMESTAMP WITH TIME ZONE" in ddl
    assert "unit_certified BOOLEAN DEFAULT false NOT NULL" in ddl
    assert "ck_instrument_specs_contract_size_positive" in ddl
    assert "ck_instrument_specs_quantity_unit" in ddl
    assert "ck_instrument_specs_provenance" in ddl
    assert "ck_instrument_specs_unit_certified" in ddl
    assert "DROP COLUMN contract_size" in ddl
    expected = {
        "contract_size": (sa.Numeric, True),
        "quantity_unit": (sa.String, True),
        "spec_source": (sa.String, True),
        "spec_version": (sa.String, True),
        "spec_observed_at": (TZDateTime, True),
        "unit_certified": (sa.Boolean, False),
    }
    for name, (type_class, nullable) in expected.items():
        column = InstrumentSpec.__table__.c[name]
        assert isinstance(column.type, type_class)
        assert column.nullable is nullable
