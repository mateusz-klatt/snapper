"""Tests for per-asset spot precision evidence migration 0028."""

import importlib
import sqlite3
from io import StringIO
from pathlib import Path
from unittest.mock import patch
from uuid import uuid4

import pytest
import sqlalchemy as sa
from alembic import command
from alembic.config import Config
from alembic.migration import MigrationContext
from alembic.operations import Operations

from snapper.data import models
from snapper.data.models import SpotAssetPrecisionEvidence
from snapper.data.models import TZDateTime
from snapper.data.models import UUIDColumn

ALEMBIC_INI = Path(__file__).resolve().parents[2] / "alembic.ini"
_ACTIVE = "9999-12-31 23:59:59.000000"
_FIRST_TS = "2026-07-16 08:00:00.000000"
_SECOND_TS = "2026-07-16 09:00:00.000000"
_AS_OF_SQL = (
    "SELECT * FROM spot_asset_precision_evidence "
    "WHERE exchange = :exchange AND asset IN (:asset) "
    "AND timestamp <= :as_of AND known_to > :as_of "
    "ORDER BY asset, known_to, timestamp, id"
)


def _config(db_url: str) -> Config:
    """Build Alembic configuration for one throwaway database."""
    config = Config(str(ALEMBIC_INI))
    config.set_main_option("sqlalchemy.url", db_url)
    return config


def _insert(
    engine: sa.Engine,
    public_id: str,
    timestamp: str,
    known_to: str,
    *,
    asset: str = "EUR",
    balance_decimals: int | None = None,
    balance_decimals_max: int | None = None,
    balance_max_source: str | None = None,
    balance_max_version: str | None = None,
    balance_max_observed_at: str | None = None,
    balance_source: str | None = None,
    balance_version: str | None = None,
    balance_observed_at: str | None = None,
    fee_decimals: int | None = 2,
    fee_source: str | None = "reviewed_fee_policy",
    fee_version: str | None = "fee-v1",
    fee_observed_at: str | None = _FIRST_TS,
) -> None:
    """Insert one migration-level precision evidence row."""
    with engine.begin() as connection:
        connection.execute(
            sa.text(
                "INSERT INTO spot_asset_precision_evidence "
                "(exchange, asset, balance_decimals, balance_decimals_max, "
                "balance_max_source, balance_max_version, balance_max_observed_at, "
                "balance_source, balance_version, balance_observed_at, fee_decimals, "
                "fee_source, fee_version, fee_observed_at, public_id, session_id, "
                "sequence_id, timestamp, known_to) "
                "VALUES ('walutomat', :asset, :balance_decimals, :balance_decimals_max, "
                ":balance_max_source, :balance_max_version, :balance_max_observed_at, "
                ":balance_source, :balance_version, :balance_observed_at, :fee_decimals, "
                ":fee_source, :fee_version, :fee_observed_at, :public_id, :session_id, "
                "1, :timestamp, :known_to)"
            ),
            {
                "asset": asset,
                "balance_decimals": balance_decimals,
                "balance_decimals_max": balance_decimals_max,
                "balance_max_source": balance_max_source,
                "balance_max_version": balance_max_version,
                "balance_max_observed_at": balance_max_observed_at,
                "balance_source": balance_source,
                "balance_version": balance_version,
                "balance_observed_at": balance_observed_at,
                "fee_decimals": fee_decimals,
                "fee_source": fee_source,
                "fee_version": fee_version,
                "fee_observed_at": fee_observed_at,
                "public_id": public_id,
                "session_id": str(uuid4()),
                "timestamp": timestamp,
                "known_to": known_to,
            },
        )


def _insert_query_plan_history(engine: sa.Engine) -> None:
    """Populate a long same-key history with one sentinel-active successor."""
    public_id = str(uuid4())
    session_id = str(uuid4())
    rows: list[dict[str, str | int]] = [
        {
            "asset": "PLAN",
            "public_id": public_id,
            "session_id": session_id,
            "sequence_id": sequence_id,
            "timestamp": _FIRST_TS,
            "known_to": _SECOND_TS,
        }
        for sequence_id in range(1, 2_000)
    ]
    rows.append(
        {
            "asset": "PLAN",
            "public_id": public_id,
            "session_id": session_id,
            "sequence_id": 2_000,
            "timestamp": _SECOND_TS,
            "known_to": _ACTIVE,
        }
    )
    with engine.begin() as connection:
        connection.execute(
            sa.text(
                "INSERT INTO spot_asset_precision_evidence "
                "(exchange, asset, fee_decimals, fee_source, fee_version, "
                "fee_observed_at, public_id, session_id, sequence_id, timestamp, known_to) "
                "VALUES ('walutomat', :asset, 2, 'reviewed_fee_policy', 'fee-v1', "
                ":timestamp, :public_id, :session_id, :sequence_id, :timestamp, :known_to)"
            ),
            rows,
        )
        connection.execute(sa.text("ANALYZE spot_asset_precision_evidence"))


def test_0028_sqlite_upgrade_and_downgrade_create_scd2_evidence_plane(
    tmp_path: Path,
) -> None:
    """SQLite upgrade creates nullable evidence and active-unique identity."""
    db_url = f"sqlite:///{tmp_path / 'spot-asset-precision.db'}"
    config = _config(db_url)
    command.upgrade(config, "0027")
    engine = sa.create_engine(db_url)
    assert "spot_asset_precision_evidence" not in sa.inspect(engine).get_table_names()

    command.upgrade(config, "0028")
    inspector = sa.inspect(engine)
    columns = {
        column["name"]: column for column in inspector.get_columns("spot_asset_precision_evidence")
    }
    assert set(columns) == {
        "id",
        "public_id",
        "session_id",
        "sequence_id",
        "timestamp",
        "known_to",
        "exchange",
        "asset",
        "balance_decimals",
        "balance_decimals_max",
        "balance_max_source",
        "balance_max_version",
        "balance_max_observed_at",
        "balance_source",
        "balance_version",
        "balance_observed_at",
        "fee_decimals",
        "fee_source",
        "fee_version",
        "fee_observed_at",
    }
    assert columns["balance_decimals"]["nullable"] is True
    assert columns["balance_decimals_max"]["nullable"] is True
    assert columns["fee_decimals"]["nullable"] is True
    indexes = {
        index["name"]: index for index in inspector.get_indexes("spot_asset_precision_evidence")
    }
    assert indexes["uq_spot_asset_precision_evidence_exchange_asset"]["unique"] == 1
    assert indexes["uq_spot_asset_precision_evidence_exchange_asset"]["column_names"] == [
        "exchange",
        "asset",
    ]
    active_predicate = indexes["uq_spot_asset_precision_evidence_exchange_asset"][
        "dialect_options"
    ]["sqlite_where"]
    assert str(active_predicate) == f"known_to = '{_ACTIVE}'"
    assert indexes["ix_spot_asset_precision_evidence_public_id"]["unique"] == 1
    assert indexes["ix_spot_asset_precision_evidence_temporal"]["unique"] == 0
    assert indexes["ix_spot_asset_precision_evidence_temporal"]["column_names"] == [
        "exchange",
        "asset",
        "known_to",
        "timestamp",
    ]
    _insert_query_plan_history(engine)
    query_parameters = {
        "exchange": "walutomat",
        "asset": "PLAN",
        "as_of": _SECOND_TS,
    }
    with engine.connect() as connection:
        plan_rows = connection.execute(
            sa.text(f"EXPLAIN QUERY PLAN {_AS_OF_SQL}"),
            query_parameters,
        ).all()
        plan_details = [str(row[3]) for row in plan_rows]
        assert any(
            "ix_spot_asset_precision_evidence_temporal" in detail and "known_to>?" in detail
            for detail in plan_details
        ), plan_details
        driver_connection = connection.connection.driver_connection
        assert isinstance(driver_connection, sqlite3.Connection)
        vm_steps = 0

        def count_vm_step() -> int:
            """Count SQLite virtual-machine work for the production-shaped read."""
            nonlocal vm_steps
            vm_steps += 1
            return 0

        driver_connection.set_progress_handler(count_vm_step, 1)
        try:
            result_rows = connection.execute(
                sa.text(_AS_OF_SQL),
                query_parameters,
            ).all()
        finally:
            driver_connection.set_progress_handler(None, 0)
    assert len(result_rows) == 1
    assert vm_steps < 500
    constraints = {
        constraint["name"]
        for constraint in inspector.get_check_constraints("spot_asset_precision_evidence")
    }
    assert {
        "ck_spot_asset_precision_evidence_decimals",
        "ck_spot_asset_precision_evidence_balance_max_provenance",
        "ck_spot_asset_precision_evidence_balance_provenance",
        "ck_spot_asset_precision_evidence_balance_ratchet",
        "ck_spot_asset_precision_evidence_exchange_lower",
        "ck_spot_asset_precision_evidence_fee_provenance",
        "ck_spot_asset_precision_evidence_observed_plane",
        "ck_spot_asset_precision_evidence_text",
    } <= constraints

    with pytest.raises(sa.exc.IntegrityError):
        _insert(engine, str(uuid4()), _FIRST_TS, _ACTIVE, asset=" ")
    with pytest.raises(sa.exc.IntegrityError):
        _insert(engine, str(uuid4()), _FIRST_TS, _ACTIVE, asset="USD", fee_source=" ")
    with pytest.raises(sa.exc.IntegrityError):
        _insert(engine, str(uuid4()), _FIRST_TS, _ACTIVE, asset="GBP", fee_version=" ")
    with pytest.raises(sa.exc.IntegrityError):
        _insert(engine, str(uuid4()), _FIRST_TS, _ACTIVE, asset="CHF", balance_decimals=-1)
    with pytest.raises(sa.exc.IntegrityError):
        _insert(
            engine,
            str(uuid4()),
            _FIRST_TS,
            _ACTIVE,
            asset="SEK",
            balance_decimals_max=257,
            balance_max_source="balance-source",
            balance_max_version="balance-version",
            balance_max_observed_at=_FIRST_TS,
        )
    with pytest.raises(sa.exc.IntegrityError):
        _insert(
            engine,
            str(uuid4()),
            _FIRST_TS,
            _ACTIVE,
            asset="NOK",
            balance_decimals_max=4,
            balance_max_source="balance-source",
            balance_max_version="balance-version",
        )
    with pytest.raises(sa.exc.IntegrityError):
        _insert(
            engine,
            str(uuid4()),
            _FIRST_TS,
            _ACTIVE,
            asset="AUD",
            balance_decimals=4,
            balance_source="balance-source",
            balance_version="balance-version",
            balance_observed_at=_FIRST_TS,
            balance_decimals_max=2,
            balance_max_source="balance-source",
            balance_max_version="balance-version",
            balance_max_observed_at=_FIRST_TS,
        )
    with pytest.raises(sa.exc.IntegrityError):
        _insert(engine, str(uuid4()), _FIRST_TS, _ACTIVE, asset="PLN", fee_decimals=257)
    with pytest.raises(sa.exc.IntegrityError):
        _insert(
            engine,
            str(uuid4()),
            _FIRST_TS,
            _ACTIVE,
            asset="JPY",
            balance_source="partial",
        )
    with pytest.raises(sa.exc.IntegrityError):
        _insert(
            engine,
            str(uuid4()),
            _FIRST_TS,
            _ACTIVE,
            asset="CAD",
            fee_source=None,
            fee_version=None,
            fee_observed_at=None,
        )

    public_id = str(uuid4())
    _insert(engine, public_id, _FIRST_TS, _ACTIVE)
    with pytest.raises(sa.exc.IntegrityError):
        _insert(engine, str(uuid4()), _FIRST_TS, _ACTIVE)
    with engine.begin() as connection:
        connection.execute(
            sa.text(
                "UPDATE spot_asset_precision_evidence SET known_to = :known_to "
                "WHERE public_id = :public_id"
            ),
            {"known_to": _SECOND_TS, "public_id": public_id},
        )
    _insert(engine, public_id, _SECOND_TS, _ACTIVE)
    with engine.begin() as connection:
        rows = connection.execute(
            sa.text(
                "SELECT balance_decimals, fee_decimals FROM spot_asset_precision_evidence "
                "WHERE public_id = :public_id ORDER BY timestamp"
            ),
            {"public_id": public_id},
        ).all()
    assert rows == [(None, 2), (None, 2)]

    command.downgrade(config, "0027")
    assert "spot_asset_precision_evidence" not in sa.inspect(engine).get_table_names()
    engine.dispose()


def test_0028_postgresql_compile_and_model_signature() -> None:
    """PostgreSQL offline DDL and ORM metadata expose the same evidence planes."""
    migration = importlib.import_module(
        "snapper.data.migrations.versions.0028_spot_asset_precision_evidence"
    )
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
    assert migration.revision == "0028"
    assert migration.down_revision == "0027"
    assert "CREATE TABLE spot_asset_precision_evidence" in ddl
    assert "public_id UUID NOT NULL" in ddl
    assert "session_id UUID NOT NULL" in ddl
    assert "balance_observed_at TIMESTAMP WITH TIME ZONE" in ddl
    assert "balance_decimals_max INTEGER" in ddl
    assert "balance_max_observed_at TIMESTAMP WITH TIME ZONE" in ddl
    assert "fee_observed_at TIMESTAMP WITH TIME ZONE" in ddl
    assert (
        "CREATE INDEX ix_spot_asset_precision_evidence_temporal ON "
        "spot_asset_precision_evidence (exchange, asset, known_to, timestamp)" in ddl
    )
    assert "ck_spot_asset_precision_evidence_balance_max_provenance" in ddl
    assert "ck_spot_asset_precision_evidence_balance_provenance" in ddl
    assert "ck_spot_asset_precision_evidence_balance_ratchet" in ddl
    assert "ck_spot_asset_precision_evidence_fee_provenance" in ddl
    assert "ck_spot_asset_precision_evidence_observed_plane" in ddl
    assert "WHERE known_to = '9999-12-31T23:59:59+00:00'" in ddl
    assert "DROP TABLE spot_asset_precision_evidence" in ddl

    table = SpotAssetPrecisionEvidence.__table__
    assert set(table.c.keys()) == {
        "id",
        "public_id",
        "session_id",
        "sequence_id",
        "timestamp",
        "known_to",
        "exchange",
        "asset",
        "balance_decimals",
        "balance_decimals_max",
        "balance_max_source",
        "balance_max_version",
        "balance_max_observed_at",
        "balance_source",
        "balance_version",
        "balance_observed_at",
        "fee_decimals",
        "fee_source",
        "fee_version",
        "fee_observed_at",
    }
    assert isinstance(table.c.public_id.type, UUIDColumn)
    assert isinstance(table.c.session_id.type, UUIDColumn)
    assert isinstance(table.c.balance_observed_at.type, TZDateTime)
    assert isinstance(table.c.balance_max_observed_at.type, TZDateTime)
    assert isinstance(table.c.fee_observed_at.type, TZDateTime)
    assert table.c.balance_decimals_max.nullable is True
    assert table.c.balance_source.nullable is True
    assert table.c.fee_source.nullable is True
    assert {constraint.name for constraint in table.constraints if constraint.name is not None} >= {
        "ck_spot_asset_precision_evidence_balance_provenance",
        "ck_spot_asset_precision_evidence_balance_max_provenance",
        "ck_spot_asset_precision_evidence_balance_ratchet",
        "ck_spot_asset_precision_evidence_decimals",
        "ck_spot_asset_precision_evidence_exchange_lower",
        "ck_spot_asset_precision_evidence_fee_provenance",
        "ck_spot_asset_precision_evidence_observed_plane",
        "ck_spot_asset_precision_evidence_text",
    }
    assert {index.name: index.unique for index in table.indexes} == {
        "ix_spot_asset_precision_evidence_public_id": True,
        "ix_spot_asset_precision_evidence_temporal": False,
        "uq_spot_asset_precision_evidence_exchange_asset": True,
    }
    assert SpotAssetPrecisionEvidence.__name__ in models.__all__
