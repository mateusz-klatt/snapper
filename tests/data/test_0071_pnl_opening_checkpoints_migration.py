"""Tests for migration 0071 creating ``portfolio_pnl_opening_checkpoints``.

Covers SQLite upgrade/downgrade round-trip without altering preexisting tables
such as ``candles`` and ``portfolio_pnl_points``, strict ORM-versus-migration
schema parity, offline PostgreSQL DDL generation in both directions, and all
row-level CHECK constraints including arbitrary-precision decimal sum text.
"""

from importlib import import_module
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

from snapper.data.models import PortfolioPnlOpeningCheckpoint

_ALEMBIC_INI = Path(__file__).resolve().parents[2] / "alembic.ini"
_TABLE = "portfolio_pnl_opening_checkpoints"
_M_CKPT = "2026-11-20 10:03:00.000000"
_M_AS_OF = "2026-11-20 10:05:00.000000"
_M_BEFORE = "2026-11-20 10:02:00.000000"
_DIGEST = "a" * 64

_INSERT_SQL = (
    "INSERT INTO portfolio_pnl_opening_checkpoints ("
    "wallet_public_id, mode, valuation_ccy, epoch_public_id, calc_version, "
    "timeline_calc_version, fx_policy_version, schema_version, source_generation, "
    "checkpoint_minute, as_of, anchor_public_id, anchor_digest, "
    "cutoff_execution_watermarks_json, execution_chain_digests_json, "
    "cutoff_execution_counts_json, applied_watermarks_json, "
    "deferred_execution_sequences_json, prefix_has_idless_executions, "
    "execution_manifest_digest, prefix_annulment_digest, "
    "accrual_prefix_fingerprint_json, prefix_lineage_digest, "
    "prefix_fill_witness_digest, prefix_venue_event_watermarks_json, "
    "symbol_ref_digest, consumed_fx_pairs_json, consumed_fx_open_ats_json, "
    "event_fx_exchanges_digest, event_fx_candle_count, event_fx_candle_id_sum, "
    "event_fx_candle_max_id, event_fx_candle_max_ts, state_json, updated_at"
    ") VALUES ("
    ":wallet_public_id, :mode, :valuation_ccy, :epoch_public_id, :calc_version, "
    ":timeline_calc_version, :fx_policy_version, :schema_version, :source_generation, "
    ":checkpoint_minute, :as_of, :anchor_public_id, :anchor_digest, "
    ":cutoff_execution_watermarks_json, :execution_chain_digests_json, "
    ":cutoff_execution_counts_json, :applied_watermarks_json, "
    ":deferred_execution_sequences_json, :prefix_has_idless_executions, "
    ":execution_manifest_digest, :prefix_annulment_digest, "
    ":accrual_prefix_fingerprint_json, :prefix_lineage_digest, "
    ":prefix_fill_witness_digest, :prefix_venue_event_watermarks_json, "
    ":symbol_ref_digest, :consumed_fx_pairs_json, :consumed_fx_open_ats_json, "
    ":event_fx_exchanges_digest, :event_fx_candle_count, :event_fx_candle_id_sum, "
    ":event_fx_candle_max_id, :event_fx_candle_max_ts, :state_json, :updated_at)"
)


def _config(db_url: str) -> Config:
    """Build Alembic configuration for one throwaway database."""
    config = Config(str(_ALEMBIC_INI))
    config.set_main_option("sqlalchemy.url", db_url)
    return config


def _row(**overrides: object) -> dict[str, object]:
    """Build a valid checkpoint row, overriding named fields for CHECK probes."""
    base: dict[str, object] = {
        "wallet_public_id": "00000000-0000-7000-8000-000000000124",
        "mode": "live",
        "valuation_ccy": "USD",
        "epoch_public_id": "00000000-0000-7000-8000-000000000125",
        "calc_version": "5C.5",
        "timeline_calc_version": "5A.16",
        "fx_policy_version": "pnl-fiat-pointwise-v1",
        "schema_version": 1,
        "source_generation": 1,
        "checkpoint_minute": _M_CKPT,
        "as_of": _M_AS_OF,
        "anchor_public_id": "00000000-0000-7000-8000-000000000126",
        "anchor_digest": _DIGEST,
        "cutoff_execution_watermarks_json": '{"kraken":5}',
        "execution_chain_digests_json": f'{{"kraken":"{_DIGEST}"}}',
        "cutoff_execution_counts_json": '{"kraken":5}',
        "applied_watermarks_json": '{"kraken":5}',
        "deferred_execution_sequences_json": "{}",
        "prefix_has_idless_executions": False,
        "execution_manifest_digest": _DIGEST,
        "prefix_annulment_digest": _DIGEST,
        "accrual_prefix_fingerprint_json": '{"count":0,"id_sum":"0","max_id":0,"max_ts":null}',
        "prefix_lineage_digest": _DIGEST,
        "prefix_fill_witness_digest": _DIGEST,
        "prefix_venue_event_watermarks_json": "{}",
        "symbol_ref_digest": _DIGEST,
        "consumed_fx_pairs_json": "[]",
        "consumed_fx_open_ats_json": "[]",
        "event_fx_exchanges_digest": _DIGEST,
        "event_fx_candle_count": 0,
        "event_fx_candle_id_sum": "0",
        "event_fx_candle_max_id": 0,
        "event_fx_candle_max_ts": None,
        "state_json": "{}",
        "updated_at": _M_AS_OF,
    }
    base.update(overrides)
    return base


def _schema_fingerprint(engine: sa.Engine, table: str) -> dict[str, object]:
    """Reduce one live table schema to a comparable structural fingerprint."""
    inspector = sa.inspect(engine)
    return {
        "columns": [
            (column["name"], str(column["type"]), bool(column["nullable"]))
            for column in inspector.get_columns(table)
        ],
        "primary_key": inspector.get_pk_constraint(table)["constrained_columns"],
        "unique_constraints": sorted(
            (str(constraint["name"]), tuple(constraint["column_names"]))
            for constraint in inspector.get_unique_constraints(table)
        ),
        "indexes": sorted(
            (str(index["name"]), tuple(index["column_names"]), bool(index["unique"]))
            for index in inspector.get_indexes(table)
        ),
        "checks": sorted(
            (str(check["name"]), " ".join(str(check["sqltext"]).split()))
            for check in inspector.get_check_constraints(table)
        ),
    }


def test_0071_round_trip_preserves_candles_and_other_tables(tmp_path: Path) -> None:
    """Upgrade and downgrade alter only ``portfolio_pnl_opening_checkpoints``.

    Given: A SQLite database migrated through revision 0070 with ``candles``
        and ``portfolio_pnl_points`` schemas present.
    When: Revision 0071 is upgraded, downgraded back to 0070, and re-upgraded.
    Then: The opening-checkpoints table with its five-column primary key appears
        on each upgrade, disappears on downgrade, and leaves ``candles`` and
        every other preexisting table completely unchanged.
    """
    db_url = f"sqlite:///{tmp_path / 'roundtrip.db'}"
    config = _config(db_url)
    command.upgrade(config, "0070")
    engine = sa.create_engine(db_url)
    candles_before = _schema_fingerprint(engine, "candles")
    points_before = _schema_fingerprint(engine, "portfolio_pnl_points")
    assert _TABLE not in sa.inspect(engine).get_table_names()

    command.upgrade(config, "0071")
    assert _TABLE in sa.inspect(engine).get_table_names()
    assert sa.inspect(engine).get_pk_constraint(_TABLE)["constrained_columns"] == [
        "wallet_public_id",
        "mode",
        "valuation_ccy",
        "epoch_public_id",
        "calc_version",
    ]
    assert _schema_fingerprint(engine, "candles") == candles_before
    assert _schema_fingerprint(engine, "portfolio_pnl_points") == points_before

    command.downgrade(config, "0070")
    assert _TABLE not in sa.inspect(engine).get_table_names()
    assert _schema_fingerprint(engine, "candles") == candles_before
    assert _schema_fingerprint(engine, "portfolio_pnl_points") == points_before

    command.upgrade(config, "0071")
    assert _TABLE in sa.inspect(engine).get_table_names()
    engine.dispose()


def test_0071_schema_matches_orm_model_exactly(tmp_path: Path) -> None:
    """The Alembic-built and ORM-built tables share an identical fingerprint.

    Given: One SQLite database upgraded to revision 0071 and a second built
        directly via ``PortfolioPnlOpeningCheckpoint.__table__.create``.
    When: Both schemas are reduced to column, primary key, index, and CHECK
        fingerprints.
    Then: Both fingerprints match identically.
    """
    migrated_url = f"sqlite:///{tmp_path / 'migrated.db'}"
    command.upgrade(_config(migrated_url), "0071")
    migrated_engine = sa.create_engine(migrated_url)

    model_url = f"sqlite:///{tmp_path / 'model.db'}"
    model_engine = sa.create_engine(model_url)
    PortfolioPnlOpeningCheckpoint.__table__.create(model_engine)

    assert _schema_fingerprint(migrated_engine, _TABLE) == _schema_fingerprint(model_engine, _TABLE)
    migrated_engine.dispose()
    model_engine.dispose()


def test_0071_postgresql_ddl_round_trip_uses_native_types() -> None:
    """PostgreSQL offline DDL emits native UUID, BIGINT, and TIMESTAMPTZ types.

    Given: Revision 0071 executed against an offline PostgreSQL SQL recorder.
    When: Both ``upgrade()`` and ``downgrade()`` are rendered to SQL.
    Then: The revision metadata pins ``0071`` over ``0070``, creates the table
        with native PostgreSQL types and CHECKs, and drops only that table.
    """
    migration = import_module("snapper.data.migrations.versions.0071_pnl_opening_checkpoints")
    output = StringIO()
    context = MigrationContext.configure(
        dialect_name="postgresql",
        opts={"as_sql": True, "output_buffer": output},
    )
    with patch.object(migration, "op", Operations(context)):
        migration.upgrade()
        migration.downgrade()
    ddl = output.getvalue()
    assert migration.revision == "0071"
    assert migration.down_revision == "0070"
    assert f"CREATE TABLE {_TABLE}" in ddl
    assert "wallet_public_id UUID NOT NULL" in ddl
    assert "source_generation BIGINT NOT NULL" in ddl
    assert "event_fx_candle_count BIGINT NOT NULL" in ddl
    assert "event_fx_candle_max_id BIGINT NOT NULL" in ddl
    assert "checkpoint_minute TIMESTAMP WITH TIME ZONE NOT NULL" in ddl
    assert "as_of TIMESTAMP WITH TIME ZONE NOT NULL" in ddl
    assert "candles" not in ddl
    assert f"DROP TABLE {_TABLE}" in ddl


def test_0071_accepts_large_decimal_id_sum_string(tmp_path: Path) -> None:
    """Exact decimal strings above 64-bit signed integer limits persist intact.

    Given: A database upgraded to revision 0071.
    When: A valid row stores ``2**65 + 7`` as ``event_fx_candle_id_sum``.
    Then: The insert succeeds and returns the exact decimal string verbatim.
    """
    db_url = f"sqlite:///{tmp_path / 'sum.db'}"
    command.upgrade(_config(db_url), "0071")
    engine = sa.create_engine(db_url)
    large_sum = str((1 << 65) + 7)
    with engine.begin() as connection:
        connection.execute(
            sa.text(_INSERT_SQL),
            _row(
                event_fx_candle_count=2,
                event_fx_candle_id_sum=large_sum,
                event_fx_candle_max_id=1 << 62,
                event_fx_candle_max_ts=_M_AS_OF,
            ),
        )
        stored = connection.execute(
            sa.text(f"SELECT event_fx_candle_id_sum FROM {_TABLE}")
        ).scalar_one()
    assert stored == large_sum
    engine.dispose()


@pytest.mark.parametrize(
    "overrides",
    [
        pytest.param({"mode": "shadow"}, id="bad_mode"),
        pytest.param({"schema_version": 0}, id="bad_schema_version"),
        pytest.param({"source_generation": 0}, id="bad_source_generation"),
        pytest.param({"checkpoint_minute": _M_AS_OF, "as_of": _M_BEFORE}, id="ckpt_after_as_of"),
        pytest.param({"event_fx_candle_count": -1}, id="negative_fx_count"),
        pytest.param({"event_fx_candle_max_id": -1}, id="negative_fx_max_id"),
    ],
)
def test_0071_check_constraints_reject_invalid_rows(
    tmp_path: Path, overrides: dict[str, object]
) -> None:
    """Each declared CHECK constraint rejects violating inserts at the DB level.

    Given: A database upgraded to revision 0071.
    When: A row violates mode, schema version, source generation, boundary
        ordering, candle count, or candle max id.
    Then: SQLite raises ``IntegrityError`` rather than storing the invalid row.
    """
    db_url = f"sqlite:///{tmp_path / 'checks.db'}"
    command.upgrade(_config(db_url), "0071")
    engine = sa.create_engine(db_url)
    with engine.begin() as connection, pytest.raises(IntegrityError):
        connection.execute(sa.text(_INSERT_SQL), _row(**overrides))
    engine.dispose()
