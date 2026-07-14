"""Tests for the dual-dialect spot execution-replay migration."""

import importlib
from io import StringIO
from pathlib import Path
from unittest.mock import patch

import sqlalchemy as sa
from alembic import command
from alembic.config import Config
from alembic.migration import MigrationContext
from alembic.operations import Operations

from snapper.data.models import Execution
from snapper.data.models import PortfolioSpotReconciliationAnchor
from snapper.data.models import TZDateTime
from snapper.data.models import UUIDColumn

ALEMBIC_INI = Path(__file__).resolve().parents[2] / "alembic.ini"
_ACTIVE = "9999-12-31 23:59:59.000000"
_TS = "2026-07-14 08:00:00.000000"


def _config(db_url: str) -> Config:
    """Build Alembic configuration for one throwaway database."""
    config = Config(str(ALEMBIC_INI))
    config.set_main_option("sqlalchemy.url", db_url)
    return config


def test_0025_sqlite_upgrade_downgrade_reupgrade_preserves_exact_text(
    tmp_path: Path,
) -> None:
    """SQLite retains exact decimals as text instead of NUMERIC-affinity floats.

    PostgreSQL remains a separate verification concern because SQLite cannot
    expose native UUID, BIGINT, TIMESTAMPTZ, or NUMERIC driver differences.
    """
    db_url = f"sqlite:///{tmp_path / 'spot-replay.db'}"
    config = _config(db_url)
    command.upgrade(config, "0024")
    engine = sa.create_engine(db_url)
    with engine.begin() as connection:
        connection.execute(
            sa.text(
                "INSERT INTO executions "
                "(order_public_id, wallet_public_id, side, status, price, size, fee, fee_asset, "
                "session_id, sequence_id, timestamp, known_to, public_id) VALUES "
                "('order-1', 'wallet-1', 'buy', 'filled', 1.25, 2.5, 0.1, 'USD', "
                "'session-1', 1, :timestamp, :known_to, 'execution-1')"
            ),
            {"timestamp": _TS, "known_to": _ACTIVE},
        )
    command.upgrade(config, "0025")
    inspector = sa.inspect(engine)
    anchor_columns = {
        column["name"]: column
        for column in inspector.get_columns("portfolio_spot_reconciliation_anchors")
    }
    assert isinstance(anchor_columns["balances_json"]["type"], sa.Text)
    assert isinstance(anchor_columns["source_watermark"]["type"], sa.Integer)
    assert {
        item["name"] for item in inspector.get_indexes("portfolio_spot_reconciliation_anchors")
    } == {
        "ix_portfolio_spot_reconciliation_anchors_public_id",
        "uq_portfolio_spot_reconciliation_anchors_identity",
    }
    assert {
        item["name"]
        for item in inspector.get_check_constraints("portfolio_spot_reconciliation_anchors")
    } == {
        "ck_portfolio_spot_anchor_boundary_status",
        "ck_portfolio_spot_anchor_evidence_text",
        "ck_portfolio_spot_anchor_exchange_lower",
        "ck_portfolio_spot_anchor_inventory_status",
        "ck_portfolio_spot_anchor_margin_status",
        "ck_portfolio_spot_anchor_mode",
        "ck_portfolio_spot_anchor_observation",
        "ck_portfolio_spot_anchor_timestamp_order",
        "ck_portfolio_spot_anchor_watermark",
    }
    execution_columns = {column["name"] for column in inspector.get_columns("executions")}
    assert {"price_decimal", "size_decimal", "fee_decimal", "numeric_provenance"} <= (
        execution_columns
    )
    execution_checks = {item["name"] for item in inspector.get_check_constraints("executions")}
    assert "ck_executions_numeric_provenance" in execution_checks
    assert "ck_executions_raw_decimals" in execution_checks
    exact = '{"BTC":"0.100000000000000005","USD":"123456789012345678.123456789012345678"}'
    with engine.begin() as connection:
        connection.execute(
            sa.text(
                "INSERT INTO portfolio_spot_reconciliation_anchors "
                "(wallet_public_id, exchange, mode, venue_account_state_public_id, "
                "balance_observation_id, source_watermark_kind, source_watermark, balances_json, "
                "first_request_started_at, first_request_completed_at, second_request_started_at, "
                "second_request_completed_at, boundary_status, inventory_status, margin_status, "
                "provenance, public_id, session_id, sequence_id, timestamp, known_to) VALUES "
                "('wallet-1', 'kraken', 'live', 'state-1', 1, 'execution_id', 2147483649, "
                ":balances, :timestamp, :timestamp, :timestamp, :timestamp, "
                "'double_read_equal', 'certified_full', 'cash', 'test', 'anchor-1', "
                "'session-1', 1, :timestamp, :known_to)"
            ),
            {"balances": exact, "timestamp": _TS, "known_to": _ACTIVE},
        )
        stored = connection.execute(
            sa.text(
                "SELECT balances_json, source_watermark FROM "
                "portfolio_spot_reconciliation_anchors WHERE public_id = 'anchor-1'"
            )
        ).one()
        legacy = connection.execute(
            sa.text(
                "SELECT price_decimal, size_decimal, fee_decimal, numeric_provenance "
                "FROM executions WHERE public_id = 'execution-1'"
            )
        ).one()
    assert stored == (exact, 2_147_483_649)
    assert legacy == (None, None, None, None)
    command.downgrade(config, "0024")
    assert "portfolio_spot_reconciliation_anchors" not in sa.inspect(engine).get_table_names()
    assert "price_decimal" not in {
        column["name"] for column in sa.inspect(engine).get_columns("executions")
    }
    command.upgrade(config, "0025")
    assert "portfolio_spot_reconciliation_anchors" in sa.inspect(engine).get_table_names()
    engine.dispose()


def test_0025_postgresql_compile_and_model_signature() -> None:
    """PostgreSQL offline DDL exposes native types and partial-index predicates."""
    migration = importlib.import_module(
        "snapper.data.migrations.versions.0025_spot_execution_replay"
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
    assert migration.revision == "0025"
    assert migration.down_revision == "0024"
    assert "CREATE TABLE portfolio_spot_reconciliation_anchors" in ddl
    assert "wallet_public_id UUID NOT NULL" in ddl
    assert "source_watermark BIGINT NOT NULL" in ddl
    assert "TIMESTAMP WITH TIME ZONE NOT NULL" in ddl
    assert "balances_json TEXT NOT NULL" in ddl
    assert "WHERE known_to = '9999-12-31T23:59:59+00:00'" in ddl
    assert "price_decimal TEXT" in ddl
    assert "ck_executions_numeric_provenance" in ddl
    table = PortfolioSpotReconciliationAnchor.__table__
    assert isinstance(table.c.wallet_public_id.type, UUIDColumn)
    assert isinstance(table.c.source_watermark.type, sa.BigInteger)
    assert isinstance(table.c.first_request_started_at.type, TZDateTime)
    assert isinstance(table.c.balances_json.type, sa.Text)
    assert isinstance(Execution.__table__.c.price_decimal.type, sa.Text)
