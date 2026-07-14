"""Add durable reconciliation-method configuration and sentinel evidence checks.

The operator-authored configuration plane is an SCD2 table containing only
real reconciliation methods. Reconciliation evidence separately gains the
explicit ``unclassified`` sentinel while preserving every existing row,
column, and index. No account is classified or backfilled by this migration.
Revises 0025.
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy import text
from sqlalchemy.dialects import postgresql

revision: str = "0026"
down_revision: str | None = "0025"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_KNOWN_TO_ACTIVE_PG = "known_to = '9999-12-31T23:59:59+00:00'"
_KNOWN_TO_ACTIVE_SQLITE = "known_to = '9999-12-31 23:59:59.000000'"
_CK_METHOD_0025 = "method IN ('futures_position', 'spot_execution_replay')"
_CK_OBSERVATION_SPOT_ANCHOR_0025 = (
    "method != 'spot_execution_replay' OR "
    "evaluation_status NOT IN ('matched', 'mismatched') OR anchor_public_id IS NOT NULL"
)
_CK_STATE_SPOT_ANCHOR_0025 = (
    "method != 'spot_execution_replay' OR "
    "current_evaluation_status NOT IN ('matched', 'mismatched') OR "
    "anchor_public_id IS NOT NULL"
)
_CK_METHOD = (
    "method IS NOT NULL AND method IN "
    "('futures_position', 'spot_execution_replay', 'margin_ledger_replay', 'unclassified')"
)
_CK_OBSERVATION_METHOD_STATUS = (
    "method IS NOT NULL AND evaluation_status IS NOT NULL AND ("
    "(method IN ('futures_position', 'spot_execution_replay') AND "
    "evaluation_status IN ('matched', 'mismatched', 'incomplete', 'unsupported', 'error')) OR "
    "(method = 'margin_ledger_replay' AND evaluation_status = 'error') OR "
    "(method = 'unclassified' AND evaluation_status IN ('incomplete', 'error')))"
)
_CK_STATE_METHOD_STATUS = (
    "method IS NOT NULL AND current_evaluation_status IS NOT NULL AND ("
    "(method IN ('futures_position', 'spot_execution_replay') AND "
    "current_evaluation_status IN "
    "('matched', 'mismatched', 'incomplete', 'unsupported', 'error')) OR "
    "(method = 'margin_ledger_replay' AND current_evaluation_status = 'error') OR "
    "(method = 'unclassified' AND current_evaluation_status IN ('incomplete', 'error')))"
)
_CK_OBSERVATION_SPOT_ANCHOR = (
    "method IS NOT NULL AND evaluation_status IS NOT NULL AND ("
    "method IN ('futures_position', 'margin_ledger_replay', 'unclassified') OR "
    "(method = 'spot_execution_replay' AND ("
    "evaluation_status IN ('incomplete', 'unsupported', 'error') OR "
    "(evaluation_status IN ('matched', 'mismatched') AND anchor_public_id IS NOT NULL))))"
)
_CK_STATE_SPOT_ANCHOR = (
    "method IS NOT NULL AND current_evaluation_status IS NOT NULL AND ("
    "method IN ('futures_position', 'margin_ledger_replay', 'unclassified') OR "
    "(method = 'spot_execution_replay' AND ("
    "current_evaluation_status IN ('incomplete', 'unsupported', 'error') OR "
    "(current_evaluation_status IN ('matched', 'mismatched') AND "
    "anchor_public_id IS NOT NULL))))"
)
_CK_OBSERVATION_NONFULL_METHOD_EVIDENCE = (
    "method IS NOT NULL AND (method IN ('futures_position', 'spot_execution_replay') OR ("
    "method IN ('margin_ledger_replay', 'unclassified') AND "
    "venue_account_state_public_id IS NULL AND venue_account_observation_id IS NULL AND "
    "account_authoritative_until IS NULL AND source_watermark_kind IS NULL AND "
    "source_watermark IS NULL AND anchor_public_id IS NULL AND expected_json IS NULL AND "
    "actual_json IS NULL AND difference_json IS NULL AND tolerance_json IS NULL AND "
    "resulting_full_mismatch_count = 0 AND drift_episode_public_id IS NULL))"
)
_CK_STATE_NONFULL_METHOD_EVIDENCE = (
    "method IS NOT NULL AND (method IN ('futures_position', 'spot_execution_replay') OR ("
    "method IN ('margin_ledger_replay', 'unclassified') AND "
    "last_full_observation_id IS NULL AND last_full_outcome IS NULL AND "
    "detail_source_observation_id IS NULL AND consecutive_full_mismatches = 0 AND "
    "open_drift_episode_public_id IS NULL AND anchor_public_id IS NULL AND "
    "venue_account_state_public_id IS NULL AND venue_account_observation_id IS NULL AND "
    "source_watermark_kind IS NULL AND source_watermark IS NULL AND expected_json IS NULL AND "
    "actual_json IS NULL AND difference_json IS NULL AND tolerance_json IS NULL AND "
    "reconciled_at IS NULL AND authoritative_until IS NULL))"
)


def _uuid_col() -> sa.types.TypeEngine[str]:
    """Build native PostgreSQL UUID with SQLite text fallback."""
    return sa.String(36).with_variant(postgresql.UUID(as_uuid=False), "postgresql")


def _is_sqlite() -> bool:
    """Return whether the migration is executing against SQLite."""
    return op.get_bind().dialect.name == "sqlite"


def _create_method_config_table() -> None:
    """Create the empty operator-authored reconciliation-method config plane."""
    op.create_table(
        "portfolio_reconciliation_method_configs",
        sa.Column("wallet_public_id", _uuid_col(), nullable=False),
        sa.Column("exchange", sa.String(32), nullable=False),
        sa.Column("mode", sa.String(8), nullable=False, server_default="live"),
        sa.Column("method", sa.String(32), nullable=False),
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("public_id", _uuid_col(), nullable=False),
        sa.Column("session_id", _uuid_col(), nullable=False),
        sa.Column("sequence_id", sa.Integer(), nullable=False),
        sa.Column("timestamp", sa.DateTime(timezone=True), nullable=False),
        sa.Column("known_to", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.CheckConstraint(
            "exchange IS NOT NULL AND LENGTH(TRIM(exchange)) > 0 AND exchange = LOWER(exchange)",
            name="ck_portfolio_recon_method_configs_exchange_lower",
        ),
        sa.CheckConstraint(
            "mode IS NOT NULL AND mode = 'live'",
            name="ck_portfolio_recon_method_configs_mode",
        ),
        sa.CheckConstraint(
            "method IS NOT NULL AND method IN "
            "('futures_position', 'spot_execution_replay', 'margin_ledger_replay')",
            name="ck_portfolio_recon_method_configs_method",
        ),
        sa.CheckConstraint(
            "timestamp IS NOT NULL AND known_to IS NOT NULL AND known_to >= timestamp",
            name="ck_portfolio_recon_method_configs_temporal",
        ),
    )
    op.create_index(
        "uq_portfolio_reconciliation_method_configs_identity",
        "portfolio_reconciliation_method_configs",
        ["wallet_public_id", "exchange", "mode"],
        unique=True,
        sqlite_where=text(_KNOWN_TO_ACTIVE_SQLITE),
        postgresql_where=text(_KNOWN_TO_ACTIVE_PG),
    )
    op.create_index(
        "ix_portfolio_reconciliation_method_configs_public_id",
        "portfolio_reconciliation_method_configs",
        ["public_id"],
        unique=True,
        sqlite_where=text(_KNOWN_TO_ACTIVE_SQLITE),
        postgresql_where=text(_KNOWN_TO_ACTIVE_PG),
    )
    op.create_index(
        "ix_portfolio_reconciliation_method_configs_wallet",
        "portfolio_reconciliation_method_configs",
        ["wallet_public_id"],
    )


def _upgrade_observation_constraints() -> None:
    """Install expanded observation method constraints without changing rows."""
    if _is_sqlite():
        with op.batch_alter_table(
            "portfolio_reconciliation_observations", recreate="always"
        ) as batch:
            batch.drop_constraint("ck_portfolio_recon_obs_method", type_="check")
            batch.drop_constraint("ck_portfolio_recon_obs_spot_anchor", type_="check")
            batch.create_check_constraint("ck_portfolio_recon_obs_method", _CK_METHOD)
            batch.create_check_constraint(
                "ck_portfolio_recon_obs_method_status", _CK_OBSERVATION_METHOD_STATUS
            )
            batch.create_check_constraint(
                "ck_portfolio_recon_obs_spot_anchor", _CK_OBSERVATION_SPOT_ANCHOR
            )
            batch.create_check_constraint(
                "ck_portfolio_recon_obs_nonfull_method_evidence",
                _CK_OBSERVATION_NONFULL_METHOD_EVIDENCE,
            )
        return
    op.drop_constraint(
        "ck_portfolio_recon_obs_method",
        "portfolio_reconciliation_observations",
        type_="check",
    )
    op.drop_constraint(
        "ck_portfolio_recon_obs_spot_anchor",
        "portfolio_reconciliation_observations",
        type_="check",
    )
    op.create_check_constraint(
        "ck_portfolio_recon_obs_method",
        "portfolio_reconciliation_observations",
        _CK_METHOD,
    )
    op.create_check_constraint(
        "ck_portfolio_recon_obs_method_status",
        "portfolio_reconciliation_observations",
        _CK_OBSERVATION_METHOD_STATUS,
    )
    op.create_check_constraint(
        "ck_portfolio_recon_obs_spot_anchor",
        "portfolio_reconciliation_observations",
        _CK_OBSERVATION_SPOT_ANCHOR,
    )
    op.create_check_constraint(
        "ck_portfolio_recon_obs_nonfull_method_evidence",
        "portfolio_reconciliation_observations",
        _CK_OBSERVATION_NONFULL_METHOD_EVIDENCE,
    )


def _upgrade_state_constraints() -> None:
    """Install expanded state method constraints without changing rows."""
    if _is_sqlite():
        with op.batch_alter_table("portfolio_reconciliation_states", recreate="always") as batch:
            batch.drop_constraint("ck_portfolio_recon_states_method", type_="check")
            batch.drop_constraint("ck_portfolio_recon_states_spot_anchor", type_="check")
            batch.create_check_constraint("ck_portfolio_recon_states_method", _CK_METHOD)
            batch.create_check_constraint(
                "ck_portfolio_recon_states_method_status", _CK_STATE_METHOD_STATUS
            )
            batch.create_check_constraint(
                "ck_portfolio_recon_states_spot_anchor", _CK_STATE_SPOT_ANCHOR
            )
            batch.create_check_constraint(
                "ck_portfolio_recon_states_nonfull_method_evidence",
                _CK_STATE_NONFULL_METHOD_EVIDENCE,
            )
        return
    op.drop_constraint(
        "ck_portfolio_recon_states_method",
        "portfolio_reconciliation_states",
        type_="check",
    )
    op.drop_constraint(
        "ck_portfolio_recon_states_spot_anchor",
        "portfolio_reconciliation_states",
        type_="check",
    )
    op.create_check_constraint(
        "ck_portfolio_recon_states_method",
        "portfolio_reconciliation_states",
        _CK_METHOD,
    )
    op.create_check_constraint(
        "ck_portfolio_recon_states_method_status",
        "portfolio_reconciliation_states",
        _CK_STATE_METHOD_STATUS,
    )
    op.create_check_constraint(
        "ck_portfolio_recon_states_spot_anchor",
        "portfolio_reconciliation_states",
        _CK_STATE_SPOT_ANCHOR,
    )
    op.create_check_constraint(
        "ck_portfolio_recon_states_nonfull_method_evidence",
        "portfolio_reconciliation_states",
        _CK_STATE_NONFULL_METHOD_EVIDENCE,
    )


def _ensure_downgrade_history_compatible() -> None:
    """Abort before schema mutation when 0025 cannot represent stored history."""
    if op.get_context().as_sql:
        raise RuntimeError(
            "Downgrade from 0026 requires an online connection to verify reconciliation history"
        )
    bind = op.get_bind()
    observation = bind.execute(
        sa.text(
            "SELECT 1 FROM portfolio_reconciliation_observations "
            "WHERE method IN ('margin_ledger_replay', 'unclassified') LIMIT 1"
        )
    ).first()
    if observation is not None:
        raise RuntimeError(
            "Downgrade from 0026 refused because observation history uses a new method"
        )
    state = bind.execute(
        sa.text(
            "SELECT 1 FROM portfolio_reconciliation_states "
            "WHERE method IN ('margin_ledger_replay', 'unclassified') LIMIT 1"
        )
    ).first()
    if state is not None:
        raise RuntimeError("Downgrade from 0026 refused because state history uses a new method")


def _downgrade_observation_constraints() -> None:
    """Restore the 0025 observation method constraints."""
    if _is_sqlite():
        with op.batch_alter_table(
            "portfolio_reconciliation_observations", recreate="always"
        ) as batch:
            batch.drop_constraint("ck_portfolio_recon_obs_nonfull_method_evidence", type_="check")
            batch.drop_constraint("ck_portfolio_recon_obs_spot_anchor", type_="check")
            batch.drop_constraint("ck_portfolio_recon_obs_method_status", type_="check")
            batch.drop_constraint("ck_portfolio_recon_obs_method", type_="check")
            batch.create_check_constraint("ck_portfolio_recon_obs_method", _CK_METHOD_0025)
            batch.create_check_constraint(
                "ck_portfolio_recon_obs_spot_anchor", _CK_OBSERVATION_SPOT_ANCHOR_0025
            )
        return
    op.drop_constraint(
        "ck_portfolio_recon_obs_nonfull_method_evidence",
        "portfolio_reconciliation_observations",
        type_="check",
    )
    op.drop_constraint(
        "ck_portfolio_recon_obs_spot_anchor",
        "portfolio_reconciliation_observations",
        type_="check",
    )
    op.drop_constraint(
        "ck_portfolio_recon_obs_method_status",
        "portfolio_reconciliation_observations",
        type_="check",
    )
    op.drop_constraint(
        "ck_portfolio_recon_obs_method",
        "portfolio_reconciliation_observations",
        type_="check",
    )
    op.create_check_constraint(
        "ck_portfolio_recon_obs_method",
        "portfolio_reconciliation_observations",
        _CK_METHOD_0025,
    )
    op.create_check_constraint(
        "ck_portfolio_recon_obs_spot_anchor",
        "portfolio_reconciliation_observations",
        _CK_OBSERVATION_SPOT_ANCHOR_0025,
    )


def _downgrade_state_constraints() -> None:
    """Restore the 0025 state method constraints."""
    if _is_sqlite():
        with op.batch_alter_table("portfolio_reconciliation_states", recreate="always") as batch:
            batch.drop_constraint(
                "ck_portfolio_recon_states_nonfull_method_evidence", type_="check"
            )
            batch.drop_constraint("ck_portfolio_recon_states_spot_anchor", type_="check")
            batch.drop_constraint("ck_portfolio_recon_states_method_status", type_="check")
            batch.drop_constraint("ck_portfolio_recon_states_method", type_="check")
            batch.create_check_constraint("ck_portfolio_recon_states_method", _CK_METHOD_0025)
            batch.create_check_constraint(
                "ck_portfolio_recon_states_spot_anchor", _CK_STATE_SPOT_ANCHOR_0025
            )
        return
    op.drop_constraint(
        "ck_portfolio_recon_states_nonfull_method_evidence",
        "portfolio_reconciliation_states",
        type_="check",
    )
    op.drop_constraint(
        "ck_portfolio_recon_states_spot_anchor",
        "portfolio_reconciliation_states",
        type_="check",
    )
    op.drop_constraint(
        "ck_portfolio_recon_states_method_status",
        "portfolio_reconciliation_states",
        type_="check",
    )
    op.drop_constraint(
        "ck_portfolio_recon_states_method",
        "portfolio_reconciliation_states",
        type_="check",
    )
    op.create_check_constraint(
        "ck_portfolio_recon_states_method",
        "portfolio_reconciliation_states",
        _CK_METHOD_0025,
    )
    op.create_check_constraint(
        "ck_portfolio_recon_states_spot_anchor",
        "portfolio_reconciliation_states",
        _CK_STATE_SPOT_ANCHOR_0025,
    )


def upgrade() -> None:
    """Create method configuration and expand reconciliation evidence checks."""
    _create_method_config_table()
    _upgrade_observation_constraints()
    _upgrade_state_constraints()


def downgrade() -> None:
    """Restore 0025 only when every stored method remains representable."""
    _ensure_downgrade_history_compatible()
    _downgrade_observation_constraints()
    _downgrade_state_constraints()
    op.drop_table("portfolio_reconciliation_method_configs")
