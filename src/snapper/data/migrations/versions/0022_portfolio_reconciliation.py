"""Add the portfolio reconciliation evidence plane (PnL Phase 4).

Three storage-only tables preserve every reconciliation evaluation, the
sentinel-active SCD2 current state, and the SCD2 lifecycle of sustained drift
episodes. The migration adds no runtime writer, observer, or public surface.
Both supported dialects create the same CHECK-constrained schema directly.
Revises 0021.
"""

from collections.abc import Sequence
from datetime import datetime

import sqlalchemy as sa
from alembic import op
from sqlalchemy import text
from sqlalchemy.dialects import postgresql

revision: str = "0022"
down_revision: str | None = "0021"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_KNOWN_TO_ACTIVE_PG = "known_to = '9999-12-31T23:59:59+00:00'"
_KNOWN_TO_ACTIVE_SQLITE = "known_to = '9999-12-31 23:59:59.000000'"
_DRIFT_OPEN_ACTIVE_PG = "status = 'open' AND " + _KNOWN_TO_ACTIVE_PG
_DRIFT_OPEN_ACTIVE_SQLITE = "status = 'open' AND " + _KNOWN_TO_ACTIVE_SQLITE
_CK_EXCHANGE_LOWER = "exchange = LOWER(exchange)"
_CK_MODE_LIVE = "mode = 'live'"
_CK_METHOD = "method IN ('futures_position', 'spot_execution_replay')"
_CK_EVALUATION_STATUS = (
    "evaluation_status IN ('matched', 'mismatched', 'incomplete', 'unsupported', 'error')"
)
_CK_CURRENT_STATUS = (
    "current_evaluation_status IN "
    "('matched', 'mismatched', 'incomplete', 'unsupported', 'error')"
)
_CK_LAST_OUTCOME = "last_full_outcome IS NULL OR last_full_outcome IN ('matched', 'mismatched')"
_CK_WATERMARK_PAIR = (
    "(source_watermark IS NULL AND source_watermark_kind IS NULL) OR "
    "(source_watermark IS NOT NULL AND source_watermark_kind IS NOT NULL)"
)
_CK_OBSERVATION_FULL_EVIDENCE = (
    "evaluation_status NOT IN ('matched', 'mismatched') OR "
    "(venue_account_state_public_id IS NOT NULL AND "
    "venue_account_observation_id IS NOT NULL AND "
    "account_authoritative_until IS NOT NULL AND source_watermark IS NOT NULL AND "
    "source_watermark_kind IS NOT NULL AND expected_json IS NOT NULL AND "
    "actual_json IS NOT NULL AND difference_json IS NOT NULL AND tolerance_json IS NOT NULL)"
)
_CK_OBSERVATION_MATCHED = (
    "evaluation_status != 'matched' OR "
    "(resulting_full_mismatch_count = 0 AND drift_episode_public_id IS NULL AND error IS NULL)"
)
_CK_OBSERVATION_MISMATCHED = (
    "evaluation_status != 'mismatched' OR resulting_full_mismatch_count >= 1"
)
_CK_OBSERVATION_EPISODE_THRESHOLD = (
    "(resulting_full_mismatch_count < 3 AND drift_episode_public_id IS NULL) OR "
    "(resulting_full_mismatch_count >= 3 AND drift_episode_public_id IS NOT NULL)"
)
_CK_OBSERVATION_SPOT_ANCHOR = (
    "method != 'spot_execution_replay' OR "
    "evaluation_status NOT IN ('matched', 'mismatched') OR anchor_public_id IS NOT NULL"
)
_CK_OBSERVATION_ERROR_TEXT = (
    "evaluation_status != 'error' OR (error IS NOT NULL AND LENGTH(TRIM(error)) > 0)"
)
_CK_STATE_ERROR_TEXT = (
    "current_evaluation_status != 'error' OR (error IS NOT NULL AND LENGTH(TRIM(error)) > 0)"
)
_CK_ERROR_LENGTH = "error IS NULL OR LENGTH(error) <= 512"
_CK_STATE_DETAIL = (
    "(detail_source_observation_id IS NULL AND last_full_observation_id IS NULL AND "
    "last_full_outcome IS NULL AND venue_account_state_public_id IS NULL AND "
    "venue_account_observation_id IS NULL AND anchor_public_id IS NULL AND "
    "source_watermark_kind IS NULL AND "
    "source_watermark IS NULL AND expected_json IS NULL AND actual_json IS NULL AND "
    "difference_json IS NULL AND tolerance_json IS NULL AND reconciled_at IS NULL AND "
    "authoritative_until IS NULL) OR "
    "(detail_source_observation_id IS NOT NULL AND "
    "last_full_observation_id IS NOT NULL AND "
    "detail_source_observation_id = last_full_observation_id AND "
    "last_full_outcome IS NOT NULL AND venue_account_state_public_id IS NOT NULL AND "
    "venue_account_observation_id IS NOT NULL AND source_watermark_kind IS NOT NULL AND "
    "source_watermark IS NOT NULL AND expected_json IS NOT NULL AND actual_json IS NOT NULL AND "
    "difference_json IS NOT NULL AND tolerance_json IS NOT NULL AND reconciled_at IS NOT NULL AND "
    "authoritative_until IS NOT NULL)"
)
_CK_STATE_CURRENT_FULL = (
    "current_evaluation_status NOT IN ('matched', 'mismatched') OR "
    "(last_full_observation_id IS NOT NULL AND "
    "detail_source_observation_id IS NOT NULL AND "
    "current_observation_id = last_full_observation_id AND "
    "current_observation_id = detail_source_observation_id AND "
    "last_full_outcome IS NOT NULL AND current_evaluation_status = last_full_outcome AND "
    "venue_account_state_public_id IS NOT NULL AND "
    "venue_account_observation_id IS NOT NULL AND "
    "source_watermark_kind IS NOT NULL AND source_watermark IS NOT NULL AND "
    "expected_json IS NOT NULL AND actual_json IS NOT NULL AND "
    "difference_json IS NOT NULL AND tolerance_json IS NOT NULL)"
)
_CK_STATE_MATCHED = (
    "(last_full_outcome IS NULL OR last_full_outcome != 'matched' OR "
    "(consecutive_full_mismatches = 0 AND open_drift_episode_public_id IS NULL)) AND "
    "(current_evaluation_status != 'matched' OR error IS NULL)"
)
_CK_STATE_MISMATCHED = (
    "last_full_outcome IS NULL OR last_full_outcome != 'mismatched' OR "
    "consecutive_full_mismatches >= 1"
)
_CK_STATE_NO_FULL = (
    "last_full_outcome IS NOT NULL OR "
    "(consecutive_full_mismatches = 0 AND open_drift_episode_public_id IS NULL)"
)
_CK_STATE_EPISODE = (
    "(open_drift_episode_public_id IS NULL AND "
    "(last_full_outcome IS NULL OR last_full_outcome = 'matched' OR "
    "(last_full_outcome = 'mismatched' AND consecutive_full_mismatches < 3))) OR "
    "(open_drift_episode_public_id IS NOT NULL AND last_full_outcome IS NOT NULL AND "
    "last_full_outcome = 'mismatched' AND "
    "consecutive_full_mismatches >= 3)"
)
_CK_STATE_SPOT_ANCHOR = (
    "method != 'spot_execution_replay' OR "
    "current_evaluation_status NOT IN ('matched', 'mismatched') OR anchor_public_id IS NOT NULL"
)
_CK_EPISODE_STATUS = "status IN ('open', 'resolved', 'rebased')"
_CK_EPISODE_OBSERVATION_ORDER = "last_observation_id >= trigger_observation_id"
_CK_EPISODE_DETAIL_ORDER = (
    "details_source_observation_id >= trigger_observation_id AND "
    "details_source_observation_id <= last_observation_id"
)
_CK_EPISODE_CLOSED_ORDER = "closed_at IS NULL OR closed_at >= opened_at"
_CK_EPISODE_MISMATCH_COUNT = "latest_full_mismatch_count >= 3"
_CK_EPISODE_OPEN = (
    "status != 'open' OR (closed_at IS NULL AND resolution_reason IS NULL AND "
    "closed_by_user_public_id IS NULL AND closed_by_operator_public_id IS NULL AND "
    "rebase_anchor_public_id IS NULL)"
)
_CK_EPISODE_RESOLVED = (
    "status != 'resolved' OR (closed_at IS NOT NULL AND resolution_reason IS NOT NULL AND "
    "resolution_reason = 'matched' AND "
    "closed_by_user_public_id IS NULL AND closed_by_operator_public_id IS NULL AND "
    "rebase_anchor_public_id IS NULL)"
)
_CK_EPISODE_REBASED = (
    "status != 'rebased' OR (closed_at IS NOT NULL AND "
    "resolution_reason IS NOT NULL AND resolution_reason = 'operator_rebase' AND "
    "((closed_by_user_public_id IS NOT NULL AND closed_by_operator_public_id IS NULL) OR "
    "(closed_by_user_public_id IS NULL AND closed_by_operator_public_id IS NOT NULL)) AND "
    "rebase_anchor_public_id IS NOT NULL)"
)


def _uuid_col() -> sa.types.TypeEngine[str]:
    """Build the per-dialect public identity column type.

    Returns:
        SQLAlchemy type using native PostgreSQL UUID and SQLite text storage.
    """
    return sa.String(36).with_variant(postgresql.UUID(as_uuid=False), "postgresql")


def _watermark_col() -> sa.types.TypeEngine[int]:
    """Build the per-dialect 64-bit watermark column type.

    Returns:
        Big integer storage with SQLite's integer-compatible variant.
    """
    return sa.BigInteger().with_variant(sa.Integer(), "sqlite")


def _temporal_columns() -> tuple[
    sa.Column[int],
    sa.Column[str],
    sa.Column[str],
    sa.Column[int],
    sa.Column[datetime],
    sa.Column[datetime],
]:
    """Build the standard temporal columns shared by all three tables.

    Returns:
        Fresh SQLAlchemy column objects for one table.
    """
    return (
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("public_id", _uuid_col(), nullable=False),
        sa.Column("session_id", _uuid_col(), nullable=False),
        sa.Column("sequence_id", sa.Integer(), nullable=False),
        sa.Column("timestamp", sa.DateTime(timezone=True), nullable=False),
        sa.Column("known_to", sa.DateTime(timezone=True), nullable=False),
    )


def upgrade() -> None:
    """Create reconciliation observation, state, and drift-episode tables.

    Returns:
        None.
    """
    op.create_table(
        "portfolio_reconciliation_observations",
        sa.Column("wallet_public_id", _uuid_col(), nullable=False),
        sa.Column("exchange", sa.String(32), nullable=False),
        sa.Column("mode", sa.String(8), nullable=False, server_default="live"),
        sa.Column("method", sa.String(32), nullable=False),
        sa.Column("evaluation_status", sa.String(16), nullable=False),
        sa.Column("venue_account_state_public_id", _uuid_col(), nullable=True),
        sa.Column("venue_account_observation_id", sa.Integer(), nullable=True),
        sa.Column("account_authoritative_until", sa.DateTime(timezone=True), nullable=True),
        sa.Column("source_watermark_kind", sa.String(32), nullable=True),
        sa.Column("source_watermark", _watermark_col(), nullable=True),
        sa.Column("anchor_public_id", _uuid_col(), nullable=True),
        sa.Column("expected_json", sa.Text(), nullable=True),
        sa.Column("actual_json", sa.Text(), nullable=True),
        sa.Column("difference_json", sa.Text(), nullable=True),
        sa.Column("tolerance_json", sa.Text(), nullable=True),
        sa.Column("resulting_full_mismatch_count", sa.Integer(), nullable=False),
        sa.Column("drift_episode_public_id", _uuid_col(), nullable=True),
        sa.Column("error", sa.String(512), nullable=True),
        *_temporal_columns(),
        sa.PrimaryKeyConstraint("id"),
        sa.CheckConstraint(_CK_EXCHANGE_LOWER, name="ck_portfolio_recon_obs_exchange_lower"),
        sa.CheckConstraint(_CK_MODE_LIVE, name="ck_portfolio_recon_obs_mode"),
        sa.CheckConstraint(_CK_METHOD, name="ck_portfolio_recon_obs_method"),
        sa.CheckConstraint(_CK_EVALUATION_STATUS, name="ck_portfolio_recon_obs_evaluation_status"),
        sa.CheckConstraint(
            "resulting_full_mismatch_count >= 0",
            name="ck_portfolio_recon_obs_mismatch_count",
        ),
        sa.CheckConstraint(
            _CK_OBSERVATION_FULL_EVIDENCE,
            name="ck_portfolio_recon_obs_full_evidence",
        ),
        sa.CheckConstraint(_CK_OBSERVATION_MATCHED, name="ck_portfolio_recon_obs_matched"),
        sa.CheckConstraint(_CK_OBSERVATION_MISMATCHED, name="ck_portfolio_recon_obs_mismatched"),
        sa.CheckConstraint(
            _CK_OBSERVATION_EPISODE_THRESHOLD,
            name="ck_portfolio_recon_obs_episode_threshold",
        ),
        sa.CheckConstraint(_CK_OBSERVATION_SPOT_ANCHOR, name="ck_portfolio_recon_obs_spot_anchor"),
        sa.CheckConstraint(_CK_WATERMARK_PAIR, name="ck_portfolio_recon_obs_watermark_pair"),
        sa.CheckConstraint(_CK_OBSERVATION_ERROR_TEXT, name="ck_portfolio_recon_obs_error_text"),
        sa.CheckConstraint(_CK_ERROR_LENGTH, name="ck_portfolio_recon_obs_error_length"),
    )
    op.create_index(
        "ix_portfolio_reconciliation_observations_public_id",
        "portfolio_reconciliation_observations",
        ["public_id"],
        unique=True,
        sqlite_where=text(_KNOWN_TO_ACTIVE_SQLITE),
        postgresql_where=text(_KNOWN_TO_ACTIVE_PG),
    )
    op.create_index(
        "ix_portfolio_reconciliation_observations_identity",
        "portfolio_reconciliation_observations",
        ["wallet_public_id", "exchange", "mode"],
    )
    op.create_index(
        "uq_portfolio_reconciliation_observations_evaluation",
        "portfolio_reconciliation_observations",
        ["wallet_public_id", "exchange", "mode", "session_id", "sequence_id"],
        unique=True,
    )
    op.create_table(
        "portfolio_reconciliation_states",
        sa.Column("wallet_public_id", _uuid_col(), nullable=False),
        sa.Column("exchange", sa.String(32), nullable=False),
        sa.Column("mode", sa.String(8), nullable=False, server_default="live"),
        sa.Column("method", sa.String(32), nullable=False),
        sa.Column("current_evaluation_status", sa.String(16), nullable=False),
        sa.Column("current_observation_id", sa.Integer(), nullable=False),
        sa.Column("last_full_observation_id", sa.Integer(), nullable=True),
        sa.Column("last_full_outcome", sa.String(16), nullable=True),
        sa.Column("detail_source_observation_id", sa.Integer(), nullable=True),
        sa.Column("consecutive_full_mismatches", sa.Integer(), nullable=False),
        sa.Column("open_drift_episode_public_id", _uuid_col(), nullable=True),
        sa.Column("anchor_public_id", _uuid_col(), nullable=True),
        sa.Column("venue_account_state_public_id", _uuid_col(), nullable=True),
        sa.Column("venue_account_observation_id", sa.Integer(), nullable=True),
        sa.Column("source_watermark_kind", sa.String(32), nullable=True),
        sa.Column("source_watermark", _watermark_col(), nullable=True),
        sa.Column("expected_json", sa.Text(), nullable=True),
        sa.Column("actual_json", sa.Text(), nullable=True),
        sa.Column("difference_json", sa.Text(), nullable=True),
        sa.Column("tolerance_json", sa.Text(), nullable=True),
        sa.Column("reconciled_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("authoritative_until", sa.DateTime(timezone=True), nullable=True),
        sa.Column("error", sa.String(512), nullable=True),
        *_temporal_columns(),
        sa.PrimaryKeyConstraint("id"),
        sa.CheckConstraint(_CK_EXCHANGE_LOWER, name="ck_portfolio_recon_states_exchange_lower"),
        sa.CheckConstraint(_CK_MODE_LIVE, name="ck_portfolio_recon_states_mode"),
        sa.CheckConstraint(_CK_METHOD, name="ck_portfolio_recon_states_method"),
        sa.CheckConstraint(_CK_CURRENT_STATUS, name="ck_portfolio_recon_states_current_status"),
        sa.CheckConstraint(_CK_LAST_OUTCOME, name="ck_portfolio_recon_states_last_outcome"),
        sa.CheckConstraint(
            "consecutive_full_mismatches >= 0",
            name="ck_portfolio_recon_states_mismatch_count",
        ),
        sa.CheckConstraint(_CK_STATE_DETAIL, name="ck_portfolio_recon_states_detail"),
        sa.CheckConstraint(_CK_STATE_CURRENT_FULL, name="ck_portfolio_recon_states_current_full"),
        sa.CheckConstraint(_CK_STATE_MATCHED, name="ck_portfolio_recon_states_matched"),
        sa.CheckConstraint(_CK_STATE_MISMATCHED, name="ck_portfolio_recon_states_mismatched"),
        sa.CheckConstraint(_CK_STATE_NO_FULL, name="ck_portfolio_recon_states_no_full"),
        sa.CheckConstraint(_CK_STATE_EPISODE, name="ck_portfolio_recon_states_episode"),
        sa.CheckConstraint(_CK_STATE_SPOT_ANCHOR, name="ck_portfolio_recon_states_spot_anchor"),
        sa.CheckConstraint(_CK_STATE_ERROR_TEXT, name="ck_portfolio_recon_states_error_text"),
        sa.CheckConstraint(_CK_ERROR_LENGTH, name="ck_portfolio_recon_states_error_length"),
    )
    op.create_index(
        "uq_portfolio_reconciliation_states_identity",
        "portfolio_reconciliation_states",
        ["wallet_public_id", "exchange", "mode"],
        unique=True,
        sqlite_where=text(_KNOWN_TO_ACTIVE_SQLITE),
        postgresql_where=text(_KNOWN_TO_ACTIVE_PG),
    )
    op.create_index(
        "ix_portfolio_reconciliation_states_public_id",
        "portfolio_reconciliation_states",
        ["public_id"],
        unique=True,
        sqlite_where=text(_KNOWN_TO_ACTIVE_SQLITE),
        postgresql_where=text(_KNOWN_TO_ACTIVE_PG),
    )
    op.create_index(
        "ix_portfolio_reconciliation_states_wallet",
        "portfolio_reconciliation_states",
        ["wallet_public_id"],
    )
    op.create_table(
        "portfolio_drift_episodes",
        sa.Column("wallet_public_id", _uuid_col(), nullable=False),
        sa.Column("exchange", sa.String(32), nullable=False),
        sa.Column("mode", sa.String(8), nullable=False, server_default="live"),
        sa.Column("status", sa.String(16), nullable=False),
        sa.Column("opened_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("closed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("trigger_observation_id", sa.Integer(), nullable=False),
        sa.Column("last_observation_id", sa.Integer(), nullable=False),
        sa.Column("details_source_observation_id", sa.Integer(), nullable=False),
        sa.Column("latest_full_mismatch_count", sa.Integer(), nullable=False),
        sa.Column("resolution_reason", sa.String(32), nullable=True),
        sa.Column("closed_by_user_public_id", _uuid_col(), nullable=True),
        sa.Column("closed_by_operator_public_id", _uuid_col(), nullable=True),
        sa.Column("rebase_anchor_public_id", _uuid_col(), nullable=True),
        *_temporal_columns(),
        sa.PrimaryKeyConstraint("id"),
        sa.CheckConstraint(_CK_EXCHANGE_LOWER, name="ck_portfolio_drift_exchange_lower"),
        sa.CheckConstraint(_CK_MODE_LIVE, name="ck_portfolio_drift_mode"),
        sa.CheckConstraint(_CK_EPISODE_STATUS, name="ck_portfolio_drift_status"),
        sa.CheckConstraint(
            _CK_EPISODE_OBSERVATION_ORDER,
            name="ck_portfolio_drift_observation_order",
        ),
        sa.CheckConstraint(_CK_EPISODE_DETAIL_ORDER, name="ck_portfolio_drift_detail_order"),
        sa.CheckConstraint(_CK_EPISODE_CLOSED_ORDER, name="ck_portfolio_drift_closed_order"),
        sa.CheckConstraint(_CK_EPISODE_MISMATCH_COUNT, name="ck_portfolio_drift_mismatch_count"),
        sa.CheckConstraint(_CK_EPISODE_OPEN, name="ck_portfolio_drift_open"),
        sa.CheckConstraint(_CK_EPISODE_RESOLVED, name="ck_portfolio_drift_resolved"),
        sa.CheckConstraint(_CK_EPISODE_REBASED, name="ck_portfolio_drift_rebased"),
    )
    op.create_index(
        "uq_portfolio_drift_episodes_open_identity",
        "portfolio_drift_episodes",
        ["wallet_public_id", "exchange", "mode"],
        unique=True,
        sqlite_where=text(_DRIFT_OPEN_ACTIVE_SQLITE),
        postgresql_where=text(_DRIFT_OPEN_ACTIVE_PG),
    )
    op.create_index(
        "ix_portfolio_drift_episodes_public_id",
        "portfolio_drift_episodes",
        ["public_id"],
        unique=True,
        sqlite_where=text(_KNOWN_TO_ACTIVE_SQLITE),
        postgresql_where=text(_KNOWN_TO_ACTIVE_PG),
    )
    op.create_index(
        "ix_portfolio_drift_episodes_status_opened",
        "portfolio_drift_episodes",
        ["status", "opened_at"],
    )


def downgrade() -> None:
    """Drop the reconciliation storage plane.

    Returns:
        None.
    """
    op.drop_table("portfolio_drift_episodes")
    op.drop_table("portfolio_reconciliation_states")
    op.drop_table("portfolio_reconciliation_observations")
