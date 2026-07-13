"""Add the venue account-truth plane (PnL Phase 3).

Two dedicated tables, DISJOINT from the order-lifecycle ``venue_events``
table and the fill-derived ``positions`` projection: venue account
snapshots are observed by a per-wallet account observer and are never
folded into a shard, never touch TradeService, and never seed recovery.

``venue_account_observations`` is an append-only log of every poll
ATTEMPT — including failures and unsupported venues — so authority is
never silently invented. ``venue_account_states`` is the SCD2 current
truth (one active row per wallet/exchange/mode) with authority-driving
fields as first-class CHECK-constrained columns: a ``simulated`` status
can only ride a paper row, an ``observed`` balance must carry its
observation timestamp, and ``valuation_status`` is ``native_only``
(Phase 3 does zero USD math). Both tables are created directly on each
dialect — SQLite batch recreation is only needed when ALTERing an
existing table. Downgrade drops both tables. Revises 0020.
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy import text
from sqlalchemy.dialects import postgresql

revision: str = "0021"
down_revision: str | None = "0020"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_KNOWN_TO_ACTIVE_PG = "known_to = '9999-12-31T23:59:59+00:00'"
_KNOWN_TO_ACTIVE_SQLITE = "known_to = '9999-12-31 23:59:59.000000'"
_CK_EXCHANGE_LOWER = "exchange = LOWER(exchange)"
_CK_MODE_LIVE_PAPER = "mode IN ('live', 'paper')"
_CK_ATTEMPT_STATUS = "attempt_status IN ('observed', 'simulated', 'unsupported', 'error')"
_CK_SYNC_STATUS = "sync_status IN ('observed', 'simulated', 'unsupported', 'error')"
_CK_BALANCE_STATUS = "balance_status IN ('observed', 'simulated', 'unsupported', 'error')"
_CK_POSITION_STATUS = "position_status IN ('observed', 'unsupported', 'not_applicable', 'error')"
_CK_VALUATION_STATUS = "valuation_status IN ('native_only')"
_CK_SIMULATED_PAPER = (
    "(sync_status != 'simulated' AND balance_status != 'simulated') OR mode = 'paper'"
)
_CK_BALANCE_OBSERVED_AT = "balance_status != 'observed' OR balance_observed_at IS NOT NULL"
_CK_OBSERVED_BALANCE = "sync_status != 'observed' OR balance_status = 'observed'"
_CK_OBSERVED_POSITION = (
    "sync_status != 'observed' OR position_status IN ('observed', 'not_applicable')"
)
_CK_OBSERVED_AUTHORITY = "sync_status != 'observed' OR authoritative_until IS NOT NULL"
_CK_OBS_SIMULATED_PAPER = (
    "(attempt_status != 'simulated' AND balance_status != 'simulated') OR mode = 'paper'"
)
_CK_OBS_OBSERVED_BALANCE = "attempt_status != 'observed' OR balance_status = 'observed'"
_CK_OBS_OBSERVED_POSITION = (
    "attempt_status != 'observed' OR position_status IN ('observed', 'not_applicable')"
)
_CK_BALANCE_JSON_PRESENT = (
    "balance_status NOT IN ('observed', 'simulated') OR balances_json IS NOT NULL"
)
_CK_POSITION_OBSERVED_PRESENT = (
    "position_status != 'observed' OR "
    "(open_positions_json IS NOT NULL AND position_observed_at IS NOT NULL)"
)
_CK_BALANCE_PAYLOAD_SOURCE = (
    "(balances_json IS NULL AND balance_payload_source_observation_id IS NULL) OR "
    "(balances_json IS NOT NULL AND balance_payload_source_observation_id IS NOT NULL)"
)
_CK_POSITION_PAYLOAD_SOURCE = (
    "(open_positions_json IS NULL AND position_payload_source_observation_id IS NULL) OR "
    "(open_positions_json IS NOT NULL AND position_payload_source_observation_id IS NOT NULL)"
)
_CK_BALANCE_FRESH_SOURCE = (
    "balance_status NOT IN ('observed', 'simulated') OR "
    "balance_payload_source_observation_id = current_attempt_observation_id"
)
_CK_POSITION_FRESH_SOURCE = (
    "position_status != 'observed' OR "
    "position_payload_source_observation_id = current_attempt_observation_id"
)


def _uuid_col() -> sa.types.TypeEngine[str]:
    """Build the per-dialect public identity column type.

    Returns:
        SQLAlchemy type using native PostgreSQL UUID and SQLite text storage.
    """
    return sa.String(36).with_variant(postgresql.UUID(as_uuid=False), "postgresql")


def upgrade() -> None:
    """Create the venue account observation and state tables with indexes.

    Returns:
        None.
    """
    op.create_table(
        "venue_account_observations",
        sa.Column("wallet_public_id", _uuid_col(), nullable=False),
        sa.Column("exchange", sa.String(32), nullable=False),
        sa.Column("mode", sa.String(8), nullable=False, server_default="live"),
        sa.Column("attempt_status", sa.String(16), nullable=False),
        sa.Column("balance_status", sa.String(16), nullable=False),
        sa.Column("position_status", sa.String(16), nullable=False),
        sa.Column("balances_json", sa.Text(), nullable=True),
        sa.Column("open_positions_json", sa.Text(), nullable=True),
        sa.Column("balance_observed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("position_observed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("error", sa.String(512), nullable=True),
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("public_id", _uuid_col(), nullable=False),
        sa.Column("session_id", _uuid_col(), nullable=False),
        sa.Column("sequence_id", sa.Integer(), nullable=False),
        sa.Column("timestamp", sa.DateTime(timezone=True), nullable=False),
        sa.Column("known_to", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.CheckConstraint(_CK_EXCHANGE_LOWER, name="ck_venue_account_obs_exchange_lower"),
        sa.CheckConstraint(_CK_MODE_LIVE_PAPER, name="ck_venue_account_obs_mode"),
        sa.CheckConstraint(_CK_ATTEMPT_STATUS, name="ck_venue_account_obs_attempt_status"),
        sa.CheckConstraint(_CK_BALANCE_STATUS, name="ck_venue_account_obs_balance_status"),
        sa.CheckConstraint(_CK_POSITION_STATUS, name="ck_venue_account_obs_position_status"),
        sa.CheckConstraint(
            _CK_BALANCE_OBSERVED_AT, name="ck_venue_account_obs_balance_observed_at"
        ),
        sa.CheckConstraint(_CK_OBS_SIMULATED_PAPER, name="ck_venue_account_obs_simulated_paper"),
        sa.CheckConstraint(_CK_OBS_OBSERVED_BALANCE, name="ck_venue_account_obs_observed_balance"),
        sa.CheckConstraint(
            _CK_OBS_OBSERVED_POSITION, name="ck_venue_account_obs_observed_position"
        ),
        sa.CheckConstraint(
            _CK_BALANCE_JSON_PRESENT, name="ck_venue_account_obs_balance_json_present"
        ),
        sa.CheckConstraint(
            _CK_POSITION_OBSERVED_PRESENT,
            name="ck_venue_account_obs_position_observed_present",
        ),
    )
    op.create_index(
        "ix_venue_account_observations_public_id",
        "venue_account_observations",
        ["public_id"],
        unique=True,
        sqlite_where=text(_KNOWN_TO_ACTIVE_SQLITE),
        postgresql_where=text(_KNOWN_TO_ACTIVE_PG),
    )
    op.create_index(
        "ix_venue_account_observations_identity",
        "venue_account_observations",
        ["wallet_public_id", "exchange", "mode"],
    )
    op.create_table(
        "venue_account_states",
        sa.Column("wallet_public_id", _uuid_col(), nullable=False),
        sa.Column("exchange", sa.String(32), nullable=False),
        sa.Column("mode", sa.String(8), nullable=False, server_default="live"),
        sa.Column("sync_status", sa.String(16), nullable=False),
        sa.Column("balance_status", sa.String(16), nullable=False),
        sa.Column("position_status", sa.String(16), nullable=False),
        sa.Column("valuation_status", sa.String(16), nullable=False, server_default="native_only"),
        sa.Column("balances_json", sa.Text(), nullable=True),
        sa.Column("open_positions_json", sa.Text(), nullable=True),
        sa.Column("balance_observed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("position_observed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("current_attempt_observation_id", sa.Integer(), nullable=False),
        sa.Column("balance_payload_source_observation_id", sa.Integer(), nullable=True),
        sa.Column("position_payload_source_observation_id", sa.Integer(), nullable=True),
        sa.Column("authoritative_until", sa.DateTime(timezone=True), nullable=True),
        sa.Column("error", sa.String(512), nullable=True),
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("public_id", _uuid_col(), nullable=False),
        sa.Column("session_id", _uuid_col(), nullable=False),
        sa.Column("sequence_id", sa.Integer(), nullable=False),
        sa.Column("timestamp", sa.DateTime(timezone=True), nullable=False),
        sa.Column("known_to", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.CheckConstraint(_CK_EXCHANGE_LOWER, name="ck_venue_account_states_exchange_lower"),
        sa.CheckConstraint(_CK_MODE_LIVE_PAPER, name="ck_venue_account_states_mode"),
        sa.CheckConstraint(_CK_SYNC_STATUS, name="ck_venue_account_states_sync_status"),
        sa.CheckConstraint(_CK_BALANCE_STATUS, name="ck_venue_account_states_balance_status"),
        sa.CheckConstraint(_CK_POSITION_STATUS, name="ck_venue_account_states_position_status"),
        sa.CheckConstraint(_CK_VALUATION_STATUS, name="ck_venue_account_states_valuation_status"),
        sa.CheckConstraint(_CK_SIMULATED_PAPER, name="ck_venue_account_states_simulated_paper"),
        sa.CheckConstraint(
            _CK_BALANCE_OBSERVED_AT, name="ck_venue_account_states_balance_observed_at"
        ),
        sa.CheckConstraint(_CK_OBSERVED_BALANCE, name="ck_venue_account_states_observed_balance"),
        sa.CheckConstraint(_CK_OBSERVED_POSITION, name="ck_venue_account_states_observed_position"),
        sa.CheckConstraint(
            _CK_OBSERVED_AUTHORITY, name="ck_venue_account_states_observed_authority"
        ),
        sa.CheckConstraint(
            _CK_BALANCE_JSON_PRESENT, name="ck_venue_account_states_balance_json_present"
        ),
        sa.CheckConstraint(
            _CK_POSITION_OBSERVED_PRESENT,
            name="ck_venue_account_states_position_observed_present",
        ),
        sa.CheckConstraint(
            _CK_BALANCE_PAYLOAD_SOURCE,
            name="ck_venue_account_states_balance_payload_source",
        ),
        sa.CheckConstraint(
            _CK_POSITION_PAYLOAD_SOURCE,
            name="ck_venue_account_states_position_payload_source",
        ),
        sa.CheckConstraint(
            _CK_BALANCE_FRESH_SOURCE,
            name="ck_venue_account_states_balance_fresh_source",
        ),
        sa.CheckConstraint(
            _CK_POSITION_FRESH_SOURCE,
            name="ck_venue_account_states_position_fresh_source",
        ),
    )
    op.create_index(
        "uq_venue_account_states_identity",
        "venue_account_states",
        ["wallet_public_id", "exchange", "mode"],
        unique=True,
        sqlite_where=text(_KNOWN_TO_ACTIVE_SQLITE),
        postgresql_where=text(_KNOWN_TO_ACTIVE_PG),
    )
    op.create_index(
        "ix_venue_account_states_public_id",
        "venue_account_states",
        ["public_id"],
        unique=True,
        sqlite_where=text(_KNOWN_TO_ACTIVE_SQLITE),
        postgresql_where=text(_KNOWN_TO_ACTIVE_PG),
    )
    op.create_index(
        "ix_venue_account_states_wallet",
        "venue_account_states",
        ["wallet_public_id"],
    )


def downgrade() -> None:
    """Drop the venue account observation and state tables.

    Returns:
        None.
    """
    op.drop_table("venue_account_states")
    op.drop_table("venue_account_observations")
