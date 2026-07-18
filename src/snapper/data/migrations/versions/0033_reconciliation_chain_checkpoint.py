"""Add the matched-verdict chain-tip checkpoint to the reconciliation planes.

S4c-4a (plan ``map_2026_07_18_s4c4_build_plan_v2.md`` §3, Open Decision 4): the
spot bundle's anti-rollback clause needs a durable ``(W_prev, H_prev)``
checkpoint per matched verdict — without it the ``H_anchor``-rooted fold
verifies row integrity but cannot detect tail deletion between cycles. This
adds a NULLABLE ``source_chain_tip`` (64-lowercase-hex, format CHECK only) to
the append-only reconciliation observations (audit) and the SCD2 state (fast
checkpoint read beside ``source_watermark``). Purely additive: no backfill (a
NULL tip is the honest pre-0033 value and every existing row — including the
populated kraken_futures reconciliation planes — passes the nullable-tolerant
CHECK), no data validation, and downgrade simply drops the columns (a
checkpoint is derived evidence, re-established by the next matched verdict).
Semantic gating (tip present only on full spot outcomes) is writer-owned, not
schema-owned, so the futures plane never has to satisfy a spot-shaped CHECK.
"""

import sqlalchemy as sa
from alembic import op

revision: str = "0033"
down_revision: str | None = "0032"
branch_labels: str | None = None
depends_on: str | None = None

_CK_CHAIN_TIP = (
    "source_chain_tip IS NULL OR ("
    "LENGTH(source_chain_tip) = 64 AND "
    "source_chain_tip = LOWER(source_chain_tip) AND "
    "source_chain_tip = TRIM(source_chain_tip))"
)
_TABLES = (
    ("portfolio_reconciliation_observations", "ck_portfolio_recon_obs_chain_tip"),
    ("portfolio_reconciliation_states", "ck_portfolio_recon_states_chain_tip"),
)


def upgrade() -> None:
    """Add the nullable chain-tip column and its format CHECK to both planes."""
    for table_name, check_name in _TABLES:
        with op.batch_alter_table(table_name) as batch:
            batch.add_column(sa.Column("source_chain_tip", sa.String(64), nullable=True))
            batch.create_check_constraint(check_name, _CK_CHAIN_TIP)


def downgrade() -> None:
    """Drop the chain-tip CHECK and column from both planes."""
    for table_name, check_name in reversed(_TABLES):
        with op.batch_alter_table(table_name) as batch:
            batch.drop_constraint(check_name, type_="check")
            batch.drop_column("source_chain_tip")
