"""Add backtest_comparisons table (Phase 2c Step 4).

Persists a comparison request (auto or manual pairing of two
terminal backtest runs). The diff payload is recomputed on GET
from the current artifact rows so metric-schema changes never
stale the diff.

Pair normalisation: handler writes (min, max) by lexical
``public_id`` before INSERT so (A,B) and (B,A) collapse to the
same row. Partial unique index on the ordered pair prevents
active duplicates under race.

SQLite uses ``op.batch_alter_table`` only for column-level ops;
new-table creation goes through ``op.create_table`` + followups.
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0007"
down_revision: str | None = "0006"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Create backtest_comparisons with temporal + pair indexes."""
    op.create_table(
        "backtest_comparisons",
        sa.Column("id", sa.Integer, primary_key=True, autoincrement=True),
        sa.Column("public_id", sa.String(36), nullable=False),
        sa.Column("session_id", sa.String(36), nullable=False),
        sa.Column("sequence_id", sa.Integer, nullable=False),
        sa.Column("timestamp", sa.DateTime(timezone=True), nullable=False),
        sa.Column("known_to", sa.DateTime(timezone=True), nullable=False),
        sa.Column("wallet_public_id", sa.String(36), nullable=False),
        sa.Column("operator_public_id", sa.String(36), nullable=True),
        sa.Column("created_by_user_id", sa.String(64), nullable=True),
        sa.Column("run_a_public_id", sa.String(36), nullable=False),
        sa.Column("run_b_public_id", sa.String(36), nullable=False),
        sa.Column("config_hash", sa.String(64), nullable=True),
        sa.Column("pairing_mode", sa.String(16), nullable=False),
        sa.Column("anchor_run_public_id", sa.String(36), nullable=True),
    )
    op.create_index(
        "ix_bc_wallet_hash_time",
        "backtest_comparisons",
        ["wallet_public_id", "config_hash", "timestamp"],
        unique=False,
    )
    op.create_index(
        "ix_bc_public_id",
        "backtest_comparisons",
        ["public_id"],
        unique=False,
    )


def downgrade() -> None:
    """Drop the table + indexes cascade."""
    op.drop_table("backtest_comparisons")
