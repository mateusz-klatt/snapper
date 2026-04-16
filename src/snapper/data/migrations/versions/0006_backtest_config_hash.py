"""Add config_hash column + index to backtest_runs (Phase 2c Step 3).

Enables auto-pair comparison: runs with the same pairing-stable
``compute_fingerprint(config, for_pairing=True)`` digest share a
``config_hash`` value and can be grouped/paired for side-by-side
analysis.

Column is nullable so pre-0006 rows are valid (NULL). The route
layer's list-by-config-hash lookup skips NULL hashes; the comparison
endpoint rejects auto-pair on anchors lacking the hash. Only new
runs get hashed.

SQLite uses ``op.batch_alter_table`` to stay portable.
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0006"
down_revision: str | None = "0005"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Add config_hash column (nullable) + wallet+hash+timestamp index."""
    with op.batch_alter_table("backtest_runs") as batch_op:
        batch_op.add_column(sa.Column("config_hash", sa.String(64), nullable=True))
        batch_op.create_index(
            "ix_backtest_runs_config_hash",
            ["wallet_public_id", "config_hash", "timestamp"],
            unique=False,
        )


def downgrade() -> None:
    """Drop index + column in reverse order."""
    with op.batch_alter_table("backtest_runs") as batch_op:
        batch_op.drop_index("ix_backtest_runs_config_hash")
        batch_op.drop_column("config_hash")
