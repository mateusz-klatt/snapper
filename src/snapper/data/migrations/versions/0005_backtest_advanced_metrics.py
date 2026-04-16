"""Add 8 advanced-metric columns to backtest_results (Phase 2c Step 1).

Promotes 5 metrics previously kept as JSON blobs inside ``extra_metrics``
(``sortino_ratio``, ``cagr``, ``calmar_ratio``, ``expectancy``,
``avg_trade_pnl``) to typed nullable float columns, and adds 3 new
metrics that did not previously exist anywhere (``max_drawdown_duration_seconds``,
``exposure_ratio``, ``turnover_ratio``). All 8 columns are ``nullable=True``
with no server default so pre-migration rows keep ``NULL`` — no backfill.
The ``extra_metrics`` JSON column stays in place for forward-compatibility
(future non-promoted metrics can still live there) but the runner stops
writing the 5 promoted keys to it.

SQLite needs ``op.batch_alter_table`` for ADD COLUMN-sans-default to stay
portable; the same path also works on Postgres.
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0005"
down_revision: str | None = "0004"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


_NEW_COLUMNS: tuple[str, ...] = (
    "sortino_ratio",
    "cagr",
    "calmar_ratio",
    "expectancy",
    "avg_trade_pnl",
    "max_drawdown_duration_seconds",
    "exposure_ratio",
    "turnover_ratio",
)


def upgrade() -> None:
    """Add 8 nullable float columns to backtest_results."""
    with op.batch_alter_table("backtest_results") as batch_op:
        for name in _NEW_COLUMNS:
            batch_op.add_column(sa.Column(name, sa.Float(), nullable=True))


def downgrade() -> None:
    """Drop the 8 advanced-metric columns in reverse order."""
    with op.batch_alter_table("backtest_results") as batch_op:
        for name in reversed(_NEW_COLUMNS):
            batch_op.drop_column(name)
