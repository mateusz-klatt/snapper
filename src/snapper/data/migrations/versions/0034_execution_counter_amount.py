"""Add the exact per-fill counter-amount to executions (S4c-4c Phase 2 / OD6).

The reconciliation replay used to fold the quote leg as ``size * price`` — a
half-tick approximation that price improvement (Walutomat quotes better than
the limit) turned into a persistent false ``mismatched``. This adds a nullable
``counter_amount_decimal`` carrying the venue's EXACT opposite-currency amount
for the fill, which the evaluator folds verbatim (dropping the price-tick
tolerance) so the quote leg reconciles exactly.

Purely additive: a NULLABLE column with no default and no backfill. ``ADD
COLUMN`` is metadata-only DDL on both dialects — it does not rewrite rows and
it does not fire the ``executions`` append-only row triggers (which reject
UPDATE/DELETE, not schema changes), so the immutability guard is untouched and
no fence is needed. The existing rows (paper executions only at deploy time)
take ``NULL``. The raw-decimals CHECK is intentionally NOT widened: a
counter-only row already satisfies its all-three-NULL clause, and the evaluator
plus the writer's provenance stamp enforce the field's shape, so the constraint
change (which would force a trigger-dropping table recreate on SQLite) buys no
real protection. The field is folded into the execution hash chain (bumped to
``v2``); this is sound only while no chain tip has been sealed — true at deploy
because no spot anchor or matched checkpoint exists yet (``W = 0``).
"""

import sqlalchemy as sa
from alembic import op

revision: str = "0034"
down_revision: str | None = "0033"
branch_labels: str | None = None
depends_on: str | None = None


def upgrade() -> None:
    """Add the nullable ``counter_amount_decimal`` column to executions."""
    op.add_column("executions", sa.Column("counter_amount_decimal", sa.Text(), nullable=True))


def downgrade() -> None:
    """Drop the ``counter_amount_decimal`` column from executions."""
    op.drop_column("executions", "counter_amount_decimal")
