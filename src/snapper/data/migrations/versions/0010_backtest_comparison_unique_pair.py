"""Add partial unique index for one-active-comparison-per-(wallet, pair).

Codex Phase 2c final-gate F2: the `/api/backtests/compare` POST route
does a SELECT-then-INSERT idempotency check + catches `SqlIntegrityError`
for race recovery (see `src/snapper/server/backtest_routes.py:606-646`),
but no partial unique index existed on active rows of
`backtest_comparisons` for the normalised (run_a, run_b) pair. Two
concurrent POSTs with the same normalised pair could race past the
SELECT and both commit, producing duplicate active rows — and the
`SqlIntegrityError` catch branch was effectively dead code because
nothing enforced uniqueness at the DB level.

Adds a partial unique index matching the ORM declaration in
`src/snapper/data/models.py:2086-2094`:
`(wallet_public_id, run_a_public_id, run_b_public_id)` filtered to
the SCD2 active slice (`known_to = KNOWN_TO_MAX`). Mirrors the
`uq_ep_active_bracket_per_cycle` / `uq_ep_active_trailing_stop_per_cycle`
shape from migrations 0001 + 0009.

Greenfield databases that run `create_all()` already pick this up
from the ORM metadata; databases that came through 0001 + backtest
feature migrations (0002..0007) need this migration to enforce the
invariant.
"""

from collections.abc import Sequence

from alembic import op
from sqlalchemy import text

revision: str = "0010"
down_revision: str | None = "0009"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_KNOWN_TO_ACTIVE_PG = "known_to = '9999-12-31T23:59:59+00:00'"
_KNOWN_TO_ACTIVE_SQLITE = "known_to = '9999-12-31 23:59:59.000000'"


def upgrade() -> None:
    """Create the comparison active-pair partial unique index if absent."""
    dialect = op.get_bind().dialect.name
    active_filter = _KNOWN_TO_ACTIVE_PG if dialect == "postgresql" else _KNOWN_TO_ACTIVE_SQLITE
    with op.get_context().autocommit_block():
        op.execute(
            text(
                "CREATE UNIQUE INDEX IF NOT EXISTS uq_bc_active_pair_per_wallet "
                "ON backtest_comparisons "
                "(wallet_public_id, run_a_public_id, run_b_public_id) "
                f"WHERE {active_filter}"
            )
        )


def downgrade() -> None:
    """Drop the comparison active-pair partial unique index if present."""
    with op.get_context().autocommit_block():
        op.execute(text("DROP INDEX IF EXISTS uq_bc_active_pair_per_wallet"))
