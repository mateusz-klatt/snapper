"""Add truth provenance to positions and relax valuation nullability.

PnL Phase 2 (truthful positions surface): before this phase the
``positions`` table had NO production writer — the REST/MCP read
surface served an empty projection while the authoritative state lived
in-memory in TradeService plus the durable ``venue_events`` /
``trade_projection_checkpoints`` chain. The trader now projects one
active SCD2 row per (instrument_public_id, mode, wallet_public_id)
identity after every committed checkpoint and rebuilds on recovery.

Three nullable provenance columns ride every row: ``mark_price`` and
``marked_at`` echo the active market snapshot verbatim (stale-VISIBLE
— the snapshot's own bus timestamp, no age gate, never a carried-
forward previous mark), and ``source_venue_event_id`` records the
maximum durable venue-event watermark consumed into the state (a
recovery watermark, not the exact causal fill). ``average_price`` and
``unrealized_pnl`` relax to nullable because an aggregate of opposing-
direction paper strategy shards has no truthful single entry price and
a position without a usable mark has no truthful unrealized PnL —
NULL is the honest value, zero would be a lie.

No data backfill: the table is empty in production (the only historic
writer was a screenshot seeding script) and the first trader recovery
after deploy populates it. SQLite needs the batch recreate for the
nullability changes. Downgrade drops the three provenance columns and
re-tightens nullability after coalescing NULLs to zero. Revises 0019.
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy import text

revision: str = "0020"
down_revision: str | None = "0019"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def _is_sqlite() -> bool:
    """Return True when the migration runs against SQLite.

    Returns:
        Whether the bound dialect is SQLite.
    """
    return op.get_bind().dialect.name == "sqlite"


def upgrade() -> None:
    """Add provenance columns and relax valuation nullability.

    Returns:
        None.
    """
    if _is_sqlite():
        with op.batch_alter_table("positions", recreate="always") as batch:
            batch.alter_column("average_price", existing_type=sa.Float(), nullable=True)
            batch.alter_column("unrealized_pnl", existing_type=sa.Float(), nullable=True)
            batch.add_column(sa.Column("mark_price", sa.Float(), nullable=True))
            batch.add_column(sa.Column("marked_at", sa.DateTime(timezone=True), nullable=True))
            batch.add_column(sa.Column("source_venue_event_id", sa.Integer(), nullable=True))
        return
    op.alter_column("positions", "average_price", existing_type=sa.Float(), nullable=True)
    op.alter_column("positions", "unrealized_pnl", existing_type=sa.Float(), nullable=True)
    op.add_column("positions", sa.Column("mark_price", sa.Float(), nullable=True))
    op.add_column("positions", sa.Column("marked_at", sa.DateTime(timezone=True), nullable=True))
    op.add_column("positions", sa.Column("source_venue_event_id", sa.Integer(), nullable=True))


def downgrade() -> None:
    """Drop provenance columns and re-tighten valuation nullability.

    NULL valuations are coalesced to zero first so the NOT NULL
    constraints can be restored — downgrading forfeits the honest-NULL
    semantics by definition.

    Returns:
        None.
    """
    op.get_bind().execute(
        text(
            "UPDATE positions SET average_price = COALESCE(average_price, 0.0), "
            "unrealized_pnl = COALESCE(unrealized_pnl, 0.0)"
        )
    )
    if _is_sqlite():
        with op.batch_alter_table("positions", recreate="always") as batch:
            batch.drop_column("source_venue_event_id")
            batch.drop_column("marked_at")
            batch.drop_column("mark_price")
            batch.alter_column("average_price", existing_type=sa.Float(), nullable=False)
            batch.alter_column("unrealized_pnl", existing_type=sa.Float(), nullable=False)
        return
    op.drop_column("positions", "source_venue_event_id")
    op.drop_column("positions", "marked_at")
    op.drop_column("positions", "mark_price")
    op.alter_column("positions", "average_price", existing_type=sa.Float(), nullable=False)
    op.alter_column("positions", "unrealized_pnl", existing_type=sa.Float(), nullable=False)
