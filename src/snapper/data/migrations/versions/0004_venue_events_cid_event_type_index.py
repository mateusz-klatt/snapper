"""Add a venue_events (client_order_id, event_type) index for fill recovery.

The paired-execution guard's recovery-replay fill parity pass
(``recover_paired_execution_leg_fills``) resolves, for each owned active leg,
the maximum cumulative ``fill_observed`` venue event by ``client_order_id`` so a
fill observed while the coordinator was down is re-projected onto the leg on
restart. The existing venue_events indexes cover ``(shard_key, id)`` and other
fields but NOT client-order lookups, so that query would seq-scan an
append-only, long-lived table. This composite index makes the lookup an index
range scan. Revises 0003.
"""

from collections.abc import Sequence

from alembic import op

revision: str = "0004"
down_revision: str | None = "0003"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Create the venue_events client-order / event-type lookup index."""
    op.create_index(
        "ix_venue_events_cid_event_type",
        "venue_events",
        ["client_order_id", "event_type"],
    )


def downgrade() -> None:
    """Drop the venue_events client-order / event-type lookup index."""
    op.drop_index("ix_venue_events_cid_event_type", table_name="venue_events")
