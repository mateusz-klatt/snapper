"""Add replay provenance to trade_commands and classify the backlog.

PnL Phase 1 S4 (incident 2026-07-10 #3): replayed historical market
frames could drive the LIVE engine into emitting market commands that
fill a live paper account at historical prices — nothing on the
command recorded its origin, so no guard could exist. ``origin``
(``live``/``replay``, CHECK-constrained, default ``live``) plus the
nullable replay window datetimes now ride every command from the
triggering frame's stamped provenance; every executor rejects
``replay`` submits pre-venue.

Backlog classification: rows predating the column all get the
``live`` server default, which would RELEASE any queued replay-origin
paper backlog the moment the guard deploys. Conservatively, still-
pending paper STRATEGY submits (mode='paper', NULL plan_public_id,
submit-like command_type, pre-dispatch statuses) are marked
``replay`` — a queued live paper strategy command mislabelled replay
is merely rejected and re-emitted by the next signal, while a replay
command mislabelled live would EXECUTE. Paired-execution SAFETY
machinery is exempt (``idempotency_key LIKE 'paired:%'`` — the guard
scanner's reduce-only flattens and compensations are plan-less paper
submits too, and rejecting an exposure-REDUCING command would be the
opposite of safety). SQLite needs the batch recreate for the CHECK.
Downgrade drops all three columns. Revises 0018.
"""

from collections.abc import Sequence
from datetime import UTC
from datetime import datetime

import sqlalchemy as sa
from alembic import op
from sqlalchemy import text

revision: str = "0019"
down_revision: str | None = "0018"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_CK_ORIGIN = "origin IN ('live', 'replay')"

_BACKLOG_SQL = (
    "UPDATE trade_commands SET origin = 'replay' "
    "WHERE mode = 'paper' "
    "AND plan_public_id IS NULL "
    "AND command_type IN ('create', 'submit', 'replace') "
    "AND status IN ('created', 'dispatched') "
    "AND (idempotency_key IS NULL OR idempotency_key NOT LIKE 'paired:%') "
    "AND known_to = :active"
)

_KNOWN_TO_ACTIVE_SQLITE = "9999-12-31 23:59:59.000000"


def _is_sqlite() -> bool:
    """Return True when the migration runs against SQLite.

    Returns:
        Whether the bound dialect is SQLite.
    """
    return op.get_bind().dialect.name == "sqlite"


def _active_value() -> str | datetime:
    """Return the dialect-typed active-row ``known_to`` bind value.

    Returns:
        The SQLite storage string, or an aware UTC datetime for
        PostgreSQL.
    """
    if _is_sqlite():
        return _KNOWN_TO_ACTIVE_SQLITE
    return datetime(9999, 12, 31, 23, 59, 59, tzinfo=UTC)


def upgrade() -> None:
    """Add origin + replay window columns and classify the backlog.

    Returns:
        None.
    """
    if _is_sqlite():
        with op.batch_alter_table("trade_commands", recreate="always") as batch:
            batch.add_column(
                sa.Column("origin", sa.String(8), nullable=False, server_default="live")
            )
            batch.add_column(sa.Column("replay_window_start", sa.DateTime(timezone=True)))
            batch.add_column(sa.Column("replay_window_end", sa.DateTime(timezone=True)))
            batch.create_check_constraint("ck_trade_commands_origin", _CK_ORIGIN)
    else:
        op.add_column(
            "trade_commands",
            sa.Column("origin", sa.String(8), nullable=False, server_default="live"),
        )
        op.add_column(
            "trade_commands", sa.Column("replay_window_start", sa.DateTime(timezone=True))
        )
        op.add_column("trade_commands", sa.Column("replay_window_end", sa.DateTime(timezone=True)))
        op.create_check_constraint("ck_trade_commands_origin", "trade_commands", _CK_ORIGIN)
    op.get_bind().execute(text(_BACKLOG_SQL), {"active": _active_value()})


def downgrade() -> None:
    """Drop the CHECK and the three provenance columns.

    Returns:
        None.
    """
    if _is_sqlite():
        with op.batch_alter_table("trade_commands", recreate="always") as batch:
            batch.drop_constraint("ck_trade_commands_origin", type_="check")
            batch.drop_column("replay_window_end")
            batch.drop_column("replay_window_start")
            batch.drop_column("origin")
        return
    op.drop_constraint("ck_trade_commands_origin", "trade_commands", type_="check")
    op.drop_column("trade_commands", "replay_window_end")
    op.drop_column("trade_commands", "replay_window_start")
    op.drop_column("trade_commands", "origin")
