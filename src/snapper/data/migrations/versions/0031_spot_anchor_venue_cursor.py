"""Complete the spot anchor with the venue history cursor and the chain tip.

The bootstrap anchor becomes a provable certificate rather than a stored
guess. It gains the venue history left endpoint ``H0`` verbatim
(``venue_cursor_kind``/``venue_cursor_scheme``/``venue_cursor_value``), the
conservative read-order instants that make the sandwich proof checkable
(``venue_cursor_requested_at``/``_observed_at``/``_confirmed_at`` and
``source_watermark_requested_at``/``_captured_at``), and the per-scope
execution chain tip ``source_chain_tip`` that seals the sealed prefix the
anchor certifies. The domain CHECKs narrow to the single certified literal in
each dimension so a degraded anchor is unrepresentable: ``source_watermark``
tightens ``>= 0`` to ``>= 1`` (an anchor with no committed execution has no
witness), ``boundary_status`` to ``'cursor_certified'``, ``inventory_status``
to ``'venue_reported_full'`` (the anchor names only what the venue reports,
never a full-inventory claim), and ``margin_status`` to ``'cash'``. The
four-instant timestamp order is replaced by the ten-instant read-order chain
over the two balance reads, the two cursor reads, and the watermark capture.

Refusal atomicity, per 0029: SQLite offers no transactional DDL, so the one
data-dependent validation (anchor emptiness) runs BEFORE the first DDL
statement — a refusal leaves the schema at exactly revision 0030 and a
remediate-and-retry starts clean. No write fence is needed here (unlike 0029):
the anchor table has no shipped writer, so there is no concurrent inserter to
race, and on PostgreSQL ``ADD COLUMN ... NOT NULL`` with no default fails on a
non-empty table anyway, enforcing the emptiness invariant a second time. The
NEW columns carry no server_default: a pre-existing anchor predates the cursor
requirement and is degraded by definition, and inventing cursor values would
fabricate the exact evidence this plane exists to prove — so the migration
refuses a populated table rather than backfilling it. Every deployment
measures zero anchor rows. Revises 0030.
"""

from collections.abc import Sequence
from datetime import datetime

import sqlalchemy as sa
from alembic import op

revision: str = "0031"
down_revision: str | None = "0030"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_TABLE = "portfolio_spot_reconciliation_anchors"

_CK_WATERMARK_OLD = "source_watermark_kind = 'scope_sequence' AND source_watermark >= 0"
_CK_WATERMARK_NEW = "source_watermark_kind = 'scope_sequence' AND source_watermark >= 1"
_CK_BOUNDARY_OLD = "boundary_status IN ('cursor_certified', 'double_read_equal', 'uncertified')"
_CK_BOUNDARY_NEW = "boundary_status = 'cursor_certified'"
_CK_INVENTORY_OLD = "inventory_status IN ('certified_full', 'uncertified', 'suspect_partial')"
_CK_INVENTORY_NEW = "inventory_status = 'venue_reported_full'"
_CK_TIMESTAMP_ORDER_OLD = (
    "first_request_completed_at >= first_request_started_at AND "
    "second_request_started_at >= first_request_completed_at AND "
    "second_request_completed_at >= second_request_started_at AND "
    "timestamp >= second_request_completed_at"
)
_CK_TIMESTAMP_ORDER_NEW = (
    "venue_cursor_requested_at <= venue_cursor_observed_at AND "
    "venue_cursor_observed_at <= source_watermark_requested_at AND "
    "source_watermark_requested_at <= source_watermark_captured_at AND "
    "source_watermark_captured_at <= first_request_started_at AND "
    "first_request_completed_at >= first_request_started_at AND "
    "second_request_started_at >= first_request_completed_at AND "
    "second_request_completed_at >= second_request_started_at AND "
    "venue_cursor_confirmed_at >= second_request_completed_at AND "
    "timestamp >= venue_cursor_confirmed_at"
)
_CK_VENUE_CURSOR = (
    "venue_cursor_kind IN ('account_history_item_id') AND "
    "LENGTH(TRIM(venue_cursor_scheme)) > 0 AND "
    "venue_cursor_scheme = LOWER(venue_cursor_scheme) AND "
    "venue_cursor_scheme = TRIM(venue_cursor_scheme) AND "
    "LENGTH(TRIM(venue_cursor_value)) > 0 AND "
    "venue_cursor_value = TRIM(venue_cursor_value)"
)
_CK_CURSOR_VENUE_BINDING = "SUBSTR(venue_cursor_scheme, 1, LENGTH(exchange) + 1) = exchange || ':'"
_CK_CHAIN_TIP = (
    "LENGTH(source_chain_tip) = 64 AND "
    "source_chain_tip = LOWER(source_chain_tip) AND "
    "source_chain_tip = TRIM(source_chain_tip)"
)

_SWAPPED_CHECKS = (
    ("ck_portfolio_spot_anchor_watermark", _CK_WATERMARK_OLD, _CK_WATERMARK_NEW),
    ("ck_portfolio_spot_anchor_boundary_status", _CK_BOUNDARY_OLD, _CK_BOUNDARY_NEW),
    ("ck_portfolio_spot_anchor_inventory_status", _CK_INVENTORY_OLD, _CK_INVENTORY_NEW),
    ("ck_portfolio_spot_anchor_timestamp_order", _CK_TIMESTAMP_ORDER_OLD, _CK_TIMESTAMP_ORDER_NEW),
)
_ADDED_CHECKS = (
    ("ck_portfolio_spot_anchor_venue_cursor", _CK_VENUE_CURSOR),
    ("ck_portfolio_spot_anchor_cursor_venue_binding", _CK_CURSOR_VENUE_BINDING),
    ("ck_portfolio_spot_anchor_chain_tip", _CK_CHAIN_TIP),
)


def _is_sqlite() -> bool:
    """Return whether the migration is executing against SQLite."""
    return op.get_bind().dialect.name == "sqlite"


def _require_online_bind() -> sa.engine.Connection:
    """Return a live connection, refusing offline ``--sql`` rendering.

    The anchor-emptiness assert must READ data to fail closed; offline
    rendering cannot, and skipping it silently would let a degraded anchor be
    tightened under CHECKs it cannot satisfy. The assert calls this FIRST and
    runs before the first DDL statement, so an offline invocation refuses with
    ZERO rendered output.
    """
    if op.get_context().as_sql:
        raise RuntimeError(
            "migration 0031 requires an online connection: its anchor-emptiness "
            "assert reads data"
        )
    return op.get_bind()


def _new_columns() -> tuple[sa.Column[str] | sa.Column[datetime], ...]:
    """Build the NOT NULL columns added to the anchor, in stable order."""
    return (
        sa.Column("source_chain_tip", sa.String(64), nullable=False),
        sa.Column("venue_cursor_kind", sa.String(32), nullable=False),
        sa.Column("venue_cursor_scheme", sa.String(64), nullable=False),
        sa.Column("venue_cursor_value", sa.String(128), nullable=False),
        sa.Column("venue_cursor_requested_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("venue_cursor_observed_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("venue_cursor_confirmed_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("source_watermark_requested_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("source_watermark_captured_at", sa.DateTime(timezone=True), nullable=False),
    )


def _assert_anchor_table_empty(action: str) -> None:
    """ABORT the given action if any spot reconciliation anchor exists.

    Upgrade: no anchor writer has shipped, so the table is empty in every
    deployment; a populated table would carry degraded cursor-less anchors the
    NEW NOT NULL columns and CHECKs cannot represent, and a migration cannot
    perform authenticated venue reads to backfill ``venue_cursor_*`` honestly.
    Downgrade: dropping ``venue_cursor_*`` permanently destroys ``H_anchor``,
    which cannot be re-derived from any surviving evidence — so the downgrade
    refuses while any anchor (active OR closed) exists, BEFORE any destructive
    DDL, because SQLite offers no transactional DDL and a late refusal would
    leave the table rebuilt with the revision still stamped 0031.
    """
    bind = _require_online_bind()
    count = bind.execute(sa.text(f"SELECT COUNT(*) FROM {_TABLE}")).scalar()
    if not count:
        return
    if action == "upgrade":
        raise RuntimeError(
            f"migration 0031 aborted: {int(count)} spot reconciliation anchor row(s) "
            "exist at revision 0030, but the S4c-3 writer cannot insert without the "
            "venue_cursor_* / source_chain_tip columns this migration adds, and a "
            "migration cannot perform authenticated venue reads to backfill them "
            "honestly — investigate before re-running"
        )
    raise RuntimeError(
        f"migration 0031 downgrade aborted: {int(count)} spot reconciliation anchor "
        "row(s) exist; dropping venue_cursor_* permanently destroys H_anchor, which "
        "cannot be re-derived from any surviving evidence and forecloses the venue "
        "history proof for these accounts; delete the anchors explicitly if you "
        "intend that loss, then re-run"
    )


def upgrade() -> None:
    """Add the cursor + chain-tip columns and narrow every anchor CHECK.

    The one data-dependent validation runs before the first DDL. On SQLite one
    ``batch_alter_table(recreate="always")`` adds the columns and swaps the
    CHECKs in a single table rebuild; on PostgreSQL plain ``op.*`` statements do
    the same. Adding NOT NULL columns with no default is legal only because the
    table is empty (asserted above, and enforced again by PostgreSQL itself).
    """
    _assert_anchor_table_empty("upgrade")
    if _is_sqlite():
        with op.batch_alter_table(_TABLE, recreate="always") as batch:
            for column in _new_columns():
                batch.add_column(column)
            for name, _old, new in _SWAPPED_CHECKS:
                batch.drop_constraint(name, type_="check")
                batch.create_check_constraint(name, new)
            for name, body in _ADDED_CHECKS:
                batch.create_check_constraint(name, body)
        return
    for column in _new_columns():
        op.add_column(_TABLE, column)
    for name, _old, new in _SWAPPED_CHECKS:
        op.drop_constraint(name, _TABLE, type_="check")
        op.create_check_constraint(name, _TABLE, new)
    for name, body in _ADDED_CHECKS:
        op.create_check_constraint(name, _TABLE, body)


def downgrade() -> None:
    """Restore the wide anchor CHECKs and drop the cursor + chain-tip columns.

    Refuses BEFORE any destructive DDL while any anchor row exists (active or
    closed), because a closed anchor's cursor is still the only record of the
    venue position it was retired at, and dropping it would manufacture exactly
    the degraded cursor-less anchor this slice exists to prevent.
    """
    _assert_anchor_table_empty("downgrade")
    column_names = tuple(column.name for column in reversed(_new_columns()))
    if _is_sqlite():
        with op.batch_alter_table(_TABLE, recreate="always") as batch:
            for name, _body in _ADDED_CHECKS:
                batch.drop_constraint(name, type_="check")
            for name, old, _new in _SWAPPED_CHECKS:
                batch.drop_constraint(name, type_="check")
                batch.create_check_constraint(name, old)
            for column_name in column_names:
                batch.drop_column(column_name)
        return
    for name, _body in _ADDED_CHECKS:
        op.drop_constraint(name, _TABLE, type_="check")
    for name, old, _new in _SWAPPED_CHECKS:
        op.drop_constraint(name, _TABLE, type_="check")
        op.create_check_constraint(name, _TABLE, old)
    for column_name in column_names:
        op.drop_column(_TABLE, column_name)
