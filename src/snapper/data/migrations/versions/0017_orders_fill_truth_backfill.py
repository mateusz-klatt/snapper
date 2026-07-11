"""Backfill orders.filled_size / average_price from fill evidence.

Until PnL Phase 1 the executor never wrote fill truth onto the orders
row — ``filled_size`` stayed 0.0 and ``average_price`` NULL for every
order ever filled, while the truth accrued in ``venue_events``
(``fill_observed`` delta rows) and ``executions`` (published delta
rows). This one-shot data migration repairs CURRENT versions only
(active row per SCD2, terminal statuses included).

Cumulative precedence per order: (1) the ADDITIVE sum of
``fill_observed.fill_size`` deduplicated by the CANONICAL replay rule
(exec_id OR trade_id, else the ``fallback-{cid}-{size}-{price}`` key,
first row wins — mirroring ``TradeService.dedup_fill_events``),
because ``MAX(cum_fill_size)`` is fabricated from the published
watermark on delta-only venues and under-reports after a publish
failure; (2) legacy ``MAX(cum_fill_size)`` over the same rows for
history whose ``fill_size`` was never recorded; (3) the executions
delta sum. Correlation uses the STABLE order identity —
client_order_id + wallet_public_id + mode + the active instrument's
exchange, plus the order's exchange_order_id when present (rows
carrying a DIFFERENT venue id are excluded, id-less rows count) —
never the native symbol spelling, which a rename would invalidate.
The average is the executions VWAP but ONLY when the delta sum
reproduces the chosen cumulative (a missed frame would make the VWAP
a lie — such rows keep NULL).

Evidence is BULK-loaded: one pass over ``fill_observed`` rows grouped
by identity in Python, one grouped executions aggregate, one orders
scan, and a single batched UPDATE — round trips stay constant no
matter how many all-time orders exist. The active-row ``known_to``
bind is dialect-typed: the SQLite storage string vs an aware UTC
``datetime`` for PostgreSQL (asyncpg's TIMESTAMPTZ codec rejects
untyped strings). Historical (closed) versions are untouched;
downgrade is a documented no-op. Revises 0016.
"""

from collections.abc import Sequence
from datetime import UTC
from datetime import datetime
from typing import Any

import sqlalchemy as sa
from alembic import op
from sqlalchemy import text

revision: str = "0017"
down_revision: str | None = "0016"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_KNOWN_TO_ACTIVE_SQLITE = "9999-12-31 23:59:59.000000"
_KNOWN_TO_ACTIVE_DT = datetime(9999, 12, 31, 23, 59, 59, tzinfo=UTC)
_SUM_MATCH_TOLERANCE = 1e-9

_ORDERS_SQL = (
    "SELECT o.id, o.public_id, o.client_order_id, o.wallet_public_id, "
    "o.mode, o.exchange_order_id, i.exchange "
    "FROM orders o "
    "JOIN instruments i ON i.public_id = o.instrument_public_id "
    " AND i.known_to = :active "
    "WHERE o.known_to = :active"
)

_ALL_FILL_ROWS_SQL = (
    "SELECT ve.client_order_id, ve.wallet_public_id, ve.mode, ve.exchange, "
    "ve.fill_size, ve.fill_price, ve.cum_fill_size, ve.exec_id, ve.trade_id, "
    "ve.exchange_order_id "
    "FROM venue_events ve "
    "WHERE ve.event_type = 'fill_observed' "
    "ORDER BY ve.id ASC"
)

_EXEC_SUMS_SQL = (
    "SELECT e.order_public_id, SUM(e.size), SUM(e.price * e.size) "
    "FROM executions e "
    "WHERE e.status IN ('filled', 'partial') "
    "AND e.known_to = :active "
    "GROUP BY e.order_public_id"
)


def _active_value() -> str | datetime:
    """Return the dialect-typed active-row ``known_to`` bind value.

    Returns:
        The SQLite storage string, or an aware UTC datetime for
        PostgreSQL (asyncpg's TIMESTAMPTZ codec requires a datetime).
    """
    if op.get_bind().dialect.name == "sqlite":
        return _KNOWN_TO_ACTIVE_SQLITE
    return _KNOWN_TO_ACTIVE_DT


def _typed_statement(sql: str) -> sa.TextClause:
    """Build a statement whose ``:active`` bind is dialect-typed.

    PostgreSQL: the bind is EXPLICITLY typed ``DateTime(timezone=True)``
    so asyncpg sends a timestamptz-OID parameter exactly like every ORM
    query does — deployments carrying LEGACY naive-``timestamp``
    ``known_to`` columns (tables created before the 2026-05-16
    native-types decision) would otherwise describe an untyped
    parameter as naive ``timestamp``, which asyncpg refuses to encode
    an aware datetime into. The upgrade pins the migration
    transaction's session to UTC so cross-type coercion against naive
    columns is deterministic. SQLite keeps the raw string bind.

    Args:
        sql: Raw SQL carrying a single ``:active`` parameter.

    Returns:
        The executable statement.
    """
    stmt = text(sql)
    if op.get_bind().dialect.name == "sqlite":
        return stmt
    return stmt.bindparams(sa.bindparam("active", type_=sa.DateTime(timezone=True)))


def _dedup_additive_and_legacy(
    fill_rows: list[Any], order_exchange_order_id: str | None
) -> tuple[float, float | None]:
    """Compute the deduplicated additive sum and the legacy cumulative.

    Mirrors ``TradeService.dedup_fill_events``: a row is a duplicate
    when ANY of its identity keys (exec_id, trade_id — or the
    ``fallback-{cid}-{size}-{price}`` key when both are absent) was
    already seen; the FIRST row wins. Rows carrying a venue order id
    DIFFERENT from the order's are foreign evidence and excluded
    (id-less rows count).

    Args:
        fill_rows: Id-ordered ``(cid, fill_size, fill_price,
            cum_fill_size, exec_id, trade_id, row_xoid)`` projections.
        order_exchange_order_id: The order row's venue id, if any.

    Returns:
        Tuple of (additive fill_size sum, MAX cum_fill_size or None).
    """
    seen: set[str] = set()
    additive = 0.0
    legacy_cum: float | None = None
    for cid, fill_size, fill_price, cum_fill_size, exec_id, trade_id, row_xoid in fill_rows:
        if (
            order_exchange_order_id is not None
            and row_xoid is not None
            and row_xoid != order_exchange_order_id
        ):
            continue
        if cum_fill_size is not None and (legacy_cum is None or cum_fill_size > legacy_cum):
            legacy_cum = float(cum_fill_size)
        keys = [key for key in (exec_id, trade_id) if key]
        if not keys:
            keys = [f"fallback-{cid}-{fill_size}-{fill_price}"]
        if any(key in seen for key in keys):
            continue
        seen.update(keys)
        if fill_size is not None:
            additive += float(fill_size)
    return additive, legacy_cum


def upgrade() -> None:
    """Repair fill columns on active order rows carrying fill evidence.

    Returns:
        None.
    """
    connection = op.get_bind()
    if connection.dialect.name != "sqlite":
        connection.execute(text("SET LOCAL TIME ZONE 'UTC'"))
    active = _active_value()
    fills_by_identity: dict[tuple[str, str, str, str], list[Any]] = {}
    for row in connection.execute(text(_ALL_FILL_ROWS_SQL)).fetchall():
        cid, wallet, mode, exchange, fill_size, fill_price, cum, exec_id, trade_id, xoid = row
        fills_by_identity.setdefault((cid, wallet, mode, exchange), []).append(
            (cid, fill_size, fill_price, cum, exec_id, trade_id, xoid)
        )
    exec_sums = {
        public_id: (exec_sum, exec_notional)
        for public_id, exec_sum, exec_notional in connection.execute(
            _typed_statement(_EXEC_SUMS_SQL), {"active": active}
        ).fetchall()
    }
    pending_updates: list[dict[str, Any]] = []
    orders = connection.execute(_typed_statement(_ORDERS_SQL), {"active": active}).fetchall()
    for order_id, public_id, cid, wallet, mode, order_xoid, exchange in orders:
        fill_rows = fills_by_identity.get((cid, wallet, mode, exchange), [])
        additive, legacy_cum = _dedup_additive_and_legacy(fill_rows, order_xoid)
        exec_sum, exec_notional = exec_sums.get(public_id, (None, None))
        filled = None
        if additive > 0.0:
            filled = additive
        elif legacy_cum is not None and legacy_cum > 0.0:
            filled = legacy_cum
        elif exec_sum is not None and float(exec_sum) > 0.0:
            filled = float(exec_sum)
        if filled is None:
            continue
        average = None
        if (
            exec_sum is not None
            and float(exec_sum) > 0.0
            and exec_notional is not None
            and abs(float(exec_sum) - filled) < _SUM_MATCH_TOLERANCE
        ):
            average = float(exec_notional) / float(exec_sum)
        pending_updates.append({"filled": filled, "average": average, "order_id": order_id})
    if pending_updates:
        connection.execute(
            text(
                "UPDATE orders SET filled_size = :filled, average_price = :average "
                "WHERE id = :order_id"
            ),
            pending_updates,
        )


def downgrade() -> None:
    """No-op: repaired fill values stay in place.

    Zeroing ``filled_size``/``average_price`` back would re-corrupt
    rows the executor has since updated truthfully, and repaired
    values are indistinguishable from executor-written ones.

    Returns:
        None.
    """
