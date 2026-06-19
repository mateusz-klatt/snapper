"""Candle provenance taxonomy: add ``calculated`` to the source vocabulary.

The 0011 ``source`` vocabulary (``native`` | ``synthesized``) overloaded
``native`` to mean both venue-precomputed upstream OHLC (Kraken spot ``ohlc:1m``,
Polygon grouped-daily history, and the Kraken futures/equities REST aggregate
backfills) AND Snapper-computed live base 1m bars built from the trade/quote
stream (Kraken equities + futures via ``TradeCandleBuilder``; Walutomat via REST
quote polling). This migration adds a third value so live computed bars can be
tagged distinctly going forward:

- ``native``: venue-precomputed upstream OHLC (Kraken spot, Polygon history,
  Kraken futures/equities 1m REST aggregate backfills).
- ``calculated``: Snapper-computed live base 1m from trades/quotes/ticks.
- ``synthesized``: Snapper higher-timeframe rollups from the 1m stream.

This is FORWARD-ONLY: the upgrade only widens the CHECK; it does NOT retag
existing rows. A blanket historical retag was rejected in review because for
Kraken equities/futures a ``native`` 1m row is indistinguishable in the data
between an upstream REST-aggregate backfill (correctly ``native``) and a live
trade-built bar (``calculated``), so retagging by exchange would mislabel real
upstream rows; and ``instruments.exchange`` is not temporally stable under
SCD2 revision. From this migration forward the live publishers tag their
computed 1m as ``calculated`` (see ``_candle_source_for``); pre-existing rows
keep their ``native`` tag. The downgrade direction IS unambiguous — every
``calculated`` row was written by post-0012 live code — so it reverts those to
``native`` (id-range batched on PostgreSQL) before narrowing the CHECK.

On PostgreSQL the CHECK is dropped (``IF EXISTS``) and re-added ``NOT VALID``
then ``VALIDATE``-d inside an ``autocommit_block`` so the widening never holds a
long ``ACCESS EXCLUSIVE`` validating scan over the ~15M-row table. On SQLite the
CHECK is swapped via ``batch_alter_table(recreate="always")``. Revises 0011.
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0012"
down_revision: str | None = "0011"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_CK_OLD = "source IN ('native', 'synthesized')"
_CK_NEW = "source IN ('native', 'calculated', 'synthesized')"
_BATCH_ROWS = 100_000


def _is_sqlite() -> bool:
    """Return True when the bound connection is SQLite (test fixture)."""
    return op.get_bind().dialect.name == "sqlite"


def _retag_calculated_to_native_batched() -> None:
    """Revert ``calculated`` rows to ``native`` in id-range batches.

    Used only by the downgrade: every ``calculated`` row was written by
    post-0012 live code, so the reversion is unambiguous. Batching keeps any
    single transaction from spanning the whole table.
    """
    bind = op.get_bind()
    bounds = bind.execute(sa.text("SELECT min(id), max(id) FROM candles")).one()
    lo, hi = bounds[0], bounds[1]
    if lo is None or hi is None:
        return
    start = int(lo)
    end = int(hi)
    while start <= end:
        op.execute(
            sa.text(
                "UPDATE candles SET source = 'native' "
                "WHERE id >= :lo AND id < :hi AND source = 'calculated'"
            ).bindparams(lo=start, hi=start + _BATCH_ROWS)
        )
        start += _BATCH_ROWS


def upgrade() -> None:
    """Widen the source CHECK to include ``calculated`` (forward-only, no retag)."""
    if _is_sqlite():
        with op.batch_alter_table("candles", recreate="always") as batch:
            batch.drop_constraint("ck_candle_source", type_="check")
            batch.create_check_constraint("ck_candle_source", _CK_NEW)
        return
    with op.get_context().autocommit_block():
        op.execute("ALTER TABLE candles DROP CONSTRAINT IF EXISTS ck_candle_source")
        op.execute(
            f"ALTER TABLE candles ADD CONSTRAINT ck_candle_source CHECK ({_CK_NEW}) NOT VALID"
        )
        op.execute("ALTER TABLE candles VALIDATE CONSTRAINT ck_candle_source")


def downgrade() -> None:
    """Revert ``calculated`` rows to ``native`` and narrow the source CHECK."""
    if _is_sqlite():
        op.execute("UPDATE candles SET source = 'native' WHERE source = 'calculated'")
        with op.batch_alter_table("candles", recreate="always") as batch:
            batch.drop_constraint("ck_candle_source", type_="check")
            batch.create_check_constraint("ck_candle_source", _CK_OLD)
        return
    with op.get_context().autocommit_block():
        op.execute("ALTER TABLE candles DROP CONSTRAINT IF EXISTS ck_candle_source")
        _retag_calculated_to_native_batched()
        op.execute(
            f"ALTER TABLE candles ADD CONSTRAINT ck_candle_source CHECK ({_CK_OLD}) NOT VALID"
        )
        op.execute("ALTER TABLE candles VALIDATE CONSTRAINT ck_candle_source")
