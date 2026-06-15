"""Candle provenance: add ``source`` + ``complete`` columns to candles.

Candle synthesis Phase 3 persists synthesized higher-timeframe candles into
the same ``candles`` table as native 1m bars. To keep the read path
single-source and let consumers reason about a bar's origin/trust, every
candle row carries:

- ``source`` (``native`` | ``synthesized``): native = exchange-native OHLC
  (all 1m, plus any native higher-TF + backfilled history); synthesized =
  aggregator rollup from the 1m stream.
- ``complete`` (bool): the aggregator's trustworthy-boundary flag (the window
  was seeded from the durable plane or opened at/after the live epoch). It is
  NOT a full-minute-coverage assertion — the 1m corpus is inherently gappy, so
  a minute-count gate was empirically rejected (parent plan §11.3/CA-2).

Both get server defaults (``source='native'``, ``complete=true``) so every
existing row and the unchanged 1m write path are unaffected, and a ``CHECK``
restricts ``source`` to the two-value vocabulary. Adding a CHECK requires table
recreation on SQLite (no ALTER-ADD-CHECK), so the SQLite path runs inside
``op.batch_alter_table(recreate="always")``; PostgreSQL uses direct DDL (the
candles table is append-mostly and the columns carry defaults, so no backfill
scan is needed). Revises 0010.
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0011"
down_revision: str | None = "0010"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_CK_CANDLE_SOURCE = "source IN ('native', 'synthesized')"


def _is_sqlite() -> bool:
    """Return True when the bound connection is SQLite (test fixture)."""
    return op.get_bind().dialect.name == "sqlite"


def upgrade() -> None:
    """Add ``source`` + ``complete`` to candles with defaults and a source CHECK."""
    if _is_sqlite():
        with op.batch_alter_table("candles", recreate="always") as batch:
            batch.add_column(
                sa.Column("source", sa.String(16), nullable=False, server_default="native")
            )
            batch.add_column(
                sa.Column("complete", sa.Boolean(), nullable=False, server_default=sa.text("true"))
            )
            batch.create_check_constraint("ck_candle_source", _CK_CANDLE_SOURCE)
        return
    op.add_column(
        "candles",
        sa.Column("source", sa.String(16), nullable=False, server_default="native"),
    )
    op.add_column(
        "candles",
        sa.Column("complete", sa.Boolean(), nullable=False, server_default=sa.text("true")),
    )
    op.create_check_constraint("ck_candle_source", "candles", _CK_CANDLE_SOURCE)


def downgrade() -> None:
    """Drop the candle provenance CHECK + columns."""
    if _is_sqlite():
        with op.batch_alter_table("candles", recreate="always") as batch:
            batch.drop_constraint("ck_candle_source", type_="check")
            batch.drop_column("complete")
            batch.drop_column("source")
        return
    op.drop_constraint("ck_candle_source", "candles", type_="check")
    op.drop_column("candles", "complete")
    op.drop_column("candles", "source")
