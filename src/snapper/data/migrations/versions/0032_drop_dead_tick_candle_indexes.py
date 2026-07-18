"""Drop three never-read tick/candle indexes (215 GB of write amplification).

Production forensics on the 688 GB database (2026-07-18, pg_stat_user_indexes
with a stats window that recorded 118M scans on the hot trades dedup index,
so zero readings are real absences, not a short window) found:

- ``ix_ticks_instrument_public_id`` (10 GB, idx_scan=0): a single-column
  prefix of the composite ``ix_tick_instrument_ts`` — structurally redundant
  for every possible query regardless of usage.
- ``ix_candles_instrument_public_id`` (2.9 GB, idx_scan=0): the same
  redundant-prefix shape next to ``ix_candle_instrument_open``.
- ``ix_ticks_public_id`` (50 GB, idx_scan=0): the bitemporal partial-unique
  identity index, unused on ticks because the tick write path is append-only
  bulk INSERT with no conflict semantics (``upsert_ticks``: uniqueness comes
  from UUID7 generation, never from enforcement) and no read path resolves a
  tick by ``public_id`` (repo-wide gate grep found zero readers).

Ticks are pure telemetry: 1.59B rows, n_tup_upd=0, and every one of the four
day-one indexes has never served a scan. The composite
``ix_tick_instrument_ts`` (108 GB) is deliberately KEPT — ``get_ticks`` /
``iter_ticks`` back the backtest/replay read surface and would otherwise
degrade to sequential scans over a 209 GB heap. Dropping the three dead
indexes returns ~63 GB to the filesystem immediately (DROP INDEX frees the
relation files, unlike DELETE) and removes three per-insert index maintenance
writes from the hottest table in the system (sustained 1500 rows/s burst).

Drops run ``CONCURRENTLY`` on PostgreSQL inside an autocommit block so the
hot tick/candle write paths are never blocked; if an interrupted concurrent
drop leaves an INVALID index behind, rerun the upgrade after dropping it
manually. SQLite ignores the dialect flag and drops normally. The downgrade
rebuilds all three exactly as 0001 defined them (the partial-unique predicate
literals are reproduced verbatim). Revises 0031.
"""

from collections.abc import Sequence

from alembic import op
from sqlalchemy import text

revision: str = "0032"
down_revision: str | None = "0031"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_KNOWN_TO_ACTIVE_PG = "known_to = '9999-12-31T23:59:59+00:00'"
_KNOWN_TO_ACTIVE_SQLITE = "known_to = '9999-12-31 23:59:59.000000'"


def upgrade() -> None:
    """Drop the three zero-scan indexes without blocking tick/candle writes."""
    with op.get_context().autocommit_block():
        op.drop_index(
            "ix_ticks_instrument_public_id",
            table_name="ticks",
            postgresql_concurrently=True,
        )
        op.drop_index(
            "ix_ticks_public_id",
            table_name="ticks",
            postgresql_concurrently=True,
        )
        op.drop_index(
            "ix_candles_instrument_public_id",
            table_name="candles",
            postgresql_concurrently=True,
        )


def downgrade() -> None:
    """Rebuild the three dropped indexes exactly as 0001 created them."""
    with op.get_context().autocommit_block():
        op.create_index(
            "ix_ticks_instrument_public_id",
            "ticks",
            ["instrument_public_id"],
            postgresql_concurrently=True,
        )
        op.create_index(
            "ix_ticks_public_id",
            "ticks",
            ["public_id"],
            unique=True,
            sqlite_where=text(_KNOWN_TO_ACTIVE_SQLITE),
            postgresql_where=text(_KNOWN_TO_ACTIVE_PG),
            postgresql_concurrently=True,
        )
        op.create_index(
            "ix_candles_instrument_public_id",
            "candles",
            ["instrument_public_id"],
            postgresql_concurrently=True,
        )
