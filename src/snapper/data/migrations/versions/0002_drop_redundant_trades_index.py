"""Drop redundant ``ix_trades_instrument_public_id`` single-column index.

The composite index ``ix_trade_instrument_ts (instrument_public_id,
"timestamp")`` from 0001 already covers all instrument-only lookups
because Postgres can use the leftmost prefix of a btree composite for
queries that filter only on ``instrument_public_id``. The standalone
single-column ``ix_trades_instrument_public_id`` is therefore redundant
for read paths.

It still has cost on the write path: every ``INSERT`` into ``trades``
updates one extra btree, adding WAL writes + index maintenance latency.
On the 19.9M-row production ``trades`` table this index occupies ~1 GB
of disk and contributes to the per-row insert cost behind the
``writer_upsert_call`` p99 stalls captured by ``TradeProbe`` on
2026-05-27 (see [[project-2026-05-27-trade-writer-db-bottleneck]] and
the Codex architect review thread 019e69e4 recommending the drop as a
Quick-effort tuning step before Phase C).

Cross-dialect notes:

* ``op.drop_index`` emits ``DROP INDEX`` (non-CONCURRENTLY) which takes a
  brief ``ACCESS EXCLUSIVE`` lock on the table. On the current prod
  Postgres ``trades`` table the drop is metadata + page-free only and
  completes in well under a second. Applying during quiet trading
  windows is preferred but not required.
* SQLite (used by the dev test fixture) supports ``DROP INDEX`` natively
  and the lock is held only for the duration of the metadata change.
* ``IF EXISTS`` is set so a hand-applied production drop (CONCURRENTLY
  via psql) before this migration ships does not break the upgrade.

There is no functional rollback target — re-creating the index restores
the pre-0002 write-path overhead. ``downgrade()`` is implemented for
Alembic completeness so ``alembic downgrade -1`` still works.
"""

from collections.abc import Sequence

from alembic import op

revision: str = "0002"
down_revision: str | None = "0001"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Drop the redundant single-column instrument index on ``trades``.

    Uses ``op.drop_index`` with ``if_exists=True`` so a manual
    ``DROP INDEX CONCURRENTLY`` applied before the migration ships does
    not break the upgrade path. The covering composite index
    ``ix_trade_instrument_ts`` remains in place.
    """
    op.drop_index("ix_trades_instrument_public_id", table_name="trades", if_exists=True)


def downgrade() -> None:
    """Recreate the single-column instrument index on ``trades``.

    Restores the redundant index so callers running
    ``alembic downgrade 0001`` end up with the schema matching the
    initial migration. The composite ``ix_trade_instrument_ts`` is left
    untouched (it was created by 0001 and is owned by 0001's
    ``downgrade``).
    """
    op.create_index(
        "ix_trades_instrument_public_id", "trades", ["instrument_public_id"], if_not_exists=True
    )
