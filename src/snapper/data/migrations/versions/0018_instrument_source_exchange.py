"""Add instruments.source_exchange for the paper identity mapping.

PnL Phase 1: caps and USD valuation key by the instrument public id
from EMISSION (the paper instrument), while market snapshots and AI
reviews key by the SOURCE venue's instrument — so paper strategy emits
could never be priced. ``source_exchange`` records which real venue a
paper instrument replays/prices from, authored by the source-bound
paper publisher. The CHECK constrains it to lowercase values on paper
rows only; every non-paper instrument keeps NULL. Nullable + additive:
existing rows stay NULL until the publisher re-ensures them. SQLite
cannot ALTER-ADD a CHECK, so the table is recreated via batch mode on
that dialect. Revises 0017.
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0018"
down_revision: str | None = "0017"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_CK_SOURCE_EXCHANGE = (
    "source_exchange IS NULL OR "
    "(source_exchange = LOWER(source_exchange) AND exchange = 'paper')"
)


def _is_sqlite() -> bool:
    """Return True when the migration runs against SQLite.

    Returns:
        Whether the bound dialect is SQLite.
    """
    return op.get_bind().dialect.name == "sqlite"


def upgrade() -> None:
    """Add the nullable source_exchange column with its CHECK.

    Returns:
        None.
    """
    if _is_sqlite():
        with op.batch_alter_table("instruments", recreate="always") as batch:
            batch.add_column(sa.Column("source_exchange", sa.String(32), nullable=True))
            batch.create_check_constraint("ck_instrument_source_exchange", _CK_SOURCE_EXCHANGE)
        return
    op.add_column("instruments", sa.Column("source_exchange", sa.String(32), nullable=True))
    op.create_check_constraint("ck_instrument_source_exchange", "instruments", _CK_SOURCE_EXCHANGE)


def downgrade() -> None:
    """Drop the CHECK and the source_exchange column.

    Returns:
        None.
    """
    if _is_sqlite():
        with op.batch_alter_table("instruments", recreate="always") as batch:
            batch.drop_constraint("ck_instrument_source_exchange", type_="check")
            batch.drop_column("source_exchange")
        return
    op.drop_constraint("ck_instrument_source_exchange", "instruments", type_="check")
    op.drop_column("instruments", "source_exchange")
