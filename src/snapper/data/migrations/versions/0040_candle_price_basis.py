"""Add the nullable ``candles.price_basis`` provenance discriminator.

``source`` already names the MECHANISM that produced a bar (``native`` /
``calculated`` / ``synthesized``), but it says nothing about WHICH PRICE the
bar was marked on. ``calculated`` now spans two conventions: Kraken
futures/equities bars built from trade prints, and Walutomat bars built from
the midpoint of the venue's two-sided top-of-book quote. This column names the
price so a stored row stays legible after the Walutomat mark convention
changed forward-only from an externally-sourced reference rate to that mid.

No server default and no backfill: NULL on every pre-cutover row is the
honest encoding of "written before the convention was labelled", and history
is never rewritten. Nullable-with-no-default is a metadata-only DDL on
PostgreSQL. Deliberately no CHECK constraint - the vocabulary is enforced in
Python by :data:`snapper.core.types.PriceBasis`, precisely so a future value
needs no migration. Revises 0039.
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0040"
down_revision: str | None = "0039"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_TABLE = "candles"
_COLUMN = "price_basis"


def upgrade() -> None:
    """Add the nullable price-basis column to ``candles``.

    Returns:
        None.
    """
    op.add_column(_TABLE, sa.Column(_COLUMN, sa.String(16), nullable=True))


def downgrade() -> None:
    """Drop the price-basis column from ``candles``.

    Returns:
        None.
    """
    op.drop_column(_TABLE, _COLUMN)
