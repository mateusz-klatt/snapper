"""Widen instrument position-limit columns to 64-bit for large venue limits.

Kraken Futures reports position limits up to ~10^12, which overflows the
original 32-bit ``INTEGER`` ``position_limit_long``/``position_limit_short`` on
PostgreSQL (the S2a futures updater is the first writer to populate them). This
widens both columns to ``BIGINT`` on PostgreSQL so venue-declared limits are
stored faithfully rather than discarded. SQLite ``INTEGER`` is already a 64-bit
dynamic type, so its storage is unchanged and the migration is a no-op there.
Revises 0023.
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0024"
down_revision: str | None = "0023"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_COLUMNS = ("position_limit_long", "position_limit_short")


def _is_sqlite() -> bool:
    """Return whether the migration is executing against SQLite."""
    return op.get_bind().dialect.name == "sqlite"


def upgrade() -> None:
    """Widen the position-limit columns to BIGINT on PostgreSQL."""
    if _is_sqlite():
        return
    for column in _COLUMNS:
        op.alter_column(
            "instrument_specs",
            column,
            existing_type=sa.Integer(),
            type_=sa.BigInteger(),
            existing_nullable=True,
        )


def downgrade() -> None:
    """Narrow the position-limit columns back to 32-bit INTEGER on PostgreSQL."""
    if _is_sqlite():
        return
    for column in _COLUMNS:
        op.alter_column(
            "instrument_specs",
            column,
            existing_type=sa.BigInteger(),
            type_=sa.Integer(),
            existing_nullable=True,
        )
