"""Advance the FX conversion identity sequences past their existing rows.

Revision 0045 rebuilt both tables on PostgreSQL, and 0046 restored the identity
sequences' NAME, OWNERSHIP and column DEFAULT — but not their VALUE. A rebuilt
table keeps the copied rows' ids while its fresh sequence restarts near one, so
``nextval`` walks upward through whatever ids happen to be free and raises a
primary-key violation the moment it reaches an occupied one.

Measured on production before this ran: ``fx_conversion_proofs`` held ids up to
4872 while its sequence sat at 2385, so proof inserts had been failing since
the rebuild. ``fx_conversion_elections`` had already climbed clear of its rows
under ordinary write volume, which is why refusal audits kept succeeding and
hid the defect.

Idempotent, and correct for an empty table.
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0049"
down_revision: str | None = "0048"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_TABLES = ("fx_conversion_elections", "fx_conversion_proofs")


def _advance(table: str) -> None:
    """Point one identity sequence just past the table's greatest id.

    ``is_called`` is false so the next ``nextval`` returns the supplied value
    itself, which is why the expression adds one rather than relying on the
    sequence to increment past a value it never issued.

    Args:
        table: Table whose identity sequence is advanced.
    """
    op.execute(
        sa.text(
            f"SELECT setval('{table}_id_seq', "
            f"COALESCE((SELECT max(id) FROM {table}), 0) + 1, false)"
        )
    )


def upgrade() -> None:
    """Resynchronise both identity sequences with their surviving rows."""
    if op.get_bind().dialect.name != "postgresql":
        return
    for table in _TABLES:
        _advance(table)


def downgrade() -> None:
    """Leave the sequences alone.

    Rewinding a sequence can only reintroduce the collision this revision
    exists to clear, so the reversal is deliberately inert.
    """
