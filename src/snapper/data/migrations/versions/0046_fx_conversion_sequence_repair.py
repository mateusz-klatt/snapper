"""Restore canonical identity sequence names after the 0045 batch rebuild."""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0046"
down_revision: str | None = "0045"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_TABLES = ("fx_conversion_elections", "fx_conversion_proofs")


def _rename_temporary_sequence(table: str) -> None:
    """Point one table's identity back at a canonically named sequence.

    Alembic implements a CHECK change in batch mode by building a replacement
    table, and on PostgreSQL it carried the ``_alembic_tmp_`` sequence into the
    finished table rather than restoring the original name. Inserts still work,
    so this is not an outage — but the schema then disagrees with what the ORM
    would create, and a later batch migration touching the same table could
    reasonably drop an object that name says is temporary.

    Args:
        table: Market-data table whose identity sequence is being restored.
    """
    temporary = f"_alembic_tmp_{table}_id_seq"
    canonical = f"{table}_id_seq"
    bind = op.get_bind()
    present = bind.execute(
        sa.text("SELECT to_regclass(:name) IS NOT NULL"), {"name": temporary}
    ).scalar_one()
    if not present:
        return
    taken = bind.execute(
        sa.text("SELECT to_regclass(:name) IS NOT NULL"), {"name": canonical}
    ).scalar_one()
    if taken:
        return
    op.execute(sa.text(f'ALTER SEQUENCE "{temporary}" RENAME TO "{canonical}"'))
    op.execute(sa.text(f'ALTER SEQUENCE "{canonical}" OWNED BY "{table}".id'))
    op.execute(
        sa.text(f'ALTER TABLE "{table}" ALTER COLUMN id SET DEFAULT nextval(\'"{canonical}"\')')
    )


def upgrade() -> None:
    """Rename any surviving batch-rebuild sequence back to its canonical name.

    A no-op on SQLite, which has no sequences, and on any database whose 0045
    run did not leave a temporary name behind.
    """
    if op.get_bind().dialect.name != "postgresql":
        return
    for table in _TABLES:
        _rename_temporary_sequence(table)


def downgrade() -> None:
    """Keep the canonical names: reintroducing a temporary one has no value."""
