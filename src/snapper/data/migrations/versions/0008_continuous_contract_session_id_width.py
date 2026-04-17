"""Narrow continuous_contract_configs.session_id to String(36).

Aligns the column width with ``TemporalMixin`` (``models.py:145``,
``String(36)``). The init migration ``0001_init.py:1105`` declared
``String(64)`` by mistake — every other ``TemporalMixin`` table in
the init migration already uses ``String(36)``.

The upgrade runs a defensive pre-migration audit that fails loudly
if any existing row carries a ``LENGTH(session_id) > 36`` value.
Production writers are ``SequenceTracker``-driven and always emit
36-char UUID7 strings, so the audit is expected to pass; it exists
only as defence-in-depth against hand-inserted test rows or an
accidental producer anomaly. The column alter itself uses
``op.batch_alter_table`` to remain SQLite-portable.
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0008"
down_revision: str | None = "0007"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Audit + shrink continuous_contract_configs.session_id to String(36)."""
    bind = op.get_bind()
    oversize = bind.execute(
        sa.text("SELECT COUNT(*) FROM continuous_contract_configs WHERE LENGTH(session_id) > 36")
    ).scalar()
    if oversize:
        raise RuntimeError(
            "Refusing to narrow continuous_contract_configs.session_id: "
            f"{oversize} row(s) have LENGTH(session_id) > 36. Inspect and "
            "either clean up or abort the migration."
        )
    with op.batch_alter_table("continuous_contract_configs") as batch:
        batch.alter_column(
            "session_id",
            existing_type=sa.String(64),
            type_=sa.String(36),
            existing_nullable=False,
        )


def downgrade() -> None:
    """Restore continuous_contract_configs.session_id to String(64)."""
    with op.batch_alter_table("continuous_contract_configs") as batch:
        batch.alter_column(
            "session_id",
            existing_type=sa.String(36),
            type_=sa.String(64),
            existing_nullable=False,
        )
