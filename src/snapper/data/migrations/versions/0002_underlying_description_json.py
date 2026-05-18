"""Store underlying descriptions as locale-keyed JSON.

Existing scalar descriptions are moved under the ``en`` key. Downgrade
extracts only ``en`` and discards non-English locale entries.
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0002"
down_revision: str | None = "0001"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def _upgrade_postgresql() -> None:
    """Upgrade the Postgres column to JSONB with scalar backfill."""
    op.execute(
        "ALTER TABLE underlying_assets "
        "ALTER COLUMN description TYPE JSONB "
        "USING CASE "
        "WHEN description IS NULL THEN NULL "
        "ELSE jsonb_build_object('en', description) "
        "END"
    )


def _upgrade_sqlite() -> None:
    """Upgrade the SQLite column through Alembic batch recreation."""
    with op.batch_alter_table("underlying_assets") as batch_op:
        batch_op.alter_column(
            "description",
            existing_type=sa.String(256),
            type_=sa.JSON(),
            existing_nullable=True,
        )
    op.execute(
        "UPDATE underlying_assets "
        "SET description = CASE "
        "WHEN description IS NULL THEN NULL "
        "ELSE json_object('en', description) "
        "END"
    )


def _upgrade_generic() -> None:
    """Upgrade JSON-capable dialects other than the supported production paths."""
    op.alter_column(
        "underlying_assets",
        "description",
        existing_type=sa.String(256),
        type_=sa.JSON(),
        existing_nullable=True,
    )


def _downgrade_postgresql() -> None:
    """Downgrade the Postgres column to scalar English descriptions."""
    op.execute(
        "ALTER TABLE underlying_assets "
        "ALTER COLUMN description TYPE VARCHAR(256) "
        "USING CASE "
        "WHEN description IS NULL THEN NULL "
        "ELSE description ->> 'en' "
        "END"
    )


def _downgrade_sqlite() -> None:
    """Downgrade the SQLite column through Alembic batch recreation."""
    op.execute(
        "UPDATE underlying_assets "
        "SET description = CASE "
        "WHEN description IS NULL THEN NULL "
        "ELSE json_extract(description, '$.en') "
        "END"
    )
    with op.batch_alter_table("underlying_assets") as batch_op:
        batch_op.alter_column(
            "description",
            existing_type=sa.JSON(),
            type_=sa.String(256),
            existing_nullable=True,
        )


def _downgrade_generic() -> None:
    """Downgrade JSON-capable dialects other than the supported production paths."""
    op.alter_column(
        "underlying_assets",
        "description",
        existing_type=sa.JSON(),
        type_=sa.String(256),
        existing_nullable=True,
    )


def upgrade() -> None:
    """Apply the migration for the active database dialect."""
    dialect_name = op.get_bind().dialect.name
    if dialect_name == "postgresql":
        _upgrade_postgresql()
        return
    if dialect_name == "sqlite":
        _upgrade_sqlite()
        return
    _upgrade_generic()


def downgrade() -> None:
    """Revert the migration for the active database dialect."""
    dialect_name = op.get_bind().dialect.name
    if dialect_name == "postgresql":
        _downgrade_postgresql()
        return
    if dialect_name == "sqlite":
        _downgrade_sqlite()
        return
    _downgrade_generic()
