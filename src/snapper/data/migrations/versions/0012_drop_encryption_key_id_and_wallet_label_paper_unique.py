"""Drop wallet_credentials.encryption_key_id + rework wallets unique index.

Post-Phase-0c cleanup item 0 — seed hygiene for the multi-tenant
foundation. Two decoupled schema changes bundled into one migration
because they share the same "wallet model cleanup" theme:

1. **DROP ``wallet_credentials.encryption_key_id``.** The column was
   a placeholder for a future multi-key vault / KMS rotation contract
   (plan Section 4.2). The settings table already proves that a
   single master-password Fernet scheme is sufficient for Snapper's
   scale — when the master rotates, all encrypted values re-encrypt
   in lockstep and there is never an overlap window where multiple
   active keys must coexist. There is no scenario today that benefits
   from a per-row ``encryption_key_id`` identifier, so the column is
   dead weight and its ``"v1"`` sentinel is a maintenance hazard.
   Rotation semantics align with the settings table: rotate master
   password → re-encrypt all wallet_credentials rows → restart.

2. **Rework ``wallets`` unique index from ``(label)`` to
   ``(label, is_paper)``.** Wallet labels should describe identity
   (``default``, ``alpha``, ``alice``) and the ``is_paper`` flag
   disambiguates mode. Two wallets named ``default`` — one paper,
   one live — must coexist in a typical deployment (paper as test
   bed, live for real trading). Tightening the unique key to include
   ``is_paper`` is the only change needed to support this.

Both changes are SQLite-safe via ``batch_alter_table(recreate="auto")``
— SQLite does not natively support DROP COLUMN or ALTER INDEX on
partial indexes, so batch mode copies the table and re-creates
constraints from the new definition.

Plan and rationale: ``proprietary/plans/plan_multi_tenant_foundation.md``
Sections 4.2 + post-0c cleanup backlog item 0.
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0012"
down_revision: str = "0011"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_KNOWN_TO_ACTIVE_SQLITE: str = "known_to = '9999-12-31 23:59:59.000000'"
_KNOWN_TO_ACTIVE_PG: str = "known_to = '9999-12-31T23:59:59+00:00'"


def upgrade() -> None:
    """Drop encryption_key_id + swap wallets unique index to (label, is_paper)."""
    with op.batch_alter_table("wallet_credentials", recreate="auto") as batch_op:
        batch_op.drop_column("encryption_key_id")

    with op.batch_alter_table("wallets", recreate="auto") as batch_op:
        batch_op.drop_index("ix_wallets_label_active")
        batch_op.create_index(
            "ix_wallets_label_is_paper_active",
            ["label", "is_paper"],
            unique=True,
            sqlite_where=sa.text(_KNOWN_TO_ACTIVE_SQLITE),
            postgresql_where=sa.text(_KNOWN_TO_ACTIVE_PG),
        )


def downgrade() -> None:
    """Restore encryption_key_id + the ``(label)``-only wallets unique index."""
    with op.batch_alter_table("wallets", recreate="auto") as batch_op:
        batch_op.drop_index("ix_wallets_label_is_paper_active")
        batch_op.create_index(
            "ix_wallets_label_active",
            ["label"],
            unique=True,
            sqlite_where=sa.text(_KNOWN_TO_ACTIVE_SQLITE),
            postgresql_where=sa.text(_KNOWN_TO_ACTIVE_PG),
        )

    with op.batch_alter_table("wallet_credentials", recreate="auto") as batch_op:
        batch_op.add_column(
            sa.Column(
                "encryption_key_id",
                sa.String(64),
                nullable=False,
                server_default="v1",
            )
        )
