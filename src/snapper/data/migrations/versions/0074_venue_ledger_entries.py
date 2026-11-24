"""Add append-only Kraken venue ledger entries and the ingest cursor.

Observation storage for accounts whose reconciliation method is
``margin_ledger_replay``. The tables are additive. Existing reconciliation
checks and rows are left unchanged. Revises 0073.
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

from snapper.data.models import CK_VENUE_LEDGER_CURSOR_COUNTS
from snapper.data.models import CK_VENUE_LEDGER_ENTRY_TEXT
from snapper.data.models import CK_VENUE_LEDGER_EVIDENCE_STATUS
from snapper.data.models import CK_VENUE_LEDGER_EXCHANGE
from snapper.data.models import CK_VENUE_LEDGER_MODE
from snapper.data.models import CK_VENUE_LEDGER_WINDOW
from snapper.data.models import TZDateTime
from snapper.data.models import UUIDColumn

revision: str = "0074"
down_revision: str | None = "0073"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Create ``venue_ledger_entries`` and ``venue_ledger_cursors``."""
    op.create_table(
        "venue_ledger_entries",
        sa.Column(
            "id",
            sa.BigInteger().with_variant(sa.Integer(), "sqlite"),
            primary_key=True,
            autoincrement=True,
        ),
        sa.Column("wallet_public_id", UUIDColumn(), nullable=False),
        sa.Column("exchange", sa.String(32), nullable=False),
        sa.Column("mode", sa.String(8), nullable=False),
        sa.Column("venue_ledger_id", sa.String(64), nullable=False),
        sa.Column("refid", sa.String(64), nullable=False),
        sa.Column("ledger_time_text", sa.String(64), nullable=False),
        sa.Column("ledger_type", sa.String(32), nullable=False),
        sa.Column("subtype", sa.String(32), nullable=False),
        sa.Column("aclass", sa.String(32), nullable=False),
        sa.Column("asset", sa.String(32), nullable=False),
        sa.Column("amount_text", sa.String(64), nullable=False),
        sa.Column("fee_text", sa.String(64), nullable=False),
        sa.Column("balance_text", sa.String(64), nullable=False),
        sa.Column("observed_at", TZDateTime(), nullable=False),
        sa.Column("observation_id", UUIDColumn(), nullable=False),
        sa.UniqueConstraint(
            "wallet_public_id",
            "exchange",
            "mode",
            "venue_ledger_id",
            name="uq_venue_ledger_entries_identity",
        ),
        sa.CheckConstraint(CK_VENUE_LEDGER_EXCHANGE, name="ck_venue_ledger_entries_exchange"),
        sa.CheckConstraint(CK_VENUE_LEDGER_MODE, name="ck_venue_ledger_entries_mode"),
        sa.CheckConstraint(CK_VENUE_LEDGER_ENTRY_TEXT, name="ck_venue_ledger_entries_text"),
    )
    op.create_table(
        "venue_ledger_cursors",
        sa.Column(
            "id",
            sa.BigInteger().with_variant(sa.Integer(), "sqlite"),
            primary_key=True,
            autoincrement=True,
        ),
        sa.Column("wallet_public_id", UUIDColumn(), nullable=False),
        sa.Column("exchange", sa.String(32), nullable=False),
        sa.Column("mode", sa.String(8), nullable=False),
        sa.Column(
            "cursor_second",
            sa.BigInteger().with_variant(sa.Integer(), "sqlite"),
            nullable=True,
        ),
        sa.Column("cursor_ids_json", sa.Text(), nullable=False),
        sa.Column(
            "window_start",
            sa.BigInteger().with_variant(sa.Integer(), "sqlite"),
            nullable=True,
        ),
        sa.Column(
            "window_end",
            sa.BigInteger().with_variant(sa.Integer(), "sqlite"),
            nullable=True,
        ),
        sa.Column("next_ofs", sa.Integer(), nullable=True),
        sa.Column("expected_count", sa.Integer(), nullable=True),
        sa.Column(
            "window_cursor_second",
            sa.BigInteger().with_variant(sa.Integer(), "sqlite"),
            nullable=True,
        ),
        sa.Column("window_cursor_ids_json", sa.Text(), nullable=True),
        sa.Column("boundary_as_of", TZDateTime(), nullable=False),
        sa.Column(
            "boundary_watermark",
            sa.BigInteger().with_variant(sa.Integer(), "sqlite"),
            nullable=False,
        ),
        sa.Column("boundary_session_id", UUIDColumn(), nullable=False),
        sa.Column("boundary_sequence_id", sa.Integer(), nullable=False),
        sa.Column("evidence_status", sa.String(64), nullable=True),
        sa.Column("reported_count", sa.Integer(), nullable=True),
        sa.Column("ingested_count", sa.Integer(), nullable=False),
        sa.Column("pass_started_at", TZDateTime(), nullable=False),
        sa.Column("observed_at", TZDateTime(), nullable=False),
        sa.Column("observation_id", UUIDColumn(), nullable=False),
        sa.Column("trade_balance_json", sa.Text(), nullable=True),
        sa.Column("trade_balance_observed_at", TZDateTime(), nullable=True),
        sa.Column("api_points_used", sa.Integer(), nullable=False),
        sa.UniqueConstraint(
            "wallet_public_id",
            "exchange",
            "mode",
            name="uq_venue_ledger_cursors_scope",
        ),
        sa.CheckConstraint(CK_VENUE_LEDGER_EXCHANGE, name="ck_venue_ledger_cursors_exchange"),
        sa.CheckConstraint(CK_VENUE_LEDGER_MODE, name="ck_venue_ledger_cursors_mode"),
        sa.CheckConstraint(CK_VENUE_LEDGER_EVIDENCE_STATUS, name="ck_venue_ledger_cursors_status"),
        sa.CheckConstraint(CK_VENUE_LEDGER_CURSOR_COUNTS, name="ck_venue_ledger_cursors_counts"),
        sa.CheckConstraint(CK_VENUE_LEDGER_WINDOW, name="ck_venue_ledger_cursors_window"),
    )


def downgrade() -> None:
    """Drop the Kraken ledger cursor and append-only entries tables."""
    op.drop_table("venue_ledger_cursors")
    op.drop_table("venue_ledger_entries")
