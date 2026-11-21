"""Add non-temporal instrument_spec_observation_spans and backfill active specs.

Observation spans track contiguous catalog observations of each exact
``InstrumentSpec`` version row ``(instrument_public_id, spec_public_id,
spec_timestamp)`` so value-identical updater runs extend an open span in place
instead of rewriting SCD2 rows. Backfill covers only active specs with a
non-null ``spec_observed_at`` and never modifies ``instrument_specs`` history.
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

from snapper.data.models import KNOWN_TO_MAX
from snapper.data.models import TZDateTime
from snapper.data.models import UUIDColumn

revision: str = "0072"
down_revision: str | None = "0071"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def _backfill_active_spec_spans() -> None:
    """Insert one initial span per active spec with non-null ``spec_observed_at``."""
    if op.get_context().as_sql:
        op.execute(
            sa.text(
                "INSERT INTO instrument_spec_observation_spans ("
                "instrument_public_id, spec_public_id, spec_timestamp, spec_version, "
                "first_observed_at, last_observed_at, session_id, sequence_id"
                ") SELECT "
                "instrument_public_id, public_id, timestamp, spec_version, "
                "spec_observed_at, spec_observed_at, session_id, sequence_id "
                "FROM instrument_specs "
                "WHERE known_to = TIMESTAMP WITH TIME ZONE '9999-12-31 23:59:59+00' "
                "AND spec_observed_at IS NOT NULL"
            )
        )
        return
    specs = sa.table(
        "instrument_specs",
        sa.column("instrument_public_id", UUIDColumn()),
        sa.column("public_id", UUIDColumn()),
        sa.column("timestamp", TZDateTime()),
        sa.column("known_to", TZDateTime()),
        sa.column("spec_version", sa.String(96)),
        sa.column("spec_observed_at", TZDateTime()),
        sa.column("session_id", UUIDColumn()),
        sa.column("sequence_id", sa.Integer()),
    )
    spans = sa.table(
        "instrument_spec_observation_spans",
        sa.column("instrument_public_id", UUIDColumn()),
        sa.column("spec_public_id", UUIDColumn()),
        sa.column("spec_timestamp", TZDateTime()),
        sa.column("spec_version", sa.String(96)),
        sa.column("first_observed_at", TZDateTime()),
        sa.column("last_observed_at", TZDateTime()),
        sa.column("session_id", UUIDColumn()),
        sa.column("sequence_id", sa.Integer()),
    )
    op.execute(
        sa.insert(spans).from_select(
            [
                "instrument_public_id",
                "spec_public_id",
                "spec_timestamp",
                "spec_version",
                "first_observed_at",
                "last_observed_at",
                "session_id",
                "sequence_id",
            ],
            sa.select(
                specs.c.instrument_public_id,
                specs.c.public_id,
                specs.c.timestamp,
                specs.c.spec_version,
                specs.c.spec_observed_at,
                specs.c.spec_observed_at,
                specs.c.session_id,
                specs.c.sequence_id,
            ).where(
                specs.c.known_to == KNOWN_TO_MAX,
                specs.c.spec_observed_at.is_not(None),
            ),
        )
    )


def upgrade() -> None:
    """Create ``instrument_spec_observation_spans`` and backfill active specs only."""
    op.create_table(
        "instrument_spec_observation_spans",
        sa.Column(
            "id",
            sa.BigInteger().with_variant(sa.Integer(), "sqlite"),
            primary_key=True,
            autoincrement=True,
        ),
        sa.Column("instrument_public_id", UUIDColumn(), nullable=False),
        sa.Column("spec_public_id", UUIDColumn(), nullable=False),
        sa.Column("spec_timestamp", TZDateTime(), nullable=False),
        sa.Column("spec_version", sa.String(96), nullable=True),
        sa.Column("first_observed_at", TZDateTime(), nullable=False),
        sa.Column("last_observed_at", TZDateTime(), nullable=False),
        sa.Column("session_id", UUIDColumn(), nullable=False),
        sa.Column("sequence_id", sa.Integer(), nullable=False),
        sa.CheckConstraint(
            "first_observed_at <= last_observed_at",
            name="ck_inst_spec_obs_spans_bounds",
        ),
    )
    op.create_index(
        "ix_inst_spec_obs_spans_lookup",
        "instrument_spec_observation_spans",
        [
            "instrument_public_id",
            "spec_public_id",
            "spec_timestamp",
            sa.text("last_observed_at DESC"),
        ],
    )
    _backfill_active_spec_spans()


def downgrade() -> None:
    """Drop observation spans while leaving ``instrument_specs`` untouched."""
    op.drop_index(
        "ix_inst_spec_obs_spans_lookup",
        table_name="instrument_spec_observation_spans",
    )
    op.drop_table("instrument_spec_observation_spans")
