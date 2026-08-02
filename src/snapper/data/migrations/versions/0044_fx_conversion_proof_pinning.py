"""Add bitemporal FX conversion election and proof artifacts."""

from collections.abc import Sequence
from datetime import datetime

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

from snapper.data.fx_conversion_triggers import drop_fx_conversion_immutability_triggers
from snapper.data.fx_conversion_triggers import install_fx_conversion_immutability_triggers

revision: str = "0044"
down_revision: str | None = "0043"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_ACTIVE_SQLITE = sa.text("known_to = '9999-12-31 23:59:59.000000'")
_ACTIVE_PG = sa.text("known_to = '9999-12-31T23:59:59+00:00'")
_UUID = sa.String(36).with_variant(postgresql.UUID(as_uuid=False), "postgresql")
_TIMESTAMP = sa.DateTime(timezone=True)


def _temporal_columns() -> tuple[
    sa.Column[int],
    sa.Column[str],
    sa.Column[str],
    sa.Column[int],
    sa.Column[datetime],
    sa.Column[datetime],
]:
    """Build the standard Snapper temporal column set for one new table."""
    return (
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("public_id", _UUID, nullable=False),
        sa.Column("session_id", _UUID, nullable=False),
        sa.Column("sequence_id", sa.Integer(), nullable=False),
        sa.Column("timestamp", _TIMESTAMP, nullable=False),
        sa.Column("known_to", _TIMESTAMP, nullable=False),
    )


def upgrade() -> None:
    """Create the additive election and proof tables and active indexes."""
    op.create_table(
        "fx_conversion_elections",
        *_temporal_columns(),
        sa.Column("scope_kind", sa.String(24), nullable=False),
        sa.Column("consumer_instrument_public_id", _UUID, nullable=True),
        sa.Column("source_currency", sa.String(16), nullable=False),
        sa.Column("target_currency", sa.String(16), nullable=False),
        sa.Column("unordered_pair", sa.String(33), nullable=False),
        sa.Column("requirement_manifest_digest", sa.String(64), nullable=False),
        sa.Column("requested_knowledge_at", _TIMESTAMP, nullable=False),
        sa.Column("resolved_knowledge_at", _TIMESTAMP, nullable=False),
        sa.Column("election_policy_version", sa.String(32), nullable=False),
        sa.Column("calculation_version", sa.String(32), nullable=False),
        sa.Column("selected_source_exchange", sa.String(32), nullable=True),
        sa.Column("selected_source_instrument_public_id", _UUID, nullable=True),
        sa.Column("selected_native_symbol", sa.String(128), nullable=True),
        sa.Column("selected_base", sa.String(16), nullable=True),
        sa.Column("selected_quote", sa.String(16), nullable=True),
        sa.Column("selected_orientation", sa.String(8), nullable=True),
        sa.Column("decision_inputs_digest", sa.String(64), nullable=False),
        sa.Column("completeness_state", sa.String(16), nullable=False),
        sa.Column("refusal_reason_json", sa.Text(), nullable=True),
        sa.CheckConstraint(
            "scope_kind IN ('shared_pair', 'instrument_owned')",
            name="ck_fx_elections_scope_kind",
        ),
        sa.CheckConstraint(
            "(scope_kind = 'shared_pair' AND consumer_instrument_public_id IS NULL) OR "
            "(scope_kind = 'instrument_owned' AND consumer_instrument_public_id IS NOT NULL)",
            name="ck_fx_elections_scope_owner",
        ),
        sa.CheckConstraint(
            "completeness_state IN ('complete', 'partial', 'refused')",
            name="ck_fx_elections_completeness",
        ),
        sa.CheckConstraint(
            "(completeness_state IN ('complete', 'partial') AND refusal_reason_json IS NULL AND "
            "selected_source_exchange IS NOT NULL AND selected_source_instrument_public_id IS NOT NULL "
            "AND selected_native_symbol IS NOT NULL AND selected_base IS NOT NULL "
            "AND selected_quote IS NOT NULL AND selected_orientation IS NOT NULL) OR "
            "(completeness_state = 'refused' AND refusal_reason_json IS NOT NULL AND "
            "selected_source_exchange IS NULL AND selected_source_instrument_public_id IS NULL "
            "AND selected_native_symbol IS NULL AND selected_base IS NULL "
            "AND selected_quote IS NULL AND selected_orientation IS NULL)",
            name="ck_fx_elections_outcome",
        ),
        sa.CheckConstraint(
            "selected_orientation IS NULL OR selected_orientation IN ('direct', 'inverse')",
            name="ck_fx_elections_orientation",
        ),
    )
    op.create_index(
        "uq_fx_elections_shared_identity",
        "fx_conversion_elections",
        [
            "requirement_manifest_digest",
            "election_policy_version",
            "calculation_version",
            "scope_kind",
            "source_currency",
            "target_currency",
            "unordered_pair",
            "resolved_knowledge_at",
        ],
        unique=True,
        sqlite_where=sa.text(
            "known_to = '9999-12-31 23:59:59.000000' AND scope_kind = 'shared_pair' "
            "AND completeness_state IN ('complete', 'partial')"
        ),
        postgresql_where=sa.text(
            "known_to = '9999-12-31T23:59:59+00:00' AND scope_kind = 'shared_pair' "
            "AND completeness_state IN ('complete', 'partial')"
        ),
    )
    op.create_index(
        "uq_fx_elections_instrument_identity",
        "fx_conversion_elections",
        [
            "requirement_manifest_digest",
            "election_policy_version",
            "calculation_version",
            "scope_kind",
            "consumer_instrument_public_id",
            "source_currency",
            "target_currency",
            "unordered_pair",
            "resolved_knowledge_at",
        ],
        unique=True,
        sqlite_where=sa.text(
            "known_to = '9999-12-31 23:59:59.000000' AND scope_kind = 'instrument_owned' "
            "AND completeness_state IN ('complete', 'partial')"
        ),
        postgresql_where=sa.text(
            "known_to = '9999-12-31T23:59:59+00:00' AND scope_kind = 'instrument_owned' "
            "AND completeness_state IN ('complete', 'partial')"
        ),
    )
    op.create_index(
        "ix_fx_elections_public_id",
        "fx_conversion_elections",
        ["public_id"],
        unique=True,
        sqlite_where=_ACTIVE_SQLITE,
        postgresql_where=_ACTIVE_PG,
    )
    exact_minute_sql = (
        "datetime(conversion_minute) = datetime(candle_open_minute, '+1 minute')"
        if op.get_bind().dialect.name == "sqlite"
        else "conversion_minute = candle_open_minute + INTERVAL '1 minute'"
    )
    op.create_table(
        "fx_conversion_proofs",
        *_temporal_columns(),
        sa.Column("election_public_id", _UUID, nullable=False),
        sa.Column("conversion_minute", _TIMESTAMP, nullable=False),
        sa.Column("candle_open_minute", _TIMESTAMP, nullable=False),
        sa.Column("candle_id", sa.BigInteger(), nullable=False),
        sa.Column("candle_public_id", _UUID, nullable=False),
        sa.Column("candle_session_id", _UUID, nullable=False),
        sa.Column("candle_sequence_id", sa.Integer(), nullable=False),
        sa.Column("candle_timestamp", _TIMESTAMP, nullable=False),
        sa.Column("candle_known_to", _TIMESTAMP, nullable=False),
        sa.Column("raw_close_decimal", sa.Text(), nullable=False),
        sa.Column("operation", sa.String(8), nullable=False),
        sa.Column("conversion_rate_decimal", sa.Text(), nullable=False),
        sa.Column("source_instrument_public_id", _UUID, nullable=False),
        sa.Column("proof_digest", sa.String(64), nullable=False),
        sa.CheckConstraint("operation IN ('direct', 'inverse')", name="ck_fx_proofs_operation"),
        sa.CheckConstraint(
            exact_minute_sql,
            name="ck_fx_proofs_exact_minute",
        ),
    )
    op.create_index(
        "uq_fx_proofs_election_minute",
        "fx_conversion_proofs",
        ["election_public_id", "conversion_minute"],
        unique=True,
        sqlite_where=_ACTIVE_SQLITE,
        postgresql_where=_ACTIVE_PG,
    )
    op.create_index(
        "ix_fx_proofs_public_id",
        "fx_conversion_proofs",
        ["public_id"],
        unique=True,
        sqlite_where=_ACTIVE_SQLITE,
        postgresql_where=_ACTIVE_PG,
    )
    install_fx_conversion_immutability_triggers(op.get_bind())


def downgrade() -> None:
    """Drop the proof artifacts in child-first order."""
    drop_fx_conversion_immutability_triggers(op.get_bind())
    op.drop_table("fx_conversion_proofs")
    op.drop_table("fx_conversion_elections")
