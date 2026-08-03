"""Allow bounded carry-forward of an FX mark into a gap minute."""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

from snapper.data.fx_conversion_carry import MAX_CARRIED_MINUTES
from snapper.data.fx_conversion_triggers import drop_fx_conversion_immutability_triggers
from snapper.data.fx_conversion_triggers import install_fx_conversion_immutability_triggers

revision: str = "0045"
down_revision: str | None = "0044"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_EXACT_MINUTE_PG = "conversion_minute = candle_open_minute + INTERVAL '1 minute'"
_EXACT_MINUTE_SQLITE = "datetime(conversion_minute) = datetime(candle_open_minute, '+1 minute')"

_CARRY_MINUTE_PG = (
    "conversion_minute = candle_open_minute + INTERVAL '1 minute' "
    "+ carried_minutes * INTERVAL '1 minute'"
)
_CARRY_MINUTE_SQLITE = (
    "datetime(conversion_minute) = "
    "datetime(candle_open_minute, '+' || (carried_minutes + 1) || ' minutes')"
)

_COMPLETENESS_BEFORE = "completeness_state IN ('complete', 'partial', 'refused')"
_COMPLETENESS_AFTER = "completeness_state IN ('complete', 'carried', 'partial', 'refused')"

_RESOLVED_SELECTION = (
    "selected_source_exchange IS NOT NULL AND selected_source_instrument_public_id IS NOT NULL "
    "AND selected_native_symbol IS NOT NULL AND selected_base IS NOT NULL "
    "AND selected_quote IS NOT NULL AND selected_orientation IS NOT NULL"
)

_OUTCOME_BEFORE = (
    "(completeness_state = 'complete' AND refusal_reason_json IS NULL AND "
    "refusal_reason_digest IS NULL AND "
    f"{_RESOLVED_SELECTION}) OR "
    "(completeness_state = 'partial' AND refusal_reason_json IS NOT NULL AND "
    "refusal_reason_digest IS NOT NULL AND "
    f"{_RESOLVED_SELECTION}) OR "
    "(completeness_state = 'refused' AND refusal_reason_json IS NOT NULL AND "
    "refusal_reason_digest IS NOT NULL AND "
    "selected_source_exchange IS NULL AND selected_source_instrument_public_id IS NULL "
    "AND selected_native_symbol IS NULL AND selected_base IS NULL "
    "AND selected_quote IS NULL AND selected_orientation IS NULL)"
)

_OUTCOME_AFTER = (
    "(completeness_state IN ('complete', 'carried') AND refusal_reason_json IS NULL AND "
    "refusal_reason_digest IS NULL AND "
    f"{_RESOLVED_SELECTION}) OR "
    "(completeness_state = 'partial' AND refusal_reason_json IS NOT NULL AND "
    "refusal_reason_digest IS NOT NULL AND "
    f"{_RESOLVED_SELECTION}) OR "
    "(completeness_state = 'refused' AND refusal_reason_json IS NOT NULL AND "
    "refusal_reason_digest IS NOT NULL AND "
    "selected_source_exchange IS NULL AND selected_source_instrument_public_id IS NULL "
    "AND selected_native_symbol IS NULL AND selected_base IS NULL "
    "AND selected_quote IS NULL AND selected_orientation IS NULL)"
)


def _carry_minute_sql() -> str:
    """Return the dialect-specific carried-minute equality predicate."""
    return _CARRY_MINUTE_SQLITE if op.get_bind().dialect.name == "sqlite" else _CARRY_MINUTE_PG


def _exact_minute_sql() -> str:
    """Return the dialect-specific pre-carry exact-minute predicate."""
    return _EXACT_MINUTE_SQLITE if op.get_bind().dialect.name == "sqlite" else _EXACT_MINUTE_PG


def upgrade() -> None:
    """Widen the proof minute rule and admit the carried election outcome.

    The append-only triggers are dropped first because SQLite implements a
    CHECK change by recreating the table, which would otherwise leave the
    triggers bound to a table that no longer exists.
    """
    drop_fx_conversion_immutability_triggers(op.get_bind())
    with op.batch_alter_table("fx_conversion_proofs", recreate="always") as batch:
        batch.add_column(
            sa.Column(
                "carried_minutes",
                sa.Integer(),
                nullable=False,
                server_default=sa.text("0"),
            )
        )
        batch.drop_constraint("ck_fx_proofs_exact_minute", type_="check")
        batch.create_check_constraint("ck_fx_proofs_carried_minute", _carry_minute_sql())
        batch.create_check_constraint(
            "ck_fx_proofs_carried_bound",
            f"carried_minutes BETWEEN 0 AND {MAX_CARRIED_MINUTES}",
        )
    with op.batch_alter_table("fx_conversion_elections", recreate="always") as batch:
        batch.drop_constraint("ck_fx_elections_completeness", type_="check")
        batch.drop_constraint("ck_fx_elections_outcome", type_="check")
        batch.create_check_constraint("ck_fx_elections_completeness", _COMPLETENESS_AFTER)
        batch.create_check_constraint("ck_fx_elections_outcome", _OUTCOME_AFTER)
    install_fx_conversion_immutability_triggers(op.get_bind())


def downgrade() -> None:
    """Restore the exact-minute rule, refusing if carried proofs exist.

    Raises:
        RuntimeError: If any proof was written with a carried mark, because
            restoring the exact-minute CHECK would silently orphan evidence
            that a P&L replay still depends on.
    """
    bind = op.get_bind()
    carried = bind.execute(
        sa.text("SELECT count(*) FROM fx_conversion_proofs WHERE carried_minutes > 0")
    ).scalar_one()
    if carried:
        raise RuntimeError(
            f"refused: {carried} proof(s) carry a mark across a gap; "
            "downgrading would drop evidence a replay depends on"
        )
    drop_fx_conversion_immutability_triggers(bind)
    with op.batch_alter_table("fx_conversion_elections", recreate="always") as batch:
        batch.drop_constraint("ck_fx_elections_completeness", type_="check")
        batch.drop_constraint("ck_fx_elections_outcome", type_="check")
        batch.create_check_constraint("ck_fx_elections_completeness", _COMPLETENESS_BEFORE)
        batch.create_check_constraint("ck_fx_elections_outcome", _OUTCOME_BEFORE)
    with op.batch_alter_table("fx_conversion_proofs", recreate="always") as batch:
        batch.drop_constraint("ck_fx_proofs_carried_bound", type_="check")
        batch.drop_constraint("ck_fx_proofs_carried_minute", type_="check")
        batch.create_check_constraint("ck_fx_proofs_exact_minute", _exact_minute_sql())
        batch.drop_column("carried_minutes")
    install_fx_conversion_immutability_triggers(bind)
