"""Widen the carried-mark bound from 15 to 20 minutes.

Revision 0045 baked ``carried_minutes BETWEEN 0 AND 15`` into databases that
ran it before the constant changed; a fresh chain replay already creates the
current bound because 0045 reads :data:`MAX_CARRIED_MINUTES`. This revision
exists for the databases with the historical bound and is a harmless
drop-and-recreate of the same predicate everywhere else.
"""

from collections.abc import Sequence
from typing import Literal

import sqlalchemy as sa
from alembic import op

from snapper.data.fx_conversion_carry import MAX_CARRIED_MINUTES
from snapper.data.fx_conversion_triggers import drop_fx_conversion_immutability_triggers
from snapper.data.fx_conversion_triggers import install_fx_conversion_immutability_triggers

revision: str = "0047"
down_revision: str | None = "0046"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_PREVIOUS_BOUND = 15


def _recreate_mode() -> Literal["auto", "always"]:
    """Return the batch recreate mode this dialect actually needs.

    SQLite cannot alter a CHECK constraint in place, so its table must be
    rebuilt. PostgreSQL alters constraints directly; forcing a rebuild there
    copies every row for nothing and leaves the finished table bound to the
    ``_alembic_tmp_`` identity sequence the rebuild created.

    Returns:
        ``"always"`` on SQLite, ``"auto"`` elsewhere.
    """
    return "always" if op.get_bind().dialect.name == "sqlite" else "auto"


def _replace_bound(bound: int) -> None:
    """Recreate the carried-minutes bound CHECK with one inclusive ceiling.

    The append-only triggers are dropped first because SQLite implements a
    CHECK change by recreating the table, which would otherwise leave the
    triggers bound to a table that no longer exists.

    Args:
        bound: Inclusive ceiling the recreated constraint enforces.
    """
    bind = op.get_bind()
    drop_fx_conversion_immutability_triggers(bind)
    with op.batch_alter_table("fx_conversion_proofs", recreate=_recreate_mode()) as batch:
        batch.drop_constraint("ck_fx_proofs_carried_bound", type_="check")
        batch.create_check_constraint(
            "ck_fx_proofs_carried_bound",
            f"carried_minutes BETWEEN 0 AND {bound}",
        )
    install_fx_conversion_immutability_triggers(bind)


def upgrade() -> None:
    """Replace the carried-minutes ceiling with the current constant."""
    _replace_bound(MAX_CARRIED_MINUTES)


def downgrade() -> None:
    """Restore the 15-minute ceiling, refusing if wider carries exist.

    Raises:
        RuntimeError: If any proof carries a mark further than the historical
            bound, because tightening the CHECK under it would orphan evidence
            a P&L replay still depends on.
    """
    wider = (
        op.get_bind()
        .execute(
            sa.text(
                "SELECT count(*) FROM fx_conversion_proofs "
                f"WHERE carried_minutes > {_PREVIOUS_BOUND}"
            )
        )
        .scalar_one()
    )
    if wider:
        raise RuntimeError(
            f"refused: {wider} proof(s) carry a mark beyond {_PREVIOUS_BOUND} minutes; "
            "tightening the bound would drop evidence a replay depends on"
        )
    _replace_bound(_PREVIOUS_BOUND)
