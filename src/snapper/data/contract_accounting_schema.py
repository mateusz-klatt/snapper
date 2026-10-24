"""Shared constraints for immutable accounting evidence models and migration DDL."""

from sqlalchemy import CheckConstraint
from sqlalchemy import UniqueConstraint
from sqlalchemy.schema import Constraint


def accounting_evidence_constraints(
    prefix: str, *, has_payload: bool = True
) -> tuple[Constraint, ...]:
    """Keep indexed account scope and immutability invariants identical across DDL paths.

    Args:
        prefix: Statically selected constraint-name prefix for one evidence table.
        has_payload: Whether this table retains canonical material and its content digest.

    Returns:
        Physical UUID uniqueness, valid scope/environment and permanent known-to guards.
    """
    constraints: tuple[Constraint, ...] = (
        UniqueConstraint("public_id", name=f"uq_{prefix}_public_id"),
        CheckConstraint("mode IN ('live', 'paper')", name=f"ck_{prefix}_mode"),
        CheckConstraint(
            "environment IN ('production', 'demo', 'paper')",
            name=f"ck_{prefix}_environment",
        ),
        CheckConstraint(
            "(mode = 'paper' AND environment = 'paper') OR "
            "(mode = 'live' AND environment IN ('production', 'demo'))",
            name=f"ck_{prefix}_mode_environment",
        ),
        CheckConstraint(
            "(exchange = 'paper' AND mode = 'paper') OR (exchange != 'paper' AND mode = 'live')",
            name=f"ck_{prefix}_exchange_mode",
        ),
        CheckConstraint(
            "exchange = LOWER(exchange) AND length(trim(exchange)) > 0",
            name=f"ck_{prefix}_exchange",
        ),
        CheckConstraint(
            "margin_account IS NULL OR length(trim(margin_account)) > 0",
            name=f"ck_{prefix}_margin_account",
        ),
        CheckConstraint(
            "known_to = '9999-12-31 23:59:59.000000'", name=f"ck_{prefix}_known_to_max"
        ).ddl_if(dialect="sqlite"),
        CheckConstraint(
            "known_to = TIMESTAMPTZ '9999-12-31 23:59:59+00:00'",
            name=f"ck_{prefix}_known_to_max",
        ).ddl_if(dialect="postgresql"),
    )
    if not has_payload:
        return constraints
    return constraints + (
        CheckConstraint("length(content_digest) = 64", name=f"ck_{prefix}_digest"),
        CheckConstraint("length(trim(canonical_payload)) > 0", name=f"ck_{prefix}_payload"),
    )
