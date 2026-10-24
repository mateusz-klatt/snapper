"""Shared physical contracts for immutable Futures product read attempts and outcomes."""

from sqlalchemy import CheckConstraint
from sqlalchemy import ForeignKeyConstraint
from sqlalchemy import UniqueConstraint
from sqlalchemy.schema import Constraint

from snapper.data.contract_accounting_schema import accounting_evidence_constraints


def _family_constraints(prefix: str) -> tuple[Constraint, ...]:
    """Keep current read ordering independent of margin labels and credential rotation."""
    return accounting_evidence_constraints(prefix) + (
        CheckConstraint("margin_account IS NULL", name=f"ck_{prefix}_no_margin"),
        CheckConstraint(
            "exchange = 'kraken_futures' AND mode = 'live' "
            "AND environment IN ('production', 'demo')",
            name=f"ck_{prefix}_source",
        ),
        CheckConstraint("length(scope_digest) = 64", name=f"ck_{prefix}_scope"),
        CheckConstraint("family_sequence > 0", name=f"ck_{prefix}_sequence"),
    )


def futures_product_read_attempt_constraints() -> tuple[Constraint, ...]:
    """Allocate one immutable local ordering coordinate in an actual source family.

    Returns:
        Fresh identical model/migration scope, ordering, digest and start-clock constraints.
    """
    return _family_constraints("futures_product_attempt") + (
        UniqueConstraint(
            "scope_digest", "family_sequence", name="uq_futures_product_attempt_sequence"
        ),
        UniqueConstraint(
            "wallet_public_id",
            "exchange",
            "mode",
            "environment",
            "family_sequence",
            name="uq_futures_product_attempt_family",
        ),
        CheckConstraint(
            "length(client_selection_digest) = 64", name="ck_futures_product_attempt_client"
        ),
        ForeignKeyConstraint(
            ["key_proof_public_id"],
            ["native_account_identity_proofs.public_id"],
            name="fk_futures_product_attempt_key_proof",
        ),
        CheckConstraint(
            "length(key_proof_content_digest) = 64", name="ck_futures_product_attempt_key_digest"
        ),
        CheckConstraint("started_at = timestamp", name="ck_futures_product_attempt_start"),
    )


def futures_product_read_outcome_constraints() -> tuple[Constraint, ...]:
    """Retain one honest outcome for an actual previously stored attempt.

    Returns:
        Fresh model/migration parent, outcome-kind and completion constraints.
    """
    return _family_constraints("futures_product_outcome") + (
        UniqueConstraint("attempt_public_id", name="uq_futures_product_outcome_attempt"),
        ForeignKeyConstraint(
            ["attempt_public_id"],
            ["futures_product_read_attempts.public_id"],
            name="fk_futures_product_outcome_attempt",
        ),
        CheckConstraint(
            "length(attempt_content_digest) = 64", name="ck_futures_product_outcome_attempt_digest"
        ),
        CheckConstraint("completed_at = timestamp", name="ck_futures_product_outcome_completion"),
        CheckConstraint(
            "result_kind IN ('captured', 'failed')", name="ck_futures_product_outcome_kind"
        ),
    )
