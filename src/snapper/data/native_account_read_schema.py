"""Shared physical contracts for immutable native account read attempts and outcomes."""

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


def native_account_read_attempt_constraints() -> tuple[Constraint, ...]:
    """Allocate one immutable local ordering coordinate in an actual source family.

    Returns:
        Fresh identical model/migration scope, ordering, digest and start-clock constraints.
    """
    return _family_constraints("native_read_attempt") + (
        UniqueConstraint("scope_digest", "family_sequence", name="uq_native_read_attempt_sequence"),
        UniqueConstraint(
            "wallet_public_id",
            "exchange",
            "mode",
            "environment",
            "family_sequence",
            name="uq_native_read_attempt_family",
        ),
        CheckConstraint(
            "length(client_selection_digest) = 64", name="ck_native_read_attempt_client"
        ),
        CheckConstraint("started_at = timestamp", name="ck_native_read_attempt_start"),
    )


def native_account_read_outcome_constraints() -> tuple[Constraint, ...]:
    """Retain one honest outcome for an actual previously stored attempt.

    Returns:
        Fresh model/migration parent, outcome-kind, captured-source and completion constraints.
    """
    return _family_constraints("native_read_outcome") + (
        UniqueConstraint("attempt_public_id", name="uq_native_read_outcome_attempt"),
        UniqueConstraint("raw_receipt_public_id", name="uq_native_read_outcome_raw"),
        ForeignKeyConstraint(
            ["attempt_public_id"],
            ["native_account_read_attempts.public_id"],
            name="fk_native_read_outcome_attempt",
        ),
        ForeignKeyConstraint(
            ["raw_receipt_public_id"],
            ["accounting_raw_receipts.public_id"],
            name="fk_native_read_outcome_raw",
        ),
        CheckConstraint(
            "length(attempt_content_digest) = 64", name="ck_native_read_outcome_attempt_digest"
        ),
        CheckConstraint("completed_at = timestamp", name="ck_native_read_outcome_completion"),
        CheckConstraint(
            "result_kind IN ('captured', 'failed')", name="ck_native_read_outcome_kind"
        ),
        CheckConstraint(
            "result_kind = 'failed' OR raw_receipt_public_id IS NOT NULL",
            name="ck_native_read_outcome_capture_source",
        ),
    )
