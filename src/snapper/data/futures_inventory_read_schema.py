"""Shared physical contracts for immutable Futures inventory read attempts and outcomes."""

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
        CheckConstraint(
            "purpose IN ('kraken_futures.open_orders', 'kraken_futures.open_positions')",
            name=f"ck_{prefix}_purpose",
        ),
    )


def futures_inventory_read_attempt_constraints() -> tuple[Constraint, ...]:
    """Allocate one immutable local ordering coordinate in an actual source family.

    Returns:
        Fresh identical model/migration scope, ordering, digest and start-clock constraints.
    """
    return _family_constraints("futures_inventory_attempt") + (
        UniqueConstraint(
            "public_id",
            "content_digest",
            "purpose",
            "scope_digest",
            "family_sequence",
            "wallet_public_id",
            "exchange",
            "mode",
            "environment",
            name="uq_futures_inventory_attempt_binding",
        ),
        UniqueConstraint(
            "scope_digest", "family_sequence", name="uq_futures_inventory_attempt_sequence"
        ),
        UniqueConstraint(
            "wallet_public_id",
            "exchange",
            "mode",
            "environment",
            "purpose",
            "family_sequence",
            name="uq_futures_inventory_attempt_family",
        ),
        CheckConstraint(
            "length(client_selection_digest) = 64", name="ck_futures_inventory_attempt_client"
        ),
        ForeignKeyConstraint(
            ["key_proof_public_id"],
            ["native_account_identity_proofs.public_id"],
            name="fk_futures_inventory_attempt_key_proof",
        ),
        CheckConstraint(
            "length(key_proof_content_digest) = 64",
            name="ck_futures_inventory_attempt_key_digest",
        ),
        CheckConstraint("started_at = timestamp", name="ck_futures_inventory_attempt_start"),
    )


def futures_inventory_read_outcome_constraints() -> tuple[Constraint, ...]:
    """Retain one honest outcome for an actual previously stored attempt.

    Returns:
        Fresh model/migration parent, outcome-kind and completion constraints.
    """
    return _family_constraints("futures_inventory_outcome") + (
        UniqueConstraint("attempt_public_id", name="uq_futures_inventory_outcome_attempt"),
        ForeignKeyConstraint(
            [
                "attempt_public_id",
                "attempt_content_digest",
                "purpose",
                "scope_digest",
                "family_sequence",
                "wallet_public_id",
                "exchange",
                "mode",
                "environment",
            ],
            [
                "futures_inventory_read_attempts.public_id",
                "futures_inventory_read_attempts.content_digest",
                "futures_inventory_read_attempts.purpose",
                "futures_inventory_read_attempts.scope_digest",
                "futures_inventory_read_attempts.family_sequence",
                "futures_inventory_read_attempts.wallet_public_id",
                "futures_inventory_read_attempts.exchange",
                "futures_inventory_read_attempts.mode",
                "futures_inventory_read_attempts.environment",
            ],
            name="fk_futures_inventory_outcome_attempt",
        ),
        CheckConstraint(
            "length(attempt_content_digest) = 64",
            name="ck_futures_inventory_outcome_attempt_digest",
        ),
        CheckConstraint("completed_at = timestamp", name="ck_futures_inventory_outcome_completion"),
        CheckConstraint(
            "result_kind IN ('captured', 'failed')", name="ck_futures_inventory_outcome_kind"
        ),
    )
