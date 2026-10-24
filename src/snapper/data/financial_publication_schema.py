"""Shared physical constraints for immutable publication intents and local delivery receipts."""

from sqlalchemy import CheckConstraint
from sqlalchemy import UniqueConstraint
from sqlalchemy.schema import Constraint

from snapper.data.contract_accounting_schema import accounting_evidence_constraints


def financial_intent_constraints() -> tuple[Constraint, ...]:
    """Create fresh identical constraints for model and migration publication-intent tables.

    Returns:
        Immutable scope, source fact and actual target uniqueness constraints.
    """
    return accounting_evidence_constraints("financial_intent") + (
        UniqueConstraint("execution_public_id", name="uq_financial_intent_execution"),
        UniqueConstraint("position_event_public_id", name="uq_financial_intent_position"),
        UniqueConstraint(
            "account_scope_digest", "source_execution_id", name="uq_financial_intent_source"
        ),
        CheckConstraint("length(account_scope_digest) = 64", name="ck_financial_intent_scope"),
        CheckConstraint("length(trim(source_execution_id)) > 0", name="ck_financial_intent_source"),
        CheckConstraint("length(trim(topic)) > 0", name="ck_financial_intent_topic"),
    )


def financial_delivery_constraints() -> tuple[Constraint, ...]:
    """Create fresh scope-bound delivery constraints without claiming consumer acknowledgement.

    Returns:
        Immutable local publisher-return evidence with one receipt per intent.
    """
    return accounting_evidence_constraints("financial_delivery", has_payload=False) + (
        UniqueConstraint("publication_public_id", name="uq_financial_delivery_publication"),
        CheckConstraint("length(publication_digest) = 64", name="ck_financial_delivery_digest"),
        CheckConstraint(
            "delivery_semantics = 'publisher_returned'", name="ck_financial_delivery_semantics"
        ),
        CheckConstraint("published_at <= timestamp", name="ck_financial_delivery_clock"),
    )
