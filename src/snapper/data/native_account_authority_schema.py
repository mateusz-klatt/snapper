"""Shared physical contract for source-bound native account authorities without renewable old reads."""

from sqlalchemy import CheckConstraint
from sqlalchemy import ForeignKeyConstraint
from sqlalchemy import UniqueConstraint
from sqlalchemy.schema import Constraint

from snapper.data.contract_accounting_schema import accounting_evidence_constraints


def native_account_authority_constraints() -> tuple[Constraint, ...]:
    """Bind one immutable literal native profile to its real read graph and original validity.

    Returns:
        Fresh model/migration source, graph, digest, issuance and nonrenewable-profile constraints.
    """
    return accounting_evidence_constraints("native_authority") + (
        UniqueConstraint("content_digest", name="uq_native_authority_content"),
        UniqueConstraint(
            "read_attempt_public_id", "margin_account", name="uq_native_authority_read_profile"
        ),
        CheckConstraint(
            "exchange = 'kraken_futures' AND mode = 'live' AND environment IN ('production', 'demo')",
            name="ck_native_authority_route",
        ),
        CheckConstraint("length(account_scope_digest) = 64", name="ck_native_authority_scope"),
        CheckConstraint("length(route_revision) = 64", name="ck_native_authority_revision"),
        CheckConstraint(
            "observed_at <= timestamp AND timestamp < valid_until",
            name="ck_native_authority_chronology",
        ),
        CheckConstraint(
            "credential_timestamp <= observed_at", name="ck_native_authority_credential_clock"
        ),
        CheckConstraint(
            "(environment = 'production' AND public_api_origin = 'https://futures.kraken.com') OR (environment = 'demo' AND public_api_origin = 'https://demo-futures.kraken.com')",
            name="ck_native_authority_origin",
        ),
        ForeignKeyConstraint(
            ["owner_proof_public_id"],
            ["native_account_identity_proofs.public_id"],
            name="fk_native_authority_owner_proof",
        ),
        ForeignKeyConstraint(
            ["read_attempt_public_id"],
            ["native_account_read_attempts.public_id"],
            name="fk_native_authority_read_attempt",
        ),
        ForeignKeyConstraint(
            ["read_outcome_public_id"],
            ["native_account_read_outcomes.public_id"],
            name="fk_native_authority_read_outcome",
        ),
        ForeignKeyConstraint(
            ["raw_receipt_public_id"],
            ["accounting_raw_receipts.public_id"],
            name="fk_native_authority_raw_receipt",
        ),
    )
