"""Shared physical constraints for immutable native account-log identity ownership."""

from sqlalchemy import CheckConstraint
from sqlalchemy import UniqueConstraint
from sqlalchemy.schema import Constraint

from snapper.data.contract_accounting_schema import accounting_evidence_constraints


def accounting_native_source_constraints() -> tuple[Constraint, ...]:
    """Bind one canonical native log to one event across account kinds and margin labels.

    Returns:
        Identical fresh model and migration constraints for retained native source claims.
    """
    return accounting_evidence_constraints("acct_native_claim") + (
        UniqueConstraint(
            "scope_digest",
            "source_domain",
            "source_identity_digest",
            name="uq_acct_native_claim_source",
        ),
        UniqueConstraint(
            "wallet_public_id",
            "exchange",
            "mode",
            "environment",
            "source_domain",
            "source_log_id",
            name="uq_acct_native_claim_namespace",
        ),
        UniqueConstraint("accounting_event_public_id", name="uq_acct_native_claim_event"),
        CheckConstraint("length(scope_digest) = 64", name="ck_acct_native_claim_scope"),
        CheckConstraint("length(account_scope_digest) = 64", name="ck_acct_native_claim_account"),
        CheckConstraint(
            "length(source_identity_digest) = 64", name="ck_acct_native_claim_identity"
        ),
        CheckConstraint("length(semantic_digest) = 64", name="ck_acct_native_claim_semantic"),
        CheckConstraint("length(trim(booking_uid)) > 0", name="ck_acct_native_claim_booking"),
        CheckConstraint(
            "source_domain = 'kraken_futures.account_log' AND exchange = 'kraken_futures'",
            name="ck_acct_native_claim_domain",
        ),
        CheckConstraint(
            "kind IN ('native_cash', 'contract_position_fill')",
            name="ck_acct_native_claim_kind",
        ),
        CheckConstraint(
            "length(source_log_id) > 0 AND substr(source_log_id, 1, 1) BETWEEN '1' AND '9' "
            "AND source_log_id NOT GLOB '*[^0-9]*' AND instr(source_log_id, char(0)) = 0",
            name="ck_acct_native_claim_log_id",
        ).ddl_if(dialect="sqlite"),
        CheckConstraint(
            "source_log_id ~ '^[1-9]' AND source_log_id !~ '[^0-9]'",
            name="ck_acct_native_claim_log_id",
        ).ddl_if(dialect="postgresql"),
    )
