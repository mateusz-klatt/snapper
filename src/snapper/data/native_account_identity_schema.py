"""Shared physical declarations for immutable redacted native account identity proofs."""

from sqlalchemy import CheckConstraint
from sqlalchemy import ForeignKeyConstraint
from sqlalchemy import Index
from sqlalchemy import UniqueConstraint
from sqlalchemy import text
from sqlalchemy.schema import Constraint

from snapper.data.contract_accounting_schema import accounting_evidence_constraints

NATIVE_ACCOUNT_IDENTITY_INITIAL_ROOT_KEYS: tuple[tuple[str, tuple[str, ...]], ...] = (
    (
        "uq_native_identity_initial_family",
        ("wallet_public_id", "exchange", "mode", "environment"),
    ),
    ("uq_native_identity_initial_scope", ("scope_digest",)),
)
NATIVE_ACCOUNT_IDENTITY_INITIAL_ROOT_PREDICATE = "owner_root_public_id IS NULL"


def native_account_identity_constraints() -> tuple[Constraint, ...]:
    """Keep every redacted proof tied to an actual attempt and an immutable original root.

    Returns:
        Fresh model/migration scope, digest, parent, root-tuple, permission and clock constraints.
    """
    return accounting_evidence_constraints("native_identity") + (
        CheckConstraint("margin_account IS NULL", name="ck_native_identity_no_margin"),
        CheckConstraint(
            "exchange = 'kraken_futures' AND mode = 'live' "
            "AND environment IN ('production', 'demo')",
            name="ck_native_identity_route",
        ),
        CheckConstraint("length(scope_digest) = 64", name="ck_native_identity_scope"),
        CheckConstraint("family_sequence > 0", name="ck_native_identity_sequence"),
        CheckConstraint("length(client_selection_digest) = 64", name="ck_native_identity_client"),
        CheckConstraint(
            "length(attempt_content_digest) = 64", name="ck_native_identity_attempt_digest"
        ),
        UniqueConstraint("attempt_public_id", name="uq_native_identity_attempt"),
        ForeignKeyConstraint(
            ["attempt_public_id"],
            ["native_account_read_attempts.public_id"],
            name="fk_native_identity_attempt",
        ),
        ForeignKeyConstraint(
            ["owner_root_public_id"],
            ["native_account_identity_proofs.public_id"],
            name="fk_native_identity_owner_root",
        ),
        CheckConstraint(
            "(owner_root_public_id IS NULL AND owner_root_content_digest IS NULL "
            "AND owner_root_account_uid IS NULL) OR "
            "(owner_root_public_id IS NOT NULL AND owner_root_content_digest IS NOT NULL "
            "AND owner_root_account_uid IS NOT NULL)",
            name="ck_native_identity_root_tuple",
        ),
        CheckConstraint(
            "owner_root_content_digest IS NULL OR length(owner_root_content_digest) = 64",
            name="ck_native_identity_root_digest",
        ),
        CheckConstraint(
            "owner_root_public_id IS NULL OR owner_root_public_id != public_id",
            name="ck_native_identity_root_not_self",
        ),
        CheckConstraint(
            "source_policy = 'kraken_futures_key_check_redacted_v1'",
            name="ck_native_identity_source_policy",
        ),
        CheckConstraint(
            "general_access IN ('NO_ACCESS', 'READ_ONLY', 'FULL_ACCESS')",
            name="ck_native_identity_general_access",
        ),
        CheckConstraint(
            "transfer_access IN ('NO_ACCESS', 'READ_ONLY', 'FULL_ACCESS')",
            name="ck_native_identity_transfer_access",
        ),
        CheckConstraint(
            "request_started_at <= observed_at AND observed_at <= timestamp",
            name="ck_native_identity_chronology",
        ),
    )


def native_account_identity_root_indexes() -> tuple[Index, ...]:
    """Keep initial-owner uniqueness independent of credentials, margin labels and indexed digests.

    Returns:
        Fresh partial unique indexes for the exact direct family and its coarse digest.
    """
    return tuple(
        Index(
            name,
            *columns,
            unique=True,
            sqlite_where=text(NATIVE_ACCOUNT_IDENTITY_INITIAL_ROOT_PREDICATE),
            postgresql_where=text(NATIVE_ACCOUNT_IDENTITY_INITIAL_ROOT_PREDICATE),
        )
        for name, columns in NATIVE_ACCOUNT_IDENTITY_INITIAL_ROOT_KEYS
    )
