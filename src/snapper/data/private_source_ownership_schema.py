"""Physical contract for original private receive ownership and direct recovery projections.

The canonical payload and a closed receive-ticket producer remain required.
These columns alone do not establish SDK object ownership, authentication or
financial account assignment. The fixed literals match the agreed immutable safe frame context; only the
closed actual receive path can issue authentication evidence.
"""

from sqlalchemy import CheckConstraint
from sqlalchemy import ForeignKeyConstraint
from sqlalchemy import UniqueConstraint
from sqlalchemy.schema import Constraint

from snapper.data.contract_accounting_schema import accounting_evidence_constraints

PRIVATE_SOURCE_OWNERSHIP_TABLE = "private_source_ownership_receipts"
PRIVATE_SOURCE_OWNERSHIP_FAMILY = "kraken_futures_private_ws"
PRIVATE_SOURCE_OWNERSHIP_POLICY = "kraken_futures_closed_private_transport_v1"


def private_source_ownership_constraints() -> tuple[Constraint, ...]:
    """Retain original clocks and exact source references without a renewable current permit.

    Returns:
        Fresh common, route, source, root, clock and direct-origin constraints.
    """
    return accounting_evidence_constraints("private_owner") + (
        CheckConstraint(
            "(subscription_feed = 'fills' AND raw_feed IN ('fills', 'fills_snapshot')) OR (subscription_feed = 'account_log' AND raw_feed IN ('account_log', 'account_log_snapshot'))",
            name="ck_private_owner_feed_pair",
        ),
        CheckConstraint(
            "binding_kind = 'actual_receive' OR target_json_pointer IS NOT NULL",
            name="ck_private_owner_recovery_selector",
        ),
        UniqueConstraint("raw_receipt_public_id", name="uq_private_owner_raw"),
        CheckConstraint("schema_version = 1", name="ck_private_owner_version"),
        CheckConstraint("family = 'kraken_futures_private_ws'", name="ck_private_owner_family"),
        CheckConstraint(
            "source_policy = 'kraken_futures_closed_private_transport_v1'",
            name="ck_private_owner_policy",
        ),
        CheckConstraint(
            "binding_kind IN ('actual_receive', 'recovery_projection')",
            name="ck_private_owner_kind",
        ),
        CheckConstraint(
            "exchange = 'kraken_futures' AND mode = 'live' AND environment IN ('production', 'demo')",
            name="ck_private_owner_route",
        ),
        CheckConstraint("margin_account IS NULL", name="ck_private_owner_no_margin"),
        CheckConstraint(
            "(environment = 'production' AND public_api_origin = 'https://futures.kraken.com') OR (environment = 'demo' AND public_api_origin = 'https://demo-futures.kraken.com')",
            name="ck_private_owner_origin",
        ),
        CheckConstraint("length(account_family_digest) = 64", name="ck_private_owner_scope"),
        CheckConstraint("length(client_selection_digest) = 64", name="ck_private_owner_selection"),
        CheckConstraint("length(raw_content_digest) = 64", name="ck_private_owner_raw_digest"),
        CheckConstraint("length(frame_basis_digest) = 64", name="ck_private_owner_frame_digest"),
        CheckConstraint(
            "length(key_proof_content_digest) = 64", name="ck_private_owner_key_digest"
        ),
        CheckConstraint(
            "length(key_owner_root_content_digest) = 64", name="ck_private_owner_root_digest"
        ),
        CheckConstraint(
            "raw_feed IN ('fills', 'fills_snapshot', 'account_log', 'account_log_snapshot')",
            name="ck_private_owner_feed",
        ),
        CheckConstraint(
            "credential_timestamp <= key_proof_recorded_at AND key_proof_recorded_at <= auth_encoded_at AND auth_encoded_at <= ack_observed_at AND ack_observed_at <= frame_observed_at AND frame_observed_at <= raw_observed_at AND raw_observed_at <= raw_recorded_at AND raw_recorded_at <= timestamp",
            name="ck_private_owner_chronology",
        ),
        CheckConstraint(
            "(binding_kind = 'actual_receive' AND origin_sidecar_public_id IS NULL AND origin_sidecar_content_digest IS NULL) OR (binding_kind = 'recovery_projection' AND origin_sidecar_public_id IS NOT NULL AND origin_sidecar_content_digest IS NOT NULL)",
            name="ck_private_owner_origin_tuple",
        ),
        CheckConstraint(
            "origin_sidecar_content_digest IS NULL OR length(origin_sidecar_content_digest) = 64",
            name="ck_private_owner_origin_digest",
        ),
        CheckConstraint(
            "origin_sidecar_public_id IS NULL OR origin_sidecar_public_id != public_id",
            name="ck_private_owner_not_self",
        ),
        CheckConstraint(
            "target_json_pointer IS NULL OR ((raw_feed = 'account_log' AND target_json_pointer = '/new_entry') OR (raw_feed IN ('fills', 'fills_snapshot') AND substr(target_json_pointer, 1, 7) = '/fills/' AND length(substr(target_json_pointer, 8)) > 0 AND substr(target_json_pointer, 8) NOT GLOB '*[^0-9]*' AND (substr(target_json_pointer, 8) = '0' OR substr(target_json_pointer, 8, 1) BETWEEN '1' AND '9')) OR (raw_feed = 'account_log_snapshot' AND substr(target_json_pointer, 1, 6) = '/logs/' AND length(substr(target_json_pointer, 7)) > 0 AND substr(target_json_pointer, 7) NOT GLOB '*[^0-9]*' AND (substr(target_json_pointer, 7) = '0' OR substr(target_json_pointer, 7, 1) BETWEEN '1' AND '9')))",
            name="ck_private_owner_selector",
        ).ddl_if(dialect="sqlite"),
        CheckConstraint(
            "target_json_pointer IS NULL OR ((raw_feed = 'account_log' AND target_json_pointer = '/new_entry') OR (raw_feed IN ('fills', 'fills_snapshot') AND target_json_pointer ~ '^/fills/(0|[1-9][0-9]*)$') OR (raw_feed = 'account_log_snapshot' AND target_json_pointer ~ '^/logs/(0|[1-9][0-9]*)$'))",
            name="ck_private_owner_selector",
        ).ddl_if(dialect="postgresql"),
        ForeignKeyConstraint(
            ["raw_receipt_public_id"],
            ["accounting_raw_receipts.public_id"],
            name="fk_private_owner_raw",
        ),
        ForeignKeyConstraint(
            ["key_proof_public_id"],
            ["native_account_identity_proofs.public_id"],
            name="fk_private_owner_key_proof",
        ),
        ForeignKeyConstraint(
            ["key_owner_root_public_id"],
            ["native_account_identity_proofs.public_id"],
            name="fk_private_owner_root",
        ),
        ForeignKeyConstraint(
            ["origin_sidecar_public_id"],
            ["private_source_ownership_receipts.public_id"],
            name="fk_private_owner_original_receive",
        ),
    )
