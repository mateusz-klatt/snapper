"""Exact immutable inventory lifecycle projections for insertion and historical verification.

Source/privacy and full semantic binding precede outcome projection. These
helpers preserve original clocks and the separate orders/positions family;
canonical/hash helpers cannot create actual transport or committed evidence.
"""

from snapper.core.execution_route_authority import client_selection_digest
from snapper.core.futures_inventory_read import FuturesInventoryReadAttempt
from snapper.core.futures_inventory_read import FuturesInventoryReadOutcome
from snapper.core.futures_inventory_read import canonical_futures_inventory_attempt
from snapper.core.futures_inventory_read import canonical_futures_inventory_outcome
from snapper.core.futures_inventory_read import futures_inventory_attempt_digest
from snapper.core.futures_inventory_read import futures_inventory_outcome_digest
from snapper.core.futures_inventory_read import futures_inventory_read_family


def inventory_read_scope_projection(payload: FuturesInventoryReadAttempt) -> dict[str, object]:
    """Keep endpoint ordering distinct while spanning credential revisions and margin profiles.

    Args:
        payload: Strict original purpose-specific inventory request.

    Returns:
        Identical family projection for request and result rows, with no invented margin selector.
    """
    proven = FuturesInventoryReadAttempt.model_validate(payload)
    selected = proven.client_selection
    return {
        "wallet_public_id": selected.wallet_public_id,
        "exchange": selected.exchange,
        "mode": selected.mode,
        "environment": selected.environment,
        "margin_account": None,
        "scope_digest": futures_inventory_read_family(selected, proven.purpose),
        "purpose": proven.purpose,
        "family_sequence": proven.family_sequence,
    }


def inventory_read_attempt_projection(payload: FuturesInventoryReadAttempt) -> dict[str, object]:
    """Bind every indexed request field to the exact canonical public source reference.

    Args:
        payload: Strict original request whose actual proof/root is independently rebound.

    Returns:
        Complete request projection excluding independently supplied temporal insertion provenance.
    """
    proven = FuturesInventoryReadAttempt.model_validate(payload)
    return inventory_read_scope_projection(proven) | {
        "client_selection_digest": client_selection_digest(proven.client_selection),
        "key_proof_public_id": proven.key_proof.public_id,
        "key_proof_content_digest": proven.key_proof.content_digest,
        "started_at": proven.started_at,
        "timestamp": proven.started_at,
        "content_digest": futures_inventory_attempt_digest(proven),
        "canonical_payload": canonical_futures_inventory_attempt(proven),
    }


def inventory_read_outcome_projection(payload: FuturesInventoryReadOutcome) -> dict[str, object]:
    """Preserve the original request and result after independent full source/semantic validation.

    Args:
        payload: Strict original outcome after independent writer or historical source checks.

    Returns:
        Complete outcome projection without using a financial raw receipt or renewing source age.
    """
    proven = FuturesInventoryReadOutcome.model_validate(payload)
    return inventory_read_scope_projection(proven.attempt) | {
        "attempt_public_id": proven.attempt_public_id,
        "attempt_content_digest": proven.attempt_content_digest,
        "completed_at": proven.completed_at,
        "timestamp": proven.completed_at,
        "result_kind": proven.result.kind,
        "content_digest": futures_inventory_outcome_digest(proven),
        "canonical_payload": canonical_futures_inventory_outcome(proven),
    }
