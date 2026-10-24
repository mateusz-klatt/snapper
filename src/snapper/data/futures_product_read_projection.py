"""Exact immutable product lifecycle projections shared by insertion and historical verification."""

from snapper.core.execution_route_authority import client_selection_digest
from snapper.core.futures_product_read import FuturesProductReadAttempt
from snapper.core.futures_product_read import FuturesProductReadOutcome
from snapper.core.futures_product_read import canonical_futures_product_attempt
from snapper.core.futures_product_read import canonical_futures_product_outcome
from snapper.core.futures_product_read import futures_product_attempt_digest
from snapper.core.futures_product_read import futures_product_outcome_digest
from snapper.core.futures_product_read import futures_product_read_family


def product_read_scope_projection(payload: FuturesProductReadAttempt) -> dict[str, object]:
    """Keep indexed coarse-family ordering separate from credential and margin labels.

    Args:
        payload: Strict original fixed-purpose product attempt.

    Returns:
        The identical scope projection for both attempt and outcome tables.
    """
    selected = payload.client_selection
    return {
        "wallet_public_id": selected.wallet_public_id,
        "exchange": selected.exchange,
        "mode": selected.mode,
        "environment": selected.environment,
        "margin_account": None,
        "scope_digest": futures_product_read_family(selected),
        "family_sequence": payload.family_sequence,
    }


def product_read_attempt_projection(payload: FuturesProductReadAttempt) -> dict[str, object]:
    """Bind every independent indexed attempt copy to canonical public source evidence.

    Args:
        payload: Strict original attempt whose actual proof/root is independently rebound.

    Returns:
        Full stored projection, excluding independent temporal identity provenance.
    """
    return product_read_scope_projection(payload) | {
        "client_selection_digest": client_selection_digest(payload.client_selection),
        "key_proof_public_id": payload.key_proof.public_id,
        "key_proof_content_digest": payload.key_proof.content_digest,
        "started_at": payload.started_at,
        "timestamp": payload.started_at,
        "content_digest": futures_product_attempt_digest(payload),
        "canonical_payload": canonical_futures_product_attempt(payload),
    }


def product_read_outcome_projection(payload: FuturesProductReadOutcome) -> dict[str, object]:
    """Bind terminal results to the exact original attempt without financial receipt relabeling.

    Args:
        payload: Strict original result after successful entity privacy and parser validation.

    Returns:
        Full stored result projection without changing original source or completion clocks.
    """
    return product_read_scope_projection(payload.attempt) | {
        "attempt_public_id": payload.attempt_public_id,
        "attempt_content_digest": payload.attempt_content_digest,
        "completed_at": payload.completed_at,
        "timestamp": payload.completed_at,
        "result_kind": payload.result.kind,
        "content_digest": futures_product_outcome_digest(payload),
        "canonical_payload": canonical_futures_product_outcome(payload),
    }
