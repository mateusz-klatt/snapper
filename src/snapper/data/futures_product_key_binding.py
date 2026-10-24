"""Rebind product-read key references to actual immutable safe proof and original owner history.

This historical loader neither refreshes the source nor claims current key
permissions. The separate current reader checks the latest product attempt,
selected credential and actual accounts authority under the execution fence.
"""

from sqlalchemy.ext.asyncio import AsyncSession

from snapper.core.futures_product_read import ProductReadKeyReference
from snapper.core.native_account_identity import native_account_identity_digest
from snapper.data.native_account_identity_store import BoundNativeAccountIdentityProof
from snapper.data.native_account_identity_store import get_bound_native_account_identity


class FuturesProductBindingError(ValueError):
    """Actual immutable product source, request ordering or account ownership cannot be proven."""


def product_read_key_reference(proof: BoundNativeAccountIdentityProof) -> ProductReadKeyReference:
    """Project the public reference from an actual bound proof without exposing transient key data.

    Args:
        proof: Actual historical store result; construction of this reference alone proves no source.

    Returns:
        Exact public proof, observation and original-owner coordinates for independent rebinding.
    """
    payload = proof.payload
    if not payload.matches_owner:
        raise FuturesProductBindingError("futures_product_original_owner_mismatch")
    root = payload.owner_root
    digest = native_account_identity_digest(payload)
    return ProductReadKeyReference(
        public_id=proof.public_id,
        content_digest=digest,
        read_attempt_public_id=payload.attempt_public_id,
        account_uid=payload.observation.account_uid,
        client_selection=payload.observation.client_selection,
        key_owner_root_public_id=proof.public_id if root is None else root.public_id,
        key_owner_root_content_digest=digest if root is None else root.content_digest,
        observed_at=payload.observation.observed_at,
        recorded_at=payload.recorded_at,
    )


async def get_bound_product_read_key(
    session: AsyncSession, reference: ProductReadKeyReference
) -> BoundNativeAccountIdentityProof:
    """Reload the actual proof, account-read attempt and first owner before trusting a product copy.

    Args:
        session: Caller-owned transaction with no implicit commit or current-permission claim.
        reference: Strict public coordinates retained inside the original product attempt.

    Returns:
        Independently rebound historical proof with matching actual original owner and all clocks.

    Raises:
        FuturesProductBindingError: If any retained copy disagrees with its original immutable source.
    """
    expected = ProductReadKeyReference.model_validate(reference)
    actual = await get_bound_native_account_identity(session, expected.public_id)
    if product_read_key_reference(actual) != expected:
        raise FuturesProductBindingError("futures_product_actual_key_proof_mismatch")
    return actual
