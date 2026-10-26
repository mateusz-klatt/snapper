"""Rebind inventory key references to actual native proof, request and original owner history.

This historical reader does not refresh key permissions or select a margin
profile. READ_ONLY remains a valid observation source. Current selection,
latest inventory attempts, funds and order permission are separate decisions.
"""

from sqlalchemy.ext.asyncio import AsyncSession

from snapper.core.futures_inventory_read import InventoryReadKeyReference
from snapper.core.native_account_identity import NativeAccountIdentityProofPayload
from snapper.core.native_account_identity import native_account_identity_digest
from snapper.data.native_account_identity_store import BoundNativeAccountIdentityProof
from snapper.data.native_account_identity_store import get_bound_native_account_identity


class FuturesInventoryBindingError(ValueError):
    """Actual immutable inventory source, request or original owner cannot be rebound."""


def inventory_read_key_reference(
    proof: BoundNativeAccountIdentityProof,
) -> InventoryReadKeyReference:
    """Project exact public coordinates from a bound proof without claiming source from shape.

    Args:
        proof: Actual historical loader result with verified original request/root history.

    Returns:
        Original proof, owner, credential and knowledge coordinates without secret material.
    """
    payload = NativeAccountIdentityProofPayload.model_validate(proof.payload)
    if not payload.matches_owner:
        raise FuturesInventoryBindingError("futures_inventory_original_owner_mismatch")
    root = payload.owner_root
    digest = native_account_identity_digest(payload)
    return InventoryReadKeyReference(
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


async def get_bound_inventory_read_key(
    session: AsyncSession, reference: InventoryReadKeyReference
) -> BoundNativeAccountIdentityProof:
    """Load actual native proof/request/root history before accepting any retained inventory copy.

    Args:
        session: Caller-owned historical or fenced transaction, without an implicit commit.
        reference: Original strict public proof reference from the independently ordered request.

    Returns:
        The actual historical native proof after every public coordinate matches its real source.

    Raises:
        FuturesInventoryBindingError: If proof/root loading or any retained reference fails binding.
    """
    try:
        expected = InventoryReadKeyReference.model_validate(reference)
        actual = await get_bound_native_account_identity(session, expected.public_id)
        matches = inventory_read_key_reference(actual) == expected
    except ValueError:
        raise FuturesInventoryBindingError(
            "futures_inventory_actual_key_proof_unavailable"
        ) from None
    if not matches:
        raise FuturesInventoryBindingError("futures_inventory_actual_key_proof_mismatch")
    return actual
