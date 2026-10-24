"""Current native Futures order-access prerequisite, separate from account observation.

READ_ONLY remains valid evidence for account reading and financial bookkeeping.
Only this new-order prerequisite requires actual FULL_ACCESS general permission.
It performs no venue I/O, financial posting, reservation or ownership assignment
and never issues a standalone CREATE or resend permit.
"""

from dataclasses import dataclass
from datetime import UTC
from datetime import datetime

from sqlalchemy.ext.asyncio import AsyncSession

from snapper.core.execution_unit_admission import ExecutionRouteReference
from snapper.core.native_account_authority import NativeAccountAuthorityPayload
from snapper.data.execution_account_authority_store import BoundAccountAuthority
from snapper.data.execution_account_authority_store import require_current_account_authority
from snapper.data.native_account_authority_store import NativeAuthorityFreshnessPolicy
from snapper.data.native_account_identity_store import BoundNativeAccountIdentityProof
from snapper.data.native_account_identity_store import get_bound_native_account_identity


class NativeFuturesOrderAccessUnavailableError(ValueError):
    """The current actual native source does not establish the required order-access prerequisite."""


@dataclass(frozen=True, slots=True)
class NativeFuturesOrderAccessPrerequisite:
    """Actual current authority/key-proof evidence at one caller-fenced evaluation clock.

    This public evidence object is not an opaque dispatch capability. Actual
    current source checks must run again at the physical dispatch boundary,
    alongside product eligibility, financial owner, funds, source ownership and
    the durable attempt/submit protocol. It creates no permission for another call.
    """

    authority: BoundAccountAuthority
    key_proof: BoundNativeAccountIdentityProof
    checked_at: datetime


async def require_current_native_futures_order_access(
    session: AsyncSession,
    reference: ExecutionRouteReference,
    policy: NativeAuthorityFreshnessPolicy,
) -> NativeFuturesOrderAccessPrerequisite:
    """Rebind current native authority and actual same-attempt safe key permission.

    Args:
        session: Active caller transaction already holding the actual intended wallet execution fence.
        reference: Exact current account/environment/credential route selected for this operation.
        policy: Trusted native source-age/server-clock policy, never an observed provider field.

    Returns:
        Bound public authority/key evidence showing the FULL_ACCESS prerequisite at the actual clock.

    Raises:
        RuntimeError: When no caller-owned transaction exists before the first source access.
        NativeFuturesOrderAccessUnavailableError: When the route is unsupported, legacy or read-only.
        ValueError: When actual current discovery, freshness, latest read or key-proof binding fails.
    """
    if not session.in_transaction():
        raise RuntimeError("Native order access requires an existing wallet-fenced transaction")
    actual = ExecutionRouteReference.model_validate(reference)
    if actual.scope.exchange != "kraken_futures" or actual.scope.mode != "live":
        raise NativeFuturesOrderAccessUnavailableError("native_order_access_requires_live_futures")
    checked_at = datetime.now(UTC)
    authority = await require_current_account_authority(session, actual, checked_at, policy)
    if authority.family != "native_accounts_v1":
        raise NativeFuturesOrderAccessUnavailableError(
            "native_order_access_requires_native_authority"
        )
    native = NativeAccountAuthorityPayload.model_validate(authority.payload)
    proof = await get_bound_native_account_identity(session, native.owner_proof.public_id)
    if proof.payload.observation.general_access != "FULL_ACCESS":
        raise NativeFuturesOrderAccessUnavailableError("native_order_access_requires_full_access")
    return NativeFuturesOrderAccessPrerequisite(authority, proof, checked_at)
