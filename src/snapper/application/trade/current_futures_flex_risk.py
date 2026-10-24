"""Bind current flex risk observations to actual native accounts without granting funds permission.

READ_ONLY is sufficient to observe risk. A later order path must independently
require FULL_ACCESS, product/position eligibility, financial completeness,
reconciled open orders and durable reservations at its final dispatch boundary.
"""

from dataclasses import dataclass
from datetime import UTC
from datetime import datetime

from sqlalchemy.ext.asyncio import AsyncSession

from snapper.application.trade.futures_flex_risk import FuturesFlexRiskSnapshot
from snapper.application.trade.futures_flex_risk import FuturesFlexRiskSourceError
from snapper.application.trade.futures_flex_risk import parse_futures_flex_risk
from snapper.core.execution_unit_admission import ExecutionRouteReference
from snapper.core.native_account_authority import NativeAccountAuthorityPayload
from snapper.data.accounting_observation_binding import get_bound_accounting_observation
from snapper.data.execution_account_authority_store import BoundAccountAuthority
from snapper.data.execution_account_authority_store import require_current_account_authority
from snapper.data.native_account_authority_store import NativeAuthorityFreshnessPolicy


@dataclass(frozen=True, slots=True)
class CurrentFuturesFlexRiskSource:
    """Actual current account identity and original risk projection, without a spendable amount."""

    authority: BoundAccountAuthority
    snapshot: FuturesFlexRiskSnapshot
    checked_at: datetime


async def require_current_futures_flex_risk(
    session: AsyncSession,
    reference: ExecutionRouteReference,
    policy: NativeAuthorityFreshnessPolicy,
) -> CurrentFuturesFlexRiskSource:
    """Read the original guarded flex source after actual current account/proof/freshness rebinding.

    Args:
        session: Active caller-owned transaction already holding the actual wallet execution fence.
        reference: Exact selected native flex account route for this operation.
        policy: Trusted account request-age and provider-clock bounds.

    Returns:
        Original endpoint-specific risk observations and current source identity, never funds authority.

    Raises:
        RuntimeError: If no caller-owned transaction exists before current-source evaluation.
        ValueError: If the actual source, selected account, original fields or freshness cannot be proven.
    """
    if not session.in_transaction():
        raise RuntimeError("Current flex risk requires an existing wallet-fenced transaction")
    route = ExecutionRouteReference.model_validate(reference)
    if (
        route.scope.exchange != "kraken_futures"
        or route.scope.mode != "live"
        or route.scope.margin_account != "flex"
    ):
        raise FuturesFlexRiskSourceError("futures_flex_current_native_route_required")
    checked_at = datetime.now(UTC)
    authority = await require_current_account_authority(session, route, checked_at, policy)
    if authority.family != "native_accounts_v1":
        raise FuturesFlexRiskSourceError("futures_flex_current_native_source_required")
    native = NativeAccountAuthorityPayload.model_validate(authority.payload)
    raw = await get_bound_accounting_observation(session, native.native_read.raw_receipt_public_id)
    snapshot = parse_futures_flex_risk(raw)
    return CurrentFuturesFlexRiskSource(authority, snapshot, checked_at)
