"""Recheck current authenticated product restrictions beside actual native order-access evidence.

This is a prerequisite read under the caller's existing execution fence. It
neither reserves funds nor checks financial ownership, position reduction or
outgoing post-only operands, and it issues no physical dispatch capability.
"""

import base64
from dataclasses import dataclass
from datetime import timedelta

from sqlalchemy.ext.asyncio import AsyncSession

from snapper.application.trade.futures_product_source import parse_safe_futures_product_source
from snapper.application.trade.futures_trading_instruments import FuturesProductRequirements
from snapper.application.trade.futures_trading_instruments import futures_product_requirements
from snapper.application.trade.native_order_access import NativeFuturesOrderAccessPrerequisite
from snapper.application.trade.native_order_access import (
    require_current_native_futures_order_access,
)
from snapper.core.execution_unit_admission import ExecutionRouteReference
from snapper.core.futures_product_read import FuturesProductReadSource
from snapper.data.futures_product_key_binding import FuturesProductBindingError
from snapper.data.futures_product_key_binding import product_read_key_reference
from snapper.data.futures_product_read_store import BoundFuturesProductOutcome
from snapper.data.futures_product_read_store import get_latest_futures_product_read
from snapper.data.native_account_authority_store import NativeAuthorityFreshnessPolicy


@dataclass(frozen=True, slots=True)
class FuturesProductFreshnessPolicy:
    """Trusted current age and provider-clock bounds independent of response recording time."""

    native_accounts: NativeAuthorityFreshnessPolicy
    maximum_product_source_age: timedelta
    maximum_product_server_clock_skew: timedelta

    def __post_init__(self) -> None:
        """Require finite positive source age and a nonnegative provider-clock comparison bound."""
        if self.maximum_product_source_age <= timedelta(
            0
        ) or self.maximum_product_server_clock_skew < timedelta(0):
            raise ValueError("Product freshness bounds must be positive and nonnegative")


@dataclass(frozen=True, slots=True)
class CurrentFuturesProductPrerequisite:
    """Actual current native/key/product evidence and still-unsatisfied outgoing order conditions."""

    access: NativeFuturesOrderAccessPrerequisite
    product_outcome: BoundFuturesProductOutcome
    requirements: FuturesProductRequirements


def _require_compatible_account(
    outcome: BoundFuturesProductOutcome, access: NativeFuturesOrderAccessPrerequisite
) -> None:
    """Compare actual account, root and credential identity without equating independent proof clocks."""
    product = outcome.payload.attempt.key_proof
    account = product_read_key_reference(access.key_proof)
    if (
        product.client_selection,
        product.account_uid,
        product.key_owner_root_public_id,
        product.key_owner_root_content_digest,
    ) != (
        account.client_selection,
        account.account_uid,
        account.key_owner_root_public_id,
        account.key_owner_root_content_digest,
    ):
        raise FuturesProductBindingError("futures_product_current_account_or_credential_changed")


def _require_original_product_age(
    source: FuturesProductReadSource,
    access: NativeFuturesOrderAccessPrerequisite,
    policy: FuturesProductFreshnessPolicy,
) -> None:
    """Refuse delayed capture or recording without allowing a later completion to renew source age."""
    now = access.checked_at
    if not (
        source.request_started_at <= source.observed_at <= now
        and now - source.request_started_at < policy.maximum_product_source_age
    ):
        raise FuturesProductBindingError("futures_product_original_source_stale")


async def require_current_futures_product(
    session: AsyncSession,
    route: ExecutionRouteReference,
    wire_symbol: str,
    policy: FuturesProductFreshnessPolicy,
) -> CurrentFuturesProductPrerequisite:
    """Require current native access and the newest actual same-account product observation.

    Args:
        session: Existing caller transaction already holding the actual wallet execution fence.
        route: Exact actual native account route selected by the original order operation.
        wire_symbol: Exact original outgoing exchange symbol, already resolved from its bound catalog.
        policy: Trusted current account and product source-age/server-clock policy.

    Returns:
        Current source evidence plus necessary post-only and reduction conditions, never a grant.

    Raises:
        ValueError: If actual access, latest source, account equivalence, source age or flags fail.
    """
    access = await require_current_native_futures_order_access(
        session, route, policy.native_accounts
    )
    selected = access.key_proof.payload.observation.client_selection
    latest = await get_latest_futures_product_read(session, selected, access.checked_at)
    if latest is None or latest.outcome is None:
        raise FuturesProductBindingError("futures_product_latest_read_missing_or_pending")
    outcome = latest.outcome
    source = outcome.payload.result
    if not isinstance(source, FuturesProductReadSource):
        raise FuturesProductBindingError("futures_product_latest_read_unsuccessful")
    _require_compatible_account(outcome, access)
    _require_original_product_age(source, access, policy)
    snapshot = parse_safe_futures_product_source(base64.b64decode(source.body, validate=True))
    offset = snapshot.server_time - source.observed_at
    if not (
        -policy.maximum_product_server_clock_skew
        <= offset
        <= policy.maximum_product_server_clock_skew
    ):
        raise FuturesProductBindingError("futures_product_server_clock_outside_policy")
    return CurrentFuturesProductPrerequisite(
        access, outcome, futures_product_requirements(snapshot, wire_symbol)
    )
