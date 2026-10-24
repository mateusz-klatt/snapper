"""Bind current authenticated margin observations to the actual native multi-collateral profile.

This reader retains the original raw threshold field and rates. It supplies no
notional conversion, tier formula, required margin, funds or CREATE permission.
Single-collateral product-to-wallet settlement binding remains unsupported here.
"""

import base64
from dataclasses import dataclass

from sqlalchemy.ext.asyncio import AsyncSession

from snapper.application.trade.futures_margin_schedule import FuturesMarginSchedule
from snapper.application.trade.futures_margin_schedule import parse_futures_margin_schedule
from snapper.application.trade.futures_product_eligibility import CurrentFuturesProductPrerequisite
from snapper.application.trade.futures_product_eligibility import FuturesProductFreshnessPolicy
from snapper.application.trade.futures_product_eligibility import require_current_futures_product
from snapper.core.execution_unit_admission import ExecutionRouteReference
from snapper.core.futures_product_read import FuturesProductReadSource
from snapper.core.native_account_authority import NativeAccountAuthorityPayload
from snapper.core.native_account_authority import NativeAuthorityProfile


class CurrentFuturesMarginUnavailableError(ValueError):
    """Actual current account/profile source cannot establish the supported raw margin observation."""


@dataclass(frozen=True, slots=True)
class CurrentFuturesMarginSchedulePrerequisite:
    """Original same-account source/profile and literal tiers, without financial conversion or grant."""

    product: CurrentFuturesProductPrerequisite
    native_profile: NativeAuthorityProfile
    schedule: FuturesMarginSchedule


def _native_flex_profile(product: CurrentFuturesProductPrerequisite) -> NativeAuthorityProfile:
    """Use the independently bound actual profile instead of product enum or wallet-name guesses."""
    authority = NativeAccountAuthorityPayload.model_validate(product.access.authority.payload)
    profile = authority.native_profile
    if (
        profile.account_type != "multiCollateralMarginAccount"
        or profile.native_account_key != "flex"
    ):
        raise CurrentFuturesMarginUnavailableError("futures_margin_native_flex_profile_required")
    return profile


async def require_current_futures_margin_schedule(
    session: AsyncSession,
    route: ExecutionRouteReference,
    wire_symbol: str,
    policy: FuturesProductFreshnessPolicy,
) -> CurrentFuturesMarginSchedulePrerequisite:
    """Project literal tiers from the same actual current authenticated product source and account.

    Args:
        session: Existing actual caller transaction already holding the intended wallet fence.
        route: Exact native route whose source rebinds the actual observed account/profile.
        wire_symbol: Exact intended exchange symbol, independently resolved from its unit catalog.
        policy: Trusted existing account/product age and provider-clock policy, never source data.

    Returns:
        Current original product/profile evidence and raw tier observations without a margin formula.

    Raises:
        ValueError: If source freshness, actual account, supported profile or original tiers fail.
    """
    product = await require_current_futures_product(session, route, wire_symbol, policy)
    profile = _native_flex_profile(product)
    source = FuturesProductReadSource.model_validate(product.product_outcome.payload.result)
    schedule = parse_futures_margin_schedule(
        base64.b64decode(source.body, validate=True), wire_symbol
    )
    if schedule.threshold_field != "numNonContractUnits":
        raise CurrentFuturesMarginUnavailableError("futures_margin_flex_threshold_field_mismatch")
    return CurrentFuturesMarginSchedulePrerequisite(product, profile, schedule)
