"""Project endpoint-specific Futures flex risk fields without inferring spendable funds.

The accounts endpoint uses FlexBalanceSummary and FlexCurrencySummary. Native
currency amounts remain separate from USD valuations and margin requirements.
No equality between provider fields, margin regime or reservation permission is
inferred. The existing native cash parser and all retained source bytes remain
unchanged. This pure interpretation does not establish actual source ownership.
"""

import base64
from collections.abc import Mapping
from decimal import Decimal
from typing import Literal

from pydantic import BaseModel
from pydantic import ConfigDict
from pydantic import Field

from snapper.application.trade.futures_native_accounts import FUTURES_PREPARED_ACCOUNTS_CHANNEL
from snapper.application.trade.futures_native_accounts import NativeAccountsSourceError
from snapper.application.trade.futures_native_accounts import _object
from snapper.application.trade.futures_native_accounts import _quantity
from snapper.application.trade.futures_native_accounts import _text
from snapper.application.trade.futures_native_accounts import parse_futures_native_accounts
from snapper.core.contract_accounting_observation import RawAccountingObservation
from snapper.core.execution_unit_snapshot import EvidenceDecimal
from snapper.core.execution_unit_snapshot import EvidenceInstant
from snapper.core.execution_unit_snapshot import EvidenceText
from snapper.infrastructure.exchanges.execution_wire_receipts import (
    decode_private_execution_receipt,
)

_MAX_ENTITY_BYTES = 8 * 1024 * 1024
_USD_FIELDS = (
    ("initial_margin_usd", "initialMargin"),
    ("initial_margin_with_orders_usd", "initialMarginWithOrders"),
    ("maintenance_margin_usd", "maintenanceMargin"),
    ("balance_value_usd", "balanceValue"),
    ("portfolio_value_usd", "portfolioValue"),
    ("collateral_value_usd", "collateralValue"),
    ("unrealized_pnl_usd", "pnl"),
    ("unrealized_funding_usd", "unrealizedFunding"),
    ("total_unrealized_usd", "totalUnrealized"),
    ("total_unrealized_as_margin_usd", "totalUnrealizedAsMargin"),
    ("available_margin_usd", "availableMargin"),
    ("margin_equity_usd", "marginEquity"),
)


class FuturesFlexRiskSourceError(NativeAccountsSourceError):
    """The original accounts source cannot supply the selected complete flex risk projection."""


class _FlexRiskEvidence(BaseModel):
    """Preserve strict immutable source observations without normalizing missing or signed values."""

    model_config = ConfigDict(
        frozen=True,
        strict=True,
        extra="forbid",
        revalidate_instances="always",
        hide_input_in_errors=True,
    )


class FuturesFlexMarginUSD(_FlexRiskEvidence):
    """Twelve independent USD observations from the accounts endpoint's FlexBalanceSummary."""

    initial_margin_usd: EvidenceDecimal
    initial_margin_with_orders_usd: EvidenceDecimal
    maintenance_margin_usd: EvidenceDecimal
    balance_value_usd: EvidenceDecimal
    portfolio_value_usd: EvidenceDecimal
    collateral_value_usd: EvidenceDecimal
    unrealized_pnl_usd: EvidenceDecimal
    unrealized_funding_usd: EvidenceDecimal
    total_unrealized_usd: EvidenceDecimal
    total_unrealized_as_margin_usd: EvidenceDecimal
    available_margin_usd: EvidenceDecimal
    margin_equity_usd: EvidenceDecimal


class FuturesFlexCurrencyRisk(_FlexRiskEvidence):
    """One literal currency's native amounts and separate USD valuation observations."""

    currency: EvidenceText
    quantity_native: EvidenceDecimal
    value_usd: EvidenceDecimal
    collateral_usd: EvidenceDecimal
    available_native: EvidenceDecimal


class FuturesFlexRiskSnapshot(_FlexRiskEvidence):
    """An additive complete flex projection without account authority or new-order funds permission."""

    source_policy: Literal["kraken_futures_flex_risk_v1"] = "kraken_futures_flex_risk_v1"
    native_account_key: Literal["flex"] = "flex"
    account_type: Literal["multiCollateralMarginAccount"] = "multiCollateralMarginAccount"
    server_time_text: EvidenceText
    server_time: EvidenceInstant
    margin_usd: FuturesFlexMarginUSD
    currencies: tuple[FuturesFlexCurrencyRisk, ...] = Field(max_length=4096)


def _summary(source: Mapping[str, object]) -> FuturesFlexMarginUSD:
    """Require every endpoint-specific USD operand without substituting zero or a derived equation."""
    values: dict[str, Decimal] = {
        target: _quantity(source.get(original)) for target, original in _USD_FIELDS
    }
    return FuturesFlexMarginUSD.model_validate(values)


def _currency(currency: str, source: object) -> FuturesFlexCurrencyRisk:
    """Keep the actual keyed currency and documented units without case folding or symbol aliases."""
    original = _object(source)
    return FuturesFlexCurrencyRisk(
        currency=_text(currency),
        quantity_native=_quantity(original.get("quantity")),
        value_usd=_quantity(original.get("value")),
        collateral_usd=_quantity(original.get("collateral")),
        available_native=_quantity(original.get("available")),
    )


def parse_futures_flex_risk(observation: RawAccountingObservation) -> FuturesFlexRiskSnapshot:
    """Interpret complete actual-source shape using the exact accounts flex schema and units.

    Args:
        observation: Original complete prepared-key-v2 accounts response, not caller-authored amounts.

    Returns:
        Independent original risk observations; provenance, freshness and reservations remain unproven.

    Raises:
        ValueError: If the whole accounts envelope, literal flex account or any required field is invalid.
    """
    raw = RawAccountingObservation.model_validate(observation)
    if raw.channel != FUTURES_PREPARED_ACCOUNTS_CHANNEL:
        raise FuturesFlexRiskSourceError("futures_flex_guarded_accounts_source_required")
    if len(raw.material) > 4 * ((_MAX_ENTITY_BYTES + 2) // 3):
        raise FuturesFlexRiskSourceError("futures_flex_original_source_too_large")
    wire = base64.b64decode(raw.material, validate=True)
    if len(wire) > _MAX_ENTITY_BYTES:
        raise FuturesFlexRiskSourceError("futures_flex_original_source_too_large")
    snapshot = parse_futures_native_accounts(raw)
    document = decode_private_execution_receipt(wire)
    if "error" in document or "errors" in document:
        raise FuturesFlexRiskSourceError("futures_flex_contradictory_success_envelope")
    source = _object(_object(document.get("accounts")).get("flex"))
    if source.get("type") != "multiCollateralMarginAccount":
        raise FuturesFlexRiskSourceError("futures_flex_literal_account_type_required")
    currencies = _object(source.get("currencies"))
    return FuturesFlexRiskSnapshot(
        server_time_text=snapshot.server_time_text,
        server_time=snapshot.server_time,
        margin_usd=_summary(source),
        currencies=tuple(_currency(name, value) for name, value in currencies.items()),
    )
