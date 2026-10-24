"""Fold explicit contract position economics without inventing account cash.

Durable accounting callers must prove the snapshot, account namespace, seed
position and event order before using these results. Position P&L has the
contract's economic settlement denomination; actual cash comes from independent
account balance evidence. Neither a fill nor a basis reset transfers principal,
posts a fee twice or establishes the account's selected payout currency.
"""

from dataclasses import dataclass
from decimal import Decimal
from decimal import DecimalException
from decimal import Inexact
from decimal import localcontext
from typing import Literal
from typing import NamedTuple
from typing import Self
from typing import cast

from pydantic import BaseModel
from pydantic import ConfigDict
from pydantic import model_validator

from snapper.core.contract_position import ContractPosition
from snapper.core.contract_position import apply_contract_fill
from snapper.core.contract_valuation import ContractValuation
from snapper.core.contract_valuation import InverseContractValuation
from snapper.core.contract_valuation import LinearContractValuation
from snapper.core.contract_valuation import quote_notional
from snapper.core.contract_valuation import settlement_pnl
from snapper.core.execution_unit_snapshot import EvidenceDecimal
from snapper.core.execution_unit_snapshot import EvidenceInstant
from snapper.core.execution_unit_snapshot import EvidenceText
from snapper.core.execution_unit_snapshot import ExecutionUnitSnapshotPayload
from snapper.core.execution_unit_snapshot import FillSnapshotTarget
from snapper.core.execution_unit_snapshot import PositiveEvidenceDecimal
from snapper.core.execution_unit_snapshot import QuantityUnit
from snapper.core.execution_unit_snapshot import SnapshotDimensions
from snapper.core.execution_unit_snapshot import SnapshotReceipt
from snapper.core.execution_unit_snapshot import SnapshotScope


class ContractEconomicsError(ValueError):
    """Explicit unit evidence or arithmetic cannot establish position economics."""


class _ProfileKey(NamedTuple):
    """The immutable economic interpretation of one position pool."""

    base: str
    quote: str
    native_unit: QuantityUnit
    internal_unit: QuantityUnit
    operand_unit: QuantityUnit
    payoff: Literal["linear", "inverse"]
    multiplier: Decimal
    contract_value_currency: str
    settlement_currency: str


def _require(condition: bool, reason: str) -> None:
    """Refuse an unsupported interpretation without proposing a state mutation."""
    if not condition:
        raise ContractEconomicsError(reason)


def _finite(value: object, name: str, *, nonnegative: bool = False) -> Decimal:
    """Validate an exact finite operand without coercing a compatibility mirror."""
    _require(isinstance(value, Decimal), f"{name}_requires_decimal")
    number = cast(Decimal, value)
    _require(number.is_finite(), f"{name}_nonfinite")
    _require(not nonnegative or number >= 0, f"{name}_negative")
    return number


def _exact_sum(first: Decimal, second: Decimal) -> Decimal:
    """Preserve cumulative P&L and turnover rather than silently rounding a delta."""
    try:
        with localcontext() as context:
            context.traps[Inexact] = True
            result = first + second
    except DecimalException as error:
        raise ContractEconomicsError("economic_accumulation_unavailable") from error
    return result


def contract_model(dimensions: SnapshotDimensions) -> ContractValuation:
    """Resolve an explicit monetary tuple without inferring payoff from a symbol.

    Args:
        dimensions: Strict snapshot dimensions with known payoff evidence.

    Returns:
        The supported normalized linear or inverse contract interpretation.
    """
    evidence = SnapshotDimensions.model_validate(dimensions)
    _require(evidence.base_currency != evidence.quote_currency, "economic_currencies_equal")
    multiplier = evidence.contract_multiplier
    _require(multiplier is not None, "payoff_evidence_unavailable")
    if evidence.payoff_model == "linear":
        _require(
            evidence.contract_value_currency == evidence.base_currency
            and evidence.settlement_currency == evidence.quote_currency,
            "linear_currency_dimensions_unsupported",
        )
        return LinearContractValuation(cast(Decimal, multiplier))
    _require(
        evidence.contract_value_currency == evidence.quote_currency
        and evidence.settlement_currency == evidence.base_currency,
        "inverse_currency_dimensions_unsupported",
    )
    return InverseContractValuation(cast(Decimal, multiplier))


def _profile(dimensions: SnapshotDimensions) -> _ProfileKey:
    """Validate a normalized contract-count position and supported identity mapping."""
    dimensions = SnapshotDimensions.model_validate(dimensions)
    model = contract_model(dimensions)
    _require(dimensions.internal_quantity_unit == "contract_count", "internal_unit_unsupported")
    direct = (
        dimensions.native_order_quantity_unit == "contract_count"
        and dimensions.signed_quantity_unit == "contract_count"
    )
    identity_base = (
        dimensions.native_order_quantity_unit == "base_asset"
        and dimensions.signed_quantity_unit == "base_asset"
        and isinstance(model, LinearContractValuation)
        and model.base_units_per_contract == 1
    )
    _require(direct or identity_base, "quantity_conversion_unavailable")
    _require(
        dimensions.price_quantity_convention == "quote_per_base"
        and dimensions.price_currency == dimensions.quote_currency,
        "price_convention_unsupported",
    )
    return _ProfileKey(
        dimensions.base_currency,
        dimensions.quote_currency,
        cast(QuantityUnit, dimensions.native_order_quantity_unit),
        "contract_count",
        cast(QuantityUnit, dimensions.signed_quantity_unit),
        cast(Literal["linear", "inverse"], dimensions.payoff_model),
        cast(Decimal, dimensions.contract_multiplier),
        cast(str, dimensions.contract_value_currency),
        cast(str, dimensions.settlement_currency),
    )


@dataclass(frozen=True, slots=True)
class ContractEconomicState:
    """One explicitly seeded instrument position, with no account cash component.

    The durable caller owns the proof of the seed position. A flat value is not
    evidence that historical executions or retained account state were empty.
    """

    scope: SnapshotScope
    dimensions_anchor: SnapshotDimensions
    position: ContractPosition
    realized_pnl: Decimal = Decimal(0)
    quote_turnover: Decimal = Decimal(0)

    def __post_init__(self) -> None:
        """Revalidate the immutable scope, unit profile and position model."""
        object.__setattr__(self, "scope", SnapshotScope.model_validate(self.scope))
        anchor = SnapshotDimensions.model_validate(self.dimensions_anchor)
        object.__setattr__(self, "dimensions_anchor", anchor)
        _profile(anchor)
        _require(isinstance(self.position, ContractPosition), "position_requires_typed_state")
        position = ContractPosition(
            self.position.specification, self.position.quantity, self.position.entry_price
        )
        _require(position.specification == contract_model(anchor), "position_payoff_mismatch")
        _finite(self.realized_pnl, "realized_pnl")
        _finite(self.quote_turnover, "quote_turnover", nonnegative=True)

    @property
    def settlement_currency(self) -> str:
        """Return the explicit economic P&L denomination.

        Returns:
            The settlement currency established by the validated unit dimensions.
        """
        return cast(str, self.dimensions_anchor.settlement_currency)


@dataclass(frozen=True, slots=True)
class ContractEconomicOutcome:
    """A proposed position-only result to persist with its source identity atomically."""

    state: ContractEconomicState
    realized_delta: Decimal
    closed_quantity: Decimal
    opened_quantity: Decimal
    quote_notional: Decimal


def _validated_state(state: ContractEconomicState) -> ContractEconomicState:
    """Recheck immutable input values before proposing a new durable state."""
    _require(isinstance(state, ContractEconomicState), "economic_state_required")
    return ContractEconomicState(
        state.scope,
        state.dimensions_anchor,
        state.position,
        state.realized_pnl,
        state.quote_turnover,
    )


def _validate_fill(
    state: ContractEconomicState, snapshot: ExecutionUnitSnapshotPayload
) -> ExecutionUnitSnapshotPayload:
    """Require a fill target and exact source units before any fold is proposed."""
    evidence = ExecutionUnitSnapshotPayload.model_validate(snapshot)
    _require(isinstance(evidence.target, FillSnapshotTarget), "fill_snapshot_required")
    _require(evidence.scope == state.scope, "position_scope_mismatch")
    _require(_profile(evidence.dimensions) == _profile(state.dimensions_anchor), "payoff_changed")
    _require(
        evidence.rebase.quantity_convention == "not_applicable"
        and evidence.receipt.reported_quantity_convention == "not_applicable",
        "rebase_interpretation_unsupported",
    )
    _require(
        evidence.receipt.reported_quantity_unit == evidence.dimensions.signed_quantity_unit,
        "receipt_operand_unit_mismatch",
    )
    _require(evidence.dimensions.price is not None, "fill_price_unavailable")
    return evidence


def fold_contract_fill(
    state: ContractEconomicState, snapshot: ExecutionUnitSnapshotPayload
) -> ContractEconomicOutcome:
    """Fold a proven fill's position economics without moving account cash.

    Args:
        state: Explicitly seeded position under an unchanged economic profile.
        snapshot: A bound fill snapshot; storage authenticity is the caller's job.

    Returns:
        Proposed position, economic realized P&L and quote turnover. Fees and
        actual cash are deliberately absent from this position-only result.
    """
    state = _validated_state(state)
    evidence = _validate_fill(state, snapshot)
    dimensions = evidence.dimensions
    price = cast(Decimal, dimensions.price)
    model = state.position.specification
    try:
        fill = apply_contract_fill(model, state.position, dimensions.signed_quantity, price)
        notional = quote_notional(model, dimensions.signed_quantity, price)
    except ValueError as error:
        raise ContractEconomicsError("contract_fill_arithmetic_unavailable") from error
    updated = ContractEconomicState(
        state.scope,
        state.dimensions_anchor,
        fill.position,
        _exact_sum(state.realized_pnl, fill.realized_settlement_pnl),
        _exact_sum(state.quote_turnover, notional),
    )
    return ContractEconomicOutcome(
        updated,
        fill.realized_settlement_pnl,
        fill.closed_quantity,
        fill.opened_quantity,
        notional,
    )


class ContractSettlementObservation(BaseModel):
    """An explicitly sourced variation reset or expiry, distinct from a trade fill."""

    model_config = ConfigDict(
        extra="forbid", frozen=True, strict=True, revalidate_instances="always"
    )

    scope: SnapshotScope
    kind: Literal["variation", "expiry"]
    price: PositiveEvidenceDecimal
    price_currency: EvidenceText
    price_quantity_convention: Literal["quote_per_base"]
    quantity_unit: Literal["contract_count"]
    settlement_type: Literal["cash"]
    resulting_quantity: EvidenceDecimal
    receipt: SnapshotReceipt
    observed_at: EvidenceInstant

    @model_validator(mode="after")
    def validate_observation(self) -> Self:
        """Reject a source receipt that had not yet been observed.

        Returns:
            The same immutable settlement observation.
        """
        if self.receipt.observed_at > self.observed_at:
            raise ContractEconomicsError("settlement_precedes_receipt")
        return self


def fold_contract_settlement(
    state: ContractEconomicState, observation: ContractSettlementObservation
) -> ContractEconomicOutcome:
    """Realize position economics and reset its basis without synthetic turnover.

    Args:
        state: Proven position immediately before the settlement source event.
        observation: Explicit source price and resulting quantity. Variation
            retains quantity; expiry must explicitly report zero quantity.

    Returns:
        The new position basis or flat expired state. Actual variation/expiry
        cash remains a separately witnessed account balance leg.
    """
    state = _validated_state(state)
    evidence = ContractSettlementObservation.model_validate(observation)
    _require(evidence.scope == state.scope, "settlement_scope_mismatch")
    _require(
        evidence.price_currency == state.dimensions_anchor.quote_currency,
        "settlement_price_currency_mismatch",
    )
    expected = state.position.quantity if evidence.kind == "variation" else Decimal(0)
    _require(evidence.resulting_quantity == expected, "settlement_quantity_mismatch")
    entry = state.position.entry_price
    try:
        realized = (
            settlement_pnl(
                state.position.specification, state.position.quantity, entry, evidence.price
            )
            if entry is not None
            else Decimal(0)
        )
    except ValueError as error:
        raise ContractEconomicsError("contract_settlement_arithmetic_unavailable") from error
    position = ContractPosition(
        state.position.specification,
        expected,
        evidence.price if expected != 0 else None,
    )
    updated = ContractEconomicState(
        state.scope,
        state.dimensions_anchor,
        position,
        _exact_sum(state.realized_pnl, realized),
        state.quote_turnover,
    )
    closed = state.position.quantity.copy_abs() if evidence.kind == "expiry" else Decimal(0)
    return ContractEconomicOutcome(updated, realized, closed, Decimal(0), Decimal(0))
