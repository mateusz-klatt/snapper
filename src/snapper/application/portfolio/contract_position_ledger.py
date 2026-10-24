"""Describe source-seeded derivative positions without creating synthetic cash or fills.

The ledger retains the venue's observed basis separately from the arithmetic
model's prediction. Both are needed when the provider's average-entry rounding
policy is unavailable. Economic P&L is settlement-denominated and measured only
since the explicit source anchor; it is never an account cash movement.
"""

import hashlib
import json
from dataclasses import replace
from decimal import ROUND_HALF_EVEN
from decimal import Context
from decimal import Decimal
from decimal import DecimalException
from decimal import DivisionByZero
from decimal import Inexact
from decimal import InvalidOperation
from decimal import Overflow
from decimal import Underflow
from decimal import localcontext
from typing import Final
from typing import Literal
from typing import Self

from pydantic import BaseModel
from pydantic import ConfigDict
from pydantic import Field
from pydantic import field_validator
from pydantic import model_validator

from snapper.application.portfolio.contract_economics import ContractEconomicState
from snapper.application.portfolio.contract_economics import contract_model
from snapper.application.portfolio.contract_economics import fold_contract_fill
from snapper.application.portfolio.futures_position_source import FuturesPositionSourceLeg
from snapper.core.contract_position import ContractPosition
from snapper.core.execution_route_authority import ExecutionAccountScope
from snapper.core.execution_unit_snapshot import EvidenceDecimal
from snapper.core.execution_unit_snapshot import EvidenceInstant
from snapper.core.execution_unit_snapshot import EvidenceText
from snapper.core.execution_unit_snapshot import EvidenceUUID
from snapper.core.execution_unit_snapshot import ExecutionUnitSnapshotPayload
from snapper.core.execution_unit_snapshot import FillSnapshotTarget
from snapper.core.execution_unit_snapshot import PositiveEvidenceDecimal

_POSITION_CONTEXT = Context(
    prec=50,
    rounding=ROUND_HALF_EVEN,
    Emin=-999999,
    Emax=999999,
    capitals=1,
    clamp=0,
    traps=[InvalidOperation, DivisionByZero, Overflow, Underflow],
)
_ANCHOR_DOMAIN = b"snapper:contract-position-anchor:v1\x00"
_EVENT_DOMAIN = b"snapper:contract-position-event:v1\x00"
type PositionDecimalPolicy = Literal[
    "contract_context_50_half_even_v1", "contract_context_50_half_even_exact_totals_v2"
]
CURRENT_POSITION_DECIMAL_POLICY: Final[PositionDecimalPolicy] = (
    "contract_context_50_half_even_exact_totals_v2"
)
"""Model each fill at50 digits, then preserve exact sums of those modeled deltas."""


class _PositionPayload(BaseModel):
    """Revalidate all source and unit operands inside a frozen financial record."""

    model_config = ConfigDict(
        extra="forbid", frozen=True, strict=True, revalidate_instances="always"
    )

    schema_version: Literal[1] = 1

    @field_validator("schema_version", mode="before")
    @classmethod
    def _strict_schema_version(cls, value: object) -> object:
        """Reject boolean and float aliases for the immutable position schema."""
        if type(value) is not int:
            raise ValueError("Position ledger schema version must be an integer")
        return value


class ContractPositionValue(_PositionPayload):
    """A native position and economic totals measured since its explicitly retained source seed."""

    quantity: EvidenceDecimal
    entry_price: PositiveEvidenceDecimal | None
    realized_pnl_since_anchor: EvidenceDecimal
    quote_turnover_since_anchor: EvidenceDecimal = Field(ge=0)

    @model_validator(mode="after")
    def validate_basis(self) -> Self:
        """Require a source entry basis exactly when the position is nonflat.

        Returns:
            The same immutable value with no invented flat entry price.
        """
        if self.quantity.is_zero() != (self.entry_price is None):
            raise ValueError("Position quantity and entry basis disagree")
        return self

    def economic_state(self, unit: ExecutionUnitSnapshotPayload) -> ContractEconomicState:
        """Build the supported economic model from explicit captured source units.

        Args:
            unit: Actual immutable execution unit witness whose lineage the store verifies.

        Returns:
            A position-only state with the exact retained native source basis.
        """
        return ContractEconomicState(
            scope=unit.scope,
            dimensions_anchor=unit.dimensions,
            position=ContractPosition(
                contract_model(unit.dimensions), self.quantity, self.entry_price
            ),
            realized_pnl=self.realized_pnl_since_anchor,
            quote_turnover=self.quote_turnover_since_anchor,
        )


def _value(state: ContractEconomicState) -> ContractPositionValue:
    """Retain all position-only fold results without transferring trade principal or fees."""
    return ContractPositionValue(
        quantity=state.position.quantity,
        entry_price=state.position.entry_price,
        realized_pnl_since_anchor=state.realized_pnl,
        quote_turnover_since_anchor=state.quote_turnover,
    )


def _exact_cumulative_sum(first: Decimal, second: Decimal) -> Decimal:
    """Preserve modeled finite deltas without a guessed settlement-currency rounding quantum."""
    if first.is_zero():
        return second
    if second.is_zero():
        return first
    minimum_exponent = min(
        first.adjusted() - len(first.as_tuple().digits) + 1,
        second.adjusted() - len(second.as_tuple().digits) + 1,
    )
    precision = max(first.adjusted(), second.adjusted()) - minimum_exponent + 2
    try:
        with localcontext(_POSITION_CONTEXT) as context:
            context.prec = precision
            context.traps[Inexact] = True
            return first + second
    except DecimalException as error:
        raise ValueError("Position cumulative arithmetic unavailable") from error


def _prediction(
    before: ContractPositionValue,
    unit: ExecutionUnitSnapshotPayload,
    profile: ExecutionUnitSnapshotPayload,
    policy: PositionDecimalPolicy,
) -> ContractPositionValue:
    """Preserve historical arithmetic while separating new per-fill modeling from exact totals."""
    with localcontext(_POSITION_CONTEXT):
        state = before.economic_state(profile)
        if policy == "contract_context_50_half_even_v1":
            return _value(fold_contract_fill(state, unit).state)
        delta_state = replace(state, realized_pnl=Decimal(0), quote_turnover=Decimal(0))
        delta = _value(fold_contract_fill(delta_state, unit).state)
    return ContractPositionValue(
        quantity=delta.quantity,
        entry_price=delta.entry_price,
        realized_pnl_since_anchor=_exact_cumulative_sum(
            before.realized_pnl_since_anchor, delta.realized_pnl_since_anchor
        ),
        quote_turnover_since_anchor=_exact_cumulative_sum(
            before.quote_turnover_since_anchor, delta.quote_turnover_since_anchor
        ),
    )


def fold_position_source(
    before: ContractPositionValue,
    unit: ExecutionUnitSnapshotPayload,
    source: FuturesPositionSourceLeg,
    profile_unit: ExecutionUnitSnapshotPayload | None = None,
    *,
    decimal_policy: PositionDecimalPolicy = CURRENT_POSITION_DECIMAL_POLICY,
) -> tuple[ContractPositionValue, ContractPositionValue]:
    """Retain arithmetic prediction and actual observed source basis as separate values.

    Args:
        before: The explicit source-seeded prior native position and period totals.
        unit: Real fill snapshot with supported native/internal quantity interpretation.
        source: Native position log paired to the exact actual source execution.
        profile_unit: Original retained unit profile for this position pool, if already seeded.
        decimal_policy: Explicit historical or current modeling and cumulative arithmetic policy.

    Returns:
        Derived and observed resulting positions with identical signed quantity.
        A basis difference remains a reconciliation concern and does not change cash.
    """
    prior = ContractPositionValue.model_validate(before)
    native = FuturesPositionSourceLeg.model_validate(source)
    if decimal_policy not in {"contract_context_50_half_even_v1", CURRENT_POSITION_DECIMAL_POLICY}:
        raise ValueError("Position arithmetic policy unsupported")
    if (prior.quantity, prior.entry_price) != (
        native.quantity_before,
        native.entry_price_before,
    ):
        raise ValueError("Position predecessor contradicts native source")
    profile = unit if profile_unit is None else profile_unit
    derived = _prediction(prior, unit, profile, decimal_policy)
    if derived.quantity != native.quantity_after:
        raise ValueError("Fill quantity contradicts native position transition")
    observed = ContractPositionValue(
        quantity=native.quantity_after,
        entry_price=native.entry_price_after,
        realized_pnl_since_anchor=derived.realized_pnl_since_anchor,
        quote_turnover_since_anchor=derived.quote_turnover_since_anchor,
    )
    return derived, observed


class ContractPositionAnchorPayload(_PositionPayload):
    """A native old-position seed that does not assert any local execution-prefix alignment."""

    scope: ExecutionAccountScope
    instrument_public_id: EvidenceUUID
    route_authority_public_id: EvidenceUUID
    position_route_authority_public_id: EvidenceUUID
    source_receipt_public_id: EvidenceUUID
    unit_snapshot_public_id: EvidenceUUID
    source_alias_public_id: EvidenceUUID
    source_alias_timestamp: EvidenceInstant
    unit: ExecutionUnitSnapshotPayload
    source_position: FuturesPositionSourceLeg
    opening: ContractPositionValue
    anchor_kind: Literal["position_source_seed"] = "position_source_seed"
    source_execution_prefix_status: Literal["unavailable"] = "unavailable"
    source_execution_scope_sequence: None = None

    @model_validator(mode="after")
    def validate_source_seed(self) -> Self:
        """Require the exact observed old quantity/basis and an explicitly new economic period.

        Returns:
            The same immutable source seed without a fabricated zero position.
        """
        expected = (
            self.source_position.quantity_before,
            self.source_position.entry_price_before,
            Decimal(0),
            Decimal(0),
        )
        actual = (
            self.opening.quantity,
            self.opening.entry_price,
            self.opening.realized_pnl_since_anchor,
            self.opening.quote_turnover_since_anchor,
        )
        if actual != expected:
            raise ValueError("Position source seed contradicts its native opening state")
        _verify_position_scope(
            self.scope, self.instrument_public_id, self.unit, self.source_position
        )
        self.opening.economic_state(self.unit)
        if self.source_position.quantity_delta != self.unit.dimensions.signed_quantity:
            raise ValueError("Position source quantity transition is not one actual source fill")
        return self


def _verify_position_scope(
    scope: ExecutionAccountScope,
    instrument_public_id: str,
    unit: ExecutionUnitSnapshotPayload,
    source: FuturesPositionSourceLeg,
) -> None:
    """Require exact actual source/account identity before interpreting its native quantities."""
    if (
        scope.exchange != "kraken_futures"
        or (scope.wallet_public_id, scope.exchange, scope.mode)
        != (unit.scope.wallet_public_id, unit.scope.exchange, unit.scope.mode)
        or instrument_public_id != unit.scope.instrument_public_id
        or source.source_instrument_public_id != instrument_public_id
        or source.source_entry.margin_account != scope.margin_account
        or source.provider_contract_symbol != unit.venue_identity.wsname
        or not isinstance(unit.target, FillSnapshotTarget)
    ):
        raise ValueError("Native position, actual account and execution units disagree")


class ContractPositionEventPayload(_PositionPayload):
    """An actual fill's observed position transition and separate settlement economic prediction."""

    decimal_policy: PositionDecimalPolicy = "contract_context_50_half_even_v1"
    scope: ExecutionAccountScope
    instrument_public_id: EvidenceUUID
    route_authority_public_id: EvidenceUUID
    position_route_authority_public_id: EvidenceUUID
    anchor_public_id: EvidenceUUID
    raw_receipt_public_id: EvidenceUUID
    position_receipt_public_id: EvidenceUUID
    execution_public_id: EvidenceUUID
    execution_scope_sequence: int = Field(gt=0)
    unit_snapshot_public_id: EvidenceUUID
    source_alias_public_id: EvidenceUUID
    source_alias_timestamp: EvidenceInstant
    venue_execution_id: EvidenceText
    executed_at: EvidenceInstant
    execution_known_at: EvidenceInstant
    ledger_sequence: int = Field(gt=0)
    kind: Literal["contract_position_fill"] = "contract_position_fill"
    unit: ExecutionUnitSnapshotPayload
    profile_unit: ExecutionUnitSnapshotPayload
    source_position: FuturesPositionSourceLeg
    before: ContractPositionValue
    derived_after: ContractPositionValue
    observed_after: ContractPositionValue
    basis_status: Literal["equal", "source_precision_unproven"]

    @model_validator(mode="after")
    def validate_fill_prediction(self) -> Self:
        """Independently rederive position economics while retaining actual source basis.

        Returns:
            A conserved actual transition with no cash posting or hold-release claim.
        """
        _verify_position_scope(
            self.scope, self.instrument_public_id, self.unit, self.source_position
        )
        target = self.unit.target
        if not isinstance(target, FillSnapshotTarget) or (
            target.execution_public_id != self.execution_public_id
            or self.source_position.source_entry.execution != self.venue_execution_id
        ):
            raise ValueError("Native position source execution identity disagrees")
        derived, observed = fold_position_source(
            self.before,
            self.unit,
            self.source_position,
            self.profile_unit,
            decimal_policy=self.decimal_policy,
        )
        status = (
            "equal" if derived.entry_price == observed.entry_price else "source_precision_unproven"
        )
        if (self.derived_after, self.observed_after, self.basis_status) != (
            derived,
            observed,
            status,
        ):
            raise ValueError(
                "Position ledger projection contradicts its source and arithmetic policy"
            )
        return self


def canonical_position_payload(
    payload: ContractPositionAnchorPayload | ContractPositionEventPayload,
) -> str:
    """Serialize complete source, native position and arithmetic policy evidence.

    Args:
        payload: The validated source anchor or real position transition.

    Returns:
        Canonical JSON retaining exact Decimal operands and all source references.
    """
    evidence = type(payload).model_validate(payload)
    return json.dumps(evidence.model_dump(mode="json"), sort_keys=True, separators=(",", ":"))


def position_payload_digest(
    payload: ContractPositionAnchorPayload | ContractPositionEventPayload,
) -> str:
    """Hash a complete immutable position record under its distinct versioned domain.

    Args:
        payload: The exact source anchor or position transition.

    Returns:
        The versioned SHA-256 content identity independent of insertion provenance.
    """
    domain = _ANCHOR_DOMAIN if isinstance(payload, ContractPositionAnchorPayload) else _EVENT_DOMAIN
    return hashlib.sha256(domain + canonical_position_payload(payload).encode()).hexdigest()
