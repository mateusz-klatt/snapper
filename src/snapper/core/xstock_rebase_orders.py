"""Check a witnessed limit-order view restatement without changing original intent.

The caller must bind the source receipt, actual order and corporate-action row
inside its durable account transaction. Equal numbers or a supplied UUID alone
do not authenticate that lineage. Changed raw quantities, prices or fills are
not accepted as a harmless display rebase by this narrow consistency check.
"""

from dataclasses import dataclass
from decimal import Decimal
from fractions import Fraction
from typing import Literal
from typing import Self

from pydantic import BaseModel
from pydantic import ConfigDict
from pydantic import model_validator

from snapper.core.execution_unit_snapshot import EvidenceDigest
from snapper.core.execution_unit_snapshot import EvidenceInstant
from snapper.core.execution_unit_snapshot import EvidenceText
from snapper.core.execution_unit_snapshot import EvidenceUUID
from snapper.core.xstock_rebase import XStockDecimal
from snapper.core.xstock_rebase import XStockInterpretation
from snapper.core.xstock_rebase import XStockMultiplierWitness
from snapper.core.xstock_rebase import XStockRebaseError
from snapper.core.xstock_rebase import XStockUnitValue
from snapper.core.xstock_rebase import convert_xstock_units
from snapper.core.xstock_rebase import exact_xstock_decimal


class XStockOrderRestatement(BaseModel):
    """Source-bound observations of the same limit order across an engine restatement."""

    model_config = ConfigDict(frozen=True, extra="forbid", revalidate_instances="always")

    order_public_id: EvidenceUUID
    corporate_action_public_id: EvidenceUUID
    source_event_id: EvidenceText
    source_receipt_digest: EvidenceDigest
    source_event_kind: Literal["restated"]
    order_type: Literal["limit"]
    before: XStockUnitValue
    after: XStockUnitValue
    before_multiplier: XStockMultiplierWitness
    after_multiplier: XStockMultiplierWitness
    before_economic_at: EvidenceInstant
    after_economic_at: EvidenceInstant
    observed_at: EvidenceInstant
    known_at: EvidenceInstant
    raw_cumulative_filled_before: XStockDecimal
    raw_cumulative_filled_after: XStockDecimal

    @model_validator(mode="after")
    def validate_clocks(self) -> Self:
        """Require ordered observations and available source knowledge.

        Returns:
            The same proposal without certifying the referenced durable source rows.
        """
        if self.before_economic_at >= self.after_economic_at:
            raise ValueError("xStocks order restatement observations are not ordered")
        if self.observed_at > self.known_at:
            raise ValueError("xStocks restatement cannot be known before observation")
        if self.known_at < max(self.before_multiplier.known_at, self.after_multiplier.known_at):
            raise ValueError("xStocks restatement cannot predate multiplier knowledge")
        return self


@dataclass(frozen=True, slots=True)
class XStockRestatedOrder:
    """Canonical invariant operands, not permission to resend or change a reservation."""

    order_public_id: str
    raw_order_quantity: Decimal
    raw_filled_quantity: Decimal
    raw_remaining_quantity: Decimal
    quote_limit_price_per_spv: Decimal


def _require(condition: bool, reason: str) -> None:
    """Refuse a proposed harmless rebase before returning any canonical order operands."""
    if not condition:
        raise XStockRebaseError(reason)


def normalize_xstock_order_restatement(evidence: XStockOrderRestatement) -> XStockRestatedOrder:
    """Require invariant raw quantity, limit price and execution progress across views.

    Args:
        evidence: Independently source-bound before/after observations and their unit profiles.

    Returns:
        Unchanged canonical limit-order operands; the original command stays immutable.
    """
    source = XStockOrderRestatement.model_validate(evidence)
    before = convert_xstock_units(
        source.before,
        source.before_multiplier,
        XStockInterpretation(economic_at=source.before_economic_at, interpreted_at=source.known_at),
        "spv_token",
    )
    after = convert_xstock_units(
        source.after,
        source.after_multiplier,
        XStockInterpretation(economic_at=source.after_economic_at, interpreted_at=source.known_at),
        "spv_token",
    )
    _require(before.scope == after.scope, "xstock_restatement_scope_mismatch")
    _require(before.quantity == after.quantity, "xstock_restatement_raw_quantity_changed")
    _require(before.price == after.price, "xstock_restatement_raw_price_changed")
    _require(
        source.raw_cumulative_filled_before == source.raw_cumulative_filled_after,
        "xstock_restatement_execution_progress_changed",
    )
    _require(
        0 <= source.raw_cumulative_filled_after <= after.quantity,
        "xstock_restatement_execution_progress_invalid",
    )
    return XStockRestatedOrder(
        source.order_public_id,
        after.quantity,
        source.raw_cumulative_filled_after,
        exact_xstock_decimal(
            Fraction(after.quantity) - Fraction(source.raw_cumulative_filled_after)
        ),
        after.price,
    )
