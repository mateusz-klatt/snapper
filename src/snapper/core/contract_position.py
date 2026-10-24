"""Fold contract fills into immutable positions with explicit settlement models.

Linear entries use a quantity-weighted arithmetic mean. Inverse entries use
the quantity-weighted harmonic mean, preserving the sum of reciprocal entry
values. Closing fills realize settlement P&L through ``contract_valuation``.

Quantities are normalized contract units supplied by the caller. A position
retains its immutable valuation specification; a changed payoff or multiplier
must start a separate pool. Instrument identity, metadata version, account
currency, fees, funding and collateral remain the caller's responsibility.
This module transfers no cash and infers no settlement currency or venue rule.

Quantity addition must be exact in the caller's Decimal context. Insufficient
precision raises instead of losing part of a fill. Entry and payoff arithmetic
follow the caller's precision, rounding and traps; non-finite or non-positive
entries fail closed. No quantity epsilon turns a small position into flat.
"""

from collections.abc import Callable
from dataclasses import dataclass
from decimal import Decimal
from decimal import DecimalException
from decimal import Inexact
from decimal import localcontext
from typing import cast

from snapper.core.contract_valuation import ContractValuation
from snapper.core.contract_valuation import InverseContractValuation
from snapper.core.contract_valuation import LinearContractValuation
from snapper.core.contract_valuation import settlement_pnl


def _specification(value: object) -> ContractValuation:
    """Require an immutable explicit linear or inverse multiplier model.

    Args:
        value: Model supplied for the position or fill.

    Returns:
        The validated immutable contract valuation model.
    """
    if not isinstance(value, (LinearContractValuation, InverseContractValuation)):
        raise ValueError("An explicit contract valuation specification is required")
    return value


def _decimal(value: object, name: str, *, positive: bool = False) -> Decimal:
    """Validate a finite Decimal operand without coercion or implicit rounding.

    Args:
        value: Operand to validate.
        name: Operand name for diagnostic errors.
        positive: Whether zero and negative operands must be refused.

    Returns:
        The original validated Decimal operand.
    """
    if not isinstance(value, Decimal) or not value.is_finite():
        raise ValueError(f"{name} must be a finite Decimal")
    if positive and value <= 0:
        raise ValueError(f"{name} must be positive")
    return value


@dataclass(frozen=True, slots=True)
class ContractPosition:
    """One contract position and its unchanged valuation interpretation.

    Attributes:
        specification: Explicit immutable payoff model and contract multiplier.
        quantity: Finite signed contract quantity; positive is long.
        entry_price: Positive quote price per base unit, absent exactly when flat.
    """

    specification: ContractValuation
    quantity: Decimal
    entry_price: Decimal | None

    def __post_init__(self) -> None:
        """Reject incomplete positions or ambiguous flat entry bases."""
        _specification(self.specification)
        quantity = _decimal(self.quantity, "quantity")
        if quantity.is_zero():
            if self.entry_price is not None:
                raise ValueError("A flat contract position must have no entry price")
        else:
            _decimal(self.entry_price, "entry_price", positive=True)


@dataclass(frozen=True, slots=True)
class ContractPositionFill:
    """Fee-exclusive outcome of one fill with no implicit cash movement.

    Attributes:
        position: Resulting position under the unchanged specification.
        realized_settlement_pnl: Price-realized P&L in the model's settlement
            denomination: quote for linear and base for inverse. This is not an
            account payout amount; actual currency identity comes from metadata.
        closed_quantity: Non-negative number of old contracts closed by the fill.
        opened_quantity: Non-negative number of new contracts added or opened.
    """

    position: ContractPosition
    realized_settlement_pnl: Decimal
    closed_quantity: Decimal
    opened_quantity: Decimal


def _calculate(operation: Callable[[], Decimal]) -> Decimal:
    """Convert Decimal traps and non-finite arithmetic into explicit refusals.

    Args:
        operation: Arithmetic using already validated operands.

    Returns:
        The finite arithmetic result under the caller's context.
    """
    try:
        result = operation()
    except DecimalException as exc:
        raise ValueError("Contract position arithmetic failed in the Decimal context") from exc
    if not result.is_finite():
        raise ValueError("Contract position arithmetic produced a non-finite Decimal")
    return result


def _sum_quantities(first: Decimal, second: Decimal) -> Decimal:
    """Require exact quantity conservation without changing the caller's context.

    Args:
        first: Existing finite signed contract quantity.
        second: Finite signed contract quantity added by the fill.

    Returns:
        Their exact signed sum at the caller's available precision.
    """
    with localcontext() as quantity_context:
        quantity_context.traps[Inexact] = True
        return _calculate(lambda: first + second)


def _mean_entry(
    position: ContractPosition,
    fill_quantity: Decimal,
    fill_price: Decimal,
) -> Decimal:
    """Combine same-side entries using the declared linear or inverse basis.

    Args:
        position: Validated non-flat contract position.
        fill_quantity: Positive magnitude of the same-side fill.
        fill_price: Positive finite price of the incoming fill.

    Returns:
        The arithmetic or harmonic entry under the caller's Decimal context.
    """
    old_quantity = position.quantity.copy_abs()
    entry = cast(Decimal, position.entry_price)
    total_quantity = old_quantity + fill_quantity
    if isinstance(position.specification, LinearContractValuation):
        return (old_quantity * entry + fill_quantity * fill_price) / total_quantity
    return total_quantity / (old_quantity / entry + fill_quantity / fill_price)


def _increasing_fill(
    position: ContractPosition,
    fill_quantity: Decimal,
    fill_price: Decimal,
    new_quantity: Decimal,
) -> ContractPositionFill:
    """Add contracts without realizing P&L or assigning fees to a currency.

    Args:
        position: Validated non-flat contract position.
        fill_quantity: Nonzero signed fill in the existing position's direction.
        fill_price: Positive finite price of the incoming fill.
        new_quantity: Exactly conserved position quantity after the fill.

    Returns:
        The increased position, zero realized P&L and added contract magnitude.
    """
    opened = fill_quantity.copy_abs()
    entry = _calculate(lambda: _mean_entry(position, opened, fill_price))
    updated = ContractPosition(position.specification, new_quantity, entry)
    return ContractPositionFill(updated, Decimal(0), Decimal(0), opened)


def _closing_fill(
    position: ContractPosition,
    fill_quantity: Decimal,
    fill_price: Decimal,
    new_quantity: Decimal,
) -> ContractPositionFill:
    """Reduce, close or flip using only the quantity actually closed for P&L.

    Args:
        position: Validated non-flat contract position.
        fill_quantity: Nonzero signed fill opposing the existing position.
        fill_price: Positive finite price of the incoming fill.
        new_quantity: Exactly conserved position quantity after the fill.

    Returns:
        The remaining or flipped position with realized settlement P&L.
    """
    closed = min(position.quantity.copy_abs(), fill_quantity.copy_abs())
    signed_closed = closed if position.quantity > 0 else closed.copy_negate()
    realized = settlement_pnl(
        position.specification, signed_closed, cast(Decimal, position.entry_price), fill_price
    )
    opened = Decimal(0)
    if new_quantity.is_zero():
        updated = ContractPosition(position.specification, Decimal(0), None)
    elif (new_quantity > 0) == (position.quantity > 0):
        updated = ContractPosition(position.specification, new_quantity, position.entry_price)
    else:
        opened = new_quantity.copy_abs()
        updated = ContractPosition(position.specification, new_quantity, fill_price)
    return ContractPositionFill(updated, realized, closed, opened)


def apply_contract_fill(
    specification: ContractValuation,
    position: ContractPosition,
    signed_fill_quantity: Decimal,
    fill_price: Decimal,
) -> ContractPositionFill:
    """Apply one signed fill to an immutable explicitly interpreted position.

    Args:
        specification: Model and multiplier required to match the existing pool.
        position: Validated position; the input record is never mutated.
        signed_fill_quantity: Finite signed normalized contract quantity.
        fill_price: Positive finite quote price per base-asset unit.

    Returns:
        The new position, settlement P&L and closed/opened contract quantities.
        A zero fill returns the original position after validating every input.

    Raises:
        ValueError: For invalid operands, changed specifications, insufficient
            quantity precision, trapped arithmetic or a non-finite result.
    """
    model = _specification(specification)
    if not isinstance(position, ContractPosition):
        raise ValueError("position must be a ContractPosition")
    if position.specification != model:
        raise ValueError("Contract valuation specification changed within the position pool")
    quantity = _decimal(signed_fill_quantity, "signed_fill_quantity")
    price = _decimal(fill_price, "fill_price", positive=True)
    if quantity.is_zero():
        return ContractPositionFill(position, Decimal(0), Decimal(0), Decimal(0))
    if position.quantity.is_zero():
        updated = ContractPosition(model, quantity, price)
        return ContractPositionFill(updated, Decimal(0), Decimal(0), quantity.copy_abs())
    new_quantity = _sum_quantities(position.quantity, quantity)
    if (position.quantity > 0) == (quantity > 0):
        return _increasing_fill(position, quantity, price, new_quantity)
    return _closing_fill(position, quantity, price, new_quantity)
