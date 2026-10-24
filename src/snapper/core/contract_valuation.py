"""Value explicitly described linear and inverse contracts using Decimal amounts.

Linear multipliers represent base-asset units per contract. Their notionals and
settlement P&L are denominated in quote currency. Inverse multipliers represent
quote-currency units per contract. Their notionals are denominated in quote
currency, while settlement P&L is denominated in the base asset.

All arithmetic follows the caller's Decimal context, including its precision,
rounding and traps. Trapped Decimal errors, non-finite results and rounded-away
nonzero results raise ValueError. Inputs are never coerced from other numeric
types, and no payoff model is inferred from an instrument name.
"""

from collections.abc import Callable
from dataclasses import dataclass
from dataclasses import field
from decimal import Decimal
from decimal import DecimalException
from enum import StrEnum
from typing import Literal


class ContractPayoffModel(StrEnum):
    """Explicit settlement model observed in a contract specification."""

    LINEAR = "linear"
    INVERSE = "inverse"


@dataclass(frozen=True, slots=True)
class LinearContractValuation:
    """A contract whose multiplier is denominated in base-asset units.

    Attributes:
        base_units_per_contract: Positive finite base-asset quantity per contract.
        payoff_model: Explicit linear model; settlement P&L is in quote currency.
    """

    base_units_per_contract: Decimal
    payoff_model: Literal[ContractPayoffModel.LINEAR] = field(
        default=ContractPayoffModel.LINEAR,
        init=False,
    )

    def __post_init__(self) -> None:
        """Reject unverified, non-Decimal or non-positive contract multipliers."""
        _require_decimal(self.base_units_per_contract, "base_units_per_contract", positive=True)


@dataclass(frozen=True, slots=True)
class InverseContractValuation:
    """A contract whose multiplier is denominated in quote-currency units.

    Attributes:
        quote_units_per_contract: Positive finite quote-currency amount per contract.
        payoff_model: Explicit inverse model; settlement P&L is in the base asset.
    """

    quote_units_per_contract: Decimal
    payoff_model: Literal[ContractPayoffModel.INVERSE] = field(
        default=ContractPayoffModel.INVERSE,
        init=False,
    )

    def __post_init__(self) -> None:
        """Reject unverified, non-Decimal or non-positive contract multipliers."""
        _require_decimal(self.quote_units_per_contract, "quote_units_per_contract", positive=True)


type ContractValuation = LinearContractValuation | InverseContractValuation


def _require_decimal(value: object, name: str, *, positive: bool = False) -> Decimal:
    """Validate one finite Decimal operand without converting its type.

    Args:
        value: Operand supplied by the caller.
        name: Operand name used in validation errors.
        positive: Whether zero and negative values must also be rejected.

    Returns:
        The original validated Decimal object.

    Raises:
        ValueError: If the operand is not a finite Decimal of the required sign.
    """
    if not isinstance(value, Decimal) or not value.is_finite():
        raise ValueError(f"{name} must be a finite Decimal")
    if positive and value <= 0:
        raise ValueError(f"{name} must be positive")
    return value


def _require_specification(spec: object) -> ContractValuation:
    """Require an explicit supported contract model before performing arithmetic.

    Args:
        spec: Contract multiplier and payoff model supplied by the caller.

    Returns:
        The validated linear or inverse specification.

    Raises:
        ValueError: If the specification does not identify a supported model.
    """
    if not isinstance(spec, (LinearContractValuation, InverseContractValuation)):
        raise ValueError("An explicit linear or inverse contract specification is required")
    return spec


def _calculate(operation: Callable[[], Decimal]) -> Decimal:
    """Apply the caller's Decimal context and reject failed monetary arithmetic.

    Args:
        operation: Arithmetic operation using already validated Decimal operands.

    Returns:
        The finite Decimal result.

    Raises:
        ValueError: If arithmetic traps or produces a non-finite result.
    """
    try:
        result = operation()
    except DecimalException as exc:
        raise ValueError("Contract valuation failed in the current Decimal context") from exc
    if not result.is_finite():
        raise ValueError("Contract valuation produced a non-finite Decimal result")
    return result


def quote_notional(spec: ContractValuation, quantity: Decimal, price: Decimal) -> Decimal:
    """Calculate unsigned contract notional denominated in quote currency.

    The supplied positive price is quote currency per base-asset unit. It is
    validated for both models; inverse quote notional is independent of its
    value. Zero quantity returns zero after all inputs have been validated.

    Args:
        spec: Explicit payoff model and denominated contract multiplier.
        quantity: Finite signed number of contracts.
        price: Positive finite quote-currency price per base-asset unit.

    Returns:
        Absolute quote-currency notional under the caller's Decimal context.

    Raises:
        ValueError: If metadata or operands are invalid, arithmetic fails, or a
            nonzero notional rounds to zero in the caller's Decimal context.
    """
    model = _require_specification(spec)
    checked_quantity = _require_decimal(quantity, "quantity")
    checked_price = _require_decimal(price, "price", positive=True)
    if checked_quantity.is_zero():
        return Decimal(0)
    result = _calculate(lambda: _quote_notional(model, checked_quantity, checked_price))
    if result.is_zero():
        raise ValueError("Nonzero contract notional rounded to zero in the Decimal context")
    return result


def _quote_notional(spec: ContractValuation, quantity: Decimal, price: Decimal) -> Decimal:
    """Apply the dimensional notional formula inside guarded Decimal arithmetic.

    Args:
        spec: Validated explicit contract specification.
        quantity: Validated finite signed contract quantity.
        price: Validated positive quote price per base-asset unit.

    Returns:
        Absolute notional in quote-currency units.
    """
    if isinstance(spec, LinearContractValuation):
        return abs(quantity) * spec.base_units_per_contract * price
    return abs(quantity) * spec.quote_units_per_contract


def settlement_pnl(
    spec: ContractValuation,
    signed_quantity: Decimal,
    entry_price: Decimal,
    exit_price: Decimal,
) -> Decimal:
    """Calculate signed P&L in the settlement unit of an explicit contract model.

    Positive quantities represent longs and negative quantities represent shorts.
    Linear P&L is in quote currency; inverse P&L is in the base asset. Zero
    quantity or unchanged price returns zero after all inputs are validated.

    Args:
        spec: Explicit payoff model and denominated contract multiplier.
        signed_quantity: Finite signed number of contracts being closed or marked.
        entry_price: Positive finite entry price in quote currency per base unit.
        exit_price: Positive finite exit price in quote currency per base unit.

    Returns:
        Signed settlement P&L under the caller's Decimal context.

    Raises:
        ValueError: If metadata or operands are invalid, arithmetic fails, or a
            nonzero payoff rounds to zero in the caller's Decimal context.
    """
    model = _require_specification(spec)
    quantity = _require_decimal(signed_quantity, "signed_quantity")
    entry = _require_decimal(entry_price, "entry_price", positive=True)
    exit_mark = _require_decimal(exit_price, "exit_price", positive=True)
    if quantity.is_zero() or entry == exit_mark:
        return Decimal(0)
    result = _calculate(lambda: _settlement_pnl(model, quantity, entry, exit_mark))
    if result.is_zero():
        raise ValueError("Nonzero contract P&L rounded to zero in the Decimal context")
    return result


def _settlement_pnl(
    spec: ContractValuation,
    quantity: Decimal,
    entry_price: Decimal,
    exit_price: Decimal,
) -> Decimal:
    """Apply the dimensional payoff formula inside guarded Decimal arithmetic.

    Args:
        spec: Validated explicit contract specification.
        quantity: Validated signed number of contracts.
        entry_price: Validated positive entry price in quote units per base unit.
        exit_price: Validated positive exit price in quote units per base unit.

    Returns:
        Linear quote-currency P&L or inverse base-asset P&L.
    """
    if isinstance(spec, LinearContractValuation):
        return quantity * spec.base_units_per_contract * (exit_price - entry_price)
    return (
        quantity
        * spec.quote_units_per_contract
        * (Decimal(1) / entry_price - Decimal(1) / exit_price)
    )
