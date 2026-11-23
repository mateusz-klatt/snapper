"""Derived display precision and dust classification for one position.

Clients format quantity and price from the venue lot and tick, and group
dust from the lot-rounded quantity. Comparisons use ``Decimal`` values
taken from each stored float's shortest round-trip representation, so a
binary residue such as ``9.99999993923e-9`` is the lot step ``1e-8``
when that is the instrument's lot size.
"""

from collections.abc import Mapping
from dataclasses import dataclass
from decimal import ROUND_HALF_UP
from decimal import Decimal
from math import isfinite
from typing import cast

__all__ = [
    "PositionDisplay",
    "PositionDisplayInput",
    "PositionSpecSteps",
    "classify_position_display",
    "display_for_quantity",
    "optional_float",
    "position_spec_steps",
]


@dataclass(frozen=True, slots=True)
class PositionSpecSteps:
    """Venue increments used to display and classify one instrument.

    Attributes:
        lot_size: Quantity increment. None when the spec omits it.
        tick_size: Price increment. None when the spec omits it.
        min_order_size: Venue minimum order quantity. None when omitted.
    """

    lot_size: float | None
    tick_size: float | None
    min_order_size: float | None


@dataclass(frozen=True, slots=True)
class PositionDisplayInput:
    """Exact inputs for one dust and precision decision.

    Attributes:
        quantity: Stored position quantity, signed.
        lot_size: Venue lot step, or None when the spec has none.
        tick_size: Venue tick step, or None when the spec has none.
        min_order_size: Venue minimum order quantity, or None.
        mark_price: Quote-currency mark, or None when no usable mark exists.
        average_price: Quote-currency entry, used when the mark is absent.
        notional_threshold: Quote-currency notional below which the
            position is dust. Compared with exact decimal arithmetic.
    """

    quantity: float
    lot_size: float | None
    tick_size: float | None
    min_order_size: float | None
    mark_price: float | None
    average_price: float | None
    notional_threshold: Decimal


@dataclass(frozen=True, slots=True)
class PositionDisplay:
    """Additive display fields for a position or position-cycle payload.

    Attributes:
        is_dust: True when the lot-rounded absolute quantity is below the
            venue minimum, or the quote notional is below the threshold.
        quantity_decimals: Decimal places of the lot step, or None.
        price_decimals: Decimal places of the tick step, or None.
    """

    is_dust: bool
    quantity_decimals: int | None
    price_decimals: int | None


def optional_float(value: object) -> float | None:
    """Return a numeric value as ``float``, ignoring booleans and other types.

    Args:
        value: Candidate stored number.

    Returns:
        The value as ``float``, or None when it is not a real number.
    """
    if isinstance(value, bool) or not isinstance(value, int | float):
        return None
    return float(value)


def position_spec_steps(spec: object) -> PositionSpecSteps:
    """Read lot, tick, and minimum size from an instrument-spec mapping.

    Args:
        spec: Active instrument spec mapping, or any other object when the
            spec is missing.

    Returns:
        Display steps. Missing or non-numeric fields are None.
    """
    if not isinstance(spec, Mapping):
        return PositionSpecSteps(None, None, None)
    mapping = cast(Mapping[str, object], spec)
    return PositionSpecSteps(
        lot_size=optional_float(mapping.get("lot_size")),
        tick_size=optional_float(mapping.get("tick_size")),
        min_order_size=optional_float(mapping.get("min_order_size")),
    )


def classify_position_display(inputs: PositionDisplayInput) -> PositionDisplay:
    """Classify dust and derive display precision for one position.

    Args:
        inputs: Stored quantity, quote prices, venue steps, and the
            notional threshold.

    Returns:
        Dust flag and the decimal places clients should use.
    """
    return PositionDisplay(
        is_dust=_is_dust(inputs),
        quantity_decimals=step_decimal_places(inputs.lot_size),
        price_decimals=step_decimal_places(inputs.tick_size),
    )


def display_for_quantity(
    quantity: float,
    steps: PositionSpecSteps | None,
    notional_threshold: Decimal,
    mark_price: float | None = None,
    average_price: float | None = None,
) -> PositionDisplay:
    """Classify one quantity against optional venue steps and quote prices.

    Args:
        quantity: Stored signed quantity, or a cycle peak quantity.
        steps: Venue lot, tick, and minimum. None uses the notional rule
            only, and only when a positive quote price is present.
        notional_threshold: Quote-currency dust threshold.
        mark_price: Preferred quote price.
        average_price: Quote price used when the mark is missing or not
            a positive finite number.

    Returns:
        Dust flag and display precision for the supplied quantity.
    """
    spec = steps or PositionSpecSteps(None, None, None)
    return classify_position_display(
        PositionDisplayInput(
            quantity=quantity,
            lot_size=spec.lot_size,
            tick_size=spec.tick_size,
            min_order_size=spec.min_order_size,
            mark_price=mark_price,
            average_price=average_price,
            notional_threshold=notional_threshold,
        )
    )


def step_decimal_places(step: float | None) -> int | None:
    """Return the decimal places of a positive increment.

    Args:
        step: Lot or tick size. Non-positive and non-finite values have
            no display scale.

    Returns:
        Decimal places, with whole-number increments returning 0, or
        None when the increment is unusable.
    """
    parsed = _positive_step(step)
    if parsed is None:
        return None
    exponent = cast(int, parsed.normalize().as_tuple().exponent)
    return max(0, -exponent)


def _is_dust(inputs: PositionDisplayInput) -> bool:
    """Return whether the lot-rounded quantity or its quote notional is dust."""
    quantity = _stored_decimal(inputs.quantity)
    if quantity is None:
        return False
    rounded = _round_to_step(quantity, _positive_step(inputs.lot_size))
    if _below_minimum(rounded, inputs.min_order_size):
        return True
    notional = _notional(rounded, _quote_price(inputs.mark_price, inputs.average_price))
    if notional is None:
        return False
    return notional < inputs.notional_threshold


def _stored_decimal(value: float) -> Decimal | None:
    """Return the shortest round-trip decimal of a finite float."""
    if not isfinite(value):
        return None
    return Decimal(str(value))


def _positive_step(value: float | None) -> Decimal | None:
    """Return a positive finite increment as a decimal, or None."""
    if value is None or not isfinite(value) or value <= 0:
        return None
    return Decimal(str(value))


def _round_to_step(quantity: Decimal, step: Decimal | None) -> Decimal:
    """Round a signed quantity to the nearest lot, half away from zero."""
    if step is None:
        return quantity
    units = (quantity / step).quantize(Decimal("1"), rounding=ROUND_HALF_UP)
    return units * step


def _below_minimum(quantity: Decimal, minimum: float | None) -> bool:
    """Return whether the absolute quantity is strictly below a venue minimum."""
    parsed = _positive_step(minimum)
    if parsed is None:
        return False
    return abs(quantity) < parsed


def _quote_price(mark_price: float | None, average_price: float | None) -> Decimal | None:
    """Return the mark when it is a positive finite price, otherwise the entry."""
    for candidate in (mark_price, average_price):
        parsed = _positive_step(candidate)
        if parsed is not None:
            return parsed
    return None


def _notional(quantity: Decimal, price: Decimal | None) -> Decimal | None:
    """Return the absolute quote notional of a lot-rounded quantity."""
    if price is None:
        return None
    return abs(quantity * price)
