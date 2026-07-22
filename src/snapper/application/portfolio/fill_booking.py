"""Position-quantity booking rules for execution fills."""

import math

from snapper.core.types import ExchangeEnum
from snapper.core.types import TradeSideEnum

_BASE_ASSET_QUANTITY_UNIT = "base_asset"
_CONTRACT_QUANTITY_UNIT = "contract_count"


def resolve_position_quantity_unit(exchange: str) -> str:
    """Resolve whether an execution books base inventory or contract count.

    Args:
        exchange: Immutable execution venue identity.

    Returns:
        Contract count for the derivative execution venue, otherwise base
        asset for the spot execution venues.
    """
    if exchange == ExchangeEnum.KRAKEN_FUTURES:
        return _CONTRACT_QUANTITY_UNIT
    return _BASE_ASSET_QUANTITY_UNIT


def native_base_asset(instrument: str) -> str:
    """Return the base asset from a valid canonical ``BASE-QUOTE`` symbol.

    Args:
        instrument: Snapper native instrument symbol.

    Returns:
        The exact base identity, or an empty string for a malformed symbol.
    """
    base_asset, separator, quote_asset = instrument.partition("-")
    if separator and base_asset and quote_asset:
        return base_asset
    return ""


def _base_fee_reduces_quantity(
    fill_size: float,
    fee: float,
    fee_asset: str | None,
    base_asset: str,
    quantity_unit: str,
) -> bool:
    """Return whether a fee is booked against base inventory rather than cash.

    A base-denominated fee is deducted from the received (BUY) or delivered
    (SELL) base quantity, not paid in the quote/cash currency. It qualifies only
    for spot inventory measured in the base asset, on a canonical base identity,
    with a finite fee strictly smaller than the fill so the signed quantity can
    never cross zero, and whose gross-plus-fee magnitude stays finite so the
    booked net quantity is always finite. Every other case (contract-count
    venues, a non-base fee asset, a non-finite fee, a fee at least as large as
    the fill, or a non-finite gross-plus-fee magnitude) leaves the fee on the
    cash leg. This single predicate keeps the quantity and the cash treatments
    exactly complementary, so a base fee is counted once — never in both position
    quantity and cash, and never in neither.

    Args:
        fill_size: Gross traded base quantity.
        fee: Fee quantity in ``fee_asset``.
        fee_asset: Canonical asset in which the venue charged the fee.
        base_asset: Canonical base asset of the traded instrument.
        quantity_unit: Unit represented by the position quantity.

    Returns:
        True when the fee reduces booked base quantity, False when it belongs to
        the cash leg.
    """
    if fill_size <= 0.0 or quantity_unit != _BASE_ASSET_QUANTITY_UNIT or not base_asset:
        return False
    if fee_asset != base_asset or not math.isfinite(fee) or fee >= fill_size:
        return False
    return math.isfinite(fill_size + fee)


def booked_signed_quantity(
    side: str,
    fill_size: float,
    fee: float,
    fee_asset: str | None,
    base_asset: str,
    quantity_unit: str,
) -> float:
    """Return the signed venue base-asset delta represented by one fill.

    A fee charged in the instrument's base asset reduces received inventory on
    a BUY and increases delivered inventory on a SELL. Fees in every other
    asset leave booked base quantity unchanged. Asset comparison is exact
    because callers supply canonical identities.

    Args:
        side: Fill side, already validated by the caller as BUY or SELL.
        fill_size: Gross traded base quantity.
        fee: Fee quantity in ``fee_asset``.
        fee_asset: Canonical asset in which the venue charged the fee.
        base_asset: Canonical base asset of the traded instrument.
        quantity_unit: Unit represented by the position quantity.

    Returns:
        Signed inventory or contract delta, with a base-denominated fee applied
        only when the position itself is measured in the base asset.
    """
    gross_delta = fill_size if side == TradeSideEnum.BUY else -fill_size
    if not _base_fee_reduces_quantity(fill_size, fee, fee_asset, base_asset, quantity_unit):
        return gross_delta
    return gross_delta - fee


def cash_fee(
    fill_size: float,
    fee: float,
    fee_asset: str | None,
    base_asset: str,
    quantity_unit: str,
) -> float:
    """Return the fee charged against the cash leg after base-asset booking.

    Zero exactly when the fee was booked against base inventory by
    :func:`booked_signed_quantity`, so a base-asset fee is never subtracted from
    both position quantity and cash; otherwise the full fee, which the cash leg
    charges as a quote-denominated cost.

    Args:
        fill_size: Gross traded base quantity.
        fee: Fee quantity in ``fee_asset``.
        fee_asset: Canonical asset in which the venue charged the fee.
        base_asset: Canonical base asset of the traded instrument.
        quantity_unit: Unit represented by the position quantity.

    Returns:
        ``0.0`` when the fee reduced booked base quantity, otherwise ``fee``.
    """
    if _base_fee_reduces_quantity(fill_size, fee, fee_asset, base_asset, quantity_unit):
        return 0.0
    return fee
