"""Derived display precision and dust classification for one position.

Clients format quantity and price from the venue lot and tick, and group
dust from the lot-rounded quantity. Comparisons use ``Decimal`` values
taken from each stored float's shortest round-trip representation, so a
binary residue such as ``9.99999993923e-9`` is the lot step ``1e-8``
when that is the instrument's lot size.

``POSITION_DUST_NOTIONAL_THRESHOLD`` is a USD amount. Spot notionals are
``abs(lot-rounded quantity * quote price)``. Kraken futures notionals use
the venue contract multiplier and payoff: linear contracts multiply by
``contract_size`` and the quote price, and inverse contracts use
``contract_size`` quote units per contract. The quote amount is then
scaled by the USD rate from the portfolio FX candles. An exact zero
quantity is flat and is never dust. A non-zero quantity that lot-rounds
to zero is dust. A missing rate, or a derivative whose contract value
is unknown, skips the notional rule and leaves the lot-round rule and
the venue minimum.
"""

from collections.abc import Mapping
from dataclasses import dataclass
from decimal import ROUND_HALF_UP
from decimal import Decimal
from math import isfinite
from typing import cast

from snapper.core.contract_valuation import ContractValuation
from snapper.core.contract_valuation import InverseContractValuation
from snapper.core.contract_valuation import LinearContractValuation
from snapper.core.contract_valuation import quote_notional
from snapper.core.types import ExchangeEnum

__all__ = [
    "PositionDisplay",
    "PositionDisplayInput",
    "PositionSpecSteps",
    "classify_position_display",
    "display_for_quantity",
    "display_steps_for_fact",
    "optional_float",
    "position_spec_steps",
]

_USD = "USD"
_DISPLAY_STEP_QUANTUM = Decimal("1e-12")
_KRAKEN_FUTURES = ExchangeEnum.KRAKEN_FUTURES.value
_DERIVATIVE_KINDS = frozenset({"perpetual", "future", "option"})


@dataclass(frozen=True, slots=True)
class PositionSpecSteps:
    """Venue increments and valuation evidence for one instrument.

    Attributes:
        lot_size: Quantity increment. None when the spec omits it.
        tick_size: Price increment. None when the spec omits it.
        min_order_size: Venue minimum order quantity. None when omitted.
        quote_currency: Instrument quote currency. None when unknown.
        usd_per_quote: USD value of one quote unit from the portfolio FX
            candles. None when no rate was found. USD quotes do not need one.
        contract: Explicit linear or inverse multiplier when the contract
            value is known. None for spot and for unknown derivatives.
        derivative_notional_unknown: True when the instrument is a
            derivative whose contract value cannot be derived. The notional
            rule is skipped in that case.
    """

    lot_size: float | None
    tick_size: float | None
    min_order_size: float | None
    quote_currency: str | None = None
    usd_per_quote: Decimal | None = None
    contract: ContractValuation | None = None
    derivative_notional_unknown: bool = False


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
        notional_threshold: USD notional below which the position is dust.
        quote_currency: Quote currency of the notional. None skips the
            notional rule rather than treating the quote as USD.
        usd_per_quote: USD per quote unit. Ignored for USD quotes. None
            skips the notional rule for every other quote.
        contract: Known contract multiplier, or None for spot math.
        derivative_notional_unknown: Skip the notional rule when the
            contract value cannot be derived.
    """

    quantity: float
    lot_size: float | None
    tick_size: float | None
    min_order_size: float | None
    mark_price: float | None
    average_price: float | None
    notional_threshold: Decimal
    quote_currency: str | None = None
    usd_per_quote: Decimal | None = None
    contract: ContractValuation | None = None
    derivative_notional_unknown: bool = False


@dataclass(frozen=True, slots=True)
class PositionDisplay:
    """Additive display fields for a position or position-cycle payload.

    Attributes:
        is_dust: True when a non-zero quantity lot-rounds to zero, the
            lot-rounded absolute quantity is below the venue minimum, or
            the USD notional is below the threshold. An exact zero
            quantity is flat and is never dust.
        quantity_decimals: Decimal places of the lot step, or None.
        price_decimals: Decimal places of the tick step, or None.
    """

    is_dust: bool
    quantity_decimals: int | None
    price_decimals: int | None


@dataclass(frozen=True, slots=True)
class PositionContractEvidence:
    """Columns that decide whether a position has a known contract value.

    Attributes:
        exchange: Venue code, or None when the instrument identity is missing.
        base: Base asset code.
        quote: Quote currency code.
        contract_size: Venue multiplier, already parsed as Decimal.
        quantity_unit: Internal position unit, ``base_asset`` or
            ``contract_count``.
        native_order_quantity_unit: Venue order unit.
        payoff_model: ``linear``, ``inverse``, or None.
        contract_value_currency: Currency the multiplier is denominated in.
        settlement_currency: Contract P&L currency before payout conversion.
        instrument_kind: Product type such as ``spot`` or ``perpetual``.
    """

    exchange: str | None
    base: str | None
    quote: str | None
    contract_size: Decimal | None
    quantity_unit: str | None
    native_order_quantity_unit: str | None
    payoff_model: str | None
    contract_value_currency: str | None
    settlement_currency: str | None
    instrument_kind: str | None


@dataclass(frozen=True, slots=True)
class PositionContractDecision:
    """Contract model used for one dust notional, or an explicit skip.

    Attributes:
        contract: Linear or inverse model. None for spot and for skips.
        unknown: True when a derivative has no usable contract value and
            the notional rule must be skipped.
    """

    contract: ContractValuation | None
    unknown: bool


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


def display_steps_for_fact(
    fact: object,
    usd_per_quote: Decimal | None = None,
) -> PositionSpecSteps:
    """Build display steps from one batched instrument fact.

    Args:
        fact: Column mapping from the position-display spec read. Any other
            object contributes empty steps.
        usd_per_quote: USD per quote unit from the portfolio FX candles.
            None leaves a non-USD quote without a notional rate.

    Returns:
        Lot, tick, minimum, quote, rate, and contract evidence. A derivative
        with no known contract value sets ``derivative_notional_unknown``.
    """
    if not isinstance(fact, Mapping):
        return PositionSpecSteps(None, None, None)
    mapping = cast(Mapping[str, object], fact)
    decision = resolve_position_contract(_contract_evidence(mapping))
    return PositionSpecSteps(
        lot_size=optional_float(mapping.get("lot_size")),
        tick_size=optional_float(mapping.get("tick_size")),
        min_order_size=optional_float(mapping.get("min_order_size")),
        quote_currency=_optional_text(mapping.get("quote")),
        usd_per_quote=usd_per_quote,
        contract=decision.contract,
        derivative_notional_unknown=decision.unknown,
    )


def classify_position_display(inputs: PositionDisplayInput) -> PositionDisplay:
    """Classify dust and derive display precision for one position.

    Args:
        inputs: Stored quantity, quote prices, venue steps, USD threshold,
            and the FX rate or contract model when one is known.

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
    """Classify one quantity against venue steps, prices, and USD evidence.

    Args:
        quantity: Stored signed quantity, or a cycle peak quantity.
        steps: Venue lot, tick, minimum, quote, USD rate, and contract.
            None uses neither a minimum nor a notional.
        notional_threshold: USD dust threshold.
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
            quote_currency=spec.quote_currency,
            usd_per_quote=spec.usd_per_quote,
            contract=spec.contract,
            derivative_notional_unknown=spec.derivative_notional_unknown,
        )
    )


def resolve_position_contract(evidence: PositionContractEvidence) -> PositionContractDecision:
    """Resolve a Kraken futures contract, or record that the value is unknown.

    Spot instruments with no contract evidence use quantity times price.
    Kraken futures use ``contract_size`` and the payoff currencies published
    with the instrument. Any other derivative, including a futures contract
    whose multiplier or payoff is missing, skips the notional rule.

    Args:
        evidence: Identity and spec columns for one instrument.

    Returns:
        A known contract model, a spot decision, or an unknown derivative.
    """
    if not _derivative_evidence(evidence):
        return PositionContractDecision(None, False)
    contract = _kraken_futures_contract(evidence)
    if contract is None:
        return PositionContractDecision(None, True)
    return PositionContractDecision(contract, False)


def step_decimal_places(step: float | None) -> int | None:
    """Return the decimal places of a positive increment.

    The increment is quantized to ``1e-12`` before the places are counted,
    so a binary residue such as ``0.30000000000000004`` reports the places
    of ``0.3`` and cannot report 17. A step that quantizes to zero has no
    usable display scale.

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
    quantized = parsed.quantize(_DISPLAY_STEP_QUANTUM, rounding=ROUND_HALF_UP)
    if quantized <= 0:
        return None
    exponent = cast(int, quantized.normalize().as_tuple().exponent)
    return max(0, -exponent)


def _is_dust(inputs: PositionDisplayInput) -> bool:
    """Return whether the quantity or its USD notional is dust.

    An exact zero quantity is flat and is never dust. A non-zero quantity
    that lot-rounds to zero is dust. Otherwise the lot-rounded absolute
    quantity is dust when it is below the venue minimum, or when its USD
    notional is below the threshold.
    """
    quantity = _stored_decimal(inputs.quantity)
    if quantity is None or quantity == 0:
        return False
    rounded = _round_to_step(quantity, _positive_step(inputs.lot_size))
    if rounded == 0 or _below_minimum(rounded, inputs.min_order_size):
        return True
    notional = _usd_notional(inputs, rounded)
    return notional is not None and notional < inputs.notional_threshold


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


def _usd_notional(inputs: PositionDisplayInput, quantity: Decimal) -> Decimal | None:
    """Return the absolute USD notional, or None when it cannot be priced."""
    if inputs.derivative_notional_unknown:
        return None
    price = _quote_price(inputs.mark_price, inputs.average_price)
    if price is None:
        return None
    return _amount_in_usd(
        _quote_amount(quantity, price, inputs.contract),
        inputs.quote_currency,
        inputs.usd_per_quote,
    )


def _quote_amount(
    quantity: Decimal,
    price: Decimal,
    contract: ContractValuation | None,
) -> Decimal:
    """Return the absolute quote notional of a spot quantity or a known contract."""
    if contract is None:
        return abs(quantity * price)
    return quote_notional(contract, quantity, price)


def _amount_in_usd(
    amount: Decimal,
    quote_currency: str | None,
    usd_per_quote: Decimal | None,
) -> Decimal | None:
    """Scale a quote amount into USD, or return None when the rate is missing.

    USD is an identity and does not consult ``usd_per_quote``. Every other
    currency needs a positive finite portfolio rate. No rate is invented.
    """
    if not quote_currency:
        return None
    if quote_currency == _USD:
        return amount
    if usd_per_quote is None or not usd_per_quote.is_finite() or usd_per_quote <= 0:
        return None
    return abs(amount * usd_per_quote)


def _contract_evidence(mapping: Mapping[str, object]) -> PositionContractEvidence:
    """Read contract columns from one fact mapping."""
    return PositionContractEvidence(
        exchange=_optional_text(mapping.get("exchange")),
        base=_optional_text(mapping.get("base")),
        quote=_optional_text(mapping.get("quote")),
        contract_size=_optional_decimal(mapping.get("contract_size")),
        quantity_unit=_optional_text(mapping.get("quantity_unit")),
        native_order_quantity_unit=_optional_text(mapping.get("native_order_quantity_unit")),
        payoff_model=_optional_text(mapping.get("contract_payoff_model")),
        contract_value_currency=_optional_text(mapping.get("contract_value_currency")),
        settlement_currency=_optional_text(mapping.get("settlement_currency")),
        instrument_kind=_optional_text(mapping.get("instrument_kind")),
    )


def _derivative_evidence(evidence: PositionContractEvidence) -> bool:
    """Return whether the row must be valued as a derivative or not at all."""
    if evidence.exchange == _KRAKEN_FUTURES:
        return True
    if evidence.quantity_unit == "contract_count":
        return True
    if evidence.native_order_quantity_unit == "contract_count":
        return True
    if evidence.instrument_kind in _DERIVATIVE_KINDS:
        return True
    return evidence.payoff_model is not None


def _kraken_futures_contract(evidence: PositionContractEvidence) -> ContractValuation | None:
    """Return the Kraken futures model, or None when the value is not explicit."""
    if evidence.exchange != _KRAKEN_FUTURES:
        return None
    if evidence.instrument_kind not in {"perpetual", "future"}:
        return None
    size = evidence.contract_size
    if size is None or not size.is_finite() or size <= 0:
        return None
    if not _supported_native_unit(evidence, size):
        return None
    return _payoff_contract(evidence, size)


def _supported_native_unit(evidence: PositionContractEvidence, size: Decimal) -> bool:
    """Accept contract counts, or a unit flexible contract quoted in the base asset."""
    native = evidence.native_order_quantity_unit
    if native == "base_asset":
        return evidence.payoff_model == "linear" and size == 1
    return native == "contract_count"


def _payoff_contract(
    evidence: PositionContractEvidence,
    size: Decimal,
) -> ContractValuation | None:
    """Match payoff currencies to the instrument base and quote."""
    currencies = (evidence.contract_value_currency, evidence.settlement_currency)
    if evidence.payoff_model == "linear" and currencies == (evidence.base, evidence.quote):
        return LinearContractValuation(size)
    if evidence.payoff_model == "inverse" and currencies == (evidence.quote, evidence.base):
        return InverseContractValuation(size)
    return None


def _optional_text(value: object) -> str | None:
    """Return a non-empty string, or None."""
    if isinstance(value, str) and value:
        return value
    return None


def _optional_decimal(value: object) -> Decimal | None:
    """Return a finite Decimal, parsing real numbers without guessing text."""
    if isinstance(value, Decimal):
        return value if value.is_finite() else None
    if isinstance(value, bool) or not isinstance(value, int | float):
        return None
    if isinstance(value, float) and not isfinite(value):
        return None
    return Decimal(str(value))
