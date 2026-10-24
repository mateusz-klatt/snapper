"""Dimensional, sign and Decimal-context regressions for contract valuation."""

from dataclasses import FrozenInstanceError
from decimal import ROUND_DOWN
from decimal import Decimal
from decimal import Inexact
from decimal import Overflow
from decimal import localcontext
from typing import cast

import pytest

from snapper.core.contract_valuation import ContractPayoffModel
from snapper.core.contract_valuation import ContractValuation
from snapper.core.contract_valuation import InverseContractValuation
from snapper.core.contract_valuation import LinearContractValuation
from snapper.core.contract_valuation import quote_notional
from snapper.core.contract_valuation import settlement_pnl

_INVALID_DECIMALS: tuple[object, ...] = (
    None,
    True,
    False,
    1,
    1.0,
    "1",
    Decimal("NaN"),
    Decimal("sNaN"),
    Decimal("Infinity"),
    Decimal("-Infinity"),
)
_INVALID_POSITIVE_DECIMALS = (*_INVALID_DECIMALS, Decimal(0), Decimal("-1"))
_MODELS: tuple[ContractValuation, ...] = (
    LinearContractValuation(Decimal("0.1")),
    InverseContractValuation(Decimal("100")),
)


@pytest.mark.parametrize("model_type", (LinearContractValuation, InverseContractValuation))
@pytest.mark.parametrize("multiplier", _INVALID_POSITIVE_DECIMALS)
def test_invalid_contract_multiplier_is_rejected(
    model_type: type[LinearContractValuation] | type[InverseContractValuation],
    multiplier: object,
) -> None:
    """Refuse invalid multiplier types and non-positive or non-finite metadata.

    Given: A multiplier that is not a positive finite Decimal,
    When: An explicit contract model is constructed,
    Then: Validation raises without coercing or accepting the metadata.
    """
    with pytest.raises(ValueError):
        model_type(cast(Decimal, multiplier))


def test_model_metadata_retains_multiplier_denomination_without_mutation() -> None:
    """Keep multiplier denomination and explicit payoff models immutable.

    Given: Positive finite Decimal multipliers for linear and inverse contracts,
    When: Their metadata is read and an attempted reassignment is made,
    Then: Original Decimal objects and explicit models remain unchanged.
    """
    base_multiplier = Decimal("0.125")
    quote_multiplier = Decimal("25")
    linear = LinearContractValuation(base_multiplier)
    inverse = InverseContractValuation(quote_multiplier)

    assert linear.base_units_per_contract is base_multiplier
    assert inverse.quote_units_per_contract is quote_multiplier
    assert linear.payoff_model is ContractPayoffModel.LINEAR
    assert inverse.payoff_model is ContractPayoffModel.INVERSE
    for model, field_name in (
        (linear, "base_units_per_contract"),
        (inverse, "quote_units_per_contract"),
    ):
        with pytest.raises(FrozenInstanceError):
            setattr(model, field_name, Decimal(1))


@pytest.mark.parametrize(
    ("spec", "quantity", "expected"),
    (
        (LinearContractValuation(Decimal("0.1")), Decimal("3"), Decimal("6000")),
        (LinearContractValuation(Decimal("0.1")), Decimal("-3"), Decimal("6000")),
        (InverseContractValuation(Decimal("100")), Decimal("3"), Decimal("300")),
        (InverseContractValuation(Decimal("100")), Decimal("-3"), Decimal("300")),
        (LinearContractValuation(Decimal("0.1")), Decimal(0), Decimal(0)),
        (InverseContractValuation(Decimal("100")), Decimal("-0"), Decimal(0)),
    ),
)
def test_quote_notional_uses_explicit_multiplier_units(
    spec: ContractValuation,
    quantity: Decimal,
    expected: Decimal,
) -> None:
    """Calculate quote notional according to the multiplier denomination.

    Given: Signed contract quantities and a quote price of 20000 per base unit,
    When: Notional is calculated for an explicit linear or inverse contract,
    Then: Quote notional is unsigned and uses the declared multiplier units.
    """
    assert quote_notional(spec, quantity, Decimal("20000")) == expected


def test_inverse_quote_notional_does_not_scale_with_mark_price() -> None:
    """Keep inverse quote face value independent of the underlying market price.

    Given: A thousand inverse contracts representing one quote unit each,
    When: The base asset price changes from one to one hundred thousand,
    Then: Both notional observations remain one thousand quote units.
    """
    spec = InverseContractValuation(Decimal(1))

    assert quote_notional(spec, Decimal("1000"), Decimal(1)) == Decimal("1000")
    assert quote_notional(spec, Decimal("1000"), Decimal("100000")) == Decimal("1000")


@pytest.mark.parametrize(
    ("quantity", "entry", "exit_mark", "expected"),
    (
        ("2", "100", "110", "10"),
        ("-2", "100", "110", "-10"),
        ("2", "110", "100", "-10"),
        ("-2", "110", "100", "10"),
        ("0", "100", "110", "0"),
        ("2", "100", "100", "0"),
    ),
)
def test_linear_pnl_settles_signed_price_change_in_quote_currency(
    quantity: str,
    entry: str,
    exit_mark: str,
    expected: str,
) -> None:
    """Calculate long and short linear payoffs with a base-denominated multiplier.

    Given: A linear contract representing half a base unit,
    When: A long or short closes above, below or at its entry price,
    Then: Settlement P&L has the expected sign and quote-currency amount.
    """
    actual = settlement_pnl(
        LinearContractValuation(Decimal("0.5")),
        Decimal(quantity),
        Decimal(entry),
        Decimal(exit_mark),
    )

    assert actual == Decimal(expected)


@pytest.mark.parametrize(
    ("quantity", "entry", "exit_mark", "expected"),
    (
        ("100", "100", "200", "0.5"),
        ("-100", "100", "200", "-0.5"),
        ("100", "200", "100", "-0.5"),
        ("-100", "200", "100", "0.5"),
        ("0", "100", "200", "0"),
        ("100", "100", "100", "0"),
    ),
)
def test_inverse_pnl_settles_reciprocal_price_change_in_base_asset(
    quantity: str,
    entry: str,
    exit_mark: str,
    expected: str,
) -> None:
    """Calculate inverse long and short payoffs in the base settlement asset.

    Given: An inverse contract representing one quote-currency unit,
    When: A long or short closes at a changed underlying price,
    Then: Reciprocal prices produce the expected base-asset settlement amount.
    """
    actual = settlement_pnl(
        InverseContractValuation(Decimal(1)),
        Decimal(quantity),
        Decimal(entry),
        Decimal(exit_mark),
    )

    assert actual == Decimal(expected)


def test_inverse_pnl_applies_quote_denomination_multiplier() -> None:
    """Scale inverse settlement by the declared quote-currency contract value.

    Given: Four inverse contracts each representing twenty-five quote units,
    When: Their underlying price increases from one hundred to two hundred,
    Then: Settlement realizes half a base unit rather than an unscaled payoff.
    """
    actual = settlement_pnl(
        InverseContractValuation(Decimal(25)), Decimal(4), Decimal(100), Decimal(200)
    )

    assert actual == Decimal("0.5")


@pytest.mark.parametrize("spec", _MODELS)
@pytest.mark.parametrize("quantity", _INVALID_DECIMALS)
def test_invalid_quantity_is_rejected_without_coercion(
    spec: ContractValuation,
    quantity: object,
) -> None:
    """Refuse non-Decimal and non-finite signed contract quantities.

    Given: An explicit contract model and an invalid quantity operand,
    When: Notional or settlement P&L is requested,
    Then: Both functions raise before monetary arithmetic.
    """
    invalid_quantity = cast(Decimal, quantity)

    with pytest.raises(ValueError, match="finite Decimal"):
        quote_notional(spec, invalid_quantity, Decimal(100))
    with pytest.raises(ValueError, match="finite Decimal"):
        settlement_pnl(spec, invalid_quantity, Decimal(100), Decimal(110))


@pytest.mark.parametrize("spec", _MODELS)
@pytest.mark.parametrize("price", _INVALID_POSITIVE_DECIMALS)
def test_invalid_prices_are_rejected_even_for_zero_quantity(
    spec: ContractValuation,
    price: object,
) -> None:
    """Validate all price operands before returning a zero position value.

    Given: Zero contracts and an invalid mark, entry or exit price,
    When: Notional or settlement P&L is requested,
    Then: Invalid prices are refused even though zero quantity would return zero.
    """
    invalid_price = cast(Decimal, price)

    with pytest.raises(ValueError):
        quote_notional(spec, Decimal(0), invalid_price)
    with pytest.raises(ValueError):
        settlement_pnl(spec, Decimal(0), invalid_price, Decimal(100))
    with pytest.raises(ValueError):
        settlement_pnl(spec, Decimal(0), Decimal(100), invalid_price)


@pytest.mark.parametrize("spec", (None, True, "linear", {"multiplier": Decimal(1)}))
def test_unspecified_or_untyped_payoff_metadata_is_rejected(spec: object) -> None:
    """Require an explicit typed contract model rather than guessing its payoff.

    Given: Untyped metadata that does not establish multiplier denomination,
    When: Notional or settlement P&L is requested,
    Then: Both functions refuse it instead of choosing a payoff convention.
    """
    invalid_spec = cast(ContractValuation, spec)

    with pytest.raises(ValueError, match="explicit linear or inverse"):
        quote_notional(invalid_spec, Decimal(1), Decimal(100))
    with pytest.raises(ValueError, match="explicit linear or inverse"):
        settlement_pnl(invalid_spec, Decimal(1), Decimal(100), Decimal(110))


def test_arithmetic_respects_context_precision_and_rounding_without_mutating_inputs() -> None:
    """Use the caller's Decimal context while retaining immutable operands.

    Given: A context with six significant digits and rounding toward zero,
    When: Linear notional and an inverse reciprocal payoff are calculated,
    Then: Results follow that context and the input models remain unchanged.
    """
    linear = LinearContractValuation(Decimal("0.125"))
    inverse = InverseContractValuation(Decimal(1))
    quantity = Decimal(2)
    price = Decimal("1.234567")
    with localcontext() as context:
        context.prec = 6
        context.rounding = ROUND_DOWN

        notional = quote_notional(linear, quantity, price)
        pnl = settlement_pnl(inverse, Decimal(1), Decimal(3), Decimal(7))

        assert notional == Decimal("0.308641")
        assert pnl == Decimal("0.190476")
        assert context.prec == 6
        assert context.rounding == ROUND_DOWN
    assert linear.base_units_per_contract == Decimal("0.125")
    assert inverse.quote_units_per_contract == Decimal(1)
    assert quantity == Decimal(2)
    assert price == Decimal("1.234567")


@pytest.mark.parametrize("trap_overflow", (True, False))
def test_decimal_overflow_never_returns_an_infinite_monetary_result(
    trap_overflow: bool,
) -> None:
    """Refuse overflow whether the caller traps it or returns infinity.

    Given: A Decimal context too small to represent the monetary result,
    When: A valid finite contract notional or payoff overflows,
    Then: Both functions raise a validation error instead of returning infinity.
    """
    spec = LinearContractValuation(Decimal(1))
    with localcontext() as context:
        context.Emax = 9
        context.traps[Overflow] = trap_overflow

        with pytest.raises(ValueError, match="Decimal"):
            quote_notional(spec, Decimal("1e9"), Decimal(10))
        with pytest.raises(ValueError, match="Decimal"):
            settlement_pnl(spec, Decimal("1e9"), Decimal(1), Decimal(11))


def test_trapped_inexact_reciprocals_raise_a_context_error() -> None:
    """Honor the caller's decision to trap inexact Decimal arithmetic.

    Given: A Decimal context that traps inexact reciprocal prices,
    When: An inverse contract payoff requires a repeating decimal,
    Then: The calculation raises a contextual validation error.
    """
    with localcontext() as context:
        context.prec = 5
        context.traps[Inexact] = True

        with pytest.raises(ValueError, match="current Decimal context"):
            settlement_pnl(InverseContractValuation(Decimal(1)), Decimal(1), Decimal(3), Decimal(7))


def test_context_underflow_cannot_turn_a_nonzero_notional_or_payoff_into_zero() -> None:
    """Refuse silently rounded-away monetary values for nonzero positions.

    Given: A Decimal context that underflows products of tiny finite operands,
    When: A nonzero contract notional or payoff rounds to zero,
    Then: Both calculations raise instead of reporting a flat position.
    """
    spec = LinearContractValuation(Decimal("1e-9"))
    with localcontext() as context:
        context.prec = 9
        context.Emin = -9
        context.Emax = 9

        with pytest.raises(ValueError, match="rounded to zero"):
            quote_notional(spec, Decimal("1e-9"), Decimal("1e-9"))
        with pytest.raises(ValueError, match="rounded to zero"):
            settlement_pnl(spec, Decimal("1e-9"), Decimal("1e-9"), Decimal("2e-9"))


def test_context_precision_cannot_hide_a_nonzero_inverse_payoff() -> None:
    """Refuse reciprocal cancellation caused by insufficient caller precision.

    Given: Distinct entry and exit prices whose rounded reciprocals coincide,
    When: A nonzero inverse position is valued with one significant digit,
    Then: A rounded-away payoff is refused rather than reported as zero.
    """
    with localcontext() as context:
        context.prec = 1

        with pytest.raises(ValueError, match="rounded to zero"):
            settlement_pnl(
                InverseContractValuation(Decimal(1)), Decimal(1), Decimal(101), Decimal(102)
            )


def test_zero_payoffs_do_not_require_representable_intermediate_products() -> None:
    """Return exact zero for zero quantity or unchanged validated prices.

    Given: Valid finite inputs whose intermediate products would overflow,
    When: Quantity is zero or settlement prices are unchanged,
    Then: Exact zero is returned without unnecessary monetary arithmetic.
    """
    spec = LinearContractValuation(Decimal("1e9"))
    with localcontext() as context:
        context.Emax = 9

        assert quote_notional(spec, Decimal(0), Decimal("1e9")) == Decimal(0)
        assert settlement_pnl(spec, Decimal("1e9"), Decimal(100), Decimal(100)) == Decimal(0)
