"""Tests for position dust classification and display precision."""

from decimal import Decimal

import pytest

from snapper.application.portfolio.position_display import PositionDisplay
from snapper.application.portfolio.position_display import PositionDisplayInput
from snapper.application.portfolio.position_display import PositionSpecSteps
from snapper.application.portfolio.position_display import classify_position_display
from snapper.application.portfolio.position_display import display_for_quantity
from snapper.application.portfolio.position_display import display_steps_for_fact
from snapper.application.portfolio.position_display import optional_float
from snapper.application.portfolio.position_display import position_spec_steps
from snapper.core.contract_valuation import InverseContractValuation
from snapper.core.contract_valuation import LinearContractValuation

_THRESHOLD = Decimal("0.50")


def _result(inputs: PositionDisplayInput) -> PositionDisplay:
    """Classify one prepared input."""
    return classify_position_display(inputs)


def _input(quantity: float) -> PositionDisplayInput:
    """Build a no-spec input whose notional uses the default threshold."""
    return PositionDisplayInput(
        quantity=quantity,
        lot_size=None,
        tick_size=None,
        min_order_size=None,
        mark_price=None,
        average_price=None,
        quote_currency="USD",
        notional_threshold=_THRESHOLD,
    )


def test_xrp_float_residue_is_one_lot_and_dust() -> None:
    """A sub-lot float residue rounds to the venue lot and stays dust.

    Given: quantity 9.99999993923e-9, lot 1e-8, and minimum 1.65 XRP,
    When: the position is classified,
    Then: it is dust and the quantity shows 8 decimal places.
    """
    display = _result(
        PositionDisplayInput(
            quantity=9.99999993923e-9,
            lot_size=1e-8,
            tick_size=1e-5,
            min_order_size=1.65,
            mark_price=1.5,
            average_price=None,
            quote_currency="USD",
            notional_threshold=_THRESHOLD,
        )
    )
    assert display.is_dust is True
    assert display.quantity_decimals == 8
    assert display.price_decimals == 5


def test_short_xrp_residue_uses_absolute_quantity() -> None:
    """A short residue is classified on its absolute lot-rounded size.

    Given: a negative copy of the XRP dust residue,
    When: the position is classified,
    Then: it is dust at 8 quantity decimals.
    """
    display = _result(
        PositionDisplayInput(
            quantity=-9.99999993923e-9,
            lot_size=1e-8,
            tick_size=None,
            min_order_size=1.65,
            mark_price=None,
            average_price=None,
            quote_currency="USD",
            notional_threshold=_THRESHOLD,
        )
    )
    assert display.is_dust is True
    assert display.quantity_decimals == 8


def test_notional_equal_to_the_threshold_is_not_dust() -> None:
    """The notional rule is strict: the threshold itself is not dust.

    Given: a 1-unit position marked at exactly 0.50 with no venue minimum,
    When: the position is classified against the default threshold,
    Then: it is not dust and both precision fields are absent.
    """
    base = _input(1.0)
    display = _result(
        PositionDisplayInput(
            quantity=base.quantity,
            lot_size=None,
            tick_size=None,
            min_order_size=None,
            mark_price=0.50,
            average_price=None,
            quote_currency="USD",
            notional_threshold=_THRESHOLD,
        )
    )
    assert display.is_dust is False
    assert display.quantity_decimals is None
    assert display.price_decimals is None


def test_notional_below_the_threshold_is_dust() -> None:
    """A quote notional under the threshold is dust without a venue minimum.

    Given: a 1-unit position marked at 0.49 and no instrument spec,
    When: the position is classified,
    Then: the notional rule marks it dust.
    """
    display = _result(
        PositionDisplayInput(
            quantity=1.0,
            lot_size=None,
            tick_size=None,
            min_order_size=None,
            mark_price=0.49,
            average_price=None,
            quote_currency="USD",
            notional_threshold=_THRESHOLD,
        )
    )
    assert display.is_dust is True
    assert display.quantity_decimals is None
    assert display.price_decimals is None


def test_lot_rounding_onto_the_threshold_is_not_dust() -> None:
    """Float noise just under a lot step is judged after rounding.

    Given: quantity 0.499999999 with lot 0.01 and a unit price,
    When: the position is classified,
    Then: the rounded 0.50 notional is not below the threshold.
    """
    display = _result(
        PositionDisplayInput(
            quantity=0.499999999,
            lot_size=0.01,
            tick_size=None,
            min_order_size=None,
            mark_price=1.0,
            average_price=None,
            quote_currency="USD",
            notional_threshold=_THRESHOLD,
        )
    )
    assert display.is_dust is False
    assert display.quantity_decimals == 2


def test_lot_rounding_just_below_the_threshold_is_dust() -> None:
    """A quantity that stays under the threshold after lot rounding is dust.

    Given: quantity 0.494 with lot 0.01 and a unit price,
    When: the position is classified,
    Then: the rounded 0.49 notional is dust.
    """
    display = _result(
        PositionDisplayInput(
            quantity=0.494,
            lot_size=0.01,
            tick_size=None,
            min_order_size=None,
            mark_price=1.0,
            average_price=None,
            quote_currency="USD",
            notional_threshold=_THRESHOLD,
        )
    )
    assert display.is_dust is True


def test_quantity_equal_to_the_venue_minimum_is_not_dust() -> None:
    """The venue-minimum rule is strict.

    Given: a quantity equal to the minimum and a notional above the threshold,
    When: the position is classified,
    Then: it is not dust.
    """
    display = _result(
        PositionDisplayInput(
            quantity=1.65,
            lot_size=0.01,
            tick_size=None,
            min_order_size=1.65,
            mark_price=10.0,
            average_price=None,
            quote_currency="USD",
            notional_threshold=_THRESHOLD,
        )
    )
    assert display.is_dust is False


def test_float_noise_that_rounds_up_to_the_minimum_is_not_dust() -> None:
    """A residue just below the minimum is judged after lot rounding.

    Given: quantity 1.649999999, lot 0.01, and minimum 1.65,
    When: the position is classified with a large notional,
    Then: the rounded quantity is not below the minimum.
    """
    display = _result(
        PositionDisplayInput(
            quantity=1.649999999,
            lot_size=0.01,
            tick_size=None,
            min_order_size=1.65,
            mark_price=10.0,
            average_price=None,
            quote_currency="USD",
            notional_threshold=_THRESHOLD,
        )
    )
    assert display.is_dust is False


def test_quantity_below_the_minimum_is_dust_despite_a_large_notional() -> None:
    """Either dust rule is sufficient.

    Given: a lot-rounded quantity below the venue minimum and a large notional,
    When: the position is classified,
    Then: the minimum-size rule marks it dust.
    """
    display = _result(
        PositionDisplayInput(
            quantity=1.644,
            lot_size=0.01,
            tick_size=None,
            min_order_size=1.65,
            mark_price=10.0,
            average_price=None,
            quote_currency="USD",
            notional_threshold=_THRESHOLD,
        )
    )
    assert display.is_dust is True


def test_missing_steps_skip_both_dust_rules() -> None:
    """A caller with no steps does not invent a currency or a minimum.

    Given: no display steps and a small mark,
    When: the quantity is classified,
    Then: it is not dust and precision is absent.
    """
    display = display_for_quantity(1.0, None, _THRESHOLD, mark_price=0.10)
    assert display.is_dust is False
    assert display.quantity_decimals is None
    assert display.price_decimals is None


def test_missing_spec_uses_the_notional_rule_only() -> None:
    """Missing lot, tick, and minimum leave precision empty.

    Given: no instrument spec and a notional below the threshold,
    When: the position is classified,
    Then: precision is absent and the notional rule marks it dust.
    """
    display = display_for_quantity(
        1.0,
        PositionSpecSteps(None, None, None, quote_currency="USD"),
        _THRESHOLD,
        mark_price=0.10,
    )
    assert display.is_dust is True
    assert display.quantity_decimals is None
    assert display.price_decimals is None


def test_supplied_steps_drive_precision_and_the_minimum() -> None:
    """A caller can pass venue steps without building the full input.

    Given: lot 1e-8, tick 0.25, and a quantity below the minimum,
    When: display_for_quantity classifies it,
    Then: the quantity is dust at 8 decimals and the price at 2.
    """
    display = display_for_quantity(
        1e-8,
        PositionSpecSteps(lot_size=1e-8, tick_size=0.25, min_order_size=1.65),
        _THRESHOLD,
        mark_price=2.0,
    )
    assert display.is_dust is True
    assert display.quantity_decimals == 8
    assert display.price_decimals == 2


def test_missing_spec_and_large_notional_is_not_dust() -> None:
    """A missing spec does not invent a minimum order size.

    Given: no instrument spec and a notional above the threshold,
    When: the position is classified,
    Then: it is not dust and precision is absent.
    """
    display = _result(
        PositionDisplayInput(
            quantity=1.0,
            lot_size=None,
            tick_size=None,
            min_order_size=None,
            mark_price=2.0,
            average_price=None,
            quote_currency="USD",
            notional_threshold=_THRESHOLD,
        )
    )
    assert display.is_dust is False
    assert display.quantity_decimals is None
    assert display.price_decimals is None


def test_mark_price_is_preferred_over_average_price() -> None:
    """The quote notional uses the mark when both prices are usable.

    Given: a mark above the threshold and an entry below it,
    When: the position is classified,
    Then: the mark keeps the position out of dust.
    """
    display = _result(
        PositionDisplayInput(
            quantity=1.0,
            lot_size=None,
            tick_size=None,
            min_order_size=None,
            mark_price=2.0,
            average_price=0.10,
            quote_currency="USD",
            notional_threshold=_THRESHOLD,
        )
    )
    assert display.is_dust is False


def test_unusable_mark_falls_back_to_average_price() -> None:
    """A non-positive mark does not hide a dust entry notional.

    Given: a zero mark and an entry notional below the threshold,
    When: the position is classified,
    Then: the entry price marks it dust.
    """
    zero_mark = _result(
        PositionDisplayInput(
            quantity=1.0,
            lot_size=None,
            tick_size=None,
            min_order_size=None,
            mark_price=0.0,
            average_price=0.10,
            quote_currency="USD",
            notional_threshold=_THRESHOLD,
        )
    )
    negative_mark = _result(
        PositionDisplayInput(
            quantity=1.0,
            lot_size=None,
            tick_size=None,
            min_order_size=None,
            mark_price=-1.0,
            average_price=0.10,
            quote_currency="USD",
            notional_threshold=_THRESHOLD,
        )
    )
    assert zero_mark.is_dust is True
    assert negative_mark.is_dust is True


def test_missing_price_and_minimum_is_not_dust() -> None:
    """Dust requires a venue minimum or a quote price.

    Given: a finite quantity with no spec and no price,
    When: the position is classified,
    Then: it is not dust.
    """
    assert _result(_input(1.0)).is_dust is False


def test_missing_price_still_applies_the_venue_minimum() -> None:
    """The minimum-size rule does not need a quote price.

    Given: a quantity below the venue minimum and no price,
    When: the position is classified,
    Then: it is dust.
    """
    display = _result(
        PositionDisplayInput(
            quantity=1.0,
            lot_size=None,
            tick_size=None,
            min_order_size=2.0,
            mark_price=None,
            average_price=None,
            quote_currency="USD",
            notional_threshold=_THRESHOLD,
        )
    )
    assert display.is_dust is True


def test_non_finite_quantity_is_not_dust() -> None:
    """A non-finite quantity cannot satisfy either dust rule.

    Given: a NaN quantity and a usable lot size,
    When: the position is classified,
    Then: it is not dust and the lot still supplies quantity decimals.
    """
    nan_quantity = _result(
        PositionDisplayInput(
            quantity=float("nan"),
            lot_size=0.01,
            tick_size=None,
            min_order_size=None,
            mark_price=None,
            average_price=None,
            quote_currency="USD",
            notional_threshold=_THRESHOLD,
        )
    )
    infinite_quantity = _result(
        PositionDisplayInput(
            quantity=float("inf"),
            lot_size=None,
            tick_size=None,
            min_order_size=1.0,
            mark_price=None,
            average_price=None,
            quote_currency="USD",
            notional_threshold=_THRESHOLD,
        )
    )
    assert nan_quantity.is_dust is False
    assert nan_quantity.quantity_decimals == 2
    assert infinite_quantity.is_dust is False


@pytest.mark.parametrize(
    ("step", "places"),
    [
        (None, None),
        (float("nan"), None),
        (float("inf"), None),
        (0.0, None),
        (-0.01, None),
        (1.0, 0),
        (10.0, 0),
        (0.25, 2),
        (1e-8, 8),
        (1e-12, 12),
        (1e-13, None),
        (0.30000000000000004, 1),
    ],
)
def test_step_decimal_places(step: float | None, places: int | None) -> None:
    """Lot and tick scales come from the positive increment only.

    Given: a usable or unusable increment,
    When: it is supplied as the lot size,
    Then: quantity decimals match the increment's decimal places.
    """
    display = _result(
        PositionDisplayInput(
            quantity=1.0,
            lot_size=step,
            tick_size=None,
            min_order_size=None,
            mark_price=2.0,
            average_price=None,
            quote_currency="USD",
            notional_threshold=_THRESHOLD,
        )
    )
    assert display.quantity_decimals == places


def test_half_lot_rounds_away_from_zero() -> None:
    """An exact half lot rounds away from zero before the minimum comparison.

    Given: half a lot, a minimum equal to one lot, and no quote price,
    When: the position is classified,
    Then: the rounded quantity equals the minimum and is not dust.
    """
    display = _result(
        PositionDisplayInput(
            quantity=5e-9,
            lot_size=1e-8,
            tick_size=None,
            min_order_size=1e-8,
            mark_price=None,
            average_price=None,
            quote_currency="USD",
            notional_threshold=_THRESHOLD,
        )
    )
    assert display.is_dust is False


def test_zero_quantity_with_a_price_is_not_dust() -> None:
    """A flat position holds nothing, so it is not dust.

    Given: quantity zero, a positive mark, and a venue minimum,
    When: the position is classified,
    Then: neither the minimum nor the notional rule marks it dust.
    """
    display = _result(
        PositionDisplayInput(
            quantity=0.0,
            lot_size=None,
            tick_size=None,
            min_order_size=1.0,
            mark_price=10.0,
            average_price=None,
            quote_currency="USD",
            notional_threshold=_THRESHOLD,
        )
    )
    assert display.is_dust is False


def test_quantity_that_lot_rounds_to_zero_is_not_dust() -> None:
    """A residue that rounds to no lots is an empty position.

    Given: a quantity smaller than half a lot and a venue minimum,
    When: the position is classified,
    Then: the rounded zero quantity is not dust.
    """
    display = _result(
        PositionDisplayInput(
            quantity=1e-4,
            lot_size=0.01,
            tick_size=None,
            min_order_size=1.0,
            mark_price=10.0,
            average_price=None,
            quote_currency="USD",
            notional_threshold=_THRESHOLD,
        )
    )
    assert display.is_dust is False


def test_zero_threshold_disables_the_notional_rule() -> None:
    """A zero threshold never treats a positive notional as dust.

    Given: a small notional and a zero threshold,
    When: the position is classified without a venue minimum,
    Then: it is not dust.
    """
    display = _result(
        PositionDisplayInput(
            quantity=1.0,
            lot_size=None,
            tick_size=None,
            min_order_size=None,
            mark_price=0.10,
            average_price=None,
            quote_currency="USD",
            notional_threshold=Decimal("0"),
        )
    )
    assert display.is_dust is False


def test_optional_float_rejects_non_numbers() -> None:
    """Booleans and other objects are not display numbers.

    Given: a boolean, a string, None, and real numbers,
    When: each value is parsed,
    Then: only the real numbers are returned.
    """
    assert optional_float(True) is None
    assert optional_float("1.0") is None
    assert optional_float(None) is None
    assert optional_float(1) == 1.0
    assert optional_float(1.5) == 1.5


def test_position_spec_steps_read_numeric_fields_only() -> None:
    """Spec mappings contribute only numeric lot, tick, and minimum fields.

    Given: a non-mapping and a mapping with a boolean lot size,
    When: display steps are read,
    Then: the non-mapping is empty and the boolean field is ignored.
    """
    assert position_spec_steps("missing") == PositionSpecSteps(None, None, None)
    assert position_spec_steps({"lot_size": True, "tick_size": 1, "min_order_size": None}) == (
        PositionSpecSteps(None, 1.0, None)
    )


def _classify(quantity: float, fact: dict[str, object], mark_price: float) -> PositionDisplay:
    """Classify one quantity from a fact row at one mark."""
    return display_for_quantity(
        quantity,
        display_steps_for_fact(fact),
        _THRESHOLD,
        mark_price=mark_price,
    )


def test_missing_quote_currency_skips_the_notional_rule() -> None:
    """A notional without a quote currency is not treated as USD.

    Given: a small price and no quote currency or venue minimum,
    When: the position is classified,
    Then: the notional rule does not mark it dust.
    """
    display = _result(
        PositionDisplayInput(
            quantity=1.0,
            lot_size=None,
            tick_size=None,
            min_order_size=None,
            mark_price=0.10,
            average_price=None,
            notional_threshold=_THRESHOLD,
        )
    )
    assert display.is_dust is False


def test_missing_fx_rate_skips_a_non_usd_notional() -> None:
    """A quote that cannot be converted to USD keeps only the minimum rule.

    Given: a 0.10 EUR notional and no USD rate,
    When: the position is classified,
    Then: it is not dust.
    """
    display = _result(
        PositionDisplayInput(
            quantity=1.0,
            lot_size=None,
            tick_size=None,
            min_order_size=None,
            mark_price=0.10,
            average_price=None,
            quote_currency="EUR",
            usd_per_quote=None,
            notional_threshold=_THRESHOLD,
        )
    )
    assert display.is_dust is False


def test_eur_notional_uses_the_usd_rate() -> None:
    """The threshold compares the quote notional after the USD conversion.

    Given: one unit marked at 0.40 EUR and a rate of 1.10 USD per EUR,
    When: the position is classified,
    Then: the 0.44 USD notional is dust, and 0.50 EUR at the same rate is not.
    """
    dust = _result(
        PositionDisplayInput(
            quantity=1.0,
            lot_size=None,
            tick_size=None,
            min_order_size=None,
            mark_price=0.40,
            average_price=None,
            quote_currency="EUR",
            usd_per_quote=Decimal("1.10"),
            notional_threshold=_THRESHOLD,
        )
    )
    held = _result(
        PositionDisplayInput(
            quantity=1.0,
            lot_size=None,
            tick_size=None,
            min_order_size=None,
            mark_price=0.50,
            average_price=None,
            quote_currency="EUR",
            usd_per_quote=Decimal("1.10"),
            notional_threshold=_THRESHOLD,
        )
    )
    assert dust.is_dust is True
    assert held.is_dust is False


def test_non_positive_fx_rate_skips_the_notional_rule() -> None:
    """A zero, negative, or non-finite rate is not a conversion.

    Given: a small EUR notional and an unusable rate,
    When: the position is classified,
    Then: it is not dust.
    """
    for rate in (Decimal(0), Decimal("-1"), Decimal("NaN")):
        display = _result(
            PositionDisplayInput(
                quantity=1.0,
                lot_size=None,
                tick_size=None,
                min_order_size=None,
                mark_price=0.10,
                average_price=None,
                quote_currency="EUR",
                usd_per_quote=rate,
                notional_threshold=_THRESHOLD,
            )
        )
        assert display.is_dust is False


def test_linear_futures_notional_uses_the_contract_multiplier() -> None:
    """A Kraken linear future prices contracts, not the raw quantity.

    Given: one contract with multiplier 10 marked at 0.10 USD,
    When: the position is classified,
    Then: the 1.00 USD contract notional is not dust.
    """
    fact = _futures_fact(Decimal("10"), "linear", "BTC", "USD")
    display = _classify(1.0, fact, 0.10)
    assert isinstance(display_steps_for_fact(fact).contract, LinearContractValuation)
    assert display.is_dust is False


def test_inverse_futures_notional_uses_quote_units_per_contract() -> None:
    """An inverse contract notional does not multiply by the quote price.

    Given: one inverse contract worth 0.40 USD and a large mark,
    When: the position is classified,
    Then: the 0.40 USD notional is dust.
    """
    fact = _futures_fact(Decimal("0.40"), "inverse", "USD", "BTC")
    display = _classify(1.0, fact, 50_000.0)
    assert isinstance(display_steps_for_fact(fact).contract, InverseContractValuation)
    assert display.is_dust is True


def test_unknown_derivative_contract_skips_the_notional_rule() -> None:
    """A futures row without a contract value is not guessed from quantity.

    Given: a Kraken futures instrument with no multiplier and a 0.10 mark,
    When: the position is classified,
    Then: the notional rule does not mark it dust.
    """
    fact = _futures_fact(None, None, None, None)
    steps = display_steps_for_fact(fact)
    assert steps.derivative_notional_unknown is True
    assert steps.contract is None
    assert _classify(1.0, fact, 0.10).is_dust is False


def test_flexible_unit_contract_matches_base_asset_notional() -> None:
    """A unit flexible future is one base unit per contract.

    Given: a linear flexible contract whose multiplier is 1,
    When: one unit is marked at 0.10 USD,
    Then: the notional is dust, the same as a spot quantity.
    """
    fact = _futures_fact(
        Decimal(1), "linear", "BTC", "USD", native_order_quantity_unit="base_asset"
    )
    assert _classify(1.0, fact, 0.10).is_dust is True


def test_unsupported_flexible_multiplier_skips_the_notional_rule() -> None:
    """A flexible multiplier other than 1 has no validated unit.

    Given: a base-asset contract whose multiplier is 2 and a 0.10 mark,
    When: the position is classified,
    Then: the spot notional is not used and the position is not dust.
    """
    fact = _futures_fact(
        Decimal(2), "linear", "BTC", "USD", native_order_quantity_unit="base_asset"
    )
    assert display_steps_for_fact(fact).derivative_notional_unknown is True
    assert _classify(1.0, fact, 0.10).is_dust is False


def test_non_futures_contract_evidence_skips_the_notional_rule() -> None:
    """Contract evidence on another venue is not given a futures model.

    Given: a Kraken spot row marked as a perpetual with no payoff,
    When: the position is classified at 0.10 USD,
    Then: the notional rule is skipped.
    """
    fact = {
        "exchange": "kraken",
        "base": "BTC",
        "quote": "USD",
        "instrument_kind": "perpetual",
        "quantity_unit": "contract_count",
    }
    assert display_steps_for_fact(fact).derivative_notional_unknown is True
    assert _classify(1.0, fact, 0.10).is_dust is False


def test_unusable_contract_columns_do_not_invent_a_model() -> None:
    """Non-numeric contract columns stay absent on a spot instrument.

    Given: a spot fact with a boolean, text, and non-finite multiplier,
    When: display steps are built,
    Then: the row stays spot and a non-mapping fact is empty.
    """
    for contract_size in (True, "1", float("nan"), Decimal("NaN"), 1, 1.5):
        steps = display_steps_for_fact(
            {
                "exchange": "kraken",
                "base": "XRP",
                "quote": "USD",
                "instrument_kind": "spot",
                "contract_size": contract_size,
            }
        )
        assert steps.contract is None
        assert steps.derivative_notional_unknown is False
        assert steps.quote_currency == "USD"
    assert display_steps_for_fact("missing") == PositionSpecSteps(None, None, None)


def test_mismatched_payoff_currencies_skip_the_notional_rule() -> None:
    """Payoff currencies must match the instrument base and quote.

    Given: a linear future whose value currency is the quote,
    When: the position is classified at 0.10 USD,
    Then: the contract value is unknown and the position is not dust.
    """
    fact = _futures_fact(Decimal(1), "linear", "USD", "BTC")
    assert display_steps_for_fact(fact).derivative_notional_unknown is True
    assert _classify(1.0, fact, 0.10).is_dust is False


def test_spot_fact_uses_quantity_times_price() -> None:
    """A spot instrument with no contract evidence uses the quote notional.

    Given: one Kraken spot unit marked at 0.10 USD,
    When: the position is classified,
    Then: the 0.10 USD notional is dust.
    """
    fact = {"exchange": "kraken", "base": "XRP", "quote": "USD", "instrument_kind": "spot"}
    steps = display_steps_for_fact(fact)
    assert steps.contract is None
    assert steps.derivative_notional_unknown is False
    assert _classify(1.0, fact, 0.10).is_dust is True


def test_blank_quote_skips_the_notional_rule() -> None:
    """An empty quote is not treated as USD.

    Given: a small mark and a blank quote currency,
    When: the position is classified,
    Then: it is not dust.
    """
    display = _result(
        PositionDisplayInput(
            quantity=1.0,
            lot_size=None,
            tick_size=None,
            min_order_size=None,
            mark_price=0.10,
            average_price=None,
            quote_currency="",
            notional_threshold=_THRESHOLD,
        )
    )
    assert display.is_dust is False
    assert display_steps_for_fact({"quote": "", "exchange": 1}).quote_currency is None


@pytest.mark.parametrize(
    "fact",
    [
        {"exchange": "kraken", "quote": "USD", "quantity_unit": "contract_count"},
        {"exchange": "kraken", "quote": "USD", "native_order_quantity_unit": "contract_count"},
        {"exchange": "kraken", "quote": "USD", "instrument_kind": "option"},
        {"exchange": "kraken", "quote": "USD", "contract_payoff_model": "linear"},
    ],
)
def test_derivative_signals_skip_the_notional_rule(fact: dict[str, object]) -> None:
    """Any contract signal outside a known Kraken futures model skips notional.

    Given: a small USD mark and one derivative signal,
    When: the position is classified,
    Then: the notional rule does not mark it dust.
    """
    assert display_steps_for_fact(fact).derivative_notional_unknown is True
    assert _classify(1.0, fact, 0.10).is_dust is False


def test_futures_contract_value_must_be_explicit() -> None:
    """Incomplete Kraken futures evidence does not fall back to spot math.

    Given: futures rows whose kind, size, unit, or payoff is unusable,
    When: each row is classified at 0.10 USD,
    Then: every row skips the notional rule.
    """
    rows = [
        {**_futures_fact(Decimal(1), "linear", "BTC", "USD"), "instrument_kind": "spot"},
        _futures_fact(Decimal(0), "linear", "BTC", "USD"),
        _futures_fact(Decimal("NaN"), "linear", "BTC", "USD"),
        {**_futures_fact(Decimal(1), "linear", "BTC", "USD"), "native_order_quantity_unit": None},
        {**_futures_fact(Decimal(1), "linear", "BTC", "USD"), "native_order_quantity_unit": "lot"},
        _futures_fact(Decimal(1), "inverse", "USD", "BTC", native_order_quantity_unit="base_asset"),
        _futures_fact(Decimal(1), None, None, None),
        _futures_fact(Decimal(1), "inverse", "BTC", "USD"),
    ]
    for fact in rows:
        assert display_steps_for_fact(fact).derivative_notional_unknown is True
        assert _classify(1.0, fact, 0.10).is_dust is False


def test_integer_contract_size_builds_a_linear_model() -> None:
    """A whole-number multiplier is accepted as a contract size.

    Given: a linear future whose stored multiplier is the integer 10,
    When: one contract is marked at 0.10 USD,
    Then: the 1.00 USD contract notional is not dust.
    """
    fact = _futures_fact(Decimal(1), "linear", "BTC", "USD")
    fact["contract_size"] = 10
    assert isinstance(display_steps_for_fact(fact).contract, LinearContractValuation)
    assert _classify(1.0, fact, 0.10).is_dust is False


def _futures_fact(
    contract_size: Decimal | None,
    payoff_model: str | None,
    value_currency: str | None,
    settlement_currency: str | None,
    native_order_quantity_unit: str = "contract_count",
) -> dict[str, object]:
    """Build one Kraken futures fact row."""
    return {
        "exchange": "kraken_futures",
        "base": "BTC",
        "quote": "USD",
        "instrument_kind": "perpetual",
        "quantity_unit": "contract_count",
        "native_order_quantity_unit": native_order_quantity_unit,
        "contract_size": contract_size,
        "contract_payoff_model": payoff_model,
        "contract_value_currency": value_currency,
        "settlement_currency": settlement_currency,
    }
