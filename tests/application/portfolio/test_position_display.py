"""Tests for position dust classification and display precision."""

from decimal import Decimal

import pytest

from snapper.application.portfolio.position_display import PositionDisplay
from snapper.application.portfolio.position_display import PositionDisplayInput
from snapper.application.portfolio.position_display import PositionSpecSteps
from snapper.application.portfolio.position_display import classify_position_display
from snapper.application.portfolio.position_display import display_for_quantity
from snapper.application.portfolio.position_display import optional_float
from snapper.application.portfolio.position_display import position_spec_steps

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
            notional_threshold=_THRESHOLD,
        )
    )
    assert display.is_dust is True


def test_missing_spec_uses_the_notional_rule_only() -> None:
    """Missing lot, tick, and minimum leave precision empty.

    Given: no instrument spec and a notional below the threshold,
    When: the position is classified,
    Then: precision is absent and the notional rule marks it dust.
    """
    display = display_for_quantity(1.0, None, _THRESHOLD, mark_price=0.10)
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
            notional_threshold=_THRESHOLD,
        )
    )
    assert display.is_dust is False


def test_zero_quantity_with_a_price_is_dust() -> None:
    """A flat quantity has zero quote notional.

    Given: quantity zero and a positive mark,
    When: the position is classified,
    Then: the notional rule marks it dust.
    """
    display = _result(
        PositionDisplayInput(
            quantity=0.0,
            lot_size=None,
            tick_size=None,
            min_order_size=None,
            mark_price=10.0,
            average_price=None,
            notional_threshold=_THRESHOLD,
        )
    )
    assert display.is_dust is True


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
