"""Carried FX conversion keeps exact behavior and refuses contradictory evidence."""

import math
from datetime import UTC
from datetime import datetime
from datetime import timedelta

import pytest

from snapper.application.portfolio.fx_rates import FX_CARRY_BOUND
from snapper.application.portfolio.fx_rates import FxRateKey
from snapper.application.portfolio.fx_rates import FxRatePlane
from snapper.application.portfolio.fx_rates import FxVenueMap
from snapper.application.portfolio.fx_rates import build_fx_carry_book
from snapper.application.portfolio.fx_rates import convert_amount
from snapper.application.portfolio.fx_rates import exact_fx_book

_MINUTE = datetime(2026, 11, 9, 12, 0, tzinfo=UTC)
_PLANE: FxRatePlane = ("EUR", "USD", "kraken")
_VENUES: FxVenueMap = {("EUR", "USD"): _PLANE}


def _at(minutes: int) -> datetime:
    """Return a minute relative to the valued minute."""
    return _MINUTE + timedelta(minutes=minutes)


def _key(minutes: int) -> FxRateKey:
    """Return the rate key of the fixed plane at a relative minute."""
    return ("EUR", "USD", "kraken", _at(minutes))


@pytest.mark.parametrize(
    ("from_currency", "to_currency"),
    [("EUR", "USD"), ("USD", "EUR")],
)
def test_exact_minute_matches_the_exact_only_conversion_bit_for_bit(
    from_currency: str,
    to_currency: str,
) -> None:
    """An exact rate is unchanged in value and needs no carry disclosure.

    Given: A close published at the valued minute.
    When: Either orientation converts through the carry book.
    Then: The amount equals the exact-only result and the age is zero.
    """
    rates = {_key(0): 1.0873}
    expected = convert_amount(123.456, from_currency, to_currency, _MINUTE, rates, _VENUES)
    converted = build_fx_carry_book(rates).convert(
        123.456, from_currency, to_currency, _MINUTE, _VENUES
    )
    assert converted is not None
    assert converted.amount == expected
    assert converted.rate is not None
    assert converted.rate.age_minutes == 0
    assert converted.rate.source_minute == _MINUTE


def test_missing_minute_uses_the_latest_earlier_close_and_discloses_it() -> None:
    """Only the newest earlier close serves a minute that was never published.

    Given: Two earlier closes and nothing at the valued minute.
    When: The amount is converted.
    Then: The newer close is used and its minute and age are reported.
    """
    book = build_fx_carry_book({_key(-90): 1.5, _key(-7): 2.0})
    converted = book.convert(10.0, "EUR", "USD", _MINUTE, _VENUES)
    assert converted is not None
    assert converted.amount == 20.0
    assert converted.rate is not None
    assert converted.rate.plane == _PLANE
    assert converted.rate.orientation == "direct"
    assert converted.rate.source_minute == _at(-7)
    assert converted.rate.age_minutes == 7


def test_carry_reaches_exactly_the_bound_and_no_further() -> None:
    """The bound is inclusive and one minute beyond it is withheld.

    Given: A single close exactly at the bound, then one minute older.
    When: Each book resolves the valued minute.
    Then: The first serves it and the second refuses.
    """
    bound_minutes = int(FX_CARRY_BOUND.total_seconds() // 60)
    inside = build_fx_carry_book({_key(-bound_minutes): 1.25})
    outside = build_fx_carry_book({_key(-bound_minutes - 1): 1.25})
    resolved = inside.resolve("EUR", "USD", _MINUTE, _VENUES)
    assert resolved is not None
    assert resolved.age_minutes == bound_minutes
    assert outside.resolve("EUR", "USD", _MINUTE, _VENUES) is None


@pytest.mark.parametrize("invalid", [math.nan, 0.0, -1.0, math.inf])
def test_contradictory_exact_evidence_is_withheld_despite_an_earlier_close(
    invalid: float,
) -> None:
    """Observed but unusable evidence is not the same as a missing minute.

    Given: A conflicted or invalid close at the valued minute and a valid earlier one.
    When: The amount is converted.
    Then: The conversion refuses instead of reaching back.
    """
    book = build_fx_carry_book({_key(0): invalid, _key(-1): 1.2})
    assert book.convert(10.0, "EUR", "USD", _MINUTE, _VENUES) is None


@pytest.mark.parametrize("invalid", [math.nan, 0.0, -1.0, math.inf])
def test_search_steps_over_unusable_earlier_closes(invalid: float) -> None:
    """A conflicted or invalid earlier minute can never become the carried source.

    Given: An unusable close immediately before the gap and a valid older one.
    When: The missing minute is resolved.
    Then: The older valid close serves it.
    """
    book = build_fx_carry_book({_key(-1): invalid, _key(-3): 1.4})
    resolved = book.resolve("EUR", "USD", _MINUTE, _VENUES)
    assert resolved is not None
    assert resolved.close == 1.4
    assert resolved.source_minute == _at(-3)


def test_unpinned_or_mismatched_pairs_are_refused() -> None:
    """Carry never chooses a plane the election did not pin.

    Given: Evidence with no pin, no venue map, and a pin of other currencies.
    When: Each lookup runs.
    Then: All refuse.
    """
    book = build_fx_carry_book({_key(-1): 1.2})
    assert book.resolve("EUR", "USD", _MINUTE, {}) is None
    assert book.resolve("EUR", "USD", _MINUTE, None) is None
    wrong = {("EUR", "USD"): ("GBP", "CHF", "kraken")}
    assert book.resolve("EUR", "USD", _MINUTE, wrong) is None


def test_zero_identity_and_overflow_match_the_exact_rules() -> None:
    """Conversions needing no evidence report none, and overflow is refused.

    Given: A zero amount, an identity conversion and an overflowing product.
    When: Each is converted with no usable or an extreme rate.
    Then: Zero and identity succeed without a rate and overflow returns nothing.
    """
    empty = build_fx_carry_book({})
    zero = empty.convert(0.0, "EUR", "USD", _MINUTE, _VENUES)
    identity = empty.convert(5.0, "USD", "USD", _MINUTE, _VENUES)
    assert zero is not None
    assert (zero.amount, zero.rate) == (0.0, None)
    assert identity is not None
    assert (identity.amount, identity.rate) == (5.0, None)
    assert empty.convert(5.0, "EUR", "USD", _MINUTE, _VENUES) is None
    extreme = build_fx_carry_book({_key(0): 1e308})
    assert extreme.convert(1e308, "EUR", "USD", _MINUTE, _VENUES) is None


def test_exact_only_book_cannot_carry() -> None:
    """Exact-only callers share the conversion path without gaining carry.

    Given: An exact-only book holding only an earlier close.
    When: The missing minute and the published minute are resolved.
    Then: The published minute works and the missing one refuses.
    """
    book = exact_fx_book({_key(-1): 1.2})
    assert book.resolve("EUR", "USD", _MINUTE, _VENUES) is None
    assert book.resolve("EUR", "USD", _at(-1), _VENUES) is not None
