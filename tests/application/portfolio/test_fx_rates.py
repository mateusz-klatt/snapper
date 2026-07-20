"""Tests for point-in-time FX resolution from finalized 1m candles.

Covers the identity, direct, and inverse conversion paths, the pair enumeration
the loader drives off, and — most importantly — every refusal: a missing pair, a
missing minute, and a degenerate close that would otherwise fabricate an infinite
or nonsensical rate. A wrong rate silently misstates money, so each refusal must
return ``None`` rather than a number.
"""

from datetime import UTC
from datetime import datetime
from datetime import timedelta

import pytest

from snapper.application.portfolio.fx_rates import FxRateMap
from snapper.application.portfolio.fx_rates import convert_amount
from snapper.application.portfolio.fx_rates import required_pairs

_M = datetime(2026, 7, 19, 19, 22, tzinfo=UTC)


def _rates(**pairs: float) -> FxRateMap:
    """Build a rate map at :data:`_M` from ``BASEQUOTE=close`` keyword pairs."""
    return {(name[:3], name[3:], _M): close for name, close in pairs.items()}


class TestIdentityAndZero:
    """Cover the conversions that need no evidence at all."""

    def test_same_currency_passes_through(self) -> None:
        """A USD amount on a USD series converts without consulting any rate."""
        assert convert_amount(12.5, "USD", "USD", _M, {}) == 12.5

    def test_zero_is_currency_invariant(self) -> None:
        """Exact zero converts to zero even with an unknown denomination.

        Production writes a fee-free fill as ``fee=0.0`` with an EMPTY fee asset;
        demanding a rate for it would withhold an entire series over nothing.
        """
        assert convert_amount(0.0, "", "USD", _M, {}) == 0.0
        assert convert_amount(0.0, "EUR", "USD", _M, {}) == 0.0


class TestDirectAndInverse:
    """Cover the two evidence-backed conversion directions."""

    def test_direct_pair_multiplies(self) -> None:
        """A EUR fee converts to USD through the quoted EUR-USD close."""
        assert convert_amount(0.04, "EUR", "USD", _M, _rates(EURUSD=1.25)) == pytest.approx(0.05)

    def test_inverse_pair_divides(self) -> None:
        """With only USD-PLN listed, a PLN amount converts by the reciprocal."""
        result = convert_amount(8.0, "PLN", "USD", _M, _rates(USDPLN=4.0))
        assert result == pytest.approx(2.0)

    def test_direct_wins_over_inverse(self) -> None:
        """A listed direct pair is preferred over inverting the opposite one."""
        rates = {("EUR", "USD", _M): 1.25, ("USD", "EUR", _M): 99.0}
        assert convert_amount(1.0, "EUR", "USD", _M, rates) == pytest.approx(1.25)

    def test_negative_amount_keeps_its_sign(self) -> None:
        """A rebate (negative fee) stays negative through conversion."""
        assert convert_amount(-2.0, "EUR", "USD", _M, _rates(EURUSD=1.5)) == pytest.approx(-3.0)


class TestRefusals:
    """Cover every path that must withhold rather than invent a rate."""

    def test_missing_pair_returns_none(self) -> None:
        """An unlistable pair yields no rate instead of a guess."""
        assert convert_amount(1.0, "JPY", "USD", _M, _rates(EURUSD=1.25)) is None

    def test_missing_minute_returns_none(self) -> None:
        """A rate from another minute is never carried forward to this one."""
        rates = {("EUR", "USD", _M - timedelta(minutes=1)): 1.25}
        assert convert_amount(1.0, "EUR", "USD", _M, rates) is None

    def test_zero_inverse_close_is_refused(self) -> None:
        """A zero close is refused rather than inverted into an infinite rate."""
        assert convert_amount(1.0, "PLN", "USD", _M, _rates(USDPLN=0.0)) is None

    def test_negative_inverse_close_is_refused(self) -> None:
        """A negative close cannot be a price and is refused."""
        assert convert_amount(1.0, "PLN", "USD", _M, _rates(USDPLN=-4.0)) is None

    def test_non_finite_direct_close_falls_through_to_none(self) -> None:
        """A NaN direct close is ignored, and with no inverse the result is None."""
        assert convert_amount(1.0, "EUR", "USD", _M, _rates(EURUSD=float("nan"))) is None

    def test_non_finite_direct_close_falls_back_to_inverse(self) -> None:
        """A NaN direct close still allows a usable inverse pair to answer."""
        rates = {("EUR", "USD", _M): float("inf"), ("USD", "EUR", _M): 0.8}
        assert convert_amount(1.0, "EUR", "USD", _M, rates) == pytest.approx(1.25)

    def test_overflowing_direct_conversion_is_refused(self) -> None:
        """A finite rate whose product overflows withholds instead of returning inf."""
        assert convert_amount(1e308, "EUR", "USD", _M, _rates(EURUSD=1e308)) is None

    def test_overflowing_inverse_conversion_is_refused(self) -> None:
        """A tiny inverse close that overflows the quotient is refused."""
        assert convert_amount(1e308, "PLN", "USD", _M, _rates(USDPLN=1e-308)) is None


class TestRequiredPairs:
    """Cover the pair enumeration the candle loader is driven by."""

    def test_both_orientations_are_requested(self) -> None:
        """Each foreign currency is requested directly and inverted."""
        assert required_pairs(frozenset({"EUR"}), "USD") == frozenset(
            {("EUR", "USD"), ("USD", "EUR")}
        )

    def test_valuation_currency_needs_no_pair(self) -> None:
        """The identity currency is excluded — it needs no evidence."""
        assert required_pairs(frozenset({"USD"}), "USD") == frozenset()

    def test_empty_denomination_is_skipped(self) -> None:
        """An empty fee asset never asks for a pair; only a zero amount uses it."""
        assert required_pairs(frozenset({""}), "USD") == frozenset()

    def test_multiple_currencies_accumulate(self) -> None:
        """Every distinct foreign currency contributes its own pair orientations."""
        assert required_pairs(frozenset({"EUR", "PLN", "USD", ""}), "USD") == frozenset(
            {("EUR", "USD"), ("USD", "EUR"), ("PLN", "USD"), ("USD", "PLN")}
        )
