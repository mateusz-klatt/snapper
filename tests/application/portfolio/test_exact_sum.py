"""Tests for the exact double accumulator used by the P&L cumulative flows."""

import math

import pytest

from snapper.application.portfolio.exact_sum import _MIN_SUBNORMAL
from snapper.application.portfolio.exact_sum import _SCALE_DENOMINATOR
from snapper.application.portfolio.exact_sum import _SCALE_EXPONENT
from snapper.application.portfolio.exact_sum import ExactSum
from snapper.application.portfolio.exact_sum import exact_units_of
from snapper.application.portfolio.exact_sum import faithful_roundings
from snapper.application.portfolio.exact_sum import project_units

_MAX_DOUBLE = 1.7976931348623157e308


def _summed(values: list[float]) -> ExactSum:
    """Accumulate ``values`` into a fresh accumulator."""
    accumulator = ExactSum()
    for value in values:
        accumulator.add(value)
    return accumulator


class TestScale:
    """The unit the accumulator counts in."""

    def test_the_literal_scale_matches_its_exponent(self) -> None:
        """A wrong literal would halve or double every accumulated value."""
        assert math.ldexp(1.0, -_SCALE_EXPONENT) == _MIN_SUBNORMAL
        assert _SCALE_DENOMINATOR == 1 << _SCALE_EXPONENT

    def test_the_scale_divides_every_finite_double_exactly(self) -> None:
        """Exactness rests on this: no finite double has a remainder."""
        for value in (
            0.0,
            -0.0,
            1.0,
            -1.0,
            0.1,
            _MIN_SUBNORMAL,
            3.0 * _MIN_SUBNORMAL,
            _MAX_DOUBLE,
            -_MAX_DOUBLE,
            -0.011365700000000001,
        ):
            assert exact_units_of(value) / _SCALE_DENOMINATOR == value


class TestExactAccumulation:
    """A sum of finite doubles is exact, and rounded exactly once."""

    def test_the_outage_stream_matches_a_correctly_rounded_sum(self) -> None:
        """The six production fee terms that went dark on 2026-08-04.

        Pinned against ``math.fsum`` rather than a literal: the claim is that
        this accumulator is correctly rounded, and fsum is the reference for
        that, whereas a literal would only restate whatever it produced.
        """
        terms = [
            -0.025856285000000003,
            -0.02585765,
            -0.011365700000000001,
            -0.0261742859,
            -0.025857194999999996,
            -0.0512862924,
        ]
        assert _summed(terms).to_float() == math.fsum(terms)

    def test_it_beats_a_naive_loop_where_the_loop_drifts(self) -> None:
        """The defect being fixed: a running ``+=`` loses what this keeps."""
        terms = [1.0, 2.0**-54, 2.0**-54, 2.0**-54]
        naive = 0.0
        for term in terms:
            naive += term
        assert naive == 1.0
        assert _summed(terms).to_float() == math.fsum(terms) != naive

    def test_an_empty_accumulator_is_a_finite_zero(self) -> None:
        """The identity a per-instrument bucket starts from."""
        accumulator = ExactSum()
        assert accumulator.is_finite()
        assert accumulator.exact_units() == 0
        assert accumulator.to_float() == 0.0

    def test_exact_units_survives_a_projection_that_would_not(self) -> None:
        """The exact total stays available past the double range.

        Conservation is checked in units, never in floats, so an intermediate
        beyond the double range must not destroy the comparison the caller
        actually makes.
        """
        accumulator = _summed([_MAX_DOUBLE, _MAX_DOUBLE, -_MAX_DOUBLE])
        assert accumulator.is_finite()
        assert accumulator.exact_units() == exact_units_of(_MAX_DOUBLE)
        assert accumulator.to_float() == _MAX_DOUBLE


class TestNonFiniteLatching:
    """Where ``math.fsum`` raises, this must answer.

    ``math.fsum([1.7e308, 1.7e308])`` raises ``OverflowError`` on FINITE input,
    and ``math.fsum([inf, -inf])`` raises ``ValueError``. Both would turn a
    withheld P&L minute into a 500 on the engine's hottest path.
    """

    @pytest.mark.parametrize(
        ("values", "expected"),
        (
            ([math.inf], math.inf),
            ([-math.inf], -math.inf),
            ([1.0, math.inf, 2.0], math.inf),
            ([_MAX_DOUBLE, _MAX_DOUBLE], math.inf),
            ([-_MAX_DOUBLE, -_MAX_DOUBLE], -math.inf),
        ),
    )
    def test_an_infinity_is_reported_not_raised(self, values: list[float], expected: float) -> None:
        """A single infinity, or an overflow of finite terms, answers plainly."""
        accumulator = _summed(values)
        assert not accumulator.is_finite() or accumulator.to_float() == expected
        assert accumulator.to_float() == expected

    @pytest.mark.parametrize(
        "values",
        (
            [math.nan],
            [1.0, math.nan],
            [math.nan, math.inf],
            [math.inf, -math.inf],
            [-math.inf, math.inf, 1.0],
        ),
    )
    def test_nan_and_opposing_infinities_answer_nan(self, values: list[float]) -> None:
        """Exactly what the naive loop produces, and never an exception."""
        assert math.isnan(_summed(values).to_float())

    def test_a_latched_accumulator_reports_itself_unusable(self) -> None:
        """``is_finite`` is the caller's signal that the units are meaningless."""
        assert not _summed([math.nan]).is_finite()
        assert not _summed([math.inf]).is_finite()
        assert _summed([1.0, -1.0]).is_finite()

    def test_a_cancelled_overflow_recovers_the_true_zero(self) -> None:
        """Deliberate behaviour change, recorded rather than discovered.

        A naive loop overflows to ``inf`` on the second term and never comes
        back, so today this withholds. The exact sum is ``0.0`` and is
        representable, so it now publishes the true value — the same correction
        as the pinned cancellation case, not a new leniency.
        """
        values = [_MAX_DOUBLE, _MAX_DOUBLE, -_MAX_DOUBLE, -_MAX_DOUBLE]
        naive = 0.0
        for value in values:
            naive += value
        assert math.isinf(naive)
        assert _summed(values).to_float() == 0.0


class TestUnitResidue:
    """The exact remainder of a split rides in units, never as a double."""

    def test_a_split_with_a_unit_residue_conserves_exactly(self) -> None:
        """An allocation's final bucket recovers precisely what rounding lost."""
        whole = 0.1
        part = 0.1 / 3.0
        bucket_a = ExactSum()
        bucket_a.add(part)
        bucket_b = ExactSum()
        bucket_b.add(part)
        bucket_c = ExactSum()
        bucket_c.add_units(exact_units_of(whole) - 2 * exact_units_of(part))
        total = bucket_a.exact_units() + bucket_b.exact_units() + bucket_c.exact_units()
        assert total == exact_units_of(whole)

    def test_a_negative_residue_subtracts(self) -> None:
        """Residues carry sign, because rounding can overshoot either way."""
        accumulator = ExactSum()
        accumulator.add(1.0)
        accumulator.add_units(-exact_units_of(1.0))
        assert accumulator.exact_units() == 0
        assert accumulator.is_finite()


class TestProjection:
    """One correctly rounded divide, and honest infinities past the range."""

    def test_projection_is_correctly_rounded(self) -> None:
        """Round-tripping a double through units is the identity."""
        for value in (0.1, -0.011365700000000001, _MAX_DOUBLE, -_MIN_SUBNORMAL):
            assert project_units(exact_units_of(value)) == value

    def test_projection_overflows_to_the_signed_infinity(self) -> None:
        """Past the double range the naive loop answered ``inf``; so does this."""
        beyond = 2 * exact_units_of(_MAX_DOUBLE)
        assert project_units(beyond) == math.inf
        assert project_units(-beyond) == -math.inf


class TestFaithfulRoundings:
    """Only the two doubles bracketing the exact sum may be published."""

    def test_a_representable_total_has_one_candidate(self) -> None:
        """When the exact sum IS a double there is nothing to bracket."""
        assert faithful_roundings(exact_units_of(1.5)) == (1.5,)
        assert faithful_roundings(0) == (0.0,)

    def test_an_unrepresentable_total_has_two_ordered_candidates(self) -> None:
        """Nearest first, then the neighbour on the exact value's other side."""
        units = exact_units_of(1.0) + 1
        candidates = faithful_roundings(units)
        assert candidates == (1.0, math.nextafter(1.0, math.inf))

    def test_the_second_candidate_tracks_the_residue_sign(self) -> None:
        """A residue below the nearest double brackets downward, not upward."""
        units = exact_units_of(1.0) - 1
        candidates = faithful_roundings(units)
        assert candidates == (1.0, math.nextafter(1.0, -math.inf))

    def test_an_infinite_neighbour_is_never_offered(self) -> None:
        """A count just past the largest double has one candidate, not two.

        Offering the infinite neighbour let a finite exact total publish as
        ``inf`` — the adversarial review's counterexample. An infinity is an
        overflow marker, never a faithful rounding of a finite value.
        """
        assert faithful_roundings(exact_units_of(_MAX_DOUBLE) + 1) == (_MAX_DOUBLE,)
        assert faithful_roundings(-exact_units_of(_MAX_DOUBLE) - 1) == (-_MAX_DOUBLE,)
