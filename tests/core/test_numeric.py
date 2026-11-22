"""Unit tests for shared numeric validation predicates."""

import math

import pytest

from snapper.core.numeric import is_exact_zero
from snapper.core.numeric import is_positive_finite


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        (0.0, True),
        (-0.0, True),
        (5e-324, False),
        (-5e-324, False),
        (1.0, False),
        (math.nan, False),
        (math.inf, False),
        (-math.inf, False),
    ],
)
def test_is_exact_zero_matches_only_signed_zeros(value: float, expected: bool) -> None:
    """Return True only for +0.0 and -0.0, rejecting subnormals and non-zeros.

    Given: Positive zero, negative zero, smallest IEEE-754 subnormals, one, and
        non-finite floating-point values.
    When: ``is_exact_zero`` evaluates each input.
    Then: Only ``+0.0`` and ``-0.0`` return ``True``.

    Args:
        value: Floating-point input under test.
        expected: Expected exact-zero verdict.
    """
    assert is_exact_zero(value) is expected


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        (None, False),
        (0.0, False),
        (-1.0, False),
        (math.nan, False),
        (math.inf, False),
        (5e-324, True),
        (1.0, True),
    ],
)
def test_is_positive_finite_filters_absent_nonpositive_and_nonfinite(
    value: float | None,
    expected: bool,
) -> None:
    """Return True only for positive finite floats.

    Given: Absent, zero, negative, non-finite, subnormal positive, and unit values.
    When: ``is_positive_finite`` evaluates each candidate.
    Then: Only positive finite numbers return ``True``.

    Args:
        value: Candidate numeric value.
        expected: Expected positive-finite verdict.
    """
    assert is_positive_finite(value) is expected
