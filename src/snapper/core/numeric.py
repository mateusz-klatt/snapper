"""Shared predicates for validating numeric evidence."""

import math
from typing import TypeIs


def is_exact_zero(value: float) -> bool:
    """Return whether a floating-point value is exact zero (``+0.0`` or ``-0.0``).

    Args:
        value: Floating-point value to check.

    Returns:
        ``True`` for ``+0.0`` and ``-0.0``, and ``False`` otherwise.
    """
    return math.isclose(value, 0.0, rel_tol=0.0, abs_tol=0.0)


def is_positive_finite(value: float | None) -> TypeIs[float]:
    """Return whether a numeric value is present, finite, and positive.

    Args:
        value: Candidate numeric evidence.

    Returns:
        Whether ``value`` is a positive finite float.
    """
    return value is not None and math.isfinite(value) and value > 0.0
