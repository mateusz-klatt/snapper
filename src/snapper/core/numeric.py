"""Shared predicates for validating numeric evidence."""

import math
from typing import TypeIs


def is_positive_finite(value: float | None) -> TypeIs[float]:
    """Return whether a numeric value is present, finite, and positive.

    Args:
        value: Candidate numeric evidence.

    Returns:
        Whether ``value`` is a positive finite float.
    """
    return value is not None and math.isfinite(value) and value > 0.0
