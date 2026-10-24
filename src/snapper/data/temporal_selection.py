"""Preserve SQL result cardinality after filtering exact temporal versions."""

from collections.abc import Sequence

from sqlalchemy.exc import MultipleResultsFound


def one_or_none[T](rows: Sequence[T]) -> T | None:
    """Return the sole filtered candidate without accepting ambiguous current rows.

    Args:
        rows: Candidates whose temporal eligibility has already been checked.

    Returns:
        The original sole candidate, or None when no candidate survives.

    Raises:
        MultipleResultsFound: If more than one eligible candidate remains.
    """
    if len(rows) > 1:
        raise MultipleResultsFound("Multiple rows were found when one or none was required")
    return rows[0] if rows else None
