"""Prove that only the exact temporal infinity token denotes an unclosed version."""

from datetime import UTC
from datetime import datetime
from datetime import timedelta
from datetime import timezone

import pytest

from snapper.data.models import KNOWN_TO_MAX
from snapper.data.models import is_open_ended_version


def test_exact_known_to_max_is_open_ended() -> None:
    """Accept the database sentinel at its actual whole-second precision.

    Given: The exact stored knowledge-end sentinel.
    When: The loaded version is classified.
    Then: The version is proven open-ended.
    """
    assert datetime(9999, 12, 31, 23, 59, 59, tzinfo=UTC) == KNOWN_TO_MAX
    assert is_open_ended_version(KNOWN_TO_MAX) is True


@pytest.mark.parametrize(
    "known_to",
    (
        datetime(2026, 10, 23, 12, tzinfo=UTC),
        datetime(2026, 10, 25, 12, tzinfo=UTC),
        datetime(9999, 12, 31, 23, 59, 58, 999999, tzinfo=UTC),
        datetime(9999, 12, 31, 23, 59, 59, 1, tzinfo=UTC),
        datetime.max.replace(tzinfo=UTC),
    ),
    ids=("past", "future", "just-before-token", "just-after-token", "datetime-max"),
)
def test_finite_knowledge_end_is_not_open_ended(known_to: datetime) -> None:
    """Reject finite closures even when they are beside or beyond the sentinel.

    Given: A finite knowledge end in the past, future or beside the sentinel.
    When: The scalar open-ended proof classifies that end.
    Then: The finite boundary does not authorize an unclosed version.

    Args:
        known_to: Finite knowledge end that must not count as the infinity token.
    """
    assert is_open_ended_version(known_to) is False


def test_future_closed_version_refuses_a_witness_inside_its_valid_range() -> None:
    """Distinguish historical range membership from proof of an unclosed version.

    Given: A witness after the version timestamp but before now and tomorrow's closure.
    When: The version passes the witness and current-time range checks.
    Then: Its finite future closure still refuses open-ended authority.
    """
    now = datetime(2026, 10, 24, 12, tzinfo=UTC)
    timestamp = now - timedelta(days=2)
    witness = now - timedelta(days=1)
    known_to = now + timedelta(days=1)

    assert timestamp < witness < now < known_to
    assert timestamp <= witness < known_to
    assert known_to > now
    assert is_open_ended_version(known_to) is False


def test_equivalent_sentinel_in_another_timezone_is_open_ended() -> None:
    """Recognize the sentinel's exact instant independently of its UTC offset.

    Given: The infinity instant expressed with a negative fixed timezone offset.
    When: The loaded aware datetime is classified.
    Then: Equivalent temporal evidence remains open-ended.
    """
    known_to = datetime(9999, 12, 31, 22, 59, 59, tzinfo=timezone(timedelta(hours=-1)))

    assert known_to == KNOWN_TO_MAX
    assert is_open_ended_version(known_to) is True


def test_naive_sentinel_does_not_prove_an_open_ended_version() -> None:
    """Refuse a timezone-free lookalike of the database's aware sentinel.

    Given: A naive datetime with the sentinel's calendar fields.
    When: The loaded value is checked for open-ended authority.
    Then: Missing timezone evidence does not count as the exact infinity token.
    """
    known_to = KNOWN_TO_MAX.replace(tzinfo=None)

    assert is_open_ended_version(known_to) is False
