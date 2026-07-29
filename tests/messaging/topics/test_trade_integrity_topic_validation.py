"""Tests for trade-integrity heartbeat topic validation."""

import pytest

from snapper.messaging.topics.validation import validate_topic


@pytest.mark.parametrize("monitor", ["m1", "m2"])
def test_trade_integrity_topic_accepts_only_implemented_monitors(monitor: str) -> None:
    """Accept heartbeat topics for each implemented integrity monitor.

    Given a trade-integrity heartbeat scoped to M1 or M2,
    When topic validation checks the parametrized monitor topic,
    Then validation succeeds with an empty error message.
    """
    valid, error = validate_topic(f"system.heartbeats.trade_integrity.{monitor}")

    assert valid
    assert error == ""


@pytest.mark.parametrize(
    "topic",
    [
        "system.heartbeats.trade_integrity",
        "system.heartbeats.trade_integrity.m3",
        "system.heartbeats.trade_integrity.m1.extra",
    ],
)
def test_trade_integrity_topic_rejects_unknown_or_malformed_scope(topic: str) -> None:
    """Reject trade-integrity topics with unsupported monitor scopes.

    Given a topic omits its monitor, names M3, or carries an extra segment,
    When topic validation checks that malformed scope,
    Then validation fails with an error identifying trade_integrity.
    """
    valid, error = validate_topic(topic)

    assert not valid
    assert "trade_integrity" in error
