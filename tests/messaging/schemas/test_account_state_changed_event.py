"""Tests for the account-state invalidation event schema."""

from datetime import UTC
from datetime import datetime

import pytest
from pydantic import ValidationError

from snapper.messaging.schemas.data import AccountStateChangedEventData
from snapper.messaging.schemas.messages import parse_message


def _event(kind: str = "snapshot") -> AccountStateChangedEventData:
    """Build one account-state invalidation event with stable provenance."""
    return AccountStateChangedEventData.model_validate(
        {
            "type": "account_state_changed_event",
            "sequence_id": 1,
            "public_id": "event-1",
            "timestamp": datetime(2026, 7, 16, 9, 0, tzinfo=UTC),
            "session_id": "session-1",
            "wallet_public_id": "00000000-0000-7000-8000-000000000001",
            "exchange": "kraken",
            "mode": "live",
            "kind": kind,
        }
    )


@pytest.mark.parametrize("kind", ["snapshot", "reconciliation"])
def test_event_round_trips_through_message_parser(kind: str) -> None:
    """Both committed change kinds parse through the generic registry.

    Given: A valid snapshot or reconciliation invalidation event.
    When: The generic message parser decodes its JSON.
    Then: The typed event and its invalidation scope survive the round trip.
    """
    event = _event(kind)

    parsed = parse_message(event.to_json())

    assert isinstance(parsed, AccountStateChangedEventData)
    assert parsed.wallet_public_id == event.wallet_public_id
    assert parsed.exchange == "kraken"
    assert parsed.mode == "live"
    assert parsed.kind == kind


def test_event_rejects_unknown_change_kind() -> None:
    """The invalidation schema rejects changes without a known commit kind.

    Given: An account-state event with an unsupported change kind.
    When: The schema validates the payload.
    Then: Validation fails before the frame can reach the bridge.
    """
    with pytest.raises(ValidationError):
        _event("classification")
