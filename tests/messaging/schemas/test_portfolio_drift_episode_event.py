"""Tests for the internal portfolio-drift lifecycle bus schema."""

from datetime import UTC
from datetime import datetime
from datetime import timedelta
from typing import Literal

import pytest
from pydantic import ValidationError

from snapper.messaging.schemas.data import PortfolioDriftEpisodeEventData
from snapper.messaging.schemas.messages import parse_message

_OPENED_AT = datetime(2026, 7, 15, 9, 0, tzinfo=UTC)


def _event(
    lifecycle: Literal["opened", "resolved"],
    *,
    closed_at: datetime | None = None,
    resolution_reason: Literal["matched"] | None = None,
    mismatch_count: int = 3,
) -> PortfolioDriftEpisodeEventData:
    """Build one drift lifecycle frame with stable provenance."""
    return PortfolioDriftEpisodeEventData(
        sequence_id=1,
        public_id="event-1",
        timestamp=_OPENED_AT,
        session_id="session-1",
        wallet_public_id="wallet-1",
        exchange="kraken_futures",
        episode_public_id="episode-1",
        lifecycle=lifecycle,
        opened_at=_OPENED_AT,
        closed_at=closed_at,
        mismatch_count=mismatch_count,
        resolution_reason=resolution_reason,
    )


def test_opened_frame_round_trips_through_message_parser() -> None:
    """A committed third mismatch parses as the typed opened event.

    Given: A valid opened drift-episode frame.
    When: The generic message parser decodes its JSON.
    Then: The parser returns the live typed lifecycle event.
    """
    event = _event("opened")

    parsed = parse_message(event.to_json())

    assert isinstance(parsed, PortfolioDriftEpisodeEventData)
    assert parsed.lifecycle == "opened"
    assert parsed.mode == "live"


@pytest.mark.parametrize(
    ("closed_at", "resolution_reason"),
    [
        (_OPENED_AT + timedelta(minutes=1), None),
        (None, "matched"),
    ],
)
def test_opened_frame_rejects_resolution_evidence(
    closed_at: datetime | None,
    resolution_reason: Literal["matched"] | None,
) -> None:
    """An opening frame cannot claim any close evidence.

    Given: An opened lifecycle frame carrying part of a resolution.
    When: The schema validates its lifecycle fields.
    Then: Validation rejects the contradictory close evidence.
    """
    with pytest.raises(ValidationError, match="cannot carry resolution fields"):
        _event(
            "opened",
            closed_at=closed_at,
            resolution_reason=resolution_reason,
        )


@pytest.mark.parametrize(
    ("closed_at", "resolution_reason"),
    [
        (None, "matched"),
        (_OPENED_AT + timedelta(minutes=1), None),
    ],
)
def test_resolved_frame_requires_complete_matched_close_evidence(
    closed_at: datetime | None,
    resolution_reason: Literal["matched"] | None,
) -> None:
    """A resolution frame requires both its close time and matched reason.

    Given: A resolved frame missing one required close field.
    When: The schema validates its lifecycle fields.
    Then: Validation rejects the incomplete resolution evidence.
    """
    with pytest.raises(ValidationError, match="requires matched close evidence"):
        _event(
            "resolved",
            closed_at=closed_at,
            resolution_reason=resolution_reason,
        )


def test_resolved_frame_rejects_close_before_open() -> None:
    """A lifecycle cannot resolve before its durable opening instant.

    Given: A resolved frame whose close precedes its opening.
    When: The schema validates the temporal order.
    Then: Validation rejects the impossible lifecycle.
    """
    with pytest.raises(ValidationError, match="cannot precede"):
        _event(
            "resolved",
            closed_at=_OPENED_AT - timedelta(microseconds=1),
            resolution_reason="matched",
        )


def test_resolved_frame_accepts_same_episode_identity_and_count() -> None:
    """A matched close retains the stable episode identity and mismatch count.

    Given: A complete matched resolution for an existing episode.
    When: The typed lifecycle frame is created.
    Then: It preserves the episode identity, close time, and mismatch count.
    """
    closed_at = _OPENED_AT + timedelta(minutes=8)

    event = _event(
        "resolved",
        closed_at=closed_at,
        resolution_reason="matched",
        mismatch_count=5,
    )

    assert event.episode_public_id == "episode-1"
    assert event.closed_at == closed_at
    assert event.mismatch_count == 5


def test_mismatch_count_below_open_threshold_is_rejected() -> None:
    """No event can represent a drift episode below the third mismatch.

    Given: An opened frame with only two full mismatches.
    When: The schema validates the threshold.
    Then: Validation rejects the premature drift episode.
    """
    with pytest.raises(ValidationError):
        _event("opened", mismatch_count=2)
