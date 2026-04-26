"""Tests for Plan D §3.6 caps_violation_after_ai_approve handler.

Covers :meth:`AiReviewService.handle_caps_violation_bus_message` —
re-fanouts internal caps-violation events to the external
``ai_reviews.{user}.{strategy}.caps_violation`` WS topic so the bridge
can surface the rejection on the delegate's UI.
"""

from collections.abc import Iterator
from datetime import UTC
from datetime import datetime
from typing import cast
from unittest.mock import AsyncMock
from unittest.mock import MagicMock
from uuid import uuid7

import pytest

from snapper.application.ai_review.service import AiReviewService
from snapper.messaging.infrastructure.publisher import MessagePublisher
from snapper.messaging.schemas.data import CapsViolationAfterAiApproveData


@pytest.fixture(autouse=True)
def _clear_singleton() -> Iterator[None]:
    """Reset the AiReviewService singleton between cases."""
    AiReviewService.clear_instance()
    yield
    AiReviewService.clear_instance()


def _make_event(
    *,
    review_public_id: str = "rev-1",
    user_public_id: str = "user-1",
    strategy_public_id: str = "strat-1",
    cap_type: str = "max_daily_notional_usd",
) -> CapsViolationAfterAiApproveData:
    """Build a minimal caps-violation event payload."""
    return CapsViolationAfterAiApproveData(
        public_id=str(uuid7()),
        timestamp=datetime.now(UTC),
        session_id=str(uuid7()),
        sequence_id=1,
        review_public_id=review_public_id,
        user_public_id=user_public_id,
        strategy_public_id=strategy_public_id,
        wallet_public_id="wal-A",
        instrument_public_id="inst-A",
        cap_type=cap_type,
        attempted=15000.0,
        limit=10000.0,
    )


@pytest.mark.asyncio
async def test_publishes_caps_violation_to_external_ws_topic() -> None:
    """Happy path — handler builds the WS topic + forwards via publisher.

    Plan D §3.6 + Plan A Q7 — the external topic suffix is
    ``ai_reviews.{user}.{strategy}.caps_violation`` so the bridge per-frame
    scope filter routes the frame to the right delegate.

    Given an AiReviewService with a wired publisher,
    When handle_caps_violation_bus_message receives a caps event,
    Then publisher.send is called once with the right topic + the
    same envelope passed through verbatim, and the handler returns
    True.
    """
    svc = AiReviewService.get_instance()
    publisher = MagicMock()
    publisher.send = AsyncMock()
    svc.set_msg_publisher(cast(MessagePublisher, publisher))
    msg = _make_event()
    result = await svc.handle_caps_violation_bus_message(msg)
    assert result is True
    publisher.send.assert_awaited_once()
    args, _ = publisher.send.await_args
    topic, payload = args
    assert topic == "ai_reviews.user-1.strat-1.caps_violation"
    assert payload is msg


@pytest.mark.asyncio
async def test_returns_false_when_publisher_missing() -> None:
    """No publisher injected -> log warning + return False (no raise).

    Mirrors ScopeGrantService best-effort semantics: a singleton spun
    up before the FastAPI lifespan attached one must still tolerate
    the call without crashing the subscriber loop.

    Given a service without a publisher,
    When handle_caps_violation_bus_message is invoked,
    Then it returns False without raising.
    """
    svc = AiReviewService.get_instance()
    msg = _make_event()
    result = await svc.handle_caps_violation_bus_message(msg)
    assert result is False


@pytest.mark.asyncio
async def test_swallows_publisher_failure_and_returns_false() -> None:
    """Publisher.send raising -> handler logs + returns False (no raise).

    A broken broker connection must not crash the bus subscriber
    loop — the column-level audit trail (``ai_review_events``)
    remains the source of truth.

    Given a publisher whose send() raises RuntimeError,
    When handle_caps_violation_bus_message is invoked,
    Then it returns False and does not propagate the exception.
    """
    svc = AiReviewService.get_instance()
    publisher = MagicMock()
    publisher.send = AsyncMock(side_effect=RuntimeError("broker down"))
    svc.set_msg_publisher(cast(MessagePublisher, publisher))
    msg = _make_event()
    result = await svc.handle_caps_violation_bus_message(msg)
    assert result is False


@pytest.mark.asyncio
async def test_topic_includes_user_and_strategy_ids_verbatim() -> None:
    """Topic suffix uses user_public_id + strategy_public_id verbatim.

    Per Plan D §4.3 the WS topic family is
    ``ai_reviews.{user_public_id}.{strategy_public_id}.{request|decision_ack|caps_violation}``;
    the per-frame scope filter (Plan D §9) parses the JSON envelope
    for wallet/instrument so the topic suffix only needs the user +
    strategy ids that drive routing.

    Given a UUID-shaped user_public_id + strategy_public_id,
    When handle_caps_violation_bus_message routes,
    Then the topic carries them as-is (no encoding / hashing).
    """
    svc = AiReviewService.get_instance()
    publisher = MagicMock()
    publisher.send = AsyncMock()
    svc.set_msg_publisher(cast(MessagePublisher, publisher))
    user_pid = str(uuid7())
    strategy_pid = str(uuid7())
    msg = _make_event(user_public_id=user_pid, strategy_public_id=strategy_pid)
    await svc.handle_caps_violation_bus_message(msg)
    args, _ = publisher.send.await_args
    topic = args[0]
    assert topic == f"ai_reviews.{user_pid}.{strategy_pid}.caps_violation"


@pytest.mark.asyncio
async def test_payload_carries_cap_type_attempted_and_limit() -> None:
    """Forwarded envelope preserves the cap_type / attempted / limit triple.

    The bridge -> delegate UI consumer reads these three fields to
    render a structured rejection (e.g. "Daily notional cap exceeded:
    attempted $15,000 vs $10,000 limit").

    Given an event with cap_type='max_open_orders' + attempted=11 +
    limit=10,
    When the handler routes,
    Then the forwarded payload carries the exact same triple.
    """
    svc = AiReviewService.get_instance()
    publisher = MagicMock()
    publisher.send = AsyncMock()
    svc.set_msg_publisher(cast(MessagePublisher, publisher))
    msg = CapsViolationAfterAiApproveData(
        public_id=str(uuid7()),
        timestamp=datetime.now(UTC),
        session_id=str(uuid7()),
        sequence_id=1,
        review_public_id="rev-x",
        user_public_id="user-x",
        strategy_public_id="strat-x",
        wallet_public_id="wal-x",
        instrument_public_id="inst-x",
        cap_type="max_open_orders",
        attempted=11.0,
        limit=10.0,
    )
    await svc.handle_caps_violation_bus_message(msg)
    forwarded = publisher.send.await_args.args[1]
    assert forwarded.cap_type == "max_open_orders"
    assert forwarded.attempted == 11.0
    assert forwarded.limit == 10.0
