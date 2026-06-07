"""Tests for caps_violation_after_ai_approve handler.

Covers :meth:`AiReviewService.handle_caps_violation_bus_message` —
translates the internal ``caps_violation_after_ai_approve`` bus payload
to the external ``ai_review.caps_violation`` WS frame and re-fanouts
to the ``ai_reviews.{user}.{strategy}.caps_violation`` topic so the
bridge can surface the rejection on the delegate's UI.
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
from snapper.core.partitioning import ShardOwnership
from snapper.messaging.infrastructure.publisher import MessagePublisher
from snapper.messaging.infrastructure.publisher import SequenceTracker
from snapper.messaging.schemas.data import AiReviewCapsViolationFrameData
from snapper.messaging.schemas.data import CapsViolationAfterAiApproveData


def _make_publisher() -> MagicMock:
    """Build a MagicMock publisher with a real :class:`SequenceTracker`.

    The handler stamps fresh ``sequence_id`` + ``session_id`` from the
    publisher's tracker so the gap detector keys
    the external stream by the external topic, not by the internal
    bus topic. Tests need a real tracker so ``next_sequence`` actually
    increments + so ``session_id`` returns a deterministic UUID7.
    """
    publisher = MagicMock()
    publisher.send = AsyncMock()
    publisher.tracker = SequenceTracker()
    return publisher


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
    dispatch_version: int = 1,
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
        dispatch_version=dispatch_version,
    )


@pytest.mark.asyncio
async def test_publishes_caps_violation_to_external_ws_topic() -> None:
    """Happy path — handler builds the WS topic + forwards via publisher.

    The external topic suffix is
    ``ai_reviews.{user}.{strategy}.caps_violation`` so the bridge per-frame
    scope filter routes the frame to the right delegate. The OUTBOUND
    frame discriminator is fixed at ``ai_review.caps_violation`` (a
    different value from the internal ``caps_violation_after_ai_approve``
    bus type), so the handler must translate to
    :class:`AiReviewCapsViolationFrameData` before publishing.

    Given an AiReviewService with a wired publisher,
    When handle_caps_violation_bus_message receives a caps event,
    Then publisher.send is called once with the right topic + an
    :class:`AiReviewCapsViolationFrameData` envelope (NOT the raw bus
    message), and the handler returns True.
    """
    svc = AiReviewService.get_instance()
    publisher = _make_publisher()
    svc.set_msg_publisher(cast(MessagePublisher, publisher))
    msg = _make_event()
    result = await svc.handle_caps_violation_bus_message(msg)
    assert result is True
    publisher.send.assert_awaited_once()
    args, _ = publisher.send.await_args
    topic, payload = args
    assert topic == "ai_reviews.user-1.strat-1.caps_violation"
    assert isinstance(payload, AiReviewCapsViolationFrameData)
    assert payload is not msg
    assert payload.type == "ai_review.caps_violation"


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
    publisher = _make_publisher()
    publisher.send = AsyncMock(side_effect=RuntimeError("broker down"))
    svc.set_msg_publisher(cast(MessagePublisher, publisher))
    msg = _make_event()
    result = await svc.handle_caps_violation_bus_message(msg)
    assert result is False


@pytest.mark.asyncio
async def test_topic_includes_user_and_strategy_ids_verbatim() -> None:
    """Topic suffix uses user_public_id + strategy_public_id verbatim.

    The WS topic family is
    ``ai_reviews.{user_public_id}.{strategy_public_id}.{request|decision_ack|caps_violation}``;
    the per-frame scope filter parses the JSON envelope for
    wallet/instrument so the topic suffix only needs the user +
    strategy ids that drive routing.

    Given a UUID-shaped user_public_id + strategy_public_id,
    When handle_caps_violation_bus_message routes,
    Then the topic carries them as-is (no encoding / hashing).
    """
    svc = AiReviewService.get_instance()
    publisher = _make_publisher()
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
    publisher = _make_publisher()
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
        dispatch_version=1,
    )
    await svc.handle_caps_violation_bus_message(msg)
    forwarded = publisher.send.await_args.args[1]
    assert isinstance(forwarded, AiReviewCapsViolationFrameData)
    assert forwarded.cap_type == "max_open_orders"
    assert forwarded.attempted == pytest.approx(11.0)
    assert forwarded.limit == pytest.approx(10.0)


@pytest.mark.asyncio
async def test_external_frame_carries_dispatch_version_for_q18_dedup() -> None:
    """dispatch_version forwarded to external frame.

    The bridge dedupes external frames by ``(public_id, dispatch_version)``
    so a re-fanout of the same event after the original review row's
    state has incremented the version is surfaced as a fresh frame.
    The handler MUST carry the inbound bus event's ``dispatch_version``
    onto the external frame verbatim.

    Given a bus event with dispatch_version=7,
    When the handler routes,
    Then the outbound frame carries dispatch_version=7.
    """
    svc = AiReviewService.get_instance()
    publisher = _make_publisher()
    svc.set_msg_publisher(cast(MessagePublisher, publisher))
    msg = _make_event(dispatch_version=7)
    await svc.handle_caps_violation_bus_message(msg)
    forwarded = publisher.send.await_args.args[1]
    assert forwarded.dispatch_version == 7


@pytest.mark.asyncio
async def test_external_frame_type_matches_q16_contract() -> None:
    """External frame ``type`` is fixed.

    The internal bus payload carries
    ``type="caps_violation_after_ai_approve"`` (snake-case bus name),
    but the OUTBOUND WS frame the JS bridge dispatcher reads MUST be
    ``"ai_review.caps_violation"``. The bridge dispatcher uses
    ``switch (frame.type)``. Pin the discriminator translation here
    so a future renaming of the internal schema cannot silently break
    the JS dispatcher contract.

    Given a bus message with the internal type literal,
    When handle_caps_violation_bus_message routes,
    Then the outbound frame.type equals ``"ai_review.caps_violation"``.
    """
    svc = AiReviewService.get_instance()
    publisher = _make_publisher()
    svc.set_msg_publisher(cast(MessagePublisher, publisher))
    msg = _make_event()
    assert msg.type == "caps_violation_after_ai_approve"
    await svc.handle_caps_violation_bus_message(msg)
    forwarded = publisher.send.await_args.args[1]
    assert forwarded.type == "ai_review.caps_violation"


@pytest.mark.asyncio
async def test_external_frame_preserves_routing_fields() -> None:
    """Routing fields stay at envelope top level.

    The bridge per-frame scope filter (``_enforce_ai_review_scope``)
    reads ``wallet_public_id`` + ``instrument_public_id`` directly off
    the parsed JSON envelope without descending into a nested payload.
    Routing fields + the substantive ``review_public_id`` MUST be
    forwarded verbatim from the bus message so the bridge can route
    + the JS dispatcher can correlate the rejection back to the
    originating ``ai_reviews`` row.

    Given a bus message with full routing,
    When the handler translates,
    Then every routing field appears on the outbound frame at
    top-level with the same value.
    """
    svc = AiReviewService.get_instance()
    publisher = _make_publisher()
    svc.set_msg_publisher(cast(MessagePublisher, publisher))
    msg = _make_event()
    await svc.handle_caps_violation_bus_message(msg)
    forwarded = publisher.send.await_args.args[1]
    assert forwarded.review_public_id == msg.review_public_id
    assert forwarded.user_public_id == msg.user_public_id
    assert forwarded.strategy_public_id == msg.strategy_public_id
    assert forwarded.wallet_public_id == msg.wallet_public_id
    assert forwarded.instrument_public_id == msg.instrument_public_id


def _find_owner_and_non_owner_instances(
    review_public_id: str, instance_count: int
) -> tuple[ShardOwnership, ShardOwnership]:
    """Return (owner, non-owner) :class:`ShardOwnership` for a review id.

    Ownership tests need both sides of the partition for one specific
    review id so the gating gate can be exercised symmetrically
    without relying on hash-luck. Walks every instance id in
    ``[0, instance_count)`` and picks one matching pair.
    """
    owner: ShardOwnership | None = None
    non_owner: ShardOwnership | None = None
    for candidate in range(instance_count):
        ownership = ShardOwnership(instance_id=candidate, instance_count=instance_count)
        if ownership.owns(review_public_id):
            if owner is None:
                owner = ownership
        elif non_owner is None:
            non_owner = ownership
        if owner is not None and non_owner is not None:
            return owner, non_owner
    raise AssertionError(
        f"could not find owner+non-owner pair for review_public_id={review_public_id!r} "
        f"at instance_count={instance_count}"
    )


@pytest.mark.asyncio
async def test_publishes_when_no_shard_ownership_injected_legacy_compat() -> None:
    """Pre-lifespan single-instance behaviour before sharding is wired.

    The lifespan injects :class:`ShardOwnership` AFTER the publisher seam
    but BEFORE the bus listener subscribes. Under partial-startup races,
    test fixtures, and in-process tools that exercise the handler before
    ``set_shard_ownership`` is wired, the gate must default-open so we
    keep the legacy single-instance behaviour. ``None`` MUST publish.

    Given a service with a publisher but no ShardOwnership injected,
    When handle_caps_violation_bus_message is invoked,
    Then the publish path runs and returns True.
    """
    svc = AiReviewService.get_instance()
    publisher = _make_publisher()
    svc.set_msg_publisher(cast(MessagePublisher, publisher))
    assert svc.shard_ownership is None
    msg = _make_event()
    result = await svc.handle_caps_violation_bus_message(msg)
    assert result is True
    publisher.send.assert_awaited_once()


@pytest.mark.asyncio
async def test_publishes_when_instance_count_is_one_unconditional_owner() -> None:
    """``instance_count == 1`` makes :meth:`ShardOwnership.owns` trivially True.

    Default deployment (no SNAPPER_COORDINATOR_INSTANCE_COUNT override) is
    instance_count=1 — the gate must be a deterministic no-op so behaviour
    is byte-identical to the pre-sharding single-instance path.

    Given a service with ShardOwnership(0, 1),
    When the handler is invoked,
    Then the external WS publish runs and returns True.
    """
    svc = AiReviewService.get_instance()
    publisher = _make_publisher()
    svc.set_msg_publisher(cast(MessagePublisher, publisher))
    svc.set_shard_ownership(ShardOwnership(instance_id=0, instance_count=1))
    msg = _make_event()
    result = await svc.handle_caps_violation_bus_message(msg)
    assert result is True
    publisher.send.assert_awaited_once()


@pytest.mark.asyncio
async def test_publishes_when_owner_under_multi_instance() -> None:
    """Multi-instance deployment — the owning worker re-publishes.

    Given a review_public_id whose hash partitions to instance 0 of N,
    When the handler runs on instance 0,
    Then the external WS frame is published and the call returns True.
    """
    svc = AiReviewService.get_instance()
    publisher = _make_publisher()
    svc.set_msg_publisher(cast(MessagePublisher, publisher))
    review_pid = str(uuid7())
    owner, _non_owner = _find_owner_and_non_owner_instances(review_pid, instance_count=4)
    svc.set_shard_ownership(owner)
    msg = _make_event(review_public_id=review_pid)
    result = await svc.handle_caps_violation_bus_message(msg)
    assert result is True
    publisher.send.assert_awaited_once()


@pytest.mark.asyncio
async def test_skips_when_non_owner_under_multi_instance() -> None:
    """Multi-instance deployment — the non-owning worker short-circuits.

    Dedup invariant: exactly one worker per review
    row publishes the external frame. Non-owners MUST return False without
    invoking the publisher, otherwise N workers would N-duplicate the WS
    frame to every connected subscriber.

    Given a review_public_id whose hash partitions to instance 0 of 4,
    When the handler runs on a non-owner instance,
    Then publisher.send is never invoked and the handler returns False.
    """
    svc = AiReviewService.get_instance()
    publisher = _make_publisher()
    svc.set_msg_publisher(cast(MessagePublisher, publisher))
    review_pid = str(uuid7())
    _owner, non_owner = _find_owner_and_non_owner_instances(review_pid, instance_count=4)
    svc.set_shard_ownership(non_owner)
    msg = _make_event(review_public_id=review_pid)
    result = await svc.handle_caps_violation_bus_message(msg)
    assert result is False
    publisher.send.assert_not_awaited()


@pytest.mark.asyncio
async def test_clearing_shard_ownership_restores_legacy_publish_behaviour() -> None:
    """``set_shard_ownership(None)`` reopens the publish path.

    Tests + shutdown paths clear the seam to drop references. Once cleared,
    the handler must fall back to the legacy "publish unconditionally"
    behaviour so test fixtures that exercise the handler post-shutdown do
    not silently swallow events.

    Given a service that previously had a non-owner ShardOwnership,
    When set_shard_ownership(None) is called and the handler runs,
    Then publish proceeds and returns True regardless of the prior state.
    """
    svc = AiReviewService.get_instance()
    publisher = _make_publisher()
    svc.set_msg_publisher(cast(MessagePublisher, publisher))
    review_pid = str(uuid7())
    _owner, non_owner = _find_owner_and_non_owner_instances(review_pid, instance_count=4)
    svc.set_shard_ownership(non_owner)
    svc.set_shard_ownership(None)
    msg = _make_event(review_public_id=review_pid)
    result = await svc.handle_caps_violation_bus_message(msg)
    assert result is True
    publisher.send.assert_awaited_once()


@pytest.mark.asyncio
async def test_ownership_split_partitions_distinct_review_ids_across_workers() -> None:
    """End-to-end dedup invariant for the multi-worker partitioning.

    Across ``instance_count=4`` workers, every review_public_id is
    published by exactly one worker.

    Drives N independent service singletons (clear-instance per simulated
    worker), each configured with a distinct ShardOwnership. For each of
    32 distinct review_public_ids we count how many workers publish the
    external frame. The invariant is **exactly one** per review id —
    matches the deterministic SHA-256 mod partitioning and proves the
    gate's correctness at the cluster level.

    Given 4 simulated workers + 32 distinct review ids,
    When each worker handles each review id,
    Then exactly one worker publishes per review id (32 publishes total).
    """
    instance_count = 4
    review_ids = [str(uuid7()) for _ in range(32)]
    publish_counts = dict.fromkeys(review_ids, 0)
    for instance_id in range(instance_count):
        AiReviewService.clear_instance()
        worker_svc = AiReviewService.get_instance()
        worker_publisher = _make_publisher()
        worker_svc.set_msg_publisher(cast(MessagePublisher, worker_publisher))
        worker_svc.set_shard_ownership(
            ShardOwnership(instance_id=instance_id, instance_count=instance_count)
        )
        for review_id in review_ids:
            msg = _make_event(review_public_id=review_id)
            published = await worker_svc.handle_caps_violation_bus_message(msg)
            if published:
                publish_counts[review_id] += 1
    assert all(
        count == 1 for count in publish_counts.values()
    ), f"dedup invariant broken: per-review publish counts = {publish_counts}"


@pytest.mark.asyncio
async def test_external_frame_provenance_is_stamped_from_publisher_tracker() -> None:
    """Provenance comes from the publisher's tracker, NOT the bus message.

    The bridge gap detector keys per-stream sequence by topic. The
    internal ``bus.caps_violation_after_ai_approve`` topic interleaves
    caps events from every strategy, so reusing its ``sequence_id`` on
    the per-strategy external topic
    ``ai_reviews.{user}.{strategy}.caps_violation`` would create
    false gaps + mid-stream session-id joins on every fanout. The
    handler therefore allocates fresh provenance from
    :class:`SequenceTracker` keyed to the external topic.

    Given a publisher with a real tracker + a bus message,
    When the handler routes,
    Then the outbound frame's ``session_id`` matches the publisher's
    tracker session, ``sequence_id`` increments per topic, and
    ``public_id`` is a fresh UUID7 (not the bus message's id).
    """
    svc = AiReviewService.get_instance()
    publisher = _make_publisher()
    svc.set_msg_publisher(cast(MessagePublisher, publisher))
    msg = _make_event()
    await svc.handle_caps_violation_bus_message(msg)
    msg2 = _make_event()
    await svc.handle_caps_violation_bus_message(msg2)
    first = publisher.send.await_args_list[0].args[1]
    second = publisher.send.await_args_list[1].args[1]
    assert first.session_id == publisher.tracker.session_id
    assert second.session_id == publisher.tracker.session_id
    assert first.sequence_id == 1
    assert second.sequence_id == 2
    assert first.public_id != msg.public_id
    assert second.public_id != msg2.public_id
    assert first.public_id != second.public_id
