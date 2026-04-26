"""Tests for Plan A §7.1 bus.ai_review_decision fast-path (Phase 2 #3).

Covers two sides of the strategy-await fast-path:

1. Publisher side — :meth:`AiReviewService.submit_decision` MUST emit
   the bus event AFTER the atomic resolve commits so the strategy
   primitive's registered :class:`asyncio.Future` wakes up before
   the next DB-poll jitter interval. Publish is best-effort: a broker
   hiccup MUST NOT replace the post-commit decision result.

2. Listener side — :meth:`AiReviewService.handle_ai_review_decision_bus_message`
   resolves the registered future + tolerates cross-instance frames
   (no future registered) + idempotent re-decision frames (future
   already done).
"""

import asyncio
from collections.abc import Iterator
from datetime import UTC
from datetime import datetime
from datetime import timedelta
from typing import cast
from unittest.mock import AsyncMock
from unittest.mock import MagicMock
from uuid import uuid7

import pytest

from snapper.application.ai_review.service import AiReviewService
from snapper.messaging.infrastructure.publisher import MessagePublisher
from snapper.messaging.infrastructure.publisher import SequenceTracker
from snapper.messaging.schemas.data import AiReviewDecisionData


@pytest.fixture(autouse=True)
def _clear_singleton() -> Iterator[None]:
    """Reset the AiReviewService singleton between cases."""
    AiReviewService.clear_instance()
    yield
    AiReviewService.clear_instance()


def _make_publisher() -> MagicMock:
    """Build a MagicMock publisher with a real :class:`SequenceTracker`."""
    publisher = MagicMock()
    publisher.send = AsyncMock()
    publisher.tracker = SequenceTracker()
    return publisher


def _make_decision_event(
    *,
    review_public_id: str = "rev-1",
    decision: str = "approve",
    new_status: str = "resolved_approved",
    resolution_mode: str = "pick_one_primary",
    dispatch_version: int = 3,
) -> AiReviewDecisionData:
    """Build a canonical bus event payload."""
    return AiReviewDecisionData(
        public_id=str(uuid7()),
        timestamp=datetime.now(UTC),
        session_id=str(uuid7()),
        sequence_id=1,
        review_public_id=review_public_id,
        responding_delegate_public_id="del-1",
        decision=decision,
        new_status=new_status,
        resolution_mode=resolution_mode,
        dispatch_version=dispatch_version,
    )


class TestPublishDecisionBusEvent:
    """`_publish_decision_bus_event` — post-commit best-effort fanout."""

    @pytest.mark.asyncio
    async def test_happy_path_publishes_to_correct_topic(self) -> None:
        """Decision published on bus.ai_review_decision with the right payload shape.

        The publisher tracker stamps fresh provenance; the event
        carries every field the listener needs to resolve the future +
        thread the outcome onto the strategy await loop.
        """
        svc = AiReviewService.get_instance()
        publisher = _make_publisher()
        svc.set_msg_publisher(cast(MessagePublisher, publisher))
        wall_clock = datetime.now(UTC)
        await svc._publish_decision_bus_event(
            review_public_id="rev-1",
            responding_delegate_public_id="del-1",
            decision="approve",
            new_status="resolved_approved",
            resolution_mode="pick_one_primary",
            dispatch_version=7,
            wall_clock=wall_clock,
        )
        publisher.send.assert_awaited_once()
        topic, payload = publisher.send.await_args.args
        assert topic == "bus.ai_review_decision"
        assert isinstance(payload, AiReviewDecisionData)
        assert payload.review_public_id == "rev-1"
        assert payload.responding_delegate_public_id == "del-1"
        assert payload.decision == "approve"
        assert payload.new_status == "resolved_approved"
        assert payload.resolution_mode == "pick_one_primary"
        assert payload.dispatch_version == 7
        assert payload.timestamp == wall_clock
        assert payload.sequence_id == 1
        assert payload.session_id == publisher.tracker.session_id

    @pytest.mark.asyncio
    async def test_publisher_missing_logs_warning_and_returns(self) -> None:
        """Missing publisher -> warn + no-op (graceful degradation).

        Mirrors the ScopeGrantService best-effort contract: a
        singleton spun up before the FastAPI lifespan attached one
        must still tolerate the call without crashing the
        submit_decision flow.
        """
        svc = AiReviewService.get_instance()
        await svc._publish_decision_bus_event(
            review_public_id="rev-1",
            responding_delegate_public_id="del-1",
            decision="approve",
            new_status="resolved_approved",
            resolution_mode="pick_one_primary",
            dispatch_version=1,
            wall_clock=datetime.now(UTC),
        )

    @pytest.mark.asyncio
    async def test_send_failure_swallowed_so_decision_result_propagates(self) -> None:
        """Publisher.send raising MUST NOT replace the post-commit decision.

        The DB-side decision is the primary contract; the bus event
        is auxiliary fanout. A broker hiccup must log + swallow so
        submit_decision still returns the success envelope to the
        caller (otherwise the AI delegate's MCP tool call would see a
        transport error masking a successful decision record).
        """
        svc = AiReviewService.get_instance()
        publisher = _make_publisher()
        publisher.send = AsyncMock(side_effect=RuntimeError("broker down"))
        svc.set_msg_publisher(cast(MessagePublisher, publisher))
        await svc._publish_decision_bus_event(
            review_public_id="rev-1",
            responding_delegate_public_id="del-1",
            decision="approve",
            new_status="resolved_approved",
            resolution_mode="pick_one_primary",
            dispatch_version=1,
            wall_clock=datetime.now(UTC),
        )

    @pytest.mark.asyncio
    async def test_publisher_captured_into_local_avoids_shutdown_race(self) -> None:
        """Publisher reference captured BEFORE await — concurrent shutdown safe.

        Mirrors the TradingCapsEnforcer fix-up shape from Phase 2 #2:
        if a concurrent ``set_msg_publisher(None)`` from the lifespan
        teardown path runs between the None-check + the actual await,
        the captured local must still be the original publisher so
        the .send() call doesn't crash on AttributeError.

        Given a publisher whose send() is slow, and a shutdown that
            clears the publisher slot mid-await,
        When the helper runs,
        Then the captured local resolves the in-flight send() against
            the original publisher (no AttributeError, no NoneType
            crash).
        """
        svc = AiReviewService.get_instance()
        publisher = _make_publisher()
        captured_send_event = asyncio.Event()

        async def _slow_send(*_args: object, **_kwargs: object) -> None:
            captured_send_event.set()
            await asyncio.sleep(0)

        publisher.send = AsyncMock(side_effect=_slow_send)
        svc.set_msg_publisher(cast(MessagePublisher, publisher))

        async def _publish_then_clear() -> None:
            await svc._publish_decision_bus_event(
                review_public_id="rev-1",
                responding_delegate_public_id="del-1",
                decision="approve",
                new_status="resolved_approved",
                resolution_mode="pick_one_primary",
                dispatch_version=1,
                wall_clock=datetime.now(UTC),
            )

        async def _shutdown_clearer() -> None:
            await captured_send_event.wait()
            svc.set_msg_publisher(None)

        await asyncio.gather(_publish_then_clear(), _shutdown_clearer())
        publisher.send.assert_awaited_once()


class TestHandleAiReviewDecisionBusMessage:
    """`handle_ai_review_decision_bus_message` — listener-side future resolver."""

    def test_resolves_matching_registered_future(self) -> None:
        """A future registered by the strategy primitive is resolved by the bus event.

        Plan A §7.1: the strategy await loop blocks on
        ``asyncio.wait_for(fut, ...)`` mixed with a jittered DB poll.
        When the bus event fires, ``set_result`` wakes the future and
        the strategy bypasses the remaining poll interval.
        """
        loop = asyncio.new_event_loop()
        try:
            svc = AiReviewService.get_instance()
            fut: asyncio.Future[object] = loop.create_future()
            svc.register_future("rev-1", fut)
            event = _make_decision_event(review_public_id="rev-1")
            result = svc.handle_ai_review_decision_bus_message(event)
            assert result is True
            assert fut.done()
            assert fut.result() is event
        finally:
            loop.close()

    def test_returns_false_when_no_future_registered(self) -> None:
        """Cross-instance frame: this process has no future to resolve.

        Plan D §3 single-publisher / multi-subscriber correctness:
        every instance subscribes to bus.ai_review_decision; only the
        instance that initiated the CONSULT has the future in its
        registry. Remote instances treat the event as a no-op.
        """
        svc = AiReviewService.get_instance()
        event = _make_decision_event(review_public_id="never-registered-rev")
        result = svc.handle_ai_review_decision_bus_message(event)
        assert result is False

    def test_returns_false_when_future_already_done(self) -> None:
        """Idempotent re-decision: future was already resolved by DB-poll path.

        The strategy await loop polls the DB on a jittered interval. If
        the row reaches terminal state via the poll fallback BEFORE
        the bus event fires (or a duplicate bus event is received),
        the future is already done. ``set_result`` would raise
        InvalidStateError; the handler must guard via ``done()`` and
        no-op.
        """
        loop = asyncio.new_event_loop()
        try:
            svc = AiReviewService.get_instance()
            fut: asyncio.Future[object] = loop.create_future()
            fut.set_result("poll-driven outcome")
            svc.register_future("rev-1", fut)
            event = _make_decision_event(review_public_id="rev-1")
            result = svc.handle_ai_review_decision_bus_message(event)
            assert result is False
            assert fut.result() == "poll-driven outcome"
        finally:
            loop.close()

    def test_distinct_review_ids_resolve_independently(self) -> None:
        """Multiple in-flight strategies — each future resolves on its own event."""
        loop = asyncio.new_event_loop()
        try:
            svc = AiReviewService.get_instance()
            fut_a: asyncio.Future[object] = loop.create_future()
            fut_b: asyncio.Future[object] = loop.create_future()
            svc.register_future("rev-A", fut_a)
            svc.register_future("rev-B", fut_b)
            svc.handle_ai_review_decision_bus_message(
                _make_decision_event(review_public_id="rev-A")
            )
            assert fut_a.done()
            assert not fut_b.done()
        finally:
            loop.close()


class TestRegisterAfterCreateRace:
    """Cache-driven fix for the register-after-create race.

    Closes Codex P2 finding on commit 7d26a03: a fast delegate could
    submit a decision in the gap between :meth:`create_review`'s
    INSERT-commit and the strategy primitive's :meth:`register_future`
    invocation. Pre-fix the listener no-op'd. Post-fix the listener
    stashes the event in :attr:`_pending_resolutions`; the next
    register_future drains the cache + resolves the future immediately.
    """

    def test_decision_received_before_register_future_resolves_on_register(self) -> None:
        """Bus event before register_future -> next register_future resolves immediately.

        Given a decision event arrives + listener stashes it because
            no future is registered yet,
        When the strategy primitive calls register_future for the
            same review_public_id,
        Then the future resolves immediately (no spinning DB poll).
        """
        loop = asyncio.new_event_loop()
        try:
            svc = AiReviewService.get_instance()
            event = _make_decision_event(review_public_id="rev-race")
            stashed = svc.handle_ai_review_decision_bus_message(event)
            assert stashed is False
            assert "rev-race" in svc._pending_resolutions
            fut: asyncio.Future[object] = loop.create_future()
            svc.register_future("rev-race", fut)
            assert fut.done()
            assert fut.result() is event
            assert "rev-race" not in svc._pending_resolutions
        finally:
            loop.close()

    def test_pending_cache_does_not_resolve_already_done_future(self) -> None:
        """register_future against an already-done future is a no-op even with cache hit.

        Given a future that already completed via the DB-poll path +
            a cached event from the bus listener,
        When register_future is called against the done future,
        Then the cache entry is dropped but set_result is NOT called
            on the done future (no InvalidStateError).
        """
        loop = asyncio.new_event_loop()
        try:
            svc = AiReviewService.get_instance()
            event = _make_decision_event(review_public_id="rev-stale")
            svc.handle_ai_review_decision_bus_message(event)
            assert "rev-stale" in svc._pending_resolutions
            fut: asyncio.Future[object] = loop.create_future()
            fut.set_result("poll-driven")
            svc.register_future("rev-stale", fut)
            assert fut.result() == "poll-driven"
            assert "rev-stale" not in svc._pending_resolutions
        finally:
            loop.close()

    def test_unregister_future_evicts_pending_cache_entry(self) -> None:
        """unregister_future drops the matching pending-resolution cache entry.

        Given a cached event from the bus listener for a strategy
            that has not yet registered its future,
        When unregister_future is called for the same review_public_id
            (e.g. strategy crashed),
        Then the cache entry is dropped (TTL would eventually evict
            it, but explicit removal keeps the cache tight).
        """
        svc = AiReviewService.get_instance()
        event = _make_decision_event(review_public_id="rev-unreg")
        svc.handle_ai_review_decision_bus_message(event)
        assert "rev-unreg" in svc._pending_resolutions
        svc.unregister_future("rev-unreg")
        assert "rev-unreg" not in svc._pending_resolutions

    def test_pending_cache_entry_evicted_after_ttl_expires(self) -> None:
        """Stale entries past TTL are evicted on the next register_future call.

        Given a stale cache entry past the 30s TTL,
        When register_future is called (for any review),
        Then the stale entry is evicted opportunistically and the
            corresponding future is NOT resolved (the cache miss is
            treated as a true cache miss, NOT a stale hit).
        """
        loop = asyncio.new_event_loop()
        try:
            svc = AiReviewService.get_instance()
            event = _make_decision_event(review_public_id="rev-stale")
            stale_timestamp = datetime.now(UTC) - timedelta(seconds=60)
            svc._pending_resolutions["rev-stale"] = (event, stale_timestamp)
            fut: asyncio.Future[object] = loop.create_future()
            svc.register_future("rev-stale", fut)
            assert not fut.done()
            assert "rev-stale" not in svc._pending_resolutions
        finally:
            loop.close()


class TestPendingResolutionsSizeCap:
    """Hard-size cap on _pending_resolutions cache (Phase 2 #5).

    Closes Codex non-blocking note from Phase 2 #3: TTL eviction is
    opportunistic, so a runaway publisher emitting events for review
    ids no strategy ever registers a future for could in principle
    grow the dict without bound between TTL sweeps. The hard cap
    bounds memory regardless of TTL pressure.
    """

    def test_overflow_evicts_oldest_entries_first(self) -> None:
        """Cap exceeded -> drop the oldest entries (insertion-order).

        Given the cache populated to MAX_ENTRIES+5 unique ids,
        When the cap is enforced opportunistically by the dispatch
            handler,
        Then exactly 5 entries are evicted and they are the OLDEST
            5 (Python dict preserves insertion order so popping from
            the front evicts oldest first; freshest events survive).
        """
        from snapper.application.ai_review import service as svc_module

        svc = AiReviewService.get_instance()
        cap = svc_module._PENDING_RESOLUTION_MAX_ENTRIES
        for i in range(cap + 5):
            event = _make_decision_event(review_public_id=f"rev-{i:05d}")
            svc.handle_ai_review_decision_bus_message(event)
        assert len(svc._pending_resolutions) == cap
        assert "rev-00000" not in svc._pending_resolutions
        assert "rev-00004" not in svc._pending_resolutions
        assert "rev-00005" in svc._pending_resolutions
        latest_key = f"rev-{cap + 4:05d}"
        assert latest_key in svc._pending_resolutions

    def test_at_or_below_cap_is_noop(self) -> None:
        """Cache size at-or-below the cap leaves every entry alone.

        Given the cache populated to exactly MAX_ENTRIES,
        When _enforce_pending_resolution_size_cap is invoked,
        Then no entries are evicted (overflow=0).
        """
        from snapper.application.ai_review import service as svc_module

        svc = AiReviewService.get_instance()
        cap = svc_module._PENDING_RESOLUTION_MAX_ENTRIES
        for i in range(cap):
            event = _make_decision_event(review_public_id=f"rev-{i:05d}")
            svc.handle_ai_review_decision_bus_message(event)
        assert len(svc._pending_resolutions) == cap
        svc._enforce_pending_resolution_size_cap()
        assert len(svc._pending_resolutions) == cap
        assert "rev-00000" in svc._pending_resolutions

    def test_empty_cache_size_cap_is_noop(self) -> None:
        """Empty cache -> _enforce_pending_resolution_size_cap is a no-op.

        Given an empty pending-resolution cache,
        When _enforce_pending_resolution_size_cap is invoked,
        Then no entries are evicted and no warning is logged.
        """
        svc = AiReviewService.get_instance()
        assert len(svc._pending_resolutions) == 0
        svc._enforce_pending_resolution_size_cap()
        assert len(svc._pending_resolutions) == 0
