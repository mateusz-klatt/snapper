"""Tests for bounded review deduplication and fanout eligibility."""

import asyncio
from collections.abc import Callable
from datetime import UTC
from datetime import datetime
from datetime import timedelta

import pytest

from snapper_delegate.consult import ReviewConsultContext
from snapper_delegate.review_inbox import InboxOfferOutcome
from snapper_delegate.review_inbox import ReviewInbox
from snapper_delegate.review_inbox import _fanout_passed
from snapper_delegate.review_inbox import _is_before
from snapper_delegate.review_inbox import _is_expired
from snapper_delegate.review_inbox import _ReleasePlan

_NOW = datetime(2026, 8, 2, 12, tzinfo=UTC)


class _MutableClock:
    """Expose deterministic mutable wall time to the inbox."""

    def __init__(self, current: datetime = _NOW) -> None:
        """Initialize the visible time."""
        self.current = current

    def __call__(self) -> datetime:
        """Return the current visible time."""
        return self.current

    def advance(self, seconds: float) -> None:
        """Advance visible time by a timer delay."""
        self.current += timedelta(seconds=seconds)


class _AdvancingSleeper:
    """Advance an injected clock instead of waiting in real time."""

    def __init__(self, clock: _MutableClock, early_returns: int = 0) -> None:
        """Configure how many sleeps return before advancing time."""
        self._clock = clock
        self._early_returns = early_returns
        self.calls: list[float] = []

    async def __call__(self, seconds: float) -> None:
        """Record a delay and advance unless this is an early return."""
        self.calls.append(seconds)
        await asyncio.sleep(0)
        if self._early_returns:
            self._early_returns -= 1
            return
        self._clock.advance(seconds)


class _BlockingSleeper:
    """Hold release timers until cancellation is observable."""

    def __init__(self) -> None:
        """Initialize the timer controls."""
        self.started = asyncio.Event()
        self.release = asyncio.Event()
        self.calls: list[float] = []
        self.cancelled = 0

    async def __call__(self, seconds: float) -> None:
        """Wait until explicitly released or cancelled."""
        self.calls.append(seconds)
        self.started.set()
        try:
            await self.release.wait()
        except asyncio.CancelledError:
            self.cancelled += 1
            raise


def _context() -> ReviewConsultContext:
    """Build one valid selected review context."""
    return ReviewConsultContext(
        review_public_id="review-1",
        selected_delegate_public_id="delegate-own",
        wallet_public_id="wallet-1",
        dispatch_version=1,
        deadline=_NOW + timedelta(seconds=60),
        fanout_after=_NOW + timedelta(seconds=30),
        signal_envelope={"side": "buy"},
        instrument_metadata={"symbol": "BTC/USD"},
    )


async def _wait_until(predicate: Callable[[], bool]) -> None:
    """Yield boundedly until one deterministic asynchronous condition holds."""
    for _ in range(20):
        if predicate():
            return
        await asyncio.sleep(0)
    raise AssertionError("condition did not become true")


def test_capacity_must_be_positive() -> None:
    """A zero-sized inbox is rejected before asynchronous use.

    Given an inbox configuration with no available slot,
    When the inbox is constructed,
    Then construction rejects the invalid capacity.
    """
    with pytest.raises(ValueError, match="capacity must be positive"):
        ReviewInbox("delegate-own", 0)


@pytest.mark.asyncio
async def test_selected_requests_deduplicate_and_acknowledge_by_highest_version() -> None:
    """Selected work queues immediately and retains a persistent version watermark.

    Given selected requests with duplicate, older, and newer dispatch versions,
    When requests are offered, consumed, acknowledged, and the inbox is closed,
    Then only valid new work queues and its high-water marks remain authoritative.
    """
    inbox = ReviewInbox("delegate-own", 2, clock=lambda: _NOW)
    first = _context().model_copy(update={"dispatch_version": 2})
    older = first.model_copy(update={"dispatch_version": 1})
    newer = first.model_copy(update={"dispatch_version": 3})

    assert await inbox.offer(first) is InboxOfferOutcome.QUEUED
    assert await inbox.offer(first) is InboxOfferOutcome.DUPLICATE
    assert await inbox.offer(older) is InboxOfferOutcome.DUPLICATE
    assert await inbox.get() == first
    assert await inbox.offer(newer) is InboxOfferOutcome.QUEUED
    assert await inbox.acknowledge(newer.review_public_id, 2) is InboxOfferOutcome.CANCELLED
    assert await inbox.acknowledge(newer.review_public_id, 3) is InboxOfferOutcome.IGNORED
    assert await inbox.acknowledge(newer.review_public_id, 3) is InboxOfferOutcome.IGNORED
    assert await inbox.acknowledge("unknown-review", 7) is InboxOfferOutcome.IGNORED
    assert await inbox.acknowledge("unknown-review", 6) is InboxOfferOutcome.IGNORED
    assert await inbox.offer(first) is InboxOfferOutcome.CANCELLED

    await inbox.close()
    await inbox.close()
    assert await inbox.offer(newer) is InboxOfferOutcome.CLOSED
    assert await inbox.acknowledge(newer.review_public_id, 4) is InboxOfferOutcome.CLOSED
    assert await inbox.get() is None


@pytest.mark.asyncio
async def test_ack_tombstones_precede_requests_and_watermarks_stay_bounded() -> None:
    """Early acknowledgements suppress later wakes within fixed dedup memory.

    Given acknowledgements that arrive before matching requests and a bounded cache,
    When further acknowledgement and request versions advance the watermarks,
    Then tombstoned work stays suppressed and only the newest keys remain retained.
    """
    inbox = ReviewInbox("delegate-own", 1, clock=lambda: _NOW)
    inbox._dedup_capacity = 2
    first = _context()
    second = first.model_copy(update={"review_public_id": "review-2"})
    third = first.model_copy(update={"review_public_id": "review-3"})

    assert await inbox.acknowledge("ack-1", 1) is InboxOfferOutcome.IGNORED
    assert await inbox.acknowledge("ack-2", 1) is InboxOfferOutcome.IGNORED
    assert await inbox.acknowledge("ack-3", 1) is InboxOfferOutcome.IGNORED
    acknowledged = first.model_copy(update={"review_public_id": "ack-3"})
    assert await inbox.offer(acknowledged) is InboxOfferOutcome.CANCELLED
    assert list(inbox._highest_ack_versions) == ["ack-2", "ack-3"]

    for context in (first, second, third):
        assert await inbox.offer(context) is InboxOfferOutcome.QUEUED
        assert await inbox.get() == context
    assert list(inbox._highest_request_versions) == ["review-2", "review-3"]
    await inbox.close()


@pytest.mark.asyncio
async def test_default_time_seams_queue_selected_future_work() -> None:
    """Production clock and sleeper defaults remain usable without injection.

    Given selected work with a future aware deadline and default time seams,
    When the request is offered and consumed,
    Then it queues immediately and closes cleanly.
    """
    inbox = ReviewInbox("delegate-own", 1)
    context = _context().model_copy(update={"deadline": datetime.now(UTC) + timedelta(minutes=1)})
    assert await inbox.offer(context) is InboxOfferOutcome.QUEUED
    assert await inbox.get() == context
    await inbox.close()


@pytest.mark.asyncio
async def test_capacity_counts_held_work_without_poisoning_rejected_versions() -> None:
    """Held entries consume capacity while rejected requests remain retryable.

    Given one held foreign request fills the inbox,
    When another request is refused, the held work is cancelled, and it is retried,
    Then the rejected version remains eligible and queues after capacity is released.
    """
    sleeper = _BlockingSleeper()
    inbox = ReviewInbox("delegate-own", 1, clock=lambda: _NOW, sleeper=sleeper)
    held = _context().model_copy(update={"selected_delegate_public_id": "delegate-foreign"})
    other = _context().model_copy(update={"review_public_id": "review-2"})

    assert await inbox.offer(held) is InboxOfferOutcome.HELD
    await asyncio.wait_for(sleeper.started.wait(), timeout=0.1)
    assert await inbox.offer(other) is InboxOfferOutcome.CAPACITY
    assert await inbox.acknowledge(held.review_public_id, 1) is InboxOfferOutcome.CANCELLED
    await _wait_until(lambda: sleeper.cancelled == 1)
    assert await inbox.offer(other) is InboxOfferOutcome.QUEUED
    assert await inbox.get() == other
    await inbox.close()


@pytest.mark.asyncio
async def test_newer_versions_replace_held_ready_and_expired_entries() -> None:
    """Every accepted newer version replaces the prior active representation.

    Given one review progresses through held, ready, foreign, and expired versions,
    When each strictly newer dispatch version is offered,
    Then it replaces the previous entry while older versions remain duplicates.
    """
    sleeper = _BlockingSleeper()
    inbox = ReviewInbox("delegate-own", 1, clock=lambda: _NOW, sleeper=sleeper)
    foreign = _context().model_copy(update={"selected_delegate_public_id": "delegate-foreign"})
    selected_v2 = _context().model_copy(update={"dispatch_version": 2})

    assert await inbox.offer(foreign) is InboxOfferOutcome.HELD
    await asyncio.wait_for(sleeper.started.wait(), timeout=0.1)
    assert await inbox.offer(selected_v2) is InboxOfferOutcome.REPLACED
    await _wait_until(lambda: sleeper.cancelled == 1)
    assert await inbox.get() == selected_v2

    selected_v3 = selected_v2.model_copy(update={"dispatch_version": 3})
    foreign_v4 = foreign.model_copy(update={"dispatch_version": 4})
    assert await inbox.offer(selected_v3) is InboxOfferOutcome.QUEUED
    assert await inbox.offer(foreign_v4) is InboxOfferOutcome.REPLACED
    expired_v5 = selected_v2.model_copy(update={"dispatch_version": 5, "deadline": _NOW})
    assert await inbox.offer(expired_v5) is InboxOfferOutcome.EXPIRED
    assert await inbox.offer(foreign_v4) is InboxOfferOutcome.DUPLICATE
    await inbox.close()


@pytest.mark.asyncio
async def test_foreign_request_queues_after_fanout_and_reschedules_early_timer() -> None:
    """Foreign work becomes available only after its injected fanout clock passes.

    Given a foreign request and a release sleeper that returns early once,
    When the inbox waits for fanout eligibility,
    Then it reschedules the remaining delay and releases the request only afterward.
    """
    clock = _MutableClock()
    sleeper = _AdvancingSleeper(clock, early_returns=1)
    inbox = ReviewInbox("delegate-own", 1, clock=clock, sleeper=sleeper)
    foreign = _context().model_copy(update={"selected_delegate_public_id": "delegate-foreign"})

    assert await inbox.offer(foreign) is InboxOfferOutcome.HELD
    assert await asyncio.wait_for(inbox.get(), timeout=0.1) == foreign
    assert sleeper.calls == [30.0, 30.0]
    await inbox.close()


@pytest.mark.asyncio
async def test_foreign_request_already_past_fanout_queues_immediately() -> None:
    """A foreign wake received after fanout does not incur another hold.

    Given a foreign request whose fanout boundary has already arrived,
    When the request is offered,
    Then it queues immediately without a release timer.
    """
    context = _context().model_copy(
        update={
            "selected_delegate_public_id": "delegate-foreign",
            "fanout_after": _NOW,
        }
    )
    inbox = ReviewInbox("delegate-own", 1, clock=lambda: _NOW)
    assert await inbox.offer(context) is InboxOfferOutcome.QUEUED
    assert await inbox.get() == context
    await inbox.close()


@pytest.mark.parametrize(
    "fanout_after",
    [
        None,
        datetime(2026, 8, 2, 12, 0, 30),
        _NOW + timedelta(seconds=60),
    ],
)
@pytest.mark.asyncio
async def test_unusable_fanout_expires_and_releases_capacity(
    fanout_after: datetime | None,
) -> None:
    """Missing, naive, and deadline-equal fanout times never expose foreign work.

    Given foreign work with a missing, naive, or unusably late fanout boundary,
    When its hold is evaluated against the deadline,
    Then the work expires unseen and releases capacity for a valid replacement.
    """
    clock = _MutableClock()
    sleeper = _AdvancingSleeper(clock)
    inbox = ReviewInbox("delegate-own", 1, clock=clock, sleeper=sleeper)
    foreign = _context().model_copy(
        update={
            "selected_delegate_public_id": "delegate-foreign",
            "fanout_after": fanout_after,
        }
    )

    assert await inbox.offer(foreign) is InboxOfferOutcome.HELD
    await _wait_until(lambda: not inbox._entries)
    replacement = _context().model_copy(
        update={
            "review_public_id": "review-2",
            "deadline": clock.current + timedelta(seconds=60),
        }
    )
    assert await inbox.offer(replacement) is InboxOfferOutcome.QUEUED
    assert await inbox.get() == replacement
    await inbox.close()


@pytest.mark.asyncio
async def test_expired_and_naive_requests_fail_closed() -> None:
    """Elapsed or timezone-naive request times are skipped without consuming capacity.

    Given elapsed deadlines or a timezone-naive request or clock,
    When the inbox evaluates each request,
    Then every ambiguous or expired request fails closed as expired.
    """
    inbox = ReviewInbox("delegate-own", 1, clock=lambda: _NOW)
    expired = _context().model_copy(update={"deadline": _NOW})
    naive = _context().model_copy(
        update={
            "review_public_id": "review-2",
            "deadline": datetime(2026, 8, 2, 12, 1),
        }
    )
    naive_clock = ReviewInbox(
        "delegate-own",
        1,
        clock=lambda: datetime(2026, 8, 2, 12),
    )

    assert await inbox.offer(expired) is InboxOfferOutcome.EXPIRED
    assert await inbox.offer(naive) is InboxOfferOutcome.EXPIRED
    assert await naive_clock.offer(_context()) is InboxOfferOutcome.EXPIRED
    await inbox.close()
    await naive_clock.close()


@pytest.mark.asyncio
async def test_draining_drops_pending_work_and_forgets_only_its_request_versions() -> None:
    """A drained review can be offered again, while an answered one stays answered.

    Given ready work, held foreign work, and a review whose decision was acknowledged,
    When the inbox is drained because consult duty was lost,
    Then pending work is discarded with its timer and can be re-offered at the
    same dispatch version after a resume, while the acknowledged review stays
    cancelled because a decision already sent stays sent.
    """
    sleeper = _BlockingSleeper()
    inbox = ReviewInbox("delegate-own", 3, clock=lambda: _NOW, sleeper=sleeper)
    ready = _context()
    held = _context().model_copy(
        update={
            "review_public_id": "review-foreign",
            "selected_delegate_public_id": "delegate-foreign",
        }
    )
    answered = _context().model_copy(update={"review_public_id": "review-answered"})

    assert await inbox.offer(ready) is InboxOfferOutcome.QUEUED
    assert await inbox.offer(held) is InboxOfferOutcome.HELD
    assert await inbox.offer(answered) is InboxOfferOutcome.QUEUED
    assert await inbox.acknowledge("review-answered", 1) is InboxOfferOutcome.CANCELLED
    await asyncio.wait_for(sleeper.started.wait(), timeout=0.1)

    assert await inbox.drain_pending() == 2
    assert sleeper.cancelled == 1
    assert await inbox.offer(ready) is InboxOfferOutcome.QUEUED
    assert await inbox.offer(answered) is InboxOfferOutcome.CANCELLED
    assert await inbox.get() == ready

    await inbox.close()
    assert await inbox.drain_pending() == 0


@pytest.mark.asyncio
async def test_close_cancels_holds_discards_ready_and_wakes_getters() -> None:
    """Closure promptly cancels all pending forms and unblocks empty consumers.

    Given held work, ready work, and a consumer blocked on an empty inbox,
    When each inbox is closed,
    Then timers are cancelled, queued work is discarded, and consumers receive closure.
    """
    sleeper = _BlockingSleeper()
    inbox = ReviewInbox("delegate-own", 2, clock=lambda: _NOW, sleeper=sleeper)
    held = _context().model_copy(update={"selected_delegate_public_id": "delegate-foreign"})
    ready = _context().model_copy(update={"review_public_id": "review-2"})

    assert await inbox.offer(held) is InboxOfferOutcome.HELD
    assert await inbox.offer(ready) is InboxOfferOutcome.QUEUED
    await asyncio.wait_for(sleeper.started.wait(), timeout=0.1)
    await inbox.close()
    assert sleeper.cancelled == 1
    assert await inbox.get() is None

    empty = ReviewInbox("delegate-own", 1, clock=lambda: _NOW)
    waiting = asyncio.create_task(empty.get())
    await asyncio.sleep(0)
    await empty.close()
    assert await asyncio.wait_for(waiting, timeout=0.1) is None


@pytest.mark.asyncio
async def test_release_lookup_ignores_missing_closed_mismatched_and_ready_entries() -> None:
    """Stale release plans cannot mutate a different active inbox entry.

    Given release plans for missing, closed, ready, or mismatched-version entries,
    When release lookup and delayed release run,
    Then only the exact currently held entry can be selected for mutation.
    """

    async def _immediate_sleep(seconds: float) -> None:
        """Yield once without waiting for the requested delay."""
        del seconds
        await asyncio.sleep(0)

    release_inbox = ReviewInbox(
        "delegate-own",
        1,
        clock=lambda: _NOW,
        sleeper=_immediate_sleep,
    )
    missing = _ReleasePlan("missing", 1, _NOW)
    await release_inbox._release_after(missing)
    await release_inbox.close()
    await release_inbox._release_after(missing)

    sleeper = _BlockingSleeper()
    lookup_inbox = ReviewInbox("delegate-own", 2, clock=lambda: _NOW, sleeper=sleeper)
    ready = _context()
    held = ready.model_copy(
        update={"review_public_id": "review-2", "selected_delegate_public_id": "foreign"}
    )
    assert await lookup_inbox.offer(ready) is InboxOfferOutcome.QUEUED
    assert await lookup_inbox.offer(held) is InboxOfferOutcome.HELD
    assert lookup_inbox._matching_held_entry(_ReleasePlan("missing", 1, _NOW)) is None
    assert lookup_inbox._matching_held_entry(_ReleasePlan("review-2", 2, _NOW)) is None
    assert lookup_inbox._matching_held_entry(_ReleasePlan("review-1", 1, _NOW)) is None
    assert (
        lookup_inbox._matching_held_entry(_ReleasePlan("review-2", 1, _NOW))
        is lookup_inbox._entries["review-2"]
    )
    await lookup_inbox.close()


def test_time_helpers_fail_closed_and_cover_timezone_boundaries() -> None:
    """Clock helpers reject naive comparisons and honor aware boundaries.

    Given aware and naive instants at past, equal, and future boundaries,
    When expiry, fanout, and ordering helpers compare them,
    Then ambiguous comparisons fail closed and aware comparisons honor boundaries.
    """
    naive = datetime(2026, 8, 2, 12)
    future = _NOW + timedelta(seconds=1)

    assert _is_expired(naive, _NOW) is True
    assert _is_expired(future, naive) is True
    assert _is_expired(future, _NOW) is False
    assert _fanout_passed(None, _NOW) is False
    assert _fanout_passed(naive, _NOW) is False
    assert _fanout_passed(future, naive) is False
    assert _fanout_passed(_NOW, _NOW) is True
    assert _is_before(naive, _NOW) is False
    assert _is_before(_NOW, naive) is False
    assert _is_before(_NOW, future) is True
    assert _is_before(future, _NOW) is False
