"""Bound and deduplicate review work before consultation."""

import asyncio
from collections import OrderedDict
from collections import deque
from collections.abc import Awaitable
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC
from datetime import datetime
from enum import StrEnum

from snapper_delegate.consult import ReviewConsultContext


class InboxOfferOutcome(StrEnum):
    """Classify one request or decision acknowledgement offered to the inbox."""

    QUEUED = "queued"
    HELD = "held"
    REPLACED = "replaced"
    DUPLICATE = "duplicate"
    EXPIRED = "expired"
    CAPACITY = "capacity"
    CANCELLED = "cancelled"
    IGNORED = "ignored"
    CLOSED = "closed"


class _EntryState(StrEnum):
    """Distinguish immediately available work from fanout-held work."""

    READY = "ready"
    HELD = "held"


@dataclass(frozen=True, slots=True)
class _ReleasePlan:
    """Describe when one exact held version may leave the hold."""

    review_public_id: str
    dispatch_version: int
    release_at: datetime


@dataclass(slots=True)
class _InboxEntry:
    """Store one active highest-version review and its release timer."""

    context: ReviewConsultContext
    state: _EntryState
    release_task: asyncio.Task[None] | None = None


class ReviewInbox:
    """Offer deduplicated review contexts through a bounded asynchronous queue."""

    def __init__(
        self,
        own_delegate_public_id: str,
        capacity: int,
        clock: Callable[[], datetime] | None = None,
        sleeper: Callable[[float], Awaitable[None]] | None = None,
    ) -> None:
        """Initialize an inert inbox with injectable time seams."""
        if capacity < 1:
            raise ValueError("capacity must be positive")
        self._own_delegate_public_id = own_delegate_public_id
        self._capacity = capacity
        self._clock = clock or _utc_now
        self._sleeper = sleeper or asyncio.sleep
        self._condition = asyncio.Condition()
        self._entries: dict[str, _InboxEntry] = {}
        self._highest_request_versions: OrderedDict[str, int] = OrderedDict()
        self._highest_ack_versions: OrderedDict[str, int] = OrderedDict()
        self._dedup_capacity = max(capacity, 10_000)
        self._ready: deque[str] = deque()
        self._closed = False

    async def offer(self, context: ReviewConsultContext) -> InboxOfferOutcome:
        """Accept, replace, hold, or reject one review request without blocking.

        Args:
            context: Immutable review request proposed for local processing.

        Returns:
            The queue, hold, replacement, refusal, or closure outcome.
        """
        async with self._condition:
            if self._closed:
                return InboxOfferOutcome.CLOSED
            review_id = context.review_public_id
            if review_id in self._highest_ack_versions:
                return InboxOfferOutcome.CANCELLED
            if self._is_duplicate(context):
                return InboxOfferOutcome.DUPLICATE
            previous = self._entries.get(review_id)
            if _is_expired(context.deadline, self._clock()):
                self._replace_version(context, previous)
                return InboxOfferOutcome.EXPIRED
            if previous is None and len(self._entries) >= self._capacity:
                return InboxOfferOutcome.CAPACITY
            self._replace_version(context, previous)
            self._enqueue(context)
            if previous is not None:
                return InboxOfferOutcome.REPLACED
            if self._entries[review_id].state is _EntryState.READY:
                return InboxOfferOutcome.QUEUED
            return InboxOfferOutcome.HELD

    async def acknowledge(
        self,
        review_public_id: str,
        dispatch_version: int,
    ) -> InboxOfferOutcome:
        """Cancel unresolved work when an equal or newer decision is acknowledged.

        Args:
            review_public_id: Public identifier of the resolved review.
            dispatch_version: Version observed in the decision acknowledgement.

        Returns:
            The cancellation, ignore, or closure outcome.
        """
        async with self._condition:
            if self._closed:
                return InboxOfferOutcome.CLOSED
            highest = self._highest_ack_versions.get(review_public_id)
            if highest is not None and dispatch_version <= highest:
                return InboxOfferOutcome.IGNORED
            self._remember_version(
                self._highest_ack_versions,
                review_public_id,
                dispatch_version,
            )
            entry = self._entries.get(review_public_id)
            if entry is None:
                return InboxOfferOutcome.IGNORED
            self._discard_entry(review_public_id, entry)
            return InboxOfferOutcome.CANCELLED

    async def get(self) -> ReviewConsultContext | None:
        """Wait for the next eligible context or return ``None`` after closure.

        Returns:
            The next ready consultation context, or ``None`` once closed.
        """
        async with self._condition:
            while not self._closed and not self._ready:
                await self._condition.wait()
            if self._closed:
                return None
            review_id = self._ready.popleft()
            return self._entries.pop(review_id).context

    async def drain_pending(self) -> int:
        """Drop queued and held work while keeping the inbox usable.

        This is the hold contract's no-new half: a runner that loses consult
        duty must stop answering anything it has not already started, and a
        request left sitting here would otherwise be answered late — after the
        operator believed the delegate was paused.

        Discarding an entry also forgets the request version it was seen at.
        Keeping that watermark would make the review unanswerable: the catch-up
        sweep after resume re-offers it at the same version, dedup would call
        that a replay, and work nobody ever performed would be dropped for good
        unless the server happened to bump its dispatch version. Acknowledgement
        watermarks are kept, because a decision already sent stays sent.

        Returns:
            How many pending reviews were discarded.
        """
        async with self._condition:
            if self._closed:
                return 0
            tasks = [
                entry.release_task
                for entry in self._entries.values()
                if entry.release_task is not None
            ]
            discarded = len(self._entries)
            for task in tasks:
                task.cancel()
            for review_id in self._entries:
                self._highest_request_versions.pop(review_id, None)
            self._entries.clear()
            self._ready.clear()
            self._condition.notify_all()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        return discarded

    async def close(self) -> None:
        """Cancel held releases, discard pending work, and wake blocked consumers."""
        async with self._condition:
            if self._closed:
                return
            self._closed = True
            tasks = [
                entry.release_task
                for entry in self._entries.values()
                if entry.release_task is not None
            ]
            for task in tasks:
                task.cancel()
            self._entries.clear()
            self._highest_request_versions.clear()
            self._highest_ack_versions.clear()
            self._ready.clear()
            self._condition.notify_all()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

    def _is_duplicate(self, context: ReviewConsultContext) -> bool:
        """Return whether this review version is no newer than one already seen."""
        highest = self._highest_request_versions.get(context.review_public_id)
        return highest is not None and context.dispatch_version <= highest

    def _replace_version(
        self,
        context: ReviewConsultContext,
        previous: _InboxEntry | None,
    ) -> None:
        """Record a new high-water mark and discard its prior active entry."""
        if previous is not None:
            self._discard_entry(context.review_public_id, previous)
        self._remember_version(
            self._highest_request_versions,
            context.review_public_id,
            context.dispatch_version,
        )

    def _remember_version(
        self,
        versions: OrderedDict[str, int],
        review_public_id: str,
        dispatch_version: int,
    ) -> None:
        """Store one recency-ordered watermark within the fixed memory bound."""
        if review_public_id in versions:
            versions.pop(review_public_id)
        elif len(versions) >= self._dedup_capacity:
            versions.popitem(last=False)
        versions[review_public_id] = dispatch_version

    def _enqueue(self, context: ReviewConsultContext) -> None:
        """Queue selected or fanout-eligible work and schedule all other work."""
        now = self._clock()
        if context.selected_delegate_public_id == self._own_delegate_public_id or _fanout_passed(
            context.fanout_after, now
        ):
            self._entries[context.review_public_id] = _InboxEntry(
                context=context,
                state=_EntryState.READY,
            )
            self._ready.append(context.review_public_id)
            self._condition.notify()
            return
        entry = _InboxEntry(context=context, state=_EntryState.HELD)
        self._entries[context.review_public_id] = entry
        plan = _release_plan(context)
        entry.release_task = asyncio.create_task(self._release_after(plan))

    async def _release_after(self, plan: _ReleasePlan) -> None:
        """Move one held version to ready state when its fanout time arrives."""
        delay = max(0.0, (plan.release_at - self._clock()).total_seconds())
        await self._sleeper(delay)
        async with self._condition:
            entry = self._matching_held_entry(plan)
            if self._closed or entry is None:
                return
            now = self._clock()
            if _is_before(now, plan.release_at):
                entry.release_task = asyncio.create_task(self._release_after(plan))
                return
            if _is_expired(entry.context.deadline, now):
                self._entries.pop(plan.review_public_id)
                return
            entry.state = _EntryState.READY
            entry.release_task = None
            self._ready.append(plan.review_public_id)
            self._condition.notify()

    def _matching_held_entry(self, plan: _ReleasePlan) -> _InboxEntry | None:
        """Return the held entry targeted by one exact release plan."""
        entry = self._entries.get(plan.review_public_id)
        if entry is None:
            return None
        if entry.context.dispatch_version != plan.dispatch_version:
            return None
        if entry.state is not _EntryState.HELD:
            return None
        return entry

    def _discard_entry(self, review_public_id: str, entry: _InboxEntry) -> None:
        """Remove one active entry and cancel its release if present."""
        if entry.state is _EntryState.READY:
            self._ready.remove(review_public_id)
        if entry.release_task is not None:
            entry.release_task.cancel()
        self._entries.pop(review_public_id)


def _utc_now() -> datetime:
    """Return the current timezone-aware UTC wall clock."""
    return datetime.now(UTC)


def _is_expired(deadline: datetime, now: datetime) -> bool:
    """Fail closed for naive time values and detect an elapsed deadline."""
    if deadline.utcoffset() is None or now.utcoffset() is None:
        return True
    return deadline <= now


def _fanout_passed(fanout_after: datetime | None, now: datetime) -> bool:
    """Return whether a valid foreign-review fanout time has elapsed."""
    if fanout_after is None:
        return False
    if fanout_after.utcoffset() is None or now.utcoffset() is None:
        return False
    return fanout_after <= now


def _is_before(left: datetime, right: datetime) -> bool:
    """Fail closed across naive and aware values when checking timer progress."""
    if left.utcoffset() is None or right.utcoffset() is None:
        return False
    return left < right


def _release_plan(context: ReviewConsultContext) -> _ReleasePlan:
    """Release at fanout only when it occurs strictly before the deadline."""
    fanout = context.fanout_after
    release_at = context.deadline
    if fanout is not None and fanout.utcoffset() is not None and fanout < context.deadline:
        release_at = fanout
    return _ReleasePlan(
        review_public_id=context.review_public_id,
        dispatch_version=context.dispatch_version,
        release_at=release_at,
    )
