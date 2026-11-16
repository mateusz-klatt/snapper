"""Witness explicit selected collection, original receipt evidence and cancellation boundaries."""

import asyncio
import time
from collections.abc import AsyncGenerator
from collections.abc import AsyncIterator
from contextlib import suppress
from dataclasses import dataclass
from dataclasses import field
from datetime import UTC
from datetime import datetime
from datetime import timedelta

import pytest

from snapper.data.repository_types import MarketSnapshotRefreshTargetRow
from snapper.infrastructure.exchanges.contracts import TickerUpdate
from snapper.infrastructure.market_data.selected_snapshot_collector import SelectedSnapshotBatch
from snapper.infrastructure.market_data.selected_snapshot_collector import (
    SelectedSnapshotTickerClient,
)
from snapper.infrastructure.market_data.selected_snapshot_collector import (
    collect_selected_snapshots,
)
from snapper.messaging.infrastructure.publisher import SequenceTracker

_OBSERVED = datetime(2026, 11, 16, tzinfo=UTC)


def _target(
    native: str = "BTC-EUR", identity: str = "instrument-a", exchange: str = "kraken"
) -> MarketSnapshotRefreshTargetRow:
    """Build synthetic resolved metadata without any catalog or database dependency.

    Args:
        native: Exact venue-native subscription symbol.
        identity: Exact target instrument identity.
        exchange: Venue identity used by pre-subscription validation.

    Returns:
        One typed target row.
    """
    return {
        "instrument_public_id": identity,
        "native_symbol": native,
        "exchange": exchange,
        "base": "BTC",
        "quote": "EUR",
        "asset_type": "crypto",
    }


def _tick(
    symbol: str = "BTC-EUR", last: float = 2.0, delayed: bool = False, book: bool = True
) -> TickerUpdate:
    """Build a typed synthetic venue tick.

    Args:
        symbol: Exact native symbol reported by the venue.
        last: Executable-price estimate, including deliberately invalid test values.
        delayed: Explicit venue-delayed evidence marker.
        book: Whether positive book prices are present.

    Returns:
        A complete in-memory tick payload.
    """
    return TickerUpdate(
        symbol=symbol,
        bid=1.0 if book else 0.0,
        bid_qty=3.0,
        ask=3.0 if book else 0.0,
        ask_qty=4.0,
        last=last,
        volume=5.0,
        vwap=6.0,
        low=0.5,
        high=7.0,
        change=0.25,
        change_pct=1.0,
        is_delayed=delayed,
    )


@dataclass
class _Client:
    """Socket-free async generator with observable subscription and cleanup behavior."""

    updates: list[TickerUpdate] = field(default_factory=list)
    error: Exception | None = None
    cleanup_error: Exception | None = None
    wait_after: bool = False
    wait_cleanup: bool = False
    subscriptions: list[list[str]] = field(default_factory=list)
    waiting: asyncio.Event = field(default_factory=asyncio.Event)
    cleanup_started: asyncio.Event = field(default_factory=asyncio.Event)
    unblock: asyncio.Event = field(default_factory=asyncio.Event)
    cleanup_cancelled: bool = False
    closed: bool = False
    yielded: int = 0

    def subscribe_ticks(self, symbols: list[str]) -> AsyncIterator[TickerUpdate]:
        """Record the exact requested subscription and return its owned iterator.

        Args:
            symbols: Explicit venue-native symbols supplied by the collector.

        Returns:
            The closable fake stream.
        """
        self.subscriptions.append(symbols)
        return self._stream()

    async def _stream(self) -> AsyncGenerator[TickerUpdate]:
        """Yield supplied ticks and expose deterministic wait and cleanup boundaries.

        Yields:
            Synthetic ticker payloads in their supplied order.

        Raises:
            Exception: The deliberately supplied operational failure.
        """
        try:
            for update in self.updates:
                self.yielded += 1
                yield update
            if self.error is not None:
                raise self.error
            if self.wait_after:
                self.waiting.set()
                await self.unblock.wait()
        finally:
            self.cleanup_started.set()
            try:
                if self.wait_cleanup:
                    await self.unblock.wait()
            except asyncio.CancelledError:
                self.cleanup_cancelled = True
                raise
            if self.cleanup_error is not None:
                raise self.cleanup_error
            self.closed = True


@dataclass
class _UnclosableStream:
    """An async iterator intentionally missing the required explicit cleanup method."""

    iterated: bool = False

    def __aiter__(self) -> AsyncIterator[TickerUpdate]:
        """Return the iterator itself.

        Returns:
            This unclosable ticker stream.
        """
        return self

    async def __anext__(self) -> TickerUpdate:
        """Record iteration so rejection before reading can be asserted.

        Returns:
            One synthetic ticker update.
        """
        self.iterated = True
        return _tick()


class _UnclosableClient:
    """Client whose advertised async iterator cannot be safely closed."""

    def __init__(self) -> None:
        """Create the unclosable stream owned by this fake client."""
        self.stream = _UnclosableStream()

    def subscribe_ticks(self, symbols: list[str]) -> AsyncIterator[TickerUpdate]:
        """Return an unsupported stream without reading it.

        Args:
            symbols: Requested native symbols, unused by this deliberately invalid client.

        Returns:
            An iterator without an explicit close operation.
        """
        del symbols
        return self.stream


@dataclass
class _LateClient:
    """Simulate a cooperative deadline being overrun without pretending hard termination."""

    suppress_cancellation: bool
    closed: bool = False

    def subscribe_ticks(self, symbols: list[str]) -> AsyncIterator[TickerUpdate]:
        """Return one late or cancellation-suppressing stream.

        Args:
            symbols: Explicit selected symbols, unused by this deadline witness.

        Returns:
            The deliberately slow fake generator.
        """
        del symbols
        return self._stream()

    async def _stream(self) -> AsyncGenerator[TickerUpdate]:
        """Yield either after swallowed cancellation or before blocking cleanup.

        Yields:
            One synthetic tick at the selected failure boundary.
        """
        try:
            if self.suppress_cancellation:
                try:
                    await asyncio.Event().wait()
                except asyncio.CancelledError:
                    yield _tick()
            else:
                yield _tick()
        finally:
            if not self.suppress_cancellation:
                time.sleep(0.04)
            self.closed = True


@dataclass
class _SwallowedCancellationClient:
    """Deliberately swallow a caller cancellation at an observable collection boundary."""

    phase: str
    waiting: asyncio.Event = field(default_factory=asyncio.Event)
    closed: bool = False

    def subscribe_ticks(self, symbols: list[str]) -> AsyncIterator[TickerUpdate]:
        """Return the selected cancellation witness stream.

        Args:
            symbols: Explicit selected symbols, unused by this boundary witness.

        Returns:
            A generator that deliberately suppresses cancellation once.
        """
        del symbols
        return self._stream()

    async def _swallow_request(self) -> None:
        """Signal the cancellable boundary and deliberately consume its cancellation."""
        self.waiting.set()
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            return

    async def _stream(self) -> AsyncGenerator[TickerUpdate]:
        """Consume caller cancellation while reading, returning, or cleaning up.

        Yields:
            One valid synthetic tick unless cancellation is followed by end-of-stream.
        """
        try:
            if self.phase != "cleanup":
                await self._swallow_request()
            if self.phase == "end":
                return
            yield _tick()
        finally:
            if self.phase == "cleanup":
                await self._swallow_request()
            self.closed = True


@dataclass
class _TwoCancellationClient:
    """Swallow the owned deadline request followed by a distinct caller request."""

    first_cancelled: asyncio.Event = field(default_factory=asyncio.Event)
    closed: bool = False

    def subscribe_ticks(self, symbols: list[str]) -> AsyncIterator[TickerUpdate]:
        """Return a stream exposing two independent cancellation boundaries.

        Args:
            symbols: Explicit selected symbols, unused by this cancellation witness.

        Returns:
            The cancellation-suppressing generator.
        """
        del symbols
        return self._stream()

    async def _stream(self) -> AsyncGenerator[TickerUpdate]:
        """Signal the owned timeout and then swallow the caller cancellation too.

        Yields:
            One valid tick after both cancellation requests were consumed.
        """
        try:
            try:
                await asyncio.Event().wait()
            except asyncio.CancelledError:
                self.first_cancelled.set()
            try:
                await asyncio.Event().wait()
            except asyncio.CancelledError:
                yield _tick()
        finally:
            self.closed = True


async def _collect_after_prior_cancellation(
    client: SelectedSnapshotTickerClient, timeout_seconds: float
) -> SelectedSnapshotBatch:
    """Start collection after a previously handled cancellation left its count intact.

    Args:
        client: Fake client chosen for ordinary completion or owned timeout.
        timeout_seconds: New collection budget, independent of the previous request.

    Returns:
        The collector's result without interpreting the prior count as a new request.
    """
    task = asyncio.current_task()
    assert task is not None
    task.cancel()
    with suppress(asyncio.CancelledError):
        await asyncio.sleep(0)
    assert task.cancelling() == 1
    result = await collect_selected_snapshots(
        client, [_target()], asyncio.get_running_loop().time() + timeout_seconds, SequenceTracker()
    )
    assert task.cancelling() == 1
    return result


@pytest.mark.asyncio
async def test_selected_collection_stops_early_and_preserves_receipt_and_provenance() -> None:
    """Preserve original receipt evidence and stop after the complete selected set.

    Given selected ticks followed by an unrelated update,
    When each requested identity receives its first usable tick,
    Then collection closes immediately with original timestamps and producer sequence IDs.
    """
    first = _target("BTC-EUR", "instrument-b")
    second = _target("ETH-EUR", "instrument-a")
    client = _Client([_tick(), _tick("ETH-EUR"), _tick("EXTRA-EUR")])
    timestamps = iter((_OBSERVED, _OBSERVED + timedelta(seconds=1)))
    tracker = SequenceTracker()
    assert tracker.next_sequence("snapshots") == 1
    result = await collect_selected_snapshots(
        client,
        [second, first, first],
        asyncio.get_running_loop().time() + 1,
        tracker,
        lambda: next(timestamps),
    )
    assert client.subscriptions == [["BTC-EUR", "ETH-EUR"]]
    assert client.closed and client.yielded == 2
    assert not result.timed_out and result.missing_instrument_ids == ()
    assert [snapshot.instrument_public_id for snapshot in result.snapshots] == [
        "instrument-a",
        "instrument-b",
    ]
    assert [snapshot.timestamp for snapshot in result.snapshots] == [
        _OBSERVED + timedelta(seconds=1),
        _OBSERVED,
    ]
    assert [snapshot.sequence_id for snapshot in result.snapshots] == [3, 2]
    assert all(snapshot.session_id == tracker.session_id for snapshot in result.snapshots)
    snapshot = result.snapshots[0]
    assert (snapshot.last_price, snapshot.bid, snapshot.ask) == (2.0, 1.0, 3.0)
    assert (snapshot.bid_volume, snapshot.ask_volume, snapshot.volume_24h) == (3.0, 4.0, 5.0)
    assert (snapshot.vwap_24h, snapshot.low_24h, snapshot.high_24h) == (6.0, 0.5, 7.0)
    assert (snapshot.change_24h, snapshot.spread, snapshot.spread_pct) == (0.25, 2.0, 100.0)


@pytest.mark.asyncio
async def test_selected_collection_ignores_unusable_and_duplicate_ticks() -> None:
    """Keep unusable and duplicate ticks out of the collected price evidence.

    Given wrong-symbol, delayed, invalid-price and duplicate updates,
    When the explicit selected set is collected,
    Then only the first usable selected tick counts and missing identities remain visible.
    """
    client = _Client(
        [
            _tick("OTHER"),
            _tick(last=0.0),
            _tick(last=-1.0),
            _tick(last=float("nan")),
            _tick(last=float("inf")),
            _tick(delayed=True),
            _tick(book=False),
            _tick(last=99.0),
        ]
    )
    result = await collect_selected_snapshots(
        client,
        [_target(), _target("ETH-EUR", "instrument-missing")],
        asyncio.get_running_loop().time() + 1,
        SequenceTracker(),
    )
    assert client.closed
    assert len(result.snapshots) == 1
    assert result.snapshots[0].last_price == 2.0
    assert result.snapshots[0].spread_pct == 0.0
    assert result.snapshots[0].timestamp.utcoffset() == timedelta(0)
    assert result.snapshots[0].sequence_id == 1
    assert result.missing_instrument_ids == ("instrument-missing",)
    assert not result.timed_out


_INVALID_TARGETS: list[list[MarketSnapshotRefreshTargetRow]] = [
    [],
    [_target(exchange="polygon")],
    [_target(exchange="paper")],
    [_target(), _target("ETH-EUR", "instrument-b", "walutomat")],
    [_target(native="*")],
    [_target(native="BTC-*")],
    [_target(native="")],
    [_target(identity="")],
    [_target(native=" BTC-EUR")],
    [{**_target(), "base": ""}],
    [{**_target(), "quote": " "}],
    [_target(), _target(identity="instrument-b")],
    [_target(), _target(native="ETH-EUR")],
    [_target(), {**_target(), "quote": "USD"}],
]


@pytest.mark.asyncio
@pytest.mark.parametrize("targets", _INVALID_TARGETS)
async def test_invalid_targets_refuse_before_subscription(
    targets: list[MarketSnapshotRefreshTargetRow],
) -> None:
    """Reject malformed target mappings before contacting a venue stream.

    Given empty, unsupported, wildcard or ambiguous target metadata,
    When collection starts with that explicit selection,
    Then validation fails before any subscription is requested.
    """
    client = _Client([_tick()])
    with pytest.raises(ValueError):
        await collect_selected_snapshots(
            client, targets, asyncio.get_running_loop().time() + 1, SequenceTracker()
        )
    assert client.subscriptions == []


@pytest.mark.asyncio
@pytest.mark.parametrize("deadline", [float("nan"), float("inf"), float("-inf")])
async def test_nonfinite_deadline_refuses_before_subscription(deadline: float) -> None:
    """Require a finite absolute collection budget before subscribing.

    Given a NaN or infinite deadline,
    When collection starts for an otherwise valid target,
    Then it refuses the deadline and leaves the client unsubscribed.
    """
    client = _Client([_tick()])
    with pytest.raises(ValueError, match="finite absolute deadline"):
        await collect_selected_snapshots(client, [_target()], deadline, SequenceTracker())
    assert client.subscriptions == []


@pytest.mark.asyncio
async def test_expired_deadline_reports_missing_without_subscription() -> None:
    """Report already-expired work without creating a subscription.

    Given an absolute deadline earlier than the current loop clock,
    When collection starts,
    Then the result is timed out with every target missing and no subscription.
    """
    client = _Client([_tick()])
    result = await collect_selected_snapshots(
        client, [_target()], asyncio.get_running_loop().time() - 1, SequenceTracker()
    )
    assert result.timed_out
    assert result.snapshots == () and result.missing_instrument_ids == ("instrument-a",)
    assert client.subscriptions == []


@pytest.mark.asyncio
async def test_own_timeout_retains_partial_evidence_and_closes_iterator() -> None:
    """Retain accepted evidence when the owned deadline interrupts a blocked read.

    Given one accepted target and another whose next tick never arrives,
    When the collection deadline expires,
    Then the iterator closes and the partial batch identifies the missing target.
    """
    client = _Client([_tick()], wait_after=True)
    result = await collect_selected_snapshots(
        client,
        [_target(), _target("ETH-EUR", "instrument-b")],
        asyncio.get_running_loop().time() + 0.02,
        SequenceTracker(),
    )
    assert result.timed_out and client.closed
    assert len(result.snapshots) == 1
    assert result.missing_instrument_ids == ("instrument-b",)


@pytest.mark.asyncio
async def test_iterator_cleanup_uses_the_same_absolute_deadline() -> None:
    """Charge stream cleanup to the original absolute collection deadline.

    Given all selected ticks have arrived but iterator cleanup blocks,
    When the original deadline expires during cleanup,
    Then the batch remains timed out despite having all requested snapshots.
    """
    client = _Client([_tick()], wait_cleanup=True)
    result = await collect_selected_snapshots(
        client, [_target()], asyncio.get_running_loop().time() + 0.02, SequenceTracker()
    )
    assert result.timed_out and result.missing_instrument_ids == ()
    assert len(result.snapshots) == 1
    assert client.cleanup_started.is_set() and client.cleanup_cancelled


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "error", [TimeoutError("transport deadline"), RuntimeError("transport failed")]
)
async def test_operational_failures_propagate(error: Exception) -> None:
    """Preserve transport failures instead of presenting them as deadline results.

    Given a transport exception before the collection deadline,
    When the iterator raises that exception,
    Then the original failure propagates after stream cleanup.
    """
    client = _Client(error=error)
    with pytest.raises(type(error), match=str(error)):
        await collect_selected_snapshots(
            client, [_target()], asyncio.get_running_loop().time() + 1, SequenceTracker()
        )
    assert client.closed


@pytest.mark.asyncio
async def test_operational_cleanup_timeout_propagates() -> None:
    """Distinguish cleanup's operational timeout from the collector's timeout.

    Given a selected tick followed by cleanup's own timeout exception,
    When the iterator closes before the collection deadline,
    Then the cleanup exception propagates as an operational failure.
    """
    client = _Client([_tick()], cleanup_error=TimeoutError("cleanup failure"))
    with pytest.raises(TimeoutError, match="cleanup failure"):
        await collect_selected_snapshots(
            client, [_target()], asyncio.get_running_loop().time() + 1, SequenceTracker()
        )
    assert client.cleanup_started.is_set()


@pytest.mark.asyncio
async def test_cleanup_timeout_is_not_hidden_when_own_deadline_also_expires() -> None:
    """Keep a concurrent cleanup failure visible when the collection deadline expires.

    Given cleanup raises its own timeout while handling deadline cancellation,
    When both failures occur during the same collection,
    Then the real cleanup exception is propagated rather than hidden in a partial result.
    """
    client = _Client(wait_after=True, cleanup_error=TimeoutError("cleanup during cancellation"))
    with pytest.raises(TimeoutError, match="cleanup during cancellation"):
        await collect_selected_snapshots(
            client, [_target()], asyncio.get_running_loop().time() + 0.02, SequenceTracker()
        )
    assert client.cleanup_started.is_set()


@pytest.mark.asyncio
async def test_external_cancellation_propagates_after_cleanup() -> None:
    """Propagate the caller's cancellation after closing the blocked iterator.

    Given collection is waiting for a target tick,
    When its owning task is externally cancelled,
    Then cancellation propagates after cleanup and no partial success is returned.
    """
    client = _Client(wait_after=True)
    task = asyncio.create_task(
        collect_selected_snapshots(
            client, [_target()], asyncio.get_running_loop().time() + 1, SequenceTracker()
        )
    )
    await client.waiting.wait()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert client.closed


@pytest.mark.asyncio
async def test_unclosable_stream_refuses_before_iteration() -> None:
    """Require an explicit async cleanup boundary before consuming a stream.

    Given a client returning an iterator without an async close method,
    When collection validates that stream,
    Then it rejects the iterator before reading any ticker update.
    """
    client = _UnclosableClient()
    with pytest.raises(TypeError, match="explicit async closure"):
        await collect_selected_snapshots(
            client, [_target()], asyncio.get_running_loop().time() + 1, SequenceTracker()
        )
    assert not client.stream.iterated


@pytest.mark.asyncio
async def test_swallowed_deadline_cancellation_never_accepts_the_late_tick() -> None:
    """Reject ticks yielded after a generator suppresses the owned timeout cancellation.

    Given a generator that swallows the deadline cancellation,
    When it resumes by yielding a late valid tick,
    Then the result stays timed out and the late tick cannot satisfy the missing target.
    """
    client = _LateClient(suppress_cancellation=True)
    result = await collect_selected_snapshots(
        client, [_target()], asyncio.get_running_loop().time() + 0.02, SequenceTracker()
    )
    assert client.closed and result.timed_out
    assert result.snapshots == () and result.missing_instrument_ids == ("instrument-a",)


@pytest.mark.asyncio
async def test_blocking_cleanup_overrun_never_reports_success() -> None:
    """Detect deadline overrun even when synchronous cleanup delays the loop timer.

    Given iterator cleanup briefly blocks the event loop,
    When cleanup finishes after the absolute deadline,
    Then the completed evidence is still reported with a timeout result.
    """
    client = _LateClient(suppress_cancellation=False)
    result = await collect_selected_snapshots(
        client, [_target()], asyncio.get_running_loop().time() + 0.02, SequenceTracker()
    )
    assert client.closed and result.timed_out
    assert len(result.snapshots) == 1 and result.missing_instrument_ids == ()


@pytest.mark.asyncio
@pytest.mark.parametrize("phase", ["next", "end", "cleanup"])
async def test_swallowed_external_cancellation_still_propagates(phase: str) -> None:
    """Preserve caller cancellation ownership across a producer that swallows its exception.

    Given a producer suppressing cancellation during reading, stream completion or cleanup,
    When the caller cancels and the producer resumes,
    Then collection still propagates cancellation and never returns a successful batch.
    """
    client = _SwallowedCancellationClient(phase)
    tracker = SequenceTracker()
    task = asyncio.create_task(
        collect_selected_snapshots(
            client, [_target()], asyncio.get_running_loop().time() + 2, tracker
        )
    )
    await client.waiting.wait()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert client.closed and task.cancelled()
    assert tracker.next_sequence("snapshots") == (2 if phase == "cleanup" else 1)


@pytest.mark.asyncio
@pytest.mark.parametrize("own_timeout", [False, True])
async def test_preexisting_cancellation_count_is_not_a_new_caller_request(
    own_timeout: bool,
) -> None:
    """Exclude pre-existing handled requests from the collector's cancellation ownership.

    Given an earlier handled cancellation left a nonzero task request count,
    When a new collection completes or reaches its own timeout,
    Then the old count is preserved and is not mistaken for a new caller request.
    """
    client = _LateClient(True) if own_timeout else _Client([_tick()])
    result = await asyncio.create_task(
        _collect_after_prior_cancellation(client, 0.02 if own_timeout else 1.0)
    )
    assert client.closed and result.timed_out is own_timeout


@pytest.mark.asyncio
async def test_caller_cancellation_is_not_hidden_by_an_already_expired_owned_timeout() -> None:
    """Distinguish a new caller request from the already-expired owned timeout.

    Given a producer that swallows the timeout request and then a separate caller request,
    When that producer resumes with a valid tick,
    Then the caller cancellation propagates instead of becoming an ordinary timeout batch.
    """
    client = _TwoCancellationClient()
    task = asyncio.create_task(
        collect_selected_snapshots(
            client, [_target()], asyncio.get_running_loop().time() + 0.02, SequenceTracker()
        )
    )
    await client.first_cancelled.wait()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert client.closed and task.cancelled()


@pytest.mark.asyncio
async def test_collection_without_an_owning_task_refuses(monkeypatch: pytest.MonkeyPatch) -> None:
    """Require an owning task before establishing cancellation accounting.

    Given no current task is available to own collection cancellation,
    When collection begins for valid targets,
    Then it refuses before requesting any subscription.
    """
    client = _Client([_tick()])
    with monkeypatch.context() as patcher:
        patcher.setattr(asyncio, "current_task", lambda: None)
        with pytest.raises(RuntimeError, match="owning asyncio task"):
            await collect_selected_snapshots(
                client, [_target()], asyncio.get_running_loop().time() + 1, SequenceTracker()
            )
    assert client.subscriptions == []
