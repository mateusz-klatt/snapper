"""Collect an explicit venue snapshot set within a caller-owned cooperative deadline.

This component neither connects clients nor persists rows. Its deadline covers
iteration and iterator cleanup. A generator or driver that blocks the event loop
or suppresses cancellation still requires the caller's external process bound.
"""

import asyncio
import math
from collections.abc import AsyncIterator
from collections.abc import Callable
from collections.abc import Sequence
from contextlib import aclosing
from dataclasses import dataclass
from datetime import UTC
from datetime import datetime
from typing import Protocol
from typing import runtime_checkable

from snapper.core.types import ExchangeEnum
from snapper.data.models import MarketSnapshot
from snapper.data.repository_types import MarketSnapshotRefreshTargetRow
from snapper.infrastructure.exchanges.contracts import TickerUpdate
from snapper.messaging.infrastructure.publisher import SequenceTracker

_SUPPORTED_VENUES = frozenset(
    {
        ExchangeEnum.KRAKEN.value,
        ExchangeEnum.KRAKEN_FUTURES.value,
        ExchangeEnum.KRAKEN_EQUITIES.value,
        ExchangeEnum.WALUTOMAT.value,
    }
)


class SelectedSnapshotTickerClient(Protocol):
    """Minimal existing-client surface needed for explicit ticker collection."""

    def subscribe_ticks(self, symbols: list[str]) -> AsyncIterator[TickerUpdate]:
        """Subscribe to exactly the requested venue-native symbols.

        Args:
            symbols: Explicit nonempty native symbols, without wildcards.

        Returns:
            A stream that the collector additionally verifies supports explicit closure.
        """
        ...


@runtime_checkable
class ClosableTickerStream(Protocol):
    """Runtime-verifiable async iteration and explicit cleanup boundary."""

    def __aiter__(self) -> AsyncIterator[TickerUpdate]:
        """Return the ticker iterator.

        Returns:
            An async iterator of typed ticker updates.
        """
        ...

    async def __anext__(self) -> TickerUpdate:
        """Read the next ticker update.

        Returns:
            The next observed ticker update.
        """
        ...

    async def aclose(self) -> None:
        """Close the stream and finish its cooperative cleanup."""
        ...


@dataclass(frozen=True)
class SelectedSnapshotBatch:
    """Collected evidence and explicitly missing identities for one venue."""

    snapshots: tuple[MarketSnapshot, ...]
    missing_instrument_ids: tuple[str, ...]
    timed_out: bool


@dataclass
class _CancellationWatch:
    """Distinguish caller cancellation requests from this call's own timeout request."""

    task: asyncio.Task[object]
    initial_requests: int

    def raise_if_requested(self, timeout: asyncio.Timeout | None = None) -> None:
        """Propagate new caller cancellation even if a producer swallowed its exception.

        Args:
            timeout: Active timeout context, or null after its exit removed its request.

        Raises:
            asyncio.CancelledError: If cancellation requests exceed the entry baseline
                and the one request owned by an expired active timeout context.
        """
        owned_request = int(timeout is not None and timeout.expired())
        if self.task.cancelling() > self.initial_requests + owned_request:
            raise asyncio.CancelledError("Snapshot collection cancelled by caller")


def _watch_cancellation() -> _CancellationWatch:
    """Capture the owning task's cancellation baseline before the first collection await.

    Returns:
        A watch that does not mistake pre-existing cancellation counts for new requests.

    Raises:
        RuntimeError: If collection is driven without an owning asyncio task.
    """
    task = asyncio.current_task()
    if task is None:
        raise RuntimeError("Snapshot collection requires an owning asyncio task")
    return _CancellationWatch(task, task.cancelling())


@dataclass
class _CollectionState:
    """One call's mutable pending set and original-receipt snapshot evidence."""

    pending: dict[str, MarketSnapshotRefreshTargetRow]
    snapshots: dict[str, MarketSnapshot]
    tracker: SequenceTracker
    received_at: Callable[[], datetime]
    cancellation: _CancellationWatch


def _utc_now() -> datetime:
    """Read the UTC receive clock immediately after an iterator yields.

    Returns:
        The current aware UTC timestamp.
    """
    return datetime.now(UTC)


def _validated_targets(
    targets: Sequence[MarketSnapshotRefreshTargetRow],
) -> dict[str, MarketSnapshotRefreshTargetRow]:
    """Validate a single supported venue and unambiguous explicit native mapping.

    Args:
        targets: Resolved metadata selected by the caller.

    Returns:
        Native-symbol-ordered targets with exact duplicate rows deduplicated.

    Raises:
        ValueError: If targets are empty, unsupported, malformed or ambiguous.
    """
    if not targets:
        raise ValueError("Snapshot collection requires explicit targets")
    venues = {target["exchange"] for target in targets}
    if len(venues) != 1 or not venues.issubset(_SUPPORTED_VENUES):
        raise ValueError("Snapshot collection requires one supported venue")
    by_native: dict[str, MarketSnapshotRefreshTargetRow] = {}
    by_instrument: dict[str, str] = {}
    for target in targets:
        _add_target(target, by_native, by_instrument)
    return dict(sorted(by_native.items()))


def _add_target(
    target: MarketSnapshotRefreshTargetRow,
    by_native: dict[str, MarketSnapshotRefreshTargetRow],
    by_instrument: dict[str, str],
) -> None:
    """Reject empty or ambiguous identities before any subscription exists.

    Args:
        target: One resolved metadata row.
        by_native: Targets already indexed by their exact native symbol.
        by_instrument: Reverse mapping needed to reject two names for one identity.

    Raises:
        ValueError: If the requested mapping is malformed or conflicts with prior rows.
    """
    instrument_id = target["instrument_public_id"]
    native = target["native_symbol"]
    identity_values = (instrument_id, native, target["base"], target["quote"])
    if any(not value or value != value.strip() for value in identity_values) or "*" in native:
        raise ValueError("Snapshot targets require nonblank explicit identities")
    existing = by_native.get(native)
    if existing is not None and existing != target:
        raise ValueError("Native snapshot symbol maps to conflicting target metadata")
    previous_native = by_instrument.get(instrument_id)
    if previous_native is not None and previous_native != native:
        raise ValueError("Snapshot instrument maps to conflicting native symbols")
    by_native[native] = target
    by_instrument[instrument_id] = native


def _usable_tick(ticker: TickerUpdate) -> bool:
    """Require an undelayed positive finite executable price estimate.

    Args:
        ticker: Typed venue update received by this collection call.

    Returns:
        Whether the tick can supply current last-price evidence.
    """
    return not ticker.is_delayed and math.isfinite(ticker.last) and ticker.last > 0


def _snapshot(
    ticker: TickerUpdate,
    target: MarketSnapshotRefreshTargetRow,
    observed_at: datetime,
    tracker: SequenceTracker,
) -> MarketSnapshot:
    """Build snapshot evidence at the original receive time with producer provenance.

    Args:
        ticker: Valid selected venue tick.
        target: Its resolved instrument identity.
        observed_at: Timestamp captured immediately when this tick was yielded.
        tracker: Caller-owned session and monotonic snapshot sequence.

    Returns:
        An unpersisted snapshot carrying the venue payload and receive timestamp.
    """
    spread = ticker.ask - ticker.bid
    mid = ticker.ask / 2 + ticker.bid / 2
    return MarketSnapshot(
        instrument_public_id=target["instrument_public_id"],
        bid=ticker.bid,
        bid_volume=ticker.bid_qty,
        ask=ticker.ask,
        ask_volume=ticker.ask_qty,
        last_price=ticker.last,
        volume_24h=ticker.volume,
        vwap_24h=ticker.vwap,
        low_24h=ticker.low,
        high_24h=ticker.high,
        change_24h=ticker.change,
        spread=spread,
        spread_pct=spread / mid * 100 if mid > 0 else 0.0,
        timestamp=observed_at,
        session_id=tracker.session_id,
        sequence_id=tracker.next_sequence("snapshots"),
    )


async def _consume(
    stream: ClosableTickerStream,
    state: _CollectionState,
    deadline: float,
    timeout: asyncio.Timeout,
) -> None:
    """Accept the first valid selected tick while allowing the absolute timer to run.

    Args:
        stream: Verified closable venue iterator.
        state: Pending identities, accumulated snapshots and receipt clock.
        deadline: Absolute shared deadline, including cancellation-suppressing generators.
        timeout: This call's timeout, whose cancellation must not be mistaken for the caller's.
    """
    async for ticker in stream:
        observed_at = state.received_at()
        state.cancellation.raise_if_requested(timeout)
        await asyncio.sleep(0)
        if asyncio.get_running_loop().time() >= deadline:
            return
        target = state.pending.get(ticker.symbol)
        if target is None or not _usable_tick(ticker):
            continue
        instrument_id = target["instrument_public_id"]
        state.snapshots[instrument_id] = _snapshot(ticker, target, observed_at, state.tracker)
        del state.pending[ticker.symbol]
        if not state.pending:
            return


def _batch(state: _CollectionState, timed_out: bool) -> SelectedSnapshotBatch:
    """Freeze a deterministic evidence result without inventing missing observations.

    Args:
        state: The call's completed or partially completed collection state.
        timed_out: Whether this call's own absolute timeout expired.

    Returns:
        Sorted collected snapshots, missing identities and timeout status.
    """
    return SelectedSnapshotBatch(
        snapshots=tuple(state.snapshots[key] for key in sorted(state.snapshots)),
        missing_instrument_ids=tuple(
            sorted(target["instrument_public_id"] for target in state.pending.values())
        ),
        timed_out=timed_out,
    )


async def collect_selected_snapshots(
    client: SelectedSnapshotTickerClient,
    targets: Sequence[MarketSnapshotRefreshTargetRow],
    deadline: float,
    tracker: SequenceTracker,
    received_at: Callable[[], datetime] | None = None,
) -> SelectedSnapshotBatch:
    """Collect one venue's explicit set under a caller-owned absolute loop deadline.

    The bound is cooperative and includes stream closure. A cancellation-resistant
    generator or blocking driver requires the external runner's process termination
    bound; this function does not start or supervise that process.

    Args:
        client: Existing venue client; connection lifecycle belongs to the caller.
        targets: Explicit single-venue resolved identities without wildcard symbols.
        deadline: Absolute finite deadline in the running event loop's time domain.
        tracker: Producer session and snapshot sequence shared by the caller.
        received_at: Optional UTC receipt clock for deterministic evidence tests.

    Returns:
        Collected evidence, missing identities and this deadline's expiration status.

    Raises:
        ValueError: If target identities or the deadline are invalid.
        TypeError: If the client does not provide an explicitly closable iterator.
        TimeoutError: If an operational timeout occurs without this deadline expiring.
        asyncio.CancelledError: If the caller cancels this call, including a request
            swallowed by the ticker iterator or its cleanup.
    """
    state = _CollectionState(
        _validated_targets(targets), {}, tracker, received_at or _utc_now, _watch_cancellation()
    )
    if not math.isfinite(deadline):
        raise ValueError("Snapshot collection requires a finite absolute deadline")
    if asyncio.get_running_loop().time() >= deadline:
        return _batch(state, timed_out=True)
    stream = client.subscribe_ticks(list(state.pending))
    if not isinstance(stream, ClosableTickerStream):
        raise TypeError("Snapshot ticker stream must support explicit async closure")
    timeout = asyncio.timeout_at(deadline)
    operational_timeout: TimeoutError | None = None
    try:
        async with timeout:
            try:
                async with aclosing(stream):
                    await _consume(stream, state, deadline, timeout)
                    state.cancellation.raise_if_requested(timeout)
                state.cancellation.raise_if_requested(timeout)
            except TimeoutError as error:
                operational_timeout = error
                raise
    except TimeoutError:
        state.cancellation.raise_if_requested()
        if operational_timeout is not None or not timeout.expired():
            raise
        return _batch(state, timed_out=True)
    state.cancellation.raise_if_requested()
    return _batch(
        state, timed_out=timeout.expired() or asyncio.get_running_loop().time() >= deadline
    )
