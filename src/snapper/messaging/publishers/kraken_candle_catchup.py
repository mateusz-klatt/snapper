"""Bounded native history reconciliation independent of live market events."""

import asyncio
import contextlib
import math
from collections.abc import Awaitable
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC
from datetime import datetime
from datetime import timedelta
from time import monotonic

from loguru import logger

from snapper.data.repository import Repository
from snapper.data.repository_types import CandleRow
from snapper.data.repository_types import CandleUpsertRow
from snapper.infrastructure.exchanges.contracts import CandleUpdate
from snapper.infrastructure.exchanges.implementations.kraken import KrakenExchangeClient
from snapper.messaging.infrastructure.publisher import SequenceTracker
from snapper.messaging.publishers.native_candle_finalizer import is_complete_historical_minute
from snapper.messaging.publishers.native_candle_finalizer import window_seconds


class CatchupDeferredError(RuntimeError):
    """Distinguish cooperative live-writer admission deferral from a successful replay."""


def _now() -> datetime:
    """Return the actual UTC acquisition/write clock.

    Returns:
        Current timezone-aware UTC instant.
    """
    return datetime.now(UTC)


@dataclass(frozen=True)
class CatchupPolicy:
    """Finite history, symbol, wall-time and scheduling budgets for one worker."""

    history_minutes: int = 120
    priority_history_minutes: int = 719
    symbols_per_pass: int = 3
    attempt_seconds: float = 5.0
    pass_seconds: float = 20.0
    interval_seconds: float = 60.0
    minimum_spacing_seconds: float = 5.0
    priority_symbols: tuple[str, ...] = ("XRP-EUR", "EUR-USD", "BTC-EUR", "BTC-USD", "ETH-USD")
    sweep_spacing_seconds: float = 3.0

    def __post_init__(self) -> None:
        """Reject policies that would remove a resource or pacing bound."""
        values = (
            self.history_minutes,
            self.priority_history_minutes,
            self.symbols_per_pass,
            self.attempt_seconds,
            self.pass_seconds,
            self.interval_seconds,
            self.minimum_spacing_seconds,
            self.sweep_spacing_seconds,
        )
        if any(not math.isfinite(value) or value <= 0 for value in values):
            raise ValueError("Kraken catchup budgets must be positive and finite")
        if (
            not self.history_minutes <= self.priority_history_minutes <= 719
            or self.minimum_spacing_seconds > self.interval_seconds
        ):
            raise ValueError("Kraken catchup exceeds its history or scheduling bound")
        if self.priority_symbols and self.symbols_per_pass < 2:
            raise ValueError("Kraken priority catchup must retain a fair symbol slot")
        if any(not symbol.strip() for symbol in self.priority_symbols):
            raise ValueError("Kraken catchup priority symbols must be canonical names")


@dataclass(frozen=True)
class CatchupCallbacks:
    """Publisher callbacks for identity, persistence and repairs after acknowledged changes."""

    symbols: Callable[[], list[str]]
    resolve: Callable[[str], Awaitable[str | None]]
    committed: Callable[[str, datetime, datetime, frozenset[datetime]], Awaitable[None]]
    active: Callable[[], bool]
    tracker: SequenceTracker
    clock: Callable[[], datetime] = _now
    insert_missing: Callable[[list[CandleUpsertRow]], Awaitable[list[CandleUpsertRow]]] | None = (
        None
    )
    correct_zero: Callable[[list[CandleUpsertRow]], Awaitable[list[CandleUpsertRow]]] | None = None


@dataclass(frozen=True)
class CatchupRepairWindow:
    """One closed higher-timeframe window proved by its entire minute grid."""

    timeframe: str
    begin: datetime
    minutes: frozenset[datetime]


def _floor(value: datetime, seconds: int) -> datetime:
    """Floor an instant to its canonical UTC business boundary.

    Args:
        value: Timezone-aware business instant.
        seconds: Positive interval width.

    Returns:
        UTC interval start.
    """
    return datetime.fromtimestamp(int(value.timestamp()) // seconds * seconds, UTC)


def _failure_label(error: Exception) -> str:
    """Name a failed attempt, adding the reason only for a validation refusal.

    Validation refusals carry fixed wording about the product or its response.
    Other failures are named by class alone, because transport errors can quote
    arbitrary remote content.

    Args:
        error: The failure that left a symbol or pass unresolved.

    Returns:
        The exception class, followed by its message for a ValueError.
    """
    name = type(error).__name__
    return f"{name}: {error}" if isinstance(error, ValueError) else name


def repair_read_start(start: datetime, timeframes: list[str]) -> datetime:
    """Include full enclosing windows when reconciling committed minute inputs.

    Args:
        start: First minute considered by the catchup pass.
        timeframes: Supported higher timeframes selected by the publisher.

    Returns:
        Earliest required higher-timeframe boundary, or the unchanged start.
    """
    return min((_floor(start, window_seconds(tf)) for tf in timeframes), default=start)


def complete_repair_windows(
    rows: list[CandleRow], start: datetime, end: datetime, timeframes: list[str]
) -> list[CatchupRepairWindow]:
    """Prove contiguous full minute grids without filling any absent minute.

    Args:
        rows: Persisted minute versions from one committed read cut.
        start: First minute reconciled by the native history pass.
        end: Exclusive closed-minute horizon of that pass.
        timeframes: Supported higher timeframes to reconcile.

    Returns:
        Closed windows whose every canonical minute is present exactly once.
    """
    valid: set[datetime] = set()
    invalid: set[datetime] = set()
    for row in rows:
        stamp = row["open_at"]
        if stamp in valid or not is_complete_historical_minute(row, end):
            invalid.add(stamp)
        valid.add(stamp)
    valid.difference_update(invalid)
    result: list[CatchupRepairWindow] = []
    for timeframe in timeframes:
        width = window_seconds(timeframe)
        begin = _floor(start, width)
        while begin + timedelta(seconds=width) <= end:
            minutes = frozenset(begin + timedelta(minutes=i) for i in range(width // 60))
            if minutes.issubset(valid):
                result.append(CatchupRepairWindow(timeframe, begin, minutes))
            begin += timedelta(seconds=width)
    return result


class KrakenCandleCatchup:
    """Reconcile finite native history fairly while live ingestion keeps running."""

    def __init__(
        self,
        repository: Repository,
        client: KrakenExchangeClient,
        callbacks: CatchupCallbacks,
        policy: CatchupPolicy = CatchupPolicy(),
    ) -> None:
        """Bind the publisher's routed REST transport, candle repository, callbacks and policy.

        Args:
            repository: Canonical candle store that inserts absent minutes and
                corrects disproved synthesized zeros.
            client: Existing publisher client owning routed public REST transport.
            callbacks: Publisher identity, provenance and repair boundaries.
            policy: Finite per-pass and scheduling budgets.
        """
        self._repository = repository
        self._client = client
        self._callbacks = callbacks
        self._policy = policy
        self._priority_cursor = 0
        self._fair_cursor = 0
        self._priority_size = 0
        self._fair_size = 0
        self._initial = True
        self._requested = asyncio.Event()
        self._sweep_requested = True

    @staticmethod
    def _rotate(symbols: list[str], cursor: int, count: int) -> list[str]:
        """Select a bounded pool slice without advancing unattempted symbols."""
        return [symbols[(cursor + i) % len(symbols)] for i in range(count)]

    def _select_symbols(self, symbols: list[str]) -> list[tuple[str, bool]]:
        """Reserve fair work without increasing the fixed request budget."""
        allowed = set(symbols)
        priority = list(dict.fromkeys(s for s in self._policy.priority_symbols if s in allowed))
        fair = sorted(allowed.difference(priority))
        self._priority_size, self._fair_size = len(priority), len(fair)
        count = min(self._policy.symbols_per_pass, len(allowed))
        if self._initial or not priority or not fair:
            priority_count = min(len(priority), count)
        else:
            priority_count = min(len(priority), count - 1)
        fair_count = min(len(fair), count - priority_count)
        priority_count = min(len(priority), count - fair_count)
        priority_work = [
            (s, True) for s in self._rotate(priority, self._priority_cursor, priority_count)
        ]
        fair_work = [(s, False) for s in self._rotate(fair, self._fair_cursor, fair_count)]
        if self._initial:
            return priority_work + fair_work
        return fair_work[:1] + priority_work + fair_work[1:]

    def _record_attempt(self, priority: bool) -> None:
        """Advance only the selected pool after a symbol actually starts work."""
        self._initial = False
        if priority:
            self._priority_cursor = (self._priority_cursor + 1) % self._priority_size
        else:
            self._fair_cursor = (self._fair_cursor + 1) % self._fair_size

    def request(self) -> None:
        """Coalesce reconnect notifications into one full roster sweep."""
        self._sweep_requested = True
        self._requested.set()

    async def run(self) -> None:
        """Supervise bounded passes without blocking live consumer startup.

        Raises:
            asyncio.CancelledError: Propagated so publisher shutdown joins this worker.
        """
        await asyncio.sleep(0)
        while self._callbacks.active():
            self._requested.clear()
            sweep = self._sweep_requested
            self._sweep_requested = False
            try:
                await (self.run_sweep() if sweep else self.run_pass())
            except Exception as error:
                logger.warning("Kraken candle catchup pass unresolved: {}", _failure_label(error))
            await asyncio.sleep(self._policy.minimum_spacing_seconds)
            with contextlib.suppress(TimeoutError):
                await asyncio.wait_for(
                    self._requested.wait(),
                    self._policy.interval_seconds - self._policy.minimum_spacing_seconds,
                )

    async def run_sweep(self) -> None:
        """Run a regular pass first and between paced steps of a full roster sweep.

        Regular passes reach only a few symbols per minute, so most symbols of a
        large roster would not be revisited within their history window after a
        restart. The sweep makes one REST request per symbol, for that symbol's
        window ending at the latest closed minute, and so attempts to recover
        minutes missed while the publisher was down or reconnecting. A reconnect
        notification during the sweep schedules another sweep, so symbols
        already visited are checked again for that connection break.

        A full sweep takes over an hour, so the priority symbols cannot wait
        for it: a regular pass runs before the first sweep request and again
        whenever the regular pass interval has elapsed, which also retries a
        failed priority attempt. All requests remain serial.
        """
        next_pass = await self._sweep_pass()
        for symbol in sorted(set(self._callbacks.symbols())):
            if not self._callbacks.active():
                return
            if monotonic() >= next_pass:
                next_pass = await self._sweep_pass()
                if not self._callbacks.active():
                    return
            end = _floor(self._callbacks.clock(), 60)
            start = self._history_start(symbol, end)
            try:
                async with asyncio.timeout(self._policy.attempt_seconds):
                    await self._reconcile_symbol(symbol, start, end)
            except CatchupDeferredError as error:
                logger.info(
                    "Kraken candle catchup sweep deferred: symbol={} reason={}", symbol, error
                )
            except Exception as error:
                logger.warning(
                    "Kraken candle catchup sweep unresolved: symbol={} error={}",
                    symbol,
                    _failure_label(error),
                )
            await asyncio.sleep(self._policy.sweep_spacing_seconds)

    async def _sweep_pass(self) -> float:
        """Run one regular pass inside a sweep and return the time the next one is due.

        A failed pass is logged and the sweep continues; the pause afterwards
        keeps the sweep's pacing for live writes.

        Returns:
            Monotonic time at which the sweep should run its next regular pass.
        """
        try:
            await self.run_pass()
        except Exception as error:
            logger.warning("Kraken candle catchup pass unresolved: {}", _failure_label(error))
        next_pass = monotonic() + self._policy.interval_seconds
        await asyncio.sleep(self._policy.sweep_spacing_seconds)
        return next_pass

    def _history_start(self, symbol: str, end: datetime) -> datetime:
        """Reach back further for priority symbols, still within one native response.

        Args:
            symbol: Canonical native symbol about to be reconciled.
            end: Exclusive UTC end of the requested closed-minute window.

        Returns:
            Inclusive start of the requested history window.
        """
        if symbol in self._policy.priority_symbols:
            return end - timedelta(minutes=self._policy.priority_history_minutes)
        return end - timedelta(minutes=self._policy.history_minutes)

    async def run_pass(self) -> None:
        """Visit a bounded fair slice of persisted symbols without inline retries."""
        symbols = sorted(set(self._callbacks.symbols()))
        if not symbols:
            return
        selected = self._select_symbols(symbols)
        end = _floor(self._callbacks.clock(), 60)
        async with asyncio.timeout(self._policy.pass_seconds):
            for symbol, priority in selected:
                if not self._callbacks.active():
                    return
                self._record_attempt(priority)
                try:
                    async with asyncio.timeout(self._policy.attempt_seconds):
                        await self._reconcile_symbol(symbol, self._history_start(symbol, end), end)
                except CatchupDeferredError as error:
                    logger.info(
                        "Kraken candle catchup deferred: symbol={} reason={}", symbol, error
                    )
                except Exception as error:
                    logger.warning(
                        "Kraken candle catchup symbol unresolved: symbol={} error={}",
                        symbol,
                        _failure_label(error),
                    )

    async def _reconcile_symbol(self, symbol: str, start: datetime, end: datetime) -> None:
        """Reconcile native minutes and schedule repairs only when a change commits.

        The first repository call validates the whole response; if any row is
        invalid, the symbol is refused before any chunk commits. Each chunk of
        at most 100 rows then inserts its absent minutes and afterwards
        corrects its disproved zeros, in two independent commits. Chunks run
        newest minutes first, so an attempt deadline or a deferral leaves the
        oldest minutes for a later pass rather than the newest ones, which
        valuation reads first. Each commit counts as soon as it is
        acknowledged, so repairs still follow when a later step fails or
        defers; they receive every changed minute, so a repair that already
        read a corrected minute's old version runs again.

        Args:
            symbol: Canonical native symbol selected by persistence policy.
            start: Inclusive UTC start of the requested history window.
            end: Exclusive UTC end of the requested closed-minute window.

        Raises:
            ValueError: When the persisted instrument has no canonical identity.
            Exception: Propagates acquisition, commit or repair-read failures.
        """
        instrument = await self._callbacks.resolve(symbol)
        if instrument is None:
            raise ValueError("Kraken catchup instrument identity is unresolved")
        candles = await self._client.get_closed_1m_candles(symbol, start, end)
        if symbol not in self._callbacks.symbols():
            raise ValueError("Kraken catchup symbol left the permitted persistence roster")
        author = self._callbacks.clock()
        rows = [self._row(candle, instrument, author) for candle in candles]
        await self._repository.filter_missing_candles(rows)
        changed: set[datetime] = set()
        insert = self._callbacks.insert_missing or self._repository.insert_missing_candles
        correct = self._callbacks.correct_zero or self._repository.correct_false_zero_candles
        newest_first = sorted(rows, key=lambda row: row["open_at"], reverse=True)
        try:
            for offset in range(0, len(newest_first), 100):
                chunk = newest_first[offset : offset + 100]
                changed.update(row["open_at"] for row in await insert(chunk))
                changed.update(row["open_at"] for row in await correct(chunk))
                await asyncio.sleep(0)
        finally:
            if changed:
                await self._callbacks.committed(symbol, start, end, frozenset(changed))

    def _row(self, candle: CandleUpdate, instrument: str, author: datetime) -> CandleUpsertRow:
        """Preserve venue fields with actual knowledge time and producer provenance.

        Args:
            candle: Full native closed-minute observation.
            instrument: Resolved canonical instrument identity.
            author: Actual time the native response was learned.

        Returns:
            A native complete row for insertion or a false-zero correction.
        """
        return {
            "instrument_public_id": instrument,
            "timeframe": "1m",
            "open_at": candle.interval_begin,
            "timestamp": author,
            "session_id": self._callbacks.tracker.session_id,
            "sequence_id": self._callbacks.tracker.next_sequence("candles.catchup"),
            "open": candle.open,
            "high": candle.high,
            "low": candle.low,
            "close": candle.close,
            "volume": candle.volume,
            "vwap": candle.vwap,
            "trades": candle.trades,
            "source": "native",
            "complete": True,
        }
