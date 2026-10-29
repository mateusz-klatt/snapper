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

from loguru import logger

from snapper.data.repository import Repository
from snapper.data.repository_types import CandleRow
from snapper.data.repository_types import CandleUpsertRow
from snapper.infrastructure.exchanges.contracts import CandleUpdate
from snapper.infrastructure.exchanges.implementations.kraken import KrakenExchangeClient
from snapper.messaging.infrastructure.publisher import SequenceTracker
from snapper.messaging.publishers.native_candle_finalizer import window_seconds


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
    symbols_per_pass: int = 3
    attempt_seconds: float = 5.0
    pass_seconds: float = 20.0
    interval_seconds: float = 60.0
    minimum_spacing_seconds: float = 5.0
    priority_symbols: tuple[str, ...] = ("BTC-USD", "ETH-USD", "EUR-USD")

    def __post_init__(self) -> None:
        """Reject policies that would remove a resource or pacing bound."""
        values = (
            self.history_minutes,
            self.symbols_per_pass,
            self.attempt_seconds,
            self.pass_seconds,
            self.interval_seconds,
            self.minimum_spacing_seconds,
        )
        if any(not math.isfinite(value) or value <= 0 for value in values):
            raise ValueError("Kraken catchup budgets must be positive and finite")
        if self.history_minutes > 719 or self.minimum_spacing_seconds > self.interval_seconds:
            raise ValueError("Kraken catchup exceeds its history or scheduling bound")
        if self.priority_symbols and self.symbols_per_pass < 2:
            raise ValueError("Kraken priority catchup must retain a fair symbol slot")
        if any(not symbol.strip() for symbol in self.priority_symbols):
            raise ValueError("Kraken catchup priority symbols must be canonical names")


@dataclass(frozen=True)
class CatchupCallbacks:
    """Publisher-owned identity, scheduling and committed repair boundaries."""

    symbols: Callable[[], list[str]]
    resolve: Callable[[str], Awaitable[str | None]]
    committed: Callable[[str, datetime, datetime], Awaitable[None]]
    active: Callable[[], bool]
    tracker: SequenceTracker
    clock: Callable[[], datetime] = _now


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


def repair_read_start(start: datetime, timeframes: list[str]) -> datetime:
    """Include full enclosing windows when reconciling committed minute inputs.

    Args:
        start: First minute considered by the catchup pass.
        timeframes: Supported higher timeframes selected by the publisher.

    Returns:
        Earliest required higher-timeframe boundary, or the unchanged start.
    """
    return min((_floor(start, window_seconds(tf)) for tf in timeframes), default=start)


def _eligible_minute(row: CandleRow) -> bool:
    """Refuse incomplete, non-finite or malformed durable minute evidence.

    Args:
        row: Actual persisted candle observation.

    Returns:
        Whether all required native business fields and boundaries are usable.
    """
    values = (row["open"], row["high"], row["low"], row["close"], row["volume"], row["vwap"])
    if any(value is None or not math.isfinite(value) for value in values):
        return False
    trades = row["trades"]
    return (
        row["complete"]
        and row["timeframe"] == "1m"
        and not (row["open_at"].second or row["open_at"].microsecond)
        and type(trades) is int
        and trades >= 0
        and row["volume"] >= 0
        and row["vwap"] is not None
        and row["vwap"] >= 0
        and row["low"] > 0
        and row["low"] <= min(row["open"], row["close"])
        and row["high"] >= max(row["open"], row["close"], row["low"])
    )


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
        if stamp in valid or not _eligible_minute(row):
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
        """Bind existing routed transport and a committed missing-only repository.

        Args:
            repository: Canonical candle store with atomic missing-only insertion.
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
        """Coalesce reconnect notifications into one bounded reconciliation pass."""
        self._requested.set()

    async def run(self) -> None:
        """Supervise bounded passes without blocking live consumer startup.

        Raises:
            asyncio.CancelledError: Propagated so publisher shutdown joins this worker.
        """
        await asyncio.sleep(0)
        while self._callbacks.active():
            self._requested.clear()
            try:
                await self.run_pass()
            except Exception as error:
                logger.warning("Kraken candle catchup pass unresolved: {}", type(error).__name__)
            await asyncio.sleep(self._policy.minimum_spacing_seconds)
            with contextlib.suppress(TimeoutError):
                await asyncio.wait_for(
                    self._requested.wait(),
                    self._policy.interval_seconds - self._policy.minimum_spacing_seconds,
                )

    async def run_pass(self) -> None:
        """Visit a bounded fair slice of persisted symbols without inline retries."""
        symbols = sorted(set(self._callbacks.symbols()))
        if not symbols:
            return
        selected = self._select_symbols(symbols)
        end = _floor(self._callbacks.clock(), 60)
        start = end - timedelta(minutes=self._policy.history_minutes)
        async with asyncio.timeout(self._policy.pass_seconds):
            for symbol, priority in selected:
                if not self._callbacks.active():
                    return
                self._record_attempt(priority)
                try:
                    async with asyncio.timeout(self._policy.attempt_seconds):
                        await self._reconcile_symbol(symbol, start, end)
                except Exception as error:
                    logger.warning(
                        "Kraken candle catchup symbol unresolved: symbol={} error={}",
                        symbol,
                        type(error).__name__,
                    )

    async def _reconcile_symbol(self, symbol: str, start: datetime, end: datetime) -> None:
        """Insert absent native keys, then reconcile repairs from a committed read.

        Args:
            symbol: Canonical native symbol selected by persistence policy.
            start: Inclusive UTC business bound.
            end: Exclusive closed-minute horizon.

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
        await self._repository.insert_missing_candles(rows)
        await self._callbacks.committed(symbol, start, end)

    def _row(self, candle: CandleUpdate, instrument: str, author: datetime) -> CandleUpsertRow:
        """Preserve venue fields with actual knowledge time and producer provenance.

        Args:
            candle: Full native closed-minute observation.
            instrument: Resolved canonical instrument identity.
            author: Actual time the native response was learned.

        Returns:
            A native complete row for atomic missing-only insertion.
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
