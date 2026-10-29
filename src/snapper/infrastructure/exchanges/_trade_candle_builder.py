"""Synthesize 1m OHLCV candles from a live stream of TradeUpdate messages.

Used by Kraken Equities and Kraken Futures, which have a WS trade channel
without a WS candle channel, and by Kraken Spot when trade-built candle mode
is enabled. The historical fix-up path
on these venues used REST polling of OHLCV history endpoints; the
polling approach risks rate-limit / IP-ban and duplicates work the
exchange already does on the matching engine side. Client-side
synthesis from the live trade stream avoids both problems at the cost
of producing candles only for minutes that actually had a fill —
inactive minutes simply have no candle row, which matches the
semantics the rest of Snapper already handles.

Walutomat keeps its own minute-builder because it has no WS at all
(REST polling of best-offers every 10 s is the upstream contract);
that builder operates on quotes, not trades, and remains a separate
implementation in :mod:`snapper.infrastructure.exchanges.implementations.walutomat`.
"""

import asyncio
from collections import OrderedDict
from collections.abc import Callable
from dataclasses import dataclass
from dataclasses import field
from datetime import datetime
from datetime import timedelta
from time import monotonic

from loguru import logger

from snapper.infrastructure.exchanges.contracts import CandleUpdate
from snapper.infrastructure.exchanges.contracts import TradeUpdate

_DROP_LOG_INTERVAL_S = 1.0
_drop_counters: dict[str, list[float]] = {}


@dataclass
class _CandleAccumulator:
    """Running OHLCV state for one ``(symbol, minute)`` bucket."""

    symbol: str
    open: float
    high: float
    low: float
    close: float
    volume: float
    trades: int
    vwap_sum: float
    interval_begin: datetime
    open_ts: datetime
    close_ts: datetime
    emitted: bool = False
    trade_ids: set[str] = field(default_factory=set)


def enqueue_or_drop_oldest_candle(
    queue: asyncio.Queue[CandleUpdate], item: CandleUpdate, label: str
) -> None:
    """Bounded enqueue that drops the oldest candle when the queue is full.

    Mirrors the existing ``_enqueue_or_drop_oldest`` helpers used by
    tick and trade queues across the Kraken family of clients. Drop
    summaries are rate-limited to one warning per
    :data:`_DROP_LOG_INTERVAL_S` per label so a sustained backpressure
    burst does not amplify log I/O.

    Args:
        queue: Bounded queue receiving the candle.
        item: Candle to enqueue.
        label: Short identifier used in the rate-limited drop log
            (one counter per label).
    """
    try:
        queue.put_nowait(item)
    except asyncio.QueueFull:
        counters = _drop_counters.setdefault(label, [0.0, 0.0])
        counters[0] += 1
        now = monotonic()
        if now - counters[1] >= _DROP_LOG_INTERVAL_S:
            logger.warning(
                f"{label} queue full, dropped {int(counters[0])} candles "
                f"in last {now - counters[1]:.1f}s (drop-oldest backpressure)"
            )
            counters[0] = 0.0
            counters[1] = now
        queue.get_nowait()
        queue.put_nowait(item)


type _BucketKey = tuple[str, int]
type _TradeIdentity = tuple[datetime, float, float, str, str]


@dataclass(frozen=True)
class TradeCandleRetentionPolicy:
    """Bound complete correction history, pending buckets, and stable trade identities."""

    closed_minutes_per_symbol: int = 10
    max_buckets: int = 32_768
    max_trade_ids: int = 250_000
    max_symbols: int = 4_096

    def __post_init__(self) -> None:
        """Reject limits that cannot retain even one complete observation."""
        if (
            min(
                self.closed_minutes_per_symbol,
                self.max_buckets,
                self.max_trade_ids,
                self.max_symbols,
            )
            < 1
        ):
            raise ValueError("trade candle retention limits must be positive")


_DEFAULT_RETENTION = TradeCandleRetentionPolicy()


@dataclass
class _SymbolHistory:
    """The bounded symbol's coverage boundary and refused or emitted minute fences."""

    start: datetime
    closed_minutes: set[int] = field(default_factory=set)
    closed_minute: int | None = None
    refused_through: int | None = None
    warned_through: int | None = None


@dataclass(frozen=True)
class _RetainedTrade:
    """An identified trade's exact immutable payload and owning retained bucket."""

    bucket: _BucketKey
    identity: _TradeIdentity


class TradeCandleBuilder:
    """Emit full-minute corrections from bounded, retained trade accumulators.

    Emission clears only the dirty set. The complete accumulator and its trade
    identities remain available for ten closed minutes per symbol, subject to
    global policy limits. A correction reuses that full state; an evicted or
    otherwise unproven minute is withheld instead of emitting a late fragment.

    A fresh live builder cannot know the first observed minute's earlier trades,
    so that minute and older minutes are withheld. A historical reader may supply
    its actual inclusive history start; only whole minutes beginning at or after
    that boundary can then be published. Symbol-history eviction reinstates the
    live cold-start guard. All methods are synchronous and single-threaded.
    """

    def __init__(
        self,
        interval_seconds: int = 60,
        *,
        history_start: datetime | None = None,
        retention: TradeCandleRetentionPolicy = _DEFAULT_RETENTION,
    ) -> None:
        """Create bounded state, optionally backed by an explicit UTC history boundary."""
        if history_start is not None and history_start.utcoffset() != timedelta(0):
            raise ValueError("trade candle history start must be UTC")
        self._interval_s = interval_seconds
        self._history_start = history_start
        self._retention = retention
        self._builders: OrderedDict[_BucketKey, _CandleAccumulator] = OrderedDict()
        self._pending: set[_BucketKey] = set()
        self._histories: OrderedDict[str, _SymbolHistory] = OrderedDict()
        self._forgotten_start: datetime | None = None
        self._trade_ids: dict[tuple[str, str], _RetainedTrade] = {}
        self._watermark: datetime | None = None
        self._update_count = 0
        self._late_trades_after_close = 0
        self._refused_trades = 0
        self._duplicate_trades = 0

    def _symbol_history(
        self,
        symbol: str,
        minute: datetime,
        watermark: datetime,
        observed_at: datetime | None,
    ) -> _SymbolHistory:
        """Restore cold-start protection without forgetting a lost minute's fence.

        One monotonic watermark boundary survives symbol-history eviction. New
        histories cannot publish at or before it, even if an old replay arrives
        before a fragment from the newer evicted active minute.
        """
        history = self._histories.get(symbol)
        if history is not None:
            self._histories.move_to_end(symbol)
            return history
        if len(self._histories) >= self._retention.max_symbols:
            evicted_symbol, _ = self._histories.popitem(last=False)
            self._forgotten_start = watermark.replace(second=0, microsecond=0) + timedelta(
                minutes=1
            )
            for key in [key for key in self._builders if key[0] == evicted_symbol]:
                self._evict_bucket(key)
        start = self._history_start
        if start is None:
            start = minute + timedelta(minutes=1)
        if observed_at is not None:
            start = max(start, minute + timedelta(minutes=1), observed_at)
        if self._forgotten_start is not None:
            start = max(start, minute + timedelta(minutes=1), self._forgotten_start)
        history = _SymbolHistory(start=start)
        self._histories[symbol] = history
        return history

    def _refuse(self, symbol: str, minute_ts: int, reason: str) -> None:
        """Count every refusal while bounding its warning history to the symbol cap."""
        self._refused_trades += 1
        history = self._histories[symbol]
        previous = history.refused_through
        history.refused_through = minute_ts if previous is None else max(previous, minute_ts)
        if history.warned_through is None or minute_ts > history.warned_through:
            history.warned_through = minute_ts
            logger.warning(
                "Withholding unproven trade candle: symbol={} minute={} reason={} refused={}",
                symbol,
                minute_ts,
                reason,
                self._refused_trades,
            )

    def _evict_bucket(self, key: _BucketKey) -> None:
        """Drop a whole accumulator and all its IDs, fencing off partial recreation."""
        bucket = self._builders.pop(key)
        self._pending.discard(key)
        for trade_id in bucket.trade_ids:
            del self._trade_ids[(bucket.symbol, trade_id)]
        history = self._histories.get(bucket.symbol)
        if history is not None:
            history.closed_minutes.discard(key[1])
            previous = history.refused_through
            history.refused_through = key[1] if previous is None else max(previous, key[1])

    def _already_seen(self, trade: TradeUpdate, key: _BucketKey) -> bool:
        """Suppress exact stable-ID redelivery and refuse contradictory identified evidence."""
        if trade.trade_id is None:
            return False
        retained = self._trade_ids.get((trade.symbol, trade.trade_id))
        if retained is None:
            return False
        identity = (trade.timestamp, trade.price, trade.quantity, trade.side, trade.ord_type)
        if retained.identity == identity:
            self._duplicate_trades += 1
            return True
        self._evict_bucket(retained.bucket)
        if key in self._builders:
            self._evict_bucket(key)
        self._refuse(trade.symbol, key[1], "conflicting_trade_id")
        return True

    def _bucket_for_trade(
        self, trade: TradeUpdate, minute: datetime, key: _BucketKey
    ) -> _CandleAccumulator | None:
        """Return complete retained state or admit a genuinely new proven minute."""
        history = self._histories[trade.symbol]
        if minute < history.start:
            self._refuse(trade.symbol, key[1], "unknown_leading_history")
            return None
        existing = self._builders.get(key)
        if existing is not None:
            return existing
        fences = [
            value for value in (history.closed_minute, history.refused_through) if value is not None
        ]
        if fences and key[1] <= max(fences):
            self._refuse(trade.symbol, key[1], "complete_minute_not_retained")
            return None
        if len(self._builders) >= self._retention.max_buckets:
            self._evict_bucket(next(iter(self._builders)))
            if history.refused_through is not None and key[1] <= history.refused_through:
                self._refuse(trade.symbol, key[1], "complete_minute_evicted_for_capacity")
                return None
        bucket = _CandleAccumulator(
            symbol=trade.symbol,
            open=trade.price,
            high=trade.price,
            low=trade.price,
            close=trade.price,
            volume=0.0,
            trades=0,
            vwap_sum=0.0,
            interval_begin=minute,
            open_ts=trade.timestamp,
            close_ts=trade.timestamp,
        )
        self._builders[key] = bucket
        return bucket

    def _remember_trade(self, trade: TradeUpdate, key: _BucketKey) -> bool:
        """Reserve stable identity storage by evicting complete buckets, never partial ID sets."""
        if trade.trade_id is None:
            return True
        while len(self._trade_ids) >= self._retention.max_trade_ids:
            self._evict_bucket(next(iter(self._builders)))
        if key not in self._builders:
            self._refuse(trade.symbol, key[1], "trade_identity_budget_exhausted")
            return False
        identity = (trade.timestamp, trade.price, trade.quantity, trade.side, trade.ord_type)
        self._trade_ids[(trade.symbol, trade.trade_id)] = _RetainedTrade(key, identity)
        self._builders[key].trade_ids.add(trade.trade_id)
        return True

    @staticmethod
    def _validate_observed_at(observed_at: datetime | None) -> None:
        """Require any live receipt boundary to be an aware UTC instant."""
        if observed_at is not None and observed_at.utcoffset() != timedelta(0):
            raise ValueError("trade candle observation time must be UTC")

    def update(self, trade: TradeUpdate, *, observed_at: datetime | None = None) -> None:
        """Fold one proven unique trade, preserving arrival activity even on refusal or replay.

        Args:
            trade: Received trade whose event minute and stable identity are checked before folding.
            observed_at: Actual UTC receipt time for a live observation. A new symbol cannot
                certify its first event minute or a minute preceding this boundary.
        """
        self._validate_observed_at(observed_at)
        self._update_count += 1
        if self._watermark is None or trade.timestamp > self._watermark:
            self._watermark = trade.timestamp
        minute = trade.timestamp.replace(second=0, microsecond=0)
        key = (trade.symbol, int(minute.timestamp()))
        self._symbol_history(trade.symbol, minute, self._watermark, observed_at)
        if self._already_seen(trade, key):
            return
        bucket = self._bucket_for_trade(trade, minute, key)
        if bucket is None or not self._remember_trade(trade, key):
            return
        if bucket.emitted:
            self._late_trades_after_close += 1
            if key not in self._pending:
                logger.warning(
                    "late trade after candle close re-opens bucket: symbol={} minute={} "
                    "(retained full-minute correction; counter={})",
                    trade.symbol,
                    minute.isoformat(),
                    self._late_trades_after_close,
                )
        bucket.high = max(bucket.high, trade.price)
        bucket.low = min(bucket.low, trade.price)
        if trade.timestamp < bucket.open_ts:
            bucket.open = trade.price
            bucket.open_ts = trade.timestamp
        if trade.timestamp >= bucket.close_ts:
            bucket.close = trade.price
            bucket.close_ts = trade.timestamp
        bucket.volume += trade.quantity
        bucket.trades += 1
        bucket.vwap_sum += trade.price * trade.quantity
        self._pending.add(key)
        self._builders.move_to_end(key)

    def pop_completed(self, now_utc: datetime) -> list[CandleUpdate]:
        """Emit dirty minutes before the caller's current minute, retaining full correction state.

        Args:
            now_utc: UTC wall clock whose minute bounds completed dirty buckets.

        Returns:
            Full candle snapshots for dirty minutes strictly before the current minute.
        """
        current_minute = int(now_utc.replace(second=0, microsecond=0).timestamp())
        return self._emit_dirty(lambda minute: minute < current_minute)

    def pop_completed_by_event_watermark(self, grace_seconds: float) -> list[CandleUpdate]:
        """Emit dirty minutes beyond the event watermark's interval-plus-grace boundary.

        Args:
            grace_seconds: Additional event-time delay allowed after a minute ends.

        Returns:
            Full dirty candle snapshots past the boundary, or an empty list before any trade.
        """
        if self._watermark is None:
            return []
        cutoff = self._watermark.timestamp() - self._interval_s - grace_seconds
        return self._emit_dirty(lambda minute: minute <= cutoff)

    def pop_all(self) -> list[CandleUpdate]:
        """Flush dirty idle/shutdown buckets once while retaining bounded correction history.

        Returns:
            Full snapshots of all dirty buckets; unchanged retained buckets are omitted.
        """
        return self._emit_dirty(lambda _minute: True)

    def _emit_dirty(self, should_emit: Callable[[int], bool]) -> list[CandleUpdate]:
        """Project full dirty accumulators and enforce per-symbol closed-minute retention."""
        result: list[CandleUpdate] = []
        for key in sorted(self._pending, key=lambda item: (item[1], item[0])):
            if not should_emit(key[1]):
                continue
            bucket = self._builders[key]
            result.append(
                CandleUpdate(
                    symbol=bucket.symbol,
                    open=bucket.open,
                    high=bucket.high,
                    low=bucket.low,
                    close=bucket.close,
                    vwap=bucket.vwap_sum / bucket.volume if bucket.volume > 0 else bucket.close,
                    trades=bucket.trades,
                    volume=bucket.volume,
                    interval_begin=bucket.interval_begin,
                    interval=self._interval_s,
                )
            )
            self._pending.remove(key)
            bucket.emitted = True
            history = self._histories[bucket.symbol]
            previous = history.closed_minute
            history.closed_minute = key[1] if previous is None else max(previous, key[1])
            history.closed_minutes.add(key[1])
            while len(history.closed_minutes) > self._retention.closed_minutes_per_symbol:
                self._evict_bucket((bucket.symbol, min(history.closed_minutes)))
        return result

    @property
    def watermark(self) -> datetime | None:
        """Return the greatest received event timestamp, including replays and refusals.

        Returns:
            The greatest received event timestamp, or None before the first update.
        """
        return self._watermark

    @property
    def late_trades_after_close(self) -> int:
        """Return accepted unique late trades folded into full retained correction state.

        Returns:
            The cumulative number of accepted late trades since construction.
        """
        return self._late_trades_after_close

    @property
    def update_count(self) -> int:
        """Return every received update so duplicate traffic still defers idle flushing.

        Returns:
            The cumulative number of update calls, including duplicates and refusals.
        """
        return self._update_count

    @property
    def refused_trades(self) -> int:
        """Return updates withheld because their complete minute or identity was unproven.

        Returns:
            The cumulative number of refused trade updates since construction.
        """
        return self._refused_trades

    @property
    def duplicate_trades(self) -> int:
        """Return exact stable-ID redeliveries ignored without changing candle economics.

        Returns:
            The cumulative number of exact identified trade redeliveries ignored.
        """
        return self._duplicate_trades

    @property
    def retained_buckets(self) -> int:
        """Return all pending and emitted accumulators under the global bucket cap.

        Returns:
            The number of currently retained full accumulators.
        """
        return len(self._builders)

    @property
    def retained_trade_ids(self) -> int:
        """Return all retained identities under the global ID cap.

        Returns:
            The number of currently retained stable trade identities.
        """
        return len(self._trade_ids)

    @property
    def tracked_symbols(self) -> int:
        """Return symbol coverage histories under the global symbol cap.

        Returns:
            The number of currently retained symbol coverage histories.
        """
        return len(self._histories)

    def active_buckets(self) -> int:
        """Return dirty buckets waiting for emission, excluding retained clean history.

        Returns:
            The number of dirty accumulators waiting for a pop operation.
        """
        return len(self._pending)
