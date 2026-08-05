"""Synthesize 1m OHLCV candles from a live stream of TradeUpdate messages.

Used by exchange clients that have a WS trade channel but no WS candle
channel (Kraken Equities, Kraken Futures). The historical fix-up path
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
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime
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


class TradeCandleBuilder:
    """Aggregate trade updates into 1-minute OHLCV candles client-side.

    Each call to :meth:`update` folds a single :class:`TradeUpdate`
    into the accumulator bucket for that trade's symbol + minute. A
    bucket is keyed by ``(symbol, minute_ts)`` where ``minute_ts`` is
    the floor of the trade's timestamp to its minute. :meth:`pop_completed`
    returns the candles for every minute strictly older than the
    caller's "now" and removes them from the accumulator. The wall-clock
    path holds at most one bucket per active symbol; the event-watermark
    path (:meth:`pop_completed_by_event_watermark`) may buffer the current
    minute plus any minute within ``interval + grace`` of the watermark, so
    a delayed feed can hold a few buckets per symbol at once.

    The builder is single-thread by design — callers drive it from
    the exchange client's WS callback and a once-per-second background
    aggregator. No locks. The two methods are not coroutines on
    purpose; they are cheap and synchronous to keep the WS callback
    path tight.
    """

    def __init__(self, interval_seconds: int = 60) -> None:
        """Create an empty builder.

        Args:
            interval_seconds: Candle width in seconds. ``60`` is the
                only currently-validated setting (matches the SCD2
                schema's ``candles.timeframe='1m'`` row family); larger
                intervals will work mechanically but have not been
                pressure-tested in production.
        """
        self._interval_s = interval_seconds
        self._builders: dict[str, _CandleAccumulator] = {}
        self._watermark: datetime | None = None
        self._update_count: int = 0
        self._closed_minute: dict[str, int] = {}
        self._late_trades_after_close: int = 0

    def update(self, trade: TradeUpdate) -> None:
        """Fold a single trade into its symbol's open minute-bucket.

        First trade in a bucket sets OHLC = trade.price; subsequent
        trades widen ``high`` / ``low`` and accumulate ``volume`` +
        ``vwap_sum``. ``open`` / ``close`` track the EARLIEST / LATEST trade
        by EVENT time (``trade.timestamp``), not arrival order, so a delayed
        feed's out-of-order same-minute batches still yield the correct open
        and close. Also advances the event-time
        watermark (the highest ``trade.timestamp`` folded so far, consumed by
        :meth:`pop_completed_by_event_watermark`) and the activity counter
        (consumed by the idle-flush driver — see :meth:`update_count`).
        A trade whose minute was ALREADY emitted increments
        :attr:`late_trades_after_close` (and WARNs once per re-opened
        bucket) but is still folded — its corrective candle must not be
        lost; the counter exists so the early close is visible.

        Args:
            trade: The :class:`TradeUpdate` to fold into the
                ``(symbol, minute)`` bucket derived from
                ``trade.timestamp``.
        """
        self._update_count += 1
        if self._watermark is None or trade.timestamp > self._watermark:
            self._watermark = trade.timestamp
        floor = trade.timestamp.replace(second=0, microsecond=0)
        minute_ts = int(floor.timestamp())
        key = f"{trade.symbol}_{minute_ts}"
        existing = self._builders.get(key)
        closed = self._closed_minute.get(trade.symbol)
        if closed is not None and minute_ts <= closed:
            self._late_trades_after_close += 1
            if existing is None:
                logger.warning(
                    f"late trade after candle close re-opens bucket: "
                    f"symbol={trade.symbol} minute={floor.isoformat()} "
                    f"trade_ts={trade.timestamp.isoformat()} "
                    f"(will emit a corrective candle that re-fragments the "
                    f"SCD2 row — widen the grace/idle-flush bound if this "
                    f"recurs; counter={self._late_trades_after_close})"
                )
        if existing is None:
            self._builders[key] = _CandleAccumulator(
                symbol=trade.symbol,
                open=trade.price,
                high=trade.price,
                low=trade.price,
                close=trade.price,
                volume=trade.quantity,
                trades=1,
                vwap_sum=trade.price * trade.quantity,
                interval_begin=floor,
                open_ts=trade.timestamp,
                close_ts=trade.timestamp,
            )
            return
        existing.high = max(existing.high, trade.price)
        existing.low = min(existing.low, trade.price)
        if trade.timestamp < existing.open_ts:
            existing.open = trade.price
            existing.open_ts = trade.timestamp
        if trade.timestamp >= existing.close_ts:
            existing.close = trade.price
            existing.close_ts = trade.timestamp
        existing.volume += trade.quantity
        existing.trades += 1
        existing.vwap_sum += trade.price * trade.quantity

    def pop_completed(self, now_utc: datetime) -> list[CandleUpdate]:
        """Emit and discard every bucket whose minute has finished.

        A bucket is "complete" when its minute is strictly less than
        the caller's current-minute floor — i.e. no further trades for
        that minute can possibly arrive. The active (current-minute)
        bucket is left in place so subsequent trades continue to
        update it.

        Args:
            now_utc: The caller's "now" in UTC. Floored to the minute
                internally; the comparison is ``bucket_minute <
                current_minute``.

        Returns:
            List of :class:`CandleUpdate` items in arbitrary order.
            The caller is responsible for routing them downstream (eg.
            via :func:`enqueue_or_drop_oldest_candle`).
        """
        current_minute_ts = int(now_utc.replace(second=0, microsecond=0).timestamp())
        return self._emit_and_remove(lambda begin_ts: begin_ts < current_minute_ts)

    def pop_completed_by_event_watermark(self, grace_seconds: float) -> list[CandleUpdate]:
        """Emit buckets the feed's own event-clock has already moved past.

        Wall-clock completion (:meth:`pop_completed`) is correct only for a
        real-time feed. For a DELAYED feed (e.g. Kraken Equities ~10 min) an
        event-minute's trades arrive across many wall-clock minutes, so
        wall-clock close fragments each minute into partial, mutually-
        superseding candles (the last fragment wrongly becoming current).
        This instead closes a bucket only once the event watermark (the
        highest ``trade.timestamp`` folded so far) has advanced past the
        bucket's minute END by ``grace_seconds`` of skew tolerance, so every
        delayed batch for a minute accumulates into ONE candle before it is
        emitted. Returns ``[]`` until the first trade sets the watermark.

        Args:
            grace_seconds: Event-time slack beyond a bucket's minute end
                before it is final — absorbs cross-symbol arrival skew
                within the same feed. A trade arriving more than this past
                its minute's close (reordering beyond ``interval + grace``)
                lands in a fresh bucket and yields a separate corrective
                candle; the feed is delayed-but-monotonic so that is not
                expected — widen ``grace`` rather than rely on it.

        Returns:
            Completed :class:`CandleUpdate` items; the watermark's own
            minute and any minute within ``interval + grace_seconds`` of the
            watermark stay in place to keep accumulating.
        """
        if self._watermark is None:
            return []
        cutoff = self._watermark.timestamp() - self._interval_s - grace_seconds
        return self._emit_and_remove(lambda begin_ts: begin_ts <= cutoff)

    def pop_all(self) -> list[CandleUpdate]:
        """Emit and discard EVERY remaining bucket regardless of completion.

        Idle/shutdown flush: when a delayed feed goes quiet the event
        watermark stalls and :meth:`pop_completed_by_event_watermark` would
        strand the final minute's bucket indefinitely. A driver invokes this
        only after a wall-clock silence threshold so the last bar is not lost
        until the feed resumes.

        Returns:
            All buffered :class:`CandleUpdate` items; the accumulator is
            empty afterwards.
        """
        return self._emit_and_remove(lambda _begin_ts: True)

    @property
    def watermark(self) -> datetime | None:
        """Return the highest ``trade.timestamp`` folded so far, or ``None``.

        Monotonic non-decreasing across :meth:`update` calls. Exposed so a
        driver loop can detect whether the feed's event-clock is still
        advancing and trigger an idle flush when it stalls.

        Returns:
            The event watermark, or ``None`` before the first trade.
        """
        return self._watermark

    @property
    def late_trades_after_close(self) -> int:
        """Return how many trades arrived for an already-emitted minute.

        Each such trade lands in (or re-opens) a bucket whose
        ``(symbol, minute)`` was already emitted by one of the pop
        methods, so its eventual emission is a CORRECTIVE candle that
        re-fragments the SCD2 row — the failure class behind the
        2026-06 Kraken Equities candle-fragmentation incident. A fresh
        re-open additionally logs a WARNING with the symbol and minute.
        This counter is the agreed trigger for widening the
        event-watermark grace or the idle-flush bound: zero in steady
        state; any growth means the close predicate fired too early for
        the feed's real delay.

        Returns:
            Count of late trades since construction (monotonic).
        """
        return self._late_trades_after_close

    @property
    def update_count(self) -> int:
        """Return the total number of trades folded so far.

        A monotonically increasing activity counter (exposed for the
        idle-flush driver and tests/metrics, like :meth:`active_buckets`). A
        driver compares it across ticks to distinguish "the feed delivered
        trades" (which must DEFER an idle flush) from "the feed is silent" —
        crucially even when those trades do NOT advance the event watermark
        (out-of-order, duplicate, or same-minute delayed batches whose
        timestamp is ``<=`` the watermark). Keying idle detection on the
        watermark alone would wrongly flush mid-activity.

        Returns:
            Count of :meth:`update` calls since construction.
        """
        return self._update_count

    def _emit_and_remove(self, should_emit: Callable[[int], bool]) -> list[CandleUpdate]:
        """Emit + discard every bucket whose minute satisfies ``should_emit``.

        Shared kernel behind :meth:`pop_completed`,
        :meth:`pop_completed_by_event_watermark`, and :meth:`pop_all` so the
        OHLCV-to-:class:`CandleUpdate` projection lives in one place. Also
        advances the per-symbol closed-minute watermark that feeds
        :attr:`late_trades_after_close` — every pop path (including the
        idle flush) counts as a close for late-trade accounting.

        A bucket carrying no volume at all falls back to ``b.close`` rather
        than to ``0.0``. A bucket exists here only because at least one trade
        was accumulated into it, so its OHLC names a real traded price; zero
        would price that bar at nothing for every reader joining on VWAP.
        This is the third and last copy of the same guard — the other two are
        ``CandleAggregator._to_candle_update`` and ``_build_rows`` in
        ``scripts/backfill_synth_candles.py``. All three write into the same
        durable candle plane, so they must agree.

        Args:
            should_emit: Predicate over a bucket's minute-start UNIX
                timestamp (int seconds); ``True`` emits + removes that
                bucket, ``False`` leaves it in place.

        Returns:
            The emitted candles, in arbitrary order.
        """
        out: list[CandleUpdate] = []
        to_remove: list[str] = []
        for key, b in self._builders.items():
            if not should_emit(int(b.interval_begin.timestamp())):
                continue
            vwap = b.vwap_sum / b.volume if b.volume > 0 else b.close
            out.append(
                CandleUpdate(
                    symbol=b.symbol,
                    open=b.open,
                    high=b.high,
                    low=b.low,
                    close=b.close,
                    vwap=vwap,
                    trades=b.trades,
                    volume=b.volume,
                    interval_begin=b.interval_begin,
                    interval=self._interval_s,
                )
            )
            to_remove.append(key)
            begin_ts = int(b.interval_begin.timestamp())
            prev_closed = self._closed_minute.get(b.symbol)
            if prev_closed is None or begin_ts > prev_closed:
                self._closed_minute[b.symbol] = begin_ts
        for key in to_remove:
            del self._builders[key]
        return out

    def active_buckets(self) -> int:
        """Return the number of in-flight buckets — exposed for tests / metrics.

        Returns:
            Count of ``(symbol, minute)`` accumulators currently holding
            state. On the wall-clock path this equals the distinct symbols
            with a trade in the current still-open minute; on the
            event-watermark path it also includes minutes still inside the
            watermark's ``interval + grace`` window (a delayed feed can hold
            a few minutes per symbol). Past minutes are popped by
            :meth:`pop_completed` / :meth:`pop_completed_by_event_watermark`.
        """
        return len(self._builders)
