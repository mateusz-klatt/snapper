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
    caller's "now" and removes them from the accumulator, so the
    accumulator only ever holds at most one bucket per active symbol.

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

    def update(self, trade: TradeUpdate) -> None:
        """Fold a single trade into its symbol's open minute-bucket.

        First trade in a bucket sets OHLC = trade.price; subsequent
        trades widen ``high`` / ``low``, advance ``close``, and
        accumulate ``volume`` + ``vwap_sum``.

        Args:
            trade: The :class:`TradeUpdate` to fold into the
                ``(symbol, minute)`` bucket derived from
                ``trade.timestamp``.
        """
        floor = trade.timestamp.replace(second=0, microsecond=0)
        minute_ts = int(floor.timestamp())
        key = f"{trade.symbol}_{minute_ts}"
        existing = self._builders.get(key)
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
            )
            return
        existing.high = max(existing.high, trade.price)
        existing.low = min(existing.low, trade.price)
        existing.close = trade.price
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
        current_minute_floor = now_utc.replace(second=0, microsecond=0)
        current_minute_ts = int(current_minute_floor.timestamp())
        out: list[CandleUpdate] = []
        to_remove: list[str] = []
        for key, b in self._builders.items():
            bucket_minute = int(b.interval_begin.timestamp())
            if bucket_minute >= current_minute_ts:
                continue
            vwap = b.vwap_sum / b.volume if b.volume > 0 else 0.0
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
        for key in to_remove:
            del self._builders[key]
        return out

    def active_buckets(self) -> int:
        """Return the number of in-flight buckets — exposed for tests / metrics.

        Returns:
            Count of ``(symbol, minute)`` accumulators currently
            holding state. Equal to the number of distinct symbols
            with at least one trade in the current (still-open)
            minute — past minutes are popped by :meth:`pop_completed`.
        """
        return len(self._builders)
