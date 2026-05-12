"""Unit tests for ``TradeCandleBuilder`` + ``enqueue_or_drop_oldest_candle``.

The builder is the shared kernel behind candle synthesis for any
exchange whose WS feed lacks a server-side OHLC channel
(Kraken Equities, Kraken Futures). The helper module sits outside
the per-exchange test files because the same kernel powers multiple
clients; testing it once here is cheaper than duplicating the same
assertions inside every client test file.
"""

import asyncio
from datetime import UTC
from datetime import datetime
from datetime import timedelta

import pytest
from loguru import logger

from snapper.infrastructure.exchanges import _trade_candle_builder as tcb
from snapper.infrastructure.exchanges._trade_candle_builder import TradeCandleBuilder
from snapper.infrastructure.exchanges._trade_candle_builder import enqueue_or_drop_oldest_candle
from snapper.infrastructure.exchanges.contracts import CandleUpdate
from snapper.infrastructure.exchanges.contracts import TradeUpdate


def _trade(
    symbol: str,
    price: float,
    quantity: float,
    ts: datetime,
) -> TradeUpdate:
    """Build a minimal ``TradeUpdate`` for one trade fold step.

    The builder ignores side / trade_id / ord_type but contracts
    require them; this helper just fills in throwaway values so each
    test stays focused on the OHLC math.
    """
    return TradeUpdate(
        symbol=symbol,
        side="buy",
        quantity=quantity,
        price=price,
        ord_type="fill",
        timestamp=ts,
        trade_id=f"{symbol}-{ts.isoformat()}-{price}",
    )


class TestTradeCandleBuilder:
    """Tests for the per-minute OHLCV accumulator."""

    def test_first_trade_sets_open_high_low_close(self) -> None:
        """A single trade in a fresh minute produces a candle where OHLC == price.

        Given: An empty builder,
        When: One trade is folded,
        Then: ``active_buckets`` becomes 1 and a later ``pop_completed``
            past the bucket's minute returns ``open=high=low=close=price``,
            ``volume=quantity``, ``trades=1``, ``vwap=price``.
        """
        minute = datetime(2026, 5, 12, 18, 30, tzinfo=UTC)
        b = TradeCandleBuilder()
        b.update(_trade("BTC-USD-PERP", price=50000.0, quantity=0.5, ts=minute))
        assert b.active_buckets() == 1
        candles = b.pop_completed(minute + timedelta(minutes=1))
        assert len(candles) == 1
        c = candles[0]
        assert c.symbol == "BTC-USD-PERP"
        assert c.open == c.high == c.low == c.close == pytest.approx(50000.0)
        assert c.volume == pytest.approx(0.5)
        assert c.trades == 1
        assert c.vwap == pytest.approx(50000.0)
        assert c.interval == 60
        assert b.active_buckets() == 0

    def test_subsequent_trades_widen_high_low_and_volume(self) -> None:
        """Repeated trades in the same minute widen OHLC and accumulate volume.

        Given: Three trades in the same minute at prices 100 / 110 / 95,
            quantities 1 / 2 / 1,
        When: ``pop_completed`` runs after the minute closes,
        Then: ``open=100``, ``high=110``, ``low=95``, ``close=95``,
            ``volume=4``, ``trades=3``, ``vwap`` is the volume-weighted
            mean (``(100*1+110*2+95*1)/4``).
        """
        minute = datetime(2026, 5, 12, 18, 30, tzinfo=UTC)
        b = TradeCandleBuilder()
        b.update(_trade("MNQM6-CME", 100.0, 1.0, minute))
        b.update(_trade("MNQM6-CME", 110.0, 2.0, minute + timedelta(seconds=10)))
        b.update(_trade("MNQM6-CME", 95.0, 1.0, minute + timedelta(seconds=20)))
        candles = b.pop_completed(minute + timedelta(minutes=1))
        assert len(candles) == 1
        c = candles[0]
        assert c.open == pytest.approx(100.0)
        assert c.high == pytest.approx(110.0)
        assert c.low == pytest.approx(95.0)
        assert c.close == pytest.approx(95.0)
        assert c.volume == pytest.approx(4.0)
        assert c.trades == 3
        assert c.vwap == pytest.approx((100.0 + 220.0 + 95.0) / 4.0)

    def test_active_minute_not_emitted(self) -> None:
        """The bucket for the *current* minute is left in place — only strictly past minutes are emitted.

        Given: A trade in the current minute,
        When: ``pop_completed`` is called with ``now`` still inside
            that same minute,
        Then: No candles are emitted and the bucket remains live.
        """
        minute = datetime(2026, 5, 12, 18, 30, tzinfo=UTC)
        b = TradeCandleBuilder()
        b.update(_trade("BTC-USD-PERP", 50000.0, 0.1, minute))
        candles = b.pop_completed(minute + timedelta(seconds=45))
        assert candles == []
        assert b.active_buckets() == 1

    def test_separate_minutes_emit_independently(self) -> None:
        """Trades in two different minutes produce two candles.

        Given: A trade in minute M and another in minute M+1,
        When: ``pop_completed`` runs at minute M+2,
        Then: Two candles are returned (one per minute) and the
            accumulator is empty afterwards.
        """
        m1 = datetime(2026, 5, 12, 18, 30, tzinfo=UTC)
        m2 = m1 + timedelta(minutes=1)
        b = TradeCandleBuilder()
        b.update(_trade("ETH-USD-PERP", 3000.0, 1.0, m1 + timedelta(seconds=5)))
        b.update(_trade("ETH-USD-PERP", 3010.0, 1.0, m2 + timedelta(seconds=5)))
        candles = b.pop_completed(m2 + timedelta(minutes=1))
        assert len(candles) == 2
        opens = sorted(c.open for c in candles)
        assert opens == [pytest.approx(3000.0), pytest.approx(3010.0)]
        assert b.active_buckets() == 0

    def test_zero_volume_branch_falls_through_to_vwap_zero(self) -> None:
        """An accumulator with zero volume yields a ``vwap=0.0`` candle.

        Given: A trade with ``quantity == 0`` (a degenerate but
            schema-legal case — exchanges occasionally send these for
            informational fills),
        When: The minute closes and ``pop_completed`` runs,
        Then: The emitted candle has ``vwap == 0.0`` (the
            ``b.volume > 0`` guard short-circuits the divide).
        """
        minute = datetime(2026, 5, 12, 18, 30, tzinfo=UTC)
        b = TradeCandleBuilder()
        b.update(_trade("X-USD", 1.0, 0.0, minute))
        candles = b.pop_completed(minute + timedelta(minutes=1))
        assert len(candles) == 1
        assert candles[0].volume == pytest.approx(0.0)
        assert candles[0].vwap == pytest.approx(0.0)


class TestEnqueueOrDropOldestCandle:
    """Tests for the bounded candle queue helper."""

    def test_enqueue_normal(self) -> None:
        """Enqueue succeeds while capacity is available.

        Given: A bounded queue with capacity 2 holding one candle,
        When: A second candle is enqueued via the helper,
        Then: The queue size becomes 2 and the call did not drop
            the existing item.
        """
        q: asyncio.Queue[CandleUpdate] = asyncio.Queue(maxsize=2)
        seed = CandleUpdate(
            symbol="X",
            open=1.0,
            high=1.0,
            low=1.0,
            close=1.0,
            vwap=1.0,
            trades=1,
            volume=1.0,
            interval_begin=datetime(2026, 5, 12, 18, 0, tzinfo=UTC),
            interval=60,
        )
        q.put_nowait(seed)
        enqueue_or_drop_oldest_candle(q, seed, "test-candle")
        assert q.qsize() == 2

    def test_drops_oldest_when_full(self) -> None:
        """Helper drops the oldest candle when the queue is at capacity.

        Given: A bounded queue with capacity 1 already holding a
            candle,
        When: ``enqueue_or_drop_oldest_candle`` is called with a new
            candle,
        Then: The new candle is in the queue and the previous one is
            gone — first-in-first-out semantics under backpressure.
        """
        q: asyncio.Queue[CandleUpdate] = asyncio.Queue(maxsize=1)
        first = CandleUpdate(
            symbol="OLD",
            open=1.0,
            high=1.0,
            low=1.0,
            close=1.0,
            vwap=1.0,
            trades=1,
            volume=1.0,
            interval_begin=datetime(2026, 5, 12, 18, 0, tzinfo=UTC),
            interval=60,
        )
        second = CandleUpdate(
            symbol="NEW",
            open=2.0,
            high=2.0,
            low=2.0,
            close=2.0,
            vwap=2.0,
            trades=1,
            volume=1.0,
            interval_begin=datetime(2026, 5, 12, 18, 1, tzinfo=UTC),
            interval=60,
        )
        q.put_nowait(first)
        enqueue_or_drop_oldest_candle(q, second, "test-candle")
        assert q.qsize() == 1
        assert q.get_nowait().symbol == "NEW"

    def test_drop_log_is_rate_limited(self, caplog: pytest.LogCaptureFixture) -> None:
        """50 drops within one rate-limit window collapse to <=1 warning.

        Given: A capacity-1 queue and a freshly-reset drop counter,
        When: 50 drop-oldest events fire under the same wall-clock
            window,
        Then: At most one summary log line is emitted (matches the
            per-second rate limit other queue helpers share).
        """
        tcb._drop_counters.clear()
        q: asyncio.Queue[CandleUpdate] = asyncio.Queue(maxsize=1)
        seed = CandleUpdate(
            symbol="X",
            open=1.0,
            high=1.0,
            low=1.0,
            close=1.0,
            vwap=1.0,
            trades=1,
            volume=1.0,
            interval_begin=datetime(2026, 5, 12, 18, 0, tzinfo=UTC),
            interval=60,
        )
        q.put_nowait(seed)
        sink_id = logger.add(caplog.handler, format="{message}", level="WARNING")
        try:
            for _ in range(50):
                enqueue_or_drop_oldest_candle(q, seed, "candle-label")
        finally:
            logger.remove(sink_id)
        tcb._drop_counters.clear()
        summaries = [rec for rec in caplog.records if "candle-label queue full" in rec.message]
        assert len(summaries) <= 1
