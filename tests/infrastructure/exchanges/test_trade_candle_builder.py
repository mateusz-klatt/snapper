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


class TestTradeCandleBuilderEventWatermark:
    """Tests for the delayed-feed event-clock completion path."""

    def test_watermark_none_until_first_trade_then_monotonic(self) -> None:
        """The watermark starts ``None`` and only ever advances forward.

        Given: A fresh builder,
        When: Trades fold in out-of-order timestamps,
        Then: ``watermark`` is ``None`` initially, becomes the first
            trade's timestamp, ignores an earlier trade, and advances on a
            later trade.
        """
        b = TradeCandleBuilder()
        assert b.watermark is None
        m = datetime(2026, 6, 8, 14, 39, tzinfo=UTC)
        b.update(_trade("MCLQ6-CME", 89.5, 1.0, m + timedelta(seconds=30)))
        assert b.watermark == m + timedelta(seconds=30)
        b.update(_trade("MCLQ6-CME", 89.6, 1.0, m + timedelta(seconds=10)))
        assert b.watermark == m + timedelta(seconds=30)
        b.update(_trade("MCLQ6-CME", 89.7, 1.0, m + timedelta(seconds=50)))
        assert b.watermark == m + timedelta(seconds=50)

    def test_event_watermark_returns_empty_before_first_trade(self) -> None:
        """No watermark yet → the event-clock pop is a no-op.

        Given: A fresh builder with no trades,
        When: ``pop_completed_by_event_watermark`` runs,
        Then: It returns ``[]`` (the ``watermark is None`` guard).
        """
        b = TradeCandleBuilder()
        assert b.pop_completed_by_event_watermark(60.0) == []

    def test_event_watermark_keeps_minute_open_within_grace(self) -> None:
        """A minute stays open while the watermark is within interval+grace.

        Given: A single trade in minute M (watermark == M's region),
        When: ``pop_completed_by_event_watermark(60)`` runs,
        Then: Nothing is emitted and the bucket stays live — the feed's
            event-clock has not yet advanced an interval + grace past M.
        """
        m = datetime(2026, 6, 8, 14, 39, tzinfo=UTC)
        b = TradeCandleBuilder()
        b.update(_trade("MCLQ6-CME", 89.5, 1.0, m + timedelta(seconds=30)))
        assert b.pop_completed_by_event_watermark(60.0) == []
        assert b.active_buckets() == 1

    def test_event_watermark_accumulates_delayed_batches_into_one_candle(self) -> None:
        """Delayed multi-batch trades for one minute fold into ONE candle.

        Regression guard for the delayed-feed fragmentation bug: three trades
        for minute M arrive across batches, then a later trade in M+2 advances
        the event watermark past M's end + grace.

        Given: M trades (89.59 / 90.00 / 89.74; volumes 1 / 33 / 8) then an
            M+2 trade,
        When: ``pop_completed_by_event_watermark(60)`` runs,
        Then: Exactly ONE candle for M is emitted carrying the FULL minute
            (open 89.59, high 90.00, low 89.59, close 89.74, volume 42,
            trades 3); the M+2 bucket stays open.
        """
        m = datetime(2026, 6, 8, 14, 39, tzinfo=UTC)
        b = TradeCandleBuilder()
        b.update(_trade("MCLQ6-CME", 89.59, 1.0, m + timedelta(seconds=5)))
        b.update(_trade("MCLQ6-CME", 90.00, 33.0, m + timedelta(seconds=30)))
        b.update(_trade("MCLQ6-CME", 89.74, 8.0, m + timedelta(seconds=58)))
        assert b.pop_completed_by_event_watermark(60.0) == []
        b.update(_trade("MCLQ6-CME", 89.80, 1.0, m + timedelta(minutes=2)))
        candles = b.pop_completed_by_event_watermark(60.0)
        assert len(candles) == 1
        c = candles[0]
        assert c.open == pytest.approx(89.59)
        assert c.high == pytest.approx(90.00)
        assert c.low == pytest.approx(89.59)
        assert c.close == pytest.approx(89.74)
        assert c.volume == pytest.approx(42.0)
        assert c.trades == 3
        assert b.active_buckets() == 1

    def test_pop_all_emits_every_remaining_bucket(self) -> None:
        """``pop_all`` flushes all buckets regardless of completion.

        Given: Two live buckets for different symbols,
        When: ``pop_all`` is called,
        Then: Both are emitted, the accumulator is empty, and a second
            ``pop_all`` returns ``[]``.
        """
        m = datetime(2026, 6, 8, 14, 39, tzinfo=UTC)
        b = TradeCandleBuilder()
        b.update(_trade("A-USD", 1.0, 1.0, m + timedelta(seconds=5)))
        b.update(_trade("B-USD", 2.0, 1.0, m + timedelta(seconds=5)))
        flushed = b.pop_all()
        assert len(flushed) == 2
        assert b.active_buckets() == 0
        assert b.pop_all() == []

    def test_update_count_increments_per_trade(self) -> None:
        """``update_count`` counts every folded trade, including out-of-order.

        Given: A fresh builder,
        When: Two trades fold (the second with an earlier timestamp),
        Then: ``update_count`` is 0, then 2 — it tracks activity regardless
            of whether a trade advances the watermark (the property the
            idle-flush driver relies on).
        """
        m = datetime(2026, 6, 8, 14, 39, tzinfo=UTC)
        b = TradeCandleBuilder()
        assert b.update_count == 0
        b.update(_trade("X-USD", 1.0, 1.0, m))
        b.update(_trade("X-USD", 1.0, 1.0, m - timedelta(minutes=5)))
        assert b.update_count == 2

    def test_event_watermark_boundary_at_and_just_before_cutoff(self) -> None:
        """A bucket closes exactly at watermark = begin + interval + grace.

        Given: Bucket M (interval 60, grace 60 → closes when watermark
            reaches M+120s),
        When: The watermark is advanced to M+119s (one short) then M+120s,
        Then: Nothing closes at M+119s; bucket M closes at exactly M+120s.
        """
        m = datetime(2026, 6, 8, 14, 39, tzinfo=UTC)
        b = TradeCandleBuilder()
        b.update(_trade("X-USD", 1.0, 1.0, m + timedelta(seconds=10)))
        b.update(_trade("X-USD", 1.0, 1.0, m + timedelta(seconds=119)))
        assert b.pop_completed_by_event_watermark(60.0) == []
        b.update(_trade("X-USD", 1.0, 1.0, m + timedelta(seconds=120)))
        candles = b.pop_completed_by_event_watermark(60.0)
        assert [c.interval_begin for c in candles] == [m]

    def test_global_watermark_closes_a_quiet_symbol_bucket(self) -> None:
        """The GLOBAL watermark closes a quiet symbol's bucket via other activity.

        Given: Symbol A trades once in minute M then goes quiet, while symbol
            B trades into M+2,
        When: B's trades advance the global event watermark past
            M + interval + grace and ``pop_completed_by_event_watermark`` runs,
        Then: A's M bucket is closed — no per-symbol stall — proving the
            global (not per-symbol) watermark design. B's later bucket stays.
        """
        m = datetime(2026, 6, 8, 14, 39, tzinfo=UTC)
        b = TradeCandleBuilder()
        b.update(_trade("A-USD", 1.0, 1.0, m + timedelta(seconds=10)))
        b.update(_trade("B-USD", 2.0, 1.0, m + timedelta(minutes=2)))
        candles = b.pop_completed_by_event_watermark(60.0)
        assert {c.symbol for c in candles} == {"A-USD"}
        assert b.active_buckets() == 1

    def test_open_close_track_event_order_not_arrival_order(self) -> None:
        """``open`` / ``close`` follow event-time, not arrival order.

        Regression for delayed out-of-order same-minute batches: a late older
        trade must not overwrite ``close`` and a late earliest trade must set
        ``open``.

        Given: four trades for one minute folded in NON-event order — mid
            (m+30s, 100), latest (m+50s, 105), earliest (m+5s, 95), inner
            (m+40s, 110) — then a trade that advances the watermark,
        When: the minute closes,
        Then: ``open`` = earliest-EVENT price (95), ``close`` = latest-EVENT
            price (105), ``high``=110, ``low``=95 — independent of arrival
            order.
        """
        m = datetime(2026, 6, 8, 14, 39, tzinfo=UTC)
        b = TradeCandleBuilder()
        b.update(_trade("X-USD", 100.0, 1.0, m + timedelta(seconds=30)))
        b.update(_trade("X-USD", 105.0, 1.0, m + timedelta(seconds=50)))
        b.update(_trade("X-USD", 95.0, 1.0, m + timedelta(seconds=5)))
        b.update(_trade("X-USD", 110.0, 1.0, m + timedelta(seconds=40)))
        b.update(_trade("X-USD", 1.0, 1.0, m + timedelta(minutes=2)))
        candles = [c for c in b.pop_completed_by_event_watermark(60.0) if c.interval_begin == m]
        assert len(candles) == 1
        c = candles[0]
        assert c.open == pytest.approx(95.0)
        assert c.close == pytest.approx(105.0)
        assert c.high == pytest.approx(110.0)
        assert c.low == pytest.approx(95.0)

    def test_open_close_tie_semantics_on_equal_timestamps(self) -> None:
        """Equal-timestamp ties: open keeps first-arriving, close takes last.

        Given: three trades at the SAME event timestamp, arriving in sequence
            (100, 105, 110), then a trade that advances the watermark,
        When: the minute closes,
        Then: open = first-arriving (100, strict ``<`` never replaces a tie)
            and close = last-arriving (110, ``>=`` takes the latest tie) —
            the documented tie rule.
        """
        m = datetime(2026, 6, 8, 14, 39, tzinfo=UTC)
        ts = m + timedelta(seconds=30)
        b = TradeCandleBuilder()
        b.update(_trade("X-USD", 100.0, 1.0, ts))
        b.update(_trade("X-USD", 105.0, 1.0, ts))
        b.update(_trade("X-USD", 110.0, 1.0, ts))
        b.update(_trade("X-USD", 1.0, 1.0, m + timedelta(minutes=2)))
        c = [x for x in b.pop_completed_by_event_watermark(60.0) if x.interval_begin == m][0]
        assert c.open == pytest.approx(100.0)
        assert c.close == pytest.approx(110.0)


class TestLateTradesAfterClose:
    """Tests for the late-trade-after-close counter and re-open WARNING.

    A trade landing in an already-emitted ``(symbol, minute)`` produces
    a corrective candle that re-fragments the SCD2 row — the 2026-06
    Kraken Equities fragmentation class. The counter is the agreed
    trigger for widening the grace/idle-flush bound, so these tests pin
    every accounting path: fresh re-open, fold into a re-opened bucket,
    idle flush as a close, per-symbol independence, and the zero
    steady-state.
    """

    def test_counter_zero_in_steady_state(self) -> None:
        """On-time trades never touch the late counter.

        Given: Trades folding into open minutes and a normal
            watermark-driven pop,
        When: No trade targets an already-emitted minute,
        Then: ``late_trades_after_close`` stays 0.
        """
        m = datetime(2026, 6, 8, 14, 39, tzinfo=UTC)
        b = TradeCandleBuilder()
        b.update(_trade("X-USD", 100.0, 1.0, m + timedelta(seconds=10)))
        b.update(_trade("X-USD", 101.0, 1.0, m + timedelta(seconds=40)))
        b.update(_trade("X-USD", 102.0, 1.0, m + timedelta(minutes=3)))
        b.pop_completed_by_event_watermark(60.0)
        assert b.late_trades_after_close == 0

    def test_late_trade_reopens_bucket_counts_and_warns(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        """A late trade for an emitted minute counts and WARNs once.

        Given: Minute M emitted via the event-watermark pop,
        When: A trade for M arrives afterwards,
        Then: The counter increments, a fresh bucket exists for M (the
            corrective candle is not lost), and the re-open WARNING was
            logged exactly once.
        """
        m = datetime(2026, 6, 8, 14, 39, tzinfo=UTC)
        b = TradeCandleBuilder()
        b.update(_trade("X-USD", 100.0, 1.0, m + timedelta(seconds=10)))
        b.update(_trade("X-USD", 102.0, 1.0, m + timedelta(minutes=3)))
        emitted = b.pop_completed_by_event_watermark(60.0)
        assert [c.interval_begin for c in emitted] == [m]
        sink_id = logger.add(caplog.handler, format="{message}", level="WARNING")
        try:
            b.update(_trade("X-USD", 99.0, 1.0, m + timedelta(seconds=50)))
        finally:
            logger.remove(sink_id)
        assert b.late_trades_after_close == 1
        assert b.active_buckets() == 2
        reopens = [rec for rec in caplog.records if "re-opens bucket" in rec.message]
        assert len(reopens) == 1

    def test_fold_into_reopened_bucket_counts_without_second_warning(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        """Subsequent late trades count but do not spam the log.

        Given: A re-opened bucket for an emitted minute,
        When: Another late trade folds into the SAME re-opened bucket,
        Then: The counter increments again but no second re-open
            WARNING fires (the bucket already exists).
        """
        m = datetime(2026, 6, 8, 14, 39, tzinfo=UTC)
        b = TradeCandleBuilder()
        b.update(_trade("X-USD", 100.0, 1.0, m + timedelta(seconds=10)))
        b.update(_trade("X-USD", 102.0, 1.0, m + timedelta(minutes=3)))
        b.pop_completed_by_event_watermark(60.0)
        b.update(_trade("X-USD", 99.0, 1.0, m + timedelta(seconds=50)))
        sink_id = logger.add(caplog.handler, format="{message}", level="WARNING")
        try:
            b.update(_trade("X-USD", 98.0, 1.0, m + timedelta(seconds=55)))
        finally:
            logger.remove(sink_id)
        assert b.late_trades_after_close == 2
        reopens = [rec for rec in caplog.records if "re-opens bucket" in rec.message]
        assert len(reopens) == 0
        final = b.pop_all()
        corrective = [c for c in final if c.interval_begin == m][0]
        assert corrective.trades == 2
        assert corrective.close == pytest.approx(98.0)

    def test_idle_flush_counts_as_close(self) -> None:
        """``pop_all`` closes minutes for late-trade accounting.

        Given: A bucket flushed by the idle path (``pop_all``), the
            exact bypass that can re-fragment after a delivery pause,
        When: A trade for the flushed minute arrives later,
        Then: The counter increments — the idle flush must not be
            invisible to the early-close signal.
        """
        m = datetime(2026, 6, 8, 14, 39, tzinfo=UTC)
        b = TradeCandleBuilder()
        b.update(_trade("X-USD", 100.0, 1.0, m + timedelta(seconds=10)))
        b.pop_all()
        b.update(_trade("X-USD", 99.0, 1.0, m + timedelta(seconds=50)))
        assert b.late_trades_after_close == 1

    def test_late_accounting_is_per_symbol(self) -> None:
        """Closing one symbol's minute does not mark another's late.

        Given: Symbol A's minute M emitted while symbol B never traded,
        When: B trades in minute M and A trades in a LATER minute,
        Then: Neither counts as late — the closed-minute watermark is
            per symbol and only minutes at or before it count.
        """
        m = datetime(2026, 6, 8, 14, 39, tzinfo=UTC)
        b = TradeCandleBuilder()
        b.update(_trade("A-USD", 100.0, 1.0, m + timedelta(seconds=10)))
        b.update(_trade("A-USD", 102.0, 1.0, m + timedelta(minutes=3)))
        b.pop_completed_by_event_watermark(60.0)
        b.update(_trade("B-USD", 50.0, 1.0, m + timedelta(seconds=20)))
        b.update(_trade("A-USD", 103.0, 1.0, m + timedelta(minutes=4)))
        assert b.late_trades_after_close == 0


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
