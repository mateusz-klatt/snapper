"""Tests for the control items of the trade-built candle stream."""

from dataclasses import FrozenInstanceError
from datetime import UTC
from datetime import datetime
from datetime import timedelta

import pytest

from snapper.infrastructure.exchanges.contracts import CandleUpdate
from snapper.infrastructure.exchanges.trade_candle_stream import LostBars
from snapper.infrastructure.exchanges.trade_candle_stream import TradeCandleBreak
from snapper.infrastructure.exchanges.trade_candle_stream import TradeCandleBreakReason
from snapper.infrastructure.exchanges.trade_candle_stream import TradeCandleMark

_MINUTE = datetime(2026, 11, 11, 15, 0, tzinfo=UTC)


def _bar(minute: datetime) -> CandleUpdate:
    """Build one trade-built bar for a minute.

    Args:
        minute: Opening of the bar's minute.

    Returns:
        A complete one-minute bar.
    """
    return CandleUpdate(
        symbol="BTC-EUR",
        open=1.0,
        high=1.0,
        low=1.0,
        close=1.0,
        vwap=1.0,
        trades=1,
        volume=1.0,
        interval_begin=minute,
        interval=60,
    )


class TestLostBars:
    """Counting of the bars a break loses."""

    def test_counts_bars_and_tracks_their_minute_range_in_any_order(self) -> None:
        """The range spans the oldest and newest lost minute, whatever their order.

        Given: An empty record.
        When: Bars of a middle, a newer and an older minute are added.
        Then: Three bars are counted, from the oldest to the newest minute.
        """
        lost = LostBars()

        for offset in (1, 3, 0):
            lost.add(_MINUTE + timedelta(minutes=offset))

        assert (lost.count, lost.oldest, lost.newest) == (
            3,
            _MINUTE,
            _MINUTE + timedelta(minutes=3),
        )

    def test_a_lost_mark_loses_no_evidence(self) -> None:
        """Only bars count; a mark carries no trade evidence of its own.

        Given: An empty record.
        When: A bar and a mark are added as queue items.
        Then: Only the bar is counted.
        """
        lost = LostBars()

        lost.add_item(_bar(_MINUTE))
        lost.add_item(TradeCandleMark(through=_MINUTE, generation=0, produced_at=_MINUTE))

        assert (lost.count, lost.oldest, lost.newest) == (1, _MINUTE, _MINUTE)


class TestStreamItems:
    """The immutable control items."""

    def test_marks_and_breaks_are_immutable(self) -> None:
        """A delivered mark or break cannot be changed by its consumer.

        Given: A mark and a break.
        When: A field of each is assigned.
        Then: Both assignments are refused.
        """
        mark = TradeCandleMark(through=_MINUTE, generation=1, produced_at=_MINUTE)
        brk = TradeCandleBreak(
            reason=TradeCandleBreakReason.OVERFLOW,
            generation=2,
            lost_bars=0,
            lost_from=None,
            lost_through=None,
            builder_kept=True,
            arm_after=None,
        )

        attempts: list[tuple[object, str]] = [(mark, "generation"), (brk, "lost_bars")]
        for item, name in attempts:
            with pytest.raises(FrozenInstanceError):
                setattr(item, name, 2)

    def test_break_reasons_are_stable_strings(self) -> None:
        """The reasons appear in logs and counters under fixed names.

        Given: The break reasons.
        When: Their values are read.
        Then: They are overflow, lag and reset.
        """
        assert [reason.value for reason in TradeCandleBreakReason] == ["overflow", "lag", "reset"]
