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


class TestBreakCombination:
    """Breaks the consumer has not seen yet combine into one."""

    def test_a_later_break_keeps_its_generation_and_widens_everything_else(self) -> None:
        """Nothing either break lost is forgotten.

        Given an overflow that kept the builder and a later reset that did not,
        When the first is followed by the second,
        Then the result has the reset's reason and generation, every lost bar,
            the combined range, a builder that was not kept, and the higher floor.
        """
        overflow = TradeCandleBreak(
            reason=TradeCandleBreakReason.OVERFLOW,
            generation=1,
            lost_bars=2,
            lost_from=_MINUTE + timedelta(minutes=1),
            lost_through=_MINUTE + timedelta(minutes=2),
            builder_kept=True,
            arm_after=_MINUTE + timedelta(minutes=3),
        )
        reset = TradeCandleBreak(
            reason=TradeCandleBreakReason.RESET,
            generation=2,
            lost_bars=1,
            lost_from=_MINUTE,
            lost_through=_MINUTE + timedelta(minutes=1),
            builder_kept=False,
            arm_after=_MINUTE + timedelta(minutes=2),
        )

        assert overflow.followed_by(reset) == TradeCandleBreak(
            reason=TradeCandleBreakReason.RESET,
            generation=2,
            lost_bars=3,
            lost_from=_MINUTE,
            lost_through=_MINUTE + timedelta(minutes=2),
            builder_kept=False,
            arm_after=_MINUTE + timedelta(minutes=3),
        )

    def test_missing_ranges_and_floors_take_the_other_value(self) -> None:
        """A break that lost nothing, or had no floor, does not erase the other's.

        Given a break with no lost range and no floor, and one with both,
        When either is followed by the other,
        Then the range and floor are the ones that exist.
        """
        empty = TradeCandleBreak(
            reason=TradeCandleBreakReason.RESET,
            generation=1,
            lost_bars=0,
            lost_from=None,
            lost_through=None,
            builder_kept=False,
            arm_after=None,
        )
        full = TradeCandleBreak(
            reason=TradeCandleBreakReason.RESET,
            generation=2,
            lost_bars=1,
            lost_from=_MINUTE,
            lost_through=_MINUTE,
            builder_kept=False,
            arm_after=_MINUTE,
        )

        for combined in (empty.followed_by(full), full.followed_by(empty)):
            assert (combined.lost_from, combined.lost_through, combined.arm_after) == (
                _MINUTE,
                _MINUTE,
                _MINUTE,
            )
