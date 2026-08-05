"""Tests for minute completion — the flat bar for an observed tradeless minute.

The load-bearing test in this module is
``TestLivenessGate.test_minute_the_feed_was_not_witnessed_across_emits_nothing``:
a flat bar is an ASSERTION that the venue was live and nobody traded, so a
minute the publisher cannot prove it observed must produce no row at all. Every
other refusal test exists for the same reason — degradation is always toward
today's sparse plane, never toward a fabricated bar.
"""

import asyncio
from dataclasses import dataclass
from datetime import UTC
from datetime import datetime
from typing import cast

import pytest

from snapper.config.app import AppSettings
from snapper.core.types import ExchangeEnum
from snapper.infrastructure.exchanges._subscription_health import SubscriptionStatus
from snapper.infrastructure.exchanges._subscription_health import _SymbolEntry
from snapper.infrastructure.exchanges.contracts import CandleUpdate
from snapper.infrastructure.exchanges.implementations.kraken import KrakenExchangeClient
from snapper.infrastructure.exchanges.implementations.kraken_futures import (
    KrakenFuturesExchangeClient,
)
from snapper.messaging.publishers import base as base_module
from snapper.messaging.publishers.candle_aggregator import CandleAggregator
from snapper.messaging.publishers.kraken import KrakenMarketDataPublisher
from snapper.messaging.publishers.kraken_equities import KrakenEquitiesMarketDataPublisher
from snapper.messaging.publishers.kraken_futures import KrakenFuturesMarketDataPublisher
from snapper.messaging.publishers.minute_completion import EMITTED
from snapper.messaging.publishers.minute_completion import MINUTE_SWEEP_MARGIN_S
from snapper.messaging.publishers.minute_completion import PUBLISH_FAILED
from snapper.messaging.publishers.minute_completion import SKIP_ALREADY_EMITTED
from snapper.messaging.publishers.minute_completion import SKIP_FEED_BREAK
from snapper.messaging.publishers.minute_completion import SKIP_NOT_OBSERVED
from snapper.messaging.publishers.minute_completion import SKIP_UNCONFIRMED
from snapper.messaging.publishers.minute_completion import MinuteCompletionEmitter
from snapper.messaging.publishers.minute_completion import build_flat_minute_bar
from snapper.messaging.publishers.minute_completion import confirmed_trade_symbols
from snapper.messaging.publishers.walutomat import WalutomatMarketDataPublisher
from snapper.messaging.schemas.data import CandleData

_SYMBOL = "BTC-USD"
_SETTLE_S = 8.0
_SPOT_SETTLE_S = 20.0
_MINUTE = 60
_BASE = int(datetime(2026, 8, 4, 12, 0, tzinfo=UTC).timestamp())
"""UNIX start of the reference minute every test sweeps."""


class _Roster:
    """Mutable stand-in for the publisher's confirmed-trade-subscription roster."""

    def __init__(self, symbols: set[str]) -> None:
        """Store the initially confirmed symbols.

        Args:
            symbols: Native symbols to report as confirmed.
        """
        self.symbols = symbols

    def __call__(self) -> set[str]:
        """Return the currently confirmed symbols.

        Returns:
            The confirmed native symbols.
        """
        return self.symbols


class _Switch:
    """Mutable stand-in for the DB-backed minute-completion kill switch."""

    def __init__(self, on: bool) -> None:
        """Store the initial switch position.

        Args:
            on: Whether minute completion starts switched on.
        """
        self.on = on

    def __call__(self) -> bool:
        """Return the current switch position.

        Returns:
            True while minute completion is switched on.
        """
        return self.on


class _Recorder:
    """Capture published market-data messages in place of the ZMQ publisher."""

    def __init__(self) -> None:
        """Initialize the empty capture log."""
        self.published: list[tuple[str, CandleData]] = []

    async def __call__(self, topic: str, message: CandleData) -> None:
        """Record one published message.

        Args:
            topic: ZMQ topic the publisher targeted.
            message: Published candle payload.
        """
        self.published.append((topic, message))


class _CompletionSettings:
    """Settings double exposing only the keys the emitter wiring reads."""

    def __init__(
        self,
        *,
        minute_completion: bool = True,
        candle_source: str = "trade_built",
        finalize_grace: int = 12,
    ) -> None:
        """Store the settings the Kraken minute-completion hooks read.

        Args:
            minute_completion: Value of ``candle_minute_completion``.
            candle_source: Value of ``spot_candle_source``.
            finalize_grace: Value of ``trade_built_finalize_grace_seconds``.
        """
        self.candle_minute_completion = minute_completion
        self.spot_candle_source = candle_source
        self.trade_built_finalize_grace_seconds = finalize_grace


async def _resolve_instrument(native_symbol: str) -> str:
    """Resolve any native symbol to a fixed instrument identity.

    Args:
        native_symbol: Native symbol the publisher is resolving.

    Returns:
        A fixed instrument public id.
    """
    del native_symbol
    return "inst-1"


def _make_emitter(
    *,
    roster: _Roster | None = None,
    switch: _Switch | None = None,
    settle: float = _SETTLE_S,
) -> MinuteCompletionEmitter:
    """Build an emitter wired to test doubles.

    Args:
        roster: Confirmed-subscription roster double; defaults to one symbol.
        switch: Kill-switch double; defaults to switched on.
        settle: Settle slack before a minute is swept.

    Returns:
        A configured emitter with no evidence recorded yet.
    """
    return MinuteCompletionEmitter(
        roster=roster if roster is not None else _Roster({_SYMBOL}),
        enabled=switch if switch is not None else _Switch(True),
        settle_seconds=settle,
        resubscribe_settle_seconds=90.0,
    )


def _witness_minute(emitter: MinuteCompletionEmitter, minute: int) -> None:
    """Record an unbroken stream of frames across one whole minute.

    Args:
        emitter: Emitter receiving the witness frames.
        minute: UNIX start of the minute to witness.
    """
    stamp = float(minute)
    while stamp < minute + _MINUTE:
        emitter.observe_feed_frame(stamp)
        stamp += 4.0


def _witness_live_around(emitter: MinuteCompletionEmitter, minute: int) -> None:
    """Witness the swept minute plus the minute either side of it.

    Args:
        emitter: Emitter receiving the witness frames.
        minute: UNIX start of the minute under test.
    """
    for offset in (-_MINUTE, 0, _MINUTE):
        _witness_minute(emitter, minute + offset)


def _real_bar(
    minute: int,
    *,
    close: float = 105.0,
    vwap: float = 100.0,
    volume: float = 10.0,
    trades: int = 3,
) -> CandleUpdate:
    """Build a real trade-built 1m bar with a fixed 90..110 price envelope.

    Args:
        minute: UNIX start of the bar's minute.
        close: Bar close.
        vwap: Bar volume-weighted average price.
        volume: Bar volume.
        trades: Number of fills in the bar.

    Returns:
        A complete 1m :class:`CandleUpdate`.
    """
    return CandleUpdate(
        symbol=_SYMBOL,
        open=100.0,
        high=110.0,
        low=90.0,
        close=close,
        vwap=vwap,
        trades=trades,
        volume=volume,
        interval_begin=datetime.fromtimestamp(minute, UTC),
        interval=_MINUTE,
        complete=True,
    )


@dataclass(frozen=True)
class _Envelope:
    """Explicit OHLCV envelope for a rollup fixture bar.

    Attributes:
        open_price: Bar open.
        high: Bar high.
        low: Bar low.
        close: Bar close.
        vwap: Bar volume-weighted average price.
        volume: Bar volume.
        trades: Number of fills in the bar.
    """

    open_price: float
    high: float
    low: float
    close: float
    vwap: float
    volume: float
    trades: int


def _shaped_bar(minute: int, envelope: _Envelope) -> CandleUpdate:
    """Build a real 1m bar with an explicit price envelope.

    Args:
        minute: UNIX start of the bar's minute.
        envelope: OHLCV values for the bar.

    Returns:
        A complete 1m :class:`CandleUpdate`.
    """
    return CandleUpdate(
        symbol=_SYMBOL,
        open=envelope.open_price,
        high=envelope.high,
        low=envelope.low,
        close=envelope.close,
        vwap=envelope.vwap,
        trades=envelope.trades,
        volume=envelope.volume,
        interval_begin=datetime.fromtimestamp(minute, UTC),
        interval=_MINUTE,
        complete=True,
    )


def _prime_and_sweep(emitter: MinuteCompletionEmitter, minute: int) -> list[CandleUpdate]:
    """Park the sweep cursor on the previous minute, then sweep ``minute``.

    The first call after construction never emits — the cursor is SEEDED rather
    than re-derived from the clock — so a test that wants ``minute`` swept has to
    step the cursor onto it first.

    Args:
        emitter: Emitter under test, built with the default settle.
        minute: UNIX start of the minute to sweep.

    Returns:
        Flat bars produced for ``minute``.
    """
    emitter.due_flat_bars(float(minute) + _SETTLE_S + 2.0)
    return emitter.due_flat_bars(float(minute) + _MINUTE + _SETTLE_S + 2.0)


def _sweep_at(emitter: MinuteCompletionEmitter, minute: int) -> list[CandleUpdate]:
    """Sweep one minute on an emitter whose cursor is already primed.

    Args:
        emitter: Emitter under test, built with the default settle.
        minute: UNIX start of the minute to sweep.

    Returns:
        Flat bars produced for ``minute``.
    """
    return emitter.due_flat_bars(float(minute) + _MINUTE + _SETTLE_S + 2.0)


def _begin(minute: int) -> datetime:
    """Return the UTC interval-begin for a minute timestamp.

    Args:
        minute: UNIX start of the minute.

    Returns:
        The canonical UTC minute start.
    """
    return datetime.fromtimestamp(minute, UTC)


class TestFlatBarShape:
    """The bar a tradeless minute produces."""

    def test_flat_bar_is_flat_at_the_carried_close_with_zero_volume(self) -> None:
        """A flat minute bar states emptiness exactly as the 5m..1d planes do.

        Given a carried close of 105.0 for a minute the instrument did not trade,
        When the flat bar is built,
        Then every price column is that close, volume is 0.0, trades is 0 and the
            bar is complete — byte-identical in shape to the aggregator's own
            forward-filled higher-timeframe bar, so no new row shape enters the
            corpus.
        """
        bar = build_flat_minute_bar(_SYMBOL, _begin(_BASE), 105.0)
        assert bar.open == 105.0
        assert bar.high == 105.0
        assert bar.low == 105.0
        assert bar.close == 105.0
        assert bar.vwap == 105.0
        assert bar.volume == 0.0
        assert bar.trades == 0
        assert bar.complete is True
        assert bar.interval == 60
        assert bar.interval_begin == _begin(_BASE)


class TestTradelessMinute:
    """A witnessed minute in which the instrument did not trade."""

    def test_tradeless_minute_on_a_live_feed_emits_one_flat_bar(self) -> None:
        """The operator's requirement: every minute boundary yields a candle.

        Given a feed witnessed edge to edge across the minute and a symbol whose
            last real bar closed at 105.0 in the preceding minute,
        When the settled minute is swept,
        Then exactly one flat bar is produced for that minute, flat at 105.0 with
            zero volume, zero trades and complete=True.
        """
        emitter = _make_emitter()
        _witness_live_around(emitter, _BASE)
        emitter.observe_real_bar(_real_bar(_BASE - _MINUTE, close=105.0))
        bars = _prime_and_sweep(emitter, _BASE)
        assert len(bars) == 1
        assert bars[0].symbol == _SYMBOL
        assert bars[0].interval_begin == _begin(_BASE)
        assert bars[0].close == 105.0
        assert bars[0].volume == 0.0
        assert bars[0].trades == 0
        assert bars[0].complete is True

    def test_run_of_tradeless_minutes_stays_flat_rather_than_drifting(self) -> None:
        """A carried close is copied forward, never recomputed.

        Given a first tradeless minute already filled at the carried close,
        When the following minute is also tradeless and swept,
        Then its bar carries the same close, so a run of empty minutes is a flat
            line rather than a drift.
        """
        emitter = _make_emitter()
        _witness_live_around(emitter, _BASE)
        _witness_minute(emitter, _BASE + 2 * _MINUTE)
        emitter.observe_real_bar(_real_bar(_BASE - _MINUTE, close=105.0))
        first = _prime_and_sweep(emitter, _BASE)
        second = _sweep_at(emitter, _BASE + _MINUTE)
        assert [bar.close for bar in first] == [105.0]
        assert [bar.close for bar in second] == [105.0]
        assert second[0].interval_begin == _begin(_BASE + _MINUTE)


class TestLivenessGate:
    """The honesty boundary: no live evidence, no assertion."""

    def test_minute_the_feed_was_not_witnessed_across_emits_nothing(self) -> None:
        """Without proof the venue was live, no bar may be written at all.

        Given a symbol with a known close and a feed witnessed in the minutes
            either side but NOT during the minute itself,
        When that minute is swept,
        Then nothing is emitted and the refusal is counted, because the row
            would otherwise assert "the venue was live and nobody traded" about a
            minute the publisher cannot prove it observed.
        """
        emitter = _make_emitter()
        _witness_minute(emitter, _BASE - _MINUTE)
        _witness_minute(emitter, _BASE + _MINUTE)
        emitter.observe_real_bar(_real_bar(_BASE - _MINUTE))
        assert _prime_and_sweep(emitter, _BASE) == []
        assert emitter.counters()[SKIP_NOT_OBSERVED] == 1

    def test_minute_with_an_internal_feed_stall_emits_nothing(self) -> None:
        """A socket that went quiet mid-minute did not witness the whole minute.

        Given frames at both edges of the minute but a 22-second hole in the
            middle,
        When the minute is swept,
        Then nothing is emitted: edge coverage alone would have passed, and a
            reconnect that drops and returns inside a single minute is exactly
            the case edge coverage cannot see.
        """
        emitter = _make_emitter()
        _witness_minute(emitter, _BASE - _MINUTE)
        _witness_minute(emitter, _BASE + _MINUTE)
        for offset in (0.0, 4.0, 8.0, 30.0, 34.0, 56.0):
            emitter.observe_feed_frame(float(_BASE) + offset)
        emitter.observe_real_bar(_real_bar(_BASE - _MINUTE))
        assert _prime_and_sweep(emitter, _BASE) == []
        assert emitter.counters()[SKIP_NOT_OBSERVED] == 1

    def test_minute_whose_predecessor_is_dark_emits_nothing(self) -> None:
        """The connection has to be shown to predate the minute it describes.

        Given a feed witnessed during the minute and the one after but not the
            one before,
        When the minute is swept,
        Then nothing is emitted — this is the first fully-observed minute after a
            connect, and its predecessor's silence is unexplained.
        """
        emitter = _make_emitter()
        _witness_minute(emitter, _BASE)
        _witness_minute(emitter, _BASE + _MINUTE)
        emitter.observe_real_bar(_real_bar(_BASE - _MINUTE))
        assert _prime_and_sweep(emitter, _BASE) == []

    def test_minute_whose_successor_is_dark_emits_nothing(self) -> None:
        """A feed that died at the boundary did not survive the minute.

        Given a feed witnessed before and during the minute but silent after it,
        When the minute is swept,
        Then nothing is emitted, because a socket that stopped delivering at the
            boundary may have stopped just inside it.
        """
        emitter = _make_emitter()
        _witness_minute(emitter, _BASE - _MINUTE)
        _witness_minute(emitter, _BASE)
        emitter.observe_real_bar(_real_bar(_BASE - _MINUTE))
        assert _prime_and_sweep(emitter, _BASE) == []

    def test_detected_feed_break_bars_every_overlapping_minute(self) -> None:
        """A reconnect the publisher knows about is never papered over.

        Given a live-looking witness across the minute but a feed break marked
            while it was in progress,
        When the minute is swept,
        Then nothing is emitted and the refusal is counted as a feed break — the
            subscription tracker resets its own staleness clock on re-confirm, so
            "looks fresh" is not evidence and the break has to be recorded
            explicitly.
        """
        emitter = _make_emitter()
        _witness_live_around(emitter, _BASE)
        emitter.observe_real_bar(_real_bar(_BASE - _MINUTE))
        emitter.mark_feed_break(float(_BASE) + 30.0)
        assert _prime_and_sweep(emitter, _BASE) == []
        assert emitter.counters()[SKIP_FEED_BREAK] == 1

    def test_minute_after_the_break_window_closes_is_assertable_again(self) -> None:
        """A break window expires; it does not disable the emitter for good.

        Given a feed break marked long enough before the minute that its settle
            window has closed,
        When the minute is swept,
        Then the flat bar is emitted normally.
        """
        emitter = _make_emitter()
        _witness_live_around(emitter, _BASE)
        emitter.observe_real_bar(_real_bar(_BASE - _MINUTE))
        emitter.mark_feed_break(float(_BASE) - 200.0)
        assert len(_prime_and_sweep(emitter, _BASE)) == 1


class TestPerSymbolGates:
    """Per-symbol arming: subscription confirmation and an in-session close."""

    def test_symbol_with_no_real_bar_yet_never_gets_a_flat_bar(self) -> None:
        """Before the first print there is no observed price to carry.

        Given a fully witnessed minute and a confirmed subscription but no real
            bar ever seen for the symbol in this process,
        When the minute is swept,
        Then nothing is emitted: the last close is never seeded from the durable
            plane, so a fresh process cannot assert a price it never observed.
        """
        emitter = _make_emitter()
        _witness_live_around(emitter, _BASE)
        assert _prime_and_sweep(emitter, _BASE) == []
        assert emitter.counters()[EMITTED] == 0

    def test_symbol_whose_trade_subscription_is_unconfirmed_gets_nothing(self) -> None:
        """Silence on a channel nobody is listening to is not an observation.

        Given a symbol with a known close but absent from the confirmed-trade
            roster (its subscription is pending after a replay, say),
        When the minute is swept,
        Then nothing is emitted for it and the refusal is counted.
        """
        emitter = _make_emitter(roster=_Roster(set()))
        _witness_live_around(emitter, _BASE)
        emitter.observe_real_bar(_real_bar(_BASE - _MINUTE))
        assert _prime_and_sweep(emitter, _BASE) == []
        assert emitter.counters()[SKIP_UNCONFIRMED] == 1

    def test_symbol_rejoining_the_roster_resumes_without_retro_filling(self) -> None:
        """Re-confirmation re-arms the symbol but never back-fills the hole.

        Given a symbol held out of one minute by an unconfirmed subscription,
        When it is back on the roster for the following minute,
        Then only the following minute is emitted — the held-out minute has no
            second chance and stays absent, which is the correct record.
        """
        roster = _Roster(set())
        emitter = _make_emitter(roster=roster)
        _witness_live_around(emitter, _BASE)
        _witness_minute(emitter, _BASE + 2 * _MINUTE)
        emitter.observe_real_bar(_real_bar(_BASE - _MINUTE))
        assert _prime_and_sweep(emitter, _BASE) == []
        roster.symbols = {_SYMBOL}
        resumed = _sweep_at(emitter, _BASE + _MINUTE)
        assert [bar.interval_begin for bar in resumed] == [_begin(_BASE + _MINUTE)]


class TestNoDoubleEmit:
    """A minute is asserted at most once, and never over a real bar."""

    def test_real_bar_for_the_swept_minute_suppresses_the_flat_bar(self) -> None:
        """The real bar wins; the two never collide on the same natural key.

        Given a real bar already observed for the minute being swept,
        When the sweep runs,
        Then no flat bar is produced for it, so the SCD2 natural key
            (instrument, '1m', open_at) is never contested by two different
            OHLCV values.
        """
        emitter = _make_emitter()
        _witness_live_around(emitter, _BASE)
        emitter.observe_real_bar(_real_bar(_BASE, close=111.0))
        assert _prime_and_sweep(emitter, _BASE) == []
        assert emitter.counters()[SKIP_ALREADY_EMITTED] == 1

    def test_second_sweep_of_the_same_settled_minute_emits_nothing(self) -> None:
        """The sweep runs many times a second; only the first one asserts.

        Given a minute already swept,
        When the sweep runs again while the same minute is still the newest
            settled one,
        Then nothing further is emitted.
        """
        emitter = _make_emitter()
        _witness_live_around(emitter, _BASE)
        emitter.observe_real_bar(_real_bar(_BASE - _MINUTE))
        assert len(_prime_and_sweep(emitter, _BASE)) == 1
        assert emitter.due_flat_bars(float(_BASE) + _MINUTE + _SETTLE_S + 9.0) == []

    def test_out_of_order_real_bar_does_not_rewind_the_floor(self) -> None:
        """A late correction for an older minute cannot restate a newer one.

        Given a real bar recorded for the minute and then a later-arriving bar
            for the PREVIOUS minute,
        When the following minute is swept,
        Then the carried close is still the newer bar's close.
        """
        emitter = _make_emitter()
        _witness_live_around(emitter, _BASE)
        _witness_minute(emitter, _BASE + 2 * _MINUTE)
        emitter.observe_real_bar(_real_bar(_BASE, close=222.0))
        emitter.observe_real_bar(_real_bar(_BASE - _MINUTE, close=111.0))
        emitter.due_flat_bars(float(_BASE + _MINUTE) + _SETTLE_S + 2.0)
        bars = _sweep_at(emitter, _BASE + _MINUTE)
        assert [bar.close for bar in bars] == [222.0]


class TestCursorTraps:
    """The Walutomat boundary-cursor traps this sweep inherits.

    The Walutomat per-poll candle loop learned three of these the hard way. Two
    carry over verbatim (a SEEDED cursor, and a backward clock step that stalls
    rather than rewinds); the third INVERTS, because a flat bar is an assertion
    rather than a reconstruction, so Walutomat's unbounded catch-up over every
    elapsed boundary is exactly the wrong behaviour here.
    """

    def test_first_sweep_after_construction_never_emits(self) -> None:
        """The cursor is seeded from state, never re-derived from the clock.

        Given a fresh emitter with full evidence for a settled minute,
        When the very first sweep runs,
        Then nothing is emitted: a cursor derived from the clock on the first
            tick would assert a minute the emitter was not running for.
        """
        emitter = _make_emitter()
        _witness_live_around(emitter, _BASE)
        emitter.observe_real_bar(_real_bar(_BASE - _MINUTE))
        assert emitter.due_flat_bars(float(_BASE) + _MINUTE + _SETTLE_S + 2.0) == []

    def test_backward_clock_step_stalls_instead_of_re_emitting(self) -> None:
        """A clock that goes backwards must not republish a swept minute.

        Given a minute already swept,
        When the wall clock steps back so a much older minute looks settled,
        Then nothing is emitted and the cursor does not rewind — the following
            forward sweep still emits only the next minute, never a re-run of
            the stretch in between.
        """
        emitter = _make_emitter()
        _witness_live_around(emitter, _BASE)
        _witness_minute(emitter, _BASE + 2 * _MINUTE)
        emitter.observe_real_bar(_real_bar(_BASE - _MINUTE))
        assert len(_prime_and_sweep(emitter, _BASE)) == 1
        assert emitter.due_flat_bars(float(_BASE) - 120.0) == []
        forward = _sweep_at(emitter, _BASE + _MINUTE)
        assert [bar.interval_begin for bar in forward] == [_begin(_BASE + _MINUTE)]

    def test_long_stall_asserts_only_the_newest_minute_and_never_catches_up(self) -> None:
        """Catch-up is right for reconstruction and wrong for assertion.

        Given a sweep that has not run for five minutes while the feed stayed
            live,
        When it finally runs,
        Then exactly one bar is emitted, for the newest settled minute only. The
            skipped minutes are never back-filled: the sweep considers a single
            minute, so a minute passed over has no second chance by construction.
        """
        emitter = _make_emitter()
        for index in range(-1, 8):
            _witness_minute(emitter, _BASE + index * _MINUTE)
        emitter.observe_real_bar(_real_bar(_BASE - _MINUTE))
        emitter.due_flat_bars(float(_BASE) + _SETTLE_S + 2.0)
        bars = _sweep_at(emitter, _BASE + 5 * _MINUTE)
        assert [bar.interval_begin for bar in bars] == [_begin(_BASE + 5 * _MINUTE)]

    def test_switched_off_emitter_advances_its_cursor_without_asserting(self) -> None:
        """Turning the switch back on starts at now, not at a backlog.

        Given the kill switch off across a minute,
        When it is switched back on for the following minute,
        Then only the following minute is emitted, so a long disabled stretch
            cannot flood the plane on re-enable.
        """
        switch = _Switch(False)
        emitter = _make_emitter(switch=switch)
        _witness_live_around(emitter, _BASE)
        _witness_minute(emitter, _BASE + 2 * _MINUTE)
        emitter.observe_real_bar(_real_bar(_BASE - _MINUTE))
        assert _prime_and_sweep(emitter, _BASE) == []
        switch.on = True
        resumed = _sweep_at(emitter, _BASE + _MINUTE)
        assert [bar.interval_begin for bar in resumed] == [_begin(_BASE + _MINUTE)]

    def test_witness_ring_stays_bounded(self) -> None:
        """Witness history is a bounded ring, not a growing log.

        Given far more witnessed minutes than the retention window,
        When they have all been recorded,
        Then only a bounded number of spans is retained.
        """
        emitter = _make_emitter()
        for index in range(60):
            emitter.observe_feed_frame(float(_BASE + index * _MINUTE))
        assert len(emitter._observed) <= 9


class TestSweepCounters:
    """Every refusal is legible on the operator surface."""

    def test_counters_name_the_gate_that_refused(self) -> None:
        """A hole in the plane has to be explainable, not merely asserted.

        Given a sweep refused because the minute was not witnessed,
        When the counters are read,
        Then they name that gate and report the eligible-symbol population and
            the last minute considered.
        """
        emitter = _make_emitter()
        _witness_minute(emitter, _BASE - _MINUTE)
        _witness_minute(emitter, _BASE + _MINUTE)
        emitter.observe_real_bar(_real_bar(_BASE - _MINUTE))
        _prime_and_sweep(emitter, _BASE)
        counters = emitter.counters()
        assert counters[SKIP_NOT_OBSERVED] == 1
        assert counters["known_closes"] == 1
        assert counters["last_swept_minute"] == _begin(_BASE).isoformat()


class TestPublisherRosterHooks:
    """The hooks that bind the roster gate to the live subscription tracker.

    The rest of this module exercises ``confirmed_trade_symbols`` directly and
    injects a roster double into the emitter, so without these tests the wiring
    from publisher to tracker — the link the whole liveness argument rests on —
    is never executed. They also cover the base-class defaults, which the
    venue subclasses override but which any future publisher inherits.
    """

    def test_base_publisher_names_nobody(self) -> None:
        """A publisher with no tracker must arm no symbol at all.

        Given the base implementation,
        When the roster is read,
        Then it is empty — a publisher that cannot observe per-symbol
            subscription health has no grounds to assert silence for any
            symbol, so the safe default is to name nobody rather than to
            fall back to the symbol mapper.
        """
        publisher = WalutomatMarketDataPublisher(symbols=[])
        assert publisher._confirmed_trade_roster() == set()

    def test_base_settle_is_the_bare_scheduling_margin(self) -> None:
        """Futures inherits this value, and it is the first venue to be enabled.

        Given the base implementation,
        When the settle slack is read,
        Then it is the bare margin — correct for a venue that finalizes a
            minute the instant it ends, which futures does: its aggregator
            pops completed buckets against an un-graced wall clock.
        """
        publisher = WalutomatMarketDataPublisher(symbols=[])
        assert publisher._minute_sweep_settle_seconds() == MINUTE_SWEEP_MARGIN_S

    def test_spot_settle_adds_the_venues_own_finalize_grace(self) -> None:
        """Spot pops buckets a grace period late, so the sweep must wait longer.

        Given a Spot publisher whose finalize grace is 12 seconds,
        When the settle slack is read,
        Then it is that grace plus the scheduling margin — sweeping earlier
            would race a real bar for the same minute.
        """
        publisher = KrakenMarketDataPublisher(symbols=["*"])
        publisher.settings = cast(
            AppSettings, _CompletionSettings(candle_source="trade_built", finalize_grace=12)
        )
        assert publisher._minute_sweep_settle_seconds() == 12.0 + MINUTE_SWEEP_MARGIN_S

    def test_spot_roster_is_empty_before_the_client_exists(self) -> None:
        """Startup ordering must not raise, and must not arm anything.

        Given a Spot publisher whose exchange client is not yet built,
        When the roster is read,
        Then it is empty rather than an error.
        """
        publisher = KrakenMarketDataPublisher(symbols=["*"])
        publisher._exchange_client = None
        assert publisher._confirmed_trade_roster() == set()

    def test_spot_roster_reads_the_live_tracker(self) -> None:
        """The binding under test: publisher hook to tracker to native symbols.

        Given a Spot client whose tracker holds a confirmed ticker entry and
            an unmappable trade entry,
        When the roster is read,
        Then nothing is armed — the ticker entry because ticker confirmation
            must never arm a symbol, the trade entry because an unmappable
            wire symbol is skipped rather than raised.

        Deliberately asserts a refusal rather than a mapping: the venue hook
        resolves through the DB-backed symbol mapper, so a positive case would
        pin this test to whichever aliases the fixture seeds. What needs
        covering here is the binding itself — the hook reaching a live tracker
        and projecting it — which this exercises end to end.
        """
        publisher = KrakenMarketDataPublisher(symbols=["*"])
        publisher._exchange_client = cast(
            KrakenExchangeClient,
            _TrackerStub(
                {
                    ("ticker", "XBT/USD"): _health_entry(channel="ticker"),
                    ("trade", "UNKNOWN/PAIR"): _health_entry(symbol="UNKNOWN/PAIR"),
                }
            ),
        )
        assert publisher._confirmed_trade_roster() == set()

    def test_futures_roster_is_empty_before_the_client_exists(self) -> None:
        """Startup ordering must not raise on the first venue to be enabled.

        Given a Futures publisher whose exchange client is not yet built,
        When the roster is read,
        Then it is empty rather than an error.
        """
        publisher = KrakenFuturesMarketDataPublisher(symbols=[])
        publisher._exchange_client = None
        assert publisher._confirmed_trade_roster() == set()

    def test_futures_roster_reads_the_live_tracker(self) -> None:
        """Futures binds to its own tracker with its own symbol mapping.

        Given a Futures client whose tracker holds a ticker entry and an
            unmappable trade entry,
        When the roster is read,
        Then nothing is armed.

        Scoped the same way as the Spot case above and for the same reason:
        the venue mappers resolve through the DB-backed symbol mapper, so a
        positive assertion here would pin the test to whichever aliases the
        fixture happens to seed. The projection itself is proved symbol by
        symbol in ``TestConfirmedTradeRoster``.
        """
        publisher = KrakenFuturesMarketDataPublisher(symbols=[])
        publisher._exchange_client = cast(
            KrakenFuturesExchangeClient,
            _TrackerStub(
                {
                    ("ticker", "PF_XBTUSD"): _health_entry(channel="ticker", symbol="PF_XBTUSD"),
                    ("trade", "UNKNOWN_PAIR"): _health_entry(symbol="UNKNOWN_PAIR"),
                }
            ),
        )
        assert publisher._confirmed_trade_roster() == set()


class _TrackerStub:
    """Minimal exchange-client stand-in exposing only the health snapshot."""

    def __init__(self, snapshot: dict[tuple[str, str], _SymbolEntry]) -> None:
        """Store the snapshot the publisher hook will read.

        Args:
            snapshot: Tracker rows keyed by ``(channel, wire symbol)``.
        """
        self._snapshot = snapshot

    def subscription_health_snapshot(self) -> dict[tuple[str, str], _SymbolEntry]:
        """Return the stored snapshot.

        Returns:
            Tracker rows keyed by ``(channel, wire symbol)``.
        """
        return self._snapshot


def _health_entry(
    *,
    channel: str = "trade",
    status: SubscriptionStatus = "confirmed",
    quarantined: bool = False,
    ever_confirmed: bool = True,
    symbol: str = "XBT/USD",
) -> _SymbolEntry:
    """Build a subscription-health entry for roster projection tests.

    Args:
        channel: Tracker channel key.
        status: Subscription lifecycle status.
        quarantined: Whether the entry is quarantined.
        ever_confirmed: Lifetime confirmation flag.
        symbol: Wire-format symbol.

    Returns:
        A populated entry.
    """
    return _SymbolEntry(
        channel=channel,
        symbol=symbol,
        status=status,
        requested_at=1.0,
        confirmed_at=2.0,
        quarantined=quarantined,
        ever_confirmed=ever_confirmed,
    )


def _to_native(wire_symbol: str) -> str:
    """Map a fixture wire symbol to native, rejecting one unmappable value.

    Args:
        wire_symbol: Wire-format symbol.

    Returns:
        The native symbol.

    Raises:
        ValueError: When the wire symbol is the unmappable fixture value.
    """
    if wire_symbol == "UNKNOWN/PAIR":
        raise ValueError(wire_symbol)
    return wire_symbol.replace("XBT", "BTC").replace("/", "-")


class TestConfirmedTradeRoster:
    """Projecting subscription health to an armed symbol set."""

    def test_confirmed_trade_entries_map_to_native_symbols(self) -> None:
        """A confirmed trade subscription arms its symbol.

        Given a confirmed trade entry,
        When the roster is projected,
        Then the native symbol is armed.
        """
        snapshot = {("trade", "XBT/USD"): _health_entry()}
        assert confirmed_trade_symbols(snapshot, _to_native) == {"BTC-USD"}

    def test_ticker_entries_are_ignored(self) -> None:
        """Ticker confirmation must never arm a symbol.

        Given only a confirmed TICKER entry,
        When the roster is projected,
        Then nothing is armed — a reconnect re-seeds the wildcard ticker
            universe as confirmed while trade entries drop back to pending, so
            reading ticker would arm the whole universe on a connection whose
            trade subscriptions have not returned.
        """
        snapshot = {("ticker", "XBT/USD"): _health_entry(channel="ticker")}
        assert confirmed_trade_symbols(snapshot, _to_native) == set()

    def test_pending_entries_are_ignored(self) -> None:
        """A subscription awaiting its ACK arms nothing.

        Given a trade entry still pending after a reconnect replay,
        When the roster is projected,
        Then nothing is armed.
        """
        snapshot = {("trade", "XBT/USD"): _health_entry(status="pending")}
        assert confirmed_trade_symbols(snapshot, _to_native) == set()

    def test_failed_entries_are_ignored(self) -> None:
        """A failed subscription arms nothing.

        Given a trade entry whose subscription failed,
        When the roster is projected,
        Then nothing is armed.
        """
        snapshot = {("trade", "XBT/USD"): _health_entry(status="failed")}
        assert confirmed_trade_symbols(snapshot, _to_native) == set()

    def test_quarantined_entries_are_ignored(self) -> None:
        """A quarantined never-streamer arms nothing.

        Given a trade entry the tracker has quarantined,
        When the roster is projected,
        Then nothing is armed, so a delisted pair drops out of the sweep on its
            own rather than needing a stale-carry timeout.
        """
        snapshot = {("trade", "XBT/USD"): _health_entry(quarantined=True)}
        assert confirmed_trade_symbols(snapshot, _to_native) == set()

    def test_never_confirmed_entries_are_ignored(self) -> None:
        """An entry that has never confirmed in its life arms nothing.

        Given a trade entry with the lifetime confirmation flag unset,
        When the roster is projected,
        Then nothing is armed.
        """
        snapshot = {("trade", "XBT/USD"): _health_entry(ever_confirmed=False)}
        assert confirmed_trade_symbols(snapshot, _to_native) == set()

    def test_unmappable_wire_symbols_are_skipped(self) -> None:
        """An unknown wire symbol is dropped rather than raising.

        Given a confirmed entry whose wire symbol has no native mapping,
        When the roster is projected,
        Then it is skipped and the healthy entry still arms.
        """
        snapshot = {
            ("trade", "UNKNOWN/PAIR"): _health_entry(symbol="UNKNOWN/PAIR"),
            ("trade", "XBT/USD"): _health_entry(),
        }
        assert confirmed_trade_symbols(snapshot, _to_native) == {"BTC-USD"}


class TestHigherTimeframeRollup:
    """Rolling a 5m window that contains flat bars."""

    @staticmethod
    def _aggregator(window_start: int) -> CandleAggregator:
        """Build a 5m aggregator whose live epoch predates the window.

        Args:
            window_start: UNIX start of the 5m window under test.

        Returns:
            A configured aggregator.
        """
        return CandleAggregator(["5m"], live_epoch=_begin(window_start - _MINUTE))

    @staticmethod
    def _close_window(aggregator: CandleAggregator, window_start: int) -> list[CandleUpdate]:
        """Advance the watermark past the window and collect its 5m bars.

        Two further minutes are folded because the aggregator's watermark is the
        START of the last FINALIZED minute, and a minute is finalized only once a
        strictly later one arrives.

        Args:
            aggregator: Aggregator holding the window.
            window_start: UNIX start of the 5m window.

        Returns:
            The 5m bars emitted while closing the window.
        """
        emitted: list[CandleUpdate] = []
        for minute in (window_start + 300, window_start + 360):
            emitted += [candle for _label, candle in aggregator.fold(_real_bar(minute))]
        return emitted

    def test_rollup_keeps_vwap_volume_and_trade_count_exact(self) -> None:
        """Flat bars contribute nothing to the rolled-up volume statistics.

        Given a 5m window of two real minutes (volume 10 at vwap 100, volume 30
            at vwap 200) and three flat minutes,
        When the window closes,
        Then volume is 40.0, trades is 10 and vwap is exactly 175.0 — a
            zero-volume bar adds zero to both the numerator and the denominator
            of the volume-weighted average, so the rollup is arithmetically
            identical to one computed without the flat bars.
        """
        window = _BASE
        aggregator = self._aggregator(window)
        aggregator.fold(
            _shaped_bar(
                window,
                _Envelope(
                    open_price=100.0,
                    high=110.0,
                    low=90.0,
                    close=105.0,
                    vwap=100.0,
                    volume=10.0,
                    trades=3,
                ),
            )
        )
        aggregator.fold(build_flat_minute_bar(_SYMBOL, _begin(window + 60), 105.0))
        aggregator.fold(
            _shaped_bar(
                window + 120,
                _Envelope(
                    open_price=105.0,
                    high=210.0,
                    low=105.0,
                    close=200.0,
                    vwap=200.0,
                    volume=30.0,
                    trades=7,
                ),
            )
        )
        for minute in (window + 180, window + 240):
            aggregator.fold(build_flat_minute_bar(_SYMBOL, _begin(minute), 200.0))
        bars = self._close_window(aggregator, window)
        assert len(bars) == 1
        assert bars[0].volume == 40.0
        assert bars[0].trades == 10
        assert bars[0].vwap == 175.0
        assert bars[0].open == 100.0
        assert bars[0].close == 200.0

    def test_window_opening_on_a_flat_bar_opens_at_the_carried_close(self) -> None:
        """The one real semantic shift, asserted rather than discovered later.

        Given a 5m window whose FIRST minute is tradeless at a carried close of
            200.0 and whose only real minute trades between 240.0 and 260.0,
        When the window closes,
        Then it opens at 200.0 and its low is 200.0 — the flat bar's price is a
            real prior print, so the widened extremes are defensible, but the
            window no longer opens at the first TRADED price, and bars either
            side of the cutover are computed differently.
        """
        window = _BASE
        aggregator = self._aggregator(window)
        aggregator.fold(build_flat_minute_bar(_SYMBOL, _begin(window), 200.0))
        aggregator.fold(
            _shaped_bar(
                window + 60,
                _Envelope(
                    open_price=250.0,
                    high=260.0,
                    low=240.0,
                    close=255.0,
                    vwap=250.0,
                    volume=5.0,
                    trades=2,
                ),
            )
        )
        bars = self._close_window(aggregator, window)
        assert len(bars) == 1
        assert bars[0].open == 200.0
        assert bars[0].low == 200.0
        assert bars[0].high == 260.0

    def test_fully_tradeless_window_prices_its_vwap_at_the_carried_close(self) -> None:
        """The dominant case once completion is on, and the one that was missed.

        Given a 5m window of five flat minutes at a carried close of 200.0,
        When the window closes,
        Then it is flat at 200.0 with volume 0.0, trades 0 and vwap 200.0 —
            never 0.0.

        This is not an edge case. With most instruments trading only a few
        minutes an hour, most higher-TF windows for most instruments are
        fully tradeless, so this path carries the majority of the rolled-up
        corpus once completion is enabled. It also reaches the durable plane
        offline, because the synthesized-candle backfill folds persisted 1m
        rows through this same aggregator.

        Before the volume-guard fallback carried the close, this window
        projected ``vwap=0.0`` while its OHLC named a real level. The existing
        rollup test above could not catch it: every window it builds contains
        at least one real bar, so ``volume > 0`` and the guard never fires.
        """
        window = _BASE
        aggregator = self._aggregator(window)
        for offset in range(0, 300, 60):
            aggregator.fold(build_flat_minute_bar(_SYMBOL, _begin(window + offset), 200.0))
        bars = self._close_window(aggregator, window)
        assert len(bars) == 1
        assert bars[0].volume == 0.0
        assert bars[0].trades == 0
        assert bars[0].vwap == 200.0
        assert bars[0].open == 200.0
        assert bars[0].high == 200.0
        assert bars[0].low == 200.0
        assert bars[0].close == 200.0


class TestVenueOptIn:
    """Which venues may complete their 1m plane, and which may never."""

    def test_kraken_spot_opts_in_only_in_trade_built_mode(self) -> None:
        """Native OHLC mode keeps the venue's own 1m plane untouched.

        Given a Kraken spot publisher,
        When the candle source is trade_built versus native,
        Then only trade_built supports minute completion.
        """
        publisher = KrakenMarketDataPublisher(symbols=["*"])
        publisher.settings = cast(AppSettings, _CompletionSettings(candle_source="trade_built"))
        assert publisher._supports_minute_completion() is True
        publisher.settings = cast(AppSettings, _CompletionSettings(candle_source="native"))
        assert publisher._supports_minute_completion() is False

    def test_kraken_futures_opts_in(self) -> None:
        """Perpetuals never close, so a tradeless minute is unambiguous.

        Given a Kraken Futures publisher,
        When the venue hook is read,
        Then it supports minute completion.
        """
        publisher = KrakenFuturesMarketDataPublisher(symbols=["BTC-USD-PERP"])
        assert publisher._supports_minute_completion() is True

    def test_session_based_and_polling_venues_never_opt_in(self) -> None:
        """A flat bar on a closed market is a falsehood, not a convenience.

        Given the Kraken equities publisher (session-based, with holidays) and
            the Walutomat publisher (24/5, already emitting one bar per poll),
        When the venue hook is read,
        Then neither supports minute completion, so the global setting can never
            reach them.
        """
        equities = KrakenEquitiesMarketDataPublisher(symbols=["CLM6-NYMEX"])
        walutomat = WalutomatMarketDataPublisher(symbols=[])
        assert equities._supports_minute_completion() is False
        assert walutomat._supports_minute_completion() is False

    def test_switch_off_builds_no_emitter_and_leaves_the_flush_grace_alone(self) -> None:
        """Default off is byte-identical to the behaviour before this change.

        Given a trade-built Kraken spot publisher with the setting off,
        When the emitter is built and the flush grace read,
        Then no emitter exists and the grace is the unchanged default.
        """
        publisher = KrakenMarketDataPublisher(symbols=["*"])
        publisher.settings = cast(AppSettings, _CompletionSettings(minute_completion=False))
        assert publisher._create_minute_emitter() is None
        assert publisher._candle_flush_grace_seconds() == base_module._CANDLE_FLUSH_GRACE_S

    def test_switch_on_builds_an_emitter_and_widens_the_flush_grace(self) -> None:
        """The higher-TF seal must wait for the last minute of the window.

        Given a trade-built Kraken spot publisher with a 12-second finalize
            grace and the setting on,
        When the emitter is built and the flush grace read,
        Then an emitter exists, the settle is the finalize grace plus the shared
            margin, and the flush grace exceeds that settle — otherwise every
            window would be sealed before its final minute arrived and that
            minute dropped as late.
        """
        publisher = KrakenMarketDataPublisher(symbols=["*"])
        publisher.settings = cast(AppSettings, _CompletionSettings())
        assert publisher._create_minute_emitter() is not None
        assert publisher._minute_sweep_settle_seconds() == _SPOT_SETTLE_S
        assert publisher._candle_flush_grace_seconds() > _SPOT_SETTLE_S


class TestPublisherSweep:
    """The publisher-level path: publish, persist, and forge nothing."""

    @staticmethod
    def _publisher() -> KrakenMarketDataPublisher:
        """Build a Kraken spot publisher wired for a single sweep.

        Returns:
            A running publisher carrying a live minute-completion emitter.
        """
        publisher = KrakenMarketDataPublisher(symbols=["*"])
        publisher.settings = cast(AppSettings, _CompletionSettings())
        publisher.running = True
        publisher._minute_emitter = _make_emitter(settle=_SPOT_SETTLE_S)
        return publisher

    @staticmethod
    def _prime(publisher: KrakenMarketDataPublisher, minute: int, *, with_close: bool) -> None:
        """Give the publisher's emitter evidence and park its cursor.

        Args:
            publisher: Publisher under test.
            minute: UNIX start of the minute to make assertable.
            with_close: Whether to record a prior real bar for the symbol.
        """
        emitter = publisher._minute_emitter
        assert emitter is not None
        _witness_live_around(emitter, minute)
        if with_close:
            emitter.observe_real_bar(_real_bar(minute - _MINUTE, close=105.0))
        emitter.due_flat_bars(float(minute) + _SPOT_SETTLE_S + 2.0)

    @staticmethod
    def _sweep_clock(monkeypatch: pytest.MonkeyPatch, minute: int) -> None:
        """Freeze the publisher's wall clock just past ``minute``'s settle.

        Args:
            monkeypatch: Pytest monkeypatch fixture.
            minute: UNIX start of the minute the sweep should consider settled.
        """
        settled_at = float(minute) + _MINUTE + _SPOT_SETTLE_S + 2.0
        monkeypatch.setattr(base_module, "wall_clock", lambda: settled_at)

    @pytest.mark.asyncio
    async def test_sweep_publishes_and_persists_a_flat_bar(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """The swept minute reaches ZMQ and the writer queue as one bar.

        Given a publisher whose emitter is armed for a settled minute,
        When the sweep runs,
        Then a 1m CandleData is published flat at the carried close with zero
            volume, zero trades and complete=True, and the enqueued row carries
            source='synthesized' — the tag the aggregator's own flat bars have
            used on the 5m..1d planes since forward-fill shipped, so no schema
            change is needed to tell a Snapper-authored bar from a venue one.
        """
        publisher = self._publisher()
        self._prime(publisher, _BASE, with_close=True)
        recorder = _Recorder()
        monkeypatch.setattr(publisher, "_publish_message", recorder)
        monkeypatch.setattr(publisher, "_ensure_instrument", _resolve_instrument)
        self._sweep_clock(monkeypatch, _BASE)
        await publisher._sweep_completed_minute(ExchangeEnum.KRAKEN, "1m")
        assert len(recorder.published) == 1
        topic, message = recorder.published[0]
        assert topic.endswith(".candles.1m")
        assert message.timeframe == "1m"
        assert message.open_at == _begin(_BASE)
        assert message.close == 105.0
        assert message.volume == 0.0
        assert message.trades == 0
        assert message.complete is True
        row = publisher._candle_write_queue.get_nowait()
        assert row["source"] == "synthesized"
        assert row["complete"] is True
        assert row["volume"] == 0.0
        assert row["trades"] == 0
        assert row["open"] == 105.0
        assert row["high"] == 105.0
        assert row["low"] == 105.0
        assert row["close"] == 105.0

    @pytest.mark.asyncio
    async def test_sweep_never_forges_feed_liveness(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """A self-emitted bar must not satisfy the dark-feed watchdog.

        Given a publisher whose emitter is armed for a settled minute,
        When the sweep publishes a flat bar,
        Then neither the venue message watermark, the candle watermark, nor the
            per-symbol lag timestamps advance. A flat bar every 60 seconds
            refreshing those would make the 60-second dark-feed trigger
            unreachable, declare a WebSocket restart recovered on a dead socket,
            and report a healthy lag for a venue delivering nothing.
        """
        publisher = self._publisher()
        self._prime(publisher, _BASE, with_close=True)
        monkeypatch.setattr(publisher, "_publish_message", _Recorder())
        monkeypatch.setattr(publisher, "_ensure_instrument", _resolve_instrument)
        self._sweep_clock(monkeypatch, _BASE)
        publisher._last_message_at = 0.0
        publisher._last_candle_msg_at = 0.0
        publisher._last_data_timestamps.clear()
        await publisher._sweep_completed_minute(ExchangeEnum.KRAKEN, "1m")
        assert publisher._candle_write_queue.qsize() == 1
        assert publisher._last_message_at == 0.0
        assert publisher._last_candle_msg_at == 0.0
        assert publisher._last_data_timestamps == {}

    @pytest.mark.asyncio
    async def test_sweep_folds_the_flat_bar_into_higher_timeframe_synthesis(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Completing the base plane is what makes higher-TF windows whole.

        Given a publisher with a 5m aggregator and an armed emitter,
        When the sweep runs,
        Then the flat minute has been folded into the aggregator, so the
            enclosing 5m window counts it as an observed minute rather than a
            hole.
        """
        publisher = self._publisher()
        aggregator = CandleAggregator(["5m"], live_epoch=_begin(_BASE - _MINUTE))
        publisher._candle_aggregator = aggregator
        self._prime(publisher, _BASE, with_close=True)
        monkeypatch.setattr(publisher, "_publish_message", _Recorder())
        monkeypatch.setattr(publisher, "_ensure_instrument", _resolve_instrument)
        self._sweep_clock(monkeypatch, _BASE)
        await publisher._sweep_completed_minute(ExchangeEnum.KRAKEN, "1m")
        assert _BASE in aggregator._open_minutes[_SYMBOL]

    @pytest.mark.asyncio
    async def test_real_candle_frame_still_produces_todays_bar_unchanged(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A minute WITH trades behaves exactly as it does today.

        Given a publisher running minute completion,
        When a real trade-built 1m frame is consumed,
        Then the published bar carries the real OHLCV and trade count, the row
            is tagged source='calculated' as before, and both the venue and
            candle liveness watermarks advance — nothing about the traded path
            changes.
        """
        publisher = self._publisher()
        recorder = _Recorder()
        monkeypatch.setattr(publisher, "_publish_message", recorder)
        monkeypatch.setattr(publisher, "_ensure_instrument", _resolve_instrument)
        publisher._last_message_at = 0.0
        publisher._last_candle_msg_at = 0.0
        real = _real_bar(_BASE, close=105.0, volume=12.5, trades=4)
        consumed = await publisher._handle_candle_stream_item(
            real, ExchangeEnum.KRAKEN, "kraken", "1m"
        )
        assert consumed is True
        assert len(recorder.published) == 1
        _topic, message = recorder.published[0]
        assert message.volume == 12.5
        assert message.trades == 4
        assert message.close == 105.0
        assert message.complete is True
        row = publisher._candle_write_queue.get_nowait()
        assert row["source"] == "calculated"
        assert row["volume"] == 12.5
        assert row["trades"] == 4
        assert publisher._last_message_at > 0.0
        assert publisher._last_candle_msg_at > 0.0

    @pytest.mark.asyncio
    async def test_real_bar_for_the_minute_leaves_the_sweep_with_nothing_to_do(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A traded minute is written once, by the real path only.

        Given a real frame consumed for the settled minute,
        When the sweep for that same minute runs,
        Then it publishes nothing further and the writer queue holds exactly the
            one real row, so the two paths never contest the same SCD2 natural
            key.
        """
        publisher = self._publisher()
        self._prime(publisher, _BASE, with_close=False)
        recorder = _Recorder()
        monkeypatch.setattr(publisher, "_publish_message", recorder)
        monkeypatch.setattr(publisher, "_ensure_instrument", _resolve_instrument)
        self._sweep_clock(monkeypatch, _BASE)
        await publisher._handle_candle_stream_item(
            _real_bar(_BASE, close=105.0), ExchangeEnum.KRAKEN, "kraken", "1m"
        )
        await publisher._sweep_completed_minute(ExchangeEnum.KRAKEN, "1m")
        assert len(recorder.published) == 1
        assert publisher._candle_write_queue.qsize() == 1

    @pytest.mark.asyncio
    async def test_sweep_is_inert_without_an_emitter(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """With the feature off the sweep is a no-op on every publisher.

        Given a publisher with no emitter,
        When the sweep runs,
        Then nothing is queued for persistence.
        """
        publisher = self._publisher()
        publisher._minute_emitter = None
        self._sweep_clock(monkeypatch, _BASE)
        await publisher._sweep_completed_minute(ExchangeEnum.KRAKEN, "1m")
        assert publisher._candle_write_queue.qsize() == 0

    @pytest.mark.asyncio
    async def test_one_failed_bar_does_not_stop_the_sweep(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A raising publish must never take the candle loop down with it.

        Given three armed symbols where the middle one raises on publish,
        When the sweep runs,
        Then the sweep completes, the other two symbols still publish, and
            the failure is counted.

        ``_candle_loop``'s exception handler is TERMINAL — it logs and lets
        the while loop exit — so without the per-bar guard a single raise in
        a sweep of ~1500 symbols would permanently stop 1m candle consumption
        for the whole venue. Trading one lost bar for a dead feed is never the
        right exchange.
        """
        symbols = ("A-USD", "B-USD", "C-USD")
        publisher = self._publisher()
        publisher._minute_emitter = _make_emitter(
            roster=_Roster(set(symbols)), settle=_SPOT_SETTLE_S
        )
        emitter = publisher._minute_emitter
        _witness_live_around(emitter, _BASE)
        for symbol in symbols:
            emitter.observe_real_bar(build_flat_minute_bar(symbol, _begin(_BASE - _MINUTE), 105.0))
        emitter.due_flat_bars(float(_BASE) + _SPOT_SETTLE_S + 2.0)
        published: list[str] = []

        async def _publish(candle: CandleUpdate, *_args: object) -> None:
            if candle.symbol == "B-USD":
                raise RuntimeError("zmq send failed")
            published.append(candle.symbol)

        monkeypatch.setattr(publisher, "_publish_synthesized_candle", _publish)
        self._sweep_clock(monkeypatch, _BASE)
        await publisher._sweep_completed_minute(ExchangeEnum.KRAKEN, "1m")
        assert sorted(published) == ["A-USD", "C-USD"]
        assert emitter.counters()[PUBLISH_FAILED] == 1

    @pytest.mark.asyncio
    async def test_a_failed_bars_minute_is_gone_and_says_so(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """The loss is permanent, so the counter is the only record of it.

        Given a symbol whose publish raised on the swept minute,
        When the same minute is swept again,
        Then nothing is republished — the floor advanced when the bar was
            BUILT, so the retry is refused as already emitted.

        Pinned rather than fixed: advancing the floor on publish instead of on
        build would need the emitter to hold bars until the publisher confirms
        them. Until that exists, ``publish_failed`` must stay a distinct
        counter and must never be folded into a generic error tally.
        """
        publisher = self._publisher()
        self._prime(publisher, _BASE, with_close=True)

        async def _boom(*_args: object) -> None:
            raise RuntimeError("zmq send failed")

        monkeypatch.setattr(publisher, "_publish_synthesized_candle", _boom)
        self._sweep_clock(monkeypatch, _BASE)
        await publisher._sweep_completed_minute(ExchangeEnum.KRAKEN, "1m")
        emitter = publisher._minute_emitter
        assert emitter is not None
        assert emitter.counters()[PUBLISH_FAILED] == 1
        recorder = _Recorder()
        monkeypatch.setattr(publisher, "_publish_message", recorder)
        monkeypatch.setattr(publisher, "_ensure_instrument", _resolve_instrument)
        await publisher._sweep_completed_minute(ExchangeEnum.KRAKEN, "1m")
        assert recorder.published == []

    @pytest.mark.asyncio
    async def test_sweep_cancellation_still_propagates(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Shutdown must not be swallowed by the per-bar guard.

        Given a publish that raises CancelledError,
        When the sweep runs,
        Then the cancellation propagates rather than being counted as a
            publish failure — a guard that eats CancelledError would make the
            publisher unstoppable.
        """
        publisher = self._publisher()
        self._prime(publisher, _BASE, with_close=True)

        async def _cancelled(*_args: object) -> None:
            raise asyncio.CancelledError

        monkeypatch.setattr(publisher, "_publish_synthesized_candle", _cancelled)
        self._sweep_clock(monkeypatch, _BASE)
        with pytest.raises(asyncio.CancelledError):
            await publisher._sweep_completed_minute(ExchangeEnum.KRAKEN, "1m")
        emitter = publisher._minute_emitter
        assert emitter is not None
        assert PUBLISH_FAILED not in emitter.counters()

    def test_sdk_reconnect_attempt_opens_a_feed_break(self) -> None:
        """The SDK reconnects internally; this hook is where we learn of it.

        Given a publisher with an armed emitter,
        When the patched SDK reports a reconnect attempt,
        Then a feed break is opened, so no minute overlapping the reconnect is
            asserted.
        """
        publisher = self._publisher()
        emitter = publisher._minute_emitter
        assert emitter is not None
        before = emitter._feed_break_until
        publisher._on_sdk_reconnect_attempt()
        assert emitter._feed_break_until > before

    def test_heartbeat_carries_the_sweep_ledger(self) -> None:
        """A hole in the plane must be explainable from the operator surface.

        Given a publisher with an emitter,
        When its venue feed health is read,
        Then the heartbeat meta carries the minute-completion counters.
        """
        publisher = self._publisher()
        health = publisher._venue_feed_health()
        assert "minute_completion" in health.meta
        assert health.degraded is False

    def test_heartbeat_is_unchanged_without_an_emitter(self) -> None:
        """Publishers that do not complete minutes contribute nothing.

        Given a publisher with no emitter,
        When its venue feed health is read,
        Then the meta is empty, so its heartbeat is byte-identical to before
            this hook existed.
        """
        publisher = self._publisher()
        publisher._minute_emitter = None
        health = publisher._venue_feed_health()
        assert health.meta == {}
        assert health.degraded is False
