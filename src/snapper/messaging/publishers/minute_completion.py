"""Complete the base 1m plane: a flat bar for every minute the venue was live.

Kraken's 1m corpus is trade-driven on both spot (``spot_candle_source =
trade_built``) and futures (``TradeCandleBuilder``, the only mode there), so a
minute with no fill produces no row at all. Measured on 2026-08-03: only 11 of
1962 spot instruments had all 60 minutes of the last full hour, 1767 had between
1 and 9, and the whole venue-day wrote 107 of a possible 1440 minutes per
instrument. That is not a publisher defect — the instruments lack candles
because they lack trades — and switching to Kraken's native ``ohlc:1m`` channel
would not add a minute, because that channel is trade-driven too and never
emits a bar with ``trades = 0``.

This module closes the plane at WRITE time, and only at write time. A flat bar
emitted here asserts three things the publisher can actually check while the
socket is in its hand: the venue was delivering across the whole minute, this
symbol's trade subscription was confirmed before the minute opened, and the last
price this process observed for it is ``close``. Reconstructing the same bar
afterwards cannot distinguish "nobody traded" from "we were not listening", which
is why nothing here may ever run over history: the gate is a live-evidence gate,
and a minute that fails it produces NO row rather than a guessed one.

The bar's shape is not new. ``CandleAggregator._flat_fill`` has been shipping
exactly this bar — ``open = high = low = close = vwap = last_close``,
``trades = 0``, ``volume = 0.0``, ``complete = True`` — on the 5m..1d planes for
Kraken spot ever since ``candle_forward_fill`` was enabled. Only the 1m base
plane lacked it. ``trades == 0 AND volume == 0.0`` is the discriminator: every
trade-built bar carries ``trades >= 1`` by construction, and Kraken's native
channel was probed and emits no bar anywhere with ``trades == 0``, so the marker
cannot collide with venue data and needs no schema change.

Classes:
    MinuteCompletionEmitter: Per-venue live-evidence gate and flat-bar factory.

Functions:
    confirmed_trade_symbols: Project a subscription-health snapshot to the
        native symbols whose ``trade`` channel is currently confirmed.
"""

import time
from collections.abc import Callable
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC
from datetime import datetime
from typing import Final

from snapper.core.json_types import JsonObject
from snapper.infrastructure.exchanges._subscription_health import _SymbolEntry
from snapper.infrastructure.exchanges.contracts import CandleUpdate

MINUTE_SWEEP_MARGIN_S: Final[float] = 8.0
"""Slack added to a venue's bar-finalization grace before its minute settles.

The sweep for minute ``M`` must not run until a REAL bar for ``M`` can no longer
arrive, otherwise a flat bar would race a genuine one into an SCD2 version
churn. Kraken spot finalizes a trade-built minute at ``M_end +
trade_built_finalize_grace_seconds`` (12 s by default) on a 1 Hz loop; Kraken
futures finalizes at ``M_end`` with no grace at all. This margin covers the 1 Hz
tick, the client queue hop and the publisher's own scheduling on top of whichever
grace the venue uses."""

MINUTE_RESUBSCRIBE_SETTLE_S: Final[float] = 90.0
"""Seconds a detected feed break keeps every overlapping minute ineligible.

Long enough to cover a paced post-reconnect subscription replay of the whole
~1900-symbol spot universe. It is deliberately a blunt venue-wide window rather
than a per-symbol one: the subscription tracker actively RESETS its staleness
clock when an entry re-confirms, so a post-reconnect window reads fresh before
any data has returned, and "looks fresh" is not evidence."""

_MINUTE_SECONDS: Final[int] = 60

_WITNESS_GAP_TOLERANCE_S: Final[float] = 5.0
"""Largest silence, in seconds, that still counts as a continuously live feed.

Applied three ways for one minute ``M``: at most this much silence before the
first frame inside ``M``, after the last frame inside ``M``, and between any two
consecutive frames inside ``M``. On the wildcard ticker (~1900 spot symbols) and
on the futures ticker feed a frame arrives every few milliseconds, so a 5 s hole
is a genuine stall, never a quiet market. A false negative costs coverage — the
minute is simply skipped — and never produces a bar."""

_WITNESS_RETENTION_MINUTES: Final[int] = 8
"""Minutes of witness history retained.

The sweep only ever considers the single minute that just settled, so three
consecutive spans is all the gate reads; the rest is headroom for a delayed
sweep tick. A stall longer than this window silently stops asserting, which is
the correct outcome: no evidence, no assertion."""

_TRADE_CHANNEL: Final[str] = "trade"

SKIP_NOT_OBSERVED: Final[str] = "skipped_not_observed"
SKIP_FEED_BREAK: Final[str] = "skipped_feed_break"
SKIP_UNCONFIRMED: Final[str] = "skipped_subscription_unconfirmed"
SKIP_ALREADY_EMITTED: Final[str] = "skipped_already_emitted"
EMITTED: Final[str] = "emitted"
PUBLISH_FAILED: Final[str] = "publish_failed"
"""Bars built and counted as emitted whose publish then raised.

Named separately because the per-symbol floor advances when a bar is BUILT,
not when it is published, so a failed publish loses that minute permanently —
the next sweep refuses it as already emitted. The counter is the only record
that the minute existed, so it must never be folded into a generic error tally.
"""


@dataclass(slots=True)
class _WitnessSpan:
    """Wall-clock extent of inbound venue traffic inside one minute.

    Attributes:
        first: Receipt time of the first frame observed in the minute.
        last: Receipt time of the most recent frame observed in the minute.
        continuous: False once two consecutive frames inside the minute were
            more than :data:`_WITNESS_GAP_TOLERANCE_S` apart.
    """

    first: float
    last: float
    continuous: bool


@dataclass(slots=True)
class _LastClose:
    """The most recent minute this process has a bar for, and its close.

    Attributes:
        minute_ts: UNIX start of the newest minute already accounted for, real
            or flat. It is the anti-double-emit floor.
        close: Close carried from the last REAL bar; a flat bar copies it
            forward unchanged so a run of empty minutes is flat, not drifting.
    """

    minute_ts: int
    close: float


def confirmed_trade_symbols(
    snapshot: Mapping[tuple[str, str], _SymbolEntry],
    to_native: Callable[[str], str],
) -> set[str]:
    """Return native symbols whose ``trade`` subscription is confirmed right now.

    Reads the ``trade`` channel and never ``ticker``: a reconnect re-seeds the
    wildcard ticker universe as CONFIRMED while every per-symbol trade entry
    drops back to ``pending``, so reading the ticker channel would import a
    false confirmation for the whole universe on every reconnect.

    Args:
        snapshot: Point-in-time ``(channel, wire_symbol) -> entry`` mapping from
            the exchange client's subscription-health tracker.
        to_native: Wire-format to native-symbol converter for this venue.

    Returns:
        Native symbols with a confirmed, non-quarantined trade subscription.
    """
    native: set[str] = set()
    for (channel, wire_symbol), entry in snapshot.items():
        if channel != _TRADE_CHANNEL:
            continue
        if entry.status != "confirmed" or entry.quarantined or not entry.ever_confirmed:
            continue
        try:
            native.add(to_native(wire_symbol))
        except ValueError:
            continue
    return native


def build_flat_minute_bar(symbol: str, interval_begin: datetime, close: float) -> CandleUpdate:
    """Build the flat zero-volume bar for one tradeless minute.

    Every field matches ``CandleAggregator._flat_fill`` so the 1m plane states
    emptiness exactly the way the 5m..1d planes already do. ``vwap`` carries
    ``close`` rather than ``0.0``: the higher-TF rollup is indifferent (it
    accumulates ``vwap * volume`` and divides by total volume, so a zero-volume
    bar contributes nothing to either side), but a persisted ``vwap = 0.0``
    would be a false statement about price.

    Args:
        symbol: Native symbol the bar belongs to.
        interval_begin: Canonical UTC minute start.
        close: Last close observed for the symbol on this connection.

    Returns:
        A ``complete`` zero-volume :class:`CandleUpdate` flat at ``close``.
    """
    return CandleUpdate(
        symbol=symbol,
        open=close,
        high=close,
        low=close,
        close=close,
        vwap=close,
        trades=0,
        volume=0.0,
        interval_begin=interval_begin,
        interval=_MINUTE_SECONDS,
        complete=True,
    )


class MinuteCompletionEmitter:
    """Decide which settled minutes may be asserted, and build their flat bars.

    Owns three pieces of live evidence and nothing else: a bounded ring of
    per-minute witness spans, a per-symbol last-close map fed ONLY by real bars
    observed on this connection, and a feed-break deadline pushed by every
    detected reconnect or recovery. It never reads the database, never reads the
    ticker's price, and never retro-fills: :meth:`due_flat_bars` considers the
    single minute that has just settled, so a minute passed over for any reason
    has no second chance by construction.
    """

    def __init__(
        self,
        *,
        roster: Callable[[], set[str]],
        enabled: Callable[[], bool],
        settle_seconds: float,
        resubscribe_settle_seconds: float = MINUTE_RESUBSCRIBE_SETTLE_S,
    ) -> None:
        """Initialize the emitter with no evidence and no feed break.

        A boot instant is deliberately NOT stamped as a feed break. The three
        gates already refuse everything a startup window could produce: the
        witness gate needs three consecutive fully-witnessed minutes, which a
        freshly-connected socket cannot supply for any minute that predates it;
        the roster gate refuses every symbol whose trade subscription has not
        ACKed yet; and the last-close gate refuses every symbol that has not
        printed in this process. Deriving one more window from the wall clock
        would add a hidden real-time dependency to an otherwise pure object
        without refusing a single bar the gates let through.

        Args:
            roster: Callable returning the native symbols whose trade
                subscription is confirmed right now. Re-read once per emitting
                sweep, never cached, so a symbol that drops out stops receiving
                bars within one minute.
            enabled: Live kill switch, consulted on every sweep so the emitter
                can be turned off without restarting the publisher.
            settle_seconds: Slack after a minute's end before it is swept, so a
                real bar for that minute can no longer arrive.
            resubscribe_settle_seconds: How long a detected feed break keeps
                overlapping minutes ineligible.
        """
        self._roster = roster
        self._enabled = enabled
        self._settle_s = settle_seconds
        self._resubscribe_settle_s = resubscribe_settle_seconds
        self._observed: dict[int, _WitnessSpan] = {}
        self._last_close: dict[str, _LastClose] = {}
        self._feed_break_until: float = 0.0
        self._swept_through: int | None = None
        self._last_swept_minute: int | None = None
        self._counters: dict[str, int] = {}

    def observe_feed_frame(self, now: float | None = None) -> None:
        """Record that a public venue frame arrived, stamped at RECEIPT.

        Called from the tick, trade and real-candle paths. Receipt time is the
        only honest stamp here: an event timestamp says when the venue believes
        something happened, not when this process was listening.

        Args:
            now: Receipt wall-clock time; defaults to the current time.
        """
        received = time.time() if now is None else now
        minute = int(received // _MINUTE_SECONDS) * _MINUTE_SECONDS
        span = self._observed.get(minute)
        if span is None:
            self._observed[minute] = _WitnessSpan(first=received, last=received, continuous=True)
            self._prune_observed(minute)
            return
        if received - span.last > _WITNESS_GAP_TOLERANCE_S:
            span.continuous = False
        span.last = received

    def observe_real_bar(self, candle: CandleUpdate) -> None:
        """Record a REAL 1m bar as this symbol's newest accounted-for minute.

        This is the only writer of the last-close map. Seeding it from the
        durable plane would let a fresh process assert a price it never
        observed, and seeding it from the ticker's ``last`` field would couple
        candle VALUES to a subscription the architecture keeps droppable.

        Args:
            candle: A real 1m candle observed on this connection.
        """
        minute = int(candle.interval_begin.timestamp())
        known = self._last_close.get(candle.symbol)
        if known is not None and known.minute_ts > minute:
            return
        self._last_close[candle.symbol] = _LastClose(minute_ts=minute, close=candle.close)

    def mark_feed_break(self, now: float | None = None) -> None:
        """Bar every minute overlapping a detected reconnect or recovery.

        Args:
            now: Wall-clock time of the break; defaults to the current time.
        """
        received = time.time() if now is None else now
        self._feed_break_until = max(self._feed_break_until, received + self._resubscribe_settle_s)

    def due_flat_bars(self, now: float) -> list[CandleUpdate]:
        """Return the flat bars owed for the minute that has just settled.

        Args:
            now: Current wall-clock time in UNIX seconds.

        Returns:
            One flat bar per eligible symbol, or an empty list when the cursor
            has not advanced, the emitter is switched off, or the minute failed
            the live-evidence gate.
        """
        minute = self._advance_cursor(now)
        if minute is None:
            return []
        self._last_swept_minute = minute
        if not self._minute_is_assertable(minute):
            return []
        return self._flat_bars_for(minute)

    def counters(self) -> JsonObject:
        """Return cumulative sweep outcomes for the heartbeat surface.

        A gap in the corpus has to be explainable from the operator surface,
        otherwise "a missing minute means we were not listening" is a claim
        nobody can check.

        Returns:
            Cumulative per-reason counts plus the eligible-symbol population and
            the last minute the sweep considered.
        """
        snapshot: JsonObject = {}
        for reason, count in self._counters.items():
            snapshot[reason] = count
        snapshot["known_closes"] = len(self._last_close)
        snapshot["last_swept_minute"] = (
            None
            if self._last_swept_minute is None
            else datetime.fromtimestamp(self._last_swept_minute, UTC).isoformat()
        )
        return snapshot

    def _advance_cursor(self, now: float) -> int | None:
        """Move the sweep cursor to the newest settled minute, monotonically.

        Three properties, each of which a boundary cursor has to have:

        - it is SEEDED on the first call and never re-derived from the clock
          afterwards, so a fresh emitter cannot assert a minute it was not
          running for;
        - it never rewinds, so a backward wall-clock step stalls the sweep
          instead of restating a minute that was already correct; and
        - it advances even while the emitter is switched off, so turning the
          setting back on starts at the current minute rather than draining a
          backlog of minutes nobody swept.

        Args:
            now: Current wall-clock time in UNIX seconds.

        Returns:
            The settled minute to sweep, or ``None`` when the cursor was just
            seeded, did not advance, or the emitter is switched off.
        """
        settled = (int((now - self._settle_s) // _MINUTE_SECONDS) - 1) * _MINUTE_SECONDS
        previous = self._swept_through
        if previous is None:
            self._swept_through = settled
            return None
        if settled <= previous:
            return None
        self._swept_through = settled
        if not self._enabled():
            return None
        return settled

    def _minute_is_assertable(self, minute: int) -> bool:
        """Return whether the venue was demonstrably live across the whole minute.

        Requires an unbroken witness from ``minute - tolerance`` through
        ``minute + 60 + tolerance``: the previous minute proves the connection
        predated this one, the current minute proves it spanned it without an
        internal stall, and the following minute proves it survived the
        boundary. A monotonic scalar like ``_last_message_at`` cannot answer
        this — it reports how long since the last frame NOW, never whether the
        socket was delivering during a minute that has already passed.

        Args:
            minute: UNIX start of the settled minute.

        Returns:
            True when the minute may be asserted; False (counted) otherwise.
        """
        if minute < self._feed_break_until:
            self._bump(SKIP_FEED_BREAK)
            return False
        previous = self._observed.get(minute - _MINUTE_SECONDS)
        current = self._observed.get(minute)
        following = self._observed.get(minute + _MINUTE_SECONDS)
        if previous is None or current is None or following is None:
            self._bump(SKIP_NOT_OBSERVED)
            return False
        if not self._spans_cover_minute(previous, current, following, minute):
            self._bump(SKIP_NOT_OBSERVED)
            return False
        return True

    @staticmethod
    def _spans_cover_minute(
        previous: _WitnessSpan, current: _WitnessSpan, following: _WitnessSpan, minute: int
    ) -> bool:
        """Return whether three witness spans cover a minute edge to edge.

        Args:
            previous: Witness span for the preceding minute.
            current: Witness span for the minute under test.
            following: Witness span for the succeeding minute.
            minute: UNIX start of the minute under test.

        Returns:
            True when no silence longer than the tolerance falls inside the
            minute or across either of its boundaries.
        """
        end = minute + _MINUTE_SECONDS
        tolerance = _WITNESS_GAP_TOLERANCE_S
        return (
            current.continuous
            and previous.last >= minute - tolerance
            and current.first <= minute + tolerance
            and current.last >= end - tolerance
            and following.first <= end + tolerance
        )

    def _flat_bars_for(self, minute: int) -> list[CandleUpdate]:
        """Build the flat bars for every symbol still owed one for ``minute``.

        Iterates the last-close map rather than the roster, because a symbol
        with no in-session close has no observed price to carry and must not
        receive a bar at all. Emitting also advances that symbol's floor, which
        is what makes a re-sweep idempotent and stops a flat bar landing on a
        minute a real bar already covered.

        Args:
            minute: UNIX start of the settled minute.

        Returns:
            Flat bars for the eligible symbols, possibly empty.
        """
        roster = self._roster()
        interval_begin = datetime.fromtimestamp(minute, UTC)
        bars: list[CandleUpdate] = []
        for symbol, known in tuple(self._last_close.items()):
            if symbol not in roster:
                self._bump(SKIP_UNCONFIRMED)
                continue
            if known.minute_ts >= minute:
                self._bump(SKIP_ALREADY_EMITTED)
                continue
            bars.append(build_flat_minute_bar(symbol, interval_begin, known.close))
            self._last_close[symbol] = _LastClose(minute_ts=minute, close=known.close)
        self._bump(EMITTED, len(bars))
        return bars

    def _prune_observed(self, newest: int) -> None:
        """Drop witness spans older than the retention window.

        Args:
            newest: UNIX start of the minute just witnessed.
        """
        if len(self._observed) <= _WITNESS_RETENTION_MINUTES:
            return
        horizon = newest - _WITNESS_RETENTION_MINUTES * _MINUTE_SECONDS
        for minute in tuple(self._observed):
            if minute < horizon:
                del self._observed[minute]

    def mark_publish_failed(self) -> None:
        """Record that a built bar could not be published.

        The publisher calls this when a flat bar it received from
        :meth:`due_flat_bars` raises on the way out. The bar is already
        counted in ``EMITTED`` and the symbol's floor has already advanced, so
        the minute is gone — the next sweep refuses it as already emitted.
        This counter is therefore the ONLY record that it existed, which is
        why it is a distinct name rather than a generic error tally.
        """
        self._bump(PUBLISH_FAILED)

    def _bump(self, key: str, amount: int = 1) -> None:
        """Add to a cumulative sweep counter.

        Args:
            key: Counter name.
            amount: Increment; defaults to one.
        """
        self._counters[key] = self._counters.get(key, 0) + amount
