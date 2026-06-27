"""Synthesize higher-timeframe OHLCV candles from a live 1m candle stream.

Phase 1 of the candle synthesis layer (see
``proprietary/plans/plan_2026_06_14_candle_synthesis_layer.md``): instead of
subscribing to each timeframe from the venue, the publisher subscribes to 1m
only and this aggregator rolls finalized 1m candles up into every configured
higher timeframe, emitting a closed higher-TF bar at its canonical UTC
boundary. It is the 1m-to-higher analogue of
:class:`snapper.infrastructure.exchanges._trade_candle_builder.TradeCandleBuilder`
(which is the trades-to-1m analogue) and reuses the same proven shape: a
per-symbol finalization watermark, exactly-once folding, and a late-event
counter.

Boundaries are UTC for every timeframe: fixed intervals floor to
``(unix // interval_s) * interval_s`` and 1d aligns to 00:00 UTC for the UNIX
epoch. This matches both Kraken native OHLC (1d closes 00:00 UTC) and the
Polygon grouped-daily historical corpus (UTC calendar day), so live-synthesized
bars align with the data a strategy was validated and warmed up on.

This aggregator only synthesizes and emits closed bars; it never touches the
DB. Phase 1 shipped emit-only (the caller published to ZMQ and persisted
nothing). As of Phase 3
(``proprietary/plans/plan_2026_06_16_candle_phase3_persistence.md``) the caller
(:meth:`MarketDataPublisherService._publish_synthesized_candle`) additionally
persists each emitted bar as ``source='synthesized'`` carrying its ``complete``
flag, under the same persist policy as the native 1m path; the
``candle_query`` single-source read cutover remains a later slice.
"""

from dataclasses import dataclass
from datetime import UTC
from datetime import datetime
from datetime import timedelta

from loguru import logger

from snapper.infrastructure.exchanges.contracts import CandleUpdate

_TF_SECONDS: dict[str, int] = {
    "5m": 300,
    "15m": 900,
    "30m": 1800,
    "1h": 3600,
    "4h": 14400,
    "1d": 86400,
}

SUPPORTED_SYNTHESIS_TIMEFRAMES: frozenset[str] = frozenset(_TF_SECONDS)

_WARNED_LATE_CAP: int = 4096

_FORWARD_FILL_MAX_WINDOWS: int = 2000


@dataclass
class _HtfBucket:
    """Running OHLCV state for one ``(symbol, timeframe, window)`` bucket.

    ``interval_begin`` is the canonical UTC window start; ``open_ts`` /
    ``close_ts`` track the earliest / latest folded 1m by event time so that
    out-of-order same-window 1m candles still yield the correct open and close.
    """

    symbol: str
    timeframe: str
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
    complete: bool
    seeded: bool
    expected_minute_count: int
    counted_minutes: set[datetime]


@dataclass(frozen=True)
class LateCandleDrop:
    """Pure signal that a finalized 1m frame was dropped from live folding.

    Attributes:
        symbol: Native symbol of the dropped 1m frame.
        minute: UTC minute start of the dropped 1m frame.
    """

    symbol: str
    minute: datetime


@dataclass(frozen=True)
class SeededIncompleteWindow:
    """Pure signal that a suppressed seeded window needs settled-plane repair.

    Attributes:
        symbol: Native symbol of the suppressed higher-timeframe window.
        timeframe: Higher timeframe label.
        window_begin: Canonical UTC window start.
        expected_minute_count: Distinct complete 1m minutes folded into the
            suppressed bucket.
        counted_minutes: Distinct complete 1m minute starts folded into the
            suppressed bucket.
    """

    symbol: str
    timeframe: str
    window_begin: datetime
    expected_minute_count: int
    counted_minutes: frozenset[datetime]


class CandleAggregator:
    """Roll finalized 1m candles up into configured higher timeframes.

    The aggregator is single-thread by design — the publisher drives it
    synchronously from the 1m candle loop, mirroring
    :class:`TradeCandleBuilder`. No locks.

    A 1m minute is folded into the higher-timeframe buckets exactly ONCE, with
    its FINAL value. The venue emits many in-progress frames for an open minute
    (snapshots / updates); per symbol the aggregator keeps the latest frame of
    every not-yet-finalized minute and finalizes a minute only once a STRICTLY
    LATER minute is observed (the minute can no longer change). This also
    correctly folds an out-of-order minute that arrives after a later one, as
    long as it has not already been folded. A frame whose minute is at or below
    the highest already-finalized minute for its symbol is a late full-snapshot;
    because :class:`CandleUpdate` carries a full OHLCV value (not a delta),
    re-folding it would double-count, so it is dropped and counted.

    Everything is keyed per symbol: the single 1m stream carries many symbols,
    so the finalization watermark and emission are per symbol — one symbol
    crossing midnight must never emit another symbol's daily bar.

    Only COMPLETE windows are emitted. A higher-TF bucket is published only if
    its opening minute was folded AND the window is trustworthy — either it was
    seeded from the durable plane on restart, or it OPENED at or after the live
    epoch (the aggregator's start), so every one of its minutes was observed
    live. A window that opened before the live epoch and was not seeded (a
    mid-stream join: unseeded/wildcard restart, a symbol appearing partway
    through) can never become complete — a late opening-minute frame cannot fake
    it — so it is suppressed rather than published as a truncated bar; it
    self-heals on the next window, which opens after the epoch.

    Reorder contract: the aggregator tolerates SHALLOW reordering of minutes
    above the per-symbol folded watermark (an out-of-order minute still folds as
    long as the watermark has not already advanced past it). A minute that
    arrives AFTER the watermark moved beyond it cannot be reconstructed (its
    bucket may already be closed); it is dropped and counted in
    :attr:`late_rolls_after_close`. The Kraken spot 1m feed delivers candles in
    event-time order, so this bounds to pathological reordering; a non-zero
    counter is the signal to widen handling.
    """

    def __init__(
        self,
        timeframes: list[str],
        *,
        live_epoch: datetime | None = None,
        grace_seconds: float = 0.0,
        forward_fill: bool = False,
        flush_grace_seconds: float = 0.0,
    ) -> None:
        """Create an aggregator for the given higher timeframes.

        Args:
            timeframes: Timeframe labels to synthesize. Entries not in
                :data:`_TF_SECONDS` (including ``"1m"``) are ignored — the 1m
                stream is the input, not an output of this layer.
            live_epoch: The time live folding began (the publisher start). A
                window is treated as fully observed live only if it OPENED at or
                after this epoch; an earlier window (the one in progress at
                start) is completable only via :meth:`seed_1m`. Defaults to
                ``None`` (epoch 0 — every window is live-completable), which the
                publisher overrides with its start time.
            grace_seconds: Event-time slack subtracted from the per-symbol
                watermark before a bucket is considered closed. Defaults to
                ``0.0`` (a single symbol's finalized 1m stream is monotonic);
                kept as a knob to absorb pathological skew.
            forward_fill: Whether the time-driven :meth:`flush` synthesizes a
                flat carried-close bar for empty windows (Phase 1b). Defaults to
                ``False`` (flush is fully inert and Phase-1 behaviour is
                unchanged). Enable ONLY for instruments whose warmup corpus is
                contiguous at the configured timeframe (24/7 crypto), never for
                session-based equities — forward-filling a non-contiguous corpus
                manufactures bars the strategy was never validated on.
            flush_grace_seconds: Wall-clock slack the :meth:`flush` subtracts from
                ``now`` before sealing a minute or window, so a just-ended minute is
                not finalized before the venue can deliver its FINAL frame for it
                (which would otherwise be dropped as late and seal a stale bar).
                Defaults to ``0.0`` (unit tests drive ``flush`` with explicit
                boundaries); the publisher wires a small nonzero value in
                production. Only active when forward-fill is enabled.
        """
        self._tf_seconds: dict[str, int] = {
            tf: _TF_SECONDS[tf] for tf in timeframes if tf in _TF_SECONDS
        }
        self._live_epoch_ts = (
            int(self._floor(live_epoch, 60).timestamp()) if live_epoch is not None else 0
        )
        self._grace_s = grace_seconds
        self._forward_fill = forward_fill
        self._flush_grace_s = flush_grace_seconds
        self._buckets: dict[tuple[str, str, int], _HtfBucket] = {}
        self._open_minutes: dict[str, dict[int, CandleUpdate]] = {}
        self._folded_minute: dict[str, int] = {}
        self._watermark: dict[str, datetime] = {}
        self._closed_window: dict[tuple[str, str], int] = {}
        self._last_close: dict[tuple[str, str], float] = {}
        self._last_real_window: dict[tuple[str, str], int] = {}
        self._late_rolls_after_close: int = 0
        self._warned_late: set[tuple[str, int]] = set()
        self._late_drops: list[LateCandleDrop] = []
        self._seeded_incomplete_windows: list[SeededIncompleteWindow] = []
        self._warned_fill_overflow: set[tuple[str, str]] = set()

    @staticmethod
    def _floor(ts: datetime, tf_seconds: int) -> datetime:
        """Floor a timestamp to its canonical UTC window start.

        Args:
            ts: Timestamp to floor (converted to UTC).
            tf_seconds: Window width in seconds; ``86400`` aligns 1d to
                00:00 UTC because the UNIX epoch is itself 00:00 UTC.

        Returns:
            The UTC window-start datetime.
        """
        unix = int(ts.astimezone(UTC).timestamp())
        return datetime.fromtimestamp((unix // tf_seconds) * tf_seconds, UTC)

    @staticmethod
    def _ceil(ts: int, tf_seconds: int) -> int:
        """Return the first canonical UTC window-start at or after a timestamp.

        Used to clamp the forward-fill walk to the first FULLY-LIVE window so a
        live epoch landing mid-window never yields a non-boundary
        ``interval_begin``.

        Args:
            ts: A UNIX timestamp in seconds.
            tf_seconds: Window width in seconds.

        Returns:
            The smallest multiple of ``tf_seconds`` that is at or above ``ts``.
        """
        if ts % tf_seconds == 0:
            return ts
        return ((ts // tf_seconds) + 1) * tf_seconds

    @property
    def forward_fill(self) -> bool:
        """Return whether the time-driven forward-fill flush is enabled.

        Returns:
            ``True`` when :meth:`flush` synthesizes flat carried-close bars for
            empty windows; ``False`` when it is inert (the default).
        """
        return self._forward_fill

    def fold(self, candle_1m: CandleUpdate) -> list[tuple[str, CandleUpdate]]:
        """Ingest a 1m frame and return any higher-TF bars that just closed.

        Records the frame as the latest for its minute, finalizes every open
        minute strictly older than the highest minute seen (those can no longer
        change), and drops a frame whose minute was already finalized.

        Args:
            candle_1m: A 1m :class:`CandleUpdate` frame (possibly an in-progress
                update for a still-open minute).

        Returns:
            Closed higher-TF bars as ``(timeframe_label, candle)`` pairs in
            ascending-timeframe order; empty when nothing closed.
        """
        sym = candle_1m.symbol
        minute_ts = int(candle_1m.interval_begin.timestamp())
        folded = self._folded_minute.get(sym)
        if folded is not None and minute_ts <= folded:
            self._record_late(sym, minute_ts)
            return []
        open_minutes = self._open_minutes.setdefault(sym, {})
        open_minutes[minute_ts] = candle_1m
        max_seen = max(open_minutes)
        to_finalize = sorted(minute for minute in open_minutes if minute < max_seen)
        if not to_finalize:
            return []
        for minute in to_finalize:
            self._finalize_minute(open_minutes.pop(minute))
        return self._emit_closed(sym)

    def seed_1m(self, tf: str, candle_1m: CandleUpdate) -> None:
        """Fold an already-final historical 1m into one timeframe for restart.

        Used to rebuild a timeframe's open (or just-closed) bucket from
        persisted 1m after a publisher restart. Folds DIRECTLY into the ``tf``
        bucket and bypasses the open-minute machinery and the drop-late guard,
        so seeding a later timeframe for minutes already covered by an earlier
        timeframe's seed (same symbol) is not suppressed by the shared per-symbol
        ``_folded_minute``. Never touches ``_open_minutes`` — the live stream
        owns the current minute. The caller must feed only finalized 1m strictly
        before the current minute. ``_folded_minute``/``_watermark[sym]`` track
        the max seeded minute.

        Args:
            tf: Timeframe label to seed (must be a configured higher TF).
            candle_1m: A historical, already-final 1m candle.
        """
        if tf not in self._tf_seconds:
            return
        self._fold_into_bucket(tf, candle_1m, seeded=True)
        sym = candle_1m.symbol
        minute_ts = int(candle_1m.interval_begin.timestamp())
        prev_minute = self._folded_minute.get(sym)
        if prev_minute is None or minute_ts > prev_minute:
            self._folded_minute[sym] = minute_ts
            self._watermark[sym] = candle_1m.interval_begin

    def flush(self, now: datetime) -> list[tuple[str, CandleUpdate]]:
        """Emit wall-clock-closed bars and forward-fill empty windows.

        The time-driven counterpart to :meth:`fold`, for instruments too thin for
        a later 1m to advance the watermark across a boundary. A no-op unless
        forward-fill is enabled (the default), so the Phase-1 data path is left
        byte-for-byte unchanged. Two steps:

        1. Finalize every held minute that has GENUINELY ended
           (``minute < floor(now - flush_grace, 60s)``) and run the SAME data-path
           :meth:`_emit_closed`, so a trailing real window — or a seeded/pre-epoch
           window whose triggering minute is finalized here rather than by a live
           fold — closes through the normal machinery and is never stranded. The
           current in-progress minute is never finalized; the ``flush_grace`` slack
           also keeps a JUST-ended minute held until the venue has had time to send
           its final frame (otherwise that frame is dropped as late and the bar is
           sealed stale).
        2. For each active ``(symbol, timeframe)`` walk the LIVE region
           chronologically from the frontier (up to the grace-adjusted ``now``),
           sealing any wall-clock-ended real bucket and forward-filling each empty
           window with a flat carried-close bar. The frontier advanced in step 1
           makes the walk start strictly after any window step 1 emitted, so no
           window is emitted twice.

        Args:
            now: Current wall-clock time (UTC).

        Returns:
            Closed and forward-filled bars as ``(timeframe_label, candle)`` pairs.
        """
        if not self._forward_fill:
            return []
        out: list[tuple[str, CandleUpdate]] = []
        effective = now - timedelta(seconds=self._flush_grace_s)
        minute_floor_ts = int(self._floor(effective, 60).timestamp())
        for sym in self._open_minutes:
            open_minutes = self._open_minutes[sym]
            to_finalize = sorted(minute for minute in open_minutes if minute < minute_floor_ts)
            for minute in to_finalize:
                self._finalize_minute(open_minutes.pop(minute))
            if to_finalize:
                out += self._emit_closed(sym)
        for sym in self._symbols_with_state():
            for tf, tf_seconds in self._tf_seconds.items():
                out += self._flush_timeframe(sym, tf, tf_seconds, effective)
        return out

    def window_start(self, timeframe: str, now: datetime) -> datetime | None:
        """Return the canonical UTC window start for a timeframe at ``now``.

        Used by the publisher to bound the restart-seed read range to the
        current open window of a timeframe.

        Args:
            timeframe: Timeframe label.
            now: Reference time.

        Returns:
            The UTC window-start datetime, or ``None`` if this aggregator does
            not synthesize ``timeframe``.
        """
        tf_seconds = self._tf_seconds.get(timeframe)
        if tf_seconds is None:
            return None
        return self._floor(now, tf_seconds)

    def set_live_epoch(self, epoch: datetime) -> None:
        """Set the live epoch to the moment live consumption actually begins.

        The publisher calls this AFTER the restart seed completes and right
        before it starts consuming the 1m stream, so the epoch reflects when the
        aggregator truly went live — not when it was constructed. Any window that
        opened before this instant is then completable only via a seed, so a
        startup/seed delay cannot let a mid-window-join window publish a
        truncated bar.

        The epoch is FLOORED to the minute: a restart landing partway through a
        minute (e.g. 10:00:30) still treats a higher-TF window OPENING at that
        minute (10:00) as fully observed, because the venue's snapshot-on-subscribe
        delivers the whole current minute — without flooring, that window would be
        dropped as pre-epoch (one valid bar lost). A restart landing in a LATER
        minute of a window still correctly suppresses it (earlier minutes missed).

        Args:
            epoch: The live-consumption start time.
        """
        self._live_epoch_ts = int(self._floor(epoch, 60).timestamp())

    def timeframe_seconds(self, timeframe: str) -> int:
        """Return the width in seconds of a configured higher timeframe.

        Args:
            timeframe: A timeframe label this aggregator synthesizes.

        Returns:
            The timeframe width in seconds.
        """
        return self._tf_seconds[timeframe]

    @property
    def timeframes(self) -> tuple[str, ...]:
        """Return the higher timeframes this aggregator synthesizes.

        Returns:
            Configured supported timeframe labels in fold order.
        """
        return tuple(self._tf_seconds)

    def has_closed_window(self, symbol: str, timeframe: str, window_begin: datetime) -> bool:
        """Return whether a synthesized window has already been sealed.

        Args:
            symbol: Native symbol key.
            timeframe: Higher timeframe label.
            window_begin: Canonical UTC window start.

        Returns:
            True when the per-symbol/timeframe frontier is at or beyond
            ``window_begin``.
        """
        frontier = self._closed_window.get((symbol, timeframe))
        if frontier is None:
            return False
        return int(window_begin.timestamp()) <= frontier

    def pop_late_drops(self) -> list[LateCandleDrop]:
        """Drain and return late-drop signals accumulated by recent folds.

        Returns:
            Late 1m drop signals in observation order.
        """
        drops = self._late_drops
        self._late_drops = []
        return drops

    def pop_seeded_incomplete_windows(self) -> list[SeededIncompleteWindow]:
        """Drain and return seeded incomplete close signals.

        Returns:
            Suppressed seeded windows in close order.
        """
        windows = self._seeded_incomplete_windows
        self._seeded_incomplete_windows = []
        return windows

    @property
    def late_rolls_after_close(self) -> int:
        """Return how many late 1m frames were dropped to avoid double-counting.

        A late frame is one whose minute is at or below the highest already-
        finalized minute for its symbol. Zero in steady state; growth means the
        feed reordered beyond the finalization model and the corrective value is
        not reflected in the synthesized higher-TF bars (subtract-replace is a
        later-phase hardening).

        Returns:
            Monotonic count of dropped late frames since construction.
        """
        return self._late_rolls_after_close

    def _finalize_minute(self, final_1m: CandleUpdate) -> None:
        """Fold a now-final 1m minute into every higher timeframe.

        Advances the per-symbol folded watermark and folds the final frame into
        each configured timeframe's bucket. Emission is deferred to
        :meth:`_emit_closed` so a batch of finalizations emits once.

        Args:
            final_1m: The 1m candle whose minute can no longer change.
        """
        sym = final_1m.symbol
        self._folded_minute[sym] = int(final_1m.interval_begin.timestamp())
        self._watermark[sym] = final_1m.interval_begin
        for tf in self._tf_seconds:
            self._fold_into_bucket(tf, final_1m, seeded=False)

    def _fold_into_bucket(self, tf: str, candle: CandleUpdate, *, seeded: bool) -> None:
        """Create or extend the ``(symbol, tf, window)`` bucket for a 1m candle.

        A newly-created bucket is ``complete`` only if its first folded minute is
        the window's opening minute AND the window is trustworthy — either it was
        ``seeded`` from the durable plane, or it OPENED at or after the live epoch
        (so the aggregator observed every minute of it). A window that opened
        before the live epoch and was not seeded can never become complete (a
        late opening-minute frame cannot fake it), so a mid-window-join window is
        never published as a truncated bar.

        Live folds also honour the per-timeframe frontier: a 1m mapping into a
        window at or below ``_closed_window[(symbol, tf)]`` (already emitted or
        forward-filled) is dropped and counted, because re-creating that bucket
        would resurrect a window the :meth:`flush` path has already sealed. The
        ``seeded`` path bypasses this (seeds legitimately pre-date the frontier).

        Args:
            tf: Timeframe label.
            candle: A finalized 1m candle to fold into the bucket.
            seeded: Whether this fold comes from the restart seed (durable plane)
                rather than the live stream.
        """
        tf_seconds = self._tf_seconds[tf]
        begin = self._floor(candle.interval_begin, tf_seconds)
        begin_ts = int(begin.timestamp())
        if not seeded:
            frontier = self._closed_window.get((candle.symbol, tf))
            if frontier is not None and begin_ts <= frontier:
                self._record_late(candle.symbol, int(candle.interval_begin.timestamp()))
                return
        key = (candle.symbol, tf, begin_ts)
        bucket = self._buckets.get(key)
        counted_minutes = {candle.interval_begin} if candle.complete else set()
        if bucket is None:
            self._buckets[key] = _HtfBucket(
                symbol=candle.symbol,
                timeframe=tf,
                open=candle.open,
                high=candle.high,
                low=candle.low,
                close=candle.close,
                volume=candle.volume,
                trades=candle.trades,
                vwap_sum=candle.vwap * candle.volume,
                interval_begin=begin,
                open_ts=candle.interval_begin,
                close_ts=candle.interval_begin,
                complete=self._is_complete_on_open(candle.interval_begin, begin_ts, seeded=seeded),
                seeded=seeded,
                expected_minute_count=len(counted_minutes),
                counted_minutes=counted_minutes,
            )
            return
        if seeded:
            bucket.seeded = True
        if candle.complete and candle.interval_begin not in bucket.counted_minutes:
            bucket.counted_minutes.add(candle.interval_begin)
            bucket.expected_minute_count += 1
        bucket.high = max(bucket.high, candle.high)
        bucket.low = min(bucket.low, candle.low)
        bucket.volume += candle.volume
        bucket.trades += candle.trades
        bucket.vwap_sum += candle.vwap * candle.volume
        if candle.interval_begin < bucket.open_ts:
            bucket.open = candle.open
            bucket.open_ts = candle.interval_begin
        if candle.interval_begin >= bucket.close_ts:
            bucket.close = candle.close
            bucket.close_ts = candle.interval_begin

    def _is_complete_on_open(self, first_minute: datetime, begin_ts: int, *, seeded: bool) -> bool:
        """Decide whether a freshly-opened bucket is a trustworthy (complete) bar.

        A window is trustworthy when it was seeded from the durable plane or it
        OPENED at or after the live epoch (so every one of its minutes was
        observed live). In the data path (forward-fill OFF) a window that opened
        STRICTLY AFTER the live epoch was fully observed, so a missing opening
        minute is a genuine no-trade minute and the bucket is trusted; the epoch
        window itself and any seeded window must ALSO have been opened by the
        window's first minute — a conservative guard against a mid-stream join
        (or a seed not reaching the window open) publishing a truncated bar. In
        forward-fill mode
        a missing opening minute of a fully-live window is a genuine no-trade
        minute (forward-fill ASSERTS a continuous corpus per feed), so the
        first-minute requirement is dropped and the partially-filled window is the
        true bar.

        Interaction with the CA-1 epoch floor (forward-fill ONLY): flooring the
        live epoch to the minute makes the restart-EPOCH window (whose start equals
        the floored-epoch minute) trustworthy, so in forward-fill mode it emits
        best-effort even if its first minute is absent. This is intended: the venue
        snapshot-on-subscribe delivers the in-progress minute if it traded (so the
        window is complete in the normal case), and an absent minute is a no-trade
        minute under the continuous-corpus contract — identical to any other
        interior gap forward-fill tolerates. It is strictly better than the
        pre-CA-1 alternative of suppressing the whole window (e.g. a midnight
        restart would otherwise drop the entire restart-day 1d bar). The data path
        (forward-fill OFF) is unaffected — its first-minute requirement still
        suppresses a genuinely truncated epoch window.

        Args:
            first_minute: Interval-begin of the first 1m folded into the bucket.
            begin_ts: The bucket's canonical window-start UNIX timestamp.
            seeded: Whether the opening fold came from the restart seed.

        Returns:
            Whether the bucket may be published once its window closes.
        """
        trustworthy = seeded or begin_ts >= self._live_epoch_ts
        if self._forward_fill:
            return trustworthy
        if not trustworthy:
            return False
        if not seeded and begin_ts > self._live_epoch_ts:
            return True
        return int(first_minute.timestamp()) == begin_ts

    def _emit_closed(self, sym: str) -> list[tuple[str, CandleUpdate]]:
        """Emit and remove every closed bucket for one symbol.

        A bucket ``[begin, begin + tf_seconds)`` is closed when its end is at or
        before the symbol's watermark minus grace. Closed buckets are processed
        in ascending window order so the per-timeframe frontier
        (``_closed_window``) only ever ADVANCES — a removed bucket at or below the
        existing frontier (a stranded pre-epoch / already-sealed window) is dropped
        WITHOUT emitting and WITHOUT rewinding the frontier or carried close, so the
        data and :meth:`flush` paths never disagree and never double-emit. A
        complete emit advances the frontier, carried close, and last-real-window
        marks together.

        Args:
            sym: Symbol whose buckets are evaluated.

        Returns:
            Closed bars as ``(label, candle)`` pairs, ascending timeframe.
        """
        cutoff = self._watermark[sym].timestamp() - self._grace_s
        closed: list[_HtfBucket] = []
        to_remove: list[tuple[str, str, int]] = []
        for key, bucket in self._buckets.items():
            if bucket.symbol != sym:
                continue
            tf_seconds = self._tf_seconds[bucket.timeframe]
            end_ts = int(bucket.interval_begin.timestamp()) + tf_seconds
            if end_ts > cutoff:
                continue
            to_remove.append(key)
            closed.append(bucket)
        for key in to_remove:
            del self._buckets[key]
        closed.sort(key=lambda bucket: int(bucket.interval_begin.timestamp()))
        emitted: list[tuple[int, str, CandleUpdate]] = []
        for bucket in closed:
            fkey = (sym, bucket.timeframe)
            begin_ts = int(bucket.interval_begin.timestamp())
            frontier = self._closed_window.get(fkey)
            if frontier is not None and begin_ts <= frontier:
                continue
            self._closed_window[fkey] = begin_ts
            if not bucket.complete:
                if bucket.seeded:
                    self._seeded_incomplete_windows.append(
                        SeededIncompleteWindow(
                            bucket.symbol,
                            bucket.timeframe,
                            bucket.interval_begin,
                            bucket.expected_minute_count,
                            frozenset(bucket.counted_minutes),
                        )
                    )
                continue
            self._last_close[fkey] = bucket.close
            self._last_real_window[fkey] = begin_ts
            emitted.append(
                (
                    self._tf_seconds[bucket.timeframe],
                    bucket.timeframe,
                    self._to_candle_update(bucket),
                )
            )
        emitted.sort(key=lambda item: item[0])
        return [(label, candle) for _seconds, label, candle in emitted]

    def _symbols_with_state(self) -> set[str]:
        """Return symbols with an open bucket or an established forward-fill baseline.

        Bounds the forward-fill walk to symbols the aggregator has actually
        observed; a symbol that never produced a bar is never forward-filled.

        Returns:
            The set of symbols eligible for a forward-fill walk.
        """
        symbols = {bucket.symbol for bucket in self._buckets.values()}
        symbols.update(sym for sym, _tf in self._last_close)
        return symbols

    def _earliest_ended_live_bucket(
        self, sym: str, tf: str, tf_seconds: int, current_begin_ts: int
    ) -> int | None:
        """Return the earliest live-region, fully-ended real bucket window start.

        Seeds the first forward-fill walk when no frontier exists yet. Only
        buckets that opened at or after the live epoch and whose window ended
        before the current open window count; an incomplete pre-epoch bucket is
        excluded — the data path owns that region.

        Args:
            sym: Symbol.
            tf: Timeframe label.
            tf_seconds: Timeframe width in seconds.
            current_begin_ts: Start of the current open window (UNIX seconds).

        Returns:
            The earliest qualifying window-start UNIX timestamp, or ``None``.
        """
        candidates = [
            begin_ts
            for (bucket_sym, bucket_tf, begin_ts) in self._buckets
            if bucket_sym == sym
            and bucket_tf == tf
            and begin_ts >= self._live_epoch_ts
            and begin_ts + tf_seconds <= current_begin_ts
        ]
        return min(candidates, default=None)

    def _flush_timeframe(
        self, sym: str, tf: str, tf_seconds: int, now: datetime
    ) -> list[tuple[str, CandleUpdate]]:
        """Seal ended real buckets and forward-fill empty windows for one timeframe.

        Walks the live region chronologically from the frontier up to the current
        open window, emitting each wall-clock-ended real bucket and filling each
        empty window with a flat carried-close bar. Two independent bounds keep
        synthesis finite: the per-call walk is capped at
        :data:`_FORWARD_FILL_MAX_WINDOWS` (so one flush after a long stall cannot
        emit thousands of bars), and a flat fill is suppressed once an empty window
        is more than :data:`_FORWARD_FILL_MAX_WINDOWS` past the last REAL bar (so a
        permanently-dark / delisted symbol stops manufacturing bars rather than
        forward-filling forever — the frontier still advances, so no rewalk).

        Args:
            sym: Symbol.
            tf: Timeframe label.
            tf_seconds: Timeframe width in seconds.
            now: Current wall-clock time (UTC).

        Returns:
            Sealed and forward-filled bars as ``(timeframe_label, candle)`` pairs.
        """
        current_begin_ts = int(self._floor(now, tf_seconds).timestamp())
        frontier = self._closed_window.get((sym, tf))
        last_close = self._last_close.get((sym, tf))
        last_real = self._last_real_window.get((sym, tf))
        if frontier is not None:
            w = frontier + tf_seconds
        else:
            earliest = self._earliest_ended_live_bucket(sym, tf, tf_seconds, current_begin_ts)
            if earliest is None:
                return []
            w = earliest
        w = max(w, self._ceil(self._live_epoch_ts, tf_seconds))
        horizon = _FORWARD_FILL_MAX_WINDOWS * tf_seconds
        if (current_begin_ts - w) // tf_seconds > _FORWARD_FILL_MAX_WINDOWS:
            w = current_begin_ts - horizon
            self._purge_buckets_below(sym, tf, w)
            self._warn_fill_overflow(sym, tf)
        out: list[tuple[str, CandleUpdate]] = []
        while w < current_begin_ts:
            bucket = self._buckets.pop((sym, tf, w), None)
            if bucket is not None and bucket.complete:
                out.append((tf, self._to_candle_update(bucket)))
                last_close = bucket.close
                last_real = w
                self._last_real_window[(sym, tf)] = w
            elif (
                bucket is None
                and last_close is not None
                and last_real is not None
                and w - last_real <= horizon
            ):
                out.append((tf, self._flat_fill(sym, tf_seconds, w, last_close)))
            self._closed_window[(sym, tf)] = w
            if last_close is not None:
                self._last_close[(sym, tf)] = last_close
            w += tf_seconds
        return out

    def _purge_buckets_below(self, sym: str, tf: str, begin_ts: int) -> None:
        """Drop ``(sym, tf)`` buckets that the overflow frontier jump skipped past.

        Without this an overflow jump would orphan never-emitted buckets below the
        new frontier (the frontier guard then blocks them from re-folding), so
        they would linger forever.

        Args:
            sym: Symbol.
            tf: Timeframe label.
            begin_ts: New frontier window-start; buckets strictly below it are
                removed.
        """
        stale = [
            key for key in self._buckets if key[0] == sym and key[1] == tf and key[2] < begin_ts
        ]
        for key in stale:
            del self._buckets[key]

    def _flat_fill(
        self, sym: str, tf_seconds: int, window_begin_ts: int, last_close: float
    ) -> CandleUpdate:
        """Build a flat synthetic bar carrying the prior close for an empty window.

        Args:
            sym: Symbol.
            tf_seconds: Timeframe width in seconds (carried for provenance).
            window_begin_ts: Canonical UTC window-start UNIX timestamp.
            last_close: Close of the last sealed bar, used for OHLC and VWAP.

        Returns:
            A zero-volume :class:`CandleUpdate` flat at ``last_close``.
        """
        return CandleUpdate(
            symbol=sym,
            open=last_close,
            high=last_close,
            low=last_close,
            close=last_close,
            vwap=last_close,
            trades=0,
            volume=0.0,
            interval_begin=datetime.fromtimestamp(window_begin_ts, UTC),
            interval=tf_seconds,
        )

    def _warn_fill_overflow(self, sym: str, tf: str) -> None:
        """Warn once per ``(symbol, timeframe)`` when a forward-fill is truncated.

        The dedupe set is bounded like :attr:`_warned_late`: at
        :data:`_WARNED_LATE_CAP` it is cleared so warnings may re-fire while
        memory stays bounded.

        Args:
            sym: Symbol whose forward-fill exceeded the per-flush window bound.
            tf: Timeframe label.
        """
        marker = (sym, tf)
        if marker in self._warned_fill_overflow:
            return
        if len(self._warned_fill_overflow) >= _WARNED_LATE_CAP:
            self._warned_fill_overflow.clear()
        self._warned_fill_overflow.add(marker)
        logger.warning(
            f"forward-fill exceeded {_FORWARD_FILL_MAX_WINDOWS} windows for symbol={sym} "
            f"timeframe={tf}; frontier jumped (earlier empty windows are NOT back-filled; "
            f"a long-dark or delisted instrument is the likely cause)"
        )

    def _to_candle_update(self, bucket: _HtfBucket) -> CandleUpdate:
        """Project a bucket to a :class:`CandleUpdate`.

        VWAP is the volume-weighted combination of the constituent 1m VWAPs
        (``vwap_sum / volume``). ``interval`` carries the timeframe in seconds
        for provenance only — the caller uses the explicit label, never decodes
        this field.

        Args:
            bucket: The closed bucket to project.

        Returns:
            The synthesized higher-TF :class:`CandleUpdate`.
        """
        vwap = bucket.vwap_sum / bucket.volume if bucket.volume > 0 else 0.0
        return CandleUpdate(
            symbol=bucket.symbol,
            open=bucket.open,
            high=bucket.high,
            low=bucket.low,
            close=bucket.close,
            vwap=vwap,
            trades=bucket.trades,
            volume=bucket.volume,
            interval_begin=bucket.interval_begin,
            interval=self._tf_seconds[bucket.timeframe],
            complete=bucket.complete,
        )

    def _record_late(self, sym: str, minute_ts: int) -> None:
        """Count a dropped late 1m and warn once per ``(symbol, minute)``.

        The warn-dedupe set is bounded: when it reaches
        :data:`_WARNED_LATE_CAP` it is cleared (warnings may re-fire afterwards),
        keeping memory bounded under sustained late traffic while
        :attr:`late_rolls_after_close` stays the authoritative monotonic count.

        Args:
            sym: Symbol of the late frame.
            minute_ts: Minute-start UNIX timestamp of the late frame.
        """
        self._late_rolls_after_close += 1
        marker = (sym, minute_ts)
        minute = datetime.fromtimestamp(minute_ts, UTC)
        self._late_drops.append(LateCandleDrop(symbol=sym, minute=minute))
        if marker in self._warned_late:
            return
        if len(self._warned_late) >= _WARNED_LATE_CAP:
            self._warned_late.clear()
        self._warned_late.add(marker)
        logger.warning(
            f"late 1m at/below finalized minute dropped: symbol={sym} "
            f"minute={minute.isoformat()} (its corrective value is NOT "
            f"reflected in synthesized higher-TF bars; widen grace or add "
            f"subtract-replace if this recurs; counter={self._late_rolls_after_close})"
        )
