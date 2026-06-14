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

Phase 1 is publish-only: the caller publishes emitted bars to ZMQ and does NOT
persist them. Persistence plus the ``candle_query`` single-source refactor is a
later phase and is deliberately out of scope here.
"""

from dataclasses import dataclass
from datetime import UTC
from datetime import datetime

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
        """
        self._tf_seconds: dict[str, int] = {
            tf: _TF_SECONDS[tf] for tf in timeframes if tf in _TF_SECONDS
        }
        self._live_epoch_ts = int(live_epoch.timestamp()) if live_epoch is not None else 0
        self._grace_s = grace_seconds
        self._buckets: dict[tuple[str, str, int], _HtfBucket] = {}
        self._open_minutes: dict[str, dict[int, CandleUpdate]] = {}
        self._folded_minute: dict[str, int] = {}
        self._watermark: dict[str, datetime] = {}
        self._closed_window: dict[tuple[str, str], int] = {}
        self._late_rolls_after_close: int = 0
        self._warned_late: set[tuple[str, int]] = set()

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

        Args:
            epoch: The live-consumption start time.
        """
        self._live_epoch_ts = int(epoch.timestamp())

    def timeframe_seconds(self, timeframe: str) -> int:
        """Return the width in seconds of a configured higher timeframe.

        Args:
            timeframe: A timeframe label this aggregator synthesizes.

        Returns:
            The timeframe width in seconds.
        """
        return self._tf_seconds[timeframe]

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

        Args:
            tf: Timeframe label.
            candle: A finalized 1m candle to fold into the bucket.
            seeded: Whether this fold comes from the restart seed (durable plane)
                rather than the live stream.
        """
        tf_seconds = self._tf_seconds[tf]
        begin = self._floor(candle.interval_begin, tf_seconds)
        begin_ts = int(begin.timestamp())
        key = (candle.symbol, tf, begin_ts)
        bucket = self._buckets.get(key)
        if bucket is None:
            opened_live = begin_ts >= self._live_epoch_ts
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
                complete=int(candle.interval_begin.timestamp()) == begin_ts
                and (seeded or opened_live),
            )
            return
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

    def _emit_closed(self, sym: str) -> list[tuple[str, CandleUpdate]]:
        """Emit and remove every closed bucket for one symbol.

        A bucket ``[begin, begin + tf_seconds)`` is closed when its end is at or
        before the symbol's watermark minus grace.

        Args:
            sym: Symbol whose buckets are evaluated.

        Returns:
            Closed bars as ``(label, candle)`` pairs, ascending timeframe.
        """
        cutoff = self._watermark[sym].timestamp() - self._grace_s
        emitted: list[tuple[int, str, CandleUpdate]] = []
        to_remove: list[tuple[str, str, int]] = []
        for key, bucket in self._buckets.items():
            if bucket.symbol != sym:
                continue
            tf_seconds = self._tf_seconds[bucket.timeframe]
            end_ts = int(bucket.interval_begin.timestamp()) + tf_seconds
            if end_ts > cutoff:
                continue
            to_remove.append(key)
            if not bucket.complete:
                continue
            emitted.append((tf_seconds, bucket.timeframe, self._to_candle_update(bucket)))
            self._closed_window[(sym, bucket.timeframe)] = int(bucket.interval_begin.timestamp())
        for key in to_remove:
            del self._buckets[key]
        emitted.sort(key=lambda item: item[0])
        return [(label, candle) for _seconds, label, candle in emitted]

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
        if marker in self._warned_late:
            return
        if len(self._warned_late) >= _WARNED_LATE_CAP:
            self._warned_late.clear()
        self._warned_late.add(marker)
        minute = datetime.fromtimestamp(minute_ts, UTC)
        logger.warning(
            f"late 1m at/below finalized minute dropped: symbol={sym} "
            f"minute={minute.isoformat()} (its corrective value is NOT "
            f"reflected in synthesized higher-TF bars; widen grace or add "
            f"subtract-replace if this recurs; counter={self._late_rolls_after_close})"
        )
