"""Finalize native base candles so the DB persists only closed bars by default.

A venue's native candle channel (Kraken spot ``ohlc:1m``, or a trade-built 1m)
emits many intra-minute frames for the same window as the bar forms. Publishing
every frame to ZMQ is correct (the "living" candle a UI redraws in place), but
persisting every frame SCD2-supersedes the row on each price change — the
"temporary candle" churn. Kraken gives NO venue close signal, so finality must
be inferred. This finalizer holds the latest frame per ``(instrument, timeframe)``
window and releases it as a final ``complete=True`` row exactly once:

- ``observe`` releases the previous held bar when a strictly-later ``open_at``
  arrives for the same key (boundary detection);
- ``flush`` releases held bars whose window has ended past a grace, catching an
  illiquid or stalled symbol that never gets a later bar;
- ``drain`` releases every ended held bar on shutdown before the writer queue is
  joined.

A monotonic per-key released watermark makes the three paths mutually
at-most-once (a window finalized by one is a no-op for the others) and drops
late frames at/below a finalized window (it never re-opens a sealed window — the
SCD2 re-fragment hazard the aggregator avoids). With ``persist_intermediate``
enabled the in-progress frames are also released (as built, ``complete`` carrying
the window-closed flag) ahead of the final, restoring per-frame persistence with
one extra final version per window; the default leaves the DB with exactly one
final bar per window.

It is the persistence-side analogue of
:class:`snapper.infrastructure.exchanges._trade_candle_builder.TradeCandleBuilder`
(trades to 1m) and :class:`snapper.messaging.publishers.candle_aggregator.CandleAggregator`
(1m to higher TF), and reuses their proven hold/finalize/watermark shape.
"""

from dataclasses import dataclass
from datetime import datetime
from datetime import timedelta
from typing import cast

from loguru import logger

from snapper.data.repository_types import CandleUpsertRow

_TIMEFRAME_WINDOW_SECONDS: dict[str, int] = {
    "1m": 60,
    "5m": 300,
    "15m": 900,
    "30m": 1800,
    "1h": 3600,
    "4h": 14400,
    "1d": 86400,
}
"""Canonical candle-window width per timeframe label, in seconds.

Keyed by the timeframe LABEL, not ``CandleUpdate.interval`` — venues disagree on
the latter's unit (Kraken spot encodes ``1m`` as ``interval=1`` minute, not 60
seconds), so the label is the only portable width signal."""

_WARNED_LATE_CAP: int = 4096


@dataclass
class _HeldCandle:
    """The latest in-progress row for one open window, plus its native symbol.

    ``CandleUpsertRow`` carries ``instrument_public_id`` but not the native
    symbol, which the publisher's per-symbol persist gate needs, so the symbol is
    held alongside the row.
    """

    native_symbol: str
    row: CandleUpsertRow


def window_seconds(timeframe: str) -> int:
    """Return the window width in seconds for a timeframe label, or 0 if unknown.

    Args:
        timeframe: The timeframe label (e.g. ``"1m"``).

    Returns:
        The window width in seconds, or ``0`` for an unrecognized label.
    """
    return _TIMEFRAME_WINDOW_SECONDS.get(timeframe, 0)


class NativeCandleFinalizer:
    """Hold native candle frames and release each window's final bar once."""

    def __init__(self, *, persist_intermediate: bool, flush_grace_seconds: float) -> None:
        """Initialize the finalizer.

        Args:
            persist_intermediate: When True, ``observe`` also releases the
                in-progress frame (as built) so intermediate ``complete=False``
                rows persist and are SCD2-superseded by the final; the default
                (False) persists only the final ``complete=True`` bar per window.
            flush_grace_seconds: Seconds subtracted from ``now`` in ``flush`` so
                a just-ended window is held long enough for the venue's final
                frame before being released.
        """
        self._persist_intermediate = persist_intermediate
        self._flush_grace_s = flush_grace_seconds
        self._held: dict[tuple[str, str], _HeldCandle] = {}
        self._released_open_at: dict[tuple[str, str], datetime] = {}
        self._late_count = 0
        self._warned_late: set[tuple[str, str, datetime]] = set()

    @property
    def late_count(self) -> int:
        """Return the monotonic count of dropped late frames.

        Returns:
            The number of frames dropped for arriving at or below an
            already-finalized window.
        """
        return self._late_count

    def observe(
        self, native_symbol: str, row: CandleUpsertRow
    ) -> list[tuple[str, CandleUpsertRow]]:
        """Hold ``row`` and release any bar its arrival finalizes.

        Args:
            native_symbol: The native symbol of the frame (for the persist gate).
            row: The built candle row for the current frame.

        Returns:
            ``(native_symbol, row)`` pairs to persist: the finalized predecessor
            (``complete=True``) when ``open_at`` advances past the held window,
            plus — only when ``persist_intermediate`` is set — the current frame
            as built, in finalized-then-intermediate order. Empty in the default
            mode until a window finalizes, and for a frame at/below a finalized
            window (dropped as late).
        """
        key = (row["instrument_public_id"], row["timeframe"])
        open_at = row["open_at"]
        released = self._released_open_at.get(key)
        if released is not None and open_at <= released:
            self._record_late(key, open_at)
            return []
        out: list[tuple[str, CandleUpsertRow]] = []
        held = self._held.get(key)
        if held is not None and open_at > held.row["open_at"]:
            out.append(self._finalize(key, held))
        self._held[key] = _HeldCandle(native_symbol, row)
        if self._persist_intermediate:
            out.append((native_symbol, row))
        return out

    def flush(self, now: datetime) -> list[tuple[str, CandleUpsertRow]]:
        """Release held bars whose window ended more than the grace ago.

        Args:
            now: Current wall-clock time (UTC).

        Returns:
            ``(native_symbol, final_row)`` pairs for every held window that has
            ended past the grace, finalized ``complete=True``.
        """
        return self._release_ended(now - timedelta(seconds=self._flush_grace_s))

    def drain(self, now: datetime) -> list[tuple[str, CandleUpsertRow]]:
        """Release every ended held bar on shutdown (no grace).

        The still-open current window is intentionally left unreleased (its loss
        is crash-equivalent and self-heals on restart); only windows whose end is
        at/before ``now`` are finalized. Consumers are already stopped when this
        runs, so no late frame can arrive and the grace is unnecessary.

        Args:
            now: Current wall-clock time (UTC).

        Returns:
            ``(native_symbol, final_row)`` pairs for every ended held window.
        """
        return self._release_ended(now)

    def _release_ended(self, effective: datetime) -> list[tuple[str, CandleUpsertRow]]:
        """Finalize every held window whose end is at/before ``effective``.

        Args:
            effective: The cutoff instant; a held window ending at/before it is
                released.

        Returns:
            ``(native_symbol, final_row)`` pairs for the released windows.
        """
        out: list[tuple[str, CandleUpsertRow]] = []
        for key in list(self._held.keys()):
            held = self._held[key]
            window_end = held.row["open_at"] + timedelta(
                seconds=window_seconds(held.row["timeframe"])
            )
            if window_end <= effective:
                out.append(self._finalize(key, held))
        return out

    def _finalize(self, key: tuple[str, str], held: _HeldCandle) -> tuple[str, CandleUpsertRow]:
        """Seal a held window: stamp ``complete=True``, advance the watermark.

        Args:
            key: The ``(instrument_public_id, timeframe)`` window key.
            held: The held candle being finalized.

        Returns:
            The ``(native_symbol, final_row)`` pair with ``complete=True``.
        """
        final_row = cast(CandleUpsertRow, {**held.row, "complete": True})
        self._released_open_at[key] = held.row["open_at"]
        self._held.pop(key, None)
        return (held.native_symbol, final_row)

    def _record_late(self, key: tuple[str, str], open_at: datetime) -> None:
        """Count a dropped late frame and warn once per ``(key, window)``.

        The warn-dedupe set is bounded: at :data:`_WARNED_LATE_CAP` it is cleared
        (warnings may re-fire afterwards), keeping memory bounded while
        :attr:`late_count` stays the authoritative monotonic count.

        Args:
            key: The ``(instrument_public_id, timeframe)`` window key.
            open_at: The dropped frame's window start.
        """
        self._late_count += 1
        marker = (key[0], key[1], open_at)
        if marker in self._warned_late:
            return
        if len(self._warned_late) >= _WARNED_LATE_CAP:
            self._warned_late.clear()
        self._warned_late.add(marker)
        logger.warning(
            f"native candle at/below finalized window dropped: "
            f"instrument={key[0]} timeframe={key[1]} open_at={open_at.isoformat()}"
        )
