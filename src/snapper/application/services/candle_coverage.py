"""Candle coverage verification — the single-source read-cutover gate (slice 3b).

Candle Phase 3 slice 3b (``proprietary/plans/plan_2026_06_16_candle_phase3_persistence.md``
§4d/§4e). Before slice 4 retires the on-read ``derive_snaps`` aggregation and
serves every ``>1m`` read from the persisted ``candles`` plane, this verifier
proves that plane actually holds the higher-timeframe universe the old read path
served — per instrument and timeframe, UNDER THE LIVE VENUE the read/warmup
resolves (never ``polygon``; see §4e — passing ``polygon`` is rejected).

For each ``(symbol, timeframe)`` the verifier computes an explicit canonical
window ``[start, end]`` (``end`` = the latest fully-closed window as of
``as_of - writer_lag``; ``start`` = ``end`` minus ``min_bars`` slots, and for
``1d`` additionally extended back across the ``cut_date`` seam) and RANGE-reads
the persisted plane over it. It checks the FULL expected slot grid — not just the
newest rows, so a hole anywhere in the window (including the §4e
``[cut_date, publisher_start)`` seam gap) is caught. The plane PASSES only when
every expected canonical slot in ``[start, end]`` is:

- present (no missing interior bar; the ``end`` slot present == fresh),
- ``complete=True`` (trustworthy boundary),
- correctly tagged — ``5m/15m/30m/1h/4h`` ``synthesized``; ``1d`` ``native``
  strictly before ``cut_date`` and ``synthesized`` at/after. For ``1d`` the
  window spans the seam (``cut_date - 1`` native, ``cut_date`` synthesized), so
  the native→synthesized handoff is provably contiguous (the §4e invariant —
  the implied ``warmup_as_of`` is ``cut_date - 1``). Set ``min_bars`` to the
  consumer's required depth (e.g. the warmup lookback) to verify that far back.

When a warm 1m cache is supplied, ``5m/15m/30m`` additionally get a PARITY check
against the retiring ``derive_snaps`` rollup: every derived bar that falls WITHIN
the inspected DB window must exist in the DB with matching OHLCV — the concrete
"no empty/short regression vs the retired derive path" gate. Parity is meaningful
only with a genuinely live (SUB-socket-warm) cache as an independent witness, so
the standalone CLI runs structural checks only (``cache=None``); the parity rung
is for an in-process slice-4 cutover check. Read-only: it never writes.
"""

import math
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import UTC
from datetime import date
from datetime import datetime
from datetime import time

from snapper.application.services.candle_query import DERIVED_AGGREGATION_MAP
from snapper.application.services.candle_query import derive_snaps
from snapper.application.services.market_cache import CandleSnap
from snapper.application.services.market_cache import MarketCacheService
from snapper.core.types import AllExchange
from snapper.core.types import ExchangeEnum
from snapper.data.repository import Repository
from snapper.data.repository_types import CandleRow

__all__ = [
    "CoverageEntry",
    "CoverageReport",
    "VERIFIABLE_TIMEFRAMES",
    "verify_candle_coverage",
]

_TF_SECONDS: dict[str, int] = {
    "5m": 300,
    "15m": 900,
    "30m": 1800,
    "1h": 3600,
    "4h": 14400,
    "1d": 86400,
}

VERIFIABLE_TIMEFRAMES: tuple[str, ...] = ("5m", "15m", "30m", "1h", "4h", "1d")
"""Higher timeframes whose persisted coverage the slice-4 cutover depends on."""

_CACHE_PARITY_LIMIT = 1000
_OHLCV_REL_TOL = 1e-6
_OHLCV_ABS_TOL = 1e-9


@dataclass(frozen=True, slots=True)
class CoverageEntry:
    """Coverage verdict for one ``(symbol, timeframe)`` on the persisted plane.

    Attributes:
        symbol: Native symbol.
        timeframe: Verified timeframe.
        ok: True when the plane satisfies every coverage predicate.
        reason: Human-readable pass note or the first failing predicate.
        bars: Number of persisted rows inspected over the window.
        oldest: Oldest inspected ``open_at`` (None when no rows).
        newest: Newest inspected ``open_at`` (None when no rows).
    """

    symbol: str
    timeframe: str
    ok: bool
    reason: str
    bars: int
    oldest: datetime | None
    newest: datetime | None


@dataclass(frozen=True, slots=True)
class CoverageReport:
    """Aggregate coverage result across all verified ``(symbol, timeframe)`` pairs.

    Attributes:
        entries: Per-pair verdicts.
        ok: True only when every entry passed.
    """

    entries: list[CoverageEntry]
    ok: bool


def _cut_datetime(cut_date: date) -> datetime:
    """Return ``cut_date`` as its UTC midnight boundary."""
    return datetime.combine(cut_date, time.min, tzinfo=UTC)


def _latest_closed_open(as_of: datetime, writer_lag_s: int, tf_seconds: int) -> datetime:
    """Return the open_at of the latest fully-closed canonical window.

    A canonical window opening at ``W`` (a multiple of ``tf_seconds``) is closed
    by time ``T`` iff ``W + tf_seconds <= T``. With ``T = as_of - writer_lag``
    the latest such ``W`` is ``floor(T) - tf_seconds``.

    Args:
        as_of: Reference time.
        writer_lag_s: Grace seconds for the async writer to flush the just-closed
            bar before it is expected present.
        tf_seconds: Timeframe width in seconds.

    Returns:
        UTC datetime of the latest fully-closed window's ``open_at``.
    """
    threshold = int(as_of.timestamp()) - writer_lag_s
    latest = (threshold // tf_seconds) * tf_seconds - tf_seconds
    return datetime.fromtimestamp(latest, UTC)


def _window_start(
    latest_closed: datetime, tf_seconds: int, timeframe: str, cut_dt: datetime, min_bars: int
) -> datetime:
    """Return the oldest canonical ``open_at`` the window must verify.

    The window is ``min_bars`` slots deep; for ``1d`` it is additionally extended
    back to the last native day (``cut_date - 1``) so the native→synthesized seam
    is always inside the inspected range (the §4e contiguity invariant).

    Args:
        latest_closed: Newest canonical ``open_at`` (window end).
        tf_seconds: Timeframe width in seconds.
        timeframe: Verified timeframe.
        cut_dt: Synthesis ownership boundary (UTC midnight).
        min_bars: Minimum window depth in slots.

    Returns:
        UTC datetime of the window's oldest expected ``open_at``.
    """
    start_unix = int(latest_closed.timestamp()) - (min_bars - 1) * tf_seconds
    if timeframe == "1d":
        start_unix = min(start_unix, int(cut_dt.timestamp()) - tf_seconds)
    return datetime.fromtimestamp(start_unix, UTC)


def _expected_source(timeframe: str, open_at: datetime, cut_dt: datetime) -> str:
    """Return the provenance a coverage-correct row must carry.

    ``1d`` is native history strictly before ``cut_date`` and synthesized from
    ``cut_date`` forward; every other higher timeframe is synthesized.

    Args:
        timeframe: Bar timeframe.
        open_at: Bar window start.
        cut_dt: Synthesis ownership boundary (UTC midnight).

    Returns:
        ``"native"`` or ``"synthesized"``.
    """
    if timeframe == "1d" and open_at < cut_dt:
        return "native"
    return "synthesized"


def _evaluate_grid(
    rows: Sequence[CandleRow],
    timeframe: str,
    tf_seconds: int,
    start: datetime,
    end: datetime,
    cut_dt: datetime,
) -> str:
    """Return the first failing predicate over the expected slot grid, else "".

    Walks every canonical slot in ``[start, end]`` (stepping by ``tf_seconds``)
    and requires each to be present, complete and correctly tagged. Because it
    walks the EXPECTED grid (not just the returned rows), a hole anywhere in the
    window — including the §4e seam gap — surfaces as a missing bar.

    Args:
        rows: Range-fetched persisted rows over ``[start, end]``.
        timeframe: Verified timeframe.
        tf_seconds: Timeframe width in seconds.
        start: Oldest expected ``open_at``.
        end: Newest expected ``open_at`` (latest closed window).
        cut_dt: Synthesis ownership boundary.

    Returns:
        Empty string when every slot holds, else the failure reason.
    """
    present = {int(row["open_at"].timestamp()): row for row in rows}
    cursor = int(start.timestamp())
    last = int(end.timestamp())
    while cursor <= last:
        slot = datetime.fromtimestamp(cursor, UTC)
        row = present.get(cursor)
        if row is None:
            return f"missing bar at {slot.isoformat()}"
        if not row["complete"]:
            return f"incomplete bar at {slot.isoformat()}"
        expected = _expected_source(timeframe, slot, cut_dt)
        if row["source"] != expected:
            return f"provenance {row['source']} != {expected} at {slot.isoformat()}"
        cursor += tf_seconds
    return ""


def _ohlcv_matches(row: CandleRow, snap: CandleSnap) -> bool:
    """Return True when a DB row's OHLCV equals a derived snap within tolerance.

    Args:
        row: Persisted candle row.
        snap: Derived :class:`CandleSnap` from the cache rollup.

    Returns:
        True when open/high/low/close/volume match within the OHLCV tolerance.
    """
    return all(
        math.isclose(a, b, rel_tol=_OHLCV_REL_TOL, abs_tol=_OHLCV_ABS_TOL)
        for a, b in (
            (row["open"], snap.open),
            (row["high"], snap.high),
            (row["low"], snap.low),
            (row["close"], snap.close),
            (row["volume"], snap.volume),
        )
    )


def _evaluate_derive_parity(
    rows: Sequence[CandleRow], snaps: Sequence[CandleSnap], minutes_per_bar: int
) -> str:
    """Return the first derive-parity OHLCV mismatch within the window, else "".

    Parity runs only after the structural grid check passed, which already
    guarantees every canonical slot in the inspected window is present. So the
    unique value parity adds is VALUE agreement: every ``derive_snaps`` bar that
    coincides with an inspected DB row must match its OHLCV. Derived bars whose
    slot is not among the inspected rows lie outside the window (older/newer than
    the cache overlap) and are skipped — not evidence of a regression here.

    Args:
        rows: Range-fetched persisted rows for the timeframe (ascending).
        snaps: Warm 1m cache snaps (chronological).
        minutes_per_bar: Aggregation factor for the timeframe.

    Returns:
        Empty string when parity holds, else the failure reason.
    """
    derived = derive_snaps(snaps, minutes_per_bar)
    by_ms = {int(row["open_at"].timestamp() * 1000): row for row in rows}
    for bar in derived:
        row = by_ms.get(bar.open_at_ms)
        if row is None:
            continue
        if not _ohlcv_matches(row, bar):
            return f"derive parity: OHLCV mismatch at open_at_ms={bar.open_at_ms}"
    return ""


async def verify_candle_coverage(
    *,
    repo: Repository,
    cache: MarketCacheService | None,
    exchange: AllExchange,
    native_symbols: Sequence[str],
    timeframes: Sequence[str],
    cut_date: date,
    as_of: datetime,
    writer_lag_s: int,
    min_bars: int,
) -> CoverageReport:
    """Verify the persisted plane is ready for the single-source read cutover.

    Args:
        repo: Repository handle.
        cache: Warm 1m cache for the derive-parity check, or None to skip it.
        exchange: Live venue the persisted bars + the read path resolve under
            (``polygon`` is rejected — it is the cache corpus, not a read venue).
        native_symbols: Symbols to verify (non-empty).
        timeframes: Higher timeframes to verify (non-empty subset of
            :data:`VERIFIABLE_TIMEFRAMES`).
        cut_date: Synthesis ownership boundary (decides ``1d`` provenance + seam).
        as_of: Reference time for the latest-closed-window computation.
        writer_lag_s: Grace seconds for the writer to flush the just-closed bar.
        min_bars: Minimum window depth in slots per pair.

    Returns:
        A :class:`CoverageReport` with a per-pair verdict and an aggregate flag.

    Raises:
        ValueError: When ``exchange`` is ``polygon`` (an orphaned plane), or when
            ``native_symbols``/``timeframes`` is empty, or a timeframe is not
            verifiable (would otherwise vacuously pass or raise downstream).
    """
    if exchange == ExchangeEnum.POLYGON:
        raise ValueError(
            "polygon is the CSV cache segment, not a persisted read venue; "
            "verify under the live venue"
        )
    if not native_symbols or not timeframes:
        raise ValueError("verify_candle_coverage requires non-empty native_symbols and timeframes")
    unknown = [timeframe for timeframe in timeframes if timeframe not in _TF_SECONDS]
    if unknown:
        raise ValueError(f"unverifiable timeframes: {unknown}")
    cut_dt = _cut_datetime(cut_date)
    entries: list[CoverageEntry] = []
    for symbol in native_symbols:
        for timeframe in timeframes:
            tf_seconds = _TF_SECONDS[timeframe]
            end = _latest_closed_open(as_of, writer_lag_s, tf_seconds)
            start = _window_start(end, tf_seconds, timeframe, cut_dt, min_bars)
            rows = await repo.get_candles(
                instrument=symbol,
                timeframe=timeframe,
                start=start,
                end=end,
                exchange=exchange,
                as_of=as_of,
                order="asc",
            )
            reason = _evaluate_grid(rows, timeframe, tf_seconds, start, end, cut_dt)
            if not reason and cache is not None and timeframe in DERIVED_AGGREGATION_MAP:
                snaps = await cache.get_1m_candles(exchange, symbol, limit=_CACHE_PARITY_LIMIT)
                reason = _evaluate_derive_parity(rows, snaps, DERIVED_AGGREGATION_MAP[timeframe])
            entries.append(
                CoverageEntry(
                    symbol=symbol,
                    timeframe=timeframe,
                    ok=not reason,
                    reason=reason or "ok",
                    bars=len(rows),
                    oldest=rows[0]["open_at"] if rows else None,
                    newest=rows[-1]["open_at"] if rows else None,
                )
            )
    return CoverageReport(entries=entries, ok=all(entry.ok for entry in entries))
