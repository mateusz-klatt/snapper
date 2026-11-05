"""Backfill synthesized higher-timeframe candles from persisted 1m candles.

Usage:
    PYTHONPATH=src:proprietary/src .venv/bin/python scripts/backfill_synth_candles.py \
        [--exchanges kraken,kraken_equities,kraken_futures,polygon,walutomat] \
        [--timeframes 5m,15m,30m,1h,4h,1d] \
        [--start UTC --end UTC --insert-only] [--since-days 90] [--batch-size 100] [--dry-run] \
        [--limit-instruments N] [--progress-file PATH]

Replays each instrument's current 1m candles in ascending ``open_at`` order and
rolls them up into every requested higher timeframe using the SAME OHLCV/VWAP
fold as the live ``CandleAggregator`` (open=first, close=last, high=max,
low=min, volume/trades summed, vwap = sum(vwap_i * volume_i) / sum(volume_i)).
One ``source='synthesized'`` bar is emitted per CLOSED historical window that
holds at least one real 1m candle; the still-open current window is skipped.
Bars persist through :meth:`Repository.upsert_historical_synthesized_candles`.
Bounded --start/--end runs require --insert-only and never revise any history.
They ignore the legacy progress log. Reruns cannot improve existing bars after
minute corrections. Legacy lookback runs retain the guarded revision writer.

The window boundary and rollup intentionally mirror
``snapper.messaging.publishers.candle_aggregator`` (UTC-floored windows, 1d at
00:00 UTC). It does NOT reproduce the live aggregator's startup completeness
gate (the first-minute-on-boundary requirement), because that gate exists to
suppress truncated mid-stream-join bars at live startup; every historical window
here is fully observed, so a window with a gappy first minute is the true bar.

Run on the host: the DB host in DB_URL (172.17.0.1, the docker bridge gateway)
is rewritten to 127.0.0.1 so the script reaches the published Postgres port; the
async driver suffix (+asyncpg) is preserved.
"""

import argparse
import asyncio
import sys
from collections.abc import Callable
from collections.abc import Coroutine
from collections.abc import Iterator
from dataclasses import dataclass
from dataclasses import field
from datetime import UTC
from datetime import datetime
from datetime import timedelta
from functools import partial
from itertools import count
from pathlib import Path
from typing import Any
from typing import TextIO
from typing import cast
from uuid import uuid7

from loguru import logger
from sqlalchemy.exc import DBAPIError

from snapper.application.updaters.historical.repair_batch import retry_batch
from snapper.application.updaters.historical.repair_window import Window
from snapper.config.settings import get_settings
from snapper.core.types import AllExchange
from snapper.data.repository import Repository
from snapper.data.repository import get_repository
from snapper.data.repository_types import CandleRow
from snapper.data.repository_types import CandleUpsertRow
from snapper.infrastructure.security.path_validation import resolve_operator_file
from snapper.messaging.publishers.candle_aggregator import SUPPORTED_SYNTHESIS_TIMEFRAMES

_TF_SECONDS: dict[str, int] = {
    "5m": 300,
    "15m": 900,
    "30m": 1800,
    "1h": 3600,
    "4h": 14400,
    "1d": 86400,
}

_DEFAULT_EXCHANGES: tuple[str, ...] = (
    "kraken",
    "kraken_equities",
    "kraken_futures",
    "polygon",
    "walutomat",
)


@dataclass
class _Bucket:
    """Running OHLCV state for one higher-timeframe window during a replay.

    Mirrors the live aggregator's ``_HtfBucket`` fold. Rows arrive in ascending
    ``open_at`` order, so ``open`` is set once at creation (the earliest minute)
    and ``close`` is overwritten by every later minute (the latest wins).
    """

    open: float
    high: float
    low: float
    close: float
    volume: float
    trades: int | None
    vwap_sum: float | None


@dataclass
class _Totals:
    """Run-wide counters surfaced in the final summary."""

    instruments: int = 0
    skipped_no_instrument: int = 0
    skipped_no_1m: int = 0
    windows: int = 0
    written: int = 0
    errors: int = 0
    abort: bool = False


_InstrumentWorker = Callable[[str, str, str], Coroutine[Any, Any, None]]


@dataclass(frozen=True)
class _ExchangeScheduler:
    """Resolve and schedule one exchange's eligible instruments."""

    repository: Repository
    now: datetime
    symbol_filter: set[str] | None
    limit_instruments: int | None
    done: set[tuple[str, str]]
    totals: _Totals
    worker: _InstrumentWorker

    async def process(self, exchange: str) -> None:
        """Schedule every eligible instrument for one exchange.

        Args:
            exchange: Exchange whose native symbols should be processed.
        """
        symbols = await self.repository.get_exchange_instruments(exchange, self.now)
        symbols = _select_exchange_symbols(
            symbols,
            self.symbol_filter,
            self.limit_instruments,
        )
        public_ids = await self.repository.get_instrument_public_ids_by_symbols(
            set(symbols),
            exchange,
            self.now,
        )
        logger.info(f"[{exchange}] {len(symbols)} symbols, {len(public_ids)} resolved")
        eligible: list[tuple[str, str]] = []
        for symbol in symbols:
            if (exchange, symbol) in self.done:
                continue
            instrument_public_id = public_ids.get(symbol)
            if instrument_public_id is None:
                self.totals.skipped_no_instrument += 1
                continue
            eligible.append((symbol, instrument_public_id))
        async with asyncio.TaskGroup() as task_group:
            for symbol, instrument_public_id in eligible:
                task_group.create_task(self.worker(exchange, symbol, instrument_public_id))


def _parse_args(argv: list[str]) -> argparse.Namespace:
    """Parse command-line arguments.

    Args:
        argv: Argument vector excluding the program name.

    Returns:
        The populated argparse namespace.
    """
    parser = argparse.ArgumentParser(description="Backfill synthesized higher-TF candles from 1m.")
    parser.add_argument(
        "--exchanges",
        default=",".join(_DEFAULT_EXCHANGES),
        help="Comma-separated exchanges to process.",
    )
    parser.add_argument(
        "--timeframes",
        default="5m,15m,30m,1h,4h,1d",
        help="Comma-separated higher timeframes to synthesize.",
    )
    parser.add_argument(
        "--symbols",
        default=None,
        help="Comma-separated native symbols to restrict to (default: all on the exchange).",
    )
    parser.add_argument(
        "--start", type=datetime.fromisoformat, help="Inclusive UTC minute boundary."
    )
    parser.add_argument("--end", type=datetime.fromisoformat, help="Exclusive UTC minute boundary.")
    parser.add_argument(
        "--insert-only",
        action="store_true",
        help="Insert only keys with no history; required with --start/--end.",
    )
    parser.add_argument("--since-days", type=int, default=90, help="Lookback window in days.")
    parser.add_argument(
        "--settle-minutes",
        type=int,
        default=120,
        help="Skip windows that closed within the last N minutes (left to the live writer; avoids racing it).",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        choices=range(1, 101),
        default=100,
        help="Rows per short guarded write (1..100).",
    )
    parser.add_argument(
        "--concurrency",
        type=int,
        default=6,
        help="Instruments processed in parallel (keep below the DB pool size).",
    )
    parser.add_argument(
        "--limit-instruments",
        type=int,
        default=None,
        help="Process at most N instruments per exchange (testing).",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Compute and report counts without writing to the DB.",
    )
    parser.add_argument(
        "--progress-file",
        default="scripts/.backfill_synth_progress.jsonl",
        help="File recording completed (exchange, symbol) pairs for resume.",
    )
    parser.add_argument(
        "--db-host-rewrite",
        default="172.17.0.1=127.0.0.1",
        help="SRC=DST host rewrite applied to DB_URL ('' to disable).",
    )
    return parser.parse_args(argv)


def _resolve_db_url(rewrite: str) -> str:
    """Return the bootstrap DB URL with the docker-bridge host rewritten.

    The async driver suffix is preserved; only the host substring is swapped so
    a host-run script reaches the Postgres port published on localhost.

    Args:
        rewrite: ``SRC=DST`` host rewrite, or empty to disable.

    Returns:
        The connection URL to hand to the repository factory.
    """
    db_url: str = get_settings().db_url
    if rewrite:
        src, _, dst = rewrite.partition("=")
        if src and dst:
            db_url = db_url.replace(src, dst)
    return db_url


def _requested_timeframes(raw_timeframes: str) -> tuple[list[str], dict[str, int]]:
    """Validate requested timeframe names and return their widths.

    Args:
        raw_timeframes: Comma-separated timeframe names from the CLI.

    Returns:
        The normalized names and their duration in seconds.

    Raises:
        SystemExit: If any requested timeframe is unsupported.
    """
    timeframes = [tf.strip() for tf in raw_timeframes.split(",") if tf.strip()]
    unknown = [tf for tf in timeframes if tf not in SUPPORTED_SYNTHESIS_TIMEFRAMES]
    if unknown:
        logger.error(
            f"unsupported timeframes {unknown}; supported {sorted(SUPPORTED_SYNTHESIS_TIMEFRAMES)}"
        )
        raise SystemExit(2)
    return timeframes, {tf: _TF_SECONDS[tf] for tf in timeframes}


def _requested_symbols(raw_symbols: str | None) -> set[str] | None:
    """Normalize an optional comma-separated symbol restriction.

    Args:
        raw_symbols: Symbol list from the CLI, or None for every symbol.

    Returns:
        The normalized symbol set, or None when no filter was requested.
    """
    if not raw_symbols:
        return None
    return {symbol.strip() for symbol in raw_symbols.split(",") if symbol.strip()}


def _select_exchange_symbols(
    symbols: list[str],
    symbol_filter: set[str] | None,
    limit: int | None,
) -> list[str]:
    """Apply optional symbol and count restrictions for one exchange.

    Args:
        symbols: Native symbols advertised by the exchange.
        symbol_filter: Explicit symbols requested by the operator.
        limit: Maximum number of matching instruments to process.

    Returns:
        Symbols retained in their original exchange order.
    """
    selected = symbols
    if symbol_filter is not None:
        selected = [symbol for symbol in selected if symbol in symbol_filter]
    if limit is not None:
        selected = selected[:limit]
    return selected


def _load_done(progress_file: Path) -> set[tuple[str, str]]:
    """Load already-completed (exchange, symbol) pairs from the progress file.

    Args:
        progress_file: Path to the progress log (one tab-separated
            ``exchange``/``symbol`` per line); missing file means a fresh run.

    Returns:
        The set of completed (exchange, symbol) pairs.
    """
    safe_progress_file = resolve_operator_file(progress_file, must_exist=False)
    done: set[tuple[str, str]] = set()
    if not safe_progress_file.exists():
        return done
    for line in safe_progress_file.read_text(encoding="utf-8").splitlines():
        ex, _, sym = line.partition("\t")
        if ex and sym:
            done.add((ex, sym))
    return done


def _open_progress_log(progress_file: Path) -> TextIO:
    """Open the canonical progress log for durable append operations.

    Args:
        progress_file: Operator-selected path validated before database access.

    Returns:
        An open text handle owned by the caller.
    """
    safe_progress_file = resolve_operator_file(progress_file, must_exist=False)
    return safe_progress_file.open("a", encoding="utf-8")


def _append_progress(handle: TextIO, exchange: str, symbol: str) -> None:
    """Append one completed instrument to an already-open progress log."""
    handle.write(f"{exchange}\t{symbol}\n")
    handle.flush()


async def _append_progress_safely(handle: TextIO, exchange: str, symbol: str) -> None:
    """Complete an offloaded append before propagating cancellation.

    Args:
        handle: Shared progress log owned by the run.
        exchange: Exchange recorded as complete.
        symbol: Native symbol recorded as complete.
    """
    append_task = asyncio.create_task(asyncio.to_thread(_append_progress, handle, exchange, symbol))
    try:
        await asyncio.shield(append_task)
    except asyncio.CancelledError as cancellation:
        await append_task
        raise cancellation


async def _prepare_progress_log(progress_file: Path, dry_run: bool) -> TextIO | None:
    """Open the write-mode progress log before repository construction.

    Args:
        progress_file: Canonical operator-selected progress path.
        dry_run: Whether the run must avoid creating or modifying the log.

    Returns:
        An open append handle for write mode, otherwise None.
    """
    if dry_run:
        return None
    return await asyncio.to_thread(_open_progress_log, progress_file)


async def _repository_after_progress_preflight(
    db_host_rewrite: str,
    progress_handle: TextIO | None,
) -> Repository:
    """Construct the repository after filesystem preflight succeeds.

    Args:
        db_host_rewrite: Optional host rewrite applied to the configured DB URL.
        progress_handle: Open progress log to close if construction fails.

    Returns:
        The repository used by the backfill run.
    """
    try:
        return get_repository(_resolve_db_url(db_host_rewrite))
    except Exception:
        if progress_handle is not None:
            await asyncio.to_thread(progress_handle.close)
        raise


def _fold_minute(
    buckets: dict[tuple[str, int], _Bucket],
    row: CandleRow,
    tf_secs: dict[str, int],
) -> None:
    """Fold one 1m candle into every requested timeframe's open bucket.

    Rows are replayed in ascending ``open_at`` order, so a bucket's ``open`` is
    fixed when the window is first seen and ``close`` is overwritten by every
    later minute. Unknown VWAP and trade counts propagate to the output.

    Args:
        buckets: Running state keyed by ``(timeframe, window start)``, mutated
            in place.
        row: One current 1m candle for the instrument being replayed.
        tf_secs: Mapping of timeframe label to width in seconds.
    """
    ot = int(row["open_at"].timestamp())
    close = row["close"]
    vwap = row["vwap"]
    trades = row["trades"]
    volume = row["volume"]
    for tf, width in tf_secs.items():
        begin_ts = (ot // width) * width
        key = (tf, begin_ts)
        bucket = buckets.get(key)
        if bucket is None:
            buckets[key] = _Bucket(
                open=row["open"],
                high=row["high"],
                low=row["low"],
                close=close,
                volume=volume,
                trades=trades,
                vwap_sum=vwap * volume if vwap is not None else None,
            )
            continue
        if row["high"] > bucket.high:
            bucket.high = row["high"]
        if row["low"] < bucket.low:
            bucket.low = row["low"]
        bucket.close = close
        bucket.volume += volume
        bucket.trades = (
            bucket.trades + trades if bucket.trades is not None and trades is not None else None
        )
        bucket.vwap_sum = (
            bucket.vwap_sum + vwap * volume
            if bucket.vwap_sum is not None and vwap is not None
            else None
        )


def _roll_up(
    rows: list[CandleRow],
    tf_secs: dict[str, int],
    close_before_ts: int,
    start: datetime | None = None,
) -> dict[str, list[tuple[int, _Bucket]]]:
    """Roll 1m rows up into closed higher-timeframe buckets per timeframe.

    Single ascending pass updating every timeframe's open window. A window is
    emitted only once an observed minute closes at or after its end AND its end
    is older than ``close_before_ts``. The
    ``close_before_ts`` gate leaves the live writer's hot recent windows alone so
    the backfill never races it for the same ``(instrument, timeframe, open_at)``
    current row. The effective cutoff is the smaller of the two.

    Args:
        rows: Current 1m candles for one instrument, ascending by ``open_at``
            (non-empty; the caller skips empty instruments).
        tf_secs: Mapping of timeframe label to width in seconds.
        start: Optional inclusive lower bound; discard leading partial windows.
        close_before_ts: Only emit windows whose end is at or before this UNIX
            second (the settle cutoff: ``now - settle_minutes``).

    Returns:
        Mapping of timeframe label to its list of closed buckets with the
        window-start UNIX second attached as ``(begin_ts, bucket)`` pairs.
    """
    watermark = min(int(rows[-1]["open_at"].timestamp()) + 60, close_before_ts)
    buckets: dict[tuple[str, int], _Bucket] = {}
    for c in rows:
        _fold_minute(buckets, c, tf_secs)
    closed: dict[str, list[tuple[int, _Bucket]]] = {tf: [] for tf in tf_secs}
    for (tf, begin_ts), bucket in buckets.items():
        if start is not None and begin_ts < int(start.timestamp()):
            continue
        if begin_ts + tf_secs[tf] > watermark:
            continue
        closed[tf].append((begin_ts, bucket))
    return closed


def _build_rows(
    closed: dict[str, list[tuple[int, _Bucket]]],
    instrument_public_id: str,
    now: datetime,
    seq: Iterator[int],
    session_id: str,
) -> list[CandleUpsertRow]:
    """Project closed buckets into synthesized candle upsert rows.

    Args:
        closed: Per-timeframe closed buckets from :func:`_roll_up`.
        instrument_public_id: The resolved Instrument id the read path uses.
        now: Write-time stamped on every row.
        seq: Monotonic sequence-id generator (values > 0).
        session_id: One backfill-run session id stamped on every row.

    Missing input VWAP or trade counts remain unknown in the synthesized
    output. For known input statistics, zero-volume windows retain the original
    close-price VWAP convention. Unknown input VWAP always remains null.

    Returns:
        The rows ready for :meth:`Repository.upsert_historical_synthesized_candles`.
    """
    out: list[CandleUpsertRow] = []
    for tf, items in closed.items():
        for begin_ts, bucket in items:
            vwap = None
            if bucket.vwap_sum is not None:
                vwap = bucket.vwap_sum / bucket.volume if bucket.volume > 0 else bucket.close
            out.append(
                CandleUpsertRow(
                    instrument_public_id=instrument_public_id,
                    open_at=datetime.fromtimestamp(begin_ts, UTC),
                    timestamp=now,
                    timeframe=tf,
                    open=bucket.open,
                    high=bucket.high,
                    low=bucket.low,
                    close=bucket.close,
                    volume=bucket.volume,
                    vwap=vwap,
                    trades=bucket.trades,
                    source="synthesized",
                    complete=True,
                    session_id=session_id,
                    sequence_id=next(seq),
                )
            )
    return out


async def _upsert_with_retry(repo: Repository, batch: list[CandleUpsertRow]) -> int:
    """Retry a legacy guarded batch and raise instead of silently skipping conflicts.

    Args:
        repo: Repository owning each short transaction.
        batch: Validated synthesized candidates.

    Returns:
        Acknowledged changed count; exhausted retries propagate to the run's error counter.
    """
    return await retry_batch(partial(repo.upsert_historical_synthesized_candles, batch))


async def _write_batch(repo: Repository, batch: list[CandleUpsertRow], insert_only: bool) -> int:
    """Route repair writes exclusively through all-version insert-only absence checks.

    Args:
        repo: Repository with owned short transactions.
        batch: Closed synthesized candidates.
        insert_only: Whether every source and every knowledge version is protected.

    Returns:
        Acknowledged inserted or changed row count.
    """
    if insert_only:
        inserted = await retry_batch(partial(repo.insert_missing_synthesized_candles, batch))
        return len(inserted)
    return await _upsert_with_retry(repo, batch)


@dataclass(frozen=True)
class _ReplayOptions:
    """Share fixed bounded replay settings across instrument workers."""

    tf_secs: dict[str, int]
    cutoff: datetime
    end: datetime
    close_before_ts: int
    batch_size: int
    session_id: str
    dry_run: bool
    insert_only: bool = False
    totals: _Totals = field(default_factory=_Totals)


async def _process_instrument(
    repo: Repository,
    exchange: str,
    symbol: str,
    instrument_public_id: str,
    options: _ReplayOptions,
) -> tuple[int, int, int]:
    """Synthesize whole windows through the guarded historical writer.

    Args:
        repo: Repository handle.
        exchange: Exchange owning the instrument.
        symbol: Native symbol.
        instrument_public_id: Exact active instrument identity.
        options: Fixed bounds, requested timeframes and persistence limits.

    Returns:
        Source minute count, emitted window count and changed row count.
    """
    rows = await repo.get_candles(
        symbol,
        "1m",
        options.cutoff,
        options.end,
        cast(AllExchange, exchange),
        datetime.now(UTC),
        order="asc",
    )
    rows = [
        row for row in rows if options.cutoff <= row["open_at"] < options.end and row["complete"]
    ]
    if not rows:
        return 0, 0, 0
    closed = _roll_up(rows, options.tf_secs, options.close_before_ts, options.cutoff)
    synth_rows = _build_rows(
        closed, instrument_public_id, datetime.now(UTC), count(1), options.session_id
    )
    if not synth_rows:
        return len(rows), 0, 0
    written = 0
    if not options.dry_run:
        for offset in range(0, len(synth_rows), min(options.batch_size, 100)):
            changed = await _write_batch(
                repo,
                synth_rows[offset : offset + min(options.batch_size, 100)],
                options.insert_only,
            )
            written += changed
            options.totals.written += changed
    return len(rows), len(synth_rows), written


def _requested_window(args: argparse.Namespace, acquired: datetime) -> Window:
    """Validate paired explicit bounds or derive the legacy lookback range.

    Args:
        args: Operator arguments with optional explicit start and end.
        acquired: Actual run time, independent of the requested candle range.

    Returns:
        The validated half-open historical minute range.
    """
    start = cast(datetime | None, getattr(args, "start", None))
    end = cast(datetime | None, getattr(args, "end", None))
    if (start is None) != (end is None):
        raise ValueError("--start and --end must be supplied together")
    if start is not None and not getattr(args, "insert_only", False):
        raise ValueError("--insert-only is required with --start/--end")
    if args.batch_size < 1 or args.settle_minutes < 0:
        raise ValueError("batch-size must be positive and settle-minutes nonnegative")
    if start is None or end is None:
        end = acquired.replace(second=0, microsecond=0)
        start = end - timedelta(days=args.since_days)
    return Window(start, end)


async def _run_progress(args: argparse.Namespace) -> tuple[set[tuple[str, str]], TextIO | None]:
    """Read legacy progress only when the run has no explicit repair bounds.

    Args:
        args: Parsed lookback, bounded-run and progress-file options.

    Returns:
        Completed instrument keys and an optional owned progress log handle.
    """
    progress_file = resolve_operator_file(
        Path(args.progress_file),
        must_exist=False,
    )
    done = (
        set()
        if getattr(args, "start", None) is not None
        else await asyncio.to_thread(_load_done, progress_file)
    )
    progress_handle = await _prepare_progress_log(
        progress_file, args.dry_run or getattr(args, "start", None) is not None
    )
    return done, progress_handle


async def run(args: argparse.Namespace) -> _Totals:
    """Drive the full backfill across the requested exchanges.

    Args:
        args: Parsed command-line arguments.

    Returns:
        The aggregate run counters.
    """
    timeframes, tf_secs = _requested_timeframes(args.timeframes)
    exchanges = [ex.strip() for ex in args.exchanges.split(",") if ex.strip()]
    symbol_filter = _requested_symbols(args.symbols)
    acquired = datetime.now(UTC)
    window = _requested_window(args, acquired)
    close_before_ts = min(
        int(window.end.timestamp()), int(acquired.timestamp()) - args.settle_minutes * 60
    )
    done, progress_handle = await _run_progress(args)
    session_id = str(uuid7())
    totals = _Totals()
    mode = "DRY-RUN" if args.dry_run else "WRITE"
    logger.info(
        f"[{mode}] exchanges={exchanges} timeframes={timeframes} concurrency={args.concurrency} "
        f"window={window.start.isoformat()}..{window.end.isoformat()} resume_skip={len(done)} session={session_id}"
    )
    repo = await _repository_after_progress_preflight(
        args.db_host_rewrite,
        progress_handle,
    )
    semaphore = asyncio.Semaphore(max(1, args.concurrency))
    bookkeeping = asyncio.Lock()

    async def worker(exchange: str, symbol: str, instrument_public_id: str) -> None:
        """Process one instrument under the concurrency gate and record results."""
        async with semaphore:
            if totals.abort:
                return
            try:
                n_1m, n_windows, n_written = await _process_instrument(
                    repo,
                    exchange,
                    symbol,
                    instrument_public_id,
                    _ReplayOptions(
                        tf_secs,
                        window.start,
                        window.end,
                        close_before_ts,
                        args.batch_size,
                        session_id,
                        args.dry_run,
                        insert_only=getattr(args, "insert_only", False),
                        totals=totals,
                    ),
                )
            except Exception as exc:
                async with bookkeeping:
                    totals.errors += 1
                    totals.abort |= isinstance(exc, DBAPIError)
                logger.exception(f"[{exchange}] {symbol}: FAILED: {exc}")
                return
        async with bookkeeping:
            totals.instruments += 1
            totals.windows += n_windows
            if n_1m == 0:
                totals.skipped_no_1m += 1
            logger.info(f"[{exchange}] {symbol}: 1m={n_1m} windows={n_windows} wrote={n_written}")
            if progress_handle is not None:
                await _append_progress_safely(progress_handle, exchange, symbol)

    scheduler = _ExchangeScheduler(
        repository=repo,
        now=acquired,
        symbol_filter=symbol_filter,
        limit_instruments=args.limit_instruments,
        done=done,
        totals=totals,
        worker=worker,
    )
    try:
        for exchange in exchanges:
            await scheduler.process(exchange)
            if totals.abort:
                break
    finally:
        try:
            engine = getattr(repo, "engine", None)
            if engine is not None:
                await engine.dispose()
        finally:
            if progress_handle is not None:
                await asyncio.to_thread(progress_handle.close)
    logger.info(
        f"[{mode}] DONE instruments={totals.instruments} windows={totals.windows} "
        f"wrote={totals.written} no_instrument={totals.skipped_no_instrument} "
        f"no_1m={totals.skipped_no_1m} errors={totals.errors}"
    )
    return totals


def main() -> int:
    """Entry point: configure logging, run the backfill, return an exit code.

    Returns:
        ``0`` when every instrument succeeded, ``1`` when any instrument errored.
    """
    logger.remove()
    logger.add(sys.stderr, level="INFO")
    totals = asyncio.run(run(_parse_args(sys.argv[1:])))
    return 1 if totals.errors else 0


if __name__ == "__main__":
    raise SystemExit(main())
