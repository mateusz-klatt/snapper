"""Backfill synthesized higher-timeframe candles from persisted 1m candles.

Usage:
    PYTHONPATH=src:proprietary/src .venv/bin/python scripts/backfill_synth_candles.py \
        [--exchanges kraken,kraken_equities,kraken_futures,polygon,walutomat] \
        [--timeframes 5m,15m,30m,1h,4h,1d] \
        [--since-days 90] [--batch-size 2000] [--dry-run] \
        [--limit-instruments N] [--progress-file PATH]

Replays each instrument's current 1m candles in ascending ``open_at`` order and
rolls them up into every requested higher timeframe using the SAME OHLCV/VWAP
fold as the live ``CandleAggregator`` (open=first, close=last, high=max,
low=min, volume/trades summed, vwap = sum(vwap_i * volume_i) / sum(volume_i)).
One ``source='synthesized'`` bar is emitted per CLOSED historical window that
holds at least one real 1m candle; the still-open current window is skipped.
Bars persist through the idempotent SCD2 :meth:`Repository.upsert_candles`, so
re-runs only fill gaps (an exact-match row is a no-op) and the job is resumable.

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
from collections.abc import Iterator
from dataclasses import dataclass
from datetime import UTC
from datetime import datetime
from datetime import timedelta
from itertools import count
from pathlib import Path
from typing import cast
from uuid import uuid7

from loguru import logger

from snapper.config.settings import get_settings
from snapper.core.types import AllExchange
from snapper.data.repository import Repository
from snapper.data.repository import get_repository
from snapper.data.repository_types import CandleRow
from snapper.data.repository_types import CandleUpsertRow
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
    trades: int
    vwap_sum: float


@dataclass
class _Totals:
    """Run-wide counters surfaced in the final summary."""

    instruments: int = 0
    skipped_no_instrument: int = 0
    skipped_no_1m: int = 0
    windows: int = 0
    written: int = 0
    errors: int = 0


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
    parser.add_argument("--since-days", type=int, default=90, help="Lookback window in days.")
    parser.add_argument(
        "--settle-minutes",
        type=int,
        default=120,
        help="Skip windows that closed within the last N minutes (left to the live writer; avoids racing it).",
    )
    parser.add_argument("--batch-size", type=int, default=2000, help="Rows per upsert call.")
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


def _load_done(progress_file: Path) -> set[tuple[str, str]]:
    """Load already-completed (exchange, symbol) pairs from the progress file.

    Args:
        progress_file: Path to the progress log (one tab-separated
            ``exchange``/``symbol`` per line); missing file means a fresh run.

    Returns:
        The set of completed (exchange, symbol) pairs.
    """
    done: set[tuple[str, str]] = set()
    if not progress_file.exists():
        return done
    for line in progress_file.read_text(encoding="utf-8").splitlines():
        ex, _, sym = line.partition("\t")
        if ex and sym:
            done.add((ex, sym))
    return done


def _roll_up(
    rows: list[CandleRow], tf_secs: dict[str, int], close_before_ts: int
) -> dict[str, list[tuple[int, _Bucket]]]:
    """Roll 1m rows up into closed higher-timeframe buckets per timeframe.

    Single ascending pass updating every timeframe's open window. A window is
    emitted only once a 1m at or after its end exists in the data AND its end is
    older than ``close_before_ts``. The data-watermark gate mirrors the live
    aggregator (never publish a window with no following minute); the
    ``close_before_ts`` gate leaves the live writer's hot recent windows alone so
    the backfill never races it for the same ``(instrument, timeframe, open_at)``
    current row. The effective cutoff is the smaller of the two.

    Args:
        rows: Current 1m candles for one instrument, ascending by ``open_at``
            (non-empty; the caller skips empty instruments).
        tf_secs: Mapping of timeframe label to width in seconds.
        close_before_ts: Only emit windows whose end is at or before this UNIX
            second (the settle cutoff: ``now - settle_minutes``).

    Returns:
        Mapping of timeframe label to its list of closed buckets with the
        window-start UNIX second attached as ``(begin_ts, bucket)`` pairs.
    """
    watermark = min(int(rows[-1]["open_at"].timestamp()), close_before_ts)
    buckets: dict[tuple[str, int], _Bucket] = {}
    for c in rows:
        ot = int(c["open_at"].timestamp())
        close = c["close"]
        vwap = c["vwap"] if c["vwap"] is not None else close
        trades = c["trades"] or 0
        volume = c["volume"]
        for tf, width in tf_secs.items():
            begin_ts = (ot // width) * width
            key = (tf, begin_ts)
            bucket = buckets.get(key)
            if bucket is None:
                buckets[key] = _Bucket(
                    open=c["open"],
                    high=c["high"],
                    low=c["low"],
                    close=close,
                    volume=volume,
                    trades=trades,
                    vwap_sum=vwap * volume,
                )
                continue
            if c["high"] > bucket.high:
                bucket.high = c["high"]
            if c["low"] < bucket.low:
                bucket.low = c["low"]
            bucket.close = close
            bucket.volume += volume
            bucket.trades += trades
            bucket.vwap_sum += vwap * volume
    closed: dict[str, list[tuple[int, _Bucket]]] = {tf: [] for tf in tf_secs}
    for (tf, begin_ts), bucket in buckets.items():
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

    Returns:
        The synthesized rows ready for :meth:`Repository.upsert_candles`.
    """
    out: list[CandleUpsertRow] = []
    for tf, items in closed.items():
        for begin_ts, bucket in items:
            vwap = bucket.vwap_sum / bucket.volume if bucket.volume > 0 else 0.0
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


async def _process_instrument(
    repo: Repository,
    exchange: str,
    symbol: str,
    instrument_public_id: str,
    tf_secs: dict[str, int],
    cutoff: datetime,
    now: datetime,
    close_before_ts: int,
    batch_size: int,
    session_id: str,
    dry_run: bool,
) -> tuple[int, int, int]:
    """Synthesize and persist higher-TF candles for one instrument.

    Args:
        repo: Repository handle.
        exchange: Exchange the instrument resolves under.
        symbol: Native symbol.
        instrument_public_id: Resolved Instrument id for the synthesized rows.
        tf_secs: Timeframe to width-seconds mapping.
        cutoff: Earliest ``open_at`` to read.
        now: Run start (write-time and 1m read upper bound).
        close_before_ts: Settle cutoff; windows ending after it are skipped.
        batch_size: Rows per upsert call.
        session_id: Backfill-run session id.
        dry_run: When True, compute counts but do not write.

    Returns:
        ``(n_1m, n_windows, n_written)`` for this instrument.
    """
    rows = await repo.get_candles(
        symbol, "1m", cutoff, now, cast(AllExchange, exchange), now, order="asc"
    )
    if not rows:
        return 0, 0, 0
    closed = _roll_up(rows, tf_secs, close_before_ts)
    synth_rows = _build_rows(closed, instrument_public_id, now, count(1), session_id)
    if not synth_rows:
        return len(rows), 0, 0
    written = 0
    if not dry_run:
        for offset in range(0, len(synth_rows), batch_size):
            written += await repo.upsert_candles(synth_rows[offset : offset + batch_size])
    return len(rows), len(synth_rows), written


async def run(args: argparse.Namespace) -> _Totals:
    """Drive the full backfill across the requested exchanges.

    Args:
        args: Parsed command-line arguments.

    Returns:
        The aggregate run counters.
    """
    timeframes = [tf.strip() for tf in args.timeframes.split(",") if tf.strip()]
    unknown = [tf for tf in timeframes if tf not in SUPPORTED_SYNTHESIS_TIMEFRAMES]
    if unknown:
        logger.error(
            f"unsupported timeframes {unknown}; supported {sorted(SUPPORTED_SYNTHESIS_TIMEFRAMES)}"
        )
        raise SystemExit(2)
    tf_secs = {tf: _TF_SECONDS[tf] for tf in timeframes}
    exchanges = [ex.strip() for ex in args.exchanges.split(",") if ex.strip()]
    symbol_filter = (
        {s.strip() for s in args.symbols.split(",") if s.strip()} if args.symbols else None
    )
    now = datetime.now(UTC)
    cutoff = now - timedelta(days=args.since_days)
    close_before_ts = int(now.timestamp()) - args.settle_minutes * 60
    progress_file = Path(args.progress_file)
    done = _load_done(progress_file)
    session_id = str(uuid7())
    totals = _Totals()
    mode = "DRY-RUN" if args.dry_run else "WRITE"
    logger.info(
        f"[{mode}] exchanges={exchanges} timeframes={timeframes} concurrency={args.concurrency} "
        f"window={cutoff.isoformat()}..{now.isoformat()} resume_skip={len(done)} session={session_id}"
    )
    repo = get_repository(_resolve_db_url(args.db_host_rewrite))
    progress_handle = None if args.dry_run else progress_file.open("a", encoding="utf-8")
    semaphore = asyncio.Semaphore(max(1, args.concurrency))
    bookkeeping = asyncio.Lock()

    async def worker(exchange: str, symbol: str, instrument_public_id: str) -> None:
        """Process one instrument under the concurrency gate and record results."""
        async with semaphore:
            try:
                n_1m, n_windows, n_written = await _process_instrument(
                    repo,
                    exchange,
                    symbol,
                    instrument_public_id,
                    tf_secs,
                    cutoff,
                    now,
                    close_before_ts,
                    args.batch_size,
                    session_id,
                    args.dry_run,
                )
            except Exception as exc:
                async with bookkeeping:
                    totals.errors += 1
                logger.exception(f"[{exchange}] {symbol}: FAILED: {exc}")
                return
        async with bookkeeping:
            totals.instruments += 1
            totals.windows += n_windows
            totals.written += n_written
            if n_1m == 0:
                totals.skipped_no_1m += 1
            logger.info(f"[{exchange}] {symbol}: 1m={n_1m} windows={n_windows} wrote={n_written}")
            if progress_handle is not None:
                progress_handle.write(f"{exchange}\t{symbol}\n")
                progress_handle.flush()

    try:
        for exchange in exchanges:
            symbols = await repo.get_exchange_instruments(exchange, now)
            if symbol_filter is not None:
                symbols = [s for s in symbols if s in symbol_filter]
            if args.limit_instruments is not None:
                symbols = symbols[: args.limit_instruments]
            ipid_map = await repo.get_instrument_public_ids_by_symbols(set(symbols), exchange, now)
            logger.info(f"[{exchange}] {len(symbols)} symbols, {len(ipid_map)} resolved")
            tasks: list[asyncio.Task[None]] = []
            for symbol in symbols:
                if (exchange, symbol) in done:
                    continue
                instrument_public_id = ipid_map.get(symbol)
                if instrument_public_id is None:
                    totals.skipped_no_instrument += 1
                    continue
                tasks.append(asyncio.create_task(worker(exchange, symbol, instrument_public_id)))
            if tasks:
                await asyncio.gather(*tasks)
    finally:
        if progress_handle is not None:
            progress_handle.close()
        engine = getattr(repo, "engine", None)
        if engine is not None:
            await engine.dispose()
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
