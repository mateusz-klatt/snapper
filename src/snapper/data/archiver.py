"""Candle cache projection archiver.

Exports active candle data to polygon-compatible CSV files, organized
by exchange and archive_symbol.  Supports merge/dedup with existing
CSV files on disk.

Cache structure:
    ``data/{exchange}/cache/{timespan}/{archive_symbol}/{year}/{date}.csv``

CSV format (identical to Polygon loader output):
    ``timestamp,open,high,low,close,volume,vwap,transactions``
"""

import csv
from collections import defaultdict
from dataclasses import dataclass
from datetime import date
from datetime import datetime
from decimal import Decimal
from pathlib import Path

from snapper.data.repository import DatabaseRepository

_HEADER = [
    "timestamp",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "vwap",
    "transactions",
]


def _format_decimal(value: float | None) -> str:
    """Format a float value for CSV output with full decimal precision.

    Uses Decimal internally to avoid scientific notation and trailing
    zeros, matching Polygon loader CSV output exactly.

    Args:
        value: Float value to format, or None.

    Returns:
        String representation suitable for CSV.
    """
    if value is None:
        return "0"
    s = format(Decimal(str(value)), "f")
    if "." in s:
        s = s.rstrip("0").rstrip(".")
    return s


@dataclass(slots=True)
class ExportResult:
    """Result of a candle cache export operation.

    Attributes:
        files_written: Number of CSV files written.
        rows_exported: Total number of candle rows exported.
    """

    files_written: int
    rows_exported: int


def _candle_row_to_csv_tuple(
    row: tuple[datetime, float, float, float, float, float, float | None, int | None],
) -> tuple[str, ...]:
    """Convert a candle DB row to CSV-ready string tuple.

    Args:
        row: ``(open_at, open, high, low, close, volume, vwap, trades)``.

    Returns:
        Tuple of formatted strings matching ``_HEADER`` column order.
    """
    open_at, o, h, low, c, vol, vwap, trades = row
    return (
        open_at.isoformat(),
        _format_decimal(o),
        _format_decimal(h),
        _format_decimal(low),
        _format_decimal(c),
        _format_decimal(vol),
        _format_decimal(vwap),
        str(trades or 0),
    )


def _read_existing_csv(path: Path) -> list[tuple[str, ...]]:
    """Read existing CSV file and return data rows as string tuples.

    Args:
        path: Path to existing CSV file.

    Returns:
        List of string tuples (excluding header row).
    """
    if not path.exists():
        return []
    with path.open(encoding="utf-8", newline="") as f:
        reader = csv.reader(f)
        header = next(reader, None)
        if header is None:
            return []
        return [tuple(row) for row in reader]


def _merge_and_dedup(
    existing: list[tuple[str, ...]],
    new_rows: list[tuple[str, ...]],
) -> list[tuple[str, ...]]:
    """Merge new rows into existing, dedup by full row content, sort by timestamp.

    Args:
        existing: Rows already in CSV file.
        new_rows: New rows from DB export.

    Returns:
        Deduplicated, sorted rows.
    """
    seen: set[tuple[str, ...]] = set()
    merged: list[tuple[str, ...]] = []
    for row in [*existing, *new_rows]:
        if row not in seen:
            seen.add(row)
            merged.append(row)
    merged.sort(key=lambda r: r[0])
    return merged


def _write_candle_csv(path: Path, rows: list[tuple[str, ...]]) -> None:
    """Write candle rows to CSV file in polygon-compatible format.

    Creates parent directories as needed.

    Args:
        path: Target file path.
        rows: Pre-formatted string tuples to write.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle, lineterminator="\n", quoting=csv.QUOTE_MINIMAL)
        writer.writerow(_HEADER)
        writer.writerows(rows)


def _resolve_cache_path(
    base_dir: Path,
    exchange: str,
    timespan: str,
    archive_symbol: str,
    day: date,
) -> Path:
    """Resolve CSV path for candle cache export.

    Matches the layout used by Polygon loader for round-trip
    compatibility.  Daily timespan uses monthly files.

    Args:
        base_dir: Base data directory (e.g. ``Path("data")``).
        exchange: Exchange name (e.g. ``polygon``, ``kraken``).
        timespan: Candle timespan label (e.g. ``minute``, ``day``).
        archive_symbol: Stable archive symbol directory name.
        day: Date for the file.

    Returns:
        Path to the CSV file.
    """
    year_dir = base_dir / exchange / "cache" / timespan / archive_symbol / str(day.year)
    if timespan == "day":
        return year_dir / f"{day.year}-{day.month:02d}.csv"
    return year_dir / f"{day.isoformat()}.csv"


_TIMEFRAME_TO_TIMESPAN = {
    "1m": "minute",
    "5m": "minute",
    "15m": "minute",
    "1h": "hour",
    "1d": "day",
}


def _timeframe_to_timespan(timeframe: str) -> str:
    """Convert short timeframe label to Polygon timespan directory name.

    Args:
        timeframe: Short label (e.g. ``1m``, ``1h``, ``1d``).

    Returns:
        Timespan string (``minute``, ``hour``, ``day``).
    """
    return _TIMEFRAME_TO_TIMESPAN.get(timeframe, "minute")


class CandleCacheArchiver:
    """Export active candle data to polygon-compatible CSV cache files.

    Produces one CSV per day (or per month for daily timespan), with
    merge/dedup against existing files on disk.  Uses sync DB access
    via DatabaseRepository (maintenance/CLI operation).

    Attributes:
        _repo: Sync database repository.
        _base_dir: Base data directory for cache output.
    """

    def __init__(self, repo: DatabaseRepository, base_dir: Path) -> None:
        """Initialize the candle cache archiver.

        Args:
            repo: Sync database repository.
            base_dir: Base data directory (e.g. ``Path("data")``).
        """
        self._repo = repo
        self._base_dir = base_dir

    def export(
        self,
        *,
        exchange: str | None = None,
        archive_symbol: str | None = None,
        timeframe: str = "1m",
        day_start: date,
        day_end: date,
        dry_run: bool = False,
    ) -> ExportResult:
        """Export candle cache projection for a date range.

        Iterates over all matching instruments and exports active
        candle versions grouped by day to polygon-compatible CSV.

        Args:
            exchange: Filter by exchange (optional, exports all if None).
            archive_symbol: Filter by archive_symbol (optional).
            timeframe: Candle timeframe to export (default ``1m``).
            day_start: First day of export range (inclusive).
            day_end: Last day of export range (inclusive).
            dry_run: If True, count rows without writing files.

        Returns:
            ExportResult with file and row counts.
        """
        inst_map = self._repo.get_instrument_archive_map()
        timespan = _timeframe_to_timespan(timeframe)
        files_written = 0
        rows_exported = 0
        for inst_pub_id, (arch_sym, exch) in sorted(inst_map.items()):
            if exchange is not None and exch != exchange:
                continue
            if archive_symbol is not None and arch_sym != archive_symbol:
                continue
            result = self._export_instrument(
                inst_pub_id,
                arch_sym,
                exch,
                timeframe,
                timespan,
                day_start,
                day_end,
                dry_run,
            )
            files_written += result.files_written
            rows_exported += result.rows_exported
        return ExportResult(files_written=files_written, rows_exported=rows_exported)

    def _export_instrument(
        self,
        instrument_public_id: str,
        archive_symbol: str,
        exchange: str,
        timeframe: str,
        timespan: str,
        day_start: date,
        day_end: date,
        dry_run: bool,
    ) -> ExportResult:
        """Export candles for a single instrument over a date range.

        Args:
            instrument_public_id: Instrument to export.
            archive_symbol: Stable archive symbol for path.
            exchange: Exchange name for path.
            timeframe: Candle timeframe.
            timespan: Polygon timespan directory name.
            day_start: First day (inclusive).
            day_end: Last day (inclusive).
            dry_run: Count only, do not write.

        Returns:
            ExportResult for this instrument.
        """
        db_rows = self._repo.get_candles_for_cache_export(
            instrument_public_id,
            timeframe,
            day_start,
            day_end,
        )
        if not db_rows:
            return ExportResult(files_written=0, rows_exported=0)
        csv_tuples = [_candle_row_to_csv_tuple(r) for r in db_rows]
        if timespan == "day":
            return self._write_monthly_files(
                csv_tuples,
                exchange,
                timespan,
                archive_symbol,
                day_start,
                day_end,
                dry_run,
            )
        return self._write_daily_files(
            csv_tuples,
            exchange,
            timespan,
            archive_symbol,
            dry_run,
        )

    def _write_daily_files(
        self,
        csv_tuples: list[tuple[str, ...]],
        exchange: str,
        timespan: str,
        archive_symbol: str,
        dry_run: bool,
    ) -> ExportResult:
        """Group candle rows by day and write per-day CSV files.

        Args:
            csv_tuples: Formatted CSV rows.
            exchange: Exchange name.
            timespan: Timespan directory name.
            archive_symbol: Archive symbol directory name.
            dry_run: Count only.

        Returns:
            ExportResult with counts.
        """
        by_day: dict[date, list[tuple[str, ...]]] = defaultdict(list)
        for row in csv_tuples:
            day = datetime.fromisoformat(row[0]).date()
            by_day[day].append(row)
        files_written = 0
        rows_exported = 0
        for day in sorted(by_day):
            path = _resolve_cache_path(
                self._base_dir,
                exchange,
                timespan,
                archive_symbol,
                day,
            )
            new_rows = by_day[day]
            if dry_run:
                rows_exported += len(new_rows)
                files_written += 1
                continue
            existing = _read_existing_csv(path)
            merged = _merge_and_dedup(existing, new_rows)
            _write_candle_csv(path, merged)
            rows_exported += len(new_rows)
            files_written += 1
        return ExportResult(files_written=files_written, rows_exported=rows_exported)

    def _write_monthly_files(
        self,
        csv_tuples: list[tuple[str, ...]],
        exchange: str,
        timespan: str,
        archive_symbol: str,
        day_start: date,
        day_end: date,
        dry_run: bool,
    ) -> ExportResult:
        """Group candle rows by month and write per-month CSV files.

        Used for daily timespan to match Polygon layout.

        Args:
            csv_tuples: Formatted CSV rows.
            exchange: Exchange name.
            timespan: Timespan directory name.
            archive_symbol: Archive symbol directory name.
            day_start: Range start (for month iteration).
            day_end: Range end.
            dry_run: Count only.

        Returns:
            ExportResult with counts.
        """
        by_month: dict[str, list[tuple[str, ...]]] = defaultdict(list)
        for row in csv_tuples:
            dt = datetime.fromisoformat(row[0])
            key = f"{dt.year}-{dt.month:02d}"
            by_month[key].append(row)
        files_written = 0
        rows_exported = 0
        for month_key in sorted(by_month):
            year, month = month_key.split("-")
            representative_day = date(int(year), int(month), 1)
            path = _resolve_cache_path(
                self._base_dir,
                exchange,
                timespan,
                archive_symbol,
                representative_day,
            )
            new_rows = by_month[month_key]
            if dry_run:
                rows_exported += len(new_rows)
                files_written += 1
                continue
            existing = _read_existing_csv(path)
            merged = _merge_and_dedup(existing, new_rows)
            _write_candle_csv(path, merged)
            rows_exported += len(new_rows)
            files_written += 1
        return ExportResult(files_written=files_written, rows_exported=rows_exported)
