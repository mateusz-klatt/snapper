"""Data archivers for candle cache projection and append-only event tables.

CandleCacheArchiver exports active candle data to polygon-compatible CSV,
organized by exchange and archive_symbol.

EventArchiver exports append-only event rows (Tick, Trade, Signal,
Execution, Telemetry, Control) to per-day CSV files with full temporal
metadata, supporting merge/dedup and optional purge.

Candle cache structure:
    ``data/{exchange}/cache/{timespan}/{archive_symbol}/{year}/{date}.csv``

Event archive structure:
    ``data/archive/{table}/{exchange}/{archive_symbol}/{year}/{date}.csv``
    ``data/archive/{table}/{year}/{date}.csv``  (flat tables)
"""

import csv
import json
from collections import defaultdict
from collections.abc import Callable
from dataclasses import dataclass
from datetime import date
from datetime import datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

from sqlalchemy import JSON
from sqlalchemy import Boolean
from sqlalchemy import DateTime
from sqlalchemy import Float
from sqlalchemy import Integer
from sqlalchemy.types import TypeDecorator

from snapper.data.models import Candle
from snapper.data.models import Control
from snapper.data.models import Execution
from snapper.data.models import Instrument
from snapper.data.models import InstrumentSpec
from snapper.data.models import MarketSnapshot
from snapper.data.models import Order
from snapper.data.models import Position
from snapper.data.models import ProcessRun
from snapper.data.models import Setting
from snapper.data.models import Signal
from snapper.data.models import Symbol
from snapper.data.models import SymbolAlias
from snapper.data.models import SymbolExchangeCapability
from snapper.data.models import Telemetry
from snapper.data.models import Tick
from snapper.data.models import Trade
from snapper.data.models import User
from snapper.data.models import UserLoginEvent
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
    """Result of an archive export operation.

    Attributes:
        files_written: Number of CSV files written.
        rows_exported: Total number of rows exported.
        rows_purged: Number of rows deleted from DB (0 unless purge requested).
    """

    files_written: int
    rows_exported: int
    rows_purged: int = 0


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


_TEMPORAL_COLUMNS = ("public_id", "timestamp", "known_to", "session_id", "sequence_id")


@dataclass(frozen=True, slots=True)
class EventTableSpec:
    """Configuration for an append-only event table archive.

    Attributes:
        model: SQLAlchemy model class.
        columns: Column names to export (temporal + domain), in CSV order.
        group_column: Column used for path partitioning
            (``instrument_public_id``, ``order_public_id``, or None for
            flat tables like Telemetry/Control).
    """

    model: type[Any]
    columns: tuple[str, ...]
    group_column: str | None


EVENT_TABLES: dict[str, EventTableSpec] = {
    "ticks": EventTableSpec(
        model=Tick,
        columns=(
            *_TEMPORAL_COLUMNS,
            "instrument_public_id",
            "bid",
            "ask",
            "last",
            "volume",
        ),
        group_column="instrument_public_id",
    ),
    "trades": EventTableSpec(
        model=Trade,
        columns=(
            *_TEMPORAL_COLUMNS,
            "instrument_public_id",
            "price",
            "size",
            "side",
            "trade_id",
            "executed_at",
        ),
        group_column="instrument_public_id",
    ),
    "signals": EventTableSpec(
        model=Signal,
        columns=(
            *_TEMPORAL_COLUMNS,
            "instrument_public_id",
            "fired_at",
            "side",
            "strength",
            "reason",
            "strategy_name",
            "price",
        ),
        group_column="instrument_public_id",
    ),
    "executions": EventTableSpec(
        model=Execution,
        columns=(
            *_TEMPORAL_COLUMNS,
            "order_public_id",
            "exec_id",
            "trade_id",
            "side",
            "status",
            "price",
            "size",
            "fee",
            "fee_asset",
            "executed_at",
        ),
        group_column="order_public_id",
    ),
    "telemetry": EventTableSpec(
        model=Telemetry,
        columns=(
            *_TEMPORAL_COLUMNS,
            "transport",
            "direction",
            "message_type",
            "payload",
        ),
        group_column=None,
    ),
    "control": EventTableSpec(
        model=Control,
        columns=(
            *_TEMPORAL_COLUMNS,
            "transport",
            "direction",
            "message_type",
            "outcome",
            "detail",
            "payload",
            "client_session_id",
            "client_public_id",
        ),
        group_column=None,
    ),
}


def _format_value(value: datetime | float | int | str | dict[str, Any] | list[Any] | None) -> str:
    """Format a value for archive CSV output.

    Args:
        value: Column value from DB row.

    Returns:
        String representation: ISO format for datetime, full-precision
        decimal for float, compact JSON for dict/list, empty string
        for None.
    """
    if value is None:
        return ""
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, float):
        return _format_decimal(value)
    if isinstance(value, (dict, list)):
        return json.dumps(value, separators=(",", ":"))
    return str(value)


def _merge_and_dedup_events(
    existing: list[tuple[str, ...]],
    new_rows: list[tuple[str, ...]],
) -> list[tuple[str, ...]]:
    """Merge event rows, dedup by (public_id, timestamp, known_to).

    New rows take priority over existing on key collision (e.g. re-export
    of the same event).

    Args:
        existing: Rows already in CSV file.
        new_rows: New rows from DB export.

    Returns:
        Deduplicated rows sorted by timestamp.
    """
    seen: dict[tuple[str, str, str], tuple[str, ...]] = {}
    for row in existing:
        key = (row[0], row[1], row[2])
        seen[key] = row
    for row in new_rows:
        key = (row[0], row[1], row[2])
        seen[key] = row
    return sorted(seen.values(), key=lambda r: r[1])


def _write_event_csv(
    path: Path,
    header: tuple[str, ...],
    rows: list[tuple[str, ...]],
) -> None:
    """Write event rows to CSV file with header.

    Creates parent directories as needed.

    Args:
        path: Target file path.
        header: Column names for the header row.
        rows: Pre-formatted string tuples to write.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle, lineterminator="\n", quoting=csv.QUOTE_MINIMAL)
        writer.writerow(header)
        writer.writerows(rows)


def _resolve_event_path(
    base_dir: Path,
    table_name: str,
    day: date,
    exchange: str | None = None,
    archive_symbol: str | None = None,
) -> Path:
    """Resolve CSV path for event archive export.

    Instrument/order-bound tables include exchange and archive_symbol
    in the path.  Flat tables (telemetry, control) use a simpler layout.

    Args:
        base_dir: Base data directory (e.g. ``Path("data")``).
        table_name: Table name (e.g. ``ticks``, ``telemetry``).
        day: Date for the file.
        exchange: Exchange name (for partitioned tables).
        archive_symbol: Archive symbol (for partitioned tables).

    Returns:
        Path to the CSV file.
    """
    parts = base_dir / "archive" / table_name
    if exchange is not None and archive_symbol is not None:
        parts = parts / exchange / archive_symbol
    return parts / str(day.year) / f"{day.isoformat()}.csv"


class EventArchiver:
    """Export append-only event rows to per-day CSV archive files.

    Handles three table categories:

    - **Instrument-bound** (Tick, Trade, Signal): partitioned by
      exchange and archive_symbol via ``instrument_public_id``.
    - **Order-bound** (Execution): partitioned by exchange and
      archive_symbol via ``order_public_id`` -> Order -> Instrument.
    - **Flat** (Telemetry, Control): no instrument partitioning.

    CSV files contain full temporal metadata (``public_id``, ``timestamp``,
    ``known_to``, ``session_id``, ``sequence_id``) plus all domain columns.
    Merge/dedup with existing files uses ``(public_id, timestamp, known_to)``
    as the dedup key.

    Attributes:
        _repo: Sync database repository.
        _base_dir: Base data directory for archive output.
    """

    def __init__(self, repo: DatabaseRepository, base_dir: Path) -> None:
        """Initialize the event archiver.

        Args:
            repo: Sync database repository.
            base_dir: Base data directory (e.g. ``Path("data")``).
        """
        self._repo = repo
        self._base_dir = base_dir

    def export(
        self,
        *,
        table: str,
        exchange: str | None = None,
        archive_symbol: str | None = None,
        day_start: date,
        day_end: date,
        dry_run: bool = False,
        purge: bool = False,
    ) -> ExportResult:
        """Export event rows for a date range to CSV.

        Queries all rows whose ``timestamp`` falls within
        ``[day_start, day_end]``, groups them by day (and optionally
        by exchange/archive_symbol), and writes per-day CSV files with
        merge/dedup against existing files on disk.

        Args:
            table: Event table name (``ticks``, ``trades``, ``signals``,
                ``executions``, ``telemetry``, ``control``).
            exchange: Filter by exchange (instrument/order-bound only).
            archive_symbol: Filter by archive_symbol.
            day_start: First day (inclusive) of timestamp range.
            day_end: Last day (inclusive) of timestamp range.
            dry_run: If True, count rows without writing files.
            purge: If True, delete exported rows from DB after writing.

        Returns:
            ExportResult with file, row, and purge counts.

        Raises:
            ValueError: If table name is not a valid event table.
        """
        spec = EVENT_TABLES.get(table)
        if spec is None:
            raise ValueError(f"Unknown event table: {table}")

        rows = self._repo.get_event_rows_for_archive(
            spec.model,
            spec.columns,
            day_start,
            day_end,
        )
        if not rows:
            return ExportResult(files_written=0, rows_exported=0, rows_purged=0)

        files, row_ids = self._group_rows(
            spec,
            table,
            rows,
            exchange,
            archive_symbol,
        )

        if dry_run:
            return ExportResult(
                files_written=len(files),
                rows_exported=len(row_ids),
                rows_purged=0,
            )

        files_written = 0
        for path in sorted(files):
            existing = _read_existing_csv(path)
            merged = _merge_and_dedup_events(existing, files[path])
            _write_event_csv(path, spec.columns, merged)
            files_written += 1

        rows_purged = 0
        if purge and row_ids:
            rows_purged = self._repo.delete_rows_by_id(spec.model, row_ids)

        return ExportResult(
            files_written=files_written,
            rows_exported=len(row_ids),
            rows_purged=rows_purged,
        )

    def _group_rows(
        self,
        spec: EventTableSpec,
        table: str,
        rows: list[tuple[Any, ...]],
        exchange: str | None,
        archive_symbol: str | None,
    ) -> tuple[dict[Path, list[tuple[str, ...]]], list[int]]:
        """Convert DB rows to CSV tuples and group by output file path.

        Args:
            spec: Event table specification.
            table: Table name (for path construction).
            rows: Raw DB rows ``(id, col1, col2, ...)``.
            exchange: Optional exchange filter.
            archive_symbol: Optional archive_symbol filter.

        Returns:
            Tuple of (files_dict, row_ids) where files_dict maps
            output paths to CSV row lists, and row_ids tracks exported
            DB ids for purge.
        """
        group_map = self._build_group_map(spec)
        group_col_idx = spec.columns.index(spec.group_column) + 1 if spec.group_column else None

        files: dict[Path, list[tuple[str, ...]]] = defaultdict(list)
        row_ids: list[int] = []

        for row in rows:
            csv_values = tuple(_format_value(v) for v in row[1:])
            path = self._resolve_row_path(
                row,
                table,
                group_col_idx,
                group_map,
                exchange,
                archive_symbol,
            )
            if path is None:
                continue
            files[path].append(csv_values)
            row_ids.append(row[0])

        return files, row_ids

    def _resolve_row_path(
        self,
        row: tuple[Any, ...],
        table: str,
        group_col_idx: int | None,
        group_map: dict[str, tuple[str, str]] | None,
        exchange: str | None,
        archive_symbol: str | None,
    ) -> Path | None:
        """Determine the output CSV path for a single row.

        Returns None if the row should be skipped (unmapped FK or
        filtered out by exchange/archive_symbol).

        Args:
            row: Raw DB row ``(id, col1, col2, ...)``.
            table: Table name.
            group_col_idx: Index of the grouping column in the row,
                or None for flat tables.
            group_map: FK -> (archive_symbol, exchange) mapping.
            exchange: Optional exchange filter.
            archive_symbol: Optional archive_symbol filter.

        Returns:
            Output path, or None to skip.
        """
        ts: datetime = row[2]
        if group_col_idx is None or group_map is None:
            return _resolve_event_path(self._base_dir, table, ts.date())
        fk_value = row[group_col_idx]
        mapping = group_map.get(fk_value)
        if mapping is None:
            return None
        arch_sym, exch = mapping
        if exchange is not None and exch != exchange:
            return None
        if archive_symbol is not None and arch_sym != archive_symbol:
            return None
        return _resolve_event_path(
            self._base_dir,
            table,
            ts.date(),
            exch,
            arch_sym,
        )

    def _build_group_map(
        self,
        spec: EventTableSpec,
    ) -> dict[str, tuple[str, str]] | None:
        """Build the FK -> (archive_symbol, exchange) mapping for a table.

        Args:
            spec: Event table specification.

        Returns:
            Mapping dict, or None for flat tables.
        """
        if spec.group_column == "instrument_public_id":
            return self._repo.get_instrument_archive_map()
        if spec.group_column == "order_public_id":
            return self._repo.get_order_archive_map()
        return None


_CANDLE_AUDIT_COLUMNS = (
    "public_id",
    "timestamp",
    "known_to",
    "session_id",
    "sequence_id",
    "instrument_public_id",
    "open_at",
    "timeframe",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "vwap",
    "trades",
)


class CandleAuditArchiver:
    """Export all candle SCD2 versions to per-day CSV archive files.

    Unlike CandleCacheArchiver (which exports only the latest active
    version in polygon format), this archiver exports the full version
    history with temporal metadata for audit and recovery.

    Output path:
        ``data/archive/candles/{exchange}/{archive_symbol}/{year}/{date}.csv``

    Date grouping uses ``open_at`` (candle time), not ``timestamp``
    (bus_time), so a correction at T+1 for open_at=T archives with
    T's day file.

    Attributes:
        _repo: Sync database repository.
        _base_dir: Base data directory for archive output.
    """

    def __init__(self, repo: DatabaseRepository, base_dir: Path) -> None:
        """Initialize the candle audit archiver.

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
        closed_only: bool = False,
        dry_run: bool = False,
        purge: bool = False,
    ) -> ExportResult:
        """Export candle audit rows for a date range to CSV.

        Args:
            exchange: Filter by exchange (optional).
            archive_symbol: Filter by archive_symbol (optional).
            timeframe: Candle timeframe (e.g. ``1m``, ``1h``, ``1d``).
            day_start: First day (inclusive) of ``open_at`` range.
            day_end: Last day (inclusive) of ``open_at`` range.
            closed_only: If True, only closed versions (known_to < now).
            dry_run: If True, count rows without writing files.
            purge: If True, delete exported rows from DB.
                Only allowed with ``closed_only=True``.

        Returns:
            ExportResult with file, row, and purge counts.

        Raises:
            ValueError: If purge requested without closed_only.
        """
        if purge and not closed_only:
            raise ValueError("Purge requires closed_only=True for candle audit")

        inst_map = self._repo.get_instrument_archive_map()
        files_written = 0
        rows_exported = 0
        all_row_ids: list[int] = []

        for inst_pub_id, (arch_sym, exch) in sorted(inst_map.items()):
            if exchange is not None and exch != exchange:
                continue
            if archive_symbol is not None and arch_sym != archive_symbol:
                continue
            fw, re, ids = self._export_instrument(
                inst_pub_id,
                arch_sym,
                exch,
                timeframe,
                day_start,
                day_end,
                closed_only,
                dry_run,
            )
            files_written += fw
            rows_exported += re
            all_row_ids.extend(ids)

        rows_purged = 0
        if purge and all_row_ids:
            rows_purged = self._repo.delete_rows_by_id(Candle, all_row_ids)

        return ExportResult(
            files_written=files_written,
            rows_exported=rows_exported,
            rows_purged=rows_purged,
        )

    def _export_instrument(
        self,
        instrument_public_id: str,
        archive_symbol: str,
        exchange_name: str,
        timeframe: str,
        day_start: date,
        day_end: date,
        closed_only: bool,
        dry_run: bool,
    ) -> tuple[int, int, list[int]]:
        """Export candle audit rows for one instrument.

        Args:
            instrument_public_id: Instrument to export.
            archive_symbol: Stable archive symbol for path.
            exchange_name: Exchange name for path.
            timeframe: Candle timeframe.
            day_start: First day (inclusive).
            day_end: Last day (inclusive).
            closed_only: Only closed versions.
            dry_run: Count only.

        Returns:
            Tuple of (files_written, rows_exported, row_ids).
        """
        db_rows = self._repo.get_candle_versions_for_archive(
            instrument_public_id,
            timeframe,
            day_start,
            day_end,
            closed_only,
        )
        if not db_rows:
            return 0, 0, []

        by_day: dict[date, list[tuple[str, ...]]] = defaultdict(list)
        row_ids: list[int] = []
        for row in db_rows:
            row_ids.append(row[0])
            csv_values = tuple(_format_value(v) for v in row[1:])
            open_at: datetime = row[7]
            by_day[open_at.date()].append(csv_values)

        if dry_run:
            return len(by_day), len(row_ids), row_ids

        files_written = 0
        for day in sorted(by_day):
            path = _resolve_event_path(
                self._base_dir,
                "candles",
                day,
                exchange_name,
                archive_symbol,
            )
            existing = _read_existing_csv(path)
            merged = _merge_and_dedup_events(existing, by_day[day])
            _write_event_csv(path, _CANDLE_AUDIT_COLUMNS, merged)
            files_written += 1

        return files_written, len(row_ids), row_ids


_TEMPORAL_ORDER = ("public_id", "timestamp", "known_to", "session_id", "sequence_id")


def _get_model_archive_columns(model: type[Any]) -> tuple[str, ...]:
    """Derive CSV column list from a model, temporal columns first.

    Ensures ``(public_id, timestamp, known_to)`` are at indices 0-2 for
    consistent merge/dedup key positioning across all archivers.

    Args:
        model: SQLAlchemy model class with TemporalMixin.

    Returns:
        Tuple of column names excluding ``id``.
    """
    temporal_set = set(_TEMPORAL_ORDER)
    domain = tuple(
        c.name for c in model.__table__.columns if c.name != "id" and c.name not in temporal_set
    )
    return (*_TEMPORAL_ORDER, *domain)


@dataclass(frozen=True, slots=True)
class StateTableSpec:
    """Configuration for a state-SCD2 table archive.

    Attributes:
        model: SQLAlchemy model class.
        group_column: Column name for exchange/symbol partitioning
            (``instrument_public_id``, ``symbol_public_id``, or None
            for flat tables).
    """

    model: type[Any]
    group_column: str | None


STATE_TABLES: dict[str, StateTableSpec] = {
    "orders": StateTableSpec(model=Order, group_column=None),
    "positions": StateTableSpec(model=Position, group_column=None),
    "instruments": StateTableSpec(model=Instrument, group_column="symbol_public_id"),
    "instrument_specs": StateTableSpec(model=InstrumentSpec, group_column=None),
    "settings": StateTableSpec(model=Setting, group_column=None),
    "symbols": StateTableSpec(model=Symbol, group_column=None),
    "symbol_aliases": StateTableSpec(model=SymbolAlias, group_column=None),
    "symbol_exchange_capabilities": StateTableSpec(
        model=SymbolExchangeCapability,
        group_column=None,
    ),
    "users": StateTableSpec(model=User, group_column=None),
    "user_login_events": StateTableSpec(model=UserLoginEvent, group_column=None),
    "market_snapshots": StateTableSpec(model=MarketSnapshot, group_column="instrument_public_id"),
    "process_runs": StateTableSpec(model=ProcessRun, group_column=None),
}


class StateArchiver:
    """Export state-SCD2 table rows to per-day CSV archive files.

    Handles all state-SCD2 tables (Order, Position, Instrument, Setting,
    Symbol, etc.) with full temporal metadata.  Most tables use a flat
    archive layout; Instrument and MarketSnapshot are partitioned by
    exchange and archive_symbol.

    Supports ``closed_only`` filtering and purge with Symbol anchor
    row protection.

    Attributes:
        _repo: Sync database repository.
        _base_dir: Base data directory for archive output.
    """

    def __init__(self, repo: DatabaseRepository, base_dir: Path) -> None:
        """Initialize the state archiver.

        Args:
            repo: Sync database repository.
            base_dir: Base data directory (e.g. ``Path("data")``).
        """
        self._repo = repo
        self._base_dir = base_dir

    def export(
        self,
        *,
        table: str,
        day_start: date,
        day_end: date,
        closed_only: bool = False,
        dry_run: bool = False,
        purge: bool = False,
    ) -> ExportResult:
        """Export state-SCD2 rows for a date range to CSV.

        Args:
            table: State table name (key in ``STATE_TABLES``).
            day_start: First day (inclusive) of ``timestamp`` range.
            day_end: Last day (inclusive) of ``timestamp`` range.
            closed_only: If True, only closed versions (known_to < now).
            dry_run: If True, count rows without writing files.
            purge: If True, delete exported rows from DB.
                Requires ``closed_only=True``.

        Returns:
            ExportResult with file, row, and purge counts.

        Raises:
            ValueError: If table unknown or purge without closed_only.
        """
        spec = STATE_TABLES.get(table)
        if spec is None:
            raise ValueError(f"Unknown state table: {table}")
        if purge and not closed_only:
            raise ValueError("Purge requires closed_only=True for state tables")

        columns = _get_model_archive_columns(spec.model)
        rows = self._repo.get_scd2_rows_for_archive(
            spec.model,
            columns,
            day_start,
            day_end,
            closed_only,
        )
        if not rows:
            return ExportResult(files_written=0, rows_exported=0, rows_purged=0)

        files, row_ids = self._group_rows(spec, table, columns, rows)

        if dry_run:
            return ExportResult(
                files_written=len(files),
                rows_exported=len(row_ids),
                rows_purged=0,
            )

        files_written = 0
        for path in sorted(files):
            existing = _read_existing_csv(path)
            merged = _merge_and_dedup_events(existing, files[path])
            _write_event_csv(path, columns, merged)
            files_written += 1

        rows_purged = 0
        if purge and row_ids:
            purge_ids = self._apply_purge_protection(spec, row_ids)
            rows_purged = self._repo.delete_rows_by_id(spec.model, purge_ids)

        return ExportResult(
            files_written=files_written,
            rows_exported=len(row_ids),
            rows_purged=rows_purged,
        )

    def _group_rows(
        self,
        spec: StateTableSpec,
        table: str,
        columns: tuple[str, ...],
        rows: list[tuple[Any, ...]],
    ) -> tuple[dict[Path, list[tuple[str, ...]]], list[int]]:
        """Convert DB rows to CSV tuples and group by output file path.

        Args:
            spec: State table specification.
            table: Table name (for path construction).
            columns: Column names (for index lookup).
            rows: Raw DB rows ``(id, col1, col2, ...)``.

        Returns:
            Tuple of (files_dict, row_ids).
        """
        group_map, group_col_idx, exchange_col_idx = self._build_group_context(
            spec,
            columns,
        )

        files: dict[Path, list[tuple[str, ...]]] = defaultdict(list)
        row_ids: list[int] = []

        for row in rows:
            csv_values = tuple(_format_value(v) for v in row[1:])
            path = self._resolve_row_path(
                row,
                table,
                group_map,
                group_col_idx,
                exchange_col_idx,
            )
            if path is None:
                continue
            files[path].append(csv_values)
            row_ids.append(row[0])

        return files, row_ids

    def _build_group_context(
        self,
        spec: StateTableSpec,
        columns: tuple[str, ...],
    ) -> tuple[dict[str, tuple[str, str]] | dict[str, str] | None, int | None, int | None]:
        """Build grouping context for path resolution.

        Returns:
            Tuple of (group_map, group_col_idx, exchange_col_idx).
        """
        if spec.group_column is None:
            return None, None, None
        group_col_idx = columns.index(spec.group_column) + 1
        if spec.group_column == "instrument_public_id":
            return self._repo.get_instrument_archive_map(), group_col_idx, None
        if spec.group_column == "symbol_public_id":
            exchange_col_idx = columns.index("exchange") + 1
            return self._repo.get_archive_symbols(), group_col_idx, exchange_col_idx
        raise ValueError(f"Unknown group_column: {spec.group_column}")

    def _resolve_row_path(
        self,
        row: tuple[Any, ...],
        table: str,
        group_map: dict[str, tuple[str, str]] | dict[str, str] | None,
        group_col_idx: int | None,
        exchange_col_idx: int | None,
    ) -> Path | None:
        """Determine the output CSV path for a single state row.

        Args:
            row: Raw DB row ``(id, col1, col2, ...)``.
            table: Table name.
            group_map: FK mapping, or None for flat.
            group_col_idx: Index of grouping column in row.
            exchange_col_idx: Index of exchange column (symbol partition).

        Returns:
            Output path, or None to skip.
        """
        ts: datetime = row[2]
        if group_col_idx is None or group_map is None:
            return _resolve_event_path(self._base_dir, table, ts.date())
        fk_value = row[group_col_idx]
        if exchange_col_idx is not None:
            arch_sym = group_map.get(fk_value)
            if arch_sym is None:
                return None
            exch: str = row[exchange_col_idx]
            return _resolve_event_path(self._base_dir, table, ts.date(), exch, str(arch_sym))
        mapping = group_map.get(fk_value)
        if mapping is None or not isinstance(mapping, tuple):
            return None
        arch_sym_str, exch = mapping
        return _resolve_event_path(self._base_dir, table, ts.date(), exch, arch_sym_str)

    def _apply_purge_protection(
        self,
        spec: StateTableSpec,
        row_ids: list[int],
    ) -> list[int]:
        """Filter row IDs for purge, applying anchor protection for Symbol.

        Symbol anchor rows (first version per public_id) are excluded
        from purge to preserve archive_symbol stability.

        Args:
            spec: State table specification.
            row_ids: All exported row IDs.

        Returns:
            Filtered row IDs safe to purge.
        """
        if spec.model is not Symbol:
            return row_ids
        anchor_ids = self._repo.get_symbol_anchor_ids()
        return [rid for rid in row_ids if rid not in anchor_ids]


_ALL_TABLE_SPECS: dict[str, type[Any]] = {
    **{name: spec.model for name, spec in EVENT_TABLES.items()},
    **{name: spec.model for name, spec in STATE_TABLES.items()},
    "candles": Candle,
}


def _parser_for_column(col_type: Any) -> Callable[[str], Any]:
    """Return a CSV string parser for a SQLAlchemy column type.

    Handles TypeDecorator wrapping (TZDateTime, UUIDColumn) and
    standard SQLAlchemy types.

    Args:
        col_type: SQLAlchemy column type instance.

    Returns:
        Function that converts a CSV string to the correct Python type.
    """
    if isinstance(col_type, TypeDecorator):
        impl = col_type.impl
        if impl is DateTime or isinstance(impl, DateTime):
            return _parse_datetime_csv
        return _parse_str_csv
    if isinstance(col_type, Float):
        return _parse_float_csv
    if isinstance(col_type, Integer):
        return _parse_int_csv
    if isinstance(col_type, Boolean):
        return _parse_bool_csv
    if isinstance(col_type, JSON):
        return _parse_json_csv
    if isinstance(col_type, DateTime):
        return _parse_datetime_csv
    return _parse_str_csv


def _parse_str_csv(s: str) -> str | None:
    return s if s else None


def _parse_datetime_csv(s: str) -> datetime | None:
    return datetime.fromisoformat(s) if s else None


def _parse_float_csv(s: str) -> float | None:
    return float(s) if s else None


def _parse_int_csv(s: str) -> int | None:
    return int(s) if s else None


def _parse_bool_csv(s: str) -> bool | None:
    if not s:
        return None
    return s in ("True", "1", "true")


def _parse_json_csv(s: str) -> dict[str, Any] | list[Any] | None:
    return json.loads(s) if s else None


def _build_column_parsers(
    model: type[Any],
    header: list[str],
) -> list[Callable[[str], Any]]:
    """Build a list of CSV value parsers matching the CSV header order.

    Args:
        model: SQLAlchemy model class.
        header: CSV header column names.

    Returns:
        List of parser functions, one per header column.
    """
    table_cols = {c.name: c for c in model.__table__.columns}
    unknown = [name for name in header if name not in table_cols]
    if unknown:
        raise ValueError(
            f"CSV header contains unknown columns for {model.__tablename__}: {unknown}"
        )
    return [_parser_for_column(table_cols[name].type) for name in header]


@dataclass(slots=True)
class RestoreResult:
    """Result of an archive restore operation.

    Attributes:
        files_processed: Number of CSV files read.
        rows_inserted: Number of rows inserted into DB.
        rows_skipped: Number of rows skipped (already exist).
    """

    files_processed: int
    rows_inserted: int
    rows_skipped: int


class ArchiveRestorer:
    """Restore archived CSV data back into the database.

    Supports audit restore (full history with temporal metadata) for
    all table types.  Deduplicates against existing rows by
    ``(public_id, timestamp, known_to)`` to prevent double-inserts.

    Attributes:
        _repo: Sync database repository.
    """

    def __init__(self, repo: DatabaseRepository) -> None:
        """Initialize the archive restorer.

        Args:
            repo: Sync database repository.
        """
        self._repo = repo

    def restore(
        self,
        *,
        table: str,
        paths: list[Path],
        source: str = "audit",
    ) -> RestoreResult:
        """Restore archived rows from CSV files.

        Args:
            table: Table name (must be a known event, state, or candle table).
            paths: List of CSV file paths to restore.
            source: Restore mode — ``audit`` for full history.

        Returns:
            RestoreResult with counts.

        Raises:
            ValueError: If table unknown or source not supported.
        """
        model = _ALL_TABLE_SPECS.get(table)
        if model is None:
            raise ValueError(f"Unknown table for restore: {table}")
        if source != "audit":
            raise ValueError(f"Restore source '{source}' not yet supported (use 'audit')")

        total_inserted = 0
        total_skipped = 0
        files_processed = 0

        for path in paths:
            inserted, skipped = self._restore_file(model, path)
            total_inserted += inserted
            total_skipped += skipped
            files_processed += 1

        return RestoreResult(
            files_processed=files_processed,
            rows_inserted=total_inserted,
            rows_skipped=total_skipped,
        )

    def _restore_file(
        self,
        model: type[Any],
        path: Path,
    ) -> tuple[int, int]:
        """Restore a single CSV file into the database.

        Args:
            model: SQLAlchemy model class.
            path: CSV file path.

        Returns:
            Tuple of (rows_inserted, rows_skipped).
        """
        with path.open(encoding="utf-8", newline="") as f:
            reader = csv.reader(f)
            header = next(reader, None)
            if header is None:
                return 0, 0
            raw_rows = [tuple(row) for row in reader]

        if not raw_rows:
            return 0, 0

        parsers = _build_column_parsers(model, header)
        existing_keys = self._get_existing_keys(model, raw_rows)

        new_rows: list[dict[str, Any]] = []
        skipped = 0
        for raw in raw_rows:
            key = (raw[0], raw[1], raw[2])
            if key in existing_keys:
                skipped += 1
                continue
            row_dict = {header[i]: parsers[i](raw[i]) for i in range(len(header))}
            new_rows.append(row_dict)

        inserted = self._repo.bulk_insert_from_archive(model, new_rows)
        return inserted, skipped

    def _get_existing_keys(
        self,
        model: type[Any],
        raw_rows: list[tuple[str, ...]],
    ) -> set[tuple[str, str, str]]:
        """Extract date range from CSV rows and query existing dedup keys.

        Args:
            model: SQLAlchemy model class.
            raw_rows: Raw CSV string rows (timestamp at index 1).

        Returns:
            Set of (public_id, timestamp_iso, known_to_iso) tuples.
        """
        timestamps = [r[1] for r in raw_rows]
        min_date = datetime.fromisoformat(min(timestamps)).date()
        max_date = datetime.fromisoformat(max(timestamps)).date()
        return self._repo.get_existing_archive_keys(model, min_date, max_date)
