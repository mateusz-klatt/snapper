"""Tests for candle cache projection archiver."""

import csv
from datetime import UTC
from datetime import date
from datetime import datetime
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock
from unittest.mock import patch

from typer.testing import CliRunner

from snapper.cli.app import app
from snapper.data.archiver import CandleCacheArchiver
from snapper.data.archiver import ExportResult
from snapper.data.archiver import _candle_row_to_csv_tuple
from snapper.data.archiver import _format_decimal
from snapper.data.archiver import _merge_and_dedup
from snapper.data.archiver import _read_existing_csv
from snapper.data.archiver import _resolve_cache_path
from snapper.data.archiver import _timeframe_to_timespan
from snapper.data.archiver import _write_candle_csv
from snapper.data.models import KNOWN_TO_MAX
from snapper.data.models import Base
from snapper.data.models import Candle
from snapper.data.models import Instrument
from snapper.data.models import Symbol
from snapper.data.repository import DatabaseRepository


def test_format_decimal_none() -> None:
    """Format None as zero string.

    Given: None value,
    When: _format_decimal is called,
    Then: Returns "0".
    """
    assert _format_decimal(None) == "0"


def test_format_decimal_zero_int() -> None:
    """Format integer zero as "0" with no decimal point.

    Given: Integer zero value,
    When: _format_decimal is called,
    Then: Returns "0" without decimal processing.
    """
    assert _format_decimal(0) == "0"


def test_format_decimal_integer() -> None:
    """Format integer float without trailing decimals.

    Given: Float with zero fractional part,
    When: _format_decimal is called,
    Then: Returns integer string without decimal point.
    """
    assert _format_decimal(100.0) == "100"


def test_format_decimal_preserves_precision() -> None:
    """Format float preserving significant digits.

    Given: Float with meaningful decimal digits,
    When: _format_decimal is called,
    Then: Returns string with trailing zeros stripped.
    """
    assert _format_decimal(1.23450) == "1.2345"


def test_format_decimal_small_value() -> None:
    """Format very small float without scientific notation.

    Given: Very small float value,
    When: _format_decimal is called,
    Then: Returns fixed-point string.
    """
    result = _format_decimal(0.00000123)
    assert "e" not in result
    assert result.startswith("0.0000012")


def test_candle_row_to_csv_tuple() -> None:
    """Convert candle DB row to CSV string tuple.

    Given: A candle row tuple from DB,
    When: _candle_row_to_csv_tuple is called,
    Then: Returns formatted strings matching polygon CSV format.
    """
    row = (
        datetime(2024, 1, 1, 14, 30, tzinfo=UTC),
        100.5,
        110.0,
        90.0,
        105.0,
        1234.56,
        103.2,
        42,
    )
    result = _candle_row_to_csv_tuple(row)
    assert result[0] == "2024-01-01T14:30:00+00:00"
    assert result[1] == "100.5"
    assert result[6] == "103.2"
    assert result[7] == "42"


def test_candle_row_to_csv_tuple_none_vwap_trades() -> None:
    """Handle None vwap and trades in candle row.

    Given: A candle row with None vwap and trades,
    When: _candle_row_to_csv_tuple is called,
    Then: vwap is "0" and trades is "0".
    """
    row = (
        datetime(2024, 1, 1, 14, 30, tzinfo=UTC),
        100.0,
        110.0,
        90.0,
        105.0,
        1234.0,
        None,
        None,
    )
    result = _candle_row_to_csv_tuple(row)
    assert result[6] == "0"
    assert result[7] == "0"


def test_read_existing_csv_nonexistent(tmp_path: Path) -> None:
    """Return empty list for nonexistent file.

    Given: Path that does not exist,
    When: _read_existing_csv is called,
    Then: Returns empty list.
    """
    assert _read_existing_csv(tmp_path / "missing.csv") == []


def test_read_existing_csv_with_data(tmp_path: Path) -> None:
    """Read data rows from existing CSV file.

    Given: CSV file with header and two data rows,
    When: _read_existing_csv is called,
    Then: Returns two tuples (header excluded).
    """
    path = tmp_path / "test.csv"
    path.write_text(
        "timestamp,open,high,low,close,volume,vwap,transactions\n"
        "2024-01-01T14:30:00+00:00,100,110,90,105,1234,103,42\n"
        "2024-01-01T14:31:00+00:00,105,115,95,110,2000,107,50\n",
        encoding="utf-8",
    )
    result = _read_existing_csv(path)
    assert len(result) == 2
    assert result[0][0] == "2024-01-01T14:30:00+00:00"


def test_read_existing_csv_empty_file(tmp_path: Path) -> None:
    """Return empty list for completely empty file.

    Given: An empty file with no content,
    When: _read_existing_csv is called,
    Then: Returns empty list.
    """
    path = tmp_path / "empty.csv"
    path.write_text("", encoding="utf-8")
    assert _read_existing_csv(path) == []


def test_read_existing_csv_empty_marker(tmp_path: Path) -> None:
    """Return empty list for CSV with only header.

    Given: CSV file with header but no data rows,
    When: _read_existing_csv is called,
    Then: Returns empty list.
    """
    path = tmp_path / "empty.csv"
    path.write_text(
        "timestamp,open,high,low,close,volume,vwap,transactions\n",
        encoding="utf-8",
    )
    assert _read_existing_csv(path) == []


def test_merge_and_dedup_removes_duplicates() -> None:
    """Merge two lists and remove duplicates.

    Given: Existing and new rows with overlap,
    When: _merge_and_dedup is called,
    Then: Returns deduplicated rows sorted by timestamp.
    """
    existing = [("2024-01-01T14:30:00+00:00", "100", "110", "90", "105", "1234", "103", "42")]
    new = [
        ("2024-01-01T14:30:00+00:00", "100", "110", "90", "105", "1234", "103", "42"),
        ("2024-01-01T14:31:00+00:00", "105", "115", "95", "110", "2000", "107", "50"),
    ]
    result = _merge_and_dedup(existing, new)
    assert len(result) == 2
    assert result[0][0] == "2024-01-01T14:30:00+00:00"
    assert result[1][0] == "2024-01-01T14:31:00+00:00"


def test_merge_and_dedup_sorts_by_timestamp() -> None:
    """Merged result is sorted by timestamp.

    Given: Rows in reverse order,
    When: _merge_and_dedup is called,
    Then: Returns rows sorted ascending by timestamp.
    """
    rows = [
        ("2024-01-01T14:31:00+00:00", "105", "115", "95", "110", "2000", "107", "50"),
        ("2024-01-01T14:30:00+00:00", "100", "110", "90", "105", "1234", "103", "42"),
    ]
    result = _merge_and_dedup([], rows)
    assert result[0][0] == "2024-01-01T14:30:00+00:00"


def test_write_candle_csv_creates_file(tmp_path: Path) -> None:
    """Write candle rows to CSV with polygon-compatible format.

    Given: List of formatted row tuples,
    When: _write_candle_csv is called,
    Then: Creates CSV file with correct header and data.
    """
    path = tmp_path / "subdir" / "test.csv"
    rows = [("2024-01-01T14:30:00+00:00", "100", "110", "90", "105", "1234", "103", "42")]
    _write_candle_csv(path, rows)
    assert path.exists()
    with path.open(encoding="utf-8") as f:
        lines = f.readlines()
    assert lines[0].strip() == "timestamp,open,high,low,close,volume,vwap,transactions"
    assert lines[1].strip() == "2024-01-01T14:30:00+00:00,100,110,90,105,1234,103,42"


def test_resolve_cache_path_minute() -> None:
    """Resolve cache path for minute timespan.

    Given: Minute timespan and a specific day,
    When: _resolve_cache_path is called,
    Then: Returns daily CSV file path.
    """
    path = _resolve_cache_path(Path("data"), "polygon", "minute", "BTC-USD", date(2024, 3, 15))
    assert str(path) == "data/polygon/cache/minute/BTC-USD/2024/2024-03-15.csv"


def test_resolve_cache_path_day() -> None:
    """Resolve cache path for day timespan uses monthly file.

    Given: Day timespan and a specific day,
    When: _resolve_cache_path is called,
    Then: Returns monthly CSV file path.
    """
    path = _resolve_cache_path(Path("data"), "polygon", "day", "BTC-USD", date(2024, 3, 15))
    assert str(path) == "data/polygon/cache/day/BTC-USD/2024/2024-03.csv"


def test_timeframe_to_timespan_mapping() -> None:
    """Map timeframe labels to Polygon timespan directory names.

    Given: Various timeframe labels,
    When: _timeframe_to_timespan is called,
    Then: Returns correct timespan strings.
    """
    assert _timeframe_to_timespan("1m") == "minute"
    assert _timeframe_to_timespan("5m") == "minute"
    assert _timeframe_to_timespan("1h") == "hour"
    assert _timeframe_to_timespan("1d") == "day"
    assert _timeframe_to_timespan("unknown") == "minute"


class _StubRepo:
    """Test stub for DatabaseRepository."""

    def __init__(
        self,
        instruments: dict[str, tuple[str, str]],
        candles: dict[str, list[Any]],
    ) -> None:
        self._instruments = instruments
        self._candles = candles

    def get_instrument_archive_map(self) -> dict[str, tuple[str, str]]:
        return self._instruments

    def get_candles_for_cache_export(
        self,
        instrument_public_id: str,
        timeframe: str,
        day_start: date,
        day_end: date,
    ) -> list[tuple[datetime, float, float, float, float, float, float | None, int | None]]:
        return self._candles.get(instrument_public_id, [])


def test_archiver_export_writes_csv(tmp_path: Path) -> None:
    """Full archiver export produces polygon-compatible CSV.

    Given: Stub repo with one instrument and two candles,
    When: CandleCacheArchiver.export is called,
    Then: CSV file created with correct content.
    """
    candle_rows = [
        (datetime(2024, 1, 1, 14, 30, tzinfo=UTC), 100.0, 110.0, 90.0, 105.0, 1234.0, 103.0, 42),
        (datetime(2024, 1, 1, 14, 31, tzinfo=UTC), 105.0, 115.0, 95.0, 110.0, 2000.0, None, None),
    ]
    repo = _StubRepo(
        instruments={"inst-1": ("BTC-USD", "polygon")},
        candles={"inst-1": candle_rows},
    )
    archiver = CandleCacheArchiver(repo, tmp_path)
    result = archiver.export(
        timeframe="1m",
        day_start=date(2024, 1, 1),
        day_end=date(2024, 1, 1),
    )
    assert result.files_written == 1
    assert result.rows_exported == 2
    csv_path = tmp_path / "polygon" / "cache" / "minute" / "BTC-USD" / "2024" / "2024-01-01.csv"
    assert csv_path.exists()
    with csv_path.open(encoding="utf-8") as f:
        reader = csv.reader(f)
        header = next(reader)
        rows = list(reader)
    assert header == ["timestamp", "open", "high", "low", "close", "volume", "vwap", "transactions"]
    assert len(rows) == 2
    assert rows[0][0] == "2024-01-01T14:30:00+00:00"
    assert rows[1][6] == "0"


def test_archiver_export_dry_run(tmp_path: Path) -> None:
    """Dry run counts rows without writing files.

    Given: Stub repo with candles,
    When: export called with dry_run=True,
    Then: Returns counts but no files created.
    """
    candle_rows = [
        (datetime(2024, 1, 1, 14, 30, tzinfo=UTC), 100.0, 110.0, 90.0, 105.0, 1234.0, 103.0, 42),
    ]
    repo = _StubRepo(
        instruments={"inst-1": ("BTC-USD", "polygon")},
        candles={"inst-1": candle_rows},
    )
    archiver = CandleCacheArchiver(repo, tmp_path)
    result = archiver.export(
        timeframe="1m",
        day_start=date(2024, 1, 1),
        day_end=date(2024, 1, 1),
        dry_run=True,
    )
    assert result.rows_exported == 1
    assert result.files_written == 1
    assert not (tmp_path / "polygon").exists()


def test_archiver_export_filters_exchange(tmp_path: Path) -> None:
    """Export filters by exchange.

    Given: Two instruments on different exchanges,
    When: export called with exchange=polygon,
    Then: Only polygon instrument exported.
    """
    candles = {
        "inst-poly": [
            (
                datetime(2024, 1, 1, 14, 30, tzinfo=UTC),
                100.0,
                110.0,
                90.0,
                105.0,
                1234.0,
                None,
                None,
            )
        ],
        "inst-krak": [
            (
                datetime(2024, 1, 1, 14, 30, tzinfo=UTC),
                200.0,
                210.0,
                190.0,
                205.0,
                5000.0,
                None,
                None,
            )
        ],
    }
    repo = _StubRepo(
        instruments={"inst-poly": ("BTC-USD", "polygon"), "inst-krak": ("BTC-USD", "kraken")},
        candles=candles,
    )
    archiver = CandleCacheArchiver(repo, tmp_path)
    result = archiver.export(
        exchange="polygon",
        timeframe="1m",
        day_start=date(2024, 1, 1),
        day_end=date(2024, 1, 1),
    )
    assert result.rows_exported == 1
    assert (
        tmp_path / "polygon" / "cache" / "minute" / "BTC-USD" / "2024" / "2024-01-01.csv"
    ).exists()
    assert not (tmp_path / "kraken").exists()


def test_archiver_export_filters_archive_symbol(tmp_path: Path) -> None:
    """Export filters by archive_symbol.

    Given: Two instruments with different archive_symbols,
    When: export called with archive_symbol=BTC-USD,
    Then: Only BTC-USD instrument exported.
    """
    candles = {
        "inst-btc": [
            (
                datetime(2024, 1, 1, 14, 30, tzinfo=UTC),
                100.0,
                110.0,
                90.0,
                105.0,
                1234.0,
                None,
                None,
            )
        ],
        "inst-eth": [
            (
                datetime(2024, 1, 1, 14, 30, tzinfo=UTC),
                200.0,
                210.0,
                190.0,
                205.0,
                5000.0,
                None,
                None,
            )
        ],
    }
    repo = _StubRepo(
        instruments={"inst-btc": ("BTC-USD", "polygon"), "inst-eth": ("ETH-USD", "polygon")},
        candles=candles,
    )
    archiver = CandleCacheArchiver(repo, tmp_path)
    result = archiver.export(
        archive_symbol="BTC-USD",
        timeframe="1m",
        day_start=date(2024, 1, 1),
        day_end=date(2024, 1, 1),
    )
    assert result.rows_exported == 1


def test_archiver_export_no_candles(tmp_path: Path) -> None:
    """Export with no matching candles produces no files.

    Given: Instrument with no candles in date range,
    When: export is called,
    Then: Returns zero counts.
    """
    repo = _StubRepo(instruments={"inst-1": ("BTC-USD", "polygon")}, candles={})
    archiver = CandleCacheArchiver(repo, tmp_path)
    result = archiver.export(
        timeframe="1m",
        day_start=date(2024, 1, 1),
        day_end=date(2024, 1, 1),
    )
    assert result == ExportResult(files_written=0, rows_exported=0)


def test_archiver_export_merges_with_existing(tmp_path: Path) -> None:
    """Export merges new rows with existing CSV file.

    Given: Existing CSV with one row and DB with two rows (one overlapping),
    When: export is called,
    Then: Merged file has two unique rows.
    """
    csv_dir = tmp_path / "polygon" / "cache" / "minute" / "BTC-USD" / "2024"
    csv_dir.mkdir(parents=True)
    csv_path = csv_dir / "2024-01-01.csv"
    csv_path.write_text(
        "timestamp,open,high,low,close,volume,vwap,transactions\n"
        "2024-01-01T14:30:00+00:00,100,110,90,105,1234,103,42\n",
        encoding="utf-8",
    )
    candle_rows = [
        (datetime(2024, 1, 1, 14, 30, tzinfo=UTC), 100.0, 110.0, 90.0, 105.0, 1234.0, 103.0, 42),
        (datetime(2024, 1, 1, 14, 31, tzinfo=UTC), 105.0, 115.0, 95.0, 110.0, 2000.0, 107.0, 50),
    ]
    repo = _StubRepo(
        instruments={"inst-1": ("BTC-USD", "polygon")},
        candles={"inst-1": candle_rows},
    )
    archiver = CandleCacheArchiver(repo, tmp_path)
    archiver.export(timeframe="1m", day_start=date(2024, 1, 1), day_end=date(2024, 1, 1))
    with csv_path.open(encoding="utf-8") as f:
        reader = csv.reader(f)
        next(reader)
        rows = list(reader)
    assert len(rows) == 2


def test_archiver_export_daily_timespan_monthly_files(tmp_path: Path) -> None:
    """Daily timespan produces monthly CSV files.

    Given: Candles with 1d timeframe spanning two months,
    When: export is called with timeframe=1d,
    Then: Creates monthly files matching polygon layout.
    """
    candle_rows = [
        (datetime(2024, 1, 15, tzinfo=UTC), 100.0, 110.0, 90.0, 105.0, 1234.0, None, None),
        (datetime(2024, 2, 10, tzinfo=UTC), 200.0, 210.0, 190.0, 205.0, 5000.0, None, None),
    ]
    repo = _StubRepo(
        instruments={"inst-1": ("BTC-USD", "polygon")},
        candles={"inst-1": candle_rows},
    )
    archiver = CandleCacheArchiver(repo, tmp_path)
    result = archiver.export(
        timeframe="1d",
        day_start=date(2024, 1, 1),
        day_end=date(2024, 2, 28),
    )
    assert result.files_written == 2
    jan = tmp_path / "polygon" / "cache" / "day" / "BTC-USD" / "2024" / "2024-01.csv"
    feb = tmp_path / "polygon" / "cache" / "day" / "BTC-USD" / "2024" / "2024-02.csv"
    assert jan.exists()
    assert feb.exists()


def test_archiver_export_daily_timespan_dry_run(tmp_path: Path) -> None:
    """Daily timespan dry run counts without writing.

    Given: Candles with 1d timeframe,
    When: export called with timeframe=1d and dry_run=True,
    Then: Returns counts but no files created.
    """
    candle_rows = [
        (datetime(2024, 1, 15, tzinfo=UTC), 100.0, 110.0, 90.0, 105.0, 1234.0, None, None),
    ]
    repo = _StubRepo(
        instruments={"inst-1": ("BTC-USD", "polygon")},
        candles={"inst-1": candle_rows},
    )
    archiver = CandleCacheArchiver(repo, tmp_path)
    result = archiver.export(
        timeframe="1d",
        day_start=date(2024, 1, 1),
        day_end=date(2024, 1, 31),
        dry_run=True,
    )
    assert result.rows_exported == 1
    assert result.files_written == 1
    assert not (tmp_path / "polygon").exists()


def test_archiver_export_multi_day(tmp_path: Path) -> None:
    """Export spanning multiple days creates per-day files.

    Given: Candles across two days,
    When: export is called with two-day range,
    Then: Creates separate CSV for each day.
    """
    candle_rows = [
        (datetime(2024, 1, 1, 14, 30, tzinfo=UTC), 100.0, 110.0, 90.0, 105.0, 1234.0, None, None),
        (datetime(2024, 1, 2, 10, 0, tzinfo=UTC), 200.0, 210.0, 190.0, 205.0, 5000.0, None, None),
    ]
    repo = _StubRepo(
        instruments={"inst-1": ("BTC-USD", "polygon")},
        candles={"inst-1": candle_rows},
    )
    archiver = CandleCacheArchiver(repo, tmp_path)
    result = archiver.export(
        timeframe="1m",
        day_start=date(2024, 1, 1),
        day_end=date(2024, 1, 2),
    )
    assert result.files_written == 2
    assert (
        tmp_path / "polygon" / "cache" / "minute" / "BTC-USD" / "2024" / "2024-01-01.csv"
    ).exists()
    assert (
        tmp_path / "polygon" / "cache" / "minute" / "BTC-USD" / "2024" / "2024-01-02.csv"
    ).exists()


def _make_repo_with_candles(
    tmp_path: Path,
) -> tuple[Any, str]:
    """Create DB with Symbol, Instrument, and Candle rows for integration tests.

    Returns:
        Tuple of (DatabaseRepository instance, instrument_public_id).
    """
    db_url = f"sqlite:///{tmp_path / 'test.db'}"
    repo = DatabaseRepository(db_url)
    Base.metadata.create_all(repo.engine)
    ts = datetime(2024, 1, 1, tzinfo=UTC)
    with repo.get_session() as session:
        sym = Symbol(
            public_id="sym-btc",
            native_symbol="BTC-USD",
            base="BTC",
            quote="USD",
            asset_type="crypto",
            session_id="test",
            sequence_id=1,
            timestamp=ts,
            created_at=ts,
        )
        session.add(sym)
        session.flush()
        inst = Instrument(
            public_id="inst-btc",
            symbol_public_id="sym-btc",
            exchange="polygon",
            session_id="test",
            sequence_id=2,
            timestamp=ts,
        )
        session.add(inst)
        session.flush()
        candle = Candle(
            public_id="candle-1",
            instrument_public_id="inst-btc",
            open_at=datetime(2024, 1, 1, 14, 30, tzinfo=UTC),
            timeframe="1m",
            open=100.5,
            high=110.0,
            low=90.0,
            close=105.0,
            volume=1234.56,
            vwap=103.2,
            trades=42,
            session_id="test",
            sequence_id=3,
            timestamp=ts,
            known_to=KNOWN_TO_MAX,
        )
        session.add(candle)
        session.commit()
    return repo, "inst-btc"


def test_repo_get_candles_for_cache_export(tmp_path: Path) -> None:
    """Repository returns candle data for cache export.

    Given: DB with one active candle,
    When: get_candles_for_cache_export is called,
    Then: Returns one row with correct values.
    """
    repo, inst_id = _make_repo_with_candles(tmp_path)
    rows = repo.get_candles_for_cache_export(inst_id, "1m", date(2024, 1, 1), date(2024, 1, 1))
    assert len(rows) == 1
    open_at, o, h, low, c, vol, vwap, trades = rows[0]
    assert open_at == datetime(2024, 1, 1, 14, 30, tzinfo=UTC)
    assert o == 100.5
    assert vwap == 103.2
    assert trades == 42
    repo.dispose()


def test_repo_get_candles_for_cache_export_empty_range(tmp_path: Path) -> None:
    """Repository returns empty list for date range with no candles.

    Given: DB with candle on 2024-01-01,
    When: get_candles_for_cache_export called for 2024-02-01,
    Then: Returns empty list.
    """
    repo, inst_id = _make_repo_with_candles(tmp_path)
    rows = repo.get_candles_for_cache_export(inst_id, "1m", date(2024, 2, 1), date(2024, 2, 1))
    assert rows == []
    repo.dispose()


def test_repo_get_instrument_archive_map(tmp_path: Path) -> None:
    """Repository builds instrument-to-archive-symbol mapping.

    Given: DB with Symbol, Instrument,
    When: get_instrument_archive_map is called,
    Then: Returns mapping with correct archive_symbol and exchange.
    """
    repo, _inst_id = _make_repo_with_candles(tmp_path)
    inst_map = repo.get_instrument_archive_map()
    assert "inst-btc" in inst_map
    arch_sym, exchange = inst_map["inst-btc"]
    assert arch_sym == "BTC-USD"
    assert exchange == "polygon"
    repo.dispose()


def test_repo_get_instrument_archive_map_skips_orphan(tmp_path: Path) -> None:
    """Instruments with unmapped symbol_public_id are excluded.

    Given: Instrument referencing a symbol_public_id not in archive_symbols,
    When: get_instrument_archive_map is called,
    Then: That instrument is not in the result mapping.
    """
    db_url = f"sqlite:///{tmp_path / 'orphan.db'}"
    repo = DatabaseRepository(db_url)
    Base.metadata.create_all(repo.engine)
    ts = datetime(2024, 1, 1, tzinfo=UTC)
    with repo.get_session() as session:
        inst = Instrument(
            public_id="inst-orphan",
            symbol_public_id="sym-nonexistent",
            exchange="polygon",
            session_id="test",
            sequence_id=1,
            timestamp=ts,
        )
        session.add(inst)
        session.commit()
    inst_map = repo.get_instrument_archive_map()
    assert "inst-orphan" not in inst_map
    repo.dispose()


def test_repo_resolve_native_to_archive_symbol(tmp_path: Path) -> None:
    """Resolve current native_symbol to stable archive_symbol via DB.

    Given: DB with Symbol BTC-USD,
    When: resolve_native_to_archive_symbol called with BTC-USD,
    Then: Returns BTC-USD archive_symbol.
    """
    repo, _inst_id = _make_repo_with_candles(tmp_path)
    result = repo.resolve_native_to_archive_symbol("BTC-USD")
    assert result == "BTC-USD"
    repo.dispose()


def test_repo_resolve_native_to_archive_symbol_missing(tmp_path: Path) -> None:
    """Return None when native_symbol not found in DB.

    Given: DB with Symbol BTC-USD,
    When: resolve_native_to_archive_symbol called with UNKNOWN,
    Then: Returns None.
    """
    repo, _inst_id = _make_repo_with_candles(tmp_path)
    result = repo.resolve_native_to_archive_symbol("UNKNOWN")
    assert result is None
    repo.dispose()


def test_cli_archive_dry_run(tmp_path: Path) -> None:
    """CLI archive command with --dry-run reports counts.

    Given: Mocked archiver returning export result,
    When: CLI invoked with --day and --dry-run,
    Then: Output shows row and file counts.
    """
    runner = CliRunner()
    mock_archiver = MagicMock()
    mock_archiver.export.return_value = ExportResult(files_written=5, rows_exported=100)
    with (
        patch("snapper.cli.app.BootstrapSettingsLoader") as mock_bootstrap,
        patch("snapper.cli.app.DatabaseRepository") as mock_repo_cls,
        patch("snapper.cli.app.CandleCacheArchiver", return_value=mock_archiver),
    ):
        mock_bootstrap.return_value = MagicMock(db_url="sqlite:///")
        mock_repo_cls.return_value = MagicMock(
            get_archive_symbols=lambda: {"sym-1": "BTC-USD"},
        )
        result = runner.invoke(
            app,
            ["archive", "--day", "2024-01-01", "--dry-run", "--output-dir", str(tmp_path)],
        )
    assert result.exit_code == 0
    assert "100 rows" in result.output
    assert "5 files" in result.output


def test_cli_archive_missing_date_args() -> None:
    """CLI archive command fails when no date arguments provided.

    Given: No --day or --from/--to arguments,
    When: CLI invoked,
    Then: Exits with error code 1.
    """
    runner = CliRunner()
    result = runner.invoke(app, ["archive"])
    assert result.exit_code == 1
    assert "provide --day" in result.output


def test_cli_archive_with_symbol_filter(tmp_path: Path) -> None:
    """CLI archive command resolves --symbol via native_symbol lookup.

    Given: Mocked repo with resolve_native_to_archive_symbol,
    When: CLI invoked with --symbol BTC-USD,
    Then: Archiver called with archive_symbol from DB lookup.
    """
    runner = CliRunner()
    mock_archiver = MagicMock()
    mock_archiver.export.return_value = ExportResult(files_written=1, rows_exported=10)
    with (
        patch("snapper.cli.app.BootstrapSettingsLoader") as mock_bootstrap,
        patch("snapper.cli.app.DatabaseRepository") as mock_repo_cls,
        patch("snapper.cli.app.CandleCacheArchiver", return_value=mock_archiver),
    ):
        mock_bootstrap.return_value = MagicMock(db_url="sqlite:///")
        mock_repo_cls.return_value = MagicMock(
            resolve_native_to_archive_symbol=lambda sym: "BTC-USD",
        )
        result = runner.invoke(
            app,
            [
                "archive",
                "--day",
                "2024-01-01",
                "--symbol",
                "BTC-USD",
                "--output-dir",
                str(tmp_path),
            ],
        )
    assert result.exit_code == 0
    call_kwargs = mock_archiver.export.call_args[1]
    assert call_kwargs["archive_symbol"] == "BTC-USD"


def test_cli_archive_symbol_not_found_uses_safe_path(tmp_path: Path) -> None:
    """CLI archive command falls back to safe_path when symbol not in DB.

    Given: Mocked repo where resolve_native_to_archive_symbol returns None,
    When: CLI invoked with --symbol UNKNOWN,
    Then: Archiver called with safe_path fallback and warning shown.
    """
    runner = CliRunner()
    mock_archiver = MagicMock()
    mock_archiver.export.return_value = ExportResult(files_written=0, rows_exported=0)
    with (
        patch("snapper.cli.app.BootstrapSettingsLoader") as mock_bootstrap,
        patch("snapper.cli.app.DatabaseRepository") as mock_repo_cls,
        patch("snapper.cli.app.CandleCacheArchiver", return_value=mock_archiver),
    ):
        mock_bootstrap.return_value = MagicMock(db_url="sqlite:///")
        mock_repo_cls.return_value = MagicMock(
            resolve_native_to_archive_symbol=lambda sym: None,
        )
        result = runner.invoke(
            app,
            [
                "archive",
                "--day",
                "2024-01-01",
                "--symbol",
                "unknown-sym",
                "--output-dir",
                str(tmp_path),
            ],
        )
    assert result.exit_code == 0
    assert "not found" in result.output
    call_kwargs = mock_archiver.export.call_args[1]
    assert call_kwargs["archive_symbol"] == "UNKNOWN-SYM"


def test_cli_archive_from_to_range(tmp_path: Path) -> None:
    """CLI archive command accepts --from and --to date range.

    Given: Mocked archiver,
    When: CLI invoked with --from and --to,
    Then: Archiver called with correct date range.
    """
    runner = CliRunner()
    mock_archiver = MagicMock()
    mock_archiver.export.return_value = ExportResult(files_written=2, rows_exported=20)
    with (
        patch("snapper.cli.app.BootstrapSettingsLoader") as mock_bootstrap,
        patch("snapper.cli.app.DatabaseRepository") as mock_repo_cls,
        patch("snapper.cli.app.CandleCacheArchiver", return_value=mock_archiver),
    ):
        mock_bootstrap.return_value = MagicMock(db_url="sqlite:///")
        mock_repo_cls.return_value = MagicMock(
            get_archive_symbols=lambda: {},
        )
        result = runner.invoke(
            app,
            [
                "archive",
                "--from",
                "2024-01-01",
                "--to",
                "2024-01-31",
                "--output-dir",
                str(tmp_path),
            ],
        )
    assert result.exit_code == 0
    call_kwargs = mock_archiver.export.call_args[1]
    assert call_kwargs["day_start"] == date(2024, 1, 1)
    assert call_kwargs["day_end"] == date(2024, 1, 31)
