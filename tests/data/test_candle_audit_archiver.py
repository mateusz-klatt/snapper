"""Tests for candle audit archiver (all SCD2 versions with temporal metadata)."""

import csv
from datetime import UTC
from datetime import date
from datetime import datetime
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock
from unittest.mock import patch

import pytest
from typer.testing import CliRunner

from snapper.cli.app import app
from snapper.data.archiver import _CANDLE_AUDIT_COLUMNS
from snapper.data.archiver import CandleAuditArchiver
from snapper.data.archiver import ExportResult
from snapper.data.models import KNOWN_TO_MAX
from snapper.data.models import Base
from snapper.data.models import Candle
from snapper.data.models import Instrument
from snapper.data.models import Symbol
from snapper.data.repository import DatabaseRepository


class _AuditStubRepo:
    """Test stub for DatabaseRepository supporting candle audit archiver."""

    def __init__(
        self,
        instruments: dict[str, tuple[str, str]] | None = None,
        candle_rows: dict[str, list[tuple[Any, ...]]] | None = None,
        delete_count: int = 0,
    ) -> None:
        self._instruments = instruments or {}
        self._candle_rows = candle_rows or {}
        self._delete_count = delete_count
        self.deleted_ids: list[int] = []

    def get_instrument_archive_map(self) -> dict[str, tuple[str, str]]:
        return self._instruments

    def get_candle_versions_for_archive(
        self,
        instrument_public_id: str,
        timeframe: str,
        day_start: date,
        day_end: date,
        closed_only: bool = False,
    ) -> list[tuple[Any, ...]]:
        return self._candle_rows.get(instrument_public_id, [])

    def delete_rows_by_id(self, model: type[Any], row_ids: list[int]) -> int:
        self.deleted_ids.extend(row_ids)
        return self._delete_count or len(row_ids)


def _make_candle_audit_row(
    row_id: int,
    public_id: str,
    ts: datetime,
    known_to: datetime,
    instrument_public_id: str,
    open_at: datetime,
    close: float = 105.0,
) -> tuple[Any, ...]:
    """Build a candle audit DB row tuple (id, *columns)."""
    return (
        row_id,
        public_id,
        ts,
        known_to,
        "test-session",
        row_id,
        instrument_public_id,
        open_at,
        "1m",
        100.0,
        110.0,
        90.0,
        close,
        1234.0,
        103.0,
        42,
    )


def test_candle_audit_columns_count() -> None:
    """Candle audit columns has correct count.

    Given: _CANDLE_AUDIT_COLUMNS,
    When: Length is checked,
    Then: Has 15 columns (temporal + domain).
    """
    assert len(_CANDLE_AUDIT_COLUMNS) == 15
    assert _CANDLE_AUDIT_COLUMNS[0] == "public_id"
    assert _CANDLE_AUDIT_COLUMNS[6] == "open_at"
    assert _CANDLE_AUDIT_COLUMNS[14] == "trades"


def test_candle_audit_export_writes_csv(tmp_path: Path) -> None:
    """Export candle audit rows to per-day CSV files.

    Given: Stub repo with two candle versions for one instrument,
    When: CandleAuditArchiver.export is called,
    Then: CSV file created with correct content.
    """
    open_at = datetime(2024, 1, 1, 14, 30, tzinfo=UTC)
    ts1 = datetime(2024, 1, 1, 15, 0, tzinfo=UTC)
    ts2 = datetime(2024, 1, 1, 15, 1, tzinfo=UTC)
    rows = [
        _make_candle_audit_row(1, "pub-1", ts1, KNOWN_TO_MAX, "inst-1", open_at),
        _make_candle_audit_row(2, "pub-2", ts2, KNOWN_TO_MAX, "inst-1", open_at, close=106.0),
    ]
    repo = _AuditStubRepo(
        instruments={"inst-1": ("BTC-USD", "polygon")},
        candle_rows={"inst-1": rows},
    )
    archiver = CandleAuditArchiver(repo, tmp_path)
    result = archiver.export(
        timeframe="1m",
        day_start=date(2024, 1, 1),
        day_end=date(2024, 1, 1),
    )
    assert result.files_written == 1
    assert result.rows_exported == 2
    assert result.rows_purged == 0
    csv_path = tmp_path / "archive" / "candles" / "polygon" / "BTC-USD" / "2024" / "2024-01-01.csv"
    assert csv_path.exists()
    with csv_path.open(encoding="utf-8") as f:
        reader = csv.reader(f)
        header = next(reader)
        data = list(reader)
    assert header == list(_CANDLE_AUDIT_COLUMNS)
    assert len(data) == 2
    assert data[0][0] == "pub-1"


def test_candle_audit_export_dry_run(tmp_path: Path) -> None:
    """Dry run counts rows without writing files.

    Given: Stub repo with candle rows,
    When: export called with dry_run=True,
    Then: Returns counts but no files created.
    """
    open_at = datetime(2024, 1, 1, 14, 30, tzinfo=UTC)
    ts = datetime(2024, 1, 1, 15, 0, tzinfo=UTC)
    rows = [_make_candle_audit_row(1, "pub-1", ts, KNOWN_TO_MAX, "inst-1", open_at)]
    repo = _AuditStubRepo(
        instruments={"inst-1": ("BTC-USD", "polygon")},
        candle_rows={"inst-1": rows},
    )
    archiver = CandleAuditArchiver(repo, tmp_path)
    result = archiver.export(
        timeframe="1m",
        day_start=date(2024, 1, 1),
        day_end=date(2024, 1, 1),
        dry_run=True,
    )
    assert result.rows_exported == 1
    assert result.files_written == 1
    assert not (tmp_path / "archive").exists()


def test_candle_audit_export_purge_with_closed_only(tmp_path: Path) -> None:
    """Export with purge deletes closed candle versions.

    Given: Stub repo with closed candle rows,
    When: export called with purge=True and closed_only=True,
    Then: delete_rows_by_id called with exported IDs.
    """
    open_at = datetime(2024, 1, 1, 14, 30, tzinfo=UTC)
    ts = datetime(2024, 1, 1, 15, 0, tzinfo=UTC)
    closed_at = datetime(2024, 1, 1, 15, 1, tzinfo=UTC)
    rows = [
        _make_candle_audit_row(1, "pub-1", ts, closed_at, "inst-1", open_at),
        _make_candle_audit_row(2, "pub-2", ts, closed_at, "inst-1", open_at, close=106.0),
    ]
    repo = _AuditStubRepo(
        instruments={"inst-1": ("BTC-USD", "polygon")},
        candle_rows={"inst-1": rows},
    )
    archiver = CandleAuditArchiver(repo, tmp_path)
    result = archiver.export(
        timeframe="1m",
        day_start=date(2024, 1, 1),
        day_end=date(2024, 1, 1),
        closed_only=True,
        purge=True,
    )
    assert result.rows_purged == 2
    assert repo.deleted_ids == [1, 2]


def test_candle_audit_export_purge_without_closed_only_raises(tmp_path: Path) -> None:
    """Purge without closed_only raises ValueError.

    Given: purge=True and closed_only=False,
    When: export is called,
    Then: Raises ValueError.
    """
    repo = _AuditStubRepo(instruments={"inst-1": ("BTC-USD", "polygon")})
    archiver = CandleAuditArchiver(repo, tmp_path)
    s5778_value_1 = date(2024, 1, 1)
    s5778_value_2 = date(2024, 1, 1)
    with pytest.raises(ValueError, match="closed_only"):
        archiver.export(
            timeframe="1m",
            day_start=s5778_value_1,
            day_end=s5778_value_2,
            purge=True,
        )


def test_candle_audit_export_filters_exchange(tmp_path: Path) -> None:
    """Export filters by exchange.

    Given: Two instruments on different exchanges,
    When: export called with exchange=polygon,
    Then: Only polygon candles exported.
    """
    open_at = datetime(2024, 1, 1, 14, 30, tzinfo=UTC)
    ts = datetime(2024, 1, 1, 15, 0, tzinfo=UTC)
    repo = _AuditStubRepo(
        instruments={
            "inst-poly": ("BTC-USD", "polygon"),
            "inst-krak": ("BTC-USD", "kraken"),
        },
        candle_rows={
            "inst-poly": [
                _make_candle_audit_row(1, "pub-1", ts, KNOWN_TO_MAX, "inst-poly", open_at)
            ],
            "inst-krak": [
                _make_candle_audit_row(2, "pub-2", ts, KNOWN_TO_MAX, "inst-krak", open_at)
            ],
        },
    )
    archiver = CandleAuditArchiver(repo, tmp_path)
    result = archiver.export(
        exchange="polygon",
        timeframe="1m",
        day_start=date(2024, 1, 1),
        day_end=date(2024, 1, 1),
    )
    assert result.rows_exported == 1
    assert (
        tmp_path / "archive" / "candles" / "polygon" / "BTC-USD" / "2024" / "2024-01-01.csv"
    ).exists()
    assert not (tmp_path / "archive" / "candles" / "kraken").exists()


def test_candle_audit_export_filters_archive_symbol(tmp_path: Path) -> None:
    """Export filters by archive_symbol.

    Given: Two instruments with different archive_symbols,
    When: export called with archive_symbol=BTC-USD,
    Then: Only BTC-USD candles exported.
    """
    open_at = datetime(2024, 1, 1, 14, 30, tzinfo=UTC)
    ts = datetime(2024, 1, 1, 15, 0, tzinfo=UTC)
    repo = _AuditStubRepo(
        instruments={
            "inst-btc": ("BTC-USD", "polygon"),
            "inst-eth": ("ETH-USD", "polygon"),
        },
        candle_rows={
            "inst-btc": [_make_candle_audit_row(1, "pub-1", ts, KNOWN_TO_MAX, "inst-btc", open_at)],
            "inst-eth": [_make_candle_audit_row(2, "pub-2", ts, KNOWN_TO_MAX, "inst-eth", open_at)],
        },
    )
    archiver = CandleAuditArchiver(repo, tmp_path)
    result = archiver.export(
        archive_symbol="BTC-USD",
        timeframe="1m",
        day_start=date(2024, 1, 1),
        day_end=date(2024, 1, 1),
    )
    assert result.rows_exported == 1


def test_candle_audit_export_no_candles(tmp_path: Path) -> None:
    """Export with no matching candles returns zero counts.

    Given: Instrument with no candles,
    When: export is called,
    Then: Returns zero counts.
    """
    repo = _AuditStubRepo(instruments={"inst-1": ("BTC-USD", "polygon")})
    archiver = CandleAuditArchiver(repo, tmp_path)
    result = archiver.export(
        timeframe="1m",
        day_start=date(2024, 1, 1),
        day_end=date(2024, 1, 1),
    )
    assert result == ExportResult(files_written=0, rows_exported=0, rows_purged=0)


def test_candle_audit_export_multi_day(tmp_path: Path) -> None:
    """Export spanning multiple days creates per-day files by open_at.

    Given: Candles with open_at across two days,
    When: export is called with two-day range,
    Then: Creates separate CSV for each day.
    """
    ts = datetime(2024, 1, 1, 15, 0, tzinfo=UTC)
    rows = [
        _make_candle_audit_row(
            1,
            "pub-1",
            ts,
            KNOWN_TO_MAX,
            "inst-1",
            datetime(2024, 1, 1, 14, 30, tzinfo=UTC),
        ),
        _make_candle_audit_row(
            2,
            "pub-2",
            ts,
            KNOWN_TO_MAX,
            "inst-1",
            datetime(2024, 1, 2, 10, 0, tzinfo=UTC),
        ),
    ]
    repo = _AuditStubRepo(
        instruments={"inst-1": ("BTC-USD", "polygon")},
        candle_rows={"inst-1": rows},
    )
    archiver = CandleAuditArchiver(repo, tmp_path)
    result = archiver.export(
        timeframe="1m",
        day_start=date(2024, 1, 1),
        day_end=date(2024, 1, 2),
    )
    assert result.files_written == 2
    base = tmp_path / "archive" / "candles" / "polygon" / "BTC-USD" / "2024"
    assert (base / "2024-01-01.csv").exists()
    assert (base / "2024-01-02.csv").exists()


def test_candle_audit_export_merges_with_existing(tmp_path: Path) -> None:
    """Export merges new rows with existing CSV file.

    Given: Existing CSV with one row and DB with two rows (one overlapping),
    When: export is called,
    Then: Merged file has two unique rows.
    """
    csv_dir = tmp_path / "archive" / "candles" / "polygon" / "BTC-USD" / "2024"
    csv_dir.mkdir(parents=True)
    csv_path = csv_dir / "2024-01-01.csv"
    csv_path.write_text(
        ",".join(_CANDLE_AUDIT_COLUMNS) + "\n"
        "pub-1,2024-01-01T15:00:00+00:00,9999-12-31T23:59:59+00:00,s1,1,inst-1,"
        "2024-01-01T14:30:00+00:00,1m,100,110,90,105,1234,103,42\n",
        encoding="utf-8",
    )
    open_at = datetime(2024, 1, 1, 14, 30, tzinfo=UTC)
    ts = datetime(2024, 1, 1, 15, 0, tzinfo=UTC)
    rows = [
        _make_candle_audit_row(1, "pub-1", ts, KNOWN_TO_MAX, "inst-1", open_at),
        _make_candle_audit_row(
            2,
            "pub-2",
            ts.replace(minute=1),
            KNOWN_TO_MAX,
            "inst-1",
            open_at,
            close=106.0,
        ),
    ]
    repo = _AuditStubRepo(
        instruments={"inst-1": ("BTC-USD", "polygon")},
        candle_rows={"inst-1": rows},
    )
    archiver = CandleAuditArchiver(repo, tmp_path)
    archiver.export(
        timeframe="1m",
        day_start=date(2024, 1, 1),
        day_end=date(2024, 1, 1),
    )
    with csv_path.open(encoding="utf-8") as f:
        reader = csv.reader(f)
        next(reader)
        data = list(reader)
    assert len(data) == 2


def _make_db_with_candle_versions(
    tmp_path: Path,
) -> tuple[DatabaseRepository, str]:
    """Create DB with Symbol, Instrument, and Candle rows (active + closed).

    Returns:
        Tuple of (DatabaseRepository, instrument_public_id).
    """
    db_url = f"sqlite:///{tmp_path / 'audit.db'}"
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
        closed_candle = Candle(
            public_id="candle-1",
            instrument_public_id="inst-btc",
            open_at=datetime(2024, 1, 1, 14, 30, tzinfo=UTC),
            timeframe="1m",
            open=100.0,
            high=110.0,
            low=90.0,
            close=105.0,
            volume=1234.0,
            vwap=103.0,
            trades=42,
            session_id="test",
            sequence_id=3,
            timestamp=ts,
            known_to=datetime(2024, 1, 1, 15, 0, tzinfo=UTC),
        )
        session.add(closed_candle)
        session.flush()
        active_candle = Candle(
            public_id="candle-1",
            instrument_public_id="inst-btc",
            open_at=datetime(2024, 1, 1, 14, 30, tzinfo=UTC),
            timeframe="1m",
            open=100.0,
            high=112.0,
            low=90.0,
            close=107.0,
            volume=1500.0,
            vwap=104.0,
            trades=55,
            session_id="test",
            sequence_id=4,
            timestamp=datetime(2024, 1, 1, 15, 0, tzinfo=UTC),
            known_to=KNOWN_TO_MAX,
        )
        session.add(active_candle)
        session.commit()
    return repo, "inst-btc"


def test_repo_get_candle_versions_all(tmp_path: Path) -> None:
    """Repository returns all candle versions (active + closed).

    Given: DB with one closed and one active candle version,
    When: get_candle_versions_for_archive called with closed_only=False,
    Then: Returns both versions.
    """
    repo, inst_id = _make_db_with_candle_versions(tmp_path)
    rows = repo.get_candle_versions_for_archive(
        inst_id,
        "1m",
        date(2024, 1, 1),
        date(2024, 1, 1),
    )
    assert len(rows) == 2
    assert isinstance(rows[0][0], int)
    repo.dispose()


def test_repo_get_candle_versions_closed_only(tmp_path: Path) -> None:
    """Repository returns only closed candle versions.

    Given: DB with one closed and one active candle version,
    When: get_candle_versions_for_archive called with closed_only=True,
    Then: Returns only the closed version.
    """
    repo, inst_id = _make_db_with_candle_versions(tmp_path)
    rows = repo.get_candle_versions_for_archive(
        inst_id,
        "1m",
        date(2024, 1, 1),
        date(2024, 1, 1),
        closed_only=True,
    )
    assert len(rows) == 1
    known_to_val = rows[0][3]
    assert known_to_val != KNOWN_TO_MAX
    repo.dispose()


def test_repo_get_candle_versions_empty_range(tmp_path: Path) -> None:
    """Repository returns empty list for date range with no candles.

    Given: DB with candles on 2024-01-01,
    When: get_candle_versions_for_archive called for 2024-02-01,
    Then: Returns empty list.
    """
    repo, inst_id = _make_db_with_candle_versions(tmp_path)
    rows = repo.get_candle_versions_for_archive(
        inst_id,
        "1m",
        date(2024, 2, 1),
        date(2024, 2, 1),
    )
    assert rows == []
    repo.dispose()


def test_candle_audit_integration_all_versions(tmp_path: Path) -> None:
    """Full integration: export all candle versions from DB to CSV.

    Given: DB with closed + active candle versions,
    When: CandleAuditArchiver.export is called,
    Then: CSV file has both versions.
    """
    repo, _inst_id = _make_db_with_candle_versions(tmp_path)
    archiver = CandleAuditArchiver(repo, tmp_path)
    result = archiver.export(
        timeframe="1m",
        day_start=date(2024, 1, 1),
        day_end=date(2024, 1, 1),
    )
    assert result.files_written == 1
    assert result.rows_exported == 2
    csv_path = tmp_path / "archive" / "candles" / "polygon" / "BTC-USD" / "2024" / "2024-01-01.csv"
    with csv_path.open(encoding="utf-8") as f:
        reader = csv.reader(f)
        header = next(reader)
        data = list(reader)
    assert header == list(_CANDLE_AUDIT_COLUMNS)
    assert len(data) == 2
    repo.dispose()


def test_candle_audit_integration_closed_only_purge(tmp_path: Path) -> None:
    """Full integration: export and purge only closed candle versions.

    Given: DB with closed + active candle versions,
    When: export called with closed_only=True and purge=True,
    Then: Only closed version exported and deleted; active remains.
    """
    repo, inst_id = _make_db_with_candle_versions(tmp_path)
    archiver = CandleAuditArchiver(repo, tmp_path)
    result = archiver.export(
        timeframe="1m",
        day_start=date(2024, 1, 1),
        day_end=date(2024, 1, 1),
        closed_only=True,
        purge=True,
    )
    assert result.rows_exported == 1
    assert result.rows_purged == 1
    remaining = repo.get_candle_versions_for_archive(
        inst_id,
        "1m",
        date(2024, 1, 1),
        date(2024, 1, 1),
    )
    assert len(remaining) == 1
    assert remaining[0][3] == KNOWN_TO_MAX
    repo.dispose()


def test_candle_audit_integration_idempotent(tmp_path: Path) -> None:
    """Exporting twice produces same result (merge/dedup).

    Given: DB with candle versions,
    When: export called twice,
    Then: CSV file has same number of rows.
    """
    repo, _inst_id = _make_db_with_candle_versions(tmp_path)
    archiver = CandleAuditArchiver(repo, tmp_path)
    archiver.export(timeframe="1m", day_start=date(2024, 1, 1), day_end=date(2024, 1, 1))
    archiver.export(timeframe="1m", day_start=date(2024, 1, 1), day_end=date(2024, 1, 1))
    csv_path = tmp_path / "archive" / "candles" / "polygon" / "BTC-USD" / "2024" / "2024-01-01.csv"
    with csv_path.open(encoding="utf-8") as f:
        reader = csv.reader(f)
        next(reader)
        data = list(reader)
    assert len(data) == 2
    repo.dispose()


def test_cli_archive_candles_audit(tmp_path: Path) -> None:
    """CLI dispatches to CandleAuditArchiver for candles-audit.

    Given: Mocked CandleAuditArchiver,
    When: CLI invoked with --table candles-audit,
    Then: CandleAuditArchiver.export is called.
    """
    runner = CliRunner()
    mock_archiver = MagicMock()
    mock_archiver.export.return_value = ExportResult(
        files_written=1,
        rows_exported=10,
        rows_purged=0,
    )
    with (
        patch("snapper.cli.app.BootstrapSettingsLoader") as mock_bootstrap,
        patch("snapper.cli.app.DatabaseRepository") as mock_repo_cls,
        patch("snapper.cli.app.CandleAuditArchiver", return_value=mock_archiver),
    ):
        mock_bootstrap.return_value = MagicMock(db_url="sqlite:///")
        mock_repo_cls.return_value = MagicMock()
        result = runner.invoke(
            app,
            [
                "archive",
                "--table",
                "candles-audit",
                "--day",
                "2024-01-01",
                "--output-dir",
                str(tmp_path),
            ],
        )
    assert result.exit_code == 0
    assert "10 rows" in result.output
    mock_archiver.export.assert_called_once()
    call_kwargs = mock_archiver.export.call_args[1]
    assert call_kwargs["closed_only"] is False


def test_cli_archive_candles_audit_closed_only_purge(tmp_path: Path) -> None:
    """CLI passes --closed-only and --purge to CandleAuditArchiver.

    Given: Mocked CandleAuditArchiver,
    When: CLI invoked with --table candles-audit --closed-only --purge,
    Then: CandleAuditArchiver.export called with correct flags.
    """
    runner = CliRunner()
    mock_archiver = MagicMock()
    mock_archiver.export.return_value = ExportResult(
        files_written=1,
        rows_exported=5,
        rows_purged=5,
    )
    with (
        patch("snapper.cli.app.BootstrapSettingsLoader") as mock_bootstrap,
        patch("snapper.cli.app.DatabaseRepository") as mock_repo_cls,
        patch("snapper.cli.app.CandleAuditArchiver", return_value=mock_archiver),
    ):
        mock_bootstrap.return_value = MagicMock(db_url="sqlite:///")
        mock_repo_cls.return_value = MagicMock()
        result = runner.invoke(
            app,
            [
                "archive",
                "--table",
                "candles-audit",
                "--day",
                "2024-01-01",
                "--closed-only",
                "--purge",
                "--output-dir",
                str(tmp_path),
            ],
        )
    assert result.exit_code == 0
    assert "5 purged" in result.output
    call_kwargs = mock_archiver.export.call_args[1]
    assert call_kwargs["closed_only"] is True
    assert call_kwargs["purge"] is True


def test_cli_archive_candles_audit_purge_without_closed_only_rejected() -> None:
    """CLI rejects --purge without --closed-only for candles-audit.

    Given: --table candles-audit --purge (no --closed-only),
    When: CLI invoked,
    Then: Exits with error.
    """
    runner = CliRunner()
    result = runner.invoke(
        app,
        ["archive", "--table", "candles-audit", "--purge", "--day", "2024-01-01"],
    )
    assert result.exit_code == 1
    assert "closed-only" in result.output.lower()
