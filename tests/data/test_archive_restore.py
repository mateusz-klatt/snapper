"""Tests for archive restore (audit mode) and purge/reload round-trip."""

from datetime import UTC
from datetime import date
from datetime import datetime
from pathlib import Path
from unittest.mock import MagicMock
from unittest.mock import patch

import pytest
from sqlalchemy import JSON
from sqlalchemy import DateTime
from typer.testing import CliRunner

from snapper.cli.app import app
from snapper.data.archiver import EVENT_TABLES
from snapper.data.archiver import ArchiveRestorer
from snapper.data.archiver import EventArchiver
from snapper.data.archiver import RestoreResult
from snapper.data.archiver import StateArchiver
from snapper.data.archiver import _build_column_parsers
from snapper.data.archiver import _get_model_archive_columns
from snapper.data.archiver import _parse_bool_csv
from snapper.data.archiver import _parse_datetime_csv
from snapper.data.archiver import _parse_float_csv
from snapper.data.archiver import _parse_int_csv
from snapper.data.archiver import _parse_json_csv
from snapper.data.archiver import _parse_str_csv
from snapper.data.archiver import _parser_for_column
from snapper.data.models import KNOWN_TO_MAX
from snapper.data.models import Base
from snapper.data.models import Instrument
from snapper.data.models import Setting
from snapper.data.models import Symbol
from snapper.data.models import Tick
from snapper.data.models import TZDateTime
from snapper.data.models import UUIDColumn
from snapper.data.repository import DatabaseRepository


def test_parse_str_csv() -> None:
    """Parse string CSV value."""
    assert _parse_str_csv("hello") == "hello"
    assert _parse_str_csv("") is None


def test_parse_datetime_csv() -> None:
    """Parse ISO datetime CSV value."""
    result = _parse_datetime_csv("2024-01-01T14:30:00+00:00")
    assert result == datetime(2024, 1, 1, 14, 30, tzinfo=UTC)
    assert _parse_datetime_csv("") is None


def test_parse_float_csv() -> None:
    """Parse float CSV value."""
    assert _parse_float_csv("1.5") == 1.5
    assert _parse_float_csv("") is None


def test_parse_int_csv() -> None:
    """Parse integer CSV value."""
    assert _parse_int_csv("42") == 42
    assert _parse_int_csv("") is None


def test_parse_bool_csv() -> None:
    """Parse boolean CSV value."""
    assert _parse_bool_csv("True") is True
    assert _parse_bool_csv("False") is False
    assert _parse_bool_csv("") is None


def test_parse_json_csv() -> None:
    """Parse JSON CSV value."""
    assert _parse_json_csv('{"key":"val"}') == {"key": "val"}
    assert _parse_json_csv("") is None


def test_parser_for_column_tzdatetime() -> None:
    """TZDateTime column maps to datetime parser.

    Given: TZDateTime column type,
    When: _parser_for_column is called,
    Then: Returns datetime parser.
    """
    assert _parser_for_column(TZDateTime()) is _parse_datetime_csv


def test_parser_for_column_json() -> None:
    """JSON column maps to JSON parser.

    Given: JSON column type,
    When: _parser_for_column is called,
    Then: Returns JSON parser.
    """
    assert _parser_for_column(JSON()) is _parse_json_csv


def test_parser_for_column_plain_datetime() -> None:
    """Plain DateTime column maps to datetime parser.

    Given: DateTime column type (not wrapped in TypeDecorator),
    When: _parser_for_column is called,
    Then: Returns datetime parser.
    """
    assert _parser_for_column(DateTime()) is _parse_datetime_csv


def test_parser_for_column_uuid() -> None:
    """UUIDColumn maps to string parser.

    Given: UUIDColumn type,
    When: _parser_for_column is called,
    Then: Returns string parser.
    """
    assert _parser_for_column(UUIDColumn()) is _parse_str_csv


def test_build_column_parsers_tick() -> None:
    """Build parsers for Tick model from header.

    Given: Tick model and its archive column header,
    When: _build_column_parsers is called,
    Then: Returns parser list matching column types.
    """
    header = list(_get_model_archive_columns(Tick))
    parsers = _build_column_parsers(Tick, header)
    assert len(parsers) == len(header)
    assert parsers[header.index("public_id")] is _parse_str_csv
    assert parsers[header.index("timestamp")] is _parse_datetime_csv
    assert parsers[header.index("bid")] is _parse_float_csv
    assert parsers[header.index("sequence_id")] is _parse_int_csv


def test_restore_unknown_table() -> None:
    """Restore raises ValueError for unknown table.

    Given: Invalid table name,
    When: restore is called,
    Then: Raises ValueError.
    """
    repo = MagicMock()
    restorer = ArchiveRestorer(repo)
    with pytest.raises(ValueError, match="Unknown table"):
        restorer.restore(table="bogus", paths=[])


def test_restore_unsupported_source() -> None:
    """Restore raises ValueError for unsupported source.

    Given: source='cache',
    When: restore is called with a valid table,
    Then: Raises ValueError.
    """
    repo = MagicMock()
    restorer = ArchiveRestorer(repo)
    with pytest.raises(ValueError, match="not yet supported"):
        restorer.restore(table="ticks", paths=[], source="cache")


def _make_db_with_tick(tmp_path: Path) -> tuple[DatabaseRepository, Path]:
    """Create DB with Symbol + Instrument + Tick, export to CSV, return (repo, csv_dir).

    The tick is exported using EventArchiver so the CSV matches the real format.
    """
    db_url = f"sqlite:///{tmp_path / 'restore.db'}"
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
        tick = Tick(
            public_id="tick-1",
            instrument_public_id="inst-btc",
            bid=100.5,
            ask=101.5,
            last=100.5,
            volume=1000.0,
            session_id="test",
            sequence_id=3,
            timestamp=datetime(2024, 1, 1, 14, 30, tzinfo=UTC),
            known_to=KNOWN_TO_MAX,
        )
        session.add(tick)
        session.commit()

    csv_dir = tmp_path / "export"
    archiver = EventArchiver(repo, csv_dir)
    archiver.export(table="ticks", day_start=date(2024, 1, 1), day_end=date(2024, 1, 1))
    return repo, csv_dir


def test_restore_audit_inserts_rows(tmp_path: Path) -> None:
    """Audit restore inserts rows from CSV into empty table.

    Given: DB with tick exported to CSV, then purged,
    When: ArchiveRestorer.restore called on the CSV,
    Then: Tick row re-inserted into DB.
    """
    repo, csv_dir = _make_db_with_tick(tmp_path)
    csv_files = sorted(csv_dir.rglob("*.csv"))
    assert len(csv_files) == 1

    spec = EVENT_TABLES["ticks"]
    rows_before = repo.get_event_rows_for_archive(
        spec.model,
        spec.columns,
        date(2024, 1, 1),
        date(2024, 1, 1),
    )
    repo.delete_rows_by_id(Tick, [r[0] for r in rows_before])
    rows_after_purge = repo.get_event_rows_for_archive(
        spec.model,
        spec.columns,
        date(2024, 1, 1),
        date(2024, 1, 1),
    )
    assert len(rows_after_purge) == 0

    restorer = ArchiveRestorer(repo)
    result = restorer.restore(table="ticks", paths=csv_files)
    assert result.rows_inserted == 1
    assert result.rows_skipped == 0

    rows_restored = repo.get_event_rows_for_archive(
        spec.model,
        spec.columns,
        date(2024, 1, 1),
        date(2024, 1, 1),
    )
    assert len(rows_restored) == 1
    assert rows_restored[0][1] == "tick-1"
    repo.dispose()


def test_restore_audit_dedup_skips_existing(tmp_path: Path) -> None:
    """Audit restore skips rows that already exist in DB.

    Given: DB with tick still present,
    When: ArchiveRestorer.restore called on the same CSV,
    Then: Zero rows inserted (all skipped).
    """
    repo, csv_dir = _make_db_with_tick(tmp_path)
    csv_files = sorted(csv_dir.rglob("*.csv"))

    restorer = ArchiveRestorer(repo)
    result = restorer.restore(table="ticks", paths=csv_files)
    assert result.rows_inserted == 0
    assert result.rows_skipped == 1
    repo.dispose()


def test_restore_audit_empty_file(tmp_path: Path) -> None:
    """Restore handles empty CSV file gracefully.

    Given: CSV file with only header,
    When: restore is called,
    Then: Returns zero counts.
    """
    csv_path = tmp_path / "empty.csv"
    csv_path.write_text("public_id,timestamp,known_to\n", encoding="utf-8")
    repo = MagicMock()
    restorer = ArchiveRestorer(repo)
    result = restorer.restore(table="ticks", paths=[csv_path])
    assert result.rows_inserted == 0
    assert result.files_processed == 1


def test_restore_audit_no_header(tmp_path: Path) -> None:
    """Restore handles completely empty CSV file.

    Given: Empty file with no content,
    When: restore is called,
    Then: Returns zero counts.
    """
    csv_path = tmp_path / "empty.csv"
    csv_path.write_text("", encoding="utf-8")
    repo = MagicMock()
    restorer = ArchiveRestorer(repo)
    result = restorer.restore(table="ticks", paths=[csv_path])
    assert result.rows_inserted == 0
    assert result.files_processed == 1


def _make_db_with_setting(tmp_path: Path) -> tuple[DatabaseRepository, Path]:
    """Create DB with closed + active Setting, export, return (repo, csv_dir)."""
    db_url = f"sqlite:///{tmp_path / 'restore_state.db'}"
    repo = DatabaseRepository(db_url)
    Base.metadata.create_all(repo.engine)
    ts = datetime(2024, 1, 1, tzinfo=UTC)
    with repo.get_session() as session:
        closed = Setting(
            public_id="set-1",
            key="api_key",
            value="old",
            category="general",
            session_id="test",
            sequence_id=1,
            timestamp=ts,
            known_to=datetime(2024, 1, 1, 12, 0, tzinfo=UTC),
        )
        session.add(closed)
        session.flush()
        active = Setting(
            public_id="set-1",
            key="api_key",
            value="new",
            category="general",
            session_id="test",
            sequence_id=2,
            timestamp=datetime(2024, 1, 1, 12, 0, tzinfo=UTC),
            known_to=KNOWN_TO_MAX,
        )
        session.add(active)
        session.commit()

    csv_dir = tmp_path / "export"
    state_archiver = StateArchiver(repo, csv_dir)
    state_archiver.export(table="settings", day_start=date(2024, 1, 1), day_end=date(2024, 1, 1))
    return repo, csv_dir


def test_round_trip_purge_restore_state(tmp_path: Path) -> None:
    """Round-trip: export state -> purge -> restore -> verify identical.

    Given: DB with closed + active setting versions,
    When: export all -> purge closed -> restore from CSV,
    Then: All rows present again.
    """
    repo, csv_dir = _make_db_with_setting(tmp_path)
    columns = _get_model_archive_columns(Setting)

    rows_before = repo.get_scd2_rows_for_archive(
        Setting,
        columns,
        date(2024, 1, 1),
        date(2024, 1, 1),
    )
    assert len(rows_before) == 2

    state_archiver = StateArchiver(repo, csv_dir)
    state_archiver.export(
        table="settings",
        day_start=date(2024, 1, 1),
        day_end=date(2024, 1, 1),
        closed_only=True,
        purge=True,
    )
    rows_after_purge = repo.get_scd2_rows_for_archive(
        Setting,
        columns,
        date(2024, 1, 1),
        date(2024, 1, 1),
    )
    assert len(rows_after_purge) == 1

    csv_files = sorted(csv_dir.rglob("*.csv"))
    restorer = ArchiveRestorer(repo)
    result = restorer.restore(table="settings", paths=csv_files)
    assert result.rows_inserted == 1

    rows_after_restore = repo.get_scd2_rows_for_archive(
        Setting,
        columns,
        date(2024, 1, 1),
        date(2024, 1, 1),
    )
    assert len(rows_after_restore) == 2
    repo.dispose()


def test_round_trip_purge_restore_events(tmp_path: Path) -> None:
    """Round-trip: export ticks -> purge -> restore -> verify identical.

    Given: DB with ticks,
    When: export -> purge -> restore,
    Then: Same data present.
    """
    repo, csv_dir = _make_db_with_tick(tmp_path)
    spec = EVENT_TABLES["ticks"]

    rows_before = repo.get_event_rows_for_archive(
        spec.model,
        spec.columns,
        date(2024, 1, 1),
        date(2024, 1, 1),
    )
    assert len(rows_before) == 1

    event_archiver = EventArchiver(repo, csv_dir)
    event_archiver.export(
        table="ticks",
        day_start=date(2024, 1, 1),
        day_end=date(2024, 1, 1),
        purge=True,
    )
    rows_after_purge = repo.get_event_rows_for_archive(
        spec.model,
        spec.columns,
        date(2024, 1, 1),
        date(2024, 1, 1),
    )
    assert len(rows_after_purge) == 0

    csv_files = sorted(csv_dir.rglob("*.csv"))
    restorer = ArchiveRestorer(repo)
    result = restorer.restore(table="ticks", paths=csv_files)
    assert result.rows_inserted == 1

    rows_restored = repo.get_event_rows_for_archive(
        spec.model,
        spec.columns,
        date(2024, 1, 1),
        date(2024, 1, 1),
    )
    assert len(rows_restored) == 1
    assert rows_restored[0][1] == rows_before[0][1]
    repo.dispose()


def test_cli_restore(tmp_path: Path) -> None:
    """CLI restore command dispatches to ArchiveRestorer.

    Given: Mocked ArchiveRestorer,
    When: CLI invoked with --table ticks --file path,
    Then: ArchiveRestorer.restore is called.
    """
    csv_path = tmp_path / "test.csv"
    csv_path.write_text(
        "public_id,timestamp,known_to\npub-1,2024-01-01T00:00:00+00:00,9999-12-31T23:59:59+00:00\n"
    )
    runner = CliRunner()
    mock_restorer = MagicMock()
    mock_restorer.restore.return_value = RestoreResult(
        files_processed=1,
        rows_inserted=5,
        rows_skipped=0,
    )
    with (
        patch("snapper.cli.app.BootstrapSettingsLoader") as mock_bootstrap,
        patch("snapper.cli.app.DatabaseRepository") as mock_repo_cls,
        patch("snapper.cli.app.ArchiveRestorer", return_value=mock_restorer),
    ):
        mock_bootstrap.return_value = MagicMock(db_url="sqlite:///")
        mock_repo_cls.return_value = MagicMock()
        result = runner.invoke(
            app,
            ["restore", "--table", "ticks", "--file", str(csv_path)],
        )
    assert result.exit_code == 0
    assert "5 inserted" in result.output
    mock_restorer.restore.assert_called_once()


def test_cli_restore_missing_args() -> None:
    """CLI restore fails without --file or --dir.

    Given: No --file or --dir,
    When: CLI invoked,
    Then: Exits with error.
    """
    runner = CliRunner()
    result = runner.invoke(app, ["restore", "--table", "ticks"])
    assert result.exit_code == 1
    assert "provide --file" in result.output


def test_cli_restore_file_not_found(tmp_path: Path) -> None:
    """CLI restore fails when file not found.

    Given: --file pointing to nonexistent path,
    When: CLI invoked,
    Then: Exits with error.
    """
    runner = CliRunner()
    result = runner.invoke(
        app,
        ["restore", "--table", "ticks", "--file", str(tmp_path / "missing.csv")],
    )
    assert result.exit_code == 1
    assert "not found" in result.output


def test_cli_restore_dir_not_found(tmp_path: Path) -> None:
    """CLI restore fails when directory not found.

    Given: --dir pointing to nonexistent path,
    When: CLI invoked,
    Then: Exits with error.
    """
    runner = CliRunner()
    result = runner.invoke(
        app,
        ["restore", "--table", "ticks", "--dir", str(tmp_path / "missing_dir")],
    )
    assert result.exit_code == 1
    assert "not found" in result.output


def test_cli_restore_dir_no_csv(tmp_path: Path) -> None:
    """CLI restore fails when directory has no CSV files.

    Given: --dir pointing to empty directory,
    When: CLI invoked,
    Then: Exits with error.
    """
    empty_dir = tmp_path / "empty"
    empty_dir.mkdir()
    runner = CliRunner()
    result = runner.invoke(
        app,
        ["restore", "--table", "ticks", "--dir", str(empty_dir)],
    )
    assert result.exit_code == 1
    assert "No CSV" in result.output


def test_cli_restore_dir(tmp_path: Path) -> None:
    """CLI restore finds CSV files recursively from --dir.

    Given: Directory with CSV files,
    When: CLI invoked with --dir,
    Then: All CSV files passed to restorer.
    """
    sub = tmp_path / "archive" / "ticks" / "2024"
    sub.mkdir(parents=True)
    (sub / "2024-01-01.csv").write_text("public_id,timestamp,known_to\n")
    (sub / "2024-01-02.csv").write_text("public_id,timestamp,known_to\n")
    runner = CliRunner()
    mock_restorer = MagicMock()
    mock_restorer.restore.return_value = RestoreResult(
        files_processed=2,
        rows_inserted=0,
        rows_skipped=0,
    )
    with (
        patch("snapper.cli.app.BootstrapSettingsLoader") as mock_bootstrap,
        patch("snapper.cli.app.DatabaseRepository") as mock_repo_cls,
        patch("snapper.cli.app.ArchiveRestorer", return_value=mock_restorer),
    ):
        mock_bootstrap.return_value = MagicMock(db_url="sqlite:///")
        mock_repo_cls.return_value = MagicMock()
        result = runner.invoke(
            app,
            ["restore", "--table", "ticks", "--dir", str(tmp_path / "archive")],
        )
    assert result.exit_code == 0
    call_kwargs = mock_restorer.restore.call_args[1]
    assert len(call_kwargs["paths"]) == 2
