"""Tests for generic state-SCD2 table archiver."""

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
from snapper.data.archiver import STATE_TABLES
from snapper.data.archiver import ExportResult
from snapper.data.archiver import StateArchiver
from snapper.data.archiver import StateTableSpec
from snapper.data.archiver import _format_value
from snapper.data.archiver import _get_model_archive_columns
from snapper.data.archiver import _normalize_rows_to_columns
from snapper.data.models import KNOWN_TO_MAX
from snapper.data.models import Base
from snapper.data.models import Instrument
from snapper.data.models import MarketSnapshot
from snapper.data.models import Order
from snapper.data.models import Position
from snapper.data.models import Setting
from snapper.data.models import Symbol
from snapper.data.repository import DatabaseRepository


def test_state_tables_cover_all_14() -> None:
    """STATE_TABLES dict contains all 14 state-SCD2 tables.

    Given: STATE_TABLES configuration,
    When: Keys are inspected,
    Then: All 14 tables present.
    """
    expected = {
        "orders",
        "positions",
        "instruments",
        "instrument_specs",
        "settings",
        "symbols",
        "symbol_aliases",
        "symbol_exchange_capabilities",
        "users",
        "user_login_events",
        "market_snapshots",
        "process_runs",
        "underlying_assets",
        "instrument_underlying_mappings",
        "continuous_contract_configs",
    }
    assert set(STATE_TABLES.keys()) == expected


def test_get_model_archive_columns_temporal_first() -> None:
    """Model columns start with temporal columns in correct order.

    Given: Any model with TemporalMixin,
    When: _get_model_archive_columns is called,
    Then: First 5 columns are temporal in correct order.
    """
    columns = _get_model_archive_columns(Setting)
    assert columns[:5] == ("public_id", "timestamp", "known_to", "session_id", "sequence_id")
    assert "key" in columns
    assert "id" not in columns


def test_get_model_archive_columns_instrument() -> None:
    """Instrument model columns include symbol_public_id and exchange.

    Given: Instrument model,
    When: _get_model_archive_columns is called,
    Then: Contains symbol_public_id and exchange after temporal.
    """
    columns = _get_model_archive_columns(Instrument)
    assert columns[:5] == ("public_id", "timestamp", "known_to", "session_id", "sequence_id")
    assert "symbol_public_id" in columns
    assert "exchange" in columns


def test_format_value_json_dict() -> None:
    """Format dict value as compact JSON.

    Given: Dict value (from JSON column),
    When: _format_value is called,
    Then: Returns compact JSON string.
    """
    result = _format_value({"key": "value", "n": 1})
    assert result == '{"key":"value","n":1}'


def test_format_value_json_list() -> None:
    """Format list value as compact JSON.

    Given: List value (from JSON column),
    When: _format_value is called,
    Then: Returns compact JSON string.
    """
    result = _format_value(["a", "b"])
    assert result == '["a","b"]'


class _StateStubRepo:
    """Test stub for DatabaseRepository supporting state archiver."""

    def __init__(
        self,
        instruments: dict[str, tuple[str, str]] | None = None,
        archive_symbols: dict[str, str] | None = None,
        rows: list[tuple[Any, ...]] | None = None,
        delete_count: int = 0,
        anchor_ids: set[int] | None = None,
    ) -> None:
        self._instruments = instruments or {}
        self._archive_symbols = archive_symbols or {}
        self._rows = rows or []
        self._delete_count = delete_count
        self._anchor_ids = anchor_ids or set()
        self.deleted_ids: list[int] = []

    def get_instrument_archive_map(self) -> dict[str, tuple[str, str]]:
        return self._instruments

    def get_archive_symbols(self) -> dict[str, str]:
        return self._archive_symbols

    def get_symbol_anchor_ids(self) -> set[int]:
        return self._anchor_ids

    def get_scd2_rows_for_archive(
        self,
        model: type[Any],
        columns: tuple[str, ...],
        day_start: date,
        day_end: date,
        closed_only: bool = False,
    ) -> list[tuple[Any, ...]]:
        return self._rows

    def delete_rows_by_id(self, model: type[Any], row_ids: list[int]) -> int:
        self.deleted_ids.extend(row_ids)
        return self._delete_count or len(row_ids)


def _make_setting_row(
    row_id: int,
    public_id: str,
    ts: datetime,
    known_to: datetime,
    key: str,
) -> tuple[Any, ...]:
    """Build a setting DB row tuple (id, public_id, timestamp, known_to, session_id, sequence_id, ...domain)."""
    columns = _get_model_archive_columns(Setting)
    values: dict[str, Any] = {
        "public_id": public_id,
        "timestamp": ts,
        "known_to": known_to,
        "session_id": "test-session",
        "sequence_id": row_id,
        "key": key,
        "value": "test-val",
        "category": "general",
        "description": None,
        "is_encrypted": False,
        "updated_by": None,
    }
    return (row_id, *(values[c] for c in columns))


def test_state_archiver_export_flat_table(tmp_path: Path) -> None:
    """Export flat state table (settings) to per-day CSV.

    Given: Stub repo with two setting rows,
    When: StateArchiver.export is called,
    Then: CSV file created with correct content.
    """
    ts = datetime(2024, 1, 1, 14, 30, tzinfo=UTC)
    rows = [
        _make_setting_row(1, "pub-1", ts, KNOWN_TO_MAX, "api_key"),
        _make_setting_row(2, "pub-2", ts.replace(minute=31), KNOWN_TO_MAX, "secret"),
    ]
    repo = _StateStubRepo(rows=rows)
    archiver = StateArchiver(repo, tmp_path)
    result = archiver.export(table="settings", day_start=date(2024, 1, 1), day_end=date(2024, 1, 1))
    assert result.files_written == 1
    assert result.rows_exported == 2
    csv_path = tmp_path / "archive" / "settings" / "2024" / "2024-01-01.csv"
    assert csv_path.exists()
    with csv_path.open(encoding="utf-8") as f:
        reader = csv.reader(f)
        header = next(reader)
        data = list(reader)
    assert header[0] == "public_id"
    assert len(data) == 2


def test_state_archiver_export_dry_run(tmp_path: Path) -> None:
    """Dry run counts rows without writing files.

    Given: Stub repo with setting rows,
    When: export called with dry_run=True,
    Then: Returns counts but no files created.
    """
    ts = datetime(2024, 1, 1, 14, 30, tzinfo=UTC)
    rows = [_make_setting_row(1, "pub-1", ts, KNOWN_TO_MAX, "key1")]
    repo = _StateStubRepo(rows=rows)
    archiver = StateArchiver(repo, tmp_path)
    result = archiver.export(
        table="settings",
        day_start=date(2024, 1, 1),
        day_end=date(2024, 1, 1),
        dry_run=True,
    )
    assert result.rows_exported == 1
    assert result.files_written == 1
    assert not (tmp_path / "archive").exists()


def test_state_archiver_export_purge_closed_only(tmp_path: Path) -> None:
    """Export with purge deletes closed rows.

    Given: Stub repo with closed setting rows,
    When: export called with purge=True and closed_only=True,
    Then: delete_rows_by_id called with exported IDs.
    """
    ts = datetime(2024, 1, 1, 14, 30, tzinfo=UTC)
    closed_at = datetime(2024, 1, 1, 15, 0, tzinfo=UTC)
    rows = [
        _make_setting_row(1, "pub-1", ts, closed_at, "key1"),
        _make_setting_row(2, "pub-2", ts, closed_at, "key2"),
    ]
    repo = _StateStubRepo(rows=rows)
    archiver = StateArchiver(repo, tmp_path)
    result = archiver.export(
        table="settings",
        day_start=date(2024, 1, 1),
        day_end=date(2024, 1, 1),
        closed_only=True,
        purge=True,
    )
    assert result.rows_purged == 2
    assert repo.deleted_ids == [1, 2]


def test_state_archiver_purge_without_closed_only_raises(tmp_path: Path) -> None:
    """Purge without closed_only raises ValueError.

    Given: purge=True and closed_only=False,
    When: export is called,
    Then: Raises ValueError.
    """
    repo = _StateStubRepo()
    archiver = StateArchiver(repo, tmp_path)
    s5778_value_1 = date(2024, 1, 1)
    s5778_value_2 = date(2024, 1, 1)
    with pytest.raises(ValueError, match="closed_only"):
        archiver.export(
            table="settings",
            day_start=s5778_value_1,
            day_end=s5778_value_2,
            purge=True,
        )


def test_state_archiver_unknown_table_raises(tmp_path: Path) -> None:
    """Export raises ValueError for unknown state table.

    Given: Invalid table name,
    When: export is called,
    Then: Raises ValueError.
    """
    repo = _StateStubRepo()
    archiver = StateArchiver(repo, tmp_path)
    s5778_value_1 = date(2024, 1, 1)
    s5778_value_2 = date(2024, 1, 1)
    with pytest.raises(ValueError, match="(?i)unknown"):
        archiver.export(table="bogus", day_start=s5778_value_1, day_end=s5778_value_2)


def test_state_archiver_export_no_rows(tmp_path: Path) -> None:
    """Export with no matching rows returns zero counts.

    Given: Stub repo with no rows,
    When: export is called,
    Then: Returns zero counts.
    """
    repo = _StateStubRepo()
    archiver = StateArchiver(repo, tmp_path)
    result = archiver.export(table="settings", day_start=date(2024, 1, 1), day_end=date(2024, 1, 1))
    assert result == ExportResult(files_written=0, rows_exported=0, rows_purged=0)


def test_state_archiver_symbol_anchor_protection(tmp_path: Path) -> None:
    """Symbol purge excludes anchor rows.

    Given: Two symbol rows, one is anchor (id=1),
    When: export called with purge=True and closed_only=True,
    Then: Only non-anchor row (id=2) is purged.
    """
    ts = datetime(2024, 1, 1, 14, 30, tzinfo=UTC)
    closed_at = datetime(2024, 1, 1, 15, 0, tzinfo=UTC)
    columns = _get_model_archive_columns(Symbol)
    sym_vals: dict[str, Any] = {
        "public_id": "sym-1",
        "timestamp": ts,
        "known_to": closed_at,
        "session_id": "test",
        "sequence_id": 1,
        "native_symbol": "BTC-USD",
        "base": "BTC",
        "quote": "USD",
        "asset_type": "crypto",
        "created_at": ts,
    }
    row1 = (1, *(sym_vals[c] for c in columns))
    sym_vals2 = {**sym_vals, "public_id": "sym-1", "sequence_id": 2}
    row2 = (2, *(sym_vals2[c] for c in columns))
    repo = _StateStubRepo(rows=[row1, row2], anchor_ids={1})
    archiver = StateArchiver(repo, tmp_path)
    result = archiver.export(
        table="symbols",
        day_start=date(2024, 1, 1),
        day_end=date(2024, 1, 1),
        closed_only=True,
        purge=True,
    )
    assert result.rows_exported == 2
    assert result.rows_purged == 1
    assert repo.deleted_ids == [2]


def _make_instrument_row(
    row_id: int,
    public_id: str,
    ts: datetime,
    known_to: datetime,
    symbol_public_id: str,
    exchange: str,
) -> tuple[Any, ...]:
    """Build an instrument DB row tuple."""
    columns = _get_model_archive_columns(Instrument)
    values: dict[str, Any] = {
        "public_id": public_id,
        "timestamp": ts,
        "known_to": known_to,
        "session_id": "test",
        "sequence_id": row_id,
        "symbol_public_id": symbol_public_id,
        "exchange": exchange,
        "source_exchange": None,
        "requires_ai_review": False,
    }
    return (row_id, *(values[c] for c in columns))


def test_state_archiver_instrument_partitioned(tmp_path: Path) -> None:
    """Instrument table is partitioned by exchange/archive_symbol.

    Given: Instrument rows with symbol_public_id,
    When: StateArchiver exports instruments,
    Then: CSV files created under exchange/archive_symbol path.
    """
    ts = datetime(2024, 1, 1, 14, 30, tzinfo=UTC)
    rows = [
        _make_instrument_row(1, "inst-1", ts, KNOWN_TO_MAX, "sym-btc", "polygon"),
        _make_instrument_row(2, "inst-2", ts, KNOWN_TO_MAX, "sym-btc", "kraken"),
    ]
    repo = _StateStubRepo(
        archive_symbols={"sym-btc": "BTC-USD"},
        rows=rows,
    )
    archiver = StateArchiver(repo, tmp_path)
    result = archiver.export(
        table="instruments",
        day_start=date(2024, 1, 1),
        day_end=date(2024, 1, 1),
    )
    assert result.files_written == 2
    assert result.rows_exported == 2
    assert (
        tmp_path / "archive" / "instruments" / "polygon" / "BTC-USD" / "2024" / "2024-01-01.csv"
    ).exists()
    assert (
        tmp_path / "archive" / "instruments" / "kraken" / "BTC-USD" / "2024" / "2024-01-01.csv"
    ).exists()


def test_state_archiver_market_snapshot_partitioned(tmp_path: Path) -> None:
    """MarketSnapshot table is partitioned by instrument archive map.

    Given: MarketSnapshot rows with instrument_public_id,
    When: StateArchiver exports market_snapshots,
    Then: CSV files created under exchange/archive_symbol path.
    """
    ts = datetime(2024, 1, 1, 14, 30, tzinfo=UTC)
    columns = _get_model_archive_columns(MarketSnapshot)
    values: dict[str, Any] = {
        "public_id": "snap-1",
        "timestamp": ts,
        "known_to": KNOWN_TO_MAX,
        "session_id": "test",
        "sequence_id": 1,
        "instrument_public_id": "inst-btc",
        "bid": 100.0,
        "bid_volume": 10.0,
        "ask": 101.0,
        "ask_volume": 10.0,
        "last_price": 100.5,
        "volume_24h": 50000.0,
        "vwap_24h": 100.0,
        "low_24h": 95.0,
        "high_24h": 105.0,
        "change_24h": 1.5,
        "spread": 1.0,
        "spread_pct": 0.01,
    }
    row = (1, *(values[c] for c in columns))
    repo = _StateStubRepo(
        instruments={"inst-btc": ("BTC-USD", "polygon")},
        rows=[row],
    )
    archiver = StateArchiver(repo, tmp_path)
    result = archiver.export(
        table="market_snapshots",
        day_start=date(2024, 1, 1),
        day_end=date(2024, 1, 1),
    )
    assert result.files_written == 1
    csv_path = (
        tmp_path
        / "archive"
        / "market_snapshots"
        / "polygon"
        / "BTC-USD"
        / "2024"
        / "2024-01-01.csv"
    )
    assert csv_path.exists()


def test_state_archiver_skips_unmapped_instrument(tmp_path: Path) -> None:
    """Rows referencing unmapped instruments are skipped.

    Given: Instrument row with symbol_public_id not in archive_symbols,
    When: export is called,
    Then: Row is skipped.
    """
    ts = datetime(2024, 1, 1, 14, 30, tzinfo=UTC)
    rows = [_make_instrument_row(1, "inst-1", ts, KNOWN_TO_MAX, "sym-missing", "polygon")]
    repo = _StateStubRepo(archive_symbols={"sym-other": "OTHER"}, rows=rows)
    archiver = StateArchiver(repo, tmp_path)
    result = archiver.export(
        table="instruments",
        day_start=date(2024, 1, 1),
        day_end=date(2024, 1, 1),
    )
    assert result.rows_exported == 0


def test_state_archiver_unknown_group_column_raises(tmp_path: Path) -> None:
    """Unknown group_column in spec raises ValueError.

    Given: Custom spec with unsupported group_column,
    When: export triggers _build_group_context,
    Then: Raises ValueError.
    """
    ts = datetime(2024, 1, 1, 14, 30, tzinfo=UTC)
    columns = _get_model_archive_columns(Order)
    values: dict[str, Any] = dict.fromkeys(columns, "x")
    values.update(
        {
            "public_id": "pub-1",
            "timestamp": ts,
            "known_to": KNOWN_TO_MAX,
            "session_id": "t",
            "sequence_id": 1,
            "instrument_public_id": "inst-1",
            "bogus_col": "x",
        }
    )
    row = (1, *(values.get(c, "") for c in columns))
    repo = _StateStubRepo(rows=[row])
    archiver = StateArchiver(repo, tmp_path)
    with (
        patch.dict(
            "snapper.data.archiver.STATE_TABLES",
            {"test_bogus": StateTableSpec(model=Order, group_column="client_order_id")},
        ),
        pytest.raises(ValueError, match="group_column"),
    ):
        archiver.export(
            table="test_bogus",
            day_start=date(2024, 1, 1),
            day_end=date(2024, 1, 1),
        )


def test_state_archiver_market_snapshot_skips_unmapped(tmp_path: Path) -> None:
    """MarketSnapshot rows with unmapped instrument are skipped.

    Given: MarketSnapshot row referencing unknown instrument_public_id,
    When: export is called,
    Then: Row is skipped.
    """
    ts = datetime(2024, 1, 1, 14, 30, tzinfo=UTC)
    columns = _get_model_archive_columns(MarketSnapshot)
    values: dict[str, Any] = {
        "public_id": "snap-1",
        "timestamp": ts,
        "known_to": KNOWN_TO_MAX,
        "session_id": "test",
        "sequence_id": 1,
        "instrument_public_id": "inst-missing",
        "bid": 100.0,
        "bid_volume": None,
        "ask": 101.0,
        "ask_volume": None,
        "last_price": None,
        "volume_24h": None,
        "vwap_24h": None,
        "low_24h": None,
        "high_24h": None,
        "change_24h": None,
        "spread": None,
        "spread_pct": None,
    }
    row = (1, *(values[c] for c in columns))
    repo = _StateStubRepo(
        instruments={"inst-other": ("BTC-USD", "polygon")},
        rows=[row],
    )
    archiver = StateArchiver(repo, tmp_path)
    result = archiver.export(
        table="market_snapshots",
        day_start=date(2024, 1, 1),
        day_end=date(2024, 1, 1),
    )
    assert result.rows_exported == 0


def _make_db_with_state(tmp_path: Path) -> DatabaseRepository:
    """Create DB with Symbol and Setting rows for integration tests."""
    db_url = f"sqlite:///{tmp_path / 'state.db'}"
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
        closed_setting = Setting(
            public_id="set-1",
            key="api_key",
            value="old-val",
            category="general",
            session_id="test",
            sequence_id=2,
            timestamp=ts,
            known_to=datetime(2024, 1, 1, 12, 0, tzinfo=UTC),
        )
        session.add(closed_setting)
        session.flush()
        active_setting = Setting(
            public_id="set-1",
            key="api_key",
            value="new-val",
            category="general",
            session_id="test",
            sequence_id=3,
            timestamp=datetime(2024, 1, 1, 12, 0, tzinfo=UTC),
            known_to=KNOWN_TO_MAX,
        )
        session.add(active_setting)
        session.commit()
    return repo


def test_repo_get_scd2_rows_all(tmp_path: Path) -> None:
    """Repository returns all SCD2 versions (active + closed).

    Given: DB with one closed and one active setting,
    When: get_scd2_rows_for_archive called with closed_only=False,
    Then: Returns both versions.
    """
    repo = _make_db_with_state(tmp_path)
    columns = _get_model_archive_columns(Setting)
    rows = repo.get_scd2_rows_for_archive(Setting, columns, date(2024, 1, 1), date(2024, 1, 1))
    assert len(rows) == 2
    repo.dispose()


def test_repo_get_scd2_rows_closed_only(tmp_path: Path) -> None:
    """Repository returns only closed SCD2 versions.

    Given: DB with one closed and one active setting,
    When: get_scd2_rows_for_archive called with closed_only=True,
    Then: Returns only the closed version.
    """
    repo = _make_db_with_state(tmp_path)
    columns = _get_model_archive_columns(Setting)
    rows = repo.get_scd2_rows_for_archive(
        Setting,
        columns,
        date(2024, 1, 1),
        date(2024, 1, 1),
        closed_only=True,
    )
    assert len(rows) == 1
    known_to_idx = columns.index("known_to") + 1
    assert rows[0][known_to_idx] != KNOWN_TO_MAX
    repo.dispose()


def test_state_archiver_integration_roundtrip(tmp_path: Path) -> None:
    """Full integration: export settings from DB to CSV.

    Given: DB with closed + active settings,
    When: StateArchiver.export is called,
    Then: CSV file has both versions.
    """
    repo = _make_db_with_state(tmp_path)
    archiver = StateArchiver(repo, tmp_path)
    result = archiver.export(table="settings", day_start=date(2024, 1, 1), day_end=date(2024, 1, 1))
    assert result.files_written == 1
    assert result.rows_exported == 2
    csv_path = tmp_path / "archive" / "settings" / "2024" / "2024-01-01.csv"
    with csv_path.open(encoding="utf-8") as f:
        reader = csv.reader(f)
        header = next(reader)
        data = list(reader)
    assert header[0] == "public_id"
    assert len(data) == 2
    repo.dispose()


def test_state_archiver_integration_closed_purge(tmp_path: Path) -> None:
    """Full integration: export and purge only closed settings.

    Given: DB with closed + active settings,
    When: export called with closed_only=True and purge=True,
    Then: Only closed version purged; active remains.
    """
    repo = _make_db_with_state(tmp_path)
    archiver = StateArchiver(repo, tmp_path)
    result = archiver.export(
        table="settings",
        day_start=date(2024, 1, 1),
        day_end=date(2024, 1, 1),
        closed_only=True,
        purge=True,
    )
    assert result.rows_exported == 1
    assert result.rows_purged == 1
    columns = _get_model_archive_columns(Setting)
    remaining = repo.get_scd2_rows_for_archive(
        Setting,
        columns,
        date(2024, 1, 1),
        date(2024, 1, 1),
    )
    assert len(remaining) == 1
    repo.dispose()


def test_state_archiver_integration_idempotent(tmp_path: Path) -> None:
    """Exporting twice produces same result (merge/dedup).

    Given: DB with settings,
    When: export called twice,
    Then: CSV file has same number of rows.
    """
    repo = _make_db_with_state(tmp_path)
    archiver = StateArchiver(repo, tmp_path)
    archiver.export(table="settings", day_start=date(2024, 1, 1), day_end=date(2024, 1, 1))
    archiver.export(table="settings", day_start=date(2024, 1, 1), day_end=date(2024, 1, 1))
    csv_path = tmp_path / "archive" / "settings" / "2024" / "2024-01-01.csv"
    with csv_path.open(encoding="utf-8") as f:
        reader = csv.reader(f)
        next(reader)
        data = list(reader)
    assert len(data) == 2
    repo.dispose()


def test_cli_archive_state_table(tmp_path: Path) -> None:
    """CLI dispatches to StateArchiver for state tables.

    Given: Mocked StateArchiver,
    When: CLI invoked with --table orders,
    Then: StateArchiver.export is called.
    """
    runner = CliRunner()
    mock_archiver = MagicMock()
    mock_archiver.export.return_value = ExportResult(
        files_written=1,
        rows_exported=5,
        rows_purged=0,
    )
    with (
        patch("snapper.cli.app.BootstrapSettingsLoader") as mock_bootstrap,
        patch("snapper.cli.app.DatabaseRepository") as mock_repo_cls,
        patch("snapper.cli.app.StateArchiver", return_value=mock_archiver),
    ):
        mock_bootstrap.return_value = MagicMock(db_url="sqlite:///")
        mock_repo_cls.return_value = MagicMock()
        result = runner.invoke(
            app,
            ["archive", "--table", "orders", "--day", "2024-01-01", "--output-dir", str(tmp_path)],
        )
    assert result.exit_code == 0
    assert "5 rows" in result.output
    mock_archiver.export.assert_called_once()


def test_cli_archive_state_purge_requires_closed_only() -> None:
    """CLI rejects --purge without --closed-only for state tables.

    Given: --table orders --purge (no --closed-only),
    When: CLI invoked,
    Then: Exits with error.
    """
    runner = CliRunner()
    result = runner.invoke(
        app,
        ["archive", "--table", "orders", "--purge", "--day", "2024-01-01"],
    )
    assert result.exit_code == 1
    assert "closed-only" in result.output.lower()


def test_normalize_rows_passthrough_on_matching_or_missing_header() -> None:
    """Rows pass through untouched when no remap is needed.

    Given: rows read under the current header, or no header at all,
    When: _normalize_rows_to_columns runs,
    Then: the exact row objects are returned unchanged.
    """
    columns = ("public_id", "timestamp", "known_to", "quantity")
    rows = [("p1", "2024-01-01T00:00:00", "9999-12-31T23:59:59", "1.5")]
    assert _normalize_rows_to_columns(list(columns), rows, columns) == rows
    assert _normalize_rows_to_columns(None, rows, columns) == rows


def test_normalize_rows_pads_old_narrower_header() -> None:
    """Rows written before a schema widening gain empty-string padding.

    Given: a row exported under a header lacking the trailing
        provenance columns,
    When: _normalize_rows_to_columns remaps it onto the wider header,
    Then: known columns keep their values and the new columns are
        empty strings (restored as NULL by the CSV parsers).
    """
    old_header = ["public_id", "timestamp", "known_to", "quantity"]
    columns = ("public_id", "timestamp", "known_to", "quantity", "mark_price", "marked_at")
    rows = [("p1", "2024-01-01T00:00:00", "9999-12-31T23:59:59", "1.5")]
    normalized = _normalize_rows_to_columns(old_header, rows, columns)
    assert normalized == [("p1", "2024-01-01T00:00:00", "9999-12-31T23:59:59", "1.5", "", "")]


def test_normalize_rows_remaps_by_name_and_drops_unidentifiable_rows() -> None:
    """Remapping is by column NAME and unidentifiable fragments drop.

    Given: an old header with a different column order plus a dropped
        legacy column, and one truncated row that lost its temporal
        identity cells,
    When: _normalize_rows_to_columns runs,
    Then: values land under their named columns, the legacy column is
        dropped, missing cells become empty strings, and the row
        without a recoverable (public_id, timestamp, known_to) identity
        is discarded instead of corrupting the merge.
    """
    old_header = ["quantity", "public_id", "timestamp", "known_to", "legacy_flag"]
    columns = ("public_id", "timestamp", "known_to", "quantity", "mark_price")
    rows = [("1.5", "p1", "t1", "t2", "x"), ("2.5",)]
    normalized = _normalize_rows_to_columns(old_header, rows, columns)
    assert normalized == [("p1", "t1", "t2", "1.5", "")]


def test_normalize_rows_repairs_truncated_current_header_rows() -> None:
    """Truncated rows under the CURRENT header are padded, not passed.

    Given: a file already carrying the current header where one row was
        truncated mid-write but keeps its temporal identity and another
        fragment lost even the identity cells,
    When: _normalize_rows_to_columns runs,
    Then: the identifiable row is padded to full width and the
        unidentifiable fragment is dropped — the matching-header fast
        path never lets a short row through to the merge dedup.
    """
    columns = ("public_id", "timestamp", "known_to", "quantity", "mark_price")
    rows = [("p1", "t1", "t2", "1.5"), ("p2",)]
    normalized = _normalize_rows_to_columns(list(columns), rows, columns)
    assert normalized == [("p1", "t1", "t2", "1.5", "")]


def _make_position_row(
    row_id: int,
    public_id: str,
    ts: datetime,
    known_to: datetime,
) -> tuple[Any, ...]:
    """Build a positions DB row tuple matching the archive column order.

    Args:
        row_id: Synthetic database id.
        public_id: Row public id.
        ts: Row bus timestamp.
        known_to: SCD2 close timestamp.

    Returns:
        Row tuple shaped like get_scd2_rows_for_archive output.
    """
    columns = _get_model_archive_columns(Position)
    values: dict[str, Any] = {
        "public_id": public_id,
        "timestamp": ts,
        "known_to": known_to,
        "session_id": "test-session",
        "sequence_id": row_id,
        "instrument_public_id": "inst-1",
        "mode": "paper",
        "wallet_public_id": "wallet-1",
        "quantity": 1.5,
        "average_price": 50000.0,
        "unrealized_pnl": 25.0,
        "realized_pnl": 10.0,
        "mark_price": 50050.0,
        "marked_at": ts,
        "source_venue_event_id": 42,
    }
    return (row_id, *(values[c] for c in columns))


def test_state_export_normalizes_old_header_position_archive(tmp_path: Path) -> None:
    """Merging into a pre-provenance positions archive stays aligned.

    Given: an existing positions archive file written BEFORE the 0020
        provenance columns (narrower header, one old row),
    When: a fresh export merges a new full-width row into that file,
    Then: the rewritten file carries the current header, every data row
        has the full width, and the old row's provenance cells are
        empty strings.
    """
    columns = _get_model_archive_columns(Position)
    provenance = {"mark_price", "marked_at", "source_venue_event_id"}
    old_header = [c for c in columns if c not in provenance]
    ts = datetime(2024, 1, 1, 14, 30, tzinfo=UTC)
    old_values = {
        "public_id": "pub-old",
        "timestamp": ts.isoformat(),
        "known_to": KNOWN_TO_MAX.isoformat(),
        "session_id": "old-session",
        "sequence_id": "1",
        "instrument_public_id": "inst-1",
        "mode": "paper",
        "wallet_public_id": "wallet-1",
        "quantity": "1.0",
        "average_price": "49000.0",
        "unrealized_pnl": "5.0",
        "realized_pnl": "0.0",
    }
    csv_path = tmp_path / "archive" / "positions" / "2024" / "2024-01-01.csv"
    csv_path.parent.mkdir(parents=True)
    with csv_path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f, lineterminator="\n")
        writer.writerow(old_header)
        writer.writerow([old_values[c] for c in old_header])
    repo = _StateStubRepo(
        rows=[_make_position_row(2, "pub-new", ts.replace(minute=45), KNOWN_TO_MAX)]
    )
    archiver = StateArchiver(repo, tmp_path)
    result = archiver.export(
        table="positions", day_start=date(2024, 1, 1), day_end=date(2024, 1, 1)
    )
    assert result.files_written == 1
    with csv_path.open(encoding="utf-8", newline="") as f:
        reader = csv.reader(f)
        header = next(reader)
        data = list(reader)
    assert header == list(columns)
    assert len(data) == 2
    assert all(len(row) == len(columns) for row in data)
    by_pid = {row[0]: row for row in data}
    mark_index = list(columns).index("mark_price")
    watermark_index = list(columns).index("source_venue_event_id")
    assert by_pid["pub-old"][mark_index] == ""
    assert by_pid["pub-old"][watermark_index] == ""
    assert by_pid["pub-new"][mark_index] == "50050"
    assert by_pid["pub-new"][watermark_index] == "42"
