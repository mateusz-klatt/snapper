"""Tests for append-only event table archiver."""

import csv
from datetime import UTC
from datetime import date
from datetime import datetime
from datetime import timedelta
from pathlib import Path
from typing import Any
from typing import cast
from unittest.mock import MagicMock
from unittest.mock import patch

import pytest
from sqlalchemy import Table
from sqlalchemy import func
from sqlalchemy import select
from typer.testing import CliRunner

from snapper.cli.app import app
from snapper.data.archiver import EVENT_TABLES
from snapper.data.archiver import EventArchiver
from snapper.data.archiver import ExecutionPurgeUnsupportedError
from snapper.data.archiver import ExportResult
from snapper.data.archiver import _format_value
from snapper.data.archiver import _merge_and_dedup_events
from snapper.data.archiver import _resolve_event_path
from snapper.data.archiver import _write_event_csv
from snapper.data.models import KNOWN_TO_MAX
from snapper.data.models import Base
from snapper.data.models import Execution
from snapper.data.models import Instrument
from snapper.data.models import Order
from snapper.data.models import PortfolioSpotReconciliationAnchor
from snapper.data.models import Symbol
from snapper.data.models import Tick
from snapper.data.repository import DatabaseRepository
from snapper.data.repository import ExecutionPhysicalMutationError


def test_format_value_none() -> None:
    """Format None as empty string.

    Given: None value,
    When: _format_value is called,
    Then: Returns empty string.
    """
    assert _format_value(None) == ""


def test_format_value_datetime() -> None:
    """Format datetime as ISO string.

    Given: UTC datetime,
    When: _format_value is called,
    Then: Returns ISO format string.
    """
    dt = datetime(2024, 1, 1, 14, 30, tzinfo=UTC)
    assert _format_value(dt) == "2024-01-01T14:30:00+00:00"


def test_format_value_float() -> None:
    """Format float using decimal formatting.

    Given: Float value,
    When: _format_value is called,
    Then: Returns full-precision string without scientific notation.
    """
    assert _format_value(1.2345) == "1.2345"
    assert _format_value(0.0) == "0"


def test_format_value_int() -> None:
    """Format integer as string.

    Given: Integer value,
    When: _format_value is called,
    Then: Returns string representation.
    """
    assert _format_value(42) == "42"


def test_format_value_string() -> None:
    """Format string as-is.

    Given: String value,
    When: _format_value is called,
    Then: Returns the same string.
    """
    assert _format_value("hello") == "hello"


def test_merge_and_dedup_events_removes_duplicates() -> None:
    """Merge event rows and deduplicate by key.

    Given: Existing and new rows with overlapping keys,
    When: _merge_and_dedup_events is called,
    Then: Returns deduplicated rows sorted by timestamp.
    """
    existing = [
        (
            "pub-1",
            "2024-01-01T14:30:00+00:00",
            "9999-12-31T23:59:59+00:00",
            "s1",
            "1",
            "inst-1",
            "100",
        )
    ]
    new = [
        (
            "pub-1",
            "2024-01-01T14:30:00+00:00",
            "9999-12-31T23:59:59+00:00",
            "s1",
            "1",
            "inst-1",
            "100",
        ),
        (
            "pub-2",
            "2024-01-01T14:31:00+00:00",
            "9999-12-31T23:59:59+00:00",
            "s1",
            "2",
            "inst-1",
            "200",
        ),
    ]
    result = _merge_and_dedup_events(existing, new)
    assert len(result) == 2


def test_merge_and_dedup_events_new_wins() -> None:
    """New rows take priority on key collision.

    Given: Existing and new rows with same key but different values,
    When: _merge_and_dedup_events is called,
    Then: New row value is kept.
    """
    existing = [
        ("pub-1", "2024-01-01T14:30:00+00:00", "9999-12-31T23:59:59+00:00", "s1", "1", "old-value")
    ]
    new = [
        ("pub-1", "2024-01-01T14:30:00+00:00", "9999-12-31T23:59:59+00:00", "s1", "1", "new-value")
    ]
    result = _merge_and_dedup_events(existing, new)
    assert len(result) == 1
    assert result[0][-1] == "new-value"


def test_merge_and_dedup_events_sorts_by_timestamp() -> None:
    """Merged result is sorted by timestamp.

    Given: Rows in reverse order,
    When: _merge_and_dedup_events is called,
    Then: Returns rows sorted ascending by timestamp.
    """
    rows = [
        ("pub-2", "2024-01-01T14:31:00+00:00", "9999-12-31T23:59:59+00:00", "s1", "2"),
        ("pub-1", "2024-01-01T14:30:00+00:00", "9999-12-31T23:59:59+00:00", "s1", "1"),
    ]
    result = _merge_and_dedup_events([], rows)
    assert result[0][0] == "pub-1"
    assert result[1][0] == "pub-2"


def test_write_event_csv_creates_file(tmp_path: Path) -> None:
    """Write event rows to CSV with header.

    Given: List of formatted row tuples,
    When: _write_event_csv is called,
    Then: Creates CSV file with correct header and data.
    """
    path = tmp_path / "subdir" / "test.csv"
    header = ("public_id", "timestamp", "known_to", "value")
    rows = [("pub-1", "2024-01-01T14:30:00+00:00", "9999-12-31T23:59:59+00:00", "42")]
    _write_event_csv(path, header, rows)
    assert path.exists()
    with path.open(encoding="utf-8") as f:
        lines = f.readlines()
    assert lines[0].strip() == "public_id,timestamp,known_to,value"
    assert lines[1].strip() == "pub-1,2024-01-01T14:30:00+00:00,9999-12-31T23:59:59+00:00,42"


def test_write_event_csv_quotes_payload_with_commas(tmp_path: Path) -> None:
    """CSV writer quotes fields containing commas.

    Given: Event row with comma in payload,
    When: _write_event_csv is called,
    Then: Payload field is quoted in output.
    """
    path = tmp_path / "test.csv"
    header = ("public_id", "timestamp", "known_to", "payload")
    rows = [
        ("pub-1", "2024-01-01T14:30:00+00:00", "9999-12-31T23:59:59+00:00", '{"key": "val,ue"}')
    ]
    _write_event_csv(path, header, rows)
    with path.open(encoding="utf-8") as f:
        reader = csv.reader(f)
        next(reader)
        data = list(reader)
    assert data[0][3] == '{"key": "val,ue"}'


def test_resolve_event_path_instrument_bound() -> None:
    """Resolve event path for instrument-bound table.

    Given: Table with exchange and archive_symbol,
    When: _resolve_event_path is called,
    Then: Returns path with exchange/symbol partitioning.
    """
    path = _resolve_event_path(
        Path("data"),
        "ticks",
        date(2024, 3, 15),
        "polygon",
        "BTC-USD",
    )
    assert path.as_posix() == "data/archive/ticks/polygon/BTC-USD/2024/2024-03-15.csv"


def test_resolve_event_path_flat() -> None:
    """Resolve event path for flat table.

    Given: Table without exchange/symbol,
    When: _resolve_event_path is called,
    Then: Returns path without partitioning.
    """
    path = _resolve_event_path(Path("data"), "telemetry", date(2024, 3, 15))
    assert path.as_posix() == "data/archive/telemetry/2024/2024-03-15.csv"


def test_event_table_specs_cover_all_tables() -> None:
    """EVENT_TABLES dict contains all six append-only tables.

    Given: EVENT_TABLES configuration,
    When: Keys are inspected,
    Then: All six event tables are present.
    """
    expected = {"ticks", "trades", "signals", "executions", "telemetry", "control"}
    assert set(EVENT_TABLES.keys()) == expected


def test_event_table_specs_columns_start_with_temporal() -> None:
    """All event table specs start with temporal columns.

    Given: Each event table spec,
    When: Columns are inspected,
    Then: First five columns are the temporal columns.
    """
    temporal = ("public_id", "timestamp", "known_to", "session_id", "sequence_id")
    for name, spec in EVENT_TABLES.items():
        assert spec.columns[:5] == temporal, f"{name} missing temporal columns"


class _EventStubRepo:
    """Test stub for DatabaseRepository supporting event archiver."""

    def __init__(
        self,
        instruments: dict[str, tuple[str, str]] | None = None,
        orders: dict[str, tuple[str, str]] | None = None,
        rows: list[tuple[Any, ...]] | None = None,
        delete_count: int = 0,
    ) -> None:
        self._instruments = instruments or {}
        self._orders = orders or {}
        self._rows = rows or []
        self._delete_count = delete_count
        self.deleted_ids: list[int] = []
        self.read_calls: int = 0

    def get_instrument_archive_map(self) -> dict[str, tuple[str, str]]:
        return self._instruments

    def get_order_archive_map(self) -> dict[str, tuple[str, str]]:
        return self._orders

    def get_event_rows_for_archive(
        self,
        model: type[Any],
        columns: tuple[str, ...],
        day_start: date,
        day_end: date,
    ) -> list[tuple[Any, ...]]:
        self.read_calls += 1
        return self._rows

    def delete_rows_by_id(self, model: type[Any], row_ids: list[int]) -> int:
        self.deleted_ids.extend(row_ids)
        return self._delete_count or len(row_ids)


def _make_tick_row(
    row_id: int,
    public_id: str,
    ts: datetime,
    instrument_public_id: str,
    bid: float,
) -> tuple[Any, ...]:
    """Build a tick DB row tuple (id, *columns)."""
    return (
        row_id,
        public_id,
        ts,
        KNOWN_TO_MAX,
        "test-session",
        row_id,
        instrument_public_id,
        bid,
        bid + 1.0,
        bid,
        1000.0,
    )


def test_event_archiver_export_ticks(tmp_path: Path) -> None:
    """Export tick rows to per-day CSV files.

    Given: Stub repo with two ticks for one instrument,
    When: EventArchiver.export is called for ticks,
    Then: CSV file created with correct content.
    """
    ts = datetime(2024, 1, 1, 14, 30, tzinfo=UTC)
    rows = [
        _make_tick_row(1, "pub-1", ts, "inst-1", 100.5),
        _make_tick_row(2, "pub-2", ts.replace(minute=31), "inst-1", 101.0),
    ]
    repo = _EventStubRepo(
        instruments={"inst-1": ("BTC-USD", "polygon")},
        rows=rows,
    )
    archiver = EventArchiver(repo, tmp_path)
    result = archiver.export(
        table="ticks",
        day_start=date(2024, 1, 1),
        day_end=date(2024, 1, 1),
    )
    assert result.files_written == 1
    assert result.rows_exported == 2
    assert result.rows_purged == 0
    csv_path = tmp_path / "archive" / "ticks" / "polygon" / "BTC-USD" / "2024" / "2024-01-01.csv"
    assert csv_path.exists()
    with csv_path.open(encoding="utf-8") as f:
        reader = csv.reader(f)
        header = next(reader)
        data = list(reader)
    assert header[0] == "public_id"
    assert header[5] == "instrument_public_id"
    assert len(data) == 2
    assert data[0][0] == "pub-1"


def test_event_archiver_export_dry_run(tmp_path: Path) -> None:
    """Dry run counts rows without writing files.

    Given: Stub repo with tick rows,
    When: export called with dry_run=True,
    Then: Returns counts but no files created.
    """
    ts = datetime(2024, 1, 1, 14, 30, tzinfo=UTC)
    rows = [_make_tick_row(1, "pub-1", ts, "inst-1", 100.5)]
    repo = _EventStubRepo(
        instruments={"inst-1": ("BTC-USD", "polygon")},
        rows=rows,
    )
    archiver = EventArchiver(repo, tmp_path)
    result = archiver.export(
        table="ticks",
        day_start=date(2024, 1, 1),
        day_end=date(2024, 1, 1),
        dry_run=True,
    )
    assert result.rows_exported == 1
    assert result.files_written == 1
    assert not (tmp_path / "archive").exists()


def test_event_archiver_export_with_purge(tmp_path: Path) -> None:
    """Export with purge deletes exported rows from DB.

    Given: Stub repo with tick rows,
    When: export called with purge=True,
    Then: delete_rows_by_id called with exported IDs.
    """
    ts = datetime(2024, 1, 1, 14, 30, tzinfo=UTC)
    rows = [
        _make_tick_row(1, "pub-1", ts, "inst-1", 100.5),
        _make_tick_row(2, "pub-2", ts.replace(minute=31), "inst-1", 101.0),
    ]
    repo = _EventStubRepo(
        instruments={"inst-1": ("BTC-USD", "polygon")},
        rows=rows,
    )
    archiver = EventArchiver(repo, tmp_path)
    result = archiver.export(
        table="ticks",
        day_start=date(2024, 1, 1),
        day_end=date(2024, 1, 1),
        purge=True,
    )
    assert result.rows_purged == 2
    assert repo.deleted_ids == [1, 2]


def test_event_archiver_export_filters_exchange(tmp_path: Path) -> None:
    """Export filters by exchange.

    Given: Two instruments on different exchanges,
    When: export called with exchange=polygon,
    Then: Only polygon ticks exported.
    """
    ts = datetime(2024, 1, 1, 14, 30, tzinfo=UTC)
    rows = [
        _make_tick_row(1, "pub-1", ts, "inst-poly", 100.5),
        _make_tick_row(2, "pub-2", ts.replace(minute=31), "inst-krak", 200.0),
    ]
    repo = _EventStubRepo(
        instruments={
            "inst-poly": ("BTC-USD", "polygon"),
            "inst-krak": ("BTC-USD", "kraken"),
        },
        rows=rows,
    )
    archiver = EventArchiver(repo, tmp_path)
    result = archiver.export(
        table="ticks",
        exchange="polygon",
        day_start=date(2024, 1, 1),
        day_end=date(2024, 1, 1),
    )
    assert result.rows_exported == 1


def test_event_archiver_export_filters_archive_symbol(tmp_path: Path) -> None:
    """Export filters by archive_symbol.

    Given: Two instruments with different archive_symbols,
    When: export called with archive_symbol=BTC-USD,
    Then: Only BTC-USD ticks exported.
    """
    ts = datetime(2024, 1, 1, 14, 30, tzinfo=UTC)
    rows = [
        _make_tick_row(1, "pub-1", ts, "inst-btc", 100.5),
        _make_tick_row(2, "pub-2", ts.replace(minute=31), "inst-eth", 200.0),
    ]
    repo = _EventStubRepo(
        instruments={
            "inst-btc": ("BTC-USD", "polygon"),
            "inst-eth": ("ETH-USD", "polygon"),
        },
        rows=rows,
    )
    archiver = EventArchiver(repo, tmp_path)
    result = archiver.export(
        table="ticks",
        archive_symbol="BTC-USD",
        day_start=date(2024, 1, 1),
        day_end=date(2024, 1, 1),
    )
    assert result.rows_exported == 1


def test_event_archiver_export_no_rows(tmp_path: Path) -> None:
    """Export with no matching rows returns zero counts.

    Given: Stub repo with no rows,
    When: export is called,
    Then: Returns zero counts.
    """
    repo = _EventStubRepo(instruments={"inst-1": ("BTC-USD", "polygon")})
    archiver = EventArchiver(repo, tmp_path)
    result = archiver.export(
        table="ticks",
        day_start=date(2024, 1, 1),
        day_end=date(2024, 1, 1),
    )
    assert result == ExportResult(files_written=0, rows_exported=0, rows_purged=0)


def test_event_archiver_export_unknown_table(tmp_path: Path) -> None:
    """Export raises ValueError for unknown table.

    Given: Invalid table name,
    When: export is called,
    Then: Raises ValueError.
    """
    repo = _EventStubRepo()
    archiver = EventArchiver(repo, tmp_path)
    with pytest.raises(ValueError, match="(?i)unknown"):
        archiver.export(
            table="unknown",
            day_start=date(2024, 1, 1),
            day_end=date(2024, 1, 1),
        )


def test_event_archiver_export_skips_unmapped_instrument(tmp_path: Path) -> None:
    """Rows referencing unmapped instruments are skipped.

    Given: Tick referencing an instrument not in the archive map,
    When: export is called,
    Then: Row is skipped, zero exported.
    """
    ts = datetime(2024, 1, 1, 14, 30, tzinfo=UTC)
    rows = [_make_tick_row(1, "pub-1", ts, "inst-missing", 100.5)]
    repo = _EventStubRepo(
        instruments={"inst-other": ("BTC-USD", "polygon")},
        rows=rows,
    )
    archiver = EventArchiver(repo, tmp_path)
    result = archiver.export(
        table="ticks",
        day_start=date(2024, 1, 1),
        day_end=date(2024, 1, 1),
    )
    assert result.rows_exported == 0


def _make_control_row(
    row_id: int,
    public_id: str,
    ts: datetime,
    message_type: str,
) -> tuple[Any, ...]:
    """Build a control DB row tuple (id, *columns)."""
    return (
        row_id,
        public_id,
        ts,
        KNOWN_TO_MAX,
        "test-session",
        row_id,
        "zmq",
        "inbound",
        message_type,
        "ok",
        None,
        None,
        None,
        None,
    )


def test_event_archiver_export_flat_table(tmp_path: Path) -> None:
    """Export flat table (control) without instrument partitioning.

    Given: Stub repo with control rows,
    When: EventArchiver.export is called for control,
    Then: CSV file created in flat archive layout.
    """
    ts = datetime(2024, 1, 1, 14, 30, tzinfo=UTC)
    rows = [
        _make_control_row(1, "pub-1", ts, "subscribe"),
        _make_control_row(2, "pub-2", ts.replace(minute=31), "command"),
    ]
    repo = _EventStubRepo(rows=rows)
    archiver = EventArchiver(repo, tmp_path)
    result = archiver.export(
        table="control",
        day_start=date(2024, 1, 1),
        day_end=date(2024, 1, 1),
    )
    assert result.files_written == 1
    assert result.rows_exported == 2
    csv_path = tmp_path / "archive" / "control" / "2024" / "2024-01-01.csv"
    assert csv_path.exists()
    with csv_path.open(encoding="utf-8") as f:
        reader = csv.reader(f)
        header = next(reader)
        data = list(reader)
    assert "message_type" in header
    assert len(data) == 2


def test_event_archiver_export_merges_with_existing(tmp_path: Path) -> None:
    """Export merges new rows with existing CSV file.

    Given: Existing CSV with one row and DB with two rows (one overlapping),
    When: export is called,
    Then: Merged file has two unique rows.
    """
    csv_dir = tmp_path / "archive" / "telemetry" / "2024"
    csv_dir.mkdir(parents=True)
    csv_path = csv_dir / "2024-01-01.csv"
    csv_path.write_text(
        "public_id,timestamp,known_to,session_id,sequence_id,transport,direction,message_type,payload\n"
        "pub-1,2024-01-01T14:30:00+00:00,9999-12-31T23:59:59+00:00,s1,1,zmq,inbound,ping,\n",
        encoding="utf-8",
    )
    ts = datetime(2024, 1, 1, 14, 30, tzinfo=UTC)
    rows = [
        (1, "pub-1", ts, KNOWN_TO_MAX, "s1", 1, "zmq", "inbound", "ping", None),
        (2, "pub-2", ts.replace(minute=31), KNOWN_TO_MAX, "s1", 2, "zmq", "inbound", "pong", None),
    ]
    repo = _EventStubRepo(rows=rows)
    archiver = EventArchiver(repo, tmp_path)
    archiver.export(
        table="telemetry",
        day_start=date(2024, 1, 1),
        day_end=date(2024, 1, 1),
    )
    with csv_path.open(encoding="utf-8") as f:
        reader = csv.reader(f)
        next(reader)
        data = list(reader)
    assert len(data) == 2


def test_event_archiver_export_multi_day(tmp_path: Path) -> None:
    """Export spanning multiple days creates per-day files.

    Given: Control rows across two days,
    When: export is called with two-day range,
    Then: Creates separate CSV for each day.
    """
    rows = [
        _make_control_row(1, "pub-1", datetime(2024, 1, 1, 14, 30, tzinfo=UTC), "cmd1"),
        _make_control_row(2, "pub-2", datetime(2024, 1, 2, 10, 0, tzinfo=UTC), "cmd2"),
    ]
    repo = _EventStubRepo(rows=rows)
    archiver = EventArchiver(repo, tmp_path)
    result = archiver.export(
        table="control",
        day_start=date(2024, 1, 1),
        day_end=date(2024, 1, 2),
    )
    assert result.files_written == 2
    assert (tmp_path / "archive" / "control" / "2024" / "2024-01-01.csv").exists()
    assert (tmp_path / "archive" / "control" / "2024" / "2024-01-02.csv").exists()


def _make_execution_row(
    row_id: int,
    public_id: str,
    ts: datetime,
    order_public_id: str,
) -> tuple[Any, ...]:
    """Build an execution DB row tuple (id, *columns) incl. the scope plane."""
    return (
        row_id,
        public_id,
        ts,
        KNOWN_TO_MAX,
        "test-session",
        row_id,
        order_public_id,
        f"exec-{row_id}",
        f"trade-{row_id}",
        "buy",
        "filled",
        100.0,
        1.0,
        0.1,
        "USD",
        ts,
        "00000000-0000-7000-8000-000000000001",
        "kraken",
        "live",
        row_id,
    )


def test_event_archiver_export_executions_via_order(tmp_path: Path) -> None:
    """Export executions resolves path and carries the full scope plane.

    The invariant-bearing fields (``wallet_public_id``, ``exchange``,
    ``mode``, ``scope_sequence``) must round out to the archive: without
    them a restored/reconstructed row could never satisfy the NOT NULL
    scope schema or the counter invariant.

    Given: Execution referencing an order mapped to an instrument,
    When: export is called for executions,
    Then: CSV created under the correct exchange/archive_symbol path with
        the scope columns in the header and the scope values in the row.
    """
    ts = datetime(2024, 1, 1, 14, 30, tzinfo=UTC)
    rows = [_make_execution_row(1, "pub-1", ts, "order-1")]
    repo = _EventStubRepo(
        orders={"order-1": ("BTC-USD", "kraken")},
        rows=rows,
    )
    archiver = EventArchiver(repo, tmp_path)
    result = archiver.export(
        table="executions",
        day_start=date(2024, 1, 1),
        day_end=date(2024, 1, 1),
    )
    assert result.files_written == 1
    assert result.rows_exported == 1
    csv_path = (
        tmp_path / "archive" / "executions" / "kraken" / "BTC-USD" / "2024" / "2024-01-01.csv"
    )
    assert csv_path.exists()
    with csv_path.open(encoding="utf-8") as f:
        reader = csv.reader(f)
        header = next(reader)
        data = list(reader)
    assert header[-4:] == ["wallet_public_id", "exchange", "mode", "scope_sequence"]
    assert data[0][-4:] == ["00000000-0000-7000-8000-000000000001", "kraken", "live", "1"]


def test_event_archiver_export_executions_merges_old_header_files(tmp_path: Path) -> None:
    """A pre-scope-plane archive file merges cleanly under the new header.

    Files exported before the scope columns existed carry a narrower
    header; merging their rows verbatim against the wider current header
    would misalign every column. Old rows must be remapped by column
    name and padded for the columns their era lacked.

    Given: An existing executions CSV written under the pre-scope header
        and a new export for the same day,
    When: export runs,
    Then: The merged file carries the CURRENT header, the old row padded
        with empty scope values, and the new row in full.
    """
    ts = datetime(2024, 1, 1, 14, 30, tzinfo=UTC)
    csv_path = (
        tmp_path / "archive" / "executions" / "kraken" / "BTC-USD" / "2024" / "2024-01-01.csv"
    )
    csv_path.parent.mkdir(parents=True, exist_ok=True)
    old_header = [
        "public_id",
        "timestamp",
        "known_to",
        "session_id",
        "sequence_id",
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
    ]
    old_row = [
        "pub-old",
        ts.isoformat(),
        KNOWN_TO_MAX.isoformat(),
        "old-session",
        "1",
        "order-1",
        "exec-old",
        "trade-old",
        "sell",
        "filled",
        "99.0",
        "2.0",
        "0.2",
        "USD",
        ts.isoformat(),
    ]
    with csv_path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f, lineterminator="\n")
        writer.writerow(old_header)
        writer.writerow(old_row)
    rows = [_make_execution_row(2, "pub-new", ts.replace(minute=45), "order-1")]
    repo = _EventStubRepo(
        orders={"order-1": ("BTC-USD", "kraken")},
        rows=rows,
    )
    archiver = EventArchiver(repo, tmp_path)
    result = archiver.export(
        table="executions",
        day_start=date(2024, 1, 1),
        day_end=date(2024, 1, 1),
    )
    assert result.files_written == 1
    with csv_path.open(encoding="utf-8") as f:
        reader = csv.reader(f)
        header = next(reader)
        data = {row[0]: row for row in reader}
    assert header == list(EVENT_TABLES["executions"].columns)
    assert len(data) == 2
    assert data["pub-old"][-4:] == ["", "", "", ""]
    assert data["pub-old"][5] == "order-1"
    assert data["pub-new"][-4:] == [
        "00000000-0000-7000-8000-000000000001",
        "kraken",
        "live",
        "2",
    ]


def _make_db_with_events(tmp_path: Path) -> tuple[DatabaseRepository, str]:
    """Create DB with Symbol, Instrument, and Tick rows for integration tests.

    Returns:
        Tuple of (DatabaseRepository instance, instrument_public_id).
    """
    db_url = f"sqlite:///{tmp_path / 'test.db'}"
    repo = DatabaseRepository(db_url)
    Base.metadata.create_all(
        repo.engine,
        tables=[
            cast(Table, Symbol.__table__),
            cast(Table, Instrument.__table__),
            cast(Table, Tick.__table__),
        ],
    )
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
        session.flush()
        tick2 = Tick(
            public_id="tick-2",
            instrument_public_id="inst-btc",
            bid=101.0,
            ask=102.0,
            last=101.0,
            volume=2000.0,
            session_id="test",
            sequence_id=4,
            timestamp=datetime(2024, 1, 1, 14, 31, tzinfo=UTC),
            known_to=KNOWN_TO_MAX,
        )
        session.add(tick2)
        session.commit()
    return repo, "inst-btc"


def test_repo_get_event_rows_for_archive(tmp_path: Path) -> None:
    """Repository returns event rows for archive export.

    Given: DB with two ticks,
    When: get_event_rows_for_archive is called,
    Then: Returns two rows with id as first element.
    """
    repo, _inst_id = _make_db_with_events(tmp_path)
    spec = EVENT_TABLES["ticks"]
    rows = repo.get_event_rows_for_archive(
        spec.model,
        spec.columns,
        date(2024, 1, 1),
        date(2024, 1, 1),
    )
    assert len(rows) == 2
    assert isinstance(rows[0][0], int)
    assert rows[0][1] == "tick-1"
    assert rows[1][1] == "tick-2"
    repo.dispose()


def test_repo_get_event_rows_for_archive_empty_range(tmp_path: Path) -> None:
    """Repository returns empty list for date range with no events.

    Given: DB with ticks on 2024-01-01,
    When: get_event_rows_for_archive called for 2024-02-01,
    Then: Returns empty list.
    """
    repo, _inst_id = _make_db_with_events(tmp_path)
    spec = EVENT_TABLES["ticks"]
    rows = repo.get_event_rows_for_archive(
        spec.model,
        spec.columns,
        date(2024, 2, 1),
        date(2024, 2, 1),
    )
    assert rows == []
    repo.dispose()


def test_repo_delete_rows_by_id(tmp_path: Path) -> None:
    """Repository deletes rows by id.

    Given: DB with two ticks,
    When: delete_rows_by_id is called with one id,
    Then: One row deleted, one remains.
    """
    repo, _inst_id = _make_db_with_events(tmp_path)
    spec = EVENT_TABLES["ticks"]
    rows = repo.get_event_rows_for_archive(
        spec.model,
        spec.columns,
        date(2024, 1, 1),
        date(2024, 1, 1),
    )
    first_id = rows[0][0]
    deleted = repo.delete_rows_by_id(Tick, [first_id])
    assert deleted == 1
    remaining = repo.get_event_rows_for_archive(
        spec.model,
        spec.columns,
        date(2024, 1, 1),
        date(2024, 1, 1),
    )
    assert len(remaining) == 1
    repo.dispose()


def test_repo_delete_rows_by_id_empty_list(tmp_path: Path) -> None:
    """Deleting with empty list returns zero.

    Given: Empty row_ids list,
    When: delete_rows_by_id is called,
    Then: Returns 0 without touching DB.
    """
    repo, _inst_id = _make_db_with_events(tmp_path)
    deleted = repo.delete_rows_by_id(Tick, [])
    assert deleted == 0
    repo.dispose()


def test_repo_delete_rows_by_id_batch_boundary(tmp_path: Path) -> None:
    """Delete >500 rows crosses batch boundary correctly.

    Given: DB with 520 ticks,
    When: delete_rows_by_id called with all 520 ids,
    Then: All 520 deleted (two batches: 500 + 20).
    """
    db_url = f"sqlite:///{tmp_path / 'batch.db'}"
    repo = DatabaseRepository(db_url)
    Base.metadata.create_all(repo.engine)
    ts_base = datetime(2024, 1, 1, tzinfo=UTC)
    with repo.get_session() as session:
        sym = Symbol(
            public_id="sym-btc",
            native_symbol="BTC-USD",
            base="BTC",
            quote="USD",
            asset_type="crypto",
            session_id="test",
            sequence_id=1,
            timestamp=ts_base,
            created_at=ts_base,
        )
        session.add(sym)
        session.flush()
        inst = Instrument(
            public_id="inst-btc",
            symbol_public_id="sym-btc",
            exchange="polygon",
            session_id="test",
            sequence_id=2,
            timestamp=ts_base,
        )
        session.add(inst)
        session.flush()
        for i in range(520):
            tick = Tick(
                public_id=f"tick-{i}",
                instrument_public_id="inst-btc",
                bid=100.0 + i,
                ask=101.0 + i,
                last=100.0 + i,
                volume=1000.0,
                session_id="test",
                sequence_id=100 + i,
                timestamp=ts_base.replace(second=i % 60, minute=i // 60),
                known_to=KNOWN_TO_MAX,
            )
            session.add(tick)
        session.commit()
    spec = EVENT_TABLES["ticks"]
    rows = repo.get_event_rows_for_archive(
        spec.model,
        spec.columns,
        date(2024, 1, 1),
        date(2024, 1, 1),
    )
    assert len(rows) == 520
    all_ids = [r[0] for r in rows]
    deleted = repo.delete_rows_by_id(Tick, all_ids)
    assert deleted == 520
    remaining = repo.get_event_rows_for_archive(
        spec.model,
        spec.columns,
        date(2024, 1, 1),
        date(2024, 1, 1),
    )
    assert len(remaining) == 0
    repo.dispose()


def _make_db_with_one_execution(tmp_path: Path) -> tuple[DatabaseRepository, int]:
    """Create a DB holding a single certified execution ledger row.

    The row is written directly through the ORM to seed physical state
    for the primitive-level guard tests. It stands in for a sealed
    prefix entry: ``scope_sequence`` 3 inside the
    ``(wallet, exchange, mode)`` scope, exactly the tip a watermark
    would have captured.

    Returns:
        Tuple of (repository, execution primary-key id).
    """
    db_url = f"sqlite:///{tmp_path / 'exec.db'}"
    repo = DatabaseRepository(db_url)
    Base.metadata.create_all(
        repo.engine,
        tables=[cast(Table, Execution.__table__)],
    )
    ts = datetime(2024, 1, 1, 14, 30, tzinfo=UTC)
    with repo.get_session() as session:
        execution = Execution(
            public_id="exec-pub-3",
            order_public_id="order-pub-1",
            wallet_public_id="wallet-pub-1",
            exchange="kraken",
            mode="live",
            scope_sequence=3,
            side="buy",
            status="filled",
            price=100.0,
            size=1.0,
            fee=0.1,
            fee_asset="USD",
            session_id="test",
            sequence_id=1,
            timestamp=ts,
            known_to=KNOWN_TO_MAX,
        )
        session.add(execution)
        session.commit()
        row_id = execution.id
    return repo, row_id


def test_repo_delete_rows_by_id_refuses_execution(tmp_path: Path) -> None:
    """Physical delete refuses the executions ledger and preserves the row.

    Concrete failure this closes: with a watermark captured at
    ``scope_sequence`` 3, ``delete_rows_by_id(Execution, [id])`` would
    physically remove the certified row, dropping ``max(scope_sequence)``
    to 2 so normal ingest re-allocates ``committed max + 1 == 3`` for a
    DIFFERENT fill. The counted range proof still passes over the now
    contiguous ``{1, 2, 3}`` while a sealed prefix entry has been
    silently replaced — the exact false-authoritative verdict the
    program exists to prevent.

    Given: A DB holding one certified execution row,
    When: ``delete_rows_by_id`` is called with the ``Execution`` model,
    Then: ``ExecutionPhysicalMutationError`` is raised at the primitive
        and the row still exists, so the sealed prefix is intact.
    """
    repo, row_id = _make_db_with_one_execution(tmp_path)
    with pytest.raises(
        ExecutionPhysicalMutationError,
        match="physical execution deletion is refused",
    ):
        repo.delete_rows_by_id(Execution, [row_id])
    with repo.get_session() as session:
        surviving = session.execute(select(func.count()).select_from(Execution)).scalar_one()
    assert surviving == 1
    repo.dispose()


def test_repo_delete_rows_by_id_refuses_execution_empty_list(tmp_path: Path) -> None:
    """Execution refusal fires before the empty-list shortcut.

    The theorem must hold against the primitive regardless of arguments:
    a caller passing an empty id list must still be refused for the
    executions model rather than silently returning 0, so no future
    call site can probe the primitive for the executions table under
    any argument shape.

    Given: A DB holding one certified execution row,
    When: ``delete_rows_by_id(Execution, [])`` is called,
    Then: ``ExecutionPhysicalMutationError`` is raised before the empty
        list is inspected.
    """
    repo, _row_id = _make_db_with_one_execution(tmp_path)
    with pytest.raises(
        ExecutionPhysicalMutationError,
        match="physical execution deletion is refused",
    ):
        repo.delete_rows_by_id(Execution, [])
    repo.dispose()


def test_repo_bulk_insert_from_archive_refuses_execution(tmp_path: Path) -> None:
    """Bulk insert refuses the executions ledger and inserts nothing.

    Concrete failure this closes: a fenceless, dedup-free bulk insert of
    an archived execution row could reintroduce a ``scope_sequence`` at
    or below a live anchor watermark, silently corrupting the counted
    range proof. The refusal must hold at the primitive because a future
    restore-shaped path could reach it with the ``Execution`` model.

    Given: A DB holding one certified execution row,
    When: ``bulk_insert_from_archive`` is called with the ``Execution``
        model and a candidate row,
    Then: ``ExecutionPhysicalMutationError`` is raised at the primitive
        and no row is inserted.
    """
    repo, _row_id = _make_db_with_one_execution(tmp_path)
    candidate = {
        "public_id": "exec-pub-2",
        "order_public_id": "order-pub-1",
        "wallet_public_id": "wallet-pub-1",
        "exchange": "kraken",
        "mode": "live",
        "scope_sequence": 2,
        "side": "buy",
        "status": "filled",
        "price": 99.0,
        "size": 1.0,
        "fee": 0.1,
        "fee_asset": "USD",
        "session_id": "test",
        "sequence_id": 2,
        "timestamp": datetime(2024, 1, 1, 14, 29, tzinfo=UTC),
        "known_to": KNOWN_TO_MAX,
    }
    with pytest.raises(
        ExecutionPhysicalMutationError,
        match="physical execution reintroduction is refused",
    ):
        repo.bulk_insert_from_archive(Execution, [candidate])
    with repo.get_session() as session:
        count = session.execute(select(func.count()).select_from(Execution)).scalar_one()
    assert count == 1
    repo.dispose()


def test_repo_bulk_insert_from_archive_refuses_execution_empty_rows(tmp_path: Path) -> None:
    """Execution refusal fires before the empty-rows shortcut.

    The theorem holds against the primitive under any argument shape:
    even an empty row list must be refused for the executions model, so
    the guard is not dependent on the caller supplying rows.

    Given: A DB holding one certified execution row,
    When: ``bulk_insert_from_archive(Execution, [])`` is called,
    Then: ``ExecutionPhysicalMutationError`` is raised before the empty
        list is inspected.
    """
    repo, _row_id = _make_db_with_one_execution(tmp_path)
    with pytest.raises(
        ExecutionPhysicalMutationError,
        match="physical execution reintroduction is refused",
    ):
        repo.bulk_insert_from_archive(Execution, [])
    repo.dispose()


def test_repo_get_order_archive_map(tmp_path: Path) -> None:
    """Repository maps order_public_id to (archive_symbol, exchange).

    Given: DB with Symbol, Instrument, and Order,
    When: get_order_archive_map is called,
    Then: Returns mapping from order to instrument's archive info.
    """
    db_url = f"sqlite:///{tmp_path / 'order.db'}"
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
            exchange="kraken",
            session_id="test",
            sequence_id=2,
            timestamp=ts,
        )
        session.add(inst)
        session.flush()
        order = Order(
            public_id="order-1",
            instrument_public_id="inst-btc",
            wallet_public_id="00000000-0000-7000-8000-000000000001",
            client_order_id="client-1",
            created_at=ts,
            side="buy",
            order_type="market",
            size=1.0,
            status="filled",
            filled_size=1.0,
            session_id="test",
            sequence_id=3,
            timestamp=ts,
            known_to=KNOWN_TO_MAX,
        )
        session.add(order)
        session.commit()
    order_map = repo.get_order_archive_map()
    assert "order-1" in order_map
    arch_sym, exch = order_map["order-1"]
    assert arch_sym == "BTC-USD"
    assert exch == "kraken"
    repo.dispose()


def test_repo_get_order_archive_map_skips_orphan(tmp_path: Path) -> None:
    """Orders referencing unmapped instruments are excluded.

    Given: Order whose instrument_public_id has no Symbol mapping,
    When: get_order_archive_map is called,
    Then: That order is not in the result mapping.
    """
    db_url = f"sqlite:///{tmp_path / 'orphan_order.db'}"
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
        session.flush()
        order = Order(
            public_id="order-orphan",
            instrument_public_id="inst-orphan",
            wallet_public_id="00000000-0000-7000-8000-000000000001",
            client_order_id="client-1",
            created_at=ts,
            side="buy",
            order_type="market",
            size=1.0,
            status="filled",
            filled_size=1.0,
            session_id="test",
            sequence_id=2,
            timestamp=ts,
            known_to=KNOWN_TO_MAX,
        )
        session.add(order)
        session.commit()
    order_map = repo.get_order_archive_map()
    assert "order-orphan" not in order_map
    repo.dispose()


def test_event_archiver_integration_roundtrip(tmp_path: Path) -> None:
    """Full integration: export ticks from DB to CSV.

    Given: DB with Symbol, Instrument, and Ticks,
    When: EventArchiver.export is called,
    Then: CSV file has correct content matching DB rows.
    """
    repo, _inst_id = _make_db_with_events(tmp_path)
    archiver = EventArchiver(repo, tmp_path)
    result = archiver.export(
        table="ticks",
        day_start=date(2024, 1, 1),
        day_end=date(2024, 1, 1),
    )
    assert result.files_written == 1
    assert result.rows_exported == 2
    csv_path = tmp_path / "archive" / "ticks" / "polygon" / "BTC-USD" / "2024" / "2024-01-01.csv"
    with csv_path.open(encoding="utf-8") as f:
        reader = csv.reader(f)
        header = next(reader)
        data = list(reader)
    assert header[0] == "public_id"
    assert len(data) == 2
    assert data[0][0] == "tick-1"
    assert data[0][6] == "100.5"
    repo.dispose()


def test_event_archiver_integration_purge(tmp_path: Path) -> None:
    """Full integration: export and purge ticks.

    Given: DB with two ticks,
    When: export called with purge=True,
    Then: Rows exported to CSV and deleted from DB.
    """
    repo, _inst_id = _make_db_with_events(tmp_path)
    archiver = EventArchiver(repo, tmp_path)
    result = archiver.export(
        table="ticks",
        day_start=date(2024, 1, 1),
        day_end=date(2024, 1, 1),
        purge=True,
    )
    assert result.rows_exported == 2
    assert result.rows_purged == 2
    spec = EVENT_TABLES["ticks"]
    remaining = repo.get_event_rows_for_archive(
        spec.model,
        spec.columns,
        date(2024, 1, 1),
        date(2024, 1, 1),
    )
    assert len(remaining) == 0
    repo.dispose()


def test_event_archiver_integration_merge_idempotent(tmp_path: Path) -> None:
    """Exporting twice produces same result (merge/dedup).

    Given: DB with ticks,
    When: export called twice,
    Then: CSV file has same number of rows.
    """
    repo, _inst_id = _make_db_with_events(tmp_path)
    archiver = EventArchiver(repo, tmp_path)
    archiver.export(
        table="ticks",
        day_start=date(2024, 1, 1),
        day_end=date(2024, 1, 1),
    )
    archiver.export(
        table="ticks",
        day_start=date(2024, 1, 1),
        day_end=date(2024, 1, 1),
    )
    csv_path = tmp_path / "archive" / "ticks" / "polygon" / "BTC-USD" / "2024" / "2024-01-01.csv"
    with csv_path.open(encoding="utf-8") as f:
        reader = csv.reader(f)
        next(reader)
        data = list(reader)
    assert len(data) == 2
    repo.dispose()


def test_cli_archive_event_table(tmp_path: Path) -> None:
    """CLI archive command dispatches to EventArchiver for event tables.

    Given: Mocked EventArchiver,
    When: CLI invoked with --table ticks,
    Then: EventArchiver.export is called.
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
        patch("snapper.cli.app.EventArchiver", return_value=mock_archiver),
    ):
        mock_bootstrap.return_value = MagicMock(db_url="sqlite:///")
        mock_repo_cls.return_value = MagicMock()
        result = runner.invoke(
            app,
            [
                "archive",
                "--table",
                "ticks",
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
    assert call_kwargs["table"] == "ticks"


def test_cli_archive_event_with_purge(tmp_path: Path) -> None:
    """CLI archive command passes --purge to EventArchiver.

    Given: Mocked EventArchiver,
    When: CLI invoked with --table ticks --purge,
    Then: EventArchiver.export called with purge=True.
    """
    runner = CliRunner()
    mock_archiver = MagicMock()
    mock_archiver.export.return_value = ExportResult(
        files_written=1,
        rows_exported=10,
        rows_purged=10,
    )
    with (
        patch("snapper.cli.app.BootstrapSettingsLoader") as mock_bootstrap,
        patch("snapper.cli.app.DatabaseRepository") as mock_repo_cls,
        patch("snapper.cli.app.EventArchiver", return_value=mock_archiver),
    ):
        mock_bootstrap.return_value = MagicMock(db_url="sqlite:///")
        mock_repo_cls.return_value = MagicMock()
        result = runner.invoke(
            app,
            [
                "archive",
                "--table",
                "ticks",
                "--day",
                "2024-01-01",
                "--purge",
                "--output-dir",
                str(tmp_path),
            ],
        )
    assert result.exit_code == 0
    assert "10 purged" in result.output
    call_kwargs = mock_archiver.export.call_args[1]
    assert call_kwargs["purge"] is True


def test_cli_archive_candle_purge_rejected() -> None:
    """CLI archive command rejects --purge for candle cache export.

    Given: --table candles --purge,
    When: CLI invoked,
    Then: Exits with error.
    """
    runner = CliRunner()
    result = runner.invoke(
        app,
        ["archive", "--table", "candles", "--purge", "--day", "2024-01-01"],
    )
    assert result.exit_code == 1
    assert "not supported" in result.output


def test_cli_archive_unknown_table_rejected() -> None:
    """CLI archive command rejects unknown table name.

    Given: --table unknown,
    When: CLI invoked,
    Then: Exits with error.
    """
    runner = CliRunner()
    result = runner.invoke(
        app,
        ["archive", "--table", "unknown", "--day", "2024-01-01"],
    )
    assert result.exit_code == 1
    assert "unknown" in result.output.lower()


_WALLET_ONE = "00000000-0000-7000-8000-000000000001"
_EXEC_TS = datetime(2024, 1, 1, 14, 30, tzinfo=UTC)
_EXEC_CSV_PARTS = ("archive", "executions", "kraken", "BTC-USD", "2024", "2024-01-01.csv")


def _execution_stub_repo() -> _EventStubRepo:
    """Build a stub repo holding three exportable execution rows."""
    return _EventStubRepo(
        orders={"order-1": ("BTC-USD", "kraken")},
        rows=[_make_execution_row(i, f"pub-{i}", _EXEC_TS, "order-1") for i in (1, 2, 3)],
    )


def test_event_archiver_purge_executions_refused_and_delete_unreachable(
    tmp_path: Path,
) -> None:
    """Execution purge raises and never reaches the delete path.

    Given: Exportable execution rows and a repo spy recording deletes,
    When: export is called with purge=True for executions,
    Then: ExecutionPurgeUnsupportedError is raised and delete_rows_by_id
        is never invoked. The spy — not a zero rows_purged count — is
        the assertion: a count of zero is satisfiable by a filter that
        happens to empty, whereas an unrecorded delete proves the path
        is unreachable.
    """
    repo = _execution_stub_repo()
    archiver = EventArchiver(cast(DatabaseRepository, repo), tmp_path)
    with pytest.raises(ExecutionPurgeUnsupportedError, match="execution archive purge is refused"):
        archiver.export(
            table="executions",
            day_start=date(2024, 1, 1),
            day_end=date(2024, 1, 1),
            purge=True,
        )
    assert repo.deleted_ids == []


def test_event_archiver_purge_executions_refused_before_any_side_effect(
    tmp_path: Path,
) -> None:
    """The refusal fires before rows are read or files are written.

    Given: A stub repo counting archive row reads,
    When: export is called with purge=True for executions,
    Then: No row read happens and no CSV file is produced — an
        unsupported request leaves no partial state behind.
    """
    repo = _execution_stub_repo()
    archiver = EventArchiver(cast(DatabaseRepository, repo), tmp_path)
    with pytest.raises(ExecutionPurgeUnsupportedError):
        archiver.export(
            table="executions",
            day_start=date(2024, 1, 1),
            day_end=date(2024, 1, 1),
            purge=True,
        )
    assert repo.read_calls == 0
    assert not list(tmp_path.rglob("*.csv"))


def test_event_archiver_purge_executions_refused_in_dry_run(tmp_path: Path) -> None:
    """dry_run does not exempt an execution purge request from refusal.

    Given: Exportable execution rows,
    When: export is called with dry_run=True and purge=True,
    Then: ExecutionPurgeUnsupportedError is raised rather than a plan
        being reported. A dry run that reports a purge which can never
        be executed would advertise a capability that does not exist.
    """
    repo = _execution_stub_repo()
    archiver = EventArchiver(cast(DatabaseRepository, repo), tmp_path)
    with pytest.raises(ExecutionPurgeUnsupportedError):
        archiver.export(
            table="executions",
            day_start=date(2024, 1, 1),
            day_end=date(2024, 1, 1),
            dry_run=True,
            purge=True,
        )
    assert repo.deleted_ids == []


def test_event_archiver_export_executions_without_purge_archives_every_row(
    tmp_path: Path,
) -> None:
    """Refusing the purge does not degrade the execution archive.

    Given: Three exportable execution rows,
    When: export is called with purge=False,
    Then: Every row is written to CSV and nothing is deleted — the
        archive remains complete; only the DB delete is refused.
    """
    repo = _execution_stub_repo()
    archiver = EventArchiver(cast(DatabaseRepository, repo), tmp_path)
    result = archiver.export(
        table="executions",
        day_start=date(2024, 1, 1),
        day_end=date(2024, 1, 1),
    )
    assert result.rows_exported == 3
    assert result.rows_purged == 0
    assert repo.deleted_ids == []
    with tmp_path.joinpath(*_EXEC_CSV_PARTS).open(encoding="utf-8") as f:
        reader = csv.reader(f)
        next(reader)
        data = list(reader)
    assert len(data) == 3


def test_event_archiver_purge_non_executions_still_deletes(tmp_path: Path) -> None:
    """The refusal is scoped to executions and leaves other tables purging.

    Given: Tick rows,
    When: export is called with purge=True for ticks,
    Then: All rows are deleted — no other event table is affected.
    """
    rows = [
        _make_tick_row(1, "pub-1", _EXEC_TS, "inst-1", 100.5),
        _make_tick_row(2, "pub-2", _EXEC_TS.replace(minute=31), "inst-1", 101.0),
    ]
    repo = _EventStubRepo(instruments={"inst-1": ("BTC-USD", "polygon")}, rows=rows)
    archiver = EventArchiver(cast(DatabaseRepository, repo), tmp_path)
    result = archiver.export(
        table="ticks",
        day_start=date(2024, 1, 1),
        day_end=date(2024, 1, 1),
        purge=True,
    )
    assert result.rows_purged == 2
    assert repo.deleted_ids == [1, 2]


def _build_execution(
    *,
    public_id: str,
    order_public_id: str,
    wallet_public_id: str,
    exchange: str,
    mode: str,
    scope_sequence: int,
    sequence_id: int,
) -> Execution:
    """Build an Execution ORM row for purge-refusal integration tests."""
    return Execution(
        public_id=public_id,
        order_public_id=order_public_id,
        wallet_public_id=wallet_public_id,
        exchange=exchange,
        mode=mode,
        scope_sequence=scope_sequence,
        exec_id=f"exec-{public_id}",
        trade_id=f"trade-{public_id}",
        side="buy",
        status="filled",
        price=100.0,
        size=1.0,
        fee=0.1,
        fee_asset="USD",
        executed_at=_EXEC_TS,
        session_id="test",
        sequence_id=sequence_id,
        timestamp=_EXEC_TS,
        known_to=KNOWN_TO_MAX,
    )


def _build_anchor(
    *,
    wallet_public_id: str,
    exchange: str,
    source_watermark: int,
    sequence_id: int = 900,
) -> PortfolioSpotReconciliationAnchor:
    """Build an active spot reconciliation anchor ORM row."""
    t0 = datetime(2024, 1, 1, 12, 0, tzinfo=UTC)
    return PortfolioSpotReconciliationAnchor(
        public_id=f"anchor-{wallet_public_id[-4:]}-{exchange}",
        wallet_public_id=wallet_public_id,
        exchange=exchange,
        mode="live",
        venue_account_state_public_id="00000000-0000-7000-8000-000000000201",
        balance_observation_id=41,
        source_watermark_kind="scope_sequence",
        source_watermark=source_watermark,
        balances_json='{"BTC":"0.1"}',
        first_request_started_at=t0,
        first_request_completed_at=t0 + timedelta(seconds=1),
        second_request_started_at=t0 + timedelta(seconds=1),
        second_request_completed_at=t0 + timedelta(seconds=2),
        boundary_status="cursor_certified",
        inventory_status="venue_reported_full",
        margin_status="cash",
        provenance="kraken:ccxt.fetch_balance",
        source_chain_tip="e5f6a7b8c9d0e1f2a3b4c5d6e7f8a9b0c1d2e3f4a5b6c7d8e9f0a1b2c3d4e5f6",
        venue_cursor_kind="account_history_item_id",
        venue_cursor_scheme=f"{exchange}:ccxt:account/history:v1",
        venue_cursor_value="1",
        venue_cursor_requested_at=t0 - timedelta(seconds=4),
        venue_cursor_observed_at=t0 - timedelta(seconds=3),
        venue_cursor_confirmed_at=t0 + timedelta(seconds=2),
        source_watermark_requested_at=t0 - timedelta(seconds=2),
        source_watermark_captured_at=t0 - timedelta(seconds=1),
        session_id="test",
        sequence_id=sequence_id,
        timestamp=t0 + timedelta(seconds=2),
        known_to=KNOWN_TO_MAX,
    )


_EXECUTION_SCOPES: tuple[tuple[str, int], ...] = (
    ("live", 1),
    ("live", 2),
    ("live", 3),
    ("live", 4),
    ("paper", 1),
    ("paper", 2),
)
_ALL_EXECUTION_PUBLIC_IDS = frozenset(f"pub-{mode}-{seq}" for mode, seq in _EXECUTION_SCOPES)


def _seed_execution_scopes(repo: DatabaseRepository, *, with_anchor: bool) -> None:
    """Seed lineage plus six executions, optionally under an active anchor.

    Seeds Symbol -> Instrument -> Order lineage so executions resolve an
    archive path, four live executions (sequences 1..4) and two paper
    executions (1..2), all timestamped on 2024-01-01. When
    ``with_anchor`` is True an active anchor with ``source_watermark=2``
    covers the live scope, making live sequences 3..4 the replay
    evidence a purge must never strand.
    """
    ts = datetime(2024, 1, 1, tzinfo=UTC)
    with repo.get_session() as session:
        session.add(
            Symbol(
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
        )
        session.flush()
        session.add(
            Instrument(
                public_id="inst-btc",
                symbol_public_id="sym-btc",
                exchange="kraken",
                session_id="test",
                sequence_id=2,
                timestamp=ts,
            )
        )
        session.flush()
        session.add(
            Order(
                public_id="order-1",
                instrument_public_id="inst-btc",
                wallet_public_id=_WALLET_ONE,
                client_order_id="client-1",
                created_at=ts,
                side="buy",
                order_type="market",
                size=1.0,
                status="filled",
                filled_size=1.0,
                session_id="test",
                sequence_id=3,
                timestamp=ts,
                known_to=KNOWN_TO_MAX,
            )
        )
        session.flush()
        seq_id = 4
        for mode, seq in _EXECUTION_SCOPES:
            session.add(
                _build_execution(
                    public_id=f"pub-{mode}-{seq}",
                    order_public_id="order-1",
                    wallet_public_id=_WALLET_ONE,
                    exchange="kraken",
                    mode=mode,
                    scope_sequence=seq,
                    sequence_id=seq_id,
                )
            )
            seq_id += 1
        if with_anchor:
            session.add(
                _build_anchor(
                    wallet_public_id=_WALLET_ONE,
                    exchange="kraken",
                    source_watermark=2,
                )
            )
        session.commit()


def _surviving_execution_public_ids(repo: DatabaseRepository) -> set[str]:
    """Return the public_id of every execution row still in the DB."""
    spec = EVENT_TABLES["executions"]
    rows = repo.get_event_rows_for_archive(
        spec.model,
        spec.columns,
        date(2024, 1, 1),
        date(2024, 1, 1),
    )
    return {str(row[1]) for row in rows}


def test_event_archiver_integration_purge_executions_refused_with_live_anchor(
    tmp_path: Path,
) -> None:
    """A real DB under a live anchor refuses the purge and keeps every row.

    Given: A SQLite DB with six executions and an active anchor at
        source_watermark=2,
    When: export is called with purge=True for executions,
    Then: ExecutionPurgeUnsupportedError is raised and all six rows
        survive — including live sequences 3..4 above the watermark.
    """
    repo = DatabaseRepository(f"sqlite:///{tmp_path / 'exec_purge.db'}")
    Base.metadata.create_all(repo.engine)
    _seed_execution_scopes(repo, with_anchor=True)
    archiver = EventArchiver(repo, tmp_path)
    with pytest.raises(ExecutionPurgeUnsupportedError):
        archiver.export(
            table="executions",
            day_start=date(2024, 1, 1),
            day_end=date(2024, 1, 1),
            purge=True,
        )
    assert _surviving_execution_public_ids(repo) == _ALL_EXECUTION_PUBLIC_IDS
    repo.dispose()


class _AnchorRacingRepo(DatabaseRepository):
    """Repository that lands a real anchor on a second engine at delete time.

    Reproduces N2's concrete failure interleave. The archiver decides
    which execution rows are purgeable while no anchor exists; an anchor
    then commits on an independent engine (a separate connection and
    transaction, standing in for the out-of-process anchor writer) before
    the delete lands; the delete strands rows above the freshly committed
    watermark. The anchor write is a genuine INSERT through a second
    ``DatabaseRepository``, not a mock, so reaching the delete path at
    all is what makes the race real.

    ``delete_calls`` records entry into the delete path. Because the
    purge is refused before any read, this hook must never fire for
    executions; if it does, the archiver has decided to delete execution
    rows without holding anything that serializes it against the anchor
    writer, which is exactly the defect.

    Attributes:
        delete_calls: Number of times the delete path was entered.
    """

    def __init__(self, db_url: str) -> None:
        """Initialize the racing repository.

        Args:
            db_url: Database URL for both this repo and the anchor writer.
        """
        super().__init__(db_url)
        self.delete_calls: int = 0

    def delete_rows_by_id(self, model: type[Any], row_ids: list[int]) -> int:
        """Commit an anchor on a second engine, then perform the delete.

        Args:
            model: SQLAlchemy model class.
            row_ids: Row ids the archiver decided were purgeable.

        Returns:
            Number of rows deleted by the real delete.
        """
        self.delete_calls += 1
        writer = DatabaseRepository(self.db_url)
        try:
            with writer.get_session() as session:
                session.add(
                    _build_anchor(
                        wallet_public_id=_WALLET_ONE,
                        exchange="kraken",
                        source_watermark=2,
                    )
                )
                session.commit()
        finally:
            writer.dispose()
        return super().delete_rows_by_id(model, row_ids)


def test_event_archiver_purge_executions_refuses_anchor_commit_race(
    tmp_path: Path,
) -> None:
    """The anchor-lands-mid-purge failure is unreachable, not merely filtered.

    Given: A real SQLite DB with six executions and NO anchor, plus a
        repository that commits a real anchor (source_watermark=2) from
        an independent engine at the moment the delete path is entered,
    When: export is called with purge=True for executions,
    Then: The delete path is never entered, so the anchor never lands
        mid-purge, and every execution row survives.

    This is the finding's scenario made concrete. A filtering guard reads
    the protection set in one transaction and deletes in another with
    nothing serializing the two: with no anchor visible at read time it
    would mark live sequences 1..3 and paper 1 purgeable, the anchor
    would commit at watermark 2, and the delete would then strand live
    sequence 3 above a live watermark — failing the counted range proof
    closed forever against an anchor the supported API cannot rewrite.
    Refusing the purge removes the window entirely: with no delete there
    is no race to lose.
    """
    repo = _AnchorRacingRepo(f"sqlite:///{tmp_path / 'exec_race.db'}")
    Base.metadata.create_all(repo.engine)
    _seed_execution_scopes(repo, with_anchor=False)
    archiver = EventArchiver(repo, tmp_path)
    with pytest.raises(ExecutionPurgeUnsupportedError):
        archiver.export(
            table="executions",
            day_start=date(2024, 1, 1),
            day_end=date(2024, 1, 1),
            purge=True,
        )
    assert repo.delete_calls == 0
    assert _surviving_execution_public_ids(repo) == _ALL_EXECUTION_PUBLIC_IDS
    repo.dispose()


def test_cli_archive_execution_purge_rejected() -> None:
    """The CLI rejects --purge for executions with a clean error.

    Given: --table executions --purge,
    When: CLI invoked,
    Then: Exits 1 naming executions, without a traceback.
    """
    runner = CliRunner()
    result = runner.invoke(
        app,
        ["archive", "--table", "executions", "--day", "2024-01-01", "--purge"],
    )
    assert result.exit_code == 1
    assert "not supported for executions" in result.output
