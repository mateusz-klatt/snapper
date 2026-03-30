"""Tests for append-only event table archiver."""

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
from snapper.data.archiver import EVENT_TABLES
from snapper.data.archiver import EventArchiver
from snapper.data.archiver import ExportResult
from snapper.data.archiver import _format_value
from snapper.data.archiver import _merge_and_dedup_events
from snapper.data.archiver import _resolve_event_path
from snapper.data.archiver import _write_event_csv
from snapper.data.models import KNOWN_TO_MAX
from snapper.data.models import Base
from snapper.data.models import Instrument
from snapper.data.models import Order
from snapper.data.models import Symbol
from snapper.data.models import Tick
from snapper.data.repository import DatabaseRepository


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
    """Build an execution DB row tuple (id, *columns)."""
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
    )


def test_event_archiver_export_executions_via_order(tmp_path: Path) -> None:
    """Export executions resolves path via order -> instrument chain.

    Given: Execution referencing an order mapped to an instrument,
    When: export is called for executions,
    Then: CSV created under the correct exchange/archive_symbol path.
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


def _make_db_with_events(tmp_path: Path) -> tuple[DatabaseRepository, str]:
    """Create DB with Symbol, Instrument, and Tick rows for integration tests.

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
