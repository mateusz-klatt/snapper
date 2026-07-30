"""Tests for archive restore (audit mode) and purge/reload round-trip."""

import csv
import shutil
import tracemalloc
from compression import zstd
from datetime import UTC
from datetime import date
from datetime import datetime
from pathlib import Path
from typing import cast
from unittest.mock import MagicMock
from unittest.mock import patch

import pytest
from sqlalchemy import JSON
from sqlalchemy import DateTime
from sqlalchemy import select
from typer.testing import CliRunner

from snapper.cli.app import app
from snapper.data.archiver import EVENT_TABLES
from snapper.data.archiver import ArchiveRestorer
from snapper.data.archiver import CandleAuditArchiver
from snapper.data.archiver import EventArchiver
from snapper.data.archiver import ExecutionRestoreUnsupportedError
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
from snapper.data.archiver import _restore_headers_for_table
from snapper.data.models import KNOWN_TO_MAX
from snapper.data.models import Base
from snapper.data.models import Candle
from snapper.data.models import Instrument
from snapper.data.models import Setting
from snapper.data.models import Symbol
from snapper.data.models import Tick
from snapper.data.models import Trade
from snapper.data.models import TZDateTime
from snapper.data.models import UUIDColumn
from snapper.data.repository import DatabaseRepository

_TRADE_ARCHIVE_HEADER = (
    "public_id",
    "timestamp",
    "known_to",
    "session_id",
    "sequence_id",
    "instrument_public_id",
    "trade_id",
    "executed_at",
    "price",
    "size",
    "side",
    "id",
)
_TRADE_ARCHIVE_ROW = (
    "019ea68f-d48f-7148-8cf5-418963401eb5",
    "2026-06-08 11:28:24.463041+02",
    "10000-01-01 00:59:59+01",
    "019ea3af-eaca-75f6-a7ed-38e8495c07df",
    "7",
    "019e874d-81ac-70ed-a02c-e057914554b0",
    "7636064946467373056",
    "2026-05-04 17:51:56.48+02",
    "4.988",
    "1",
    "buy",
    "30051988",
)
_LARGE_ARCHIVE_ROWS = 100_000
_MAX_RESTORE_PEAK_BYTES = 48 * 1024 * 1024


class _DiscardingArchiveRepository:
    """Measure restore batching without retaining inserted row dictionaries."""

    def __init__(self) -> None:
        """Initialize observed batch sizes and total inserted rows."""
        self.batch_sizes: list[int] = []
        self.rows_inserted = 0

    def get_existing_archive_keys(
        self,
        _model: type[Base],
        _keys_or_day_start: object,
        _day_end: date | None = None,
    ) -> set[tuple[str, datetime, datetime]]:
        """Return no existing keys for either legacy or exact-key lookup shapes."""
        return set()

    def bulk_insert_from_archive(
        self,
        _model: type[Base],
        rows: list[dict[str, object]],
    ) -> int:
        """Count one batch without retaining its allocations."""
        batch_size = len(rows)
        self.batch_sizes.append(batch_size)
        self.rows_inserted += batch_size
        return batch_size


def _write_trade_zstd_archive(path: Path) -> None:
    """Write one compressed row in the immutable production trade format."""
    with zstd.open(path, mode="wt", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle, lineterminator="\n")
        writer.writerow(_TRADE_ARCHIVE_HEADER)
        writer.writerow(_TRADE_ARCHIVE_ROW)


def _write_trade_csv_archive(path: Path) -> None:
    """Write one plain row with offset timestamps in the production format."""
    row = list(_TRADE_ARCHIVE_ROW)
    row[2] = "9999-12-31T23:59:59+00:00"
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle, lineterminator="\n")
        writer.writerow(_TRADE_ARCHIVE_HEADER)
        writer.writerow(row)


def _make_trade_restore_fixture(tmp_path: Path) -> tuple[DatabaseRepository, Path]:
    """Create an empty database and one production-shaped compressed archive."""
    repository = DatabaseRepository(f"sqlite:///{tmp_path / 'trade-restore.db'}")
    Base.metadata.create_all(repository.engine)
    archive_path = tmp_path / "trades-2026-05-04.csv.zst"
    _write_trade_zstd_archive(archive_path)
    return repository, archive_path


def _write_large_tick_archive(path: Path) -> int:
    """Write enough valid plain CSV rows to discriminate bounded batching."""
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle, lineterminator="\n")
        writer.writerow(EVENT_TABLES["ticks"].columns)
        for index in range(_LARGE_ARCHIVE_ROWS):
            writer.writerow(
                (
                    f"tick-{index}",
                    "2026-07-29T10:00:00+00:00",
                    "9999-12-31T23:59:59+00:00",
                    "session-1",
                    str(index),
                    "instrument-1",
                    "100.0",
                    "101.0",
                    "100.5",
                    "1.0",
                )
            )
    return path.stat().st_size


def _write_large_tick_zstd_archive(path: Path) -> tuple[int, int]:
    """Compress the large tick fixture without retaining its logical contents."""
    plain_path = path.with_suffix("")
    logical_size = _write_large_tick_archive(plain_path)
    with (
        plain_path.open("rb") as source,
        zstd.open(path, mode="wb") as destination,
    ):
        shutil.copyfileobj(source, destination, length=1024 * 1024)
    return path.stat().st_size, logical_size


def test_parse_str_csv() -> None:
    """Parse string CSV value."""
    assert _parse_str_csv("hello") == "hello"
    assert _parse_str_csv("") is None


def test_parse_datetime_csv() -> None:
    """Parse ISO datetime CSV value."""
    result = _parse_datetime_csv("2024-01-01T14:30:00+00:00")
    assert result == datetime(2024, 1, 1, 14, 30, tzinfo=UTC)
    assert _parse_datetime_csv("") is None


@pytest.mark.parametrize(
    ("value", "message"),
    [
        ("2024-01-01T14:30:00", "CSV datetime must include a UTC offset"),
        ("99999-12-31 00:59:59+01:00", "Invalid CSV datetime"),
        ("10000-01-01broken", "Invalid CSV datetime"),
        ("10000-01-01 00:59:58+01:00", "Unsupported overflow CSV datetime"),
        ("10000-01-01 00:59:59", "Unsupported overflow CSV datetime"),
    ],
    ids=[
        "naive",
        "non-sentinel-overflow-prefix",
        "malformed-sentinel-rendering",
        "wrong-sentinel-instant",
        "naive-sentinel-rendering",
    ],
)
def test_parse_datetime_csv_refuses_ambiguous_or_invalid_values(
    value: str,
    message: str,
) -> None:
    """Datetime parsing fails closed outside the exact offset-aware contract.

    Given: A naive, malformed, or non-sentinel overflow datetime,
    When: The archive datetime parser reads the value,
    Then: It raises the specific validation error instead of returning an instant.
    """
    with pytest.raises(ValueError, match=message):
        _parse_datetime_csv(value)


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


def test_build_column_parsers_unknown_column() -> None:
    """Build parsers raises ValueError for unknown column name.

    Given: Header with column not in model,
    When: _build_column_parsers is called,
    Then: Raises ValueError with column name.
    """
    with pytest.raises(ValueError, match="bogus_col"):
        _build_column_parsers(Tick, ["public_id", "bogus_col"])


def test_restore_malformed_csv_header(tmp_path: Path) -> None:
    """Restore fails gracefully when CSV has unknown columns.

    Given: CSV with a column name not in the model,
    When: restore is called,
    Then: Raises ValueError (not KeyError).
    """
    csv_path = tmp_path / "bad.csv"
    csv_path.write_text(
        "public_id,timestamp,known_to,bogus_column\n"
        "pub-1,2024-01-01T14:30:00+00:00,9999-12-31T23:59:59+00:00,oops\n",
        encoding="utf-8",
    )
    db_url = f"sqlite:///{tmp_path / 'bad.db'}"
    repo = DatabaseRepository(db_url)
    Base.metadata.create_all(repo.engine)
    restorer = ArchiveRestorer(repo)
    with pytest.raises(ValueError, match="bogus_column"):
        restorer.restore(table="ticks", paths=[csv_path])
    repo.dispose()


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


def test_restore_header_lookup_refuses_unregistered_table() -> None:
    """Header lookup has no permissive fallback for an unregistered table.

    Given: A table name absent from every archive table registry,
    When: The restore header contract is resolved directly,
    Then: It raises instead of inferring or returning a partial header.
    """
    with pytest.raises(ValueError, match="No archive header is defined"):
        _restore_headers_for_table("bogus")


def test_restore_rejects_zero_batch_size() -> None:
    """Restore batching rejects the zero boundary at construction time.

    Given: A repository and a requested batch size of zero,
    When: The archive restorer is constructed,
    Then: It raises before any file or database work can begin.
    """
    repository = MagicMock(spec=DatabaseRepository)

    with pytest.raises(ValueError, match="batch_size must be positive"):
        ArchiveRestorer(repository, batch_size=0)

    repository.get_session.assert_not_called()


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


def test_restore_executions_is_refused_loudly() -> None:
    """Execution restore fails closed with the named protocol error.

    The generic audit restore is a direct bulk insert that bypasses the
    per-wallet execution fence and the ``scope_sequence`` counter
    protocol — it could reintroduce a certified sequence at or below an
    existing anchor watermark. Until an anchor-aware offline
    reconstruction protocol exists, executions must never be restored,
    and the refusal must fire before any file or database work.

    Given: A restorer over a mocked repository,
    When: restore targets the ``executions`` table,
    Then: ``ExecutionRestoreUnsupportedError`` is raised naming the
        missing reconstruction protocol and the repository is untouched.
    """
    repo = MagicMock()
    restorer = ArchiveRestorer(repo)
    with pytest.raises(
        ExecutionRestoreUnsupportedError,
        match="anchor-aware offline reconstruction protocol",
    ):
        restorer.restore(table="executions", paths=[])
    repo.bulk_insert_from_archive.assert_not_called()
    repo.get_existing_archive_keys.assert_not_called()


def test_restore_refuses_transposed_trade_header(tmp_path: Path) -> None:
    """A plausible field transposition is refused before database access.

    Given: A production-shaped trade archive whose float-compatible price
        and size columns are transposed,
    When: The audit restorer validates the positional archive contract,
    Then: It raises a header mismatch instead of silently swapping values.
    """
    csv_path = tmp_path / "transposed.csv"
    header = list(_TRADE_ARCHIVE_HEADER)
    price_index = header.index("price")
    size_index = header.index("size")
    header[price_index], header[size_index] = header[size_index], header[price_index]
    row = list(_TRADE_ARCHIVE_ROW)
    row[2] = "9999-12-31T23:59:59+00:00"
    with csv_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle, lineterminator="\n")
        writer.writerow(header)
        writer.writerow(row)

    repository = MagicMock(spec=DatabaseRepository)
    repository.get_existing_archive_keys.return_value = set()
    repository.bulk_insert_from_archive.return_value = 1

    with pytest.raises(ValueError, match="CSV header mismatch"):
        ArchiveRestorer(repository).restore(table="trades", paths=[csv_path])

    repository.get_existing_archive_keys.assert_not_called()
    repository.bulk_insert_from_archive.assert_not_called()


def test_restore_refuses_unsupported_archive_extension(tmp_path: Path) -> None:
    """Restore accepts only the two explicitly supported archive extensions.

    Given: A readable archive-shaped file whose suffix is neither CSV nor CSV.zst,
    When: Audit restore attempts to open it,
    Then: It raises an extension error before querying or inserting database rows.
    """
    archive_path = tmp_path / "ticks.csv.gz"
    archive_path.write_text(",".join(EVENT_TABLES["ticks"].columns), encoding="utf-8")
    repository = MagicMock(spec=DatabaseRepository)

    with pytest.raises(ValueError, match="Unsupported archive extension"):
        ArchiveRestorer(repository).restore(table="ticks", paths=[archive_path])

    repository.get_existing_archive_keys.assert_not_called()
    repository.bulk_insert_from_archive.assert_not_called()


def test_restore_refuses_row_with_wrong_field_count(tmp_path: Path) -> None:
    """Every data row must have exactly the validated header width.

    Given: A tick archive with a complete header and a three-field data row,
    When: Audit restore streams the first batch,
    Then: It identifies line two and refuses the row before database access.
    """
    archive_path = tmp_path / "short-row.csv"
    header = EVENT_TABLES["ticks"].columns
    with archive_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle, lineterminator="\n")
        writer.writerow(header)
        writer.writerow(
            (
                "tick-1",
                "2026-07-29T10:00:00+00:00",
                "9999-12-31T23:59:59+00:00",
            )
        )
    repository = MagicMock(spec=DatabaseRepository)
    expected_message = f"CSV row 2 has 3 fields; expected {len(header)}"

    with pytest.raises(ValueError, match=expected_message):
        ArchiveRestorer(repository).restore(table="ticks", paths=[archive_path])

    repository.get_existing_archive_keys.assert_not_called()
    repository.bulk_insert_from_archive.assert_not_called()


@pytest.mark.parametrize(
    "empty_index",
    [0, 1, 2],
    ids=["public-id", "timestamp", "known-to"],
)
def test_restore_refuses_empty_temporal_identity(
    tmp_path: Path,
    empty_index: int,
) -> None:
    """Each field of the positional temporal identity is mandatory.

    Given: A complete tick row with one empty temporal identity field,
    When: Audit restore canonicalizes its deduplication key,
    Then: It raises the identity error before consulting the repository.
    """
    archive_path = tmp_path / f"empty-identity-{empty_index}.csv"
    row = [
        "tick-1",
        "2026-07-29T10:00:00+00:00",
        "9999-12-31T23:59:59+00:00",
        "session-1",
        "1",
        "instrument-1",
        "100.0",
        "101.0",
        "100.5",
        "1.0",
    ]
    row[empty_index] = ""
    with archive_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle, lineterminator="\n")
        writer.writerow(EVENT_TABLES["ticks"].columns)
        writer.writerow(row)
    repository = MagicMock(spec=DatabaseRepository)

    with pytest.raises(ValueError, match="CSV temporal identity fields must be non-empty"):
        ArchiveRestorer(repository).restore(table="ticks", paths=[archive_path])

    repository.get_existing_archive_keys.assert_not_called()
    repository.bulk_insert_from_archive.assert_not_called()


def test_existing_archive_key_lookup_short_circuits_empty_candidates() -> None:
    """An empty restore batch never issues an unconstrained database query.

    Given: No candidate temporal identities,
    When: The repository checks for existing archive keys,
    Then: It returns an empty set without opening a database session.
    """
    repository = MagicMock(spec=DatabaseRepository)

    result = DatabaseRepository.get_existing_archive_keys(repository, Tick, [])

    assert result == set()
    repository.get_session.assert_not_called()


def test_restore_compressed_trade_archive(tmp_path: Path) -> None:
    """A real-format zstd trade archive streams into typed database columns.

    Given: A compressed CSV using the immutable production header and
        PostgreSQL's offset rendering of the open-ended known-to sentinel,
    When: The trade archive is restored and then restored again,
    Then: One exact row is inserted and the rerun reports it already present.
    """
    repository, archive_path = _make_trade_restore_fixture(tmp_path)

    restorer = ArchiveRestorer(repository)
    result = restorer.restore(table="trades", paths=[archive_path])
    rerun = restorer.restore(table="trades", paths=[archive_path])

    assert result.rows_inserted == 1
    assert result.rows_skipped == 0
    assert rerun.rows_inserted == 0
    assert rerun.rows_skipped == 1
    with repository.get_session() as session:
        restored = session.execute(select(Trade)).scalar_one()
    assert restored.id == 30051988
    assert restored.public_id == _TRADE_ARCHIVE_ROW[0]
    assert restored.timestamp == datetime(2026, 6, 8, 9, 28, 24, 463041, tzinfo=UTC)
    assert restored.known_to == KNOWN_TO_MAX
    assert restored.executed_at == datetime(2026, 5, 4, 15, 51, 56, 480000, tzinfo=UTC)
    assert restored.price == 4.988
    assert restored.size == 1.0
    repository.dispose()


def test_restore_trade_archive_is_idempotent(tmp_path: Path) -> None:
    """A second offset-timestamp restore reports every archived row as present.

    Given: A production-shaped trade archive whose timestamp uses a non-UTC offset,
    When: The same archive is restored again after the first batch commits,
    Then: The rerun inserts zero rows and reports the entire file as skipped.
    """
    repository = DatabaseRepository(f"sqlite:///{tmp_path / 'idempotent-restore.db'}")
    Base.metadata.create_all(repository.engine)
    archive_path = tmp_path / "trades-2026-05-04.csv"
    _write_trade_csv_archive(archive_path)
    restorer = ArchiveRestorer(repository)

    first = restorer.restore(table="trades", paths=[archive_path])
    second = restorer.restore(table="trades", paths=[archive_path])

    assert first.rows_inserted == 1
    assert first.rows_skipped == 0
    assert second.rows_inserted == 0
    assert second.rows_skipped == 1
    with repository.get_session() as session:
        assert len(session.execute(select(Trade)).scalars().all()) == 1
    repository.dispose()


@pytest.mark.parametrize("compressed", [False, True], ids=["plain", "zstd"])
def test_restore_peak_allocation_is_bounded(
    tmp_path: Path,
    compressed: bool,
) -> None:
    """Large plain and compressed archives avoid file-sized live allocations.

    Given: A valid 100,000-row plain or zstd archive and a discarding repository,
    When: Restore allocation is measured independently of fixture generation,
    Then: Every row is processed while peak traced memory remains below 48 MiB.
    """
    if compressed:
        csv_path = tmp_path / "large-ticks.csv.zst"
        fixture_size, logical_size = _write_large_tick_zstd_archive(csv_path)
    else:
        csv_path = tmp_path / "large-ticks.csv"
        fixture_size = _write_large_tick_archive(csv_path)
        logical_size = fixture_size
    repository = _DiscardingArchiveRepository()
    restorer = ArchiveRestorer(cast(DatabaseRepository, repository))

    tracemalloc.stop()
    tracemalloc.start()
    try:
        result = restorer.restore(table="ticks", paths=[csv_path])
        _, peak_bytes = tracemalloc.get_traced_memory()
    finally:
        tracemalloc.stop()

    assert fixture_size > 0
    assert logical_size > 10 * 1024 * 1024
    assert result.rows_inserted == _LARGE_ARCHIVE_ROWS
    assert result.rows_skipped == 0
    assert repository.rows_inserted == _LARGE_ARCHIVE_ROWS
    assert peak_bytes < _MAX_RESTORE_PEAK_BYTES
    assert repository.batch_sizes == [10_000] * 10


def test_restore_uses_configured_batch_size(tmp_path: Path) -> None:
    """A caller-selected batch size bounds each repository insertion.

    Given: A five-row archive and a requested batch size of two,
    When: The archive is restored,
    Then: The repository receives two full batches and one final partial batch.
    """
    csv_path = tmp_path / "configured-batches.csv"
    with csv_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle, lineterminator="\n")
        writer.writerow(EVENT_TABLES["ticks"].columns)
        for index in range(5):
            writer.writerow(
                (
                    f"tick-{index}",
                    "2026-07-29T10:00:00+00:00",
                    "9999-12-31T23:59:59+00:00",
                    "session-1",
                    str(index),
                    "instrument-1",
                    "100.0",
                    "101.0",
                    "100.5",
                    "1.0",
                )
            )
    repository = _DiscardingArchiveRepository()

    result = ArchiveRestorer(
        cast(DatabaseRepository, repository),
        batch_size=2,
    ).restore(table="ticks", paths=[csv_path])

    assert result.rows_inserted == 5
    assert repository.batch_sizes == [2, 2, 1]


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
    csv_path.write_text(
        f"{','.join(EVENT_TABLES['ticks'].columns)}\n",
        encoding="utf-8",
    )
    repo = MagicMock()
    restorer = ArchiveRestorer(repo)
    result = restorer.restore(table="ticks", paths=[csv_path])
    assert result.rows_inserted == 0
    assert result.files_processed == 1


def test_restore_refuses_missing_header(tmp_path: Path) -> None:
    """Restore refuses a headerless file before database access.

    Given: A truncated archive with no CSV header,
    When: Restore validates the table's exact positional contract,
    Then: It raises a missing-header error without querying or inserting rows.
    """
    csv_path = tmp_path / "empty.csv"
    csv_path.write_text("", encoding="utf-8")
    repo = MagicMock()
    restorer = ArchiveRestorer(repo)
    with pytest.raises(ValueError, match="missing CSV header"):
        restorer.restore(table="ticks", paths=[csv_path])
    repo.get_existing_archive_keys.assert_not_called()
    repo.bulk_insert_from_archive.assert_not_called()


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


def _make_db_with_candle_versions(tmp_path: Path) -> tuple[DatabaseRepository, Path, str]:
    """Create DB with closed + active Candle versions, export, and return test fixtures."""
    db_url = f"sqlite:///{tmp_path / 'restore_candles.db'}"
    repo = DatabaseRepository(db_url)
    Base.metadata.create_all(repo.engine)
    ts = datetime(2024, 1, 1, tzinfo=UTC)
    inst_id = "inst-btc"
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
            public_id=inst_id,
            symbol_public_id="sym-btc",
            exchange="polygon",
            session_id="test",
            sequence_id=2,
            timestamp=ts,
        )
        session.add(inst)
        session.flush()
        closed = Candle(
            public_id="candle-1",
            instrument_public_id=inst_id,
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
        session.add(closed)
        session.flush()
        active = Candle(
            public_id="candle-1",
            instrument_public_id=inst_id,
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
        session.add(active)
        session.commit()

    csv_dir = tmp_path / "export"
    archiver = CandleAuditArchiver(repo, csv_dir)
    archiver.export(
        timeframe="1m",
        day_start=date(2024, 1, 1),
        day_end=date(2024, 1, 1),
    )
    return repo, csv_dir, inst_id


def test_round_trip_purge_restore_state(tmp_path: Path) -> None:
    """Round-trip: export state -> purge -> restore -> verify identical.

    Given: DB with closed + active setting versions,
    When: export all -> purge closed -> restore from CSV,
    Then: Every temporal and domain field is restored unchanged.
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
    assert [row[1:] for row in rows_after_restore] == [row[1:] for row in rows_before]
    repo.dispose()


def test_round_trip_purge_restore_events(tmp_path: Path) -> None:
    """Round-trip: export ticks -> purge -> restore -> verify identical.

    Given: DB with ticks,
    When: export -> purge -> restore,
    Then: Every temporal and domain field is restored unchanged.
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
    assert [row[1:] for row in rows_restored] == [row[1:] for row in rows_before]
    repo.dispose()


def test_round_trip_purge_restore_candle_audit(tmp_path: Path) -> None:
    """Round-trip: export candle audit -> purge closed -> restore -> verify identical."""
    repo, csv_dir, inst_id = _make_db_with_candle_versions(tmp_path)

    rows_before = repo.get_candle_versions_for_archive(
        inst_id,
        "1m",
        date(2024, 1, 1),
        date(2024, 1, 1),
    )
    assert len(rows_before) == 2

    archiver = CandleAuditArchiver(repo, csv_dir)
    archiver.export(
        timeframe="1m",
        day_start=date(2024, 1, 1),
        day_end=date(2024, 1, 1),
        closed_only=True,
        purge=True,
    )
    rows_after_purge = repo.get_candle_versions_for_archive(
        inst_id,
        "1m",
        date(2024, 1, 1),
        date(2024, 1, 1),
    )
    assert len(rows_after_purge) == 1
    assert rows_after_purge[0][3] == KNOWN_TO_MAX

    csv_files = sorted(csv_dir.rglob("*.csv"))
    restorer = ArchiveRestorer(repo)
    result = restorer.restore(table="candles", paths=csv_files)
    assert result.rows_inserted == 1

    rows_after_restore = repo.get_candle_versions_for_archive(
        inst_id,
        "1m",
        date(2024, 1, 1),
        date(2024, 1, 1),
    )
    assert [row[1:] for row in rows_after_restore] == [row[1:] for row in rows_before]
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
    """CLI restore finds plain and compressed CSV files recursively.

    Given: Directory with plain and zstd CSV files,
    When: CLI invoked with --dir,
    Then: All CSV files passed to restorer.
    """
    sub = tmp_path / "archive" / "ticks" / "2024"
    sub.mkdir(parents=True)
    (sub / "2024-01-01.csv").write_text("public_id,timestamp,known_to\n")
    (sub / "2024-01-02.csv").write_text("public_id,timestamp,known_to\n")
    (sub / "2024-01-03.csv.zst").write_bytes(b"fixture")
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
    assert len(call_kwargs["paths"]) == 3
