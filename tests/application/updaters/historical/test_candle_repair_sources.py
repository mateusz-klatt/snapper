"""Verify how the candle repair driver turns snapshot files and stored trades into source bars."""

import gzip
import hashlib
import json
import os
from collections.abc import AsyncIterator
from datetime import UTC
from datetime import datetime
from datetime import timedelta
from datetime import timezone
from pathlib import Path
from types import ModuleType
from types import SimpleNamespace
from unittest.mock import patch

import pytest
import pytest_asyncio
from sqlalchemy import update

from snapper.application.updaters.historical import candle_repair
from snapper.application.updaters.historical import trade_candle_backfill
from snapper.application.updaters.historical.candle_repair import RepairRefusedError
from snapper.application.updaters.historical.candle_repair import RepairSource
from snapper.application.updaters.historical.candle_repair import RepairWindow
from snapper.application.updaters.historical.candle_repair import ResolvedInstrument
from snapper.application.updaters.historical.candle_repair import SnapshotFile
from snapper.application.updaters.historical.candle_repair import canonical_json
from snapper.application.updaters.historical.candle_repair import code_revision
from snapper.application.updaters.historical.candle_repair import database_endpoint
from snapper.application.updaters.historical.candle_repair import gather_sources
from snapper.application.updaters.historical.candle_repair import kraken_rest_aliases
from snapper.application.updaters.historical.candle_repair import merge_snapshot_minutes
from snapper.application.updaters.historical.candle_repair import parse_snapshot_file
from snapper.application.updaters.historical.candle_repair import prepare_output_dir
from snapper.application.updaters.historical.candle_repair import read_snapshot
from snapper.application.updaters.historical.candle_repair import refuse_live_edge
from snapper.application.updaters.historical.candle_repair import request_windows
from snapper.application.updaters.historical.candle_repair import trade_candles
from snapper.application.updaters.historical.candle_repair import write_exclusive
from snapper.application.updaters.historical.trade_candle_backfill import TradeCandleBackfillService
from snapper.core.types import ExchangeEnum
from snapper.data.models import SymbolAlias
from snapper.data.repository import SQLAlchemyRepository
from snapper.data.repository_types import CandleUpsertRow
from snapper.infrastructure.exchanges.contracts import CandleUpdate
from snapper.infrastructure.exchanges.kraken_closed_candles import ClosedCandleRequest
from tests.helpers.candle_repair import BTC
from tests.helpers.candle_repair import ETH
from tests.helpers.candle_repair import FETCHED_AT
from tests.helpers.candle_repair import OPEN
from tests.helpers.candle_repair import RUN_TIME
from tests.helpers.candle_repair import XRP
from tests.helpers.candle_repair import kraken_row
from tests.helpers.candle_repair import kraken_rows
from tests.helpers.candle_repair import seed_instrument
from tests.helpers.candle_repair import snapshot_bytes
from tests.helpers.candle_repair import snapshot_request
from tests.helpers.candle_repair import trade
from tests.helpers.candle_repair import trades_request
from tests.helpers.candle_repair import write_snapshot

_WINDOW = RepairWindow(OPEN - timedelta(hours=1), OPEN + timedelta(hours=1))
_PLACEHOLDER: list[object] = [
    0,
    "0.00000",
    "0.00000",
    "0.00000",
    "0.00000",
    "0.00000",
    "0.00000000",
    0,
]


@pytest_asyncio.fixture
async def repository(tmp_path: Path) -> AsyncIterator[SQLAlchemyRepository]:
    """Provide a disposable file-backed repository with three Kraken symbols."""
    repo = SQLAlchemyRepository(f"sqlite+aiosqlite:///{tmp_path / 'repair-sources.db'}")
    await repo.create_all()
    await seed_instrument(repo, XRP, "XRP-EUR", "XXRPZEUR")
    await seed_instrument(repo, BTC, "BTC-EUR", "XXBTZEUR")
    await seed_instrument(repo, ETH, "ETH-EUR", "ETH/EUR", channel="ws")
    try:
        yield repo
    finally:
        await repo.engine.dispose()


def _parse(rows: list[list[object]], fetched_at: object = FETCHED_AT.timestamp()) -> SnapshotFile:
    """Parse one in-memory response for the XRP pair over the usual window."""
    data = snapshot_bytes("XXRPZEUR", rows, fetched_at)
    return parse_snapshot_file("ohlc/XXRPZEUR.json.gz", data, _WINDOW, RUN_TIME)


def _minutes(file: SnapshotFile) -> list[datetime]:
    """Return the opening minutes of a file's bars."""
    return [candle.interval_begin for candle in file.candles]


@pytest.mark.parametrize(
    ("start", "end", "message"),
    [
        (datetime(2026, 11, 10, 15, 0), datetime(2026, 11, 10, 16, 0, tzinfo=UTC), "UTC minute"),
        (
            datetime(2026, 11, 10, 15, 0, tzinfo=timezone(timedelta(hours=2))),
            datetime(2026, 11, 10, 16, 0, tzinfo=UTC),
            "UTC minute",
        ),
        (
            datetime(2026, 11, 10, 15, 0, tzinfo=UTC),
            datetime(2026, 11, 10, 16, 0, 1, tzinfo=UTC),
            "UTC minute",
        ),
        (
            datetime(2026, 11, 10, 15, 0, tzinfo=UTC),
            datetime(2026, 11, 10, 16, 0, 0, 5, tzinfo=UTC),
            "UTC minute",
        ),
        (
            datetime(2026, 11, 10, 16, 0, tzinfo=UTC),
            datetime(2026, 11, 10, 16, 0, tzinfo=UTC),
            "precede",
        ),
    ],
)
def test_window_refuses_naive_offset_unaligned_and_empty_bounds(
    start: datetime, end: datetime, message: str
) -> None:
    """Only a non-empty window between UTC minutes is a repair window.

    Given: A naive, a non-UTC, a seconds, a microseconds or an empty window.
    When: The window is built.
    Then: It is refused.
    """
    with pytest.raises(RepairRefusedError, match=message):
        RepairWindow(start, end)


def test_window_splits_at_utc_midnights() -> None:
    """Day windows are clipped to the repair window at both ends.

    Given: A window from 22:00 on one day to 01:30 two days later.
    When: It is split into days.
    Then: The three day windows cover it exactly.
    """
    window = RepairWindow(
        datetime(2026, 11, 8, 22, 0, tzinfo=UTC), datetime(2026, 11, 10, 1, 30, tzinfo=UTC)
    )
    assert window.days() == [
        (datetime(2026, 11, 8, 22, 0, tzinfo=UTC), datetime(2026, 11, 9, tzinfo=UTC)),
        (datetime(2026, 11, 9, tzinfo=UTC), datetime(2026, 11, 10, tzinfo=UTC)),
        (datetime(2026, 11, 10, tzinfo=UTC), datetime(2026, 11, 10, 1, 30, tzinfo=UTC)),
    ]
    assert window.to_json() == {
        "start": "2026-11-08T22:00:00+00:00",
        "end": "2026-11-10T01:30:00+00:00",
    }


@pytest.mark.parametrize(
    ("minutes", "expected"),
    [
        (0, []),
        (719, [(0, 719)]),
        (720, [(0, 719), (719, 720)]),
        (1440, [(0, 719), (719, 1438), (1438, 1440)]),
    ],
)
def test_request_windows_never_exceed_719_minutes(
    minutes: int, expected: list[tuple[int, int]]
) -> None:
    """A file's window is cut into consecutive parser windows of at most 719 minutes.

    Given: Spans of 0, 719, 720 and 1440 minutes.
    When: They are split.
    Then: The windows are consecutive and none is longer than 719 minutes.
    """
    windows = request_windows(OPEN, OPEN + timedelta(minutes=minutes))
    assert windows == [
        (OPEN + timedelta(minutes=lower), OPEN + timedelta(minutes=upper))
        for lower, upper in expected
    ]


def test_live_edge_refuses_minutes_newer_than_180_minutes_before_the_run() -> None:
    """A window may end at most 180 minutes before the run.

    Given: Windows ending exactly at and one minute after the edge.
    When: The edge is checked.
    Then: The first passes and the second is refused.
    """
    edge = RUN_TIME - timedelta(minutes=180)
    refuse_live_edge(RepairWindow(edge - timedelta(hours=1), edge), RUN_TIME)
    with pytest.raises(RepairRefusedError, match="180 minutes"):
        refuse_live_edge(
            RepairWindow(edge - timedelta(hours=1), edge + timedelta(minutes=1)), RUN_TIME
        )


def test_database_endpoint_names_postgresql_without_credentials_or_query() -> None:
    """The endpoint keeps dialect, host, effective port and database only.

    Given: PostgreSQL URLs with credentials and a query, with and without a port.
    When: Their endpoints are taken.
    Then: The port defaults to 5432 and no user, password or query appears.
    """
    with_port = database_endpoint(
        "postgresql+asyncpg://operator:secret@db.example:6543/snapper?application_name=x"
    )
    without_port = database_endpoint("postgresql+asyncpg://operator:secret@db.example/snapper")
    assert with_port == {
        "dialect": "postgresql",
        "host": "db.example",
        "port": 6543,
        "database": "snapper",
    }
    assert without_port["port"] == 5432
    assert "secret" not in canonical_json(with_port).decode()
    assert "operator" not in canonical_json(with_port).decode()
    assert database_endpoint("postgresql+asyncpg://") == {
        "dialect": "postgresql",
        "host": "",
        "port": 5432,
        "database": "",
    }


def test_database_endpoint_resolves_the_sqlite_file_and_refuses_others(tmp_path: Path) -> None:
    """SQLite is named by its canonical absolute path; memory and other dialects refuse.

    Given: A SQLite URL through a symbolic link, an in-memory URL and a MySQL URL.
    When: Their endpoints are taken.
    Then: The link resolves to the real file and the other two are refused.
    """
    real = tmp_path / "real.db"
    real.touch()
    link = tmp_path / "link.db"
    link.symlink_to(real)
    assert database_endpoint(f"sqlite+aiosqlite:///{link}") == {
        "dialect": "sqlite",
        "path": str(real.resolve()),
    }
    with pytest.raises(RepairRefusedError, match="database file"):
        database_endpoint("sqlite+aiosqlite:///:memory:")
    with pytest.raises(RepairRefusedError, match="database file"):
        database_endpoint("sqlite+aiosqlite://")
    with pytest.raises(RepairRefusedError, match="mysql"):
        database_endpoint("mysql+aiomysql://user:secret@host/db")


def test_code_revision_hashes_every_installed_module_that_decides_writes() -> None:
    """The revision holds the sha256 of each named source file, keyed by package path.

    Given: An extra module.
    When: The revision is taken.
    Then: The guards, parser, builder, row builder, driver and extra module are hashed
        from their installed files.
    """
    extra = ModuleType("snapper.cli.example")
    extra.__file__ = __file__
    revision = code_revision([extra])
    assert sorted(revision) == [
        "snapper/application/updaters/historical/candle_repair.py",
        "snapper/application/updaters/historical/trade_candle_rows.py",
        "snapper/cli/example.py",
        "snapper/data/repository.py",
        "snapper/infrastructure/exchanges/_trade_candle_builder.py",
        "snapper/infrastructure/exchanges/kraken_closed_candles.py",
    ]
    assert (
        revision["snapper/cli/example.py"]
        == hashlib.sha256(Path(__file__).read_bytes()).hexdigest()
    )
    assert revision["snapper/application/updaters/historical/candle_repair.py"] == (
        hashlib.sha256(Path(str(candle_repair.__file__)).read_bytes()).hexdigest()
    )


def test_output_dir_is_created_or_must_be_an_empty_directory(tmp_path: Path) -> None:
    """The output directory is new or empty, never a file or a used directory.

    Given: A missing path, an empty directory, a used directory and a file.
    When: Each is prepared.
    Then: The first two are accepted, the first created, and the others refused.
    """
    created = tmp_path / "new"
    prepare_output_dir(created)
    assert created.is_dir()
    prepare_output_dir(created)
    (created / "preview.csv").write_text("x")
    with pytest.raises(RepairRefusedError, match="empty"):
        prepare_output_dir(created)
    with pytest.raises(RepairRefusedError, match="not a directory"):
        prepare_output_dir(created / "preview.csv")


def test_files_are_created_exclusively_with_owner_only_mode(tmp_path: Path) -> None:
    """Every output file is new and readable by its owner only.

    Given: A permissive umask.
    When: A file is written, then written again.
    Then: It has mode 0600 and the second write is refused.
    """
    previous = os.umask(0)
    try:
        write_exclusive(tmp_path / "approval.json", b"{}")
    finally:
        os.umask(previous)
    assert (tmp_path / "approval.json").stat().st_mode & 0o777 == 0o600
    with pytest.raises(FileExistsError):
        write_exclusive(tmp_path / "approval.json", b"{}")


def test_canonical_json_sorts_keys_without_whitespace_and_refuses_nan() -> None:
    """Canonical JSON is byte-stable and finite.

    Given: An object with unsorted keys and a float.
    When: It is serialized, and a NaN is serialized.
    Then: The keys are sorted without spaces, and the NaN is refused.
    """
    assert canonical_json({"b": 1.5, "a": [1, "x"]}) == b'{"a":[1,"x"],"b":1.5}'
    with pytest.raises(ValueError):
        canonical_json({"a": float("nan")})


def test_full_response_drops_the_open_row_and_untraded_minutes() -> None:
    """Only closed minutes with trades become bars.

    Given: Ten closed minutes, one with a count of 0, and the open eleventh minute.
    When: The file is parsed.
    Then: The nine traded closed minutes are bars and the open minute is excluded.
    """
    rows = kraken_rows(OPEN, 10)
    rows[5] = kraken_row(OPEN + timedelta(minutes=5), 0, volume=0.0)
    rows.append(kraken_row(OPEN + timedelta(minutes=10), 4))
    file = _parse(rows)
    assert file.error is None
    assert file.fetched_at == FETCHED_AT.timestamp()
    assert file.pair == "XXRPZEUR"
    assert (
        file.sha256
        == hashlib.sha256(snapshot_bytes("XXRPZEUR", rows, FETCHED_AT.timestamp())).hexdigest()
    )
    assert _minutes(file) == [
        OPEN + timedelta(minutes=offset) for offset in range(10) if offset != 5
    ]
    assert all(candle.trades == 3 for candle in file.candles)


def test_open_row_is_excluded_against_fetched_at_even_inside_the_window() -> None:
    """The last row is the interval still open at ``fetched_at`` and never a bar.

    Given: A response whose last row's minute ended before the window end.
    When: The file is parsed.
    Then: That row is not a bar although its minute lies inside the window.
    """
    rows = kraken_rows(OPEN, 3)
    file = _parse(rows, (OPEN + timedelta(minutes=3)).timestamp())
    assert _minutes(file) == [OPEN, OPEN + timedelta(minutes=1)]


def test_window_longer_than_719_minutes_is_parsed_in_two_requests(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A full response of 720 closed minutes needs two parser windows.

    Given: 720 closed traded minutes plus the open row, and a window covering them all.
    When: The file is parsed.
    Then: The parser runs on 719 and 1 minutes with observed_at at fetched_at, and all
        720 minutes are bars.
    """
    start = OPEN - timedelta(hours=12)
    rows = kraken_rows(start, 721)
    fetched_at = start + timedelta(minutes=720, seconds=30)
    requests: list[ClosedCandleRequest] = []
    original = candle_repair.parse_closed_candles

    def recording(payload: object, request: ClosedCandleRequest) -> list[CandleUpdate]:
        """Record each request before parsing it."""
        requests.append(request)
        return original(payload, request)

    monkeypatch.setattr(candle_repair, "parse_closed_candles", recording)
    window = RepairWindow(start - timedelta(hours=1), OPEN + timedelta(hours=1))
    file = parse_snapshot_file(
        "ohlc/XXRPZEUR.json.gz",
        snapshot_bytes("XXRPZEUR", rows, fetched_at.timestamp()),
        window,
        RUN_TIME,
    )
    assert [(request.start, request.end) for request in requests] == [
        (start, start + timedelta(minutes=719)),
        (start + timedelta(minutes=719), start + timedelta(minutes=720)),
    ]
    assert {request.observed_at for request in requests} == {fetched_at}
    assert len(file.candles) == 720


def test_empty_row_list_is_valid_and_gives_no_bar() -> None:
    """A tokenized response without rows is a valid empty answer.

    Given: A response with an empty row list.
    When: The file is parsed.
    Then: It is valid and has no bar.
    """
    file = _parse([])
    assert file.error is None
    assert file.candles == ()


def test_untraded_placeholder_row_is_valid_and_gives_no_1970_bar() -> None:
    """The lone all-zero row at minute zero means the pair did not trade.

    Given: A response holding only the placeholder row.
    When: The file is parsed.
    Then: It is valid, has no bar, and nothing is dated 1970.
    """
    file = _parse([_PLACEHOLDER])
    assert file.error is None
    assert file.candles == ()


def test_file_starting_at_any_minute_with_gaps_gives_only_present_traded_minutes() -> None:
    """A missing minute is unknown, so only rows present in the file become bars.

    Given: Rows at 15:03, 15:04 and 15:07, the open row at 15:08, and a gap in between.
    When: The file is parsed.
    Then: Exactly the three present closed minutes are bars.
    """
    rows = [kraken_row(OPEN + timedelta(minutes=offset), 2) for offset in (3, 4, 7, 8)]
    file = _parse(rows, (OPEN + timedelta(minutes=8, seconds=10)).timestamp())
    assert _minutes(file) == [OPEN + timedelta(minutes=offset) for offset in (3, 4, 7)]


@pytest.mark.parametrize(
    ("window", "fetched_at"),
    [
        (RepairWindow(OPEN - timedelta(hours=2), OPEN - timedelta(hours=1)), FETCHED_AT),
        (RepairWindow(OPEN + timedelta(hours=1), OPEN + timedelta(hours=2)), FETCHED_AT),
    ],
)
def test_clipped_window_outside_the_repair_window_gives_no_bar(
    window: RepairWindow, fetched_at: datetime
) -> None:
    """A file whose rows lie outside the repair window contributes nothing.

    Given: A repair window that ends before the oldest row, or starts after the fetch.
    When: The file is parsed.
    Then: It is valid and has no bar.
    """
    rows = [*kraken_rows(OPEN, 5), kraken_row(OPEN + timedelta(minutes=5), 1)]
    data = snapshot_bytes("XXRPZEUR", rows, fetched_at.timestamp())
    file = parse_snapshot_file("ohlc/XXRPZEUR.json.gz", data, window, RUN_TIME)
    assert file.error is None
    assert file.candles == ()


def _envelope(change: dict[str, object]) -> bytes:
    """Encode a usual response with top-level fields replaced."""
    rows = [*kraken_rows(OPEN, 2), kraken_row(OPEN + timedelta(minutes=2), 1)]
    payload: dict[str, object] = {
        "error": [],
        "result": {"XXRPZEUR": rows, "last": 0},
        "fetched_at": FETCHED_AT.timestamp(),
    }
    return gzip.compress(json.dumps(payload | change).encode())


@pytest.mark.parametrize(
    ("data", "message"),
    [
        (b"not gzip", "BadGzipFile"),
        (gzip.compress(b"{not json"), "JSONDecodeError"),
        (gzip.compress(b"[]"), "not a JSON object"),
        (_envelope({"error": ["EGeneral:Too many requests"]}), "reports an error"),
        (_envelope({"fetched_at": True}), "finite number"),
        (_envelope({"fetched_at": "1762787430"}), "finite number"),
        (_envelope({"fetched_at": float("nan")}), "finite number"),
        (_envelope({"fetched_at": float("inf")}), "finite number"),
        (_envelope({"fetched_at": (RUN_TIME + timedelta(seconds=1)).timestamp()}), "later than"),
        (_envelope({"result": {"XXRPZEUR": [], "last": 0, "other": []}}), "exactly the pair"),
        (_envelope({"result": {"XRPEUR": [], "last": 0}}), "exactly the pair"),
        (_envelope({"result": []}), "exactly the pair"),
        (_envelope({"result": {"XXRPZEUR": {}, "last": 0}}), "not a list"),
        (_envelope({"result": {"XXRPZEUR": [[1, 2]], "last": 0}}), "eight-field"),
        (
            _envelope({"result": {"XXRPZEUR": [["1762787400", *_PLACEHOLDER[1:]]], "last": 0}}),
            "eight-field",
        ),
        (
            _envelope(
                {
                    "result": {
                        "XXRPZEUR": [kraken_row(FETCHED_AT + timedelta(minutes=1), 1)],
                        "last": 0,
                    }
                }
            ),
            "newer than",
        ),
        (
            _envelope(
                {
                    "result": {
                        "XXRPZEUR": [
                            [int(OPEN.timestamp()), "1.0", "1.2", "1.1", "1.0", "1.0", "1.0", 1],
                            kraken_row(OPEN + timedelta(minutes=1), 1),
                        ],
                        "last": 0,
                    }
                }
            ),
            "price bounds",
        ),
    ],
)
def test_invalid_files_are_reported_with_their_error(data: bytes, message: str) -> None:
    """A file that is not a valid saved response is invalid, never a source of bars.

    Given: A damaged archive, broken JSON, a wrong envelope, a bad acquisition time,
        a wrong identity, malformed rows, a row newer than the fetch or a bad bar.
    When: The file is parsed.
    Then: It is invalid with a short error and has neither bars nor acquisition time.
    """
    file = parse_snapshot_file("ohlc/XXRPZEUR.json.gz", data, _WINDOW, RUN_TIME)
    assert file.error is not None and message in file.error
    assert len(file.error) <= 200
    assert file.candles == ()
    assert file.fetched_at is None
    assert file.sha256 == hashlib.sha256(data).hexdigest()


def test_snapshot_reads_both_folders_in_path_order_and_ignores_other_files(
    tmp_path: Path,
) -> None:
    """Every ``*.json.gz`` file of both folders is read, nothing else.

    Given: Files in both folders, a stray text file and a subdirectory.
    When: The snapshot is read.
    Then: The two response files are read with their relative paths and pair keys.
    """
    rows = [*kraken_rows(OPEN, 2), kraken_row(OPEN + timedelta(minutes=2), 1)]
    write_snapshot(tmp_path, "XXRPZEUR", rows)
    write_snapshot(tmp_path, "AAPLxUSD", rows, folder="ohlc_tokenized")
    (tmp_path / "ohlc" / "notes.txt").write_text("ignored")
    (tmp_path / "ohlc" / "nested.json.gz").mkdir()
    files = read_snapshot(tmp_path, _WINDOW, RUN_TIME)
    assert [(file.path, file.pair) for file in files] == [
        ("ohlc/XXRPZEUR.json.gz", "XXRPZEUR"),
        ("ohlc_tokenized/AAPLxUSD.json.gz", "AAPLxUSD"),
    ]
    assert [file.error for file in files] == [None, None]


def test_snapshot_with_one_folder_is_read_and_without_folders_refused(tmp_path: Path) -> None:
    """One of the two folders suffices; a directory without either is refused.

    Given: A snapshot with only ``ohlc_tokenized/`` and a directory without folders.
    When: Each is read.
    Then: The first gives its file and the second is refused.
    """
    write_snapshot(tmp_path / "snap", "AAPLxUSD", [], folder="ohlc_tokenized")
    assert [file.path for file in read_snapshot(tmp_path / "snap", _WINDOW, RUN_TIME)] == [
        "ohlc_tokenized/AAPLxUSD.json.gz"
    ]
    with pytest.raises(RepairRefusedError, match="neither"):
        read_snapshot(tmp_path, _WINDOW, RUN_TIME)


@pytest.mark.asyncio
async def test_aliases_map_only_exact_active_kraken_rest_names(
    repository: SQLAlchemyRepository,
) -> None:
    """A pair key maps through an active REST alias of exactly that name.

    Given: Two REST aliases, a WebSocket alias and one REST alias closed before the run.
    When: Pair keys, including a legacy-normalized spelling, are resolved.
    Then: Only the exact active REST names map, to their symbol and instrument.
    """
    async with repository.session() as session:
        await session.execute(
            update(SymbolAlias)
            .where(SymbolAlias.exchange_symbol == "XXBTZEUR")
            .values(known_to=RUN_TIME - timedelta(days=1))
        )
        await session.commit()
    mapping = await kraken_rest_aliases(
        repository, {"XXRPZEUR", "XRPEUR", "XXBTZEUR", "ETH/EUR"}, RUN_TIME
    )
    assert mapping == {"XXRPZEUR": {ResolvedInstrument(XRP, "XRP-EUR")}}


@pytest.mark.asyncio
async def test_snapshot_sources_classify_files_and_select_requested_symbols(
    repository: SQLAlchemyRepository, tmp_path: Path
) -> None:
    """Valid mapped files contribute; invalid and unmapped ones are reported and skipped.

    Given: Files for XRP and BTC, an unmapped key and an invalid file.
    When: XRP alone, then every mapped symbol, is requested.
    Then: The requested files contribute their bars and the others are reported.
    """
    rows = [*kraken_rows(OPEN, 3), kraken_row(OPEN + timedelta(minutes=3), 1)]
    write_snapshot(tmp_path, "XXRPZEUR", rows)
    write_snapshot(tmp_path, "XXBTZEUR", rows)
    write_snapshot(tmp_path, "ZZZZZEUR", rows)
    (tmp_path / "ohlc" / "BROKEN.json.gz").write_bytes(b"broken")

    only_xrp = await gather_sources(
        repository, snapshot_request(tmp_path, tmp_path / "out"), RUN_TIME
    )
    every = await gather_sources(
        repository, snapshot_request(tmp_path, tmp_path / "out", symbols=()), RUN_TIME
    )

    assert only_xrp.instruments == (ResolvedInstrument(XRP, "XRP-EUR"),)
    assert [file.path for file in only_xrp.contributing] == ["ohlc/XXRPZEUR.json.gz"]
    assert [(minute.instrument.symbol, minute.known_at) for minute in only_xrp.minutes] == [
        ("XRP-EUR", FETCHED_AT)
    ] * 3
    assert only_xrp.unmapped == ("ohlc/ZZZZZEUR.json.gz",)
    assert [file.path for file in only_xrp.invalid] == ["ohlc/BROKEN.json.gz"]
    assert only_xrp.ambiguous == ()
    assert every.instruments == (
        ResolvedInstrument(XRP, "XRP-EUR"),
        ResolvedInstrument(BTC, "BTC-EUR"),
    )
    assert len(every.minutes) == 6


@pytest.mark.asyncio
async def test_requested_symbol_without_a_mapped_file_refuses(
    repository: SQLAlchemyRepository, tmp_path: Path
) -> None:
    """A symbol the operator asked for must have a valid mapped file.

    Given: A snapshot holding only the XRP file.
    When: BTC is requested as well.
    Then: The run refuses and names BTC.
    """
    write_snapshot(tmp_path, "XXRPZEUR", [])
    request = snapshot_request(tmp_path, tmp_path / "out", symbols=("BTC-EUR", "XRP-EUR"))
    with pytest.raises(RepairRefusedError, match="BTC-EUR"):
        await gather_sources(repository, request, RUN_TIME)


@pytest.mark.asyncio
async def test_pair_key_that_maps_to_several_instruments_is_ambiguous_and_skipped(
    repository: SQLAlchemyRepository, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A pair key mapping to more than one instrument maps to none.

    Given: An alias read that returns two instruments for the XRP key.
    When: Every mapped symbol is requested.
    Then: The file is reported as ambiguous and contributes nothing.
    """
    write_snapshot(tmp_path, "XXRPZEUR", [])

    async def two_instruments(
        repo: SQLAlchemyRepository, pairs: set[str], as_of: datetime
    ) -> dict[str, set[ResolvedInstrument]]:
        """Return two instruments for the XRP key."""
        del repo, pairs, as_of
        return {
            "XXRPZEUR": {ResolvedInstrument(XRP, "XRP-EUR"), ResolvedInstrument(BTC, "XRP-EUR")}
        }

    monkeypatch.setattr(candle_repair, "kraken_rest_aliases", two_instruments)
    inputs = await gather_sources(
        repository, snapshot_request(tmp_path, tmp_path / "out", symbols=()), RUN_TIME
    )
    assert inputs.ambiguous == ("ohlc/XXRPZEUR.json.gz",)
    assert inputs.unmapped == ()
    assert inputs.instruments == ()
    assert inputs.minutes == ()


def _file(path: str, fetched_at: datetime, close: float) -> SnapshotFile:
    """Build a parsed file holding one traded bar at the first minute."""
    bar = CandleUpdate(
        symbol=path,
        open=close,
        high=close,
        low=close,
        close=close,
        vwap=close,
        trades=2,
        volume=1.0,
        interval_begin=OPEN,
        interval=60,
    )
    return SnapshotFile(path, "XXRPZEUR", "0" * 64, fetched_at.timestamp(), (bar,), None)


def test_equal_bars_of_two_files_are_one_minute_known_at_the_latest_fetch() -> None:
    """Two files that agree on a minute give one source minute.

    Given: Two files with the same bar, fetched at different times.
    When: Their bars are merged.
    Then: There is one minute with that bar, known at the later fetch.
    """
    later = FETCHED_AT + timedelta(minutes=40)
    files = [
        _file("ohlc/XXRPZEUR.json.gz", FETCHED_AT, 1.5),
        _file("ohlc_tokenized/XXRPZEUR.json.gz", later, 1.5),
    ]
    minutes = merge_snapshot_minutes(files, {"XXRPZEUR": ResolvedInstrument(XRP, "XRP-EUR")})
    assert len(minutes) == 1
    assert minutes[0].known_at == later
    assert minutes[0].candle is not None and minutes[0].candle.close == 1.5


def test_different_bars_of_two_files_leave_the_minute_without_a_bar() -> None:
    """Two files that disagree on a minute give no bar for it.

    Given: Two files with different bars for the same minute.
    When: Their bars are merged.
    Then: The minute has no bar and no knowledge time.
    """
    files = [
        _file("ohlc/XXRPZEUR.json.gz", FETCHED_AT, 1.5),
        _file("ohlc_tokenized/XXRPZEUR.json.gz", FETCHED_AT, 1.6),
    ]
    minutes = merge_snapshot_minutes(files, {"XXRPZEUR": ResolvedInstrument(XRP, "XRP-EUR")})
    assert [(minute.candle, minute.known_at) for minute in minutes] == [(None, None)]


_DAY_ONE = datetime(2026, 11, 9, tzinfo=UTC)
_DAY_TWO = datetime(2026, 11, 10, tzinfo=UTC)


async def _store_day_edge_trades(repository: SQLAlchemyRepository) -> None:
    """Store trades at a day's first minute, at 23:59, at midnight and at the window end."""
    await repository.upsert_trades(
        [
            trade(XRP, _DAY_ONE + timedelta(seconds=10), 1.0, 2.0, "t1"),
            trade(XRP, _DAY_ONE + timedelta(seconds=40), 1.2, 1.0, "t2"),
            trade(XRP, _DAY_TWO - timedelta(seconds=30), 1.5, 1.0, "t3"),
            trade(XRP, _DAY_TWO, 1.7, 3.0, "t4"),
            trade(XRP, _DAY_TWO + timedelta(minutes=1), 9.0, 1.0, "t5"),
            trade(BTC, _DAY_ONE + timedelta(hours=1), 50.0, 1.0, "t6"),
        ]
    )


@pytest.mark.asyncio
async def test_trades_build_day_edge_minutes_and_drop_the_exclusive_end(
    repository: SQLAlchemyRepository, tmp_path: Path
) -> None:
    """Each UTC day is folded on its own and its exclusive end belongs to the next window.

    Given: Trades at 00:00:10 and 00:00:40, at 23:59:30, exactly at midnight and exactly
        at the window end, and an empty first day.
    When: The trades source covers the empty day, the full day and one minute after it.
    Then: The bars are 00:00, 23:59 and the midnight minute of the next day; the trade
        at the window end and the other instrument's trade are not folded.
    """
    await _store_day_edge_trades(repository)
    window = RepairWindow(_DAY_ONE - timedelta(days=1), _DAY_TWO + timedelta(minutes=1))
    inputs = await gather_sources(
        repository, trades_request(tmp_path / "out", window, ("XRP-EUR",)), RUN_TIME
    )
    assert inputs.instruments == (ResolvedInstrument(XRP, "XRP-EUR"),)
    assert inputs.contributing == ()
    bars = [(minute.minute, minute.known_at, minute.candle) for minute in inputs.minutes]
    assert [(minute, known_at) for minute, known_at, _ in bars] == [
        (_DAY_ONE, None),
        (_DAY_TWO - timedelta(minutes=1), None),
        (_DAY_TWO, None),
    ]
    first = bars[0][2]
    assert first is not None
    assert (first.open, first.high, first.low, first.close, first.volume, first.trades) == (
        1.0,
        1.2,
        1.0,
        1.2,
        3.0,
        2,
    )
    assert first.vwap == pytest.approx((1.0 * 2.0 + 1.2) / 3.0)
    midnight = bars[2][2]
    assert midnight is not None and (midnight.close, midnight.trades) == (1.7, 1)


@pytest.mark.asyncio
async def test_unknown_trade_symbol_refuses(
    repository: SQLAlchemyRepository, tmp_path: Path
) -> None:
    """Every requested symbol needs an active instrument on the exchange.

    Given: A request for XRP-EUR and an unknown symbol.
    When: The trades source resolves them.
    Then: The run refuses and names the unknown symbol.
    """
    window = RepairWindow(_DAY_ONE, _DAY_TWO)
    with pytest.raises(RepairRefusedError, match="DOGE-EUR"):
        await gather_sources(
            repository,
            trades_request(tmp_path / "out", window, ("DOGE-EUR", "XRP-EUR")),
            RUN_TIME,
        )


class _RecordingRepository(SQLAlchemyRepository):
    """A real repository whose ordinary upsert records rows instead of writing them."""

    recorded: list[CandleUpsertRow]

    async def upsert_candles(
        self, rows: list[CandleUpsertRow], session: object | None = None
    ) -> int:
        """Record the backfill's rows."""
        del session
        self.recorded.extend(rows)
        return len(rows)


@pytest.mark.asyncio
async def test_trade_bars_equal_the_backfill_builder_for_the_same_trades(tmp_path: Path) -> None:
    """The driver folds stored trades exactly as the trade candle backfill does.

    Given: Stored trades at the edges of two days.
    When: The backfill and the driver each build each day's bars.
    Then: Both give the same minutes and the same bar values.
    """
    repo = _RecordingRepository(f"sqlite+aiosqlite:///{tmp_path / 'parity.db'}")
    repo.recorded = []
    await repo.create_all()
    try:
        await seed_instrument(repo, XRP, "XRP-EUR", "XXRPZEUR")
        await _store_day_edge_trades(repo)
        settings = SimpleNamespace(db_url="unused", instruments={})
        driver: list[CandleUpdate] = []
        for lower, upper in RepairWindow(_DAY_ONE, _DAY_TWO + timedelta(days=1)).days():
            with patch.object(trade_candle_backfill, "get_settings", return_value=settings):
                service = TradeCandleBackfillService(
                    exchange=ExchangeEnum.KRAKEN,
                    start=lower,
                    end=upper,
                    symbols=["XRP-EUR"],
                    clock=lambda: RUN_TIME,
                )
            with (
                patch.object(trade_candle_backfill, "get_repository", return_value=repo),
                patch.object(trade_candle_backfill, "set_log_context"),
            ):
                await service.start()
            driver.extend(await trade_candles(repo, "kraken", XRP, (lower, upper), RUN_TIME))
    finally:
        await repo.engine.dispose()
    backfill = [
        (
            row["open_at"],
            row["open"],
            row["high"],
            row["low"],
            row["close"],
            row["vwap"],
            row["volume"],
            row["trades"],
        )
        for row in repo.recorded
    ]
    built = [
        (
            bar.interval_begin,
            bar.open,
            bar.high,
            bar.low,
            bar.close,
            bar.vwap,
            bar.volume,
            bar.trades,
        )
        for bar in driver
    ]
    assert built == backfill
    assert len(built) == 4
    assert RepairSource.TRADES.row_source == "calculated"
    assert RepairSource.SNAPSHOT.row_source == "native"
