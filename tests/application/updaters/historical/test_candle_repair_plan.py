"""Verify how the candle repair driver classifies minutes, asks the guards and binds the approval."""

import json
from collections.abc import AsyncIterator
from collections.abc import Awaitable
from collections.abc import Callable
from collections.abc import Iterator
from contextlib import asynccontextmanager
from dataclasses import replace
from datetime import datetime
from datetime import timedelta
from pathlib import Path
from typing import cast
from unittest.mock import AsyncMock
from unittest.mock import MagicMock

import pytest
import pytest_asyncio
from sqlalchemy import text

from snapper.application.updaters.historical import candle_repair
from snapper.application.updaters.historical.candle_repair import Action
from snapper.application.updaters.historical.candle_repair import PlanEntry
from snapper.application.updaters.historical.candle_repair import PreviewLine
from snapper.application.updaters.historical.candle_repair import RepairRefusedError
from snapper.application.updaters.historical.candle_repair import RepairRequest
from snapper.application.updaters.historical.candle_repair import RepairSource
from snapper.application.updaters.historical.candle_repair import RepairWindow
from snapper.application.updaters.historical.candle_repair import ResolvedInstrument
from snapper.application.updaters.historical.candle_repair import RunContext
from snapper.application.updaters.historical.candle_repair import SnapshotFile
from snapper.application.updaters.historical.candle_repair import SourceInputs
from snapper.application.updaters.historical.candle_repair import SourceMinute
from snapper.application.updaters.historical.candle_repair import WriteOptions
from snapper.application.updaters.historical.candle_repair import approval_header
from snapper.application.updaters.historical.candle_repair import build_plan
from snapper.application.updaters.historical.candle_repair import classify
from snapper.application.updaters.historical.candle_repair import load_voted_preview
from snapper.application.updaters.historical.candle_repair import parse_preview
from snapper.application.updaters.historical.candle_repair import plan_repair
from snapper.application.updaters.historical.candle_repair import preview_bytes
from snapper.application.updaters.historical.candle_repair import preview_line
from snapper.application.updaters.historical.candle_repair import read_versions
from snapper.application.updaters.historical.candle_repair import refuse_approval_mismatch
from snapper.application.updaters.historical.candle_repair import repair_row
from snapper.application.updaters.historical.candle_repair import select_voted
from snapper.application.updaters.historical.candle_repair import write_plan_artifacts
from snapper.application.updaters.historical.candle_repair import write_repair
from snapper.core.json_types import JsonObject
from snapper.data.models import KNOWN_TO_MAX
from snapper.data.repository import SQLAlchemyRepository
from snapper.data.repository_types import CandleUpsertRow
from snapper.data.repository_types import CandleVersionRow
from snapper.infrastructure.exchanges.contracts import CandleUpdate
from tests.helpers.candle_repair import BTC
from tests.helpers.candle_repair import FETCHED_AT
from tests.helpers.candle_repair import OPEN
from tests.helpers.candle_repair import RUN_TIME
from tests.helpers.candle_repair import XRP
from tests.helpers.candle_repair import candle
from tests.helpers.candle_repair import fixed_context
from tests.helpers.candle_repair import kraken_row
from tests.helpers.candle_repair import kraken_rows
from tests.helpers.candle_repair import seed_instrument
from tests.helpers.candle_repair import snapshot_request
from tests.helpers.candle_repair import trades_request
from tests.helpers.candle_repair import write_snapshot

_XRP = ResolvedInstrument(XRP, "XRP-EUR")
_BTC = ResolvedInstrument(BTC, "BTC-EUR")


def _bar(minute: datetime, close: float = 1.1, trades: int = 3) -> CandleUpdate:
    """Build a traded source bar."""
    return CandleUpdate(
        symbol="XXRPZEUR",
        open=close,
        high=close + 0.1,
        low=close - 0.1,
        close=close,
        vwap=close,
        trades=trades,
        volume=10.0,
        interval_begin=minute,
        interval=60,
    )


def _source(
    minute: datetime,
    instrument: ResolvedInstrument = _XRP,
    *,
    close: float = 1.1,
    known_at: datetime | None = FETCHED_AT,
) -> SourceMinute:
    """Build a snapshot source minute."""
    return SourceMinute(instrument, minute, _bar(minute, close), known_at)


def _version(
    minute: datetime,
    *,
    version_id: int = 1,
    known_to: datetime = KNOWN_TO_MAX,
    **changes: object,
) -> CandleVersionRow:
    """Build a stored version, by default a current flat synthesized zero."""
    row = candle(XRP, minute) | {"public_id": f"00000000-0000-4000-8000-{version_id:012d}"}
    return cast(CandleVersionRow, row | {"id": version_id, "known_to": known_to} | changes)


def _equal_version(minute: datetime, **changes: object) -> CandleVersionRow:
    """Build a current version carrying exactly the usual source bar."""
    bar = _bar(minute)
    values: dict[str, object] = {
        "source": "native",
        "open": bar.open,
        "high": bar.high,
        "low": bar.low,
        "close": bar.close,
        "vwap": bar.vwap,
        "volume": bar.volume,
        "trades": bar.trades,
    }
    return _version(minute, **(values | changes))


@pytest_asyncio.fixture
async def repository(tmp_path: Path) -> AsyncIterator[SQLAlchemyRepository]:
    """Provide a disposable file-backed repository with two Kraken symbols."""
    repo = SQLAlchemyRepository(f"sqlite+aiosqlite:///{tmp_path / 'repair-plan.db'}")
    await repo.create_all()
    await seed_instrument(repo, XRP, "XRP-EUR", "XXRPZEUR")
    await seed_instrument(repo, BTC, "BTC-EUR", "XXBTZEUR")
    try:
        yield repo
    finally:
        await repo.engine.dispose()


@pytest.mark.parametrize(
    ("versions", "action", "reason"),
    [
        ([], Action.INSERT, ""),
        (
            [_version(OPEN, known_to=OPEN + timedelta(hours=1))],
            Action.SKIP,
            "no_single_current_version",
        ),
        (
            [_version(OPEN, timestamp=RUN_TIME + timedelta(minutes=1))],
            Action.SKIP,
            "no_single_current_version",
        ),
        (
            [_version(OPEN, known_to=RUN_TIME + timedelta(hours=1)), _version(OPEN, version_id=2)],
            Action.SKIP,
            "no_single_current_version",
        ),
        ([_equal_version(OPEN)], Action.EQUAL, ""),
        ([_equal_version(OPEN, source="calculated", close=1.2)], Action.DIFFERS, ""),
        ([_equal_version(OPEN, volume=0.0, trades=0)], Action.DIFFERS, ""),
        ([_version(OPEN, volume=0.5)], Action.SKIP, "synthesized_not_flat_zero"),
        ([_version(OPEN, trades=None)], Action.SKIP, "synthesized_not_flat_zero"),
        ([_version(OPEN, high=1.5)], Action.SKIP, "synthesized_not_flat_zero"),
        (
            [
                _version(OPEN, version_id=2, source="native", known_to=OPEN + timedelta(minutes=2)),
                _version(OPEN, timestamp=OPEN + timedelta(minutes=2)),
            ],
            Action.SKIP,
            "zero_with_protected_history",
        ),
        (
            [
                _version(OPEN, version_id=2, trades=None, known_to=OPEN + timedelta(minutes=2)),
                _version(OPEN, timestamp=OPEN + timedelta(minutes=2)),
            ],
            Action.SKIP,
            "zero_with_protected_history",
        ),
        ([_version(OPEN)], Action.CORRECT, ""),
    ],
)
def test_each_stored_state_gets_its_action_and_reason(
    versions: list[CandleVersionRow], action: Action, reason: str
) -> None:
    """Every stored state of a minute is classified as the specification lists.

    Given: No version, only closed or future versions, two known versions, an equal or a
        different native or calculated row, synthesized rows that are not flat zeros,
        a zero with protected history, and a clean flat zero.
    When: The source bar for that minute is classified.
    Then: The action and reason match.
    """
    entry = classify(_source(OPEN), versions, RUN_TIME)
    assert (entry.action, entry.reason) == (action, reason)


def test_conflicting_snapshot_files_skip_the_minute_and_show_the_current_row() -> None:
    """A minute whose files disagree is skipped whatever is stored.

    Given: A source minute without an agreed bar and a current zero.
    When: It is classified and rendered.
    Then: It is skipped as conflicting, shows the zero and has no new bar.
    """
    entry = classify(replace(_source(OPEN), candle=None, known_at=None), [_version(OPEN)], RUN_TIME)
    assert (entry.action, entry.reason) == (Action.SKIP, "conflicting_snapshot_files")
    line = preview_line(entry, RepairSource.SNAPSHOT)
    assert line.current_id == "1"
    assert line.new_values == ("native", "", "", "", "", "", "", "", "")
    with pytest.raises(ValueError, match="agreed bar"):
        repair_row(entry, RepairSource.SNAPSHOT, RUN_TIME, "session", 1)


@pytest.mark.asyncio
async def test_versions_are_read_per_day_and_instrument_chunk_and_filtered(
    repository: SQLAlchemyRepository, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Every version of each wanted minute is found, read by day and by at most 20 instruments.

    Given: Closed and current versions of wanted minutes on two days, a version of an
        unwanted minute, and 25 more wanted instruments without versions.
    When: The versions of the wanted minutes are read.
    Then: Each wanted minute has all of its versions, nothing else is returned, and no
        read covers more than 20 instruments or two days.
    """
    second_day = OPEN + timedelta(days=1)
    await repository.upsert_candles([candle(XRP, OPEN)])
    await repository.upsert_candles(
        [
            candle(
                XRP,
                OPEN,
                source="native",
                volume=1.0,
                trades=2,
                timestamp=OPEN + timedelta(minutes=5),
            )
        ]
    )
    await repository.upsert_candles(
        [candle(XRP, OPEN + timedelta(minutes=1)), candle(BTC, second_day)]
    )
    await repository.upsert_candles([candle(XRP, OPEN + timedelta(minutes=7))])
    reads: list[tuple[int, datetime, datetime]] = []
    original = candle_repair._version_rows

    async def recording(
        repo: SQLAlchemyRepository,
        instrument_ids: list[str],
        lower: datetime,
        upper: datetime,
        wanted: set[tuple[str, datetime]],
    ) -> list[CandleVersionRow]:
        """Record the span of each read."""
        reads.append((len(instrument_ids), lower, upper))
        return await original(repo, instrument_ids, lower, upper, wanted)

    monkeypatch.setattr(candle_repair, "_version_rows", recording)
    others = [(f"00000000-0000-4000-8000-0000000d{index:04d}", OPEN) for index in range(25)]
    keys = [(XRP, OPEN), (XRP, OPEN + timedelta(minutes=1)), (BTC, second_day), *others]

    versions = await read_versions(repository, keys)

    assert sorted(versions) == sorted(keys[:3])
    assert sorted(row["source"] for row in versions[(XRP, OPEN)]) == ["native", "synthesized"]
    assert len(versions[(XRP, OPEN + timedelta(minutes=1))]) == 1
    assert all(count <= 20 for count, _, _ in reads)
    assert len(reads) == 3
    assert all(upper - lower <= timedelta(days=1) for _, lower, upper in reads)


@pytest.mark.asyncio
async def test_postgresql_version_reads_bound_their_statement_time() -> None:
    """Each PostgreSQL version read sets a 20-second statement timeout first.

    Given: A PostgreSQL repository double.
    When: The versions of one minute are read.
    Then: The first statement of the read transaction is the local statement timeout.
    """
    statements: list[str] = []
    result = MagicMock()
    result.mappings.return_value = []

    async def execute(statement: object) -> MagicMock:
        """Record one statement."""
        statements.append(str(statement))
        return result

    session = MagicMock(execute=AsyncMock(side_effect=execute))

    @asynccontextmanager
    async def open_session() -> AsyncIterator[MagicMock]:
        """Yield the recording session."""
        yield session

    repository = MagicMock(dialect_name="postgresql", session=open_session)
    assert await read_versions(repository, [(XRP, OPEN)]) == {}
    assert statements[0] == "SET LOCAL statement_timeout = '20s'"
    assert statements[1].startswith("SELECT")


def _inputs(*minutes: SourceMinute) -> SourceInputs:
    """Wrap source minutes of XRP and BTC."""
    return SourceInputs(instruments=(_XRP, _BTC), minutes=minutes)


@pytest.mark.asyncio
async def test_plan_confirms_writes_with_the_guards_and_marks_their_refusals(
    repository: SQLAlchemyRepository,
) -> None:
    """A planned correction the guard rejects is ``refused_by_guard`` with its action.

    Given: An absent minute, a zero learned before the snapshot fetch and a zero
        learned after it.
    When: The snapshot plan is built.
    Then: The first two are an insert and a correction, and the third, which the
        correction guard refuses because the zero is newer than the native row, is
        refused by the guard as a correction.
    """
    await repository.upsert_candles(
        [
            candle(XRP, OPEN + timedelta(minutes=1)),
            candle(XRP, OPEN + timedelta(minutes=2), timestamp=FETCHED_AT + timedelta(minutes=1)),
        ]
    )
    minutes = [_source(OPEN + timedelta(minutes=offset)) for offset in (2, 0, 1)]
    entries = await build_plan(repository, RepairSource.SNAPSHOT, _inputs(*minutes), RUN_TIME)
    assert [(entry.source.minute, entry.action, entry.reason) for entry in entries] == [
        (OPEN, Action.INSERT, ""),
        (OPEN + timedelta(minutes=1), Action.CORRECT, ""),
        (OPEN + timedelta(minutes=2), Action.REFUSED_BY_GUARD, "correct"),
    ]
    assert entries[2].current is not None


class _GuardSpy:
    """Record guarded-writer dry runs and drop selected keys from their answers."""

    def __init__(self, repository: SQLAlchemyRepository, drop: set[datetime]) -> None:
        """Wrap the repository's three guarded writers."""
        self.calls: list[tuple[str, list[CandleUpsertRow], dict[str, object]]] = []
        self._drop = drop
        for name in (
            "insert_missing_native_candles",
            "insert_missing_calculated_candles",
            "correct_false_zero_candles",
        ):
            setattr(repository, name, self._wrap(name, getattr(repository, name)))

    def _wrap(self, name: str, writer: object) -> object:
        """Return a recording replacement for one writer."""
        guarded = cast(Callable[..., Awaitable[list[CandleUpsertRow]]], writer)

        async def call(rows: list[CandleUpsertRow], **options: object) -> list[CandleUpsertRow]:
            """Record the call and drop the configured minutes from its answer."""
            self.calls.append((name, [row.copy() for row in rows], options))
            answered = await guarded(rows, **options)
            return [row for row in answered if row["open_at"] not in self._drop]

        return call


@pytest.mark.asyncio
async def test_snapshot_guard_rows_carry_the_fetch_time_and_planning_provenance(
    repository: SQLAlchemyRepository,
) -> None:
    """Snapshot dry runs send complete native rows stamped at the fetch, 100 at a time.

    Given: 150 absent minutes, one zero, and an insert guard that drops one minute.
    When: The snapshot plan is built.
    Then: Inserts go to the native writer in chunks of 100 and 50, the correction goes in
        native mode with its expected zero, every row carries one planning session and
        sequence numbers in plan order with the fetch time, and the dropped minute is
        refused by the guard as an insert.
    """
    first = OPEN - timedelta(hours=3)
    await repository.upsert_candles([candle(XRP, OPEN)])
    minutes = [_source(first + timedelta(minutes=offset)) for offset in range(150)]
    spy = _GuardSpy(repository, {first + timedelta(minutes=7)})
    entries = await build_plan(
        repository, RepairSource.SNAPSHOT, _inputs(*minutes, _source(OPEN)), RUN_TIME
    )
    assert [(name, len(rows)) for name, rows, _ in spy.calls] == [
        ("insert_missing_native_candles", 100),
        ("insert_missing_native_candles", 50),
        ("correct_false_zero_candles", 1),
    ]
    rows = [row for _, call_rows, _ in spy.calls for row in call_rows]
    assert {row["session_id"] for row in rows} == {rows[0]["session_id"]}
    assert [row["sequence_id"] for row in rows] == list(range(1, 152))
    assert {row["timestamp"] for row in rows} == {FETCHED_AT}
    assert {row["source"] for row in rows} == {"native"}
    assert all(options["dry_run"] is True for _, _, options in spy.calls)
    correction = spy.calls[2][2]
    assert correction["source"] == "native"
    assert list(cast(dict[object, object], correction["expected_predecessors"])) == [
        (XRP, "1m", OPEN)
    ]
    refused = [entry for entry in entries if entry.action is Action.REFUSED_BY_GUARD]
    assert [(entry.source.minute, entry.reason) for entry in refused] == [
        (first + timedelta(minutes=7), "insert")
    ]


@pytest.mark.asyncio
async def test_trade_guard_rows_are_calculated_and_stamped_at_the_run(
    repository: SQLAlchemyRepository,
) -> None:
    """Trade dry runs send calculated rows stamped at the time of the planning run.

    Given: One absent minute and one zero, from stored trades.
    When: The trades plan is built.
    Then: The calculated insert writer and the calculated correction are asked, with
        rows stamped at the run time.
    """
    await repository.upsert_candles([candle(XRP, OPEN)])
    minutes = [
        _source(OPEN - timedelta(minutes=1), known_at=None),
        _source(OPEN, known_at=None),
    ]
    spy = _GuardSpy(repository, set())
    entries = await build_plan(repository, RepairSource.TRADES, _inputs(*minutes), RUN_TIME)
    assert [entry.action for entry in entries] == [Action.INSERT, Action.CORRECT]
    assert [name for name, _, _ in spy.calls] == [
        "insert_missing_calculated_candles",
        "correct_false_zero_candles",
    ]
    assert {row["timestamp"] for _, rows, _ in spy.calls for row in rows} == {RUN_TIME}
    assert {row["source"] for _, rows, _ in spy.calls for row in rows} == {"calculated"}
    assert spy.calls[1][2]["source"] == "calculated"


def _entries() -> list[PlanEntry]:
    """Build a small plan: an insert, a correction and an equal minute, in plan order."""
    return [
        PlanEntry(_source(OPEN), Action.INSERT, "", None),
        PlanEntry(
            _source(OPEN + timedelta(minutes=1)),
            Action.CORRECT,
            "",
            _version(OPEN + timedelta(minutes=1)),
        ),
        PlanEntry(
            _source(OPEN + timedelta(minutes=2), _BTC),
            Action.EQUAL,
            "",
            _equal_version(OPEN + timedelta(minutes=2)),
        ),
    ]


def _artifacts_digest(
    output: Path,
    context: RunContext,
    *,
    request: RepairRequest | None = None,
    inputs: SourceInputs | None = None,
    entries: list[PlanEntry] | None = None,
) -> str:
    """Write one plan's files into a fresh directory and return its digest."""
    output.mkdir()
    base = request or snapshot_request(output.parent / "snap", output)
    files = (
        SnapshotFile(
            "ohlc/XXRPZEUR.json.gz", "XXRPZEUR", "a" * 64, FETCHED_AT.timestamp(), (), None
        ),
    )
    artifacts = write_plan_artifacts(
        replace(base, output_dir=output),
        context,
        inputs or SourceInputs(instruments=(_XRP, _BTC), minutes=(), contributing=files),
        entries or _entries(),
    )
    return artifacts.digest


def test_identical_state_gives_an_identical_approval_and_digest(tmp_path: Path) -> None:
    """The approval is canonical and holds no time of the run or generated id.

    Given: The same request, context, inputs and plan.
    When: The plan's files are written twice, into two directories.
    Then: Both approvals are byte-identical and canonical, and their digests equal.
    """
    context = fixed_context(MagicMock())
    first = _artifacts_digest(tmp_path / "first", context=context)
    second = _artifacts_digest(
        tmp_path / "second", context=replace(context, clock=lambda: RUN_TIME + timedelta(hours=1))
    )
    assert first == second
    approval = (tmp_path / "first" / "approval.json").read_bytes()
    assert approval == (tmp_path / "second" / "approval.json").read_bytes()
    document = json.loads(approval)
    assert approval == json.dumps(document, sort_keys=True, separators=(",", ":")).encode()
    assert sorted(document) == [
        "code_revision",
        "database",
        "exchange",
        "instruments",
        "preview_sha256",
        "snapshot_files",
        "source",
        "symbols",
        "timeframe",
        "window",
    ]
    assert document["snapshot_files"] == [
        {"path": "ohlc/XXRPZEUR.json.gz", "sha256": "a" * 64, "fetched_at": FETCHED_AT.timestamp()}
    ]
    summary = json.loads((tmp_path / "first" / "summary.json").read_text())
    assert summary["digest"] == first
    assert summary["actions"] == {
        "insert": 1,
        "correct": 1,
        "equal": 1,
        "differs": 0,
        "skip": 0,
        "refused_by_guard": 0,
    }
    assert summary["by_symbol"]["BTC-EUR"]["equal"] == 1
    assert summary["by_day"]["2026-11-10"]["insert"] == 1
    for name in ("preview.csv", "approval.json", "summary.json"):
        assert (tmp_path / "first" / name).stat().st_mode & 0o777 == 0o600


def test_every_bound_input_changes_the_digest(tmp_path: Path) -> None:
    """Each database, request, source, file, code and bar input is bound by the digest.

    Given: A baseline plan and variants that each change one bound input.
    When: Their files are written.
    Then: Every variant's digest differs from the baseline and from the others.
    """
    context = fixed_context(MagicMock())
    postgres = replace(
        context,
        database={"dialect": "postgresql", "host": "db", "port": 5432, "database": "snapper"},
    )
    request = snapshot_request(tmp_path / "snap", tmp_path / "unused")
    trades = trades_request(tmp_path / "unused", request.window, request.symbols)
    files = (
        SnapshotFile(
            "ohlc/XXRPZEUR.json.gz", "XXRPZEUR", "b" * 64, FETCHED_AT.timestamp(), (), None
        ),
    )
    changed_bar = _entries()
    changed_bar[0] = PlanEntry(_source(OPEN, close=1.2), Action.INSERT, "", None)
    digests = {
        "baseline": _artifacts_digest(tmp_path / "baseline", context=context),
        "sqlite_path": _artifacts_digest(
            tmp_path / "path",
            context=replace(context, database={"dialect": "sqlite", "path": "/other.db"}),
        ),
        "postgres": _artifacts_digest(tmp_path / "pg", context=postgres),
        "host": _artifacts_digest(
            tmp_path / "host",
            context=replace(postgres, database=postgres.database | {"host": "db2"}),
        ),
        "port": _artifacts_digest(
            tmp_path / "port",
            context=replace(postgres, database=postgres.database | {"port": 5433}),
        ),
        "name": _artifacts_digest(
            tmp_path / "name",
            context=replace(postgres, database=postgres.database | {"database": "other"}),
        ),
        "source": _artifacts_digest(tmp_path / "source", context=context, request=trades),
        "instrument": _artifacts_digest(
            tmp_path / "instrument",
            context=context,
            inputs=SourceInputs(instruments=(_XRP,), minutes=()),
        ),
        "file": _artifacts_digest(
            tmp_path / "file",
            context=context,
            inputs=SourceInputs(instruments=(_XRP, _BTC), minutes=(), contributing=files),
        ),
        "code": _artifacts_digest(
            tmp_path / "code",
            context=replace(context, code_revision={"snapper/data/repository.py": "1" * 64}),
        ),
        "bar": _artifacts_digest(tmp_path / "bar", context=context, entries=changed_bar),
    }
    assert len(set(digests.values())) == len(digests)


def test_preview_lines_are_sorted_typed_text_and_round_trip() -> None:
    """Preview lines carry repr floats and parse back to the same lines.

    Given: An insert and a correction.
    When: Their preview is written and parsed.
    Then: Floats use repr, the correction names its zero, and parsing returns the lines.
    """
    lines = [preview_line(entry, RepairSource.SNAPSHOT) for entry in _entries()]
    assert lines[0].current_id == ""
    assert lines[0].new_known_at == FETCHED_AT.isoformat()
    assert (lines[0].new_open, lines[0].new_high, lines[0].new_trades) == (
        "1.1",
        repr(1.1 + 0.1),
        "3",
    )
    assert (lines[1].current_source, lines[1].current_volume) == ("synthesized", "0.0")
    assert lines[1].current_known_at == OPEN.isoformat()
    data = preview_bytes(lines)
    assert data.decode().splitlines()[0].startswith("symbol,instrument_public_id,minute_utc,action")
    assert parse_preview(data) == tuple(lines)
    trade_line = preview_line(
        replace(_entries()[0], source=_source(OPEN, known_at=None)), RepairSource.TRADES
    )
    assert (trade_line.new_source, trade_line.new_known_at) == ("calculated", "")


@pytest.mark.parametrize(
    ("data", "message"),
    [
        (b"", "unexpected header"),
        (b"a,b\n", "unexpected header"),
        (b"\xff\xfe", "does not parse"),
        (b'"' + b"x" * 200_000 + b'"\n', "does not parse"),
    ],
)
def test_unparseable_previews_refuse(data: bytes, message: str) -> None:
    """A voted preview must be the driver's own CSV.

    Given: An empty file, a wrong header, invalid UTF-8 and an oversized field.
    When: It is parsed.
    Then: It is refused.
    """
    with pytest.raises(RepairRefusedError, match=message):
        parse_preview(data)


def test_malformed_or_repeated_preview_lines_refuse() -> None:
    """Every voted line has every column, a known action and its own minute.

    Given: Previews with a short line, an unknown action and a repeated minute.
    When: They are parsed.
    Then: Each is refused.
    """
    lines = [preview_line(entry, RepairSource.SNAPSHOT) for entry in _entries()]
    header = preview_bytes([]).decode()
    short = header + "XRP-EUR,abc\n"
    unknown = preview_bytes([replace(lines[0], action="delete")]).decode()
    repeated = preview_bytes([lines[0], lines[0]]).decode()
    for document, message in ((short, "malformed"), (unknown, "malformed"), (repeated, "repeats")):
        with pytest.raises(RepairRefusedError, match=message):
            parse_preview(document.encode())


def _voted_dir(tmp_path: Path, approval: bytes, preview: bytes) -> Path:
    """Write a voted run's two files."""
    directory = tmp_path / "voted"
    directory.mkdir(exist_ok=True)
    (directory / "approval.json").write_bytes(approval)
    (directory / "preview.csv").write_bytes(preview)
    return directory


def _sha(data: bytes) -> str:
    """Return the hex sha256 of bytes."""
    return candle_repair.sha256_hex(data)


def test_voted_preview_is_verified_before_use(tmp_path: Path) -> None:
    """The voted approval must hash to the digest and bind the voted preview.

    Given: A matching voted run.
    When: It is loaded with its digest.
    Then: Its approval and lines are returned.
    """
    preview = preview_bytes([preview_line(entry, RepairSource.SNAPSHOT) for entry in _entries()])
    approval = json.dumps({"preview_sha256": _sha(preview)}).encode()
    voted = load_voted_preview(_voted_dir(tmp_path, approval, preview), _sha(approval))
    assert voted.digest == _sha(approval)
    assert len(voted.lines) == 3


@pytest.mark.parametrize(
    ("approval", "preview", "digest_of", "message"),
    [
        (None, b"x", "approval", "approval.json cannot be read"),
        (b"{}", None, "approval", "preview.csv cannot be read"),
        (b"{}", b"x", "other", "differs from --expect-digest"),
        (b"{not json", b"x", "approval", "does not parse"),
        (b"[]", b"x", "approval", "not an approval"),
        (b'{"preview_sha256": 1}', b"x", "approval", "not an approval"),
        (b'{"preview_sha256": "00"}', b"x", "approval", "does not match the hash"),
    ],
)
def test_voted_preview_refusals(
    tmp_path: Path, approval: bytes | None, preview: bytes | None, digest_of: str, message: str
) -> None:
    """A missing, unhashed, unparseable or unbound voted file refuses.

    Given: A voted run with one defect.
    When: It is loaded.
    Then: It is refused with that defect named.
    """
    directory = tmp_path / "voted"
    directory.mkdir()
    if approval is not None:
        (directory / "approval.json").write_bytes(approval)
    if preview is not None:
        (directory / "preview.csv").write_bytes(preview)
    digest = _sha(approval or b"") if digest_of == "approval" else "f" * 64
    with pytest.raises(RepairRefusedError, match=message):
        load_voted_preview(directory, digest)


def test_voted_preview_that_does_not_parse_refuses(tmp_path: Path) -> None:
    """A preview that hashes correctly but is not a preview refuses.

    Given: An approval binding a preview without the expected header.
    When: It is loaded.
    Then: It is refused.
    """
    preview = b"not,a,preview\n"
    approval = json.dumps({"preview_sha256": _sha(preview)}).encode()
    with pytest.raises(RepairRefusedError, match="header"):
        load_voted_preview(_voted_dir(tmp_path, approval, preview), _sha(approval))


def test_approval_mismatch_names_each_differing_field() -> None:
    """Every binding field is compared; a complete check also rejects extra voted fields.

    Given: A voted approval and this run's header that differ in the source.
    When: They are compared partially, then completely with an extra voted field.
    Then: The source is named, and the complete check also names the extra field.
    """
    header: JsonObject = {"source": "trades", "exchange": "kraken"}
    voted: JsonObject = {"source": "snapshot", "exchange": "kraken", "preview_sha256": "x"}
    with pytest.raises(RepairRefusedError, match="in: source$"):
        refuse_approval_mismatch(voted, header, complete=False)
    refuse_approval_mismatch(voted | {"source": "trades", "later": 1}, header, complete=False)
    with pytest.raises(RepairRefusedError, match="in: later$"):
        refuse_approval_mismatch(voted | {"source": "trades", "later": 1}, header, complete=True)


def test_selection_keeps_unchanged_votes_and_reports_the_rest() -> None:
    """A planned write is selected only if the vote holds it unchanged.

    Given: A plan with an insert, a correction, a second insert, an equal minute and
        a differing minute, and a vote that holds the insert unchanged, the correction
        with another zero, nothing for the second insert, the equal minute as an insert
        of the bar now stored, the differing minute as an insert of another bar, and a
        minute the plan no longer has.
    When: The vote is applied.
    Then: The insert is selected; the correction and the second insert are not voted;
        the equal minute is already applied; the correction, the differing minute and
        the vanished minute are changed.
    """
    entries = [
        *_entries()[:2],
        PlanEntry(_source(OPEN + timedelta(minutes=3)), Action.INSERT, "", None),
        PlanEntry(
            _source(OPEN + timedelta(minutes=4)),
            Action.EQUAL,
            "",
            _equal_version(OPEN + timedelta(minutes=4)),
        ),
        PlanEntry(
            _source(OPEN + timedelta(minutes=5)),
            Action.DIFFERS,
            "",
            _equal_version(OPEN + timedelta(minutes=5), close=2.0),
        ),
    ]
    lines = [preview_line(entry, RepairSource.SNAPSHOT) for entry in entries]
    vanished = preview_line(
        PlanEntry(_source(OPEN + timedelta(minutes=9)), Action.INSERT, "", None),
        RepairSource.SNAPSHOT,
    )
    voted = [
        lines[0],
        replace(lines[1], current_id="99"),
        replace(
            lines[3], action="insert", current_id="", current_public_id="", current_known_at=""
        ),
        replace(
            lines[4], action="insert", current_id="", current_public_id="", current_known_at=""
        ),
        replace(lines[2], action="equal"),
        vanished,
    ]
    selection = select_voted(entries, lines, voted)
    assert [entry.source.minute for entry in selection.selected] == [OPEN]
    assert [entry.source.minute for entry in selection.not_voted] == [
        OPEN + timedelta(minutes=1),
        OPEN + timedelta(minutes=3),
    ]
    assert [line.minute_utc for line in selection.already_applied] == [
        (OPEN + timedelta(minutes=4)).isoformat()
    ]
    assert [line.minute_utc for line in selection.changed] == [
        (OPEN + timedelta(minutes=1)).isoformat(),
        (OPEN + timedelta(minutes=5)).isoformat(),
        (OPEN + timedelta(minutes=9)).isoformat(),
    ]


class _UntouchableRepository:
    """A repository that fails any database access."""

    dialect_name = "sqlite"

    def __getattr__(self, name: str) -> object:
        """Refuse every repository method."""
        raise AssertionError(f"database accessed through {name}")


def _write_options(voted: Path, digest: str) -> WriteOptions:
    """Build write options without pauses."""
    return WriteOptions(voted, digest, None, 0.0, 3)


@pytest.mark.asyncio
async def test_write_refuses_a_foreign_approval_before_any_database_read(tmp_path: Path) -> None:
    """An approval bound to other code or another window refuses before the database is read.

    Given: A voted approval whose code revision differs from this run's.
    When: The write starts.
    Then: It refuses naming the code revision, and the repository is never used.
    """
    context = fixed_context(cast(SQLAlchemyRepository, _UntouchableRepository()))
    request = snapshot_request(tmp_path / "snap", tmp_path / "out")
    header = approval_header(request, replace(context, code_revision={"other.py": "1" * 64}), None)
    preview = preview_bytes([])
    approval = json.dumps(header | {"preview_sha256": _sha(preview)}).encode()
    voted = _voted_dir(tmp_path, approval, preview)
    with pytest.raises(RepairRefusedError, match="in: code_revision$"):
        await write_repair(context, request, _write_options(voted, _sha(approval)))


@pytest.mark.asyncio
async def test_write_refuses_an_approval_for_other_instruments_after_resolution(
    repository: SQLAlchemyRepository, tmp_path: Path
) -> None:
    """An approval binding other instruments refuses once the instruments are resolved.

    Given: A voted approval that lists no instrument, and a snapshot file for XRP.
    When: The write starts.
    Then: It refuses naming the instruments and the snapshot files.
    """
    write_snapshot(
        tmp_path / "snap",
        "XXRPZEUR",
        [*kraken_rows(OPEN, 2), kraken_row(OPEN + timedelta(minutes=2), 1)],
    )
    context = fixed_context(repository)
    request = snapshot_request(tmp_path / "snap", tmp_path / "out")
    header = approval_header(request, context, SourceInputs(instruments=(), minutes=()))
    preview = preview_bytes([])
    approval = json.dumps(header | {"preview_sha256": _sha(preview)}).encode()
    voted = _voted_dir(tmp_path, approval, preview)
    with pytest.raises(RepairRefusedError, match="in: instruments, snapshot_files$"):
        await write_repair(context, request, _write_options(voted, _sha(approval)))


@pytest.mark.asyncio
async def test_plan_and_write_refuse_minutes_inside_the_live_edge(tmp_path: Path) -> None:
    """Neither run reads the database for a window inside the live writers' reach.

    Given: A window ending 179 minutes before the run.
    When: A dry run and a write start.
    Then: Both refuse before using the repository.
    """
    edge = RUN_TIME - timedelta(minutes=179)
    window = RepairWindow(edge - timedelta(hours=1), edge)
    context = fixed_context(cast(SQLAlchemyRepository, _UntouchableRepository()))
    request = snapshot_request(tmp_path / "snap", tmp_path / "out", window=window)
    with pytest.raises(RepairRefusedError, match="180 minutes"):
        await plan_repair(context, request)
    preview = preview_bytes([])
    approval = json.dumps({"preview_sha256": _sha(preview)}).encode()
    voted = _voted_dir(tmp_path, approval, preview)
    with pytest.raises(RepairRefusedError, match="180 minutes"):
        await write_repair(context, request, _write_options(voted, _sha(approval)))


@pytest.mark.asyncio
async def test_dry_run_writes_the_three_files_and_never_writes_a_candle(
    repository: SQLAlchemyRepository, tmp_path: Path
) -> None:
    """The dry run plans from the snapshot and leaves the candles unchanged.

    Given: A snapshot with an absent minute, a zero minute and an untraded minute.
    When: The dry run runs.
    Then: The preview names an insert and a correction, and no candle changed.
    """
    rows = [
        kraken_row(OPEN, 3),
        kraken_row(OPEN + timedelta(minutes=1), 2),
        kraken_row(OPEN + timedelta(minutes=2), 0, volume=0.0),
        kraken_row(OPEN + timedelta(minutes=3), 1),
    ]
    write_snapshot(tmp_path / "snap", "XXRPZEUR", rows)
    await repository.upsert_candles([candle(XRP, OPEN + timedelta(minutes=1))])
    async with repository.session() as session:
        before = (await session.execute(text("SELECT COUNT(*) FROM candles"))).scalar_one()
    artifacts = await plan_repair(
        fixed_context(repository), snapshot_request(tmp_path / "snap", tmp_path / "out")
    )
    assert [(line.minute_utc, line.action) for line in artifacts.lines] == [
        (OPEN.isoformat(), "insert"),
        ((OPEN + timedelta(minutes=1)).isoformat(), "correct"),
    ]
    async with repository.session() as session:
        assert (await session.execute(text("SELECT COUNT(*) FROM candles"))).scalar_one() == before
    assert sorted(path.name for path in (tmp_path / "out").iterdir()) == [
        "approval.json",
        "preview.csv",
        "summary.json",
    ]


def _iter_lines(data: bytes) -> Iterator[str]:
    """Yield the lines of a file."""
    yield from data.decode().splitlines()


def test_preview_line_for_a_trade_correction_has_no_new_known_at() -> None:
    """A trade row's knowledge time is set by the write, so the preview leaves it empty.

    Given: A trade correction.
    When: Its preview line is written.
    Then: The new knowledge time is empty and the source is calculated.
    """
    entry = PlanEntry(_source(OPEN, known_at=None), Action.CORRECT, "", _version(OPEN))
    line = preview_line(entry, RepairSource.TRADES)
    assert list(_iter_lines(preview_bytes([line])))[1].split(",")[10:12] == ["calculated", ""]
    assert isinstance(line, PreviewLine)
