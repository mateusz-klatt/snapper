"""Verify how the candle repair driver applies a voted plan, retries, reconciles and keeps receipts."""

import asyncio
import json
from collections.abc import AsyncIterator
from collections.abc import Awaitable
from collections.abc import Callable
from collections.abc import Iterator
from contextlib import asynccontextmanager
from contextlib import contextmanager
from dataclasses import replace
from datetime import UTC
from datetime import datetime
from datetime import timedelta
from pathlib import Path
from typing import cast
from unittest.mock import AsyncMock
from unittest.mock import MagicMock

import pytest
import pytest_asyncio
from sqlalchemy import event
from sqlalchemy import insert
from sqlalchemy import select
from sqlalchemy.exc import DBAPIError
from sqlalchemy.exc import IntegrityError
from sqlalchemy.exc import OperationalError
from sqlalchemy.ext.asyncio import AsyncSession

from snapper.application.updaters.historical import candle_repair
from snapper.application.updaters.historical.candle_repair import NATURAL_KEY_INDEXES
from snapper.application.updaters.historical.candle_repair import Action
from snapper.application.updaters.historical.candle_repair import PlanEntry
from snapper.application.updaters.historical.candle_repair import RepairSource
from snapper.application.updaters.historical.candle_repair import RepairWindow
from snapper.application.updaters.historical.candle_repair import ResolvedInstrument
from snapper.application.updaters.historical.candle_repair import RunContext
from snapper.application.updaters.historical.candle_repair import SourceMinute
from snapper.application.updaters.historical.candle_repair import WriteOptions
from snapper.application.updaters.historical.candle_repair import WriteReport
from snapper.application.updaters.historical.candle_repair import canonical_json
from snapper.application.updaters.historical.candle_repair import classify_failure
from snapper.application.updaters.historical.candle_repair import error_fields
from snapper.application.updaters.historical.candle_repair import is_natural_key_index
from snapper.application.updaters.historical.candle_repair import plan_repair
from snapper.application.updaters.historical.candle_repair import preview_bytes
from snapper.application.updaters.historical.candle_repair import reconcile
from snapper.application.updaters.historical.candle_repair import sha256_hex
from snapper.application.updaters.historical.candle_repair import write_chunks
from snapper.application.updaters.historical.candle_repair import write_repair
from snapper.core.json_types import JsonObject
from snapper.data import daily_partitions
from snapper.data.models import KNOWN_TO_MAX
from snapper.data.models import Candle
from snapper.data.repository import SQLAlchemyRepository
from snapper.data.repository_types import CandleUpsertRow
from snapper.data.repository_types import CandleVersionRow
from snapper.infrastructure.exchanges.contracts import CandleUpdate
from tests.helpers.candle_repair import FETCHED_AT
from tests.helpers.candle_repair import OPEN
from tests.helpers.candle_repair import RUN_TIME
from tests.helpers.candle_repair import XRP
from tests.helpers.candle_repair import candle
from tests.helpers.candle_repair import fixed_context
from tests.helpers.candle_repair import kraken_row
from tests.helpers.candle_repair import seed_instrument
from tests.helpers.candle_repair import snapshot_request
from tests.helpers.candle_repair import trade
from tests.helpers.candle_repair import trades_request
from tests.helpers.candle_repair import write_snapshot

type _Writer = Callable[..., Awaitable[list[CandleUpsertRow]]]

_NATURAL_MESSAGE = (
    "UNIQUE constraint failed: candles.instrument_public_id, candles.timeframe, candles.open_at"
)


class _DriverError(Exception):
    """A driver error carrying the attributes the real drivers expose."""

    def __init__(
        self,
        message: str = "driver failure for user secret-user",
        *,
        sqlstate: str | None = None,
        constraint_name: str | None = None,
        sqlite_errorcode: int | None = None,
        diag: object | None = None,
    ) -> None:
        """Keep each attribute only when given, as the drivers do."""
        super().__init__(message)
        if diag is not None:
            self.diag = diag
        if sqlstate is not None:
            self.sqlstate = sqlstate
        if constraint_name is not None:
            self.constraint_name = constraint_name
        if sqlite_errorcode is not None:
            self.sqlite_errorcode = sqlite_errorcode


def _db_error(kind: type[DBAPIError] = OperationalError, **attributes: str | int) -> DBAPIError:
    """Wrap a driver error as SQLAlchemy raises it."""
    return kind("INSERT INTO candles", {}, _DriverError(**attributes))


class _Sleeps:
    """Record every pause instead of waiting."""

    def __init__(self) -> None:
        """Start with no pause."""
        self.calls: list[float] = []

    async def __call__(self, seconds: float) -> None:
        """Record one pause."""
        self.calls.append(seconds)


@pytest_asyncio.fixture
async def repository(tmp_path: Path) -> AsyncIterator[SQLAlchemyRepository]:
    """Provide a disposable file-backed repository with the XRP symbol."""
    repo = SQLAlchemyRepository(f"sqlite+aiosqlite:///{tmp_path / 'repair-write.db'}")
    await repo.create_all()
    await seed_instrument(repo, XRP, "XRP-EUR", "XXRPZEUR")
    try:
        yield repo
    finally:
        await repo.engine.dispose()


async def _plan(
    repository: SQLAlchemyRepository, tmp_path: Path, *, minutes: int = 3, zeros: int = 1
) -> str:
    """Save a snapshot, store zeros for its first minutes and run the dry run into ``plan``.

    Returns:
        The digest of the plan.
    """
    rows = [kraken_row(OPEN + timedelta(minutes=offset), offset + 1) for offset in range(minutes)]
    rows.append(kraken_row(OPEN + timedelta(minutes=minutes), 1))
    fetched = OPEN + timedelta(minutes=minutes, seconds=30)
    write_snapshot(tmp_path / "snap", "XXRPZEUR", rows, fetched.timestamp())
    await repository.upsert_candles(
        [candle(XRP, OPEN + timedelta(minutes=offset)) for offset in range(zeros)]
    )
    request = snapshot_request(tmp_path / "snap", tmp_path / "plan")
    return (await plan_repair(fixed_context(repository), request)).digest


def _options(tmp_path: Path, digest: str, **changes: object) -> WriteOptions:
    """Build write options for the voted ``plan`` directory."""
    options = WriteOptions(tmp_path / "plan", digest, None, 0.5, 3)
    return replace(options, **changes)


async def _write(
    context: RunContext, tmp_path: Path, name: str, options: WriteOptions
) -> WriteReport:
    """Run one write into a fresh output directory."""
    request = snapshot_request(tmp_path / "snap", tmp_path / name)
    return await write_repair(context, request, options)


def _receipt(tmp_path: Path, name: str) -> JsonObject:
    """Read a write run's ``receipt.json``."""
    return cast(JsonObject, json.loads((tmp_path / name / "receipt.json").read_text()))


def _lines(tmp_path: Path, name: str) -> list[JsonObject]:
    """Read a write run's ``receipt.jsonl``."""
    text = (tmp_path / name / "receipt.jsonl").read_text()
    return [cast(JsonObject, json.loads(line)) for line in text.splitlines()]


def _outcomes(chunk: JsonObject) -> dict[str, int]:
    """Count a chunk receipt's rows per outcome."""
    return {
        name: len(cast(list[object], chunk[name]))
        for name in (
            "applied",
            "skipped_by_guard",
            "applied_superseded",
            "uncertain",
            "not_applied",
        )
    }


def _script(
    repository: SQLAlchemyRepository, name: str, outcomes: list[BaseException | None]
) -> list[int]:
    """Make a guarded writer's write calls fail as scripted; dry runs pass through.

    Returns:
        The sizes of the write calls, in order.
    """
    writer = cast(_Writer, getattr(repository, name))
    sizes: list[int] = []

    async def scripted(rows: list[CandleUpsertRow], **options: object) -> list[CandleUpsertRow]:
        """Fail or delegate one call."""
        if options.get("dry_run"):
            return await writer(rows, **options)
        sizes.append(len(rows))
        outcome = outcomes.pop(0) if outcomes else None
        if outcome is not None:
            raise outcome
        return await writer(rows, **options)

    setattr(repository, name, scripted)
    return sizes


async def _current(repository: SQLAlchemyRepository) -> list[dict[str, object]]:
    """Read the current one-minute versions in minute order."""
    async with repository.session() as session:
        result = await session.execute(
            select(Candle.__table__).where(Candle.known_to == KNOWN_TO_MAX).order_by(Candle.open_at)
        )
        return [dict(row) for row in result.mappings().all()]


@pytest.mark.asyncio
async def test_write_applies_the_voted_plan_and_a_rerun_reports_equal(
    repository: SQLAlchemyRepository, tmp_path: Path
) -> None:
    """A confirmed write applies the voted inserts and corrections with its own provenance.

    Given: A voted plan of one correction and two inserts.
    When: It is written, then planned again.
    Then: Inserts and the correction are applied in that order under one run id with
        sequence ids in plan order, the correction is known at the snapshot fetch, the
        receipts record one acknowledged attempt per chunk, and the rerun is all equal.
    """
    digest = await _plan(repository, tmp_path)
    report = await _write(fixed_context(repository), tmp_path, "write", _options(tmp_path, digest))

    assert (report.stop_reason, report.complete, report.voted_digest) == ("completed", True, digest)
    assert report.plan_digest == digest
    assert report.totals == {
        "applied": 3,
        "skipped_by_guard": 0,
        "applied_superseded": 0,
        "uncertain": 0,
        "not_applied": 0,
    }
    receipt = _receipt(tmp_path, "write")
    assert receipt["stop_reason"] == "completed"
    assert receipt["voted_digest"] == digest
    assert receipt["write_time"] == RUN_TIME.isoformat()
    assert (receipt["selected"], receipt["attempted"], receipt["left"]) == (3, 3, 0)
    assert receipt["not_voted"] == receipt["already_applied"] == receipt["changed"] == []
    chunks = cast(list[JsonObject], receipt["chunks"])
    assert [(chunk["action"], chunk["rows"], chunk["attempt_count"]) for chunk in chunks] == [
        ("insert", 2, 1),
        ("correct", 1, 1),
    ]
    assert _lines(tmp_path, "write") == chunks
    current = await _current(repository)
    run_id = receipt["run_id"]
    assert [(row["session_id"], row["sequence_id"], row["source"]) for row in current] == [
        (run_id, 1, "native"),
        (run_id, 2, "native"),
        (run_id, 3, "native"),
    ]
    assert current[0]["timestamp"] == OPEN + timedelta(minutes=3, seconds=30)
    for name in ("receipt.json", "receipt.jsonl"):
        assert (tmp_path / "write" / name).stat().st_mode & 0o777 == 0o600

    rerun = await plan_repair(
        fixed_context(repository, now=datetime.now(UTC)),
        snapshot_request(tmp_path / "snap", tmp_path / "rerun"),
    )
    assert {line.action for line in rerun.lines} == {"equal"}


def _entry(minute: datetime, action: Action, known_at: datetime | None) -> PlanEntry:
    """Build a writable entry with a zero predecessor for corrections."""
    bar = CandleUpdate(
        symbol=XRP,
        open=1.0,
        high=1.0,
        low=1.0,
        close=1.0,
        vwap=1.0,
        trades=1,
        volume=1.0,
        interval_begin=minute,
        interval=60,
    )
    zero = cast(
        CandleVersionRow,
        candle(XRP, minute) | {"id": 7, "public_id": "zero", "known_to": KNOWN_TO_MAX},
    )
    current = zero if action is Action.CORRECT else None
    return PlanEntry(
        SourceMinute(ResolvedInstrument(XRP, "XRP-EUR"), minute, bar, known_at),
        action,
        "",
        current,
    )


def test_write_chunks_hold_at_most_100_rows_with_inserts_first() -> None:
    """The run's rows are numbered in plan order and cut into homogeneous chunks of 100.

    Given: Interleaved selected inserts and corrections, 253 rows in all.
    When: The write chunks are built for the snapshot.
    Then: Inserts come first in chunks of 100, 100 and 50, then the three corrections;
        sequence ids follow plan order, snapshot rows keep the fetch time and each
        correction names its zero.
    """
    entries = [
        _entry(
            OPEN + timedelta(minutes=offset),
            Action.CORRECT if offset % 100 == 1 else Action.INSERT,
            FETCHED_AT,
        )
        for offset in range(253)
    ]
    chunks = write_chunks(entries, RepairSource.SNAPSHOT, "run", RUN_TIME)
    assert [(chunk.number, chunk.action, len(chunk.rows)) for chunk in chunks] == [
        (1, Action.INSERT, 100),
        (2, Action.INSERT, 100),
        (3, Action.INSERT, 50),
        (4, Action.CORRECT, 3),
    ]
    assert [row["sequence_id"] for row in chunks[3].rows] == [2, 102, 202]
    assert {row["session_id"] for chunk in chunks for row in chunk.rows} == {"run"}
    assert {row["timestamp"] for chunk in chunks for row in chunk.rows} == {FETCHED_AT}
    assert chunks[0].predecessors == {}
    assert chunks[3].predecessors[(XRP, "1m", OPEN + timedelta(minutes=1))] == {
        "id": 7,
        "public_id": "zero",
        "timestamp": OPEN,
    }


def test_trade_corrections_take_the_fixed_write_time() -> None:
    """Trade rows are known at the run's single write time, the same for every attempt.

    Given: A selected trade correction.
    When: Its chunk is built.
    Then: The row is calculated and stamped with the write time.
    """
    chunks = write_chunks(
        [_entry(OPEN, Action.CORRECT, None)], RepairSource.TRADES, "run", RUN_TIME
    )
    assert [(row["source"], row["timestamp"]) for row in chunks[0].rows] == [
        ("calculated", RUN_TIME)
    ]


@pytest.mark.asyncio
async def test_lock_timeout_is_retried_after_a_backoff_and_shown_per_attempt(
    repository: SQLAlchemyRepository, tmp_path: Path
) -> None:
    """A chunk whose lock was not granted is retried outside the transaction.

    Given: An insert chunk whose first attempt gets SQLSTATE 55P03.
    When: The voted plan is written.
    Then: The chunk is acknowledged on its second attempt after a 0.25 s backoff, the
        receipt shows both attempts, and the corrections chunk follows after the pause.
    """
    digest = await _plan(repository, tmp_path)
    sizes = _script(repository, "insert_missing_native_candles", [_db_error(sqlstate="55P03")])
    sleeps = _Sleeps()
    context = replace(fixed_context(repository), sleep=sleeps)
    report = await _write(context, tmp_path, "write", _options(tmp_path, digest))
    assert report.stop_reason == "completed"
    assert sizes == [2, 2]
    assert sleeps.calls == [0.25, 0.5]
    chunk = cast(list[JsonObject], _receipt(tmp_path, "write")["chunks"])[0]
    assert chunk["attempts"] == [
        {
            "attempt": 1,
            "outcome": "lock_timeout",
            "error": "OperationalError",
            "driver_error": "_DriverError",
            "sqlstate": "55P03",
        },
        {"attempt": 2, "outcome": "acknowledged"},
    ]


@pytest.mark.asyncio
async def test_consecutive_lock_timeouts_stop_before_the_next_attempt(
    repository: SQLAlchemyRepository, tmp_path: Path
) -> None:
    """The configured number of consecutive lock timeouts stops the run.

    Given: An insert chunk that always gets 55P03 and a limit of three.
    When: The voted plan is written.
    Then: Three attempts are made with backoffs of 0.25 and 0.5 s, the run stops for
        lock timeouts with the chunk not applied, and no correction is attempted.
    """
    digest = await _plan(repository, tmp_path)
    errors: list[BaseException | None] = [_db_error(sqlstate="55P03") for _ in range(4)]
    sizes = _script(repository, "insert_missing_native_candles", errors)
    corrections = _script(repository, "correct_false_zero_candles", [])
    sleeps = _Sleeps()
    report = await _write(
        replace(fixed_context(repository), sleep=sleeps),
        tmp_path,
        "write",
        _options(tmp_path, digest),
    )
    assert (report.stop_reason, report.complete) == ("lock_timeouts", False)
    assert sizes == [2, 2, 2]
    assert corrections == []
    assert sleeps.calls == [0.25, 0.5]
    chunk = cast(list[JsonObject], _receipt(tmp_path, "write")["chunks"])[0]
    assert [attempt["outcome"] for attempt in cast(list[JsonObject], chunk["attempts"])] == [
        "lock_timeout"
    ] * 3
    assert _outcomes(chunk)["not_applied"] == 2
    assert [row["source"] for row in await _current(repository)] == ["synthesized"]


@pytest.mark.asyncio
async def test_lock_timeout_streak_resets_after_an_acknowledged_chunk(
    repository: SQLAlchemyRepository, tmp_path: Path
) -> None:
    """Two lock timeouts per chunk never reach a limit of three across chunks.

    Given: An insert chunk and a correction chunk that each get two 55P03 before success.
    When: The voted plan is written with a limit of three.
    Then: Both chunks are acknowledged on their third attempt and the run completes.
    """
    digest = await _plan(repository, tmp_path)
    timeout = _db_error(sqlstate="55P03")
    _script(repository, "insert_missing_native_candles", [timeout, timeout])
    _script(repository, "correct_false_zero_candles", [timeout, timeout])
    report = await _write(fixed_context(repository), tmp_path, "write", _options(tmp_path, digest))
    assert report.stop_reason == "completed"
    chunks = cast(list[JsonObject], _receipt(tmp_path, "write")["chunks"])
    assert [chunk["attempt_count"] for chunk in chunks] == [3, 3]


@pytest.mark.asyncio
async def test_other_transient_errors_are_retried_and_break_a_lock_timeout_streak(
    repository: SQLAlchemyRepository, tmp_path: Path
) -> None:
    """Serialization failures and deadlocks prove nothing was committed and are retried.

    Given: An insert chunk failing with 55P03, 40001 and 40P01 before success, and a
        lock-timeout limit of one more than any uninterrupted streak.
    When: The voted plan is written.
    Then: The chunk is acknowledged on its fourth attempt.
    """
    digest = await _plan(repository, tmp_path)
    _script(
        repository,
        "insert_missing_native_candles",
        [
            _db_error(sqlstate="55P03"),
            _db_error(sqlstate="40001"),
            _db_error(sqlstate="40P01"),
        ],
    )
    report = await _write(
        fixed_context(repository),
        tmp_path,
        "write",
        _options(tmp_path, digest, max_consecutive_lock_timeouts=2),
    )
    assert report.stop_reason == "completed"
    chunk = cast(list[JsonObject], _receipt(tmp_path, "write")["chunks"])[0]
    assert [attempt["outcome"] for attempt in cast(list[JsonObject], chunk["attempts"])] == [
        "lock_timeout",
        "serialization_failure",
        "deadlock",
        "acknowledged",
    ]


@pytest.mark.asyncio
async def test_fourth_retryable_failure_exhausts_the_chunk_after_three_backoffs(
    repository: SQLAlchemyRepository, tmp_path: Path
) -> None:
    """A chunk gets at most four attempts with backoffs of 0.25, 0.5 and 1.0 s.

    Given: An insert chunk that always gets 40001.
    When: The voted plan is written.
    Then: Four attempts are made, the backoffs are 0.25, 0.5 and 1.0 s, and the run
        stops with the attempts exhausted.
    """
    digest = await _plan(repository, tmp_path)
    errors: list[BaseException | None] = [_db_error(sqlstate="40001") for _ in range(5)]
    sizes = _script(repository, "insert_missing_native_candles", errors)
    sleeps = _Sleeps()
    report = await _write(
        replace(fixed_context(repository), sleep=sleeps),
        tmp_path,
        "write",
        _options(tmp_path, digest),
    )
    assert report.stop_reason == "attempts_exhausted"
    assert len(sizes) == 4
    assert sleeps.calls == [0.25, 0.5, 1.0]


def _unique(**attributes: str | int) -> DBAPIError:
    """Build a unique violation as an IntegrityError."""
    return _db_error(IntegrityError, **attributes)


def _with_cause(constraint: str) -> DBAPIError:
    """Build an asyncpg-shaped violation whose constraint name sits on the cause."""
    error = _unique(sqlstate="23505")
    cause = _DriverError(constraint_name=constraint)
    cast(BaseException, error.orig).__cause__ = cause
    return error


def _with_diag(constraint: str) -> DBAPIError:
    """Build a psycopg-shaped violation whose constraint name sits on its diagnostics."""
    orig = _DriverError(sqlstate="23505", diag=MagicMock(constraint_name=constraint))
    return IntegrityError("INSERT INTO candles", {}, orig)


@pytest.mark.parametrize(
    "error",
    [
        _unique(sqlstate="23505", constraint_name="uq_candle_itf_open"),
        _with_cause("candles_p_uq_itf_open"),
        _with_diag("candles_d20261110_instrument_public_id_timeframe_open_at_idx"),
        _unique(
            sqlstate="23505",
            constraint_name="candles_default_instrument_public_id_timeframe_open_at_idx",
        ),
        IntegrityError("INSERT", {}, _DriverError(_NATURAL_MESSAGE, sqlite_errorcode=2067)),
    ],
)
def test_natural_key_violations_are_recognised_by_name(error: DBAPIError) -> None:
    """A unique violation of the candle natural key is a retryable conflict.

    Given: Violations naming the ordinary, the parent and two leaf indexes on any
        driver layer, and SQLite's column message.
    When: Each is classified.
    Then: Each is a natural-key conflict.
    """
    assert classify_failure(error) == "natural_key_conflict"


@pytest.mark.parametrize(
    "error",
    [
        _unique(sqlstate="23505", constraint_name="ix_candles_public_id"),
        _unique(sqlstate="23505"),
        _with_cause("trades_d20261110_instrument_public_id_timeframe_open_at_idx"),
        IntegrityError(
            "INSERT",
            {},
            _DriverError("UNIQUE constraint failed: candles.public_id", sqlite_errorcode=2067),
        ),
    ],
)
def test_other_unique_violations_are_not_conflicts(error: DBAPIError) -> None:
    """A violation of any other unique index stops the run as an error.

    Given: Violations of the public-id index, without a name, of another table's leaf,
        and SQLite's message for another column.
    When: Each is classified.
    Then: Each is an unrelated unique violation.
    """
    assert classify_failure(error) == "unique_violation"


@pytest.mark.parametrize(
    ("error", "outcome"),
    [
        (RuntimeError("boom"), "uncertain"),
        (ConnectionResetError("connection lost"), "uncertain"),
        (_db_error(sqlstate="57014"), "uncertain"),
        (_db_error(), "uncertain"),
        (_db_error(sqlstate="08006"), "uncertain"),
        (_db_error(sqlstate=5), "uncertain"),
        (_db_error(sqlstate="55P03"), "lock_timeout"),
        (_db_error(sqlstate="40001"), "serialization_failure"),
        (_db_error(sqlstate="40P01"), "deadlock"),
    ],
)
def test_failures_that_may_have_committed_are_uncertain(error: BaseException, outcome: str) -> None:
    """Only errors that prove nothing was committed allow a retry.

    Given: Non-database errors, a statement timeout, an unknown or a connection error,
        and the three retryable SQLSTATEs.
    When: Each is classified.
    Then: The first group is uncertain and the others name their retryable outcome.
    """
    assert classify_failure(error) == outcome


def test_leaf_index_names_follow_the_daily_partition_naming() -> None:
    """The recognised index names are those the partition module creates.

    Given: The candles parent index and the leaf names the partition module derives.
    When: They are checked.
    Then: The parent, every daily leaf, the default leaf and the ordinary index are
        recognised, and other names are not.
    """
    spec = daily_partitions._spec(daily_partitions.market_data_table("candles"))
    parent = next(index for index in spec.parent_indexes if index.unique)
    assert parent.name in NATURAL_KEY_INDEXES
    suffix = "_".join(parent.columns) + "_idx"
    for leaf in (
        daily_partitions._daily_name(spec, datetime(2026, 11, 10, tzinfo=FETCHED_AT.tzinfo)),
        f"{spec.table}_default",
    ):
        assert is_natural_key_index(f"{leaf}_{suffix}")
    assert is_natural_key_index("uq_candle_itf_open")
    assert not is_natural_key_index("candles_d2026111_" + suffix)
    assert not is_natural_key_index("candles_p_ix_instr_open")


@pytest.mark.asyncio
async def test_natural_key_conflict_is_retried_and_an_unrelated_violation_stops(
    repository: SQLAlchemyRepository, tmp_path: Path
) -> None:
    """A conflict on the natural key is retried; any other unique violation stops the run.

    Given: An insert chunk that first conflicts on the natural key and then succeeds, and
        a correction chunk that violates the public-id index.
    When: The voted plan is written.
    Then: The inserts are applied on the second attempt and the run stops on the
        correction without retrying it.
    """
    digest = await _plan(repository, tmp_path)
    _script(
        repository,
        "insert_missing_native_candles",
        [_unique(sqlstate="23505", constraint_name="uq_candle_itf_open")],
    )
    corrections = _script(
        repository,
        "correct_false_zero_candles",
        [_unique(sqlstate="23505", constraint_name="ix_candles_public_id")],
    )
    report = await _write(fixed_context(repository), tmp_path, "write", _options(tmp_path, digest))
    assert report.stop_reason == "unique_violation"
    assert corrections == [1]
    chunks = cast(list[JsonObject], _receipt(tmp_path, "write")["chunks"])
    assert [chunk["attempt_count"] for chunk in chunks] == [2, 1]
    assert _outcomes(chunks[0])["applied"] == 2
    assert _outcomes(chunks[1])["not_applied"] == 1


@pytest.mark.asyncio
async def test_max_rows_stops_early_and_a_resume_applies_only_what_is_left(
    repository: SQLAlchemyRepository, tmp_path: Path
) -> None:
    """A capped write leaves the rest for a resume with the same voted preview.

    Given: A voted plan of five rows.
    When: It is written with at most two rows, then written again in full.
    Then: The first write applies two rows and stops for the cap; the resume reports
        those two as already applied and applies the other three.
    """
    digest = await _plan(repository, tmp_path, minutes=5, zeros=2)
    first = await _write(
        fixed_context(repository), tmp_path, "first", _options(tmp_path, digest, max_rows=2)
    )
    receipt = _receipt(tmp_path, "first")
    assert (first.stop_reason, first.complete) == ("max_rows", True)
    assert (receipt["selected"], receipt["attempted"], receipt["left"]) == (5, 2, 3)
    assert first.totals["applied"] == 2

    second = await _write(fixed_context(repository), tmp_path, "second", _options(tmp_path, digest))
    resumed = _receipt(tmp_path, "second")
    assert second.stop_reason == "completed"
    assert second.totals["applied"] == 3
    assert [line["minute_utc"] for line in cast(list[JsonObject], resumed["already_applied"])] == [
        OPEN.isoformat(),
        (OPEN + timedelta(minutes=1)).isoformat(),
    ]
    assert resumed["changed"] == []
    assert len(await _current(repository)) == 5


@contextmanager
def _lost_acknowledgement(
    after_commit: Callable[[], Awaitable[None]] | None = None,
) -> Iterator[list[int]]:
    """Commit the first owned write, run an optional step, then lose its acknowledgement.

    Yields:
        The number of interrupted commits.
    """
    original = AsyncSession.commit
    interrupted: list[int] = []

    async def commit_then_drop(session: AsyncSession) -> None:
        """Commit durably and then fail as a lost connection would."""
        await original(session)
        if not interrupted:
            interrupted.append(1)
            if after_commit is not None:
                await after_commit()
            raise ConnectionResetError("connection to server lost for secret-user")

    with pytest.MonkeyPatch.context() as patch:
        patch.setattr(AsyncSession, "commit", commit_then_drop)
        yield interrupted


@pytest.mark.asyncio
async def test_lost_acknowledgement_after_a_commit_is_reconciled_as_applied(
    repository: SQLAlchemyRepository, tmp_path: Path
) -> None:
    """A chunk whose commit succeeded but whose answer was lost is found as applied.

    Given: An insert chunk whose commit succeeds before the connection is lost.
    When: The voted plan is written.
    Then: The chunk is reconciled as applied, the run stops as uncertain, and no
        correction is attempted.
    """
    digest = await _plan(repository, tmp_path)
    corrections = _script(repository, "correct_false_zero_candles", [])
    with _lost_acknowledgement() as interrupted:
        report = await _write(
            fixed_context(repository), tmp_path, "write", _options(tmp_path, digest)
        )
    assert interrupted == [1]
    assert (report.stop_reason, report.complete) == ("uncertain_acknowledgement", False)
    assert corrections == []
    chunk = cast(list[JsonObject], _receipt(tmp_path, "write")["chunks"])[0]
    assert _outcomes(chunk)["applied"] == 2
    assert cast(list[JsonObject], chunk["attempts"])[0]["outcome"] == "uncertain"
    assert "secret-user" not in (tmp_path / "write" / "receipt.json").read_text()


@pytest.mark.asyncio
async def test_lost_acknowledgement_of_a_row_superseded_before_reconciliation(
    repository: SQLAlchemyRepository, tmp_path: Path
) -> None:
    """A committed row that a later writer superseded is found as applied and superseded.

    Given: An insert chunk whose commit succeeds, after which a live writer versions
        one of its minutes, before the acknowledgement is lost.
    When: The voted plan is written.
    Then: That row is ``applied_superseded`` and the other ``applied``.
    """
    digest = await _plan(repository, tmp_path)

    async def live_update() -> None:
        """Version the second inserted minute with a later, different bar."""
        later = candle(
            XRP, OPEN + timedelta(minutes=1), source="native", close=2.0, volume=1.0, trades=1
        )
        await repository.upsert_candles([later | {"timestamp": datetime.now(UTC)}])

    with _lost_acknowledgement(live_update):
        report = await _write(
            replace(fixed_context(repository), clock=lambda: datetime.now(UTC)),
            tmp_path,
            "write",
            _options(tmp_path, digest),
        )
    assert report.stop_reason == "uncertain_acknowledgement"
    chunk = cast(list[JsonObject], _receipt(tmp_path, "write")["chunks"])[0]
    assert _outcomes(chunk) == {
        "applied": 1,
        "skipped_by_guard": 0,
        "applied_superseded": 1,
        "uncertain": 0,
        "not_applied": 0,
    }
    superseded = cast(list[JsonObject], chunk["applied_superseded"])[0]
    assert superseded["minute_utc"] == (OPEN + timedelta(minutes=1)).isoformat()


@contextmanager
def _statements(repository: SQLAlchemyRepository) -> Iterator[list[str]]:
    """Capture the SQL text sent to the driver."""
    captured: list[str] = []

    def capture(*arguments: object) -> None:
        """Record one statement in upper case."""
        captured.append(cast(str, arguments[2]).strip().upper())

    event.listen(repository.engine.sync_engine, "before_cursor_execute", capture)
    try:
        yield captured
    finally:
        event.remove(repository.engine.sync_engine, "before_cursor_execute", capture)


@pytest.mark.asyncio
async def test_reconciliation_while_the_original_transaction_is_open_is_uncertain_and_lockless(
    repository: SQLAlchemyRepository, tmp_path: Path
) -> None:
    """A chunk whose transaction is still open is uncertain, never skipped, and nothing waits.

    Given: An insert chunk whose rows are staged in a transaction that stays open, with
        SQLite's write lock held, when the connection error arrives.
    When: The voted plan is written.
    Then: The reconciliation reads without waiting for that lock, issues only reads,
        reports every row uncertain and the run stops.
    """
    digest = await _plan(repository, tmp_path)
    held: list[AsyncSession] = []
    statements: list[list[str]] = []
    original_reconcile = candle_repair.reconcile

    async def stage_then_fail(
        rows: list[CandleUpsertRow], **options: object
    ) -> list[CandleUpsertRow]:
        """Stage the rows in an open transaction and fail without finishing it."""
        assert not options.get("dry_run")
        session = repository.session_factory()
        held.append(session)
        for row in rows:
            await session.execute(
                insert(Candle).values(**(row | {"public_id": f"staged-{row['sequence_id']}"}))
            )
        raise ConnectionResetError("connection lost")

    async def observed(
        repo: SQLAlchemyRepository, rows: list[CandleUpsertRow], as_of: datetime
    ) -> dict[tuple[str, datetime], str]:
        """Reconcile while capturing the statements it sends."""
        with _statements(repository) as captured:
            outcome = await asyncio.wait_for(original_reconcile(repo, rows, as_of), 5)
        statements.append(captured)
        return outcome

    planned = await _write_with(repository, tmp_path, digest, stage_then_fail, observed)
    try:
        assert planned.stop_reason == "uncertain_acknowledgement"
        chunk = cast(list[JsonObject], _receipt(tmp_path, "write")["chunks"])[0]
        assert _outcomes(chunk)["uncertain"] == 2
        assert statements and all(sql.startswith("SELECT") for sql in statements[0])
    finally:
        for session in held:
            await session.rollback()
            await session.close()


async def _write_with(
    repository: SQLAlchemyRepository,
    tmp_path: Path,
    digest: str,
    insert_writer: _Writer,
    reconciler: Callable[..., Awaitable[dict[tuple[str, datetime], str]]],
) -> WriteReport:
    """Write with a replaced insert writer and reconciliation."""
    original = cast(_Writer, repository.insert_missing_native_candles)

    async def dispatch(rows: list[CandleUpsertRow], **options: object) -> list[CandleUpsertRow]:
        """Let dry runs through and send writes to the replacement."""
        if options.get("dry_run"):
            return await original(rows, **options)
        return await insert_writer(rows, **options)

    repository.insert_missing_native_candles = dispatch
    with pytest.MonkeyPatch.context() as patch:
        patch.setattr(candle_repair, "reconcile", reconciler)
        return await _write(
            fixed_context(repository), tmp_path, "write", _options(tmp_path, digest)
        )


@pytest.mark.asyncio
async def test_reconciliation_failure_leaves_the_chunk_uncertain(
    repository: SQLAlchemyRepository, tmp_path: Path
) -> None:
    """A reconciliation that fails cannot turn rows into skips.

    Given: An uncertain insert chunk whose reconciliation read fails.
    When: The voted plan is written.
    Then: Every row stays uncertain and the failure is named by class only.
    """
    digest = await _plan(repository, tmp_path)

    async def lost(rows: list[CandleUpsertRow], **options: object) -> list[CandleUpsertRow]:
        """Fail as a lost connection."""
        del rows, options
        raise ConnectionResetError("lost")

    async def failing(*arguments: object) -> dict[tuple[str, datetime], str]:
        """Fail the reconciliation read."""
        del arguments
        raise _db_error(sqlstate="57014")

    report = await _write_with(repository, tmp_path, digest, lost, failing)
    assert report.stop_reason == "uncertain_acknowledgement"
    chunk = cast(list[JsonObject], _receipt(tmp_path, "write")["chunks"])[0]
    assert _outcomes(chunk)["uncertain"] == 2
    assert chunk["reconciliation_error"] == {
        "error": "OperationalError",
        "driver_error": "_DriverError",
        "sqlstate": "57014",
    }


@pytest.mark.asyncio
async def test_reconciliation_on_postgresql_reads_committed_without_a_lock() -> None:
    """On PostgreSQL the reconciliation is one READ COMMITTED read with a statement bound.

    Given: A PostgreSQL repository double holding this run's version of one row.
    When: Two rows are reconciled.
    Then: The transaction is set to READ COMMITTED before its statement timeout and its
        only read; the found row is applied and the other uncertain.
    """
    row = candle(XRP, OPEN) | {"session_id": "run", "sequence_id": 1, "source": "native"}
    missing = candle(XRP, OPEN + timedelta(minutes=1)) | {"session_id": "run", "sequence_id": 2}
    stored = row | {"id": 3, "public_id": "p", "known_to": KNOWN_TO_MAX}
    statements: list[str] = []
    result = MagicMock()
    result.mappings.return_value = [stored]

    async def execute(statement: object) -> MagicMock:
        """Record one statement."""
        statements.append(str(statement))
        return result

    @asynccontextmanager
    async def open_session() -> AsyncIterator[MagicMock]:
        """Yield the recording session."""
        yield MagicMock(execute=AsyncMock(side_effect=execute))

    repository = MagicMock(dialect_name="postgresql", session=open_session)
    outcomes = await reconcile(repository, [row, missing], RUN_TIME)
    assert statements[:2] == [
        "SET TRANSACTION ISOLATION LEVEL READ COMMITTED",
        "SET LOCAL statement_timeout = '20s'",
    ]
    assert len(statements) == 3 and statements[2].startswith("SELECT")
    assert not any("LOCK" in statement for statement in statements)
    assert outcomes == {(XRP, OPEN): "applied", (XRP, OPEN + timedelta(minutes=1)): "uncertain"}


@pytest.mark.asyncio
async def test_failure_mid_run_still_leaves_both_receipts(
    repository: SQLAlchemyRepository, tmp_path: Path
) -> None:
    """A failure between chunks propagates after ``receipt.json`` is written.

    Given: A voted plan of two chunks and a pause that fails after the first chunk.
    When: The voted plan is written.
    Then: The failure propagates, ``receipt.jsonl`` holds the finished chunk and
        ``receipt.json`` records it with the stop reason ``error``.
    """
    digest = await _plan(repository, tmp_path)

    async def broken_pause(seconds: float) -> None:
        """Fail instead of pausing."""
        del seconds
        raise RuntimeError("pause failed")

    with pytest.raises(RuntimeError, match="pause failed"):
        await _write(
            replace(fixed_context(repository), sleep=broken_pause),
            tmp_path,
            "write",
            _options(tmp_path, digest),
        )
    receipt = _receipt(tmp_path, "write")
    assert receipt["stop_reason"] == "error"
    assert len(cast(list[JsonObject], receipt["chunks"])) == 1
    assert len(_lines(tmp_path, "write")) == 1


@pytest.mark.asyncio
async def test_cancellation_leaves_a_receipt_with_the_chunk_uncertain(
    repository: SQLAlchemyRepository, tmp_path: Path
) -> None:
    """A cancelled chunk may still commit, so its rows are uncertain in the receipt.

    Given: An insert chunk cancelled during its attempt.
    When: The voted plan is written.
    Then: The cancellation propagates and ``receipt.json`` shows the cancelled attempt
        and the chunk uncertain, with the stop reason ``cancelled``.
    """
    digest = await _plan(repository, tmp_path)
    _script(repository, "insert_missing_native_candles", [asyncio.CancelledError()])
    with pytest.raises(asyncio.CancelledError):
        await _write(fixed_context(repository), tmp_path, "write", _options(tmp_path, digest))
    receipt = _receipt(tmp_path, "write")
    assert receipt["stop_reason"] == "cancelled"
    chunk = cast(list[JsonObject], receipt["chunks"])[0]
    assert _outcomes(chunk)["uncertain"] == 2
    assert chunk["attempts"] == [
        {
            "attempt": 1,
            "outcome": "cancelled",
            "error": "CancelledError",
            "driver_error": None,
            "sqlstate": None,
        }
    ]


@pytest.mark.asyncio
async def test_correction_whose_target_changed_is_reported_and_not_written(
    repository: SQLAlchemyRepository, tmp_path: Path
) -> None:
    """A live writer that replaces the voted zero removes that correction from the write.

    Given: A voted correction whose zero a live writer replaced with another native bar.
    When: The voted plan is written.
    Then: The correction is reported as changed, the live row stays current and the
        inserts are applied.
    """
    digest = await _plan(repository, tmp_path)
    live = candle(XRP, OPEN, source="native", close=3.0, volume=2.0, trades=4, timestamp=RUN_TIME)
    await repository.upsert_candles([live])
    report = await _write(fixed_context(repository), tmp_path, "write", _options(tmp_path, digest))
    receipt = _receipt(tmp_path, "write")
    assert report.stop_reason == "completed"
    assert [line["minute_utc"] for line in cast(list[JsonObject], receipt["changed"])] == [
        OPEN.isoformat()
    ]
    assert receipt["not_voted"] == []
    current = await _current(repository)
    assert (current[0]["close"], current[0]["session_id"]) == (3.0, "live")
    assert report.totals["applied"] == 2


@pytest.mark.asyncio
async def test_rows_outside_the_vote_are_not_voted_and_never_written(
    repository: SQLAlchemyRepository, tmp_path: Path
) -> None:
    """Only the voted subset of the plan is written.

    Given: A plan of three rows and a vote, hashed again, that leaves out one insert.
    When: The vote is written.
    Then: That insert is reported as not voted and its minute stays absent.
    """
    await _plan(repository, tmp_path)
    plan_lines = (tmp_path / "plan" / "preview.csv").read_text().splitlines()
    kept = [line for line in plan_lines if (OPEN + timedelta(minutes=2)).isoformat() not in line]
    preview = ("\n".join(kept) + "\n").encode()
    approval = json.loads((tmp_path / "plan" / "approval.json").read_text())
    approval["preview_sha256"] = sha256_hex(preview)
    voted = tmp_path / "voted"
    voted.mkdir()
    (voted / "preview.csv").write_bytes(preview)
    (voted / "approval.json").write_bytes(canonical_json(approval))
    options = WriteOptions(voted, sha256_hex(canonical_json(approval)), None, 0.0, 3)
    report = await _write(fixed_context(repository), tmp_path, "write", options)
    receipt = _receipt(tmp_path, "write")
    assert report.stop_reason == "completed"
    assert receipt["not_voted"] == [
        {
            "instrument_public_id": XRP,
            "minute_utc": (OPEN + timedelta(minutes=2)).isoformat(),
            "action": "insert",
        }
    ]
    assert [row["open_at"] for row in await _current(repository)] == [
        OPEN,
        OPEN + timedelta(minutes=1),
    ]
    assert preview_bytes([]).decode() in preview.decode()


def test_error_fields_never_carry_an_error_text() -> None:
    """Receipts and refusals name an error by class and SQLSTATE only.

    Given: A plain error and a database error whose texts name a user.
    When: They are described.
    Then: No description holds the text.
    """
    plain = error_fields(RuntimeError("secret-user"))
    database = error_fields(_db_error(sqlstate="57014"))
    assert plain == {"error": "RuntimeError", "driver_error": None, "sqlstate": None}
    assert database == {
        "error": "OperationalError",
        "driver_error": "_DriverError",
        "sqlstate": "57014",
    }
    assert "secret-user" not in json.dumps([plain, database])


@pytest.mark.asyncio
async def test_trades_write_applies_calculated_rows_and_corrections_at_the_write_time(
    repository: SQLAlchemyRepository, tmp_path: Path
) -> None:
    """A trades write inserts calculated rows and corrects zeros at its single write time.

    Given: Stored trades for two minutes, the second of which holds a flat zero, and a
        voted trades plan.
    When: The plan is written at a later write time.
    Then: The absent minute is inserted and the zero corrected, both calculated, and
        the correction is known at the write time.
    """
    await repository.upsert_trades(
        [
            trade(XRP, OPEN + timedelta(seconds=10), 1.0, 2.0, "t1"),
            trade(XRP, OPEN + timedelta(minutes=1, seconds=5), 1.5, 1.0, "t2"),
        ]
    )
    await repository.upsert_candles([candle(XRP, OPEN + timedelta(minutes=1))])
    window = RepairWindow(OPEN, OPEN + timedelta(minutes=2))
    context = fixed_context(repository)
    plan = await plan_repair(context, trades_request(tmp_path / "plan", window, ("XRP-EUR",)))
    write_time = RUN_TIME + timedelta(minutes=5)

    report = await write_repair(
        replace(context, clock=lambda: write_time),
        trades_request(tmp_path / "write", window, ("XRP-EUR",)),
        WriteOptions(tmp_path / "plan", plan.digest, None, 0.0, 3),
    )

    assert report.stop_reason == "completed"
    assert report.totals["applied"] == 2
    current = await _current(repository)
    assert [(row["source"], row["trades"], row["sequence_id"]) for row in current] == [
        ("calculated", 1, 1),
        ("calculated", 1, 2),
    ]
    assert current[1]["timestamp"] == write_time
    assert current[0]["timestamp"] != write_time
