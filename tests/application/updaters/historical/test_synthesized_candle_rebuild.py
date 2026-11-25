"""Exercise authenticated rebuilds and verification against disposable SQLite."""

import asyncio
import csv
import io
import json
from collections.abc import AsyncIterator
from collections.abc import Mapping
from collections.abc import Sequence
from contextlib import asynccontextmanager
from dataclasses import dataclass
from dataclasses import replace
from datetime import UTC
from datetime import datetime
from datetime import timedelta
from math import inf
from math import nan
from math import nextafter
from pathlib import Path
from typing import cast
from unittest.mock import AsyncMock
from unittest.mock import MagicMock
from uuid import uuid4
from uuid import uuid7

import pytest
import pytest_asyncio
from loguru import logger
from sqlalchemy import insert
from sqlalchemy import select
from sqlalchemy import text
from sqlalchemy import update
from sqlalchemy.dialects.postgresql.asyncpg import PGDialect_asyncpg
from sqlalchemy.exc import OperationalError
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.sql import Select

from snapper.application.updaters.historical import candle_repair as repair
from snapper.application.updaters.historical import synthesized_candle_rebuild as rebuild
from snapper.application.updaters.historical.guarded_candle_write import CandleKey
from snapper.core.json_types import JsonObject
from snapper.data.models import Candle
from snapper.data.models import Instrument
from snapper.data.models import ProcessRun
from snapper.data.repository import SQLAlchemyRepository
from snapper.data.repository_types import CandleUpsertRow
from snapper.data.repository_types import CandleVersionRow
from snapper.data.repository_types import ClientSample
from snapper.data.repository_types import SynthesizedRebuildExpectation
from snapper.data.repository_types import SynthesizedRebuildOutcome
from snapper.data.repository_types import SynthesizedRebuildResult
from tests.helpers.candle_repair import OPEN
from tests.helpers.candle_repair import XRP
from tests.helpers.candle_repair import candle
from tests.helpers.candle_repair import install_exclusive_file_spy
from tests.helpers.candle_repair import kraken_row
from tests.helpers.candle_repair import seed_instrument
from tests.helpers.candle_repair import snapshot_request
from tests.helpers.candle_repair import trade
from tests.helpers.candle_repair import write_snapshot

_END = datetime(2026, 11, 11, tzinfo=UTC)
_REVISION = {"application.py": "a" * 64}
_IMAGE = "sha256:" + "1" * 64
_OPTIONS = ("approval.json", "receipt.json", "receipt.jsonl")


def _document(path: Path) -> JsonObject:
    """Read one artifact as JSON."""
    return cast(JsonObject, json.loads(path.read_bytes()))


def _save(path: Path, document: JsonObject) -> Path:
    """Write a hand-built coordinator document in the test directory."""
    path.write_bytes(repair.canonical_json(document))
    return path


def _binding(directory: Path) -> JsonObject:
    """Record the hashes and terminal identity of one test write."""
    receipt = _document(directory / repair.RECEIPT_FILE)
    hashes = {
        key: repair.sha256_hex((directory / name).read_bytes())
        for key, name in zip(
            ("approval_sha256", "receipt_sha256", "receipt_lines_sha256"), _OPTIONS, strict=True
        )
    }
    return {
        **hashes,
        **{key: receipt[key] for key in ("run_id", "voted_digest", "stop_reason")},
    }


def _record(directory: Path) -> JsonObject:
    """Build the full machine-readable run record for the SQLite repair."""
    receipt = _document(directory / repair.RECEIPT_FILE)
    start = datetime.fromisoformat(cast(str, receipt["started_at"])) - timedelta(seconds=1)
    finish = datetime.fromisoformat(cast(str, receipt["ended_at"])) + timedelta(seconds=1)
    return {
        **_binding(directory),
        "application_name": None,
        "observed_addresses": ["172.18.0.9/32"],
        "container": {
            "name": "repair-test",
            "image_id": _IMAGE,
            "started_at": start.isoformat(),
            "finished_at": finish.isoformat(),
            "addresses": ["172.18.0.9"],
        },
        "settlements": [],
    }


@dataclass(frozen=True, slots=True)
class _Run:
    """One SQLite repair write and its standalone coordinator record."""

    root: Path
    repository: SQLAlchemyRepository
    context: repair.RunContext
    repair_dir: Path
    records: Path

    def request(
        self, name: str, end: datetime = _END, verification: rebuild.VerificationFiles | None = None
    ) -> rebuild.RebuildRequest:
        """Create a new rebuild output directory beside the evidence."""
        output = self.root / name
        output.mkdir()
        return rebuild.RebuildRequest((self.repair_dir,), self.records, end, output, verification)

    def evidence(self, directory: Path) -> rebuild.VerificationFiles:
        """Bind a test rebuild write to a machine-readable verification record."""
        record = _save(self.root / "verification.json", _binding(directory))
        return rebuild.VerificationFiles(directory, record)


@pytest_asyncio.fixture
async def recorded_repair(tmp_path: Path) -> AsyncIterator[_Run]:
    """Repair one zero and one absent minute, then bind its actual receipt."""
    url = f"sqlite+aiosqlite:///{tmp_path / 'scratch.db'}"
    repository = SQLAlchemyRepository(url)
    await repository.create_all()
    try:
        await seed_instrument(repository, XRP, "XRP-EUR", "XXRPZEUR")
        zero = candle(XRP, OPEN, timestamp=OPEN + timedelta(minutes=1))
        await repository.upsert_candles([zero])
        snapshot = tmp_path / "snapshot"
        write_snapshot(
            snapshot,
            "XXRPZEUR",
            [
                kraken_row(OPEN, 2),
                kraken_row(OPEN + timedelta(minutes=1), 3),
                kraken_row(OPEN + timedelta(minutes=2), 1),
            ],
            (OPEN + timedelta(minutes=2, seconds=30)).timestamp(),
        )
        bound = repair.RunContext(repository, repair.database_endpoint(url), _REVISION)
        preview, directory = tmp_path / "repair-vote", tmp_path / "repair-write"
        preview.mkdir()
        directory.mkdir()
        vote = await repair.plan_repair(bound, snapshot_request(snapshot, preview))
        report = await repair.write_repair(
            bound,
            snapshot_request(snapshot, directory),
            repair.WriteOptions(preview, vote.digest, None, 0, 3),
        )
        assert report.totals["applied"] == 2
        record = _save(tmp_path / "runs.json", {"runs": [_record(directory)]})
        yield _Run(
            tmp_path,
            repository,
            replace(bound, run_id=str(uuid7())),
            directory,
            record,
        )
    finally:
        await repository.engine.dispose()


def _vote(directory: Path, digest: str, max_rows: int | None = None) -> repair.WriteOptions:
    """Construct an approved rebuild call without sleeps."""
    return repair.WriteOptions(directory, digest, max_rows, 0, 3)


def _preview(directory: Path) -> list[dict[str, str]]:
    """Read an emitted CSV in row order."""
    with (directory / repair.PREVIEW_FILE).open(newline="") as handle:
        return list(csv.DictReader(handle))


def _verification(artifacts: rebuild.PlanArtifacts, run_id: str) -> JsonObject:
    """Read the independent observation of one write from the summary."""
    snapshots = cast(JsonObject, artifacts.summary["verification"])
    return cast(JsonObject, snapshots[run_id])


async def _higher_rows(repository: SQLAlchemyRepository) -> list[CandleVersionRow]:
    """Read current and closed higher-timeframe test rows."""
    async with repository.session() as session:
        selected = await session.execute(select(Candle.__table__).where(Candle.timeframe != "1m"))
        return [cast(CandleVersionRow, dict(row)) for row in selected.mappings()]


@pytest.mark.asyncio
async def test_actual_repair_vote_write_resume_and_coherent_verification(
    recorded_repair: _Run,
) -> None:
    """Rebuild all six sparse windows in plan order and verify the first write's full scope.

    Given: An actual one-minute repair and six sparse higher-timeframe windows.
    When: Approve a rebuild, apply part of it, verify the receipt and then resume.
    Then: Verification covers the scope and the resumed write applies the remaining windows.
    """
    run = recorded_repair
    dry = run.request("voted")
    voted = await rebuild.plan_rebuild(run.context, dry)
    repeat = await rebuild.plan_rebuild(run.context, run.request("repeat"))
    assert repeat.digest == voted.digest
    assert voted.summary["actions"] == {"insert": 6}
    assert voted.summary["latest_window_end"] == _END.isoformat()
    preview = _preview(dry.output_dir)
    assert [row["timeframe"] for row in preview] == list(rebuild.TIMEFRAMES)
    assert [row["new_trades"] for row in preview] == ["5"] * 6
    assert [row["input_minutes"] for row in preview] == ["2"] * 6
    assert [row["absent_minutes"] for row in preview] == ["3", "13", "28", "58", "238", "1438"]
    assert {row["new_complete"] for row in preview} == {"true"}
    assert await _higher_rows(run.repository) == []
    assert all(path.stat().st_mode & 0o777 == 0o600 for path in dry.output_dir.iterdir())

    write = run.request("write")
    first = await rebuild.write_rebuild(run.context, write, _vote(dry.output_dir, voted.digest, 5))
    assert (first.stop_reason, first.totals["applied"]) == ("max_rows", 5)
    receipt = _document(write.output_dir / repair.RECEIPT_FILE)
    assert receipt["run_id"] == run.context.run_id
    assert receipt["sole_client_write"] == "not_applicable"
    assert receipt["start_sample"] is None
    chunks = cast(list[JsonObject], receipt["chunks"])
    assert len(chunks) == 1
    assert chunks[0]["sole_client"] == "not_applicable"
    assert chunks[0]["client_sample"] is None
    assert [row["sequence_id"] for row in await _higher_rows(run.repository)] == [1, 2, 3, 4, 5]

    request = run.request("verify", verification=run.evidence(write.output_dir))
    observed = _verification(await rebuild.plan_rebuild(run.context, request), run.context.run_id)
    assert cast(JsonObject, observed["counts"]) == {
        "verified": 5,
        "regressed": 0,
        "uncertain": 0,
        "outstanding": 1,
        "unchanged_at_write": 0,
        "excluded": 0,
    }
    outstanding = [
        row for row in cast(list[JsonObject], observed["windows"]) if row["class"] == "outstanding"
    ]
    assert [(row["timeframe"], row["reason"]) for row in outstanding] == [("1d", "not_reached")]

    second_context = replace(run.context, run_id=str(uuid7()))
    resume = await rebuild.write_rebuild(
        second_context, run.request("resume"), _vote(dry.output_dir, voted.digest)
    )
    assert (resume.stop_reason, resume.totals["applied"]) == ("completed", 1)
    assert len(await _higher_rows(run.repository)) == 6


@pytest.mark.asyncio
async def test_live_regression_is_observed_and_a_later_vote_restores_the_fold(
    recorded_repair: _Run,
) -> None:
    """Restore the rebuilt candle with a fresh vote after a stale live fold supersedes it.

    Given: A live candle that supersedes the rebuilt value with a stale fold.
    When: Verify the original write, then preview, approve and apply a fresh rebuild.
    Then: Verification reports the regression and a later check confirms the repaired fold.
    """
    run = recorded_repair
    dry, write = run.request("dry"), run.request("write")
    voted = await rebuild.plan_rebuild(run.context, dry)
    await rebuild.write_rebuild(run.context, write, _vote(dry.output_dir, voted.digest))
    row = next(row for row in await _higher_rows(run.repository) if row["timeframe"] == "5m")
    stale = {key: value for key, value in row.items() if key not in {"id", "known_to"}}
    stale.update({"volume": 0.0, "trades": 0, "timestamp": datetime.now(UTC), "session_id": "live"})
    await run.repository.upsert_candles([cast(CandleUpsertRow, stale)])

    evidence = run.evidence(write.output_dir)
    check = await rebuild.plan_rebuild(run.context, run.request("regressed", verification=evidence))
    observed = _verification(check, run.context.run_id)
    assert observed["counts"] == {
        "verified": 5,
        "regressed": 1,
        "uncertain": 0,
        "outstanding": 0,
        "unchanged_at_write": 0,
        "excluded": 0,
    }
    regressed = next(
        row for row in cast(list[JsonObject], observed["windows"]) if row["class"] == "regressed"
    )
    assert regressed["timeframe"] == "5m" and regressed["still_written"] is False

    context = replace(run.context, run_id=str(uuid7()))
    retry_vote = run.request("retry-vote")
    retry = await rebuild.plan_rebuild(context, retry_vote)
    retry_write = run.request("retry")
    report = await rebuild.write_rebuild(
        context, retry_write, _vote(retry_vote.output_dir, retry.digest)
    )
    assert report.totals["applied"] == 1
    final = await rebuild.plan_rebuild(
        context, run.request("recovered", verification=run.evidence(retry_write.output_dir))
    )
    assert _verification(final, context.run_id)["counts"] == {
        "verified": 1,
        "regressed": 0,
        "uncertain": 0,
        "outstanding": 0,
        "unchanged_at_write": 5,
        "excluded": 0,
    }


@pytest.mark.asyncio
async def test_window_end_is_inclusive_and_later_windows_remain_in_scope(
    recorded_repair: _Run,
) -> None:
    """Read the 5m window ending exactly at the bound and report the rest as after_end.

    Given: A rebuild ending exactly at the close of the shortest window.
    When: Plan, apply and verify the bounded rebuild.
    Then: The 5m window is included and later windows remain visible as exclusions.
    """
    run = recorded_repair
    request = run.request("short", OPEN + timedelta(minutes=5))
    plan = await rebuild.plan_rebuild(run.context, request)
    assert plan.summary["actions"] == {"insert": 1, "after_end": 5}
    rows = _preview(request.output_dir)
    assert rows[0]["window_start_utc"] == OPEN.isoformat()
    assert rows[1]["window_end_utc"] == (OPEN + timedelta(minutes=15)).isoformat()
    assert rows[1]["input_minutes"] == ""
    write = run.request("write-short", request.end)
    await rebuild.write_rebuild(run.context, write, _vote(request.output_dir, plan.digest))
    check = await rebuild.plan_rebuild(
        run.context, run.request("verify-short", request.end, run.evidence(write.output_dir))
    )
    observed = _verification(check, run.context.run_id)
    assert cast(JsonObject, observed["counts"])["excluded"] == 5


@pytest.mark.parametrize(
    "mutated",
    [
        "record_shape",
        "missing_directory",
        "missing_record",
        "approval",
        "receipt",
        "receipt_lines",
        "preview",
        "duplicate_run",
        "image_id",
    ],
)
@pytest.mark.asyncio
async def test_bad_repair_evidence_never_reaches_a_session(
    recorded_repair: _Run, monkeypatch: pytest.MonkeyPatch, mutated: str
) -> None:
    """Refuse records, mismatched bytes and missing directory bindings before DB access.

    Given: Missing, altered or mismatched recorded repair evidence.
    When: Plan the rebuild with that evidence.
    Then: The command refuses before opening a repository session.
    """
    run = recorded_repair
    request = run.request("bad")
    manifest = _document(run.records)
    records = cast(list[JsonObject], manifest["runs"])
    if mutated == "record_shape":
        records[0]["extra"] = True
    elif mutated == "missing_directory":
        request = replace(request, repair_runs=())
    elif mutated == "missing_record":
        records[0]["run_id"] = str(uuid7())
    elif mutated == "duplicate_run":
        records.append(records[0])
    elif mutated == "image_id":
        cast(JsonObject, records[0]["container"])["image_id"] = "abcdef"
    else:
        target = (
            run.repair_dir
            / {
                "approval": repair.APPROVAL_FILE,
                "receipt": repair.RECEIPT_FILE,
                "receipt_lines": repair.RECEIPT_LINES_FILE,
                "preview": repair.PREVIEW_FILE,
            }[mutated]
        )
        target.write_bytes(target.read_bytes() + b" ")
    _save(run.records, manifest)
    forbidden = MagicMock()
    monkeypatch.setattr(run.repository, "session", forbidden)
    with pytest.raises(repair.RepairRefusedError):
        await rebuild.plan_rebuild(run.context, request)
    forbidden.assert_not_called()


@pytest.mark.parametrize("case", ["end", "db", "code", "digest", "verify"])
@pytest.mark.asyncio
async def test_write_refuses_wrong_vote_before_session(
    recorded_repair: _Run, monkeypatch: pytest.MonkeyPatch, case: str
) -> None:
    """Verify preflight refusals before catalog or candle reads.

    Given: An otherwise valid rebuild vote with a mismatched binding or invalid mode.
    When: Attempt the confirmed write.
    Then: The command refuses before opening a repository session.
    """
    run = recorded_repair
    dry = run.request("voted")
    vote = await rebuild.plan_rebuild(run.context, dry)
    options = _vote(dry.output_dir, vote.digest)
    request, context = run.request("refused"), run.context
    if case == "end":
        request = replace(request, end=OPEN)
    elif case == "db":
        context = replace(context, database={"dialect": "sqlite", "database": "other"})
    elif case == "code":
        context = replace(context, code_revision={"application.py": "b" * 64})
    elif case == "digest":
        options = replace(options, expect_digest="0" * 64)
    elif case == "verify":
        request = replace(request, verification=rebuild.VerificationFiles(run.root, run.records))
    forbidden = MagicMock()
    monkeypatch.setattr(run.repository, "session", forbidden)
    with pytest.raises(repair.RepairRefusedError):
        await rebuild.write_rebuild(context, request, options)
    forbidden.assert_not_called()


@pytest.mark.asyncio
async def test_retry_reuses_rows_and_lost_acknowledgement_is_reconciled(
    recorded_repair: _Run, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Use the shared retry loop, then find committed rows despite an acknowledgement failure.

    Given: A guarded chunk that rolls back, then commits without acknowledging the retry.
    When: Write the approved rebuild and reconcile the ambiguous attempt.
    Then: The retry uses identical rows and the receipt records the versions found.
    """
    run = recorded_repair
    dry = run.request("dry")
    plan = await rebuild.plan_rebuild(run.context, dry)
    original = run.repository.rebuild_synthesized_candles
    captured: list[list[CandleUpsertRow]] = []

    async def write_and_lose_acknowledgement(
        rows: list[CandleUpsertRow],
        *,
        expected: Mapping[CandleKey, SynthesizedRebuildExpectation],
        dry_run: bool = False,
    ) -> SynthesizedRebuildResult:
        """First prove rollback, then commit and lose the second acknowledgement."""
        if dry_run:
            return await original(rows, expected=expected, dry_run=True)
        captured.append([row.copy() for row in rows])
        if len(captured) == 1:
            raise OperationalError(None, None, MagicMock(sqlstate="55P03"))
        await original(rows, expected=expected)
        raise RuntimeError("lost acknowledgement")

    monkeypatch.setattr(
        run.repository, "rebuild_synthesized_candles", write_and_lose_acknowledgement
    )
    sleep = AsyncMock()
    context = replace(run.context, sleep=sleep)
    write = run.request("write")
    report = await rebuild.write_rebuild(context, write, _vote(dry.output_dir, plan.digest))
    assert report.stop_reason == "uncertain_acknowledgement"
    assert report.totals["applied"] == 5
    assert report.totals["uncertain"] == 0
    assert len(captured) == 2 and captured[0] == captured[1]
    sleep.assert_awaited_once_with(0.25)


@pytest.mark.parametrize(
    ("start", "chunks", "expected"),
    [
        (None, [], "no_writes"),
        (0, ["not_observed"], "no_writes"),
        (0, [True, True], True),
        (1, [True], False),
        (0, [True, False], False),
        (None, [False], False),
    ],
)
def test_run_samples_report_only_observed_sole_client_status(
    start: int | None, chunks: list[bool | str], expected: bool | str
) -> None:
    """Report sole-client status from observed chunk outcomes.

    Given: Run receipts with acknowledged, uncertain and no-write outcomes.
    When: Summarize the sole-client observations for the run.
    Then: The reported status reflects only the observations and outcomes in the receipt.
    """
    sample = ClientSample(start, _END) if start is not None else None
    records: list[JsonObject] = [{"sole_client": value} for value in chunks]
    assert rebuild._sole_client_write("postgresql", sample, records) == expected


@pytest.mark.parametrize("latest", ["running_new", "running_old", "finished", "no_publisher"])
@pytest.mark.asyncio
async def test_recorded_process_guard_uses_newest_starts(
    recorded_repair: _Run, latest: str
) -> None:
    """Refuse a newer running writer and accept records of older or stopped writers.

    Given: Recorded publisher and candle writer start times.
    When: Plan the rebuild with each writer status and start time.
    Then: An active writer without a qualifying publisher start is refused and older or stopped
        writers are allowed.
    """
    run = recorded_repair
    at = datetime.now(UTC)
    common = {
        "role": "process_runner",
        "lifecycle": "oneshot",
        "timestamp": OPEN,
        "session_id": "test",
        "sequence_id": 1,
    }
    async with run.repository.session() as session:
        if latest != "no_publisher":
            session.add(
                ProcessRun(
                    process_name="kraken_feed_publisher",
                    tags=["publisher", "market-data"],
                    status="running",
                    started_at=at - timedelta(hours=2),
                    **common,
                )
            )
        session.add(
            ProcessRun(
                process_name="trade_candle_backfill",
                status="succeeded" if latest == "finished" else "running",
                started_at=at - timedelta(hours=3 if latest == "running_old" else 1),
                **common,
            )
        )
        await session.commit()
    if latest in {"running_new", "no_publisher"}:
        with pytest.raises(repair.RepairRefusedError, match="candle writer"):
            await rebuild.plan_rebuild(run.context, run.request("guarded"))
    else:
        result = await rebuild.plan_rebuild(run.context, run.request("guarded"))
        assert len(cast(list[JsonObject], result.summary["process_runs"])) == 2


def _capture(record: JsonObject, database: JsonObject, *, delay: int = 1) -> JsonObject:
    """Construct a settlement observation with the observer's required self-row."""
    container = cast(JsonObject, record["container"])
    finish = datetime.fromisoformat(cast(str, container["finished_at"]))
    at = (finish + timedelta(seconds=delay)).isoformat()
    role = "fixture-role"
    observer: JsonObject = {
        "pid": 100,
        "current_user": role,
        "session_user": role,
        "now": at,
        "clock_timestamp": at,
        "server_addr": "10.10.10.10",
        "server_port": 5433,
        "database": database["database"],
        "postmaster_start": finish.isoformat(),
    }
    backend: JsonObject = {
        "pid": 100,
        "usename": role,
        "datname": database["database"],
        "client_addr": "172.18.0.9/32",
        "client_port": 1025,
        "application_name": "collector",
        "backend_type": "client backend",
        "backend_start": at,
        "xact_start": at,
        "state": "active",
    }
    return {"observer": observer, "backends": [backend], "endpoint": dict(database), "role": role}


def _candidate(capture: JsonObject, **changes: object) -> JsonObject:
    """Append another backend using the exact PostgreSQL result column names."""
    rows = cast(list[JsonObject], capture["backends"])
    row: JsonObject = {**rows[0], "pid": 101}
    row.update(cast(JsonObject, changes))
    rows.append(row)
    return row


def _manifest(run: _Run, record: JsonObject) -> None:
    """Replace the test manifest after updating its coordinator evidence."""
    _save(run.records, {"runs": [record]})


def _capture_target(run: _Run, url: str = "postgresql+asyncpg://localhost:5432/scratch") -> _Run:
    """Bind mocked PostgreSQL evidence while keeping all reads and writes on SQLite."""
    database = repair.database_endpoint(url)
    path = run.repair_dir / repair.APPROVAL_FILE
    _save(path, {**_document(path), "database": database})
    return replace(run, context=replace(run.context, database=database))


@pytest.mark.asyncio
async def test_rebuild_artifacts_and_logs_exclude_connection_credentials(
    recorded_repair: _Run, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Bind the public endpoint and retain only sanitized failure details in private files.

    Given: A configured endpoint and driver error containing test credentials.
    When: Preview and attempt the rebuild while collecting files and log messages.
    Then: Owner-only artifacts and logs retain safe details without the credentials.
    """
    url = "postgresql+asyncpg://TEST_LOGIN:TEST_PASSWORD@localhost:5432/scratch"
    run = _capture_target(recorded_repair, url)
    _manifest(run, _record(run.repair_dir))
    exclusive = install_exclusive_file_spy(monkeypatch)
    dry, writing = run.request("private-vote"), run.request("private-write")
    messages: list[str] = []
    sink = logger.add(lambda message: messages.append(str(message)))
    try:
        plan = await rebuild.plan_rebuild(run.context, dry)
        original = run.repository.rebuild_synthesized_candles

        async def fail(
            rows: list[CandleUpsertRow],
            *,
            expected: Mapping[CandleKey, SynthesizedRebuildExpectation],
            dry_run: bool = False,
        ) -> SynthesizedRebuildResult:
            """Report a driver failure whose raw message contains the configured URL."""
            if dry_run:
                return await original(rows, expected=expected, dry_run=True)
            raise OSError(url)

        monkeypatch.setattr(run.repository, "rebuild_synthesized_candles", fail)
        result = await rebuild.write_rebuild(
            run.context, writing, _vote(dry.output_dir, plan.digest)
        )
    finally:
        logger.remove(sink)

    assert result.stop_reason == "uncertain_acknowledgement"
    assert _document(dry.output_dir / repair.APPROVAL_FILE)["database"] == run.context.database
    for path in (*dry.output_dir.iterdir(), *writing.output_dir.iterdir()):
        exclusive(path)
        messages.append(path.read_text())
    assert any("OSError" in message for message in messages)
    assert all(
        "TEST_LOGIN" not in message and "TEST_PASSWORD" not in message for message in messages
    )


@pytest.mark.parametrize(
    ("mode", "matching"),
    [
        ("observer_only", False),
        ("background", False),
        ("hidden_type", True),
        ("no_user_or_database", False),
        ("named_exact", True),
        ("named_other", False),
        ("named_hidden", True),
        ("named_restart", True),
        ("restart_without_reconnect", False),
        ("docker", True),
        ("observed", True),
        ("run_time", True),
        ("recent_reconnect", True),
        ("hidden_time", True),
        ("hidden_address", True),
        ("newer_unrelated", False),
    ],
)
def test_settlement_matches_only_possible_repair_backends(
    recorded_repair: _Run, mode: str, matching: bool
) -> None:
    """Respect client visibility, canonical address identity and both legacy match paths.

    Given: Settlement captures with visible and hidden clients, names and addresses.
    When: Check whether each capture can settle the recorded repair.
    Then: Only backends that could belong to the repair prevent settlement.
    """
    run = _capture_target(recorded_repair)
    record = _record(run.repair_dir)
    record["observed_addresses"] = ["172.18.0.50/32"]
    at = cast(JsonObject, record["container"])["started_at"]
    name = f"snapper:repair-candles:{record['run_id']}"
    if mode.startswith("named"):
        record["application_name"] = name
    capture = _capture(record, run.context.database)
    edits: dict[str, dict[str, object]] = {
        "background": {"backend_type": "background worker"},
        "hidden_type": {"backend_type": None, "usename": None},
        "no_user_or_database": {"usename": None, "datname": None},
        "named_exact": {"application_name": name, "client_addr": "10.0.0.1"},
        "named_other": {"application_name": "different", "backend_start": at},
        "named_hidden": {"application_name": None},
        "named_restart": {"application_name": name, "client_addr": "10.0.0.1", "backend_start": at},
        "docker": {"client_addr": "172.18.0.9/32"},
        "observed": {"client_addr": "172.18.0.50"},
        "run_time": {"backend_start": at, "client_addr": "10.0.0.2"},
        "recent_reconnect": {"backend_start": at, "client_addr": "10.0.0.2"},
        "hidden_time": {"backend_start": None, "client_addr": "10.0.0.2"},
        "hidden_address": {"client_addr": None},
        "newer_unrelated": {"client_addr": "10.0.0.3"},
    }
    if mode in edits:
        _candidate(capture, **edits[mode])
    if mode in {"recent_reconnect", "named_restart", "restart_without_reconnect"}:
        cast(JsonObject, capture["observer"])["postmaster_start"] = at
    record["settlements"] = [capture]
    parsed = rebuild._RunRecord.model_validate(record)
    rebuild._check_record(parsed, run.context.database)
    assert rebuild._settled(parsed) is (not matching)


@pytest.mark.parametrize(
    "defect",
    [
        "name",
        "container_time",
        "endpoint",
        "database",
        "role",
        "session_role",
        "pid",
        "missing_observer",
        "dupe_observer",
        "state",
        "user",
        "xact",
        "backend_start",
        "before_exit",
        "out_of_order",
    ],
)
@pytest.mark.asyncio
async def test_structural_settlement_mismatch_refuses_before_database_access(
    recorded_repair: _Run, monkeypatch: pytest.MonkeyPatch, defect: str
) -> None:
    """An invalid earlier capture refuses the record even without an uncertain row.

    Given: A malformed earlier settlement capture followed by a later capture.
    When: Plan a rebuild from the recorded repair.
    Then: The command refuses the invalid evidence before opening a session.
    """
    run = _capture_target(recorded_repair)
    record = _record(run.repair_dir)
    first, last = (_capture(record, run.context.database, delay=delay) for delay in (1, 2))
    observer = cast(JsonObject, first["observer"])
    own = cast(list[JsonObject], first["backends"])[0]
    container = cast(JsonObject, record["container"])
    updates = {
        "name": (record, "application_name", "wrong"),
        "container_time": (container, "finished_at", container["started_at"]),
        "endpoint": (cast(JsonObject, first["endpoint"]), "database", "wrong"),
        "database": (observer, "database", "wrong"),
        "role": (observer, "current_user", "another"),
        "session_role": (observer, "session_user", "another"),
        "pid": (own, "pid", 99991),
        "missing_observer": (first, "backends", []),
        "dupe_observer": (first, "backends", [own, own]),
        "state": (own, "state", "idle"),
        "user": (own, "usename", "another"),
        "xact": (own, "xact_start", container["finished_at"]),
        "backend_start": (own, "backend_start", cast(JsonObject, last["observer"])["now"]),
        "before_exit": (observer, "now", container["finished_at"]),
        "out_of_order": (last, "observer", observer),
    }
    holder, field, value = updates[defect]
    holder[field] = value
    if defect == "out_of_order":
        last["backends"] = first["backends"]
    record["settlements"] = [first, last]
    _manifest(run, record)

    forbidden = MagicMock()
    monkeypatch.setattr(run.repository, "session", forbidden)
    with pytest.raises(repair.RepairRefusedError):
        await rebuild.plan_rebuild(run.context, run.request("refused"))
    forbidden.assert_not_called()


def _rewrite_receipt(directory: Path, receipt: JsonObject) -> None:
    """Write a related receipt and its durable chunk lines together for an evidence test."""
    _save(directory / repair.RECEIPT_FILE, receipt)
    chunks = cast(list[JsonObject], receipt["chunks"])
    (directory / repair.RECEIPT_LINES_FILE).write_bytes(
        b"".join(repair.canonical_json(chunk) + b"\n" for chunk in chunks)
    )


def _mark_uncertain(directory: Path) -> tuple[JsonObject, JsonObject]:
    """Make the insertion chunk uncertain with a matching receipt and hash."""
    receipt = _document(directory / repair.RECEIPT_FILE)
    insert_chunk = cast(list[JsonObject], receipt["chunks"])[0]
    inserted = cast(list[JsonObject], insert_chunk["applied"])[0]
    insert_chunk.update(
        {
            "uncertain": [inserted],
            "applied": [],
            "attempts": [
                {
                    "attempt": 1,
                    "outcome": "uncertain",
                    "error": "OSError",
                    "driver_error": None,
                    "sqlstate": None,
                }
            ],
        }
    )
    cast(JsonObject, receipt["totals"]).update({"applied": 1, "uncertain": 1})
    receipt["stop_reason"] = "uncertain_acknowledgement"
    _rewrite_receipt(directory, receipt)
    return receipt, inserted


async def _hide_repair_minute(run: _Run, receipt: JsonObject, ref: JsonObject) -> None:
    """Remove the inserted minute from the original run's enumeration."""
    async with run.repository.session() as session:
        await session.execute(
            update(Candle)
            .where(
                Candle.session_id == receipt["run_id"],
                Candle.open_at == datetime.fromisoformat(cast(str, ref["minute_utc"])),
            )
            .values(session_id="another-writer")
        )
        await session.commit()


@pytest.mark.parametrize("capture_order", ["none", "later_clear", "later_inconclusive", "present"])
@pytest.mark.asyncio
async def test_uncertain_absence_requires_newest_capture_and_presence_uses_db(
    recorded_repair: _Run, capture_order: str
) -> None:
    """Only an absent row needs clearance, and older inconclusive captures cannot veto it.

    Given: A repair receipt with an uncertain row and varying storage and capture evidence.
    When: Plan the rebuild and resolve the original outcome.
    Then: A matching version proves application and an absent row requires the newest capture to
        settle.
    """
    run = _capture_target(recorded_repair)
    receipt, ref = _mark_uncertain(run.repair_dir)
    if capture_order != "present":
        await _hide_repair_minute(run, receipt, ref)
    record = _record(run.repair_dir)
    if capture_order != "none":
        first, last = (_capture(record, run.context.database, delay=delay) for delay in (1, 2))
        _candidate(last if capture_order == "later_inconclusive" else first)
        record["settlements"] = [first, last]
    _manifest(run, record)
    request = run.request("check")
    if capture_order in {"none", "later_inconclusive"}:
        with pytest.raises(repair.RepairRefusedError, match="lacks settlement"):
            await rebuild.plan_rebuild(run.context, request)
        return
    plan = await rebuild.plan_rebuild(run.context, request)
    assert plan.summary["actions"] == {"insert": 6}
    binding = cast(JsonObject, _document(request.output_dir / repair.APPROVAL_FILE)["run_records"])
    resolutions = cast(list[JsonObject], binding["resolutions"])
    resolved = cast(list[JsonObject], resolutions[0]["rows"])[0]
    assert resolved["outcome"] == ("applied" if capture_order == "present" else "not_applied")
    assert (resolved["version"] is None) is (capture_order != "present")


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("open", 99.0),
        ("high", 99.0),
        ("low", 99.0),
        ("close", 99.0),
        ("volume", 99.0),
        ("vwap", 99.0),
        ("trades", 99),
        ("complete", False),
        ("price_basis", "mid"),
        ("source", "calculated"),
        ("sequence_id", 99),
        ("timestamp", OPEN),
        ("public_id", "00000000-0000-4000-8000-000000000009"),
    ],
)
@pytest.mark.asyncio
async def test_enumerated_correction_requires_the_exact_intended_payload(
    recorded_repair: _Run, field: str, value: object
) -> None:
    """Refuse a committed version whose identity, time or any voted value differs.

    Given: A recorded correction with one altered stored identity, timestamp or payload field.
    When: Enumerate the repair's committed versions.
    Then: The rebuild refuses a version that differs from the approved correction.
    """
    run = recorded_repair
    receipt = _document(run.repair_dir / repair.RECEIPT_FILE)
    async with run.repository.session() as session:
        await session.execute(
            update(Candle)
            .where(Candle.session_id == receipt["run_id"], Candle.open_at == OPEN)
            .values(**{field: value})
        )
        await session.commit()
    with pytest.raises(repair.RepairRefusedError, match="repair (version|correction)"):
        await rebuild.plan_rebuild(run.context, run.request("invalid"))


@pytest.mark.parametrize(
    ("case", "expected"),
    [
        ("protected", "protected"),
        ("loss", "would_lose_observations"),
        ("history", "history_without_current"),
        ("invalid", "invalid_input"),
        ("future", "invalid_input"),
        ("null_vwap", "invalid_input"),
        ("null_trades", "invalid_input"),
        ("absent", "no_input"),
    ],
)
@pytest.mark.asyncio
async def test_planning_reports_each_exclusion(
    recorded_repair: _Run, case: str, expected: str
) -> None:
    """Expose source ownership, observation loss, history or missing inputs in the preview.

    Given: Current candles with missing inputs, source protections, history or observation loss.
    When: Plan the rebuild and inspect the preview.
    Then: Each withheld window shows its exclusion reason.
    """
    run = recorded_repair
    async with run.repository.session() as session:
        if case in {"protected", "loss", "history"}:
            row = candle(
                XRP,
                OPEN,
                timeframe="5m",
                timestamp=OPEN + timedelta(minutes=5),
                source="native" if case == "protected" else "synthesized",
                volume=1000.0,
            )
            if case == "history":
                row["known_to"] = OPEN + timedelta(hours=1)
            await session.execute(insert(Candle).values(**row))
        elif expected == "invalid_input":
            minute = OPEN + timedelta(minutes=2)
            row = candle(
                XRP,
                minute,
                timestamp=datetime.now(UTC) + timedelta(days=int(case == "future")),
                complete=case != "invalid",
                **{kind: None for kind in ("vwap", "trades") if case == f"null_{kind}"},
            )
            await session.execute(insert(Candle).values(**row))
        else:
            await session.execute(
                update(Candle)
                .where(Candle.timeframe == "1m")
                .values(known_to=datetime.now(UTC) + timedelta(minutes=1))
            )
        await session.commit()
    request = run.request("classify")
    await rebuild.plan_rebuild(run.context, request)
    line = _preview(request.output_dir)[0]
    assert line["action"] == expected
    if expected == "invalid_input":
        assert line["input_minutes"] == "2"
        assert line["absent_minutes"] == "2"
        issue = "future" if case == "future" else "invalid"
        assert line["reason"] == f"{issue}_minute@{(OPEN + timedelta(minutes=2)).isoformat()}"


@pytest.mark.parametrize("phase", ["preflight", "under_lock"])
@pytest.mark.asyncio
async def test_guard_refusal_appears_in_the_correct_artifact(
    recorded_repair: _Run, monkeypatch: pytest.MonkeyPatch, phase: str
) -> None:
    """Keep rejected input fingerprints visible in the plan or guarded row receipt.

    Given: A changed input fingerprint before planning or inside the guarded write.
    When: Apply the voted rebuild and read its artifacts.
    Then: The refusal appears in the preview or the guarded receipt at the relevant stage.
    """
    run = recorded_repair
    original = run.repository.rebuild_synthesized_candles

    async def refuse_first(
        rows: list[CandleUpsertRow],
        *,
        expected: Mapping[CandleKey, SynthesizedRebuildExpectation],
        dry_run: bool = False,
    ) -> SynthesizedRebuildResult:
        """Reject the first key only at the selected guard phase."""
        if dry_run != (phase == "preflight"):
            return await original(rows, expected=expected, dry_run=dry_run)
        key = rebuild.candle_key(rows[0])
        stale = {_key: expected[_key].copy() for _key in expected}
        stale[key]["inputs_sha256"] = "0" * 64
        return await original(rows, expected=stale, dry_run=dry_run)

    monkeypatch.setattr(run.repository, "rebuild_synthesized_candles", refuse_first)
    dry = run.request("voted")
    vote = await rebuild.plan_rebuild(run.context, dry)
    write = run.request("write")
    await rebuild.write_rebuild(run.context, write, _vote(dry.output_dir, vote.digest))
    receipt = _document(write.output_dir / repair.RECEIPT_FILE)
    first_chunk = cast(list[JsonObject], receipt["chunks"])[0]
    if phase == "preflight":
        assert _preview(dry.output_dir)[0]["reason"] == "inputs_changed"
        assert first_chunk["skipped_by_guard"] == []
    else:
        skipped = cast(list[JsonObject], first_chunk["skipped_by_guard"])
        assert skipped[0]["guard_outcome"] == {"status": "inputs_changed", "reason": None}
    check = await rebuild.plan_rebuild(
        run.context, run.request("verified", verification=run.evidence(write.output_dir))
    )
    windows = cast(list[JsonObject], _verification(check, run.context.run_id)["windows"])
    row = next(row for row in windows if row["timeframe"] == "5m")
    assert row["class"] == "outstanding"
    assert row["reason"] == ("refused_by_guard" if phase == "preflight" else "skipped_by_guard")


@pytest.mark.parametrize("evidence_defect", ["missing", "extra", "duplicate"])
@pytest.mark.asyncio
async def test_enumeration_equals_the_receipt_exactly(
    recorded_repair: _Run, evidence_defect: str
) -> None:
    """Refuse missing or unreferenced repair versions instead of expanding or dropping scope.

    Given: A bound repair receipt and an enumeration with a missing, extra or mismatched version.
    When: Derive the rebuild scope from the recorded write.
    Then: The rebuild refuses the discrepancy without altering the approved scope.
    """
    run = recorded_repair
    receipt = _document(run.repair_dir / repair.RECEIPT_FILE)
    if evidence_defect == "missing":
        reference = cast(list[JsonObject], cast(list[JsonObject], receipt["chunks"])[0]["applied"])[
            0
        ]
        await _hide_repair_minute(run, receipt, reference)
    elif evidence_defect == "extra":
        extra = candle(
            XRP,
            OPEN + timedelta(minutes=2),
            session_id=receipt["run_id"],
            sequence_id=999,
        )
        await run.repository.upsert_candles([extra])
    else:
        await _duplicate_repair_version(run, receipt)
    with pytest.raises(repair.RepairRefusedError, match="(missing|outside the receipt|repeats)"):
        await rebuild.plan_rebuild(run.context, run.request("bad-enumeration"))


async def _duplicate_repair_version(run: _Run, receipt: JsonObject) -> None:
    """Store a second physical version of the same recorded minute and session."""
    async with run.repository.session() as session:
        found = await session.execute(
            select(Candle.__table__).where(
                Candle.session_id == receipt["run_id"], Candle.open_at == OPEN
            )
        )
        values = dict(found.mappings().one())
        values.pop("id")
        values["known_to"] = _END
        await session.execute(insert(Candle).values(**values))
        await session.commit()


@pytest.mark.asyncio
async def test_verification_accepts_only_supersession_of_the_same_original_resolution(
    recorded_repair: _Run,
) -> None:
    """Verify a superseded uncertain repair row and require a fresh write vote.

    Given: A repair outcome that was originally uncertain and whose matched version was superseded.
    When: Verify the rebuild and compare the frozen resolution for a new write.
    Then: Verification accepts the supersession of the same version and a write requires a fresh
        vote.
    """
    run = recorded_repair
    _mark_uncertain(run.repair_dir)
    _manifest(run, _record(run.repair_dir))
    dry, write = run.request("dry"), run.request("write")
    plan = await rebuild.plan_rebuild(run.context, dry)
    await rebuild.write_rebuild(run.context, write, _vote(dry.output_dir, plan.digest))
    minute = candle(
        XRP, OPEN + timedelta(minutes=1), timestamp=datetime.now(UTC), volume=100.0, trades=4
    )
    await run.repository.upsert_candles([minute])
    with pytest.raises(repair.RepairRefusedError, match="different uncertain-row resolution"):
        await rebuild.write_rebuild(
            run.context, run.request("refused"), _vote(dry.output_dir, plan.digest)
        )
    request = run.request("verify", verification=run.evidence(write.output_dir))
    verified = await rebuild.plan_rebuild(run.context, request)
    assert cast(JsonObject, _verification(verified, run.context.run_id)["counts"])["regressed"] == 6


@pytest.mark.parametrize(
    ("mode", "expected"),
    [(None, "not_observed"), ("unknown", False), ("cancelled", False), (0, True), (3, False)],
)
def test_postgres_chunk_observations_are_from_the_committing_attempt(
    mode: int | str | None, expected: bool | str
) -> None:
    """Use mocked PostgreSQL outcomes to distinguish no write, uncertainty and observed counts.

    Given: Mocked guarded write outcomes with and without a committing sample.
    When: Build the chunk's client observation fields.
    Then: The receipt distinguishes committing observations, no write and uncertainty.
    """
    repository = MagicMock(spec=SQLAlchemyRepository, dialect_name="postgresql")
    context = repair.RunContext(repository, {}, {})
    adapter = rebuild._RebuildAdapter(context, {})
    sample = ClientSample(mode, _END) if isinstance(mode, int) else None
    ack = rebuild.ChunkAcknowledgement([], {}, sample) if sample is not None else None
    stops = {"unknown": "uncertain_acknowledgement", "cancelled": "cancelled"}
    result = adapter.receipt_fields(ack, stops.get(str(mode)))
    assert result["sole_client"] == expected
    assert result["client_sample"] == (
        {"other_client_backends": mode, "observed_at": _END.isoformat()}
        if sample is not None
        else None
    )


@pytest.mark.parametrize(
    "binding_error",
    [
        "record",
        "duplicate",
        "scope",
        "payload",
        "author",
        "missing",
        "unreferenced",
        "action",
        "rows",
        "selection",
    ],
)
@pytest.mark.asyncio
async def test_verification_checks_artifact_and_committed_version_integrity(
    recorded_repair: _Run, binding_error: str
) -> None:
    """Refuse a mismatched evidence file or a corrupted committed rebuild identity or payload.

    Given: A recorded rebuild with an altered artifact or a missing or changed committed version.
    When: Verify the write against the record and observed versions.
    Then: The command refuses inconsistent evidence or committed payloads.
    """
    run = recorded_repair
    dry, write = run.request("dry"), run.request("write")
    plan = await rebuild.plan_rebuild(run.context, dry)
    limit = 5 if binding_error in {"scope", "unreferenced"} else None
    await rebuild.write_rebuild(run.context, write, _vote(dry.output_dir, plan.digest, limit))
    _damage_verified_write(write.output_dir, binding_error)
    await _damage_written_payload(run, binding_error)
    evidence = run.evidence(write.output_dir)
    if binding_error == "record":
        _save(evidence.record, {**_document(evidence.record), "approval_sha256": "a" * 64})
    with pytest.raises(repair.RepairRefusedError):
        await rebuild.plan_rebuild(run.context, run.request("verify", verification=evidence))


def _damage_verified_write(directory: Path, error: str) -> None:
    """Create coherent hashes around deliberately inconsistent artifact contents."""
    if error == "scope":
        _replace_preview(directory, _preview(directory)[:-1])
        return
    receipt = _document(directory / repair.RECEIPT_FILE)
    first = cast(list[JsonObject], receipt["chunks"])[0]
    rows = cast(list[JsonObject], first["applied"])
    if error == "duplicate":
        rows.append(rows[0])
    elif error == "action":
        rows[0]["action"] = "rebuild"
    elif error == "rows":
        first["rows"] = 6
    elif error == "selection":
        reference = rows[0].copy()
        reference.pop("sequence_id")
        receipt["not_voted"] = [reference, reference]
    _rewrite_receipt(directory, receipt)


async def _damage_written_payload(run: _Run, error: str) -> None:
    """Corrupt the write's payload or move or add a version under its session."""
    changes = {
        "payload": {"close": 99.0},
        "author": {"timestamp": _END},
        "missing": {"session_id": "unexpected-writer"},
    }
    if error == "unreferenced":
        await run.repository.upsert_candles(
            [
                candle(
                    XRP,
                    _END - timedelta(days=1),
                    timeframe="1d",
                    session_id=run.context.run_id,
                    timestamp=datetime.now(UTC),
                    sequence_id=8,
                )
            ]
        )
    elif error in changes:
        async with run.repository.session() as session:
            await session.execute(
                update(Candle)
                .where(Candle.session_id == run.context.run_id, Candle.timeframe == "5m")
                .values(**changes[error])
            )
            await session.commit()


@pytest.mark.parametrize(
    ("args", "message"),
    [
        (b'{"runs":[NaN]}', "does not parse"),
        (b'{"runs":[],"runs":[]}', "does not parse"),
        (b'{"runs": [}', "does not parse"),
        (b'{"runs":[]}', "invalid format"),
    ],
)
@pytest.mark.asyncio
async def test_invalid_json_or_shape_refuses_before_reading_database(
    recorded_repair: _Run, monkeypatch: pytest.MonkeyPatch, args: bytes, message: str
) -> None:
    """Refuse structurally malformed JSON and non-standard JSON numeric tokens.

    Given: Malformed JSON, invalid record shapes or non-standard numeric tokens.
    When: Read the recorded repair evidence.
    Then: The command refuses before opening a repository session.
    """
    run = recorded_repair
    run.records.write_bytes(args)
    forbidden = MagicMock()
    monkeypatch.setattr(run.repository, "session", forbidden)
    with pytest.raises(repair.RepairRefusedError, match=message):
        await rebuild.plan_rebuild(run.context, run.request("invalid"))
    forbidden.assert_not_called()


@pytest.mark.parametrize(
    ("defect", "message"),
    [
        ("bad_total", "totals"),
        ("bad_reached", "reached"),
        ("selection_bound", "scheduled or selected"),
        ("unreached", "did not reach"),
        ("completion", "omitted selected"),
        ("limit", "no remaining"),
        ("chunk_rows", "chunk row count"),
        ("chunk_number", "chunk numbering"),
        ("attempt_order", "out of order"),
        ("attempt_fields", "attempt fields"),
        ("failed_without_error", "missing its error type"),
        ("chunk_time", "timestamps are out of order"),
        ("repeated_minute", "repeats a minute"),
        ("repeated_sequence", "repeats a minute or sequence"),
        ("out_of_scope", "outside its write preview"),
        ("duplicate_selection", "invalid selection list"),
        ("overlapping_selection", "overlaps already_applied"),
        ("identity", "identity differs"),
        ("terminal_status", "identity differs"),
        ("lines", "differs from receipt.json"),
    ],
)
@pytest.mark.asyncio
async def test_bound_receipt_consistency_is_checked_before_any_read(
    recorded_repair: _Run, monkeypatch: pytest.MonkeyPatch, defect: str, message: str
) -> None:
    """Matching hashes alone cannot authorize a structurally inconsistent receipt.

    Given: Matching file hashes for a receipt with an inconsistent identity, count or chunk.
    When: Plan the rebuild using the bound receipt.
    Then: The command refuses before opening a repository session.
    """
    run = recorded_repair
    receipt = _document(run.repair_dir / repair.RECEIPT_FILE)
    _alter_receipt(receipt, defect)
    _rewrite_receipt(run.repair_dir, receipt)
    if defect == "lines":
        (run.repair_dir / repair.RECEIPT_LINES_FILE).write_bytes(b"")
    record = _record(run.repair_dir)
    if defect == "identity":
        record["voted_digest"] = "b" * 64
    if defect == "terminal_status":
        record["stop_reason"] = "error"
    _manifest(run, record)
    forbidden = MagicMock()
    monkeypatch.setattr(run.repository, "session", forbidden)
    with pytest.raises(repair.RepairRefusedError, match=message):
        await rebuild.plan_rebuild(run.context, run.request("bad-receipt"))
    forbidden.assert_not_called()


def _alter_receipt(receipt: JsonObject, defect: str) -> None:
    """Mutate precisely the part of the receipt checked by the test."""
    chunk, correction = cast(list[JsonObject], receipt["chunks"])
    attempt = cast(list[JsonObject], chunk["attempts"])[0]
    written = cast(list[JsonObject], chunk["applied"])
    selection = {**written[0], "action": "insert"}
    selection.pop("sequence_id")
    changes = {
        "bad_total": (cast(JsonObject, receipt["totals"]), "applied", 3),
        "bad_reached": (receipt, "reached", 3),
        "selection_bound": (receipt, "selected", 1),
        "completion": (receipt, "selected", 3),
        "limit": (receipt, "stop_reason", "max_rows"),
        "chunk_rows": (chunk, "rows", 3),
        "chunk_number": (chunk, "chunk", 2),
        "attempt_order": (attempt, "attempt", 2),
        "attempt_fields": (attempt, "driver_error", None),
        "failed_without_error": (attempt, "outcome", "uncertain"),
        "chunk_time": (chunk, "ended_at", OPEN.isoformat()),
        "repeated_minute": (chunk, "applied", [written[0], written[0]]),
        "repeated_sequence": (
            cast(list[JsonObject], correction["applied"])[0],
            "sequence_id",
            written[0]["sequence_id"],
        ),
        "out_of_scope": (written[0], "minute_utc", _END.isoformat()),
        "duplicate_selection": (receipt, "not_voted", [selection, selection]),
        "overlapping_selection": (receipt, "already_applied", [selection]),
    }
    if defect in changes:
        holder, field, value = changes[defect]
        holder[field] = value
    if defect == "unreached":
        receipt.update({"selected": 3, "scheduled": 3})
    if defect == "overlapping_selection":
        receipt["changed"] = [selection]


@pytest.mark.asyncio
async def test_only_another_recorded_run_can_prove_an_already_applied_minute(
    recorded_repair: _Run,
) -> None:
    """Reuse the repair resume receipt, counting its no-op lines only against the earlier run.

    Given: A completed repair and its resumed write reporting already-applied minutes.
    When: Plan from the resume receipt with and without the earlier recorded run.
    Then: Only the other run's committed versions prove that the minutes were applied.
    """
    run = recorded_repair
    old_vote = run.root / "repair-vote"
    digest = repair.sha256_hex((old_vote / repair.APPROVAL_FILE).read_bytes())
    resumed = run.root / "repair-resume"
    original = repair.RunContext(run.repository, run.context.database, run.context.code_revision)
    report = await repair.write_repair(
        original, snapshot_request(run.root / "snapshot", resumed), _vote(old_vote, digest)
    )
    assert report.totals["applied"] == 0
    resume_record = _record(resumed)
    _manifest(run, resume_record)
    request = replace(run.request("single"), repair_runs=(resumed,))
    summary = (await rebuild.plan_rebuild(run.context, request)).summary
    assert summary["latest_window_end"] is None
    assert cast(list[JsonObject], summary["runs"])[0]["already_applied_unproven"] == 2
    _save(run.records, {"runs": [resume_record, _record(run.repair_dir)]})
    request = replace(run.request("both"), repair_runs=(resumed, run.repair_dir))
    summary = (await rebuild.plan_rebuild(run.context, request)).summary
    assert summary["actions"] == {"insert": 6}
    summaries = cast(list[JsonObject], summary["runs"])
    resumed_summary = next(row for row in summaries if row["run_id"] == resume_record["run_id"])
    assert resumed_summary["already_applied"] == 2
    assert resumed_summary["already_applied_unproven"] == 0
    await _reject_conflicting_instrument(run, resumed)


async def _reject_conflicting_instrument(run: _Run, resumed: Path) -> None:
    """Bind a different symbol to the same id in a second approved run."""
    path = resumed / repair.APPROVAL_FILE
    approval = _document(path)
    names = cast(list[JsonObject], approval["instruments"])
    names[0]["symbol"] = "XRP-USD"
    _save(path, approval)
    _replace_preview(resumed, [{**line, "symbol": "XRP-USD"} for line in _preview(resumed)])
    _save(run.records, {"runs": [_record(resumed), _record(run.repair_dir)]})
    request = replace(run.request("conflicting"), repair_runs=(resumed, run.repair_dir))
    with pytest.raises(repair.RepairRefusedError, match="disagree on an instrument symbol"):
        await rebuild.plan_rebuild(run.context, request)


@pytest.mark.parametrize(
    ("column", "value"),
    [
        ("timeframe", "1m"),
        ("window_start_utc", "bad"),
        ("window_start_utc", "2026-11-10T15:01:00+00:00"),
        ("window_start_utc", "2026-11-10T15:00:02+00:00"),
        ("window_end_utc", "2026-11-10T15:04:00+00:00"),
        ("new_complete", "wrong"),
        ("new_price_basis", "mid"),
        ("inputs_sha256", "x"),
        ("new_open", "nan"),
        ("new_trades", "1.0"),
        ("action", "rebuild"),
    ],
)
@pytest.mark.asyncio
async def test_preview_parser_rejects_malformed_votes(
    recorded_repair: _Run, column: str, value: str
) -> None:
    """Require aligned keys, validated payload and predecessor evidence for write previews.

    Given: Voted preview lines with invalid keys, payloads or predecessor evidence.
    When: Read the preview for a confirmed write.
    Then: The parser refuses malformed votes.
    """
    run = recorded_repair
    dry = run.request("dry")
    await rebuild.plan_rebuild(run.context, dry)
    preview = _preview(dry.output_dir)
    preview[0][column] = value
    output = io.StringIO(newline="")
    writer = csv.DictWriter(output, fieldnames=tuple(preview[0]))
    writer.writeheader()
    writer.writerows(preview)
    with pytest.raises(repair.RepairRefusedError):
        rebuild._parse_preview(output.getvalue().encode())


@pytest.mark.asyncio
async def test_different_input_fingerprint_is_not_voted_and_old_vote_is_changed(
    recorded_repair: _Run,
) -> None:
    """A new minute after the vote removes every affected window from the writable subset.

    Given: An approved preview and a new one-minute input for the affected grids.
    When: Replan and compare the fresh rows with the vote.
    Then: The affected rows leave the writable subset and the old votes are reported as changed.
    """
    run = recorded_repair
    dry = run.request("dry")
    plan = await rebuild.plan_rebuild(run.context, dry)
    await run.repository.upsert_candles(
        [
            candle(
                XRP, OPEN + timedelta(minutes=2), timestamp=datetime.now(UTC), volume=1.0, trades=1
            )
        ]
    )
    write = run.request("write")
    report = await rebuild.write_rebuild(run.context, write, _vote(dry.output_dir, plan.digest))
    assert report.totals["applied"] == 0
    receipt = _document(write.output_dir / repair.RECEIPT_FILE)
    assert receipt["not_voted"] == receipt["changed"]
    check = await rebuild.plan_rebuild(
        run.context, run.request("check", verification=run.evidence(write.output_dir))
    )
    observed = _verification(check, run.context.run_id)
    assert {row["reason"] for row in cast(list[JsonObject], observed["windows"])} == {"changed"}


@pytest.mark.asyncio
async def test_postgres_read_modes_use_one_session_and_required_statement_order(
    recorded_repair: _Run,
) -> None:
    """Check READ COMMITTED enumeration and repeatable read verification with a mock.

    Given: A mocked PostgreSQL session that records each issued statement.
    When: Enumerate repair versions and read a verification snapshot.
    Then: Each operation uses one session and sets the required transaction mode before reading.
    """
    run = recorded_repair
    record = rebuild._load_runs(run.request("read"), run.context.database).runs[0]
    key = (XRP, "5m", OPEN)
    unit = rebuild._units([key])[0]
    now, observed = _END, _END + timedelta(microseconds=1)
    repository = MagicMock(spec=SQLAlchemyRepository, dialect_name="postgresql")
    session = MagicMock()
    session.execute = AsyncMock(return_value=MagicMock())
    session.execute.return_value.one.return_value = (now, observed)
    session.execute.return_value.mappings.return_value = []

    @asynccontextmanager
    async def owned() -> AsyncIterator[AsyncSession]:
        """Expose one fake connection-owned session."""
        yield session

    repository.session.side_effect = owned
    assert await rebuild._enumerate_repair(repository, record) == []
    commands = [str(call.args[0]) for call in session.execute.await_args_list]
    assert commands[:2] == [
        "SET TRANSACTION ISOLATION LEVEL READ COMMITTED",
        "SET LOCAL statement_timeout = '20s'",
    ]
    assert "candles.session_id =" in commands[2]
    assert "candles.instrument_public_id IN" in commands[2]

    context = replace(run.context, repository=repository)
    session.execute.reset_mock()
    snapshot = await rebuild._snapshot(context, unit)
    assert (snapshot.before, snapshot.after, snapshot.outputs, snapshot.inputs) == (
        now,
        observed,
        {},
        {},
    )
    commands = [str(call.args[0]) for call in session.execute.await_args_list]
    assert commands[:3] == [
        "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY",
        "SELECT now(), clock_timestamp()",
        "SET LOCAL statement_timeout = '20s'",
    ]
    assert "candles.known_to >" in commands[4]
    assert all("LOCK TABLE" not in statement for statement in commands)
    session.execute.reset_mock()
    await rebuild._plan_unit(context, unit, {key: rebuild._Window(key, "XRP-EUR", 1, "no_input")})
    assert str(session.execute.await_args_list[0].args[0]) == "SET LOCAL statement_timeout = '20s'"


@pytest.mark.asyncio
@pytest.mark.parametrize("verify", [False, True])
async def test_sqlite_verification_holds_a_snapshot_across_the_two_selects(
    recorded_repair: _Run,
    monkeypatch: pytest.MonkeyPatch,
    verify: bool,
) -> None:
    """Commit a paired input/output batch between the reads and prove neither version enters.

    Given: SQLite output and minute rows read inside a verification transaction.
    When: Commit a paired update from another connection between the two reads.
    Then: Both reads observe the same snapshot and exclude the new pair.
    """
    run = recorded_repair
    async with run.repository.engine.connect() as connection:
        result = await connection.execute(text("PRAGMA journal_mode=WAL"))
        assert result.scalar_one() == "wal"
    key = (XRP, "5m", OPEN)
    unit = rebuild._units([key])[0]
    before = datetime.now(UTC)
    original = rebuild._read_output_versions

    async def output_then_concurrent_commit(
        session: AsyncSession, group: rebuild._ReadUnit
    ) -> dict[CandleKey, list[CandleVersionRow]]:
        """Hold the verification read open while a live-style batch lands."""
        seen = await original(session, group)
        await run.repository.upsert_candles(
            [
                candle(XRP, OPEN + timedelta(minutes=2), timestamp=before, volume=1.0, trades=1),
                candle(XRP, OPEN, timeframe="5m", timestamp=before, volume=11.0, trades=11),
            ]
        )
        return seen

    monkeypatch.setattr(rebuild, "_read_output_versions", output_then_concurrent_commit)
    if verify:
        snapshot = await rebuild._snapshot(run.context, unit)
        assert snapshot.outputs == {}
        assert len(snapshot.inputs[XRP]) == 2
    else:
        initial = {key: rebuild._Window(key, "XRP-EUR", 2, "no_input")}
        planned = await rebuild._plan_unit(run.context, unit, initial)
        assert planned[key].action == "insert" and planned[key].present == 3
    assert len(await _higher_rows(run.repository)) == 1


@pytest.mark.asyncio
async def test_cancellation_writes_the_rebuild_chunk_then_the_final_receipt(
    recorded_repair: _Run, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Leave a fully durable uncertain chunk when a write await is cancelled.

    Given: A write cancelled while a chunk attempt is outstanding.
    When: Allow the cancellation to propagate through the command.
    Then: The uncertain chunk is logged durably and the final receipt records the cancellation.
    """
    run = recorded_repair
    dry, write = run.request("dry"), run.request("write")
    plan = await rebuild.plan_rebuild(run.context, dry)
    original = run.repository.rebuild_synthesized_candles

    async def cancel(
        rows: list[CandleUpsertRow],
        *,
        expected: Mapping[CandleKey, SynthesizedRebuildExpectation],
        dry_run: bool = False,
    ) -> SynthesizedRebuildResult:
        """Cancel only the write attempt."""
        if dry_run:
            return await original(rows, expected=expected, dry_run=True)
        raise asyncio.CancelledError

    monkeypatch.setattr(run.repository, "rebuild_synthesized_candles", cancel)
    with pytest.raises(asyncio.CancelledError):
        await rebuild.write_rebuild(run.context, write, _vote(dry.output_dir, plan.digest))
    receipt = _document(write.output_dir / repair.RECEIPT_FILE)
    assert receipt["stop_reason"] == "cancelled"
    assert cast(JsonObject, receipt["totals"])["uncertain"] == 5
    check = await rebuild.plan_rebuild(
        run.context, run.request("verify", verification=run.evidence(write.output_dir))
    )
    observed = _verification(check, run.context.run_id)
    assert cast(JsonObject, observed["counts"])["uncertain"] == 5


def _csv_bytes(lines: list[dict[str, str]]) -> bytes:
    """Render a deliberate mutation with the writer's existing header."""
    output = io.StringIO(newline="")
    writer = csv.DictWriter(output, fieldnames=tuple(lines[0]), lineterminator="\n")
    writer.writeheader()
    writer.writerows(lines)
    return output.getvalue().encode()


def _replace_preview(directory: Path, lines: list[dict[str, str]]) -> str:
    """Bind a modified preview to its approval and return the approval digest."""
    preview = _csv_bytes(lines)
    (directory / repair.PREVIEW_FILE).write_bytes(preview)
    approval_path = directory / repair.APPROVAL_FILE
    _save(approval_path, {**_document(approval_path), "preview_sha256": repair.sha256_hex(preview)})
    return repair.sha256_hex(approval_path.read_bytes())


@pytest.mark.parametrize(
    ("case", "message"),
    [
        ("file", "cannot read run records"),
        ("instrument_repeat", "repeats an instrument"),
        ("logical_repeat", "repeats a logical minute"),
        ("preview_symbol", "unapproved minute or symbol"),
        ("active_instrument", "active Kraken spot symbol"),
        ("inactive_instrument", "active Kraken spot symbol"),
        ("insert_time", "not authored inside"),
        ("snapshot_time", "invalid preview timestamp"),
    ],
)
@pytest.mark.asyncio
async def test_repair_refuses_semantically_inconsistent_evidence(
    recorded_repair: _Run, case: str, message: str
) -> None:
    """Reject authenticated files or stored facts that cannot describe the approved run.

    Given: Authenticated artifacts with inconsistent scope, catalog data or authoring time.
    When: Plan the rebuild from that evidence.
    Then: The command refuses the facts that cannot describe the approved repair.
    """
    run = recorded_repair
    request = run.request("invalid")
    if case == "file":
        request = replace(request, run_records=run.root / "missing.json")
    elif case == "instrument_repeat":
        path = run.repair_dir / repair.APPROVAL_FILE
        approval = _document(path)
        instruments = cast(list[JsonObject], approval["instruments"])
        _save(path, {**approval, "instruments": [instruments[0], instruments[0]]})
    elif case in {"logical_repeat", "preview_symbol", "snapshot_time"}:
        _alter_repair_preview(run.repair_dir, case)
    else:
        await _alter_db_evidence(run, case)
    _manifest(run, _record(run.repair_dir))
    with pytest.raises(repair.RepairRefusedError, match=message):
        await rebuild.plan_rebuild(run.context, request)


def _alter_repair_preview(directory: Path, case: str) -> None:
    """Change one repair preview fact and preserve its hash binding."""
    lines = _preview(directory)
    first = next(line for line in lines if line["action"] == "correct")
    if case == "logical_repeat":
        lines.append({**first, "minute_utc": first["minute_utc"].replace("+00:00", "Z")})
    elif case == "preview_symbol":
        first["symbol"] = "wrong"
    else:
        first["new_known_at"] = "not-a-timestamp"
    _replace_preview(directory, lines)


async def _alter_db_evidence(run: _Run, case: str) -> None:
    """Change the catalog exchange or expiry, or an insert's author time."""
    instrument = update(Instrument).where(Instrument.public_id == XRP)
    statements = {
        "active_instrument": instrument.values(exchange="paper"),
        "inactive_instrument": instrument.values(known_to=_END),
        "insert_time": update(Candle)
        .where(Candle.open_at == OPEN + timedelta(minutes=1))
        .values(timestamp=OPEN),
    }
    async with run.repository.session() as session:
        await session.execute(statements[case])
        await session.commit()


@pytest.mark.parametrize(
    ("case", "message"),
    [
        ("approval_hash", "does not match its approval"),
        ("database", "another database or timeframe"),
        ("records", "different run records"),
        ("instruments", "different instruments"),
        ("resolved_runs", "different set of runs"),
        ("resolved_rows", "different set of uncertain rows"),
    ],
)
@pytest.mark.asyncio
async def test_rebuilt_approval_validates_every_binding(
    recorded_repair: _Run, case: str, message: str
) -> None:
    """Reject a different approved header or frozen resolution even with a matching digest.

    Given: A voted approval with a changed binding and a matching supplied digest.
    When: Validate it for the rebuilt request.
    Then: The changed approval or frozen resolution is refused.
    """
    run = recorded_repair
    dry = run.request("dry")
    await rebuild.plan_rebuild(run.context, dry)
    path = dry.output_dir / repair.APPROVAL_FILE
    approval = _document(path)
    binding = cast(JsonObject, approval["run_records"])
    entry = cast(list[JsonObject], binding["resolutions"])[0]
    edits = {
        "approval_hash": (approval, "preview_sha256", "0" * 64),
        "database": (approval, "database", {}),
        "records": (binding, "sha256", "0" * 64),
        "instruments": (approval, "instruments", []),
        "resolved_runs": (binding, "resolutions", []),
        "resolved_rows": (
            entry,
            "rows",
            [
                {
                    "instrument_public_id": XRP,
                    "minute_utc": OPEN.isoformat(),
                    "version": None,
                    "outcome": "not_applied",
                }
            ],
        ),
    }
    target, key, value = edits[case]
    target[key] = value
    _save(path, approval)
    with pytest.raises(repair.RepairRefusedError, match=message):
        await rebuild.write_rebuild(
            run.context,
            run.request("invalid"),
            _vote(dry.output_dir, repair.sha256_hex(path.read_bytes())),
        )


@pytest.mark.parametrize("change", ["remove", "add"])
@pytest.mark.asyncio
async def test_voted_preview_cannot_expand_beyond_the_derived_scope(
    recorded_repair: _Run, change: str
) -> None:
    """A missing vote withholds a candidate and an extra window is outside the scope.

    Given: A voted preview with a missing or extra window.
    When: Attempt the rebuild against the derived scope.
    Then: A missing vote withholds the candidate and an out-of-scope vote is refused.
    """
    run = recorded_repair
    dry = run.request("dry")
    await rebuild.plan_rebuild(run.context, dry)
    lines = _preview(dry.output_dir)
    if change == "add":
        original = lines[0]
        start = OPEN + timedelta(days=1)
        lines.append(
            {
                **original,
                "window_start_utc": start.isoformat(),
                "window_end_utc": (start + timedelta(minutes=5)).isoformat(),
            }
        )
    else:
        lines = lines[1:]
    digest = _replace_preview(dry.output_dir, lines)
    write = run.request("write")
    if change == "add":
        with pytest.raises(repair.RepairRefusedError, match="outside the derived scope"):
            await rebuild.write_rebuild(run.context, write, _vote(dry.output_dir, digest))
        return
    report = await rebuild.write_rebuild(run.context, write, _vote(dry.output_dir, digest))
    assert report.totals["applied"] == 5
    check = await rebuild.plan_rebuild(
        run.context, run.request("verify", verification=run.evidence(write.output_dir))
    )
    observed = _verification(check, run.context.run_id)
    assert "not_voted" in {row["reason"] for row in cast(list[JsonObject], observed["windows"])}


@pytest.mark.parametrize(
    ("data", "message"), [(b"", "unexpected header"), (b'"', "does not parse")]
)
def test_preview_csv_rejects_missing_header_or_broken_quoting(data: bytes, message: str) -> None:
    """Treat a bad CSV token or missing schema as a refusal.

    Given: CSV with a missing preview schema or broken quoting.
    When: Read the preview.
    Then: The command reports an evidence refusal.
    """
    with pytest.raises(repair.RepairRefusedError, match=message):
        rebuild._parse_preview(data)


@pytest.mark.asyncio
async def test_planner_refuses_an_ambiguous_current_version_or_missing_fold(
    recorded_repair: _Run, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Refuse ambiguous output ownership and an aggregator that emits the wrong window.

    Given: Ambiguous current versions or an aggregator result for the wrong window.
    When: Classify the rebuild candidate.
    Then: The planner refuses the inconsistent output.
    """
    run = recorded_repair
    key = (XRP, "5m", OPEN)
    window = rebuild._Window(key, "XRP-EUR", 1, "no_input")
    record = rebuild._load_runs(run.request("order"), run.context.database).runs[0]
    versions = await rebuild._enumerate_repair(run.repository, record)
    at = datetime.now(UTC)
    with pytest.raises(repair.RepairRefusedError, match="multiple current"):
        rebuild._classify(window, [versions[0], versions[0]], [], at)
    aggregator = MagicMock()
    aggregator.return_value.flush.return_value = [("15m", MagicMock())]
    monkeypatch.setattr(rebuild, "CandleAggregator", aggregator)
    with pytest.raises(repair.RepairRefusedError, match="did not emit"):
        rebuild._fold(window, versions)


def _short(run: _Run, name: str) -> rebuild.RebuildRequest:
    """Limit the writable plan to its first five-minute window."""
    return run.request(name, OPEN + timedelta(minutes=5))


async def _five_minute_candidate(run: _Run) -> rebuild._Window:
    """Read and fold the actual recorded fixture's current source minutes."""
    key = (XRP, "5m", OPEN)
    window = rebuild._Window(key, "XRP-EUR", 2, "no_input")
    return (await rebuild._plan_unit(run.context, rebuild._units([key])[0], {key: window}))[key]


async def _seed_old_output(
    run: _Run, proposed: CandleUpsertRow, **changes: object
) -> CandleVersionRow:
    """Install an older synthesized predecessor with a distinct price and fewer observations."""
    row = (
        dict(proposed)
        | {
            "close": 1.05,
            "volume": 10.0,
            "trades": 2,
            "timestamp": OPEN + timedelta(minutes=6),
            "session_id": str(uuid7()),
            "sequence_id": 1,
        }
        | changes
    )
    async with run.repository.session() as session:
        result = await session.execute(insert(Candle).values(**row).returning(Candle.__table__))
        stored = cast(CandleVersionRow, dict(result.mappings().one()))
        await session.commit()
    return stored


@pytest.mark.parametrize("subsequent", ["unchanged", "protected", "would_lose_observations"])
async def test_voted_replacement_reclassified_before_write_preserves_the_live_output(
    recorded_repair: _Run,
    subsequent: str,
) -> None:
    """Classify a voted replacement using the current output's new ownership and observations.

    Given: A voted replacement whose current output changes before application.
    When: Replan and apply the voted subset.
    Then: The fresh classification withholds the row and preserves the current output.
    """
    run = recorded_repair
    candidate = cast(CandleUpsertRow, (await _five_minute_candidate(run)).row)
    await _seed_old_output(run, candidate)
    dry = _short(run, "voted-overlap")
    vote = await rebuild.plan_rebuild(run.context, dry)
    assert _preview(dry.output_dir)[0]["action"] == "rebuild"
    live = candidate.copy()
    live.update({"timestamp": datetime.now(UTC), "session_id": str(uuid7()), "sequence_id": 1})
    if subsequent == "protected":
        live["source"] = "native"
    if subsequent == "would_lose_observations":
        live["volume"] += 1.0
    assert await run.repository.upsert_candles([live]) == 1
    before = await _higher_rows(run.repository)
    write = _short(run, "write-overlap")
    report = await rebuild.write_rebuild(run.context, write, _vote(dry.output_dir, vote.digest))
    assert report.totals["applied"] == 0
    assert await _higher_rows(run.repository) == before
    assert _preview(write.output_dir)[0]["action"] == subsequent
    receipt = _document(write.output_dir / repair.RECEIPT_FILE)
    bucket = "already_applied" if subsequent == "unchanged" else "changed"
    assert len(cast(list[JsonObject], receipt[bucket])) == 1
    request = replace(_short(run, "verify-overlap"), verification=run.evidence(write.output_dir))
    report_snapshot = _verification(
        await rebuild.plan_rebuild(run.context, request), run.context.run_id
    )
    window = cast(list[JsonObject], report_snapshot["windows"])[0]
    assert window["class"] == ("unchanged_at_write" if subsequent == "unchanged" else "excluded")
    assert window["reason"] == ("" if subsequent == "unchanged" else subsequent)


@dataclass(frozen=True, slots=True)
class _Observation:
    """Business values visible to the planner and repository at a chosen read."""

    values: Mapping[str, object]
    loss: str

    def apply(self, current: CandleVersionRow) -> CandleVersionRow:
        """Keep the exact predecessor identity and change only its observations."""
        return cast(CandleVersionRow, {**current, **self.values})


@pytest.mark.parametrize(
    "observation",
    [
        _Observation({"volume": 21.0}, "lower_volume"),
        _Observation({"volume": nextafter(20.0, inf)}, "lower_volume"),
        _Observation({"trades": 6}, "lower_trades"),
        _Observation({"volume": 21.0, "trades": 6}, "lower_volume+lower_trades"),
        _Observation({"trades": None}, "unknown_current"),
        _Observation({"volume": 21.0, "trades": None}, "lower_volume+unknown_current"),
        *[
            _Observation({field: value}, "unknown_current")
            for field in ("volume", "trades")
            for value in (nan, inf, -inf)
        ],
    ],
)
async def test_observation_loss_agrees_in_plan_preflight_and_under_the_fence(
    recorded_repair: _Run,
    observation: _Observation,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Refuse lower or unknown observations at all three boundaries, including non-finite payloads.

    Given: Rebuilt and current observations that can be lower, unknown or non-finite.
    When: Check the plan, preflight and guarded write.
    Then: The stages agree about what to write and which observations block a replacement.
    """
    run = recorded_repair
    window = await _five_minute_candidate(run)
    candidate = cast(CandleUpsertRow, window.row)
    previous = await _seed_old_output(run, candidate)
    real_read = rebuild._read_output_versions

    async def read_poisoned(
        session: AsyncSession,
        unit: rebuild._ReadUnit,
    ) -> dict[CandleKey, list[CandleVersionRow]]:
        """Expose values that SQLite cannot round-trip, including float NaN on a NOT NULL column."""
        observed = await real_read(session, unit)
        observed[window.key] = [observation.apply(previous)]
        return observed

    with monkeypatch.context() as planner:
        planner.setattr(rebuild, "_read_output_versions", read_poisoned)
        plan = await _five_minute_candidate(run)
    assert (plan.action, plan.reason) == ("would_lose_observations", observation.loss)
    await _guard_observation_at_each_read(run, plan, observation, monkeypatch)


async def _guard_observation_at_each_read(
    run: _Run,
    plan: rebuild._Window,
    observation: _Observation,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Keep the real read and lock path and inject the observed payload at its comparison boundary."""
    repository = run.repository
    compare = SQLAlchemyRepository._synthesized_rebuild_outcome
    begin = repository._begin_candle_reconciliation_write
    in_fence, poison_preflight = False, True

    async def mark_fence(session: AsyncSession, *, sqlite_write_lock: bool) -> None:
        """Take the actual fence before selecting the changed read payload."""
        nonlocal in_fence
        await begin(session, sqlite_write_lock=sqlite_write_lock)
        in_fence = True

    def compare_observed(
        row: CandleUpsertRow,
        expectation: SynthesizedRebuildExpectation,
        current: CandleVersionRow | None,
        history: bool,
        inputs: Sequence[CandleVersionRow],
    ) -> SynthesizedRebuildOutcome:
        """Expose only the current payload while keeping both repository guard phases real."""
        if current is not None and (poison_preflight or in_fence):
            current = observation.apply(current)
        return compare(row, expectation, current, history, inputs)

    monkeypatch.setattr(repository, "_begin_candle_reconciliation_write", mark_fence)
    monkeypatch.setattr(
        SQLAlchemyRepository, "_synthesized_rebuild_outcome", staticmethod(compare_observed)
    )
    at = datetime.now(UTC)
    row = rebuild._write_row(plan, run.context.run_id, at, 1)
    expected = {plan.key: rebuild._expectation(plan)}
    refused = SynthesizedRebuildOutcome("would_lose_observations", observation.loss)
    dry = await repository.rebuild_synthesized_candles([row], expected=expected, dry_run=True)
    assert dry.outcomes[plan.key] == refused
    preflight = await repository.rebuild_synthesized_candles([row], expected=expected)
    assert preflight.outcomes[plan.key] == refused and not in_fence
    poison_preflight = False
    locked = await repository.rebuild_synthesized_candles([row], expected=expected)
    assert locked.outcomes[plan.key] == refused and in_fence
    assert len(await _higher_rows(repository)) == 1


def test_application_batches_respect_row_and_grid_ceilings() -> None:
    """Bound a dense group by rows and a full-day grid by minutes.

    Given: Dense rows and grids that reach the per-chunk limits.
    When: Split the approved rows into chunks.
    Then: Each chunk stays within both the row and full-grid limits.
    """
    windows = []
    for index in range(101):
        opening = OPEN + timedelta(minutes=index * 5)
        row = candle(XRP, opening, timeframe="5m")
        windows.append(rebuild._Window((XRP, "5m", opening), "XRP-EUR", 1, "insert", row=row))
    daily = candle(XRP, _END, timeframe="1d")
    windows.append(rebuild._Window((XRP, "1d", _END), "XRP-EUR", 1, "insert", row=daily))
    chunks = rebuild._chunks(windows, str(uuid7()), _END + timedelta(days=2))
    assert [len(chunk.rows) for chunk in chunks] == [100, 1, 1]
    assert [row["sequence_id"] for chunk in chunks for row in chunk.rows] == list(range(1, 103))
    assert all(
        chunk.expected.keys() == {rebuild.candle_key(row) for row in chunk.rows} for chunk in chunks
    )


async def test_live_edge_checks_dry_run_and_confirmed_write_before_a_read(
    recorded_repair: _Run,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Refuse an otherwise valid vote when its end falls inside the invocation's live edge.

    Given: A rebuild end inside the 180-minute live edge.
    When: Attempt both preview and confirmed write.
    Then: Both modes refuse before opening a database session.
    """
    run = recorded_repair
    dry = run.request("old-vote")
    plan = await rebuild.plan_rebuild(run.context, dry)
    recent = replace(run.context, clock=lambda: _END + timedelta(minutes=179))
    session = MagicMock()
    monkeypatch.setattr(run.repository, "session", session)
    with pytest.raises(repair.RepairRefusedError, match="180 minutes"):
        await rebuild.plan_rebuild(recent, run.request("too-recent"))
    with pytest.raises(repair.RepairRefusedError, match="180 minutes"):
        await rebuild.write_rebuild(
            recent, run.request("too-recent-write"), _vote(dry.output_dir, plan.digest)
        )
    session.assert_not_called()


@dataclass(frozen=True, slots=True)
class _RetryCase:
    """A known rollback or terminal constraint error and its expected receipt."""

    sqlstate: str
    outcome: str
    failures: int
    stop: str = "completed"

    def error(self) -> OperationalError:
        """Supply only driver metadata that the retry classifier inspects."""
        name = "uq_candle_itf_open" if self.outcome == "natural_key_conflict" else "other_index"
        return OperationalError(None, None, MagicMock(sqlstate=self.sqlstate, constraint_name=name))


@pytest.mark.parametrize(
    "case",
    [
        _RetryCase("40001", "serialization_failure", 1),
        _RetryCase("40P01", "deadlock", 1),
        _RetryCase("23505", "natural_key_conflict", 1),
        _RetryCase("23505", "unique_violation", 1, "unique_violation"),
        _RetryCase("40001", "serialization_failure", 4, "attempts_exhausted"),
        _RetryCase("55P03", "lock_timeout", 3, "lock_timeouts"),
    ],
)
async def test_rebuild_retry_and_terminal_constraint_receipts(
    recorded_repair: _Run,
    case: _RetryCase,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Exercise all known-rollback paths with the actual rebuild adapter and durable receipt.

    Given: Retryable and terminal errors from the guarded writer.
    When: Apply the rebuild and inspect the per-attempt effects.
    Then: Retries keep the same rows and the receipt records the final outcome.
    """
    run = recorded_repair
    dry = _short(run, "retry-plan")
    approved = await rebuild.plan_rebuild(run.context, dry)
    writer = run.repository.rebuild_synthesized_candles
    rows_by_attempt: list[list[CandleUpsertRow]] = []

    async def attempt(
        rows: list[CandleUpsertRow],
        *,
        expected: Mapping[CandleKey, SynthesizedRebuildExpectation],
        dry_run: bool = False,
    ) -> SynthesizedRebuildResult:
        """Inject a known rollback before the first successful whole chunk."""
        if not dry_run:
            rows_by_attempt.append([row.copy() for row in rows])
            if len(rows_by_attempt) <= case.failures:
                raise case.error()
        return await writer(rows, expected=expected, dry_run=dry_run)

    reconcile = AsyncMock(wraps=run.repository.get_session_candle_versions)
    monkeypatch.setattr(run.repository, "rebuild_synthesized_candles", attempt)
    monkeypatch.setattr(run.repository, "get_session_candle_versions", reconcile)
    sleep = AsyncMock()
    context = replace(run.context, sleep=sleep)
    write = _short(run, "retry-write")
    report = await rebuild.write_rebuild(context, write, _vote(dry.output_dir, approved.digest))
    assert report.stop_reason == case.stop
    reconcile.assert_not_awaited()
    receipt = _document(write.output_dir / repair.RECEIPT_FILE)
    records = cast(list[JsonObject], receipt["chunks"])
    attempts = cast(list[JsonObject], records[0]["attempts"])
    states = [case.outcome] * case.failures + (["acknowledged"] if case.stop == "completed" else [])
    assert [attempt["outcome"] for attempt in attempts] == states
    assert records[0]["attempt_count"] == len(states)
    assert all(rows == rows_by_attempt[0] for rows in rows_by_attempt)
    assert [call.args[0] for call in sleep.await_args_list] == [0.25, 0.5, 1.0][: len(states) - 1]
    assert report.totals["applied"] == int(case.stop == "completed")
    assert report.totals["not_applied"] == int(case.stop != "completed")
    if case.stop != "completed":
        request = replace(_short(run, "verify-stop"), verification=run.evidence(write.output_dir))
        check = _verification(await rebuild.plan_rebuild(run.context, request), run.context.run_id)
        assert cast(list[JsonObject], check["windows"])[0]["reason"] == "not_applied"


async def test_lost_acknowledgement_reports_a_superseded_version(
    recorded_repair: _Run,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Find the historical version by exact payload when a live successor already replaced it.

    Given: A committed rebuild that loses its acknowledgement and gains a live successor.
    When: Reconcile the original write against stored versions.
    Then: The exact matched historical version is reported as superseded.
    """
    run = recorded_repair
    dry = _short(run, "superseded-plan")
    approved = await rebuild.plan_rebuild(run.context, dry)
    original = run.repository.rebuild_synthesized_candles

    async def lose_ack(
        rows: list[CandleUpsertRow],
        *,
        expected: Mapping[CandleKey, SynthesizedRebuildExpectation],
        dry_run: bool = False,
    ) -> SynthesizedRebuildResult:
        """Commit both versions before returning a transport error to the caller."""
        result = await original(rows, expected=expected, dry_run=dry_run)
        if dry_run:
            return result
        successor = rows[0].copy()
        successor.update(
            {
                "session_id": str(uuid7()),
                "timestamp": datetime.now(UTC),
                "volume": rows[0]["volume"] + 1.0,
            }
        )
        assert await run.repository.upsert_candles([successor]) == 1
        raise ConnectionError("lost acknowledgement")

    monkeypatch.setattr(run.repository, "rebuild_synthesized_candles", lose_ack)
    report = await rebuild.write_rebuild(
        run.context, _short(run, "superseded"), _vote(dry.output_dir, approved.digest)
    )
    assert report.stop_reason == "uncertain_acknowledgement"
    assert report.totals["applied_superseded"] == 1 and report.totals["uncertain"] == 0


@pytest.mark.parametrize("settles_as_applied", [False, True])
async def test_reconciliation_proceeds_while_the_unacknowledged_transaction_is_open(
    recorded_repair: _Run,
    monkeypatch: pytest.MonkeyPatch,
    settles_as_applied: bool,
) -> None:
    """Keep uncommitted versions uncertain and let reconciliation read behind the writer fence.

    Given: A write attempt whose transaction remains open after losing acknowledgement.
    When: Reconcile the attempted rows from another session.
    Then: The read proceeds and uncommitted rows remain uncertain.
    """
    run = recorded_repair
    dry = _short(run, "active-plan")
    approved = await rebuild.plan_rebuild(run.context, dry)
    write = _short(run, "active")
    original = run.repository.rebuild_synthesized_candles
    async with run.repository.session() as held:

        async def leave_open(
            rows: list[CandleUpsertRow],
            *,
            expected: Mapping[CandleKey, SynthesizedRebuildExpectation],
            dry_run: bool = False,
        ) -> SynthesizedRebuildResult:
            """Stage the selected row without committing or releasing the original lock."""
            if dry_run:
                return await original(rows, expected=expected, dry_run=True)
            await held.execute(text("BEGIN IMMEDIATE"))
            await held.execute(insert(Candle).values(**rows[0]))
            raise ConnectionError("connection outcome unknown")

        monkeypatch.setattr(run.repository, "rebuild_synthesized_candles", leave_open)
        async with asyncio.timeout(3):
            report = await rebuild.write_rebuild(
                run.context, write, _vote(dry.output_dir, approved.digest)
            )
        assert report.totals["uncertain"] == 1
        assert report.stop_reason == "uncertain_acknowledgement"
        if settles_as_applied:
            await held.commit()
        else:
            await held.rollback()
    assert len(await _higher_rows(run.repository)) == int(settles_as_applied)
    request = replace(_short(run, "verify-active"), verification=run.evidence(write.output_dir))
    checked = _verification(await rebuild.plan_rebuild(run.context, request), run.context.run_id)
    assert cast(list[JsonObject], checked["windows"])[0]["class"] == (
        "verified" if settles_as_applied else "uncertain"
    )


async def test_failure_between_chunks_keeps_the_committed_chunk_and_final_receipt(
    recorded_repair: _Run,
) -> None:
    """Persist the first chunk, then raise during the pause before the daily window.

    Given: An approved rebuild whose first chunk commits before a pause fails.
    When: Attempt the full write and inspect its artifacts.
    Then: The committed chunk and the final error receipt remain on disk.
    """
    run = recorded_repair
    dry = run.request("pause-vote")
    approved = await rebuild.plan_rebuild(run.context, dry)
    context = replace(run.context, sleep=AsyncMock(side_effect=RuntimeError("pause interrupted")))
    write = run.request("pause-write")
    with pytest.raises(RuntimeError, match="pause interrupted"):
        await rebuild.write_rebuild(context, write, _vote(dry.output_dir, approved.digest))
    receipt = _document(write.output_dir / repair.RECEIPT_FILE)
    assert receipt["stop_reason"] == "error"
    assert (receipt["scheduled"], receipt["reached"]) == (6, 5)
    assert cast(JsonObject, receipt["totals"])["applied"] == 5
    assert len(await _higher_rows(run.repository)) == 5
    for artifact in write.output_dir.iterdir():
        assert artifact.stat().st_mode & 0o777 == 0o600


@pytest.mark.parametrize(
    ("observations", "expected"),
    [((0, 0, 0), True), ((1, 0, 0), False), ((0, 0, 1), False), ((0, 0, None), True)],
)
async def test_run_receipts_bind_start_and_successful_chunk_samples(
    recorded_repair: _Run,
    observations: tuple[int, int, int | None],
    expected: bool,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Record sole-client transitions using the committing attempt and the start sample.

    Given: Mocked clients and acknowledged, failed or uncertain chunk attempts.
    When: Apply the rebuild and read the run and chunk receipts.
    Then: Only acknowledged attempts contribute committing samples to the reported status.
    """
    run = recorded_repair
    start_count, second, third = observations
    sample_time = datetime.now(UTC)
    start = AsyncMock(return_value=ClientSample(start_count, sample_time))
    monkeypatch.setattr(rebuild, "sample_other_client_backends", start)
    repository = MagicMock(spec=SQLAlchemyRepository, dialect_name="postgresql")
    repository.session.side_effect = run.repository.session
    stub = _SampledWrites((second, third), sample_time)
    repository.rebuild_synthesized_candles = AsyncMock(side_effect=stub.__call__)
    context = replace(run.context, repository=repository, sleep=AsyncMock())
    first = await _five_minute_candidate(run)
    daily_row = candle(XRP, _END - timedelta(days=1), timeframe="1d")
    daily = rebuild._Window(rebuild.candle_key(daily_row), "XRP-EUR", 1, "insert", row=daily_row)
    request = run.request("sampled")
    stop, _ = await rebuild._apply(
        context, request, _vote(run.root, "a" * 64), rebuild._Selection([first, daily], [], [], [])
    )
    receipt = _document(request.output_dir / repair.RECEIPT_FILE)
    assert stop == "completed" and receipt["sole_client_write"] is expected
    assert receipt["start_sample"] == {
        "other_client_backends": start_count,
        "observed_at": sample_time.isoformat(),
    }
    chunks = cast(list[JsonObject], receipt["chunks"])
    assert cast(list[JsonObject], chunks[0]["attempts"])[0]["outcome"] == "lock_timeout"
    for count, chunk in zip((second, third), chunks, strict=True):
        if count is None:
            assert chunk["sole_client"] == "not_observed" and chunk["client_sample"] is None
        else:
            assert chunk["sole_client"] is (count == 0)
            assert chunk["client_sample"] == {
                "other_client_backends": count,
                "observed_at": (
                    sample_time + timedelta(seconds=cast(int, chunk["chunk"]))
                ).isoformat(),
            }


class _SampledWrites:
    """Supply committed samples after a known first rollback."""

    def __init__(self, counts: tuple[int, int | None], at: datetime) -> None:
        """Remember the sample of each committing attempt."""
        self._counts = counts
        self._at = at
        self.attempts = 0

    async def __call__(
        self,
        rows: list[CandleUpsertRow],
        *,
        expected: Mapping[CandleKey, SynthesizedRebuildExpectation],
    ) -> SynthesizedRebuildResult:
        """Return only the observations made for successful transactions."""
        assert len(rows) == len(expected)
        self.attempts += 1
        if self.attempts == 1:
            raise OperationalError(None, None, MagicMock(sqlstate="55P03"))
        count = self._counts[self.attempts - 2]
        if count is None:
            return SynthesizedRebuildResult(
                {key: SynthesizedRebuildOutcome("inputs_changed") for key in expected}
            )
        return SynthesizedRebuildResult(
            {key: SynthesizedRebuildOutcome("inserted") for key in expected},
            ClientSample(count, self._at + timedelta(seconds=self.attempts - 1)),
        )


@pytest.mark.parametrize(
    "field",
    [
        "run_id",
        "voted_digest",
        "stop_reason",
        "approval_sha256",
        "receipt_sha256",
        "receipt_lines_sha256",
        "rebound_jsonl",
        "rebound_totals",
        "rebound_end",
        "rebound_records",
    ],
)
async def test_verification_mutations_fail_before_any_database_read(
    recorded_repair: _Run,
    field: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Refuse each record-binding mismatch and still validate rebound receipt contents.

    Given: A verification record or receipt with an altered field, hash or reference.
    When: Request verification and monitor repository access.
    Then: The command refuses the inconsistency before reading the database.
    """
    run = recorded_repair
    dry, write = _short(run, "file-vote"), _short(run, "file-write")
    approved = await rebuild.plan_rebuild(run.context, dry)
    await rebuild.write_rebuild(run.context, write, _vote(dry.output_dir, approved.digest))
    artifacts = write.output_dir
    if field == "rebound_jsonl":
        (artifacts / repair.RECEIPT_LINES_FILE).write_bytes(b"")
    if field == "rebound_totals":
        receipt = _document(artifacts / repair.RECEIPT_FILE)
        cast(JsonObject, receipt["totals"])["applied"] = 0
        _rewrite_receipt(artifacts, receipt)
    if field in {"rebound_end", "rebound_records"}:
        path = artifacts / repair.APPROVAL_FILE
        approval = _document(path)
        if field == "rebound_end":
            approval["end"] = _END.isoformat()
        else:
            cast(JsonObject, approval["run_records"])["sha256"] = "0" * 64
        _save(path, approval)
    evidence = run.evidence(artifacts)
    mismatches = {
        "run_id": str(uuid7()),
        "voted_digest": "0" * 64,
        "stop_reason": "error",
        **dict.fromkeys(("approval_sha256", "receipt_sha256", "receipt_lines_sha256"), "0" * 64),
    }
    if field in mismatches:
        _save(evidence.record, {**_document(evidence.record), field: mismatches[field]})
    session = MagicMock()
    monkeypatch.setattr(run.repository, "session", session)
    request = replace(_short(run, "file-verify"), verification=evidence)
    with pytest.raises(repair.RepairRefusedError):
        await rebuild.plan_rebuild(run.context, request)
    session.assert_not_called()


async def test_verification_accepts_a_current_fold_from_newer_committed_minutes(
    recorded_repair: _Run,
) -> None:
    """Verify a superseded target whose current value is the fold at the verification snapshot.

    Given: A superseded rebuild whose current replacement agrees with newer minute inputs.
    When: Verify the original write.
    Then: The verification snapshot still classifies the target as verified.
    """
    run = recorded_repair
    dry, write = _short(run, "old-inputs"), _short(run, "old-output")
    approved = await rebuild.plan_rebuild(run.context, dry)
    await rebuild.write_rebuild(run.context, write, _vote(dry.output_dir, approved.digest))
    assert (
        await run.repository.upsert_candles(
            [
                candle(
                    XRP,
                    OPEN + timedelta(minutes=2),
                    timestamp=datetime.now(UTC),
                    volume=1.0,
                    trades=1,
                )
            ]
        )
        == 1
    )
    candidate = await _five_minute_candidate(run)
    assert candidate.action == "rebuild"
    output = rebuild._write_row(candidate, str(uuid7()), datetime.now(UTC), 1)
    assert await run.repository.upsert_candles([output]) == 1
    request = replace(_short(run, "new-verify"), verification=run.evidence(write.output_dir))
    observed = _verification(await rebuild.plan_rebuild(run.context, request), run.context.run_id)
    row = cast(list[JsonObject], observed["windows"])[0]
    assert row["class"] == "verified" and row["still_written"] is False


async def test_recorded_trades_correction_uses_the_receipt_write_time(
    recorded_repair: _Run,
) -> None:
    """Authenticate a real stored-trade correction, then refuse a different version time.

    Given: A recorded correction calculated from stored trades.
    When: Verify its authoring time, then alter the stored version timestamp.
    Then: The original version is accepted and the altered version is refused.
    """
    run = recorded_repair
    opening = OPEN + timedelta(minutes=8)
    await run.repository.upsert_candles([candle(XRP, opening, timestamp=opening)])
    await run.repository.upsert_trades(
        [trade(XRP, opening + timedelta(seconds=10), 2.0, 3.0, "trade")]
    )
    first, written = run.root / "trades-preview", run.root / "trades-write"
    first.mkdir()
    written.mkdir()
    request = repair.RepairRequest(
        source=repair.RepairSource.TRADES,
        exchange="kraken",
        symbols=("XRP-EUR",),
        all_mapped=False,
        snapshot_dir=None,
        output_dir=first,
        window=repair.RepairWindow(opening, opening + timedelta(minutes=1)),
    )
    context = replace(run.context, run_id=str(uuid7()))
    drafted = await repair.plan_repair(context, request)
    report = await repair.write_repair(
        context, replace(request, output_dir=written), _vote(first, drafted.digest)
    )
    assert report.totals["applied"] == 1
    _manifest(run, _record(written))
    valid = replace(run.request("trades-rebuild"), repair_runs=(written,))
    summary = (await rebuild.plan_rebuild(run.context, valid)).summary
    assert summary["actions"] == {"insert": 6}
    async with run.repository.session() as session:
        await session.execute(
            update(Candle)
            .where(Candle.session_id == context.run_id)
            .values(timestamp=datetime.now(UTC) - timedelta(hours=1))
        )
        await session.commit()
    with pytest.raises(repair.RepairRefusedError, match="correction identity or timestamp"):
        await rebuild.plan_rebuild(
            run.context, replace(valid, output_dir=run.request("refuse-trades").output_dir)
        )


@pytest.mark.parametrize("list_name", ["already_applied", "changed"])
async def test_repair_selection_overlap_keeps_not_voted_independent(
    recorded_repair: _Run,
    list_name: str,
) -> None:
    """Permit a repair selection reference to appear in not_voted and one terminal selection list.

    Given: A minute appearing in the not-voted list and one terminal selection list.
    When: Validate the receipt's selection references.
    Then: The independent not-voted reference is allowed to overlap the terminal list.
    """
    run = recorded_repair
    directory = run.repair_dir
    receipt = _document(directory / repair.RECEIPT_FILE)
    reference: JsonObject = {
        "instrument_public_id": XRP,
        "minute_utc": (OPEN + timedelta(minutes=4)).isoformat(),
        "action": "insert",
    }
    receipt[list_name] = [reference]
    receipt["not_voted"] = [reference]
    _rewrite_receipt(directory, receipt)
    _manifest(run, _record(directory))
    result = await rebuild.plan_rebuild(run.context, run.request("overlap"))
    summaries = cast(list[JsonObject], result.summary["runs"])
    assert result.summary["actions"] == {"insert": 6}
    assert summaries[0]["already_applied_unproven"] == int(list_name == "already_applied")


@pytest.mark.parametrize(
    "key,value", [("dialect", "sqlite"), ("host", "elsewhere"), ("port", 5433)]
)
def test_settlement_endpoint_checks_each_part_of_the_effective_target(
    recorded_repair: _Run,
    key: str,
    value: str | int,
) -> None:
    """Bind the collector's configured target without comparing its observed server address.

    Given: A settlement capture with a changed endpoint field or observed address.
    When: Validate the capture against the approved endpoint.
    Then: Configured endpoint differences are refused and observed server address differences are
        allowed.
    """
    run = _capture_target(recorded_repair)
    record = _record(run.repair_dir)
    capture = _capture(record, run.context.database)
    record["settlements"] = [capture]
    valid = rebuild._RunRecord.model_validate(record)
    rebuild._check_record(valid, run.context.database)
    cast(JsonObject, capture["endpoint"])[key] = value
    changed = rebuild._RunRecord.model_validate(record)
    with pytest.raises(repair.RepairRefusedError, match="endpoint differs"):
        rebuild._check_record(changed, run.context.database)


@pytest.mark.parametrize(
    "field",
    [
        "run_id",
        "voted_digest",
        "stop_reason",
        "approval_sha256",
        "receipt_sha256",
        "receipt_lines_sha256",
        "application_name",
        "observed_addresses",
        "container",
        "settlements",
    ],
)
def test_run_record_requires_each_named_field(recorded_repair: _Run, field: str) -> None:
    """Require the exact run record even for an acknowledged run with no settlements.

    Given: A settled repair record from which one required field is removed.
    When: Validate the record schema.
    Then: Each omitted field causes refusal.
    """
    run = recorded_repair
    raw = _record(run.repair_dir)
    raw.pop(field)
    _manifest(run, raw)
    with pytest.raises(repair.RepairRefusedError, match="invalid format"):
        rebuild._load_runs(run.request("missing-field"), run.context.database)


async def test_sqlite_enumerates_the_approved_scope_inside_begin_immediate(
    recorded_repair: _Run,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Record the opener from the production enumeration and attest its approved scope.

    Given: A locally recorded write and a SQLite statement trace.
    When: Enumerate the approved repair scope.
    Then: The enumeration begins with BEGIN IMMEDIATE and returns the approved versions.
    """
    run = recorded_repair
    record = rebuild._load_runs(run.request("enum"), run.context.database).runs[0]
    observed: list[str] = []

    @asynccontextmanager
    async def inspect_session() -> AsyncIterator[AsyncSession]:
        """Record the real first statement while leaving its transaction intact."""
        async with run.repository.session() as session:
            execute = AsyncMock(wraps=session.execute)
            with monkeypatch.context() as patch:
                patch.setattr(session, "execute", execute)
                yield session
            observed.extend(str(call.args[0]) for call in execute.await_args_list)

    stub = MagicMock(spec=SQLAlchemyRepository, dialect_name="sqlite")
    stub.session.side_effect = inspect_session
    assert len(await rebuild._enumerate_repair(stub, record)) == 2
    assert observed[0] == "BEGIN IMMEDIATE"
    assert all("candles.session_id =" in statement for statement in observed[1:])


async def test_approval_digest_binds_each_planning_input(recorded_repair: _Run) -> None:
    """Change one dimension at a time and hash the actual approval and preview serializers.

    Given: An approval and preview with one planning input changed at a time.
    When: Compute the digest for each variant.
    Then: Every bound input changes the digest.
    """
    run = recorded_repair
    request = _short(run, "digest")
    context = run.context
    at = context.clock()
    recorded = rebuild._load_runs(request, context.database)
    scope = await rebuild._derive_scope(context, recorded, at)
    windows = await rebuild._plan(context, scope, request, at)
    preview = repair.sha256_hex(rebuild._preview_bytes(windows.values()))
    digest = _approval_digest(context, request, scope, preview)
    assert digest == _approval_digest(context, request, scope, preview)
    for changed_context in (
        replace(context, database={"dialect": "sqlite", "path": "/different.db"}),
        replace(context, code_revision={"application.py": "1" * 64}),
    ):
        assert digest != _approval_digest(changed_context, request, scope, preview)
    assert digest != _approval_digest(
        context, replace(request, end=request.end + timedelta(minutes=1)), scope, preview
    )
    for changed_records in (
        replace(recorded, digest="2" * 64),
        replace(recorded, instruments={XRP: "XRP-USD"}),
    ):
        assert digest != _approval_digest(
            context, request, replace(scope, recorded=changed_records), preview
        )
    first = scope.resolved[0]
    resolved = replace(first, resolution={(XRP, OPEN): {"outcome": "not_applied", "version": None}})
    assert digest != _approval_digest(
        context, request, replace(scope, resolved=(resolved,)), preview
    )
    bar = windows[XRP, "5m", OPEN]
    payload = cast(CandleUpsertRow, bar.row).copy()
    payload["volume"] += 1.0
    windows[bar.key] = replace(bar, row=payload)
    changed_preview = repair.sha256_hex(rebuild._preview_bytes(windows.values()))
    assert digest != _approval_digest(context, request, scope, changed_preview)


def _approval_digest(
    context: repair.RunContext,
    request: rebuild.RebuildRequest,
    scope: rebuild._Scope,
    preview: str,
) -> str:
    """Hash the same canonical approval bytes the CLI writes."""
    return repair.sha256_hex(
        repair.canonical_json(rebuild._approval(context, request, scope, preview))
    )


def test_read_units_use_at_most_twenty_instruments_and_never_cross_a_utc_day() -> None:
    """Partition the reads by calendar day before dividing instruments into batches.

    Given: Candle windows for many instruments on either side of UTC midnight.
    When: Group the windows into read units.
    Then: Each unit stays within one UTC day and includes at most twenty instruments.
    """
    keys = [(f"instrument-{index:02}", "5m", OPEN) for index in range(41)]
    keys.append(("instrument-00", "5m", OPEN + timedelta(days=1)))
    units = rebuild._units(keys)
    assert [len(unit.instruments) for unit in units] == [20, 20, 1, 1]
    assert all(key[2].date() == unit.day.date() for unit in units for key in unit.keys)
    assert {key for unit in units for key in unit.keys} == set(keys)


@pytest.mark.asyncio
async def test_output_read_ignores_a_window_outside_the_unit(recorded_repair: _Run) -> None:
    """Return only the requested window from the same instrument and day.

    Given: Two higher-timeframe rows of the same instrument and UTC day.
    When: Read a unit containing only the first window from SQLite.
    Then: Only the first window appears in the result.
    """
    requested = (XRP, "5m", OPEN)
    (unit,) = rebuild._units([requested])
    async with recorded_repair.repository.session() as session:
        await session.execute(
            insert(Candle),
            [
                candle(XRP, OPEN, timeframe="5m", close=2.0),
                candle(XRP, OPEN + timedelta(minutes=5), timeframe="5m", close=3.0),
            ],
        )
        outputs = await rebuild._read_output_versions(session, unit)

    assert set(outputs) == {requested}
    (version,) = outputs[requested]
    assert version["close"] == 2.0


@pytest.mark.asyncio
async def test_output_read_bind_count_stays_fixed_for_a_full_unit() -> None:
    """Count expanded PostgreSQL binds for sparse and full reads.

    Given: One UTC day and the same 20 instruments in a sparse and a full unit.
    When: Compile the issued queries with the asyncpg dialect and expand list binds.
    Then: Both queries bind 20 instruments, two time bounds and six timeframes.
    """
    instruments = tuple(str(uuid4()) for _ in range(20))
    day = OPEN.replace(hour=0)
    windows = rebuild._count_windows(
        [
            (instrument, day + timedelta(minutes=offset))
            for instrument in instruments
            for offset in range(0, 1440, 5)
        ]
    )
    (full,) = rebuild._units(windows)
    sparse = replace(full, keys=tuple((name, "5m", day) for name in full.instruments))
    assert len(full.keys) == 20 * 463
    assert len(sparse.keys) == 20

    execute = AsyncMock(return_value=MagicMock(mappings=MagicMock(return_value=[])))
    session = MagicMock(spec=AsyncSession, execute=execute)
    for unit in (sparse, full):
        await rebuild._read_output_versions(session, unit)

    bind_counts: list[int] = []
    for call in execute.await_args_list:
        (statement,) = call.args
        assert isinstance(statement, Select)
        compiled = statement.compile(
            dialect=PGDialect_asyncpg(), compile_kwargs={"render_postcompile": True}
        )
        assert compiled.params is not None
        assert compiled.positiontup is not None
        assert len(compiled.params) == len(compiled.positiontup)
        bind_counts.append(len(compiled.positiontup))
    assert bind_counts == [28, 28]
