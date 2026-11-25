"""Exercise authenticated rebuilds and verification against disposable SQLite."""

import asyncio
import csv
import io
import json
from collections.abc import AsyncIterator
from collections.abc import Mapping
from contextlib import asynccontextmanager
from dataclasses import dataclass
from dataclasses import replace
from datetime import UTC
from datetime import datetime
from datetime import timedelta
from pathlib import Path
from typing import cast
from unittest.mock import AsyncMock
from unittest.mock import MagicMock
from uuid import uuid7

import pytest
import pytest_asyncio
from sqlalchemy import insert
from sqlalchemy import select
from sqlalchemy import text
from sqlalchemy import update
from sqlalchemy.exc import OperationalError
from sqlalchemy.ext.asyncio import AsyncSession

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
from snapper.data.repository_types import SynthesizedRebuildResult
from tests.helpers.candle_repair import OPEN
from tests.helpers.candle_repair import XRP
from tests.helpers.candle_repair import candle
from tests.helpers.candle_repair import kraken_row
from tests.helpers.candle_repair import seed_instrument
from tests.helpers.candle_repair import snapshot_request
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
    """One real SQLite B write and its standalone coordinator record."""

    root: Path
    repository: SQLAlchemyRepository
    context: repair.RunContext
    repair_dir: Path
    records: Path

    def request(
        self, name: str, end: datetime = _END, verification: rebuild.VerificationFiles | None = None
    ) -> rebuild.RebuildRequest:
        """Create a new B2 output directory beside the evidence."""
        output = self.root / name
        output.mkdir()
        return rebuild.RebuildRequest((self.repair_dir,), self.records, end, output, verification)

    def evidence(self, directory: Path) -> rebuild.VerificationFiles:
        """Bind a test B2 write to a machine-readable verification record."""
        record = _save(self.root / "verification.json", _binding(directory))
        return rebuild.VerificationFiles(directory, record)


@pytest_asyncio.fixture
async def recorded_repair(tmp_path: Path) -> AsyncIterator[_Run]:
    """Run B against one zero and one absent minute, then bind its actual receipt."""
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
    """Construct an approved B2 call without sleeps."""
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
    """Rebuild all six sparse windows in plan order and verify the first write's full scope."""
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
    """A stale live fold may supersede B2 and a new, separately voted write can restore it."""
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
    """Read the 5m window ending exactly at the bound and report the rest as after_end."""
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
    """Refuse records, mismatched bytes and missing directory bindings before DB access."""
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
    """Verify preflight refusals before catalog or candle reads."""
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
    """Use the shared retry loop, then find committed rows despite an acknowledgement failure."""
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
    """Match decision 16 for committed, uncertain and no-write runs."""
    sample = ClientSample(start, _END) if start is not None else None
    records: list[JsonObject] = [{"sole_client": value} for value in chunks]
    assert rebuild._sole_client_write("postgresql", sample, records) == expected


@pytest.mark.parametrize("latest", ["running_new", "running_old", "finished", "no_publisher"])
@pytest.mark.asyncio
async def test_recorded_process_guard_uses_newest_starts(
    recorded_repair: _Run, latest: str
) -> None:
    """Refuse a newer running writer and accept records of older or stopped writers."""
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


def _capture_target(run: _Run) -> _Run:
    """Bind mocked PostgreSQL evidence while keeping all reads and writes on SQLite."""
    database = repair.database_endpoint("postgresql+asyncpg://localhost:5432/scratch")
    path = run.repair_dir / repair.APPROVAL_FILE
    _save(path, {**_document(path), "database": database})
    return replace(run, context=replace(run.context, database=database))


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
    """Respect client visibility, canonical address identity and both legacy match paths."""
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
    if mode == "recent_reconnect":
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
    """An invalid earlier capture refuses the record even without an uncertain row."""
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
    """Only an absent row needs clearance, and older inconclusive captures cannot veto it."""
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
    """Refuse a committed version whose identity, time or any voted value differs."""
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
        ("absent", "no_input"),
    ],
)
@pytest.mark.asyncio
async def test_planning_reports_each_exclusion(
    recorded_repair: _Run, case: str, expected: str
) -> None:
    """Expose source ownership, observation loss, history or missing inputs in the preview."""
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
        elif case in {"invalid", "future"}:
            minute = OPEN + timedelta(minutes=2)
            row = candle(
                XRP,
                minute,
                timestamp=datetime.now(UTC) + timedelta(days=1),
                complete=case == "future",
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
    if case in {"invalid", "future"}:
        assert line["input_minutes"] == "2"
        assert line["absent_minutes"] == "2"
        assert line["reason"] == f"{case}_minute@{(OPEN + timedelta(minutes=2)).isoformat()}"


@pytest.mark.parametrize("phase", ["preflight", "under_lock"])
@pytest.mark.asyncio
async def test_guard_refusal_appears_in_the_correct_artifact(
    recorded_repair: _Run, monkeypatch: pytest.MonkeyPatch, phase: str
) -> None:
    """Keep rejected input fingerprints visible in the plan or guarded row receipt."""
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
    """Refuse missing or unreferenced B versions instead of expanding or dropping scope."""
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
    """A B uncertain row superseded later is acceptable to verification but changes a vote."""
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
    """Use mocked PostgreSQL outcomes to distinguish no write, uncertainty and observed counts."""
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
    """Refuse a mismatched evidence file or a corrupted committed B2 identity or payload."""
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
    """Refuse structurally malformed JSON and non-standard JSON numeric tokens."""
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
        ("out_of_scope", "outside its write preview"),
        ("duplicate_selection", "invalid selection list"),
        ("overlapping_selection", "overlaps already_applied"),
        ("identity", "identity differs"),
        ("lines", "differs from receipt.json"),
    ],
)
@pytest.mark.asyncio
async def test_bound_receipt_consistency_is_checked_before_any_read(
    recorded_repair: _Run, monkeypatch: pytest.MonkeyPatch, defect: str, message: str
) -> None:
    """Matching hashes alone cannot authorize a structurally inconsistent receipt."""
    run = recorded_repair
    receipt = _document(run.repair_dir / repair.RECEIPT_FILE)
    _alter_receipt(receipt, defect)
    _rewrite_receipt(run.repair_dir, receipt)
    if defect == "lines":
        (run.repair_dir / repair.RECEIPT_LINES_FILE).write_bytes(b"")
    record = _record(run.repair_dir)
    if defect == "identity":
        record["voted_digest"] = "b" * 64
    _manifest(run, record)
    forbidden = MagicMock()
    monkeypatch.setattr(run.repository, "session", forbidden)
    with pytest.raises(repair.RepairRefusedError, match=message):
        await rebuild.plan_rebuild(run.context, run.request("bad-receipt"))
    forbidden.assert_not_called()


def _alter_receipt(receipt: JsonObject, defect: str) -> None:
    """Mutate precisely the part of the receipt checked by the test."""
    chunk = cast(list[JsonObject], receipt["chunks"])[0]
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
    """Reuse B's actual resume receipt, counting its no-op lines only against the earlier run."""
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
    """Require aligned keys, validated payload and predecessor evidence for write previews."""
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
    """A new minute after the vote removes every affected window from the writable subset."""
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
    """Check READ COMMITTED enumeration and repeatable read verification with a mock."""
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
async def test_sqlite_verification_holds_a_snapshot_across_the_two_selects(
    recorded_repair: _Run, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Commit a paired input/output batch between the reads and prove neither version enters."""
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
    snapshot = await rebuild._snapshot(run.context, unit)
    assert snapshot.outputs == {}
    assert len(snapshot.inputs[XRP]) == 2
    assert len(await _higher_rows(run.repository)) == 1


@pytest.mark.asyncio
async def test_cancellation_writes_the_b2_chunk_then_the_final_receipt(
    recorded_repair: _Run, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Leave a fully durable uncertain chunk when a write await is cancelled."""
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
        ("insert_time", "not authored inside"),
        ("snapshot_time", "invalid preview timestamp"),
    ],
)
@pytest.mark.asyncio
async def test_repair_refuses_semantically_inconsistent_evidence(
    recorded_repair: _Run, case: str, message: str
) -> None:
    """Reject authenticated files or stored facts that cannot describe the approved run."""
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
        _alter_b_preview(run.repair_dir, case)
    else:
        await _alter_db_evidence(run, case)
    _manifest(run, _record(run.repair_dir))
    with pytest.raises(repair.RepairRefusedError, match=message):
        await rebuild.plan_rebuild(run.context, request)


def _alter_b_preview(directory: Path, case: str) -> None:
    """Change one B preview fact and preserve its hash binding."""
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
    """Change either the catalog exchange or an insert's author time."""
    statement = (
        update(Instrument).where(Instrument.public_id == XRP).values(exchange="paper")
        if case == "active_instrument"
        else update(Candle)
        .where(Candle.open_at == OPEN + timedelta(minutes=1))
        .values(timestamp=OPEN)
    )
    async with run.repository.session() as session:
        await session.execute(statement)
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
    """Reject a different approved header or frozen resolution even with a matching digest."""
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
    """A missing vote withholds a candidate and an extra window is outside the scope."""
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
    """Treat a bad CSV token or missing schema as a refusal."""
    with pytest.raises(repair.RepairRefusedError, match=message):
        rebuild._parse_preview(data)


@pytest.mark.asyncio
async def test_planner_refuses_an_ambiguous_current_version_or_missing_fold(
    recorded_repair: _Run, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Refuse ambiguous output ownership and an aggregator that emits the wrong window."""
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
