"""Profile one read-only 5C.3 chunk; never merge or invoke a writer lifecycle.

The production owner supplies a verified scope hash and expected spot venues.
Run in an isolated container with an independent 600-second container lifetime,
not in the API process. The parent also kills its owned worker group before ten
minutes. Output contains timing, code identity and counts, never financial rows.
"""

import argparse
import asyncio
import cProfile
import hashlib
import io
import json
import logging
import os
import pstats
import re
import signal
import subprocess
import sys
import time
import warnings
from collections.abc import Callable
from collections.abc import Iterator
from contextlib import contextmanager
from contextlib import suppress
from dataclasses import dataclass
from datetime import UTC
from datetime import datetime
from datetime import timedelta
from inspect import getfile
from pathlib import Path
from typing import TextIO
from typing import TypeVar
from typing import cast

from loguru import logger
from sqlalchemy import event
from sqlalchemy import select
from sqlalchemy import text
from sqlalchemy.engine import ExceptionContext
from sqlalchemy.engine import ExecutionContext
from sqlalchemy.engine import make_url
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.ext.asyncio import async_sessionmaker
from sqlalchemy.ext.asyncio import create_async_engine

import snapper.application.portfolio.pnl_snapshot_planner as planner_module
import snapper.application.portfolio.pnl_snapshotter as snapshotter_module
import snapper.application.portfolio.pnl_timeline_service as timeline_module
import snapper.data.repository as repository_module
from snapper.application.portfolio.pnl_snapshot_planner import ChunkWindow
from snapper.application.portfolio.pnl_snapshotter import PortfolioPnlSnapshotter
from snapper.application.portfolio.pnl_snapshotter import is_futures_class_exchange
from snapper.application.portfolio.pnl_timeline import PnlTimelineResult
from snapper.application.portfolio.pnl_timeline_service import PnlFxRateSource
from snapper.data.models import KNOWN_TO_MAX
from snapper.data.models import PortfolioPnlPoint
from snapper.data.repository import SQLAlchemyRepository
from snapper.data.repository_types import PNL_SAMPLE_CALC_VERSION

_T = TypeVar("_T")
type _ValuationResult = tuple[PnlTimelineResult, tuple[PnlFxRateSource, ...]]
_EXPECTED = {
    "snapshotter": "7a58f67dc406299329aa35560bc4ed672e1160e8eebb3b034fb0ec38a762afef",
    "planner": "ed9a1bbe71767f633e72db6eaab0e10019ada8e651cce9896b2e55eebb150f6c",
    "timeline": "1af4b4492a049d59d1fc97ace5007f3267de6e7e30b8324d797b7b56c5ed3925",
    "repository": "c83747e96dcb17016b7ebe20c6aec5ebaf3b11e11620e4dd6843a8115ad5939e",
}
_FORBIDDEN_SQL = re.compile(
    r"\b(?:INSERT|UPDATE|DELETE|MERGE|CALL|DO|COPY|CREATE|ALTER|DROP|TRUNCATE|GRANT|REVOKE|LOCK|REFRESH|pg_advisory\w*)\b",
    re.IGNORECASE,
)


class SafetyRefusalError(RuntimeError):
    """Refuse an unproved scope, code identity or read-only execution boundary."""


@dataclass(frozen=True)
class Options:
    """Explicit non-secret operator inputs; no wallet identifier or URL argument."""

    output: Path
    scope_hash: str
    venues: frozenset[str]
    list_scopes: bool
    worker: bool


def _options() -> Options:
    """Parse explicit scope, venue and fresh output-directory bindings."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--scope-hash", default="")
    parser.add_argument("--venues", default="")
    parser.add_argument("--list-scopes", action="store_true")
    parser.add_argument("--_worker", action="store_true", help=argparse.SUPPRESS)
    args, unsupported = parser.parse_known_args()
    if unsupported:
        raise SafetyRefusalError("Unsupported command-line options")
    venues = frozenset(value.strip() for value in args.venues.split(",") if value.strip())
    if not args.list_scopes and (not re.fullmatch("[0-9a-f]{12}", args.scope_hash) or not venues):
        parser.error("Profiling requires an explicit 12-character scope hash and venue set")
    if any(venue == "paper" or is_futures_class_exchange(venue) for venue in venues):
        parser.error("Expected venues must be owner-verified spot venues")
    return Options(args.output, args.scope_hash, venues, args.list_scopes, args._worker)


def _scope_hash(wallet: str, epoch: str) -> str:
    """Hash the complete scope binding without exposing wallet or epoch identifiers."""
    return hashlib.md5(f"{wallet}|live|USD|{epoch}".encode(), usedforsecurity=False).hexdigest()[
        :12
    ]


def _sql_prefix(statement: str) -> str:
    """Keep at most 80 normalized SQL characters while stripping quoted values."""
    redacted = re.sub(r"'(?:''|[^'])*'", "'?'", statement)
    redacted = re.sub(r"[0-9a-fA-F]{8}(?:-[0-9a-fA-F]{4}){3}-[0-9a-fA-F]{12}", "?", redacted)
    return " ".join(redacted.split())[:80]


class Recorder:
    """Write per-statement metadata without retaining parameters, rows or errors."""

    def __init__(self, output: Path) -> None:
        """Own one restricted query log and a serial phase timer."""
        self.phase = "setup"
        self.phases: dict[str, float] = {}
        self.statements = 0
        self.started: dict[int, tuple[float, str, str, str]] = {}
        self.stream: TextIO = (output / "queries.ndjson").open("x", encoding="utf-8")

    @contextmanager
    def measured(self, name: str) -> Iterator[None]:
        """Measure a serial preparation, series or planning phase."""
        prior = self.phase
        self.phase = name
        started = time.monotonic()
        try:
            yield
        finally:
            self.phases[name] = time.monotonic() - started
            self.phase = prior

    def before(self, *args: object) -> None:
        """Reject DML, DDL and side-effect locks before the driver sees a statement."""
        statement = cast(str, args[2])
        context = cast(ExecutionContext, args[4])
        if not statement.lstrip().upper().startswith(
            ("SELECT", "WITH", "SHOW")
        ) or _FORBIDDEN_SQL.search(statement):
            raise SafetyRefusalError("Only audited read statements are permitted")
        if self.statements >= 20000:
            raise SafetyRefusalError("Statement-count ceiling exceeded")
        digest = hashlib.sha256(statement.encode()).hexdigest()
        self.started[id(context)] = (time.monotonic(), self.phase, digest, _sql_prefix(statement))

    def after(self, *args: object) -> None:
        """Record a completed driver operation without touching its result rows."""
        self._finish(cast(ExecutionContext, args[4]), "complete")

    def error(self, context: ExceptionContext) -> None:
        """Record only an exception class, never its text or bound parameters."""
        if context.execution_context is not None:
            self._finish(context.execution_context, type(context.original_exception).__name__)

    def _finish(self, context: ExecutionContext, outcome: str) -> None:
        """Publish one safe timing record for its execution context."""
        pending = self.started.pop(id(context), None)
        if pending is None:
            return
        started, phase, digest, prefix = pending
        self.statements += 1
        row = dict(
            sequence=self.statements,
            phase=phase,
            sql_sha256=digest,
            sql_prefix=prefix,
            elapsed_seconds=time.monotonic() - started,
            outcome=outcome,
        )
        self.stream.write(json.dumps(row, sort_keys=True) + "\n")
        self.stream.flush()


def _source_identity() -> dict[str, str]:
    """Require the four code files from the frozen 28d34dbe production-source witness."""
    modules = dict(
        snapshotter=snapshotter_module,
        planner=planner_module,
        timeline=timeline_module,
        repository=repository_module,
    )
    actual = {
        key: hashlib.sha256(Path(getfile(module)).read_bytes()).hexdigest()
        for key, module in modules.items()
    }
    if actual != _EXPECTED or PNL_SAMPLE_CALC_VERSION != "5C.3":
        raise SafetyRefusalError("Production source identity differs from the reviewed profile")
    return actual


async def _readonly_repository(recorder: Recorder) -> SQLAlchemyRepository:
    """Install startup read-only/time limits before the first database connection."""
    raw = os.environ.get("DB_URL", "")
    if not raw:
        raise SafetyRefusalError("An existing owner-provided database binding is required")
    url = make_url(raw)
    if url.drivername != "postgresql+asyncpg":
        raise SafetyRefusalError("An owner-provided async PostgreSQL URL is required")
    repo = SQLAlchemyRepository(raw)
    unused = repo.engine
    repo.engine = create_async_engine(
        url,
        echo=False,
        hide_parameters=True,
        pool_size=1,
        max_overflow=0,
        pool_timeout=5,
        connect_args={
            "timeout": 5,
            "command_timeout": 60,
            "server_settings": {
                "timezone": "UTC",
                "default_transaction_read_only": "on",
                "statement_timeout": "60000",
                "lock_timeout": "1000",
                "application_name": "snapperf-profile",
            },
        },
    )
    repo.session_factory = async_sessionmaker(
        repo.engine, expire_on_commit=False, class_=AsyncSession
    )
    await unused.dispose()
    event.listen(repo.engine.sync_engine, "before_cursor_execute", recorder.before)
    event.listen(repo.engine.sync_engine, "after_cursor_execute", recorder.after)
    event.listen(repo.engine.sync_engine, "handle_error", recorder.error)
    return repo


async def _profile(options: Options, recorder: Recorder, report: dict[str, object]) -> None:
    """Compute one bounded real chunk, stopping before every reconcile/write call."""
    repo = await _readonly_repository(recorder)
    try:
        with recorder.measured("read_only_admission"):
            async with repo.session() as session:
                settings = (
                    await session.execute(
                        text(
                            "SELECT current_setting('default_transaction_read_only'), current_setting('transaction_read_only'), current_setting('statement_timeout'), current_setting('lock_timeout'), current_setting('application_name')"
                        )
                    )
                ).one()
            if tuple(settings) != ("on", "on", "1min", "1s", "snapperf-profile"):
                raise SafetyRefusalError(
                    "Database session controls do not match the required receipt"
                )
            report["session_controls"] = list(settings)
        with recorder.measured("scope_discovery"):
            async with repo.session() as session:
                rows = (
                    await session.execute(
                        select(
                            PortfolioPnlPoint.wallet_public_id, PortfolioPnlPoint.epoch_public_id
                        )
                        .where(
                            PortfolioPnlPoint.point_kind == "anchor",
                            PortfolioPnlPoint.mode == "live",
                            PortfolioPnlPoint.valuation_ccy == "USD",
                            PortfolioPnlPoint.known_to == KNOWN_TO_MAX,
                        )
                        .limit(33)
                    )
                ).all()
            if len(rows) > 32:
                raise SafetyRefusalError("Scope discovery ceiling exceeded")
            if options.list_scopes:
                report["scope_hashes"] = sorted(_scope_hash(row[0], row[1]) for row in rows)
                report["status"] = "listed_scopes_only"
                return
            selected = [row for row in rows if _scope_hash(row[0], row[1]) == options.scope_hash]
            if len(selected) != 1:
                raise SafetyRefusalError("Scope hash is missing or ambiguous")
            wallet, epoch = selected[0]
        as_of = datetime.now(UTC)
        snapshotter = PortfolioPnlSnapshotter(
            repo=repo, interval_seconds=60, disabled=False, clock=lambda: as_of
        )
        with recorder.measured("scope_context"):
            anchor = await repo.get_portfolio_pnl_anchor(wallet, "live", "USD", None)
            if anchor is None or anchor["epoch_public_id"] != epoch:
                raise SafetyRefusalError("Scope anchor changed during admission")
            ctx = await snapshotter._build_scope_context(
                repo, wallet, anchor, options.venues, as_of
            )
            frontier = await repo.get_latest_portfolio_pnl_sample(ctx.query)
            start = (frontier["point_time"] if frontier is not None else ctx.t0) + timedelta(
                minutes=1
            )
            end = start + timedelta(minutes=1439)
            if (
                ctx.last_minute is None
                or end > ctx.last_minute
                or end > as_of.replace(second=0, microsecond=0)
            ):
                raise SafetyRefusalError(
                    "There is no complete 1440-minute correction chunk at this frontier"
                )
            chunk = ChunkWindow(start=start, end=end)
            report.update(
                scope_hash=options.scope_hash,
                chunk_start=start.isoformat(),
                chunk_end=end.isoformat(),
                minutes=1440,
                expected_venues=sorted(options.venues),
                expected_venues_source="owner_verified_runtime_receipt",
            )
        with recorder.measured("persisted_chunk_read"):
            existing = await repo.get_active_portfolio_pnl_samples_any_version(
                ctx.scope, start, end
            )
            report["persisted_chunk_rows"] = len(existing)
            del existing
        with recorder.measured("series"):
            result = await snapshotter._series(repo, ctx, chunk, ctx.durable_baseline)
        with recorder.measured("causal_prior_peak"):
            prior_peak = await repo.get_portfolio_pnl_sample_peak(ctx.query, start)
        with recorder.measured("observation_evidence_and_planning"):
            plan = await snapshotter._plan_chunk(repo, ctx, result, chunk, prior_peak)
        report.update(
            status="computed_without_writes",
            series_points=len(result.points),
            planned_minutes=len(plan.samples),
            withheld_minutes=len(plan.withheld),
            reconciliation_called=False,
            persisted_suffix_read="bounded to selected chunk; does not reproduce full-suffix preload cost",
        )
    finally:
        await repo.engine.dispose()


def _stop_worker(process: subprocess.Popen[bytes]) -> None:
    """Stop and reap the one owned worker process group, escalating within ten seconds."""
    with suppress(ProcessLookupError):
        os.killpg(process.pid, signal.SIGTERM)
    try:
        process.wait(timeout=3)
    except subprocess.TimeoutExpired:
        os.killpg(process.pid, signal.SIGKILL)
        process.wait(timeout=7)


def _worker(options: Options) -> int:
    """Profile async and offloaded valuation work while containing every diagnostic."""
    if os.environ.get("SNAPPER_PROFILE_PARENT_PID") != str(os.getppid()):
        raise SafetyRefusalError("Worker must be launched by the bounded parent")
    report: dict[str, object] = dict(
        started_at=datetime.now(UTC).isoformat(), status="failed", financial_values_written=False
    )
    recorder = Recorder(options.output)
    profiles = [cProfile.Profile()]
    original = timeline_module._offload_series_valuation

    async def offload(valuation: Callable[[], _ValuationResult]) -> _ValuationResult:
        def measured() -> _ValuationResult:
            profile = cProfile.Profile()
            profiles.append(profile)
            return profile.runcall(valuation)

        return await original(measured)

    timeline_module._offload_series_valuation = offload
    started = time.monotonic()
    try:
        report["source_sha256"] = _source_identity()
        profiles[0].enable()
        asyncio.run(_profile(options, recorder, report))
        return_code = 0
    except BaseException as error:
        report["error_type"] = type(error).__name__
        report["failure_phase"] = recorder.phase
        return_code = 1
    finally:
        profiles[0].disable()
        timeline_module._offload_series_valuation = original
        output = io.StringIO()
        stats = pstats.Stats(profiles[0], stream=output)
        for profile in profiles[1:]:
            stats.add(profile)
        stats.strip_dirs().sort_stats("cumulative").print_stats(30)
        (options.output / "profile-top30.txt").write_text(output.getvalue())
        report.update(
            elapsed_seconds=time.monotonic() - started,
            phases_seconds=recorder.phases,
            sql_statements=recorder.statements,
            finished_at=datetime.now(UTC).isoformat(),
        )
        recorder.stream.close()
        (options.output / "status.json").write_text(json.dumps(report, indent=2) + "\n")
    return return_code


def main() -> int:
    """Launch one bounded read-only worker; never install a timer or background service."""
    os.umask(0o077)
    logger.remove()
    logging.disable(logging.CRITICAL)
    warnings.simplefilter("error")
    options = _options()
    if options.worker:
        return _worker(options)
    options.output.mkdir(mode=0o700, parents=True, exist_ok=False)
    environment = dict(os.environ, SNAPPER_PROFILE_PARENT_PID=str(os.getpid()))
    with (options.output / "worker-output.log").open("xb") as stream:
        process = subprocess.Popen(
            [sys.executable, str(Path(__file__).resolve()), *sys.argv[1:], "--_worker"],
            env=environment,
            stdout=stream,
            stderr=stream,
            start_new_session=True,
        )
        try:
            code = process.wait(timeout=580)
        except subprocess.TimeoutExpired:
            _stop_worker(process)
            code = 124
            (options.output / "parent-timeout.json").write_text(
                json.dumps(dict(status="hard_timeout", worker_reaped=True)) + "\n"
            )
        finally:
            if process.poll() is None:
                _stop_worker(process)
    print(
        json.dumps(dict(status="finished" if code == 0 else "failed_or_timed_out", exit_code=code))
    )
    return code


if __name__ == "__main__":
    sys.excepthook = lambda error_type, error, traceback: print(
        json.dumps(dict(status="failed", error_type=error_type.__name__)), file=sys.stderr
    )
    raise SystemExit(main())
