"""Rehearse the PostgreSQL trades-table adoption on an isolated PG18 instance.

The harness creates its own Unix-socket-only PostgreSQL 18.4 cluster beneath
the ignored reports directory, lowers its own CPU and I/O priority, loads a
scaled copy of the production trades schema, runs concurrent targetless
writers, and destroys the cluster after retaining machine-readable evidence.
It never accepts a database URL and refuses port 5432, so no invocation path
can select the production database.

The proof treats every acceptance condition as a falsifiable assertion. It
compares writer identity multisets with EXCEPT ALL in both directions, proves
index adoption with exact OID multisets and explicit pg_inherits pairs, captures
PostgreSQL's DEBUG1 implication message, and independently observes every
controller kill point. Publisher watchdogs reconcile expired fence leases from
catalog state and cannot resume until the authorized DDL transaction and locks
are gone.
"""

import argparse
import asyncio
import contextlib
import hashlib
import itertools
import json
import math
import os
import re
import shutil
import signal
import subprocess
import sys
import tempfile
import time
import traceback
from collections.abc import AsyncIterator
from collections.abc import Sequence
from dataclasses import dataclass
from dataclasses import field
from datetime import UTC
from datetime import datetime
from datetime import timedelta
from pathlib import Path
from typing import Final
from typing import Protocol
from typing import TextIO
from typing import cast
from uuid import UUID
from uuid import uuid4
from uuid import uuid5

import asyncpg
from asyncpg import Connection
from asyncpg import PostgresLogMessage
from asyncpg import Record

_ROOT: Final[Path] = Path(__file__).resolve().parents[1]
_REPORT_ROOT: Final[Path] = _ROOT / "reports" / "trades_partition_rehearsal"
_VENV_BIN: Final[Path] = _ROOT / ".venv" / "bin"
_REHEARSAL_SCRIPT: Final[Path] = _ROOT / "scripts" / "trades_partition_rehearsal.py"
_REHEARSAL_TEST: Final[Path] = _ROOT / "tests" / "scripts" / "test_trades_partition_rehearsal.py"
_PG_BIN: Final[Path] = Path("/usr/lib/postgresql/18/bin")
_POSTGRES: Final[Path] = _PG_BIN / "postgres"
_INITDB: Final[Path] = _PG_BIN / "initdb"
_PG_ISREADY: Final[Path] = _PG_BIN / "pg_isready"
_SLEEP: Final[Path] = Path("/usr/bin/sleep")
_EXPECTED_VERSION: Final[str] = "18.4"
_NON_UTC_TIMEZONE: Final[str] = "Europe/Warsaw"
_PORT: Final[int] = 55439
_PRODUCTION_PORT: Final[int] = 5432
_QUALIFYING_ROWS: Final[int] = 10_000_000
_MAX_WRITERS: Final[int] = 2
_MAX_WRITER_RATE: Final[float] = 80.0
_MIN_WRITER_RATE: Final[float] = 1.0
_MAX_LOAD_PER_CPU: Final[float] = 1.5
_MIN_GUARD_SECONDS: Final[float] = 0.5
_ARCHIVE_STAGE_FREE_BYTES: Final[int] = 16 * 1024**3
_ARCHIVE_EMERGENCY_FREE_BYTES: Final[int] = 8 * 1024**3
_ARCHIVE_SQL_TIMEOUT_SECONDS: Final[float] = 1_800.0
_ARCHIVE_GATE_TIMEOUT_SECONDS: Final[float] = 14_400.0
_IDENTITY_ORACLE_TIMEOUT_SECONDS: Final[float] = 300.0
_PUBLISHER_STOP_TIMEOUT_SECONDS: Final[float] = 30.0
_TASK_CANCEL_TIMEOUT_SECONDS: Final[float] = 5.0
_CHILD_REAP_TIMEOUT_SECONDS: Final[float] = 10.0
_CONNECT_TIMEOUT_SECONDS: Final[float] = 10.0
_DEFAULT_SQL_TIMEOUT_SECONDS: Final[float] = 1_800.0
_INITDB_TIMEOUT_SECONDS: Final[float] = 120.0
_RETENTION_DROP_TIMEOUT_SECONDS: Final[float] = 15.0
_PROCESS_SESSION_CLEANUP_SECONDS: Final[float] = 5.0
_KNOWN_TO: Final[datetime] = datetime(9999, 12, 31, 23, 59, 59, tzinfo=UTC)
_IDENTITY_NAMESPACE: Final[UUID] = UUID("00000000-0000-5000-8000-00000000c701")
_SESSION_NAMESPACE: Final[UUID] = UUID("00000000-0000-5000-8000-00000000c702")
_ADVISORY_NAMESPACE: Final[int] = 771_429
_EXPECTED_INDEX_PAIRS: Final[set[tuple[str, str]]] = {
    ("ix_trade_instrument_ts", "trades_p_ix_instr_ts"),
    ("ix_trades_executed_at", "trades_p_ix_exec"),
    ("ix_trades_timestamp", "trades_p_ix_ts"),
    ("uq_trade_instr_tid_exec", "trades_p_uq_instr_tid_exec"),
}
_EXPECTED_PARENT_INDEXES: Final[set[str]] = {
    "trades_p_ix_exec",
    "trades_p_ix_instr_ts",
    "trades_p_ix_ts",
    "trades_p_uq_instr_tid_exec",
}
_EXPECTED_LEGACY_INDEXES: Final[set[str]] = {
    "ix_trade_instrument_ts",
    "ix_trades_executed_at",
    "ix_trades_public_id",
    "ix_trades_timestamp",
    "trades_pkey",
    "uq_trade_instr_tid_exec",
    "uq_trade_instrument_trade_id",
}
_DEBUG_IMPLICATION: Final[str] = (
    'partition constraint for table "trades_legacy" is implied by existing constraints'
)
_TRANSACTION_TIMEOUT_LOG: Final[str] = "terminating connection due to transaction timeout"
_HORIZON_PATTERN: Final[re.Pattern[str]] = re.compile(
    r"^TIMESTAMPTZ '\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\+00:00'$"
)
_SHA256_PATTERN: Final[re.Pattern[str]] = re.compile(r"^[0-9a-f]{64}$")
_REQUIRED_VERIFICATION_NAMES: Final[tuple[str, ...]] = (
    "targeted pytest",
    "ruff",
    "black",
    "isort",
    "targeted mypy",
    "complexity",
    "no comments",
    "docstrings",
    "coverage exclusions",
    "main guard",
    "init files",
)
_CONTROLLER_SCENARIOS: Final[set[str]] = {
    "normal",
    "after_begin",
    "during_commit",
    "after_commit",
    "orphan_timeout",
    "validate",
}
_PUBLISHER_SCENARIOS: Final[set[str]] = {"writer_drop"}
_REQUIRED_CRITERIA: Final[set[int]] = set(range(1, 7))
_REQUIRED_KILLS: Final[set[str]] = {
    "during VALIDATE",
    "after DDL BEGIN before COMMIT",
    "during COMMIT (ambiguous)",
    "after COMMIT before resume",
    "orphan transaction_timeout before lease expiry",
    "writer during ACTIVE fence abort/page",
}
_ARCHIVE_COLUMNS: Final[tuple[str, ...]] = (
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
_EXPECTED_COLUMN_SHAPE: Final[tuple[tuple[str, str, bool], ...]] = (
    ("id", "bigint", True),
    ("public_id", "uuid", True),
    ("instrument_public_id", "uuid", True),
    ("trade_id", "character varying(64)", False),
    ("price", "double precision", True),
    ("size", "double precision", True),
    ("side", "character varying(4)", True),
    ("executed_at", "timestamp with time zone", False),
    ("session_id", "uuid", True),
    ("sequence_id", "integer", True),
    ("timestamp", "timestamp with time zone", True),
    ("known_to", "timestamp with time zone", True),
)
_EXPECTED_CONSTRAINTS: Final[set[tuple[str, str]]] = {
    ("ck_trades_sequence_id", "c"),
    ("trades_id_not_null", "n"),
    ("trades_instrument_public_id_not_null", "n"),
    ("trades_known_to_not_null", "n"),
    ("trades_pkey", "p"),
    ("trades_price_not_null", "n"),
    ("trades_public_id_not_null", "n"),
    ("trades_sequence_id_not_null", "n"),
    ("trades_session_id_not_null", "n"),
    ("trades_side_not_null", "n"),
    ("trades_size_not_null", "n"),
    ("trades_timestamp_not_null", "n"),
    ("uq_trade_instrument_trade_id", "u"),
}
_EXPECTED_INDEX_SHAPE: Final[set[tuple[str, bool, bool, tuple[str, ...], bool]]] = {
    ("trades_pkey", True, True, ("id",), False),
    (
        "uq_trade_instrument_trade_id",
        True,
        False,
        ("instrument_public_id", "trade_id"),
        False,
    ),
    (
        "ix_trade_instrument_ts",
        False,
        False,
        ("instrument_public_id", "timestamp"),
        False,
    ),
    ("ix_trades_timestamp", False, False, ("timestamp",), False),
    ("ix_trades_executed_at", False, False, ("executed_at",), False),
    ("ix_trades_public_id", True, False, ("public_id",), True),
    (
        "uq_trade_instr_tid_exec",
        True,
        False,
        ("instrument_public_id", "trade_id", "executed_at"),
        False,
    ),
}


class RehearsalError(RuntimeError):
    """Raised when one falsifiable rehearsal assertion is not satisfied."""


class _TransportAccess(Protocol):
    """Expose the one asyncio transport operation used by the orphan proof."""

    def get_extra_info(self, name: str) -> object:
        """Return one named transport detail."""


class _FileDescriptorAccess(Protocol):
    """Expose a file descriptor without depending on asyncio internals."""

    def fileno(self) -> int:
        """Return the live operating-system file descriptor."""


@dataclass(frozen=True)
class RunConfig:
    """Operator-configurable bounds for one rehearsal run."""

    rows: int
    days: int
    writers: int
    writer_rate: float
    lease_seconds: float
    transaction_timeout_seconds: float
    cleanup_margin_seconds: float
    guard_seconds: float
    load_limit_per_cpu: float


@dataclass(frozen=True)
class RuntimePaths:
    """Filesystem locations allocated exclusively to one rehearsal."""

    run_id: str
    run_dir: Path
    instance_dir: Path
    pgdata: Path
    socket_dir: Path
    postgres_log: Path
    initdb_log: Path
    report_json: Path
    report_markdown: Path
    events_dir: Path
    identities_dir: Path
    archive_dir: Path
    quality_log: Path


@dataclass(frozen=True)
class SourceSnapshot:
    """Immutable digests for the script and tests being rehearsed."""

    script_sha256: str
    test_sha256: str


@dataclass(frozen=True)
class InstanceRef:
    """Verified connection coordinates for the private throwaway instance."""

    pgdata: Path
    socket_dir: Path
    port: int
    user: str
    postmaster_pid: int
    source_snapshot: SourceSnapshot


@dataclass(frozen=True)
class StableIdentity:
    """Stable writer identity and routing time used by the multiset oracle."""

    public_id: UUID
    instrument_public_id: UUID
    trade_id: str
    executed_at: datetime

    def copy_record(self) -> tuple[UUID, UUID, str, datetime]:
        """Return one typed record for asyncpg COPY.

        Returns:
            The four identity fields in table-column order.
        """
        return (
            self.public_id,
            self.instrument_public_id,
            self.trade_id,
            self.executed_at,
        )

    def json_line(self) -> str:
        """Serialize one external expected-identity record.

        Returns:
            A deterministic JSON line with all stable identity fields.
        """
        return json.dumps(
            {
                "executed_at": self.executed_at.isoformat(),
                "instrument_public_id": str(self.instrument_public_id),
                "public_id": str(self.public_id),
                "trade_id": self.trade_id,
            },
            sort_keys=True,
            separators=(",", ":"),
        )


@dataclass(frozen=True)
class WriterRow:
    """One full trades row emitted by a synthetic publisher."""

    identity: StableIdentity
    price: float
    size: float
    side: str
    session_id: UUID
    sequence_id: int
    timestamp: datetime

    def values(self) -> tuple[object, ...]:
        """Return values in the writer INSERT column order.

        Returns:
            A tuple containing every non-generated trades column.
        """
        return (
            self.identity.public_id,
            self.identity.instrument_public_id,
            self.identity.trade_id,
            self.price,
            self.size,
            self.side,
            self.identity.executed_at,
            self.session_id,
            self.sequence_id,
            self.timestamp,
            _KNOWN_TO,
        )


@dataclass
class WriterStats:
    """Mutable evidence collected for one publisher."""

    sent: int = 0
    committed: int = 0
    batches: int = 0
    surfaced_unique_violations: int = 0
    hidden_conflicts: int = 0
    failures: list[str] = field(default_factory=list)
    expected: list[StableIdentity] = field(default_factory=list)
    resumed_generations: list[int] = field(default_factory=list)
    conflict_batches_exercised: int = 0
    clean_duplicates_classified: int = 0
    mismatched_twins_quarantined: int = 0
    conflict_classification_failures: int = 0
    conflict_modes: set[str] = field(default_factory=set)


@dataclass(frozen=True)
class WriterContext:
    """Runtime dependencies for one publisher and its local watchdog."""

    instance: InstanceRef
    publisher_id: str
    scenario: str
    horizon: datetime
    writer_count: int
    writer_rate: float
    ledger_path: Path
    stop_event: asyncio.Event
    stats: WriterStats


@dataclass(frozen=True)
class ControllerSpec:
    """Arguments passed to one killable controller subprocess."""

    instance: InstanceRef
    scenario: str
    generation: int
    token: UUID
    horizon: datetime
    writers: int
    transaction_timeout_seconds: float
    cleanup_margin_seconds: float
    events_dir: Path


@dataclass(frozen=True)
class FenceLease:
    """Database-clock lease authorizing one generation of cutover DDL."""

    generation: int
    token: UUID
    opened_at: datetime
    expires_at: datetime


@dataclass(frozen=True)
class OrphanTimeoutContext:
    """Dependencies for proving timeout causality after controller SIGKILL."""

    log_path: Path
    log_offset: int
    lease: FenceLease
    spec: ControllerSpec
    observation: dict[str, object]


@dataclass(frozen=True)
class KillScenarioCleanup:
    """Owned resources that must be bounded after one controller-kill scenario."""

    scenario: str
    paths: RuntimePaths
    lease: FenceLease | None
    holder_pid: int | None
    holder_start_ticks: int | None
    process: asyncio.subprocess.Process | None
    tasks: tuple[asyncio.Task[None], ...]
    stop_event: asyncio.Event
    monitor: Connection


@dataclass(frozen=True)
class CatalogState:
    """Exact externally visible topology used for recovery decisions."""

    mode: str
    trades_kind: str | None
    staged_parent_kind: str | None
    legacy_kind: str | None
    topology_epoch: str
    legacy_attached: bool
    sequence_owned: bool
    index_pairs: tuple[tuple[str, str], ...]
    legacy_bound: str | None

    def fingerprint(self) -> str:
        """Return a stable digest of every recovery-relevant catalog fact.

        Returns:
            SHA-256 over the ordered catalog state.
        """
        payload = "|".join(
            (
                self.mode,
                str(self.trades_kind),
                str(self.staged_parent_kind),
                str(self.legacy_kind),
                self.topology_epoch,
                str(self.legacy_attached),
                str(self.sequence_owned),
                repr(self.index_pairs),
                str(self.legacy_bound),
            )
        )
        return hashlib.sha256(payload.encode("utf-8")).hexdigest()


@dataclass(frozen=True)
class RetentionPreparation:
    """Fenced and barrier-cleared state immediately before plain DETACH."""

    generation: dict[str, object]
    lease: FenceLease
    acknowledgements: list[dict[str, object]]
    barrier: dict[str, object]
    pending_reconciliation: dict[str, object]
    pending_state: CatalogState
    default_attached: bool
    default_rows: int
    concurrent_sqlstate: str
    concurrent_message: str


@dataclass(frozen=True)
class RetentionBarrierContext:
    """Shared state for preparation and final barrier attempts."""

    instance: InstanceRef
    config: RunConfig
    connection: Connection
    retention_generation: int
    horizon: datetime
    stats: Sequence[WriterStats]


@dataclass(frozen=True)
class IdentityEvidence:
    """Result of the bidirectional stable-identity multiset comparison."""

    scenario: str
    expected: int
    missing: int
    extra_or_changed: int
    missing_samples: tuple[str, ...]
    extra_samples: tuple[str, ...]

    def passed(self) -> bool:
        """Return whether both multiset differences are empty.

        Returns:
            True only when no expected or unexpected identity row exists.
        """
        return self.missing == 0 and self.extra_or_changed == 0


@dataclass(frozen=True)
class ValidationMetrics:
    """Measured VALIDATE throughput and production extrapolations."""

    fixture_rows: int
    heap_bytes: int
    seconds: float
    raw_seconds: float
    rows_per_second: float
    bytes_per_second: float
    production_rows_eta_seconds: float
    production_heap_eta_seconds: float
    conservative_eta_seconds: float
    writer_commits_during: int
    writer_overlap_backend_pid: int
    controlled_pause_seconds: float
    active_recheck: bool
    cache_condition: str
    set_not_null_seconds: float
    set_not_null_scan_control_seconds: float
    set_not_null_scan_skipped: bool
    implication_negative_controls: dict[str, object]


@dataclass(frozen=True)
class ValidationStatementEvidence:
    """Measured exact VALIDATE statement and controlled writer overlap."""

    shape: Record
    seconds: float
    raw_seconds: float
    commits_during: int
    backend_pid: int
    controlled_pause_seconds: float
    active_recheck: bool


@dataclass(frozen=True)
class CriterionResult:
    """One hard acceptance result with machine-derived evidence."""

    number: int
    name: str
    passed: bool
    evidence: dict[str, object]

    def as_json(self) -> dict[str, object]:
        """Return the JSON representation of one acceptance result.

        Returns:
            A JSON-compatible dictionary.
        """
        return {
            "evidence": self.evidence,
            "name": self.name,
            "number": self.number,
            "status": "PASS" if self.passed else "FAIL",
        }


@dataclass(frozen=True)
class KillResult:
    """One controller kill point and its independently observed outcome."""

    name: str
    passed: bool
    evidence: dict[str, object]

    def as_json(self) -> dict[str, object]:
        """Return the JSON representation of one kill result.

        Returns:
            A JSON-compatible dictionary.
        """
        return {
            "evidence": self.evidence,
            "name": self.name,
            "status": "PASS" if self.passed else "FAIL",
        }


@dataclass
class RehearsalEvidence:
    """Aggregate evidence assembled before report rendering."""

    criteria: list[CriterionResult] = field(default_factory=list)
    kills: list[KillResult] = field(default_factory=list)
    identity_checks: list[IdentityEvidence] = field(default_factory=list)
    issues: list[str] = field(default_factory=list)
    fixture: dict[str, object] = field(default_factory=dict)
    lifecycle: dict[str, object] = field(default_factory=dict)
    validate: ValidationMetrics | None = None
    verification: list[dict[str, object]] = field(default_factory=list)

    def passed(self) -> bool:
        """Return whether every hard criterion and kill point passed.

        Returns:
            True only when no required result is failed or missing.
        """
        criterion_numbers = [result.number for result in self.criteria]
        kill_names = [result.name for result in self.kills]
        return (
            len(criterion_numbers) == len(_REQUIRED_CRITERIA)
            and set(criterion_numbers) == _REQUIRED_CRITERIA
            and len(set(criterion_numbers)) == len(criterion_numbers)
            and len(kill_names) == len(_REQUIRED_KILLS)
            and set(kill_names) == _REQUIRED_KILLS
            and len(set(kill_names)) == len(kill_names)
            and all(result.passed for result in self.criteria)
            and all(result.passed for result in self.kills)
        )


@dataclass(frozen=True)
class ScenarioResult:
    """Evidence produced by one fenced cutover scenario."""

    name: str
    terminal_state: CatalogState
    controller_returncode: int
    resolution: str
    resolver: str
    resume_count: int
    winner_count: int
    stable_after_resume: bool
    stale_authority_denied: bool
    identity: IdentityEvidence
    surfaced_unique_violations: int
    hidden_conflicts: int
    fault_evidence: dict[str, object]


@dataclass(frozen=True)
class AcceptanceInputs:
    """Measured inputs used to construct the six acceptance results."""

    attach: dict[str, object]
    identities: Sequence[IdentityEvidence]
    writer_stats: Sequence[WriterStats]
    scenario_results: Sequence[ScenarioResult]
    metrics: ValidationMetrics
    detach: dict[str, object]
    horizon: datetime
    configured_rows: int


@dataclass(frozen=True)
class RecoveryResult:
    """Watchdog reconciliation and resume evidence for one fence generation."""

    resolution_row: Record
    resumes: tuple[Record, ...]
    terminal: CatalogState
    winner_count: int
    stable: bool
    stale_denied: bool
    stale_authority: dict[str, object]


def _initialize_result_matrix(evidence: RehearsalEvidence) -> None:
    """Install explicit failing rows for every unexercised required proof.

    Args:
        evidence: Empty aggregate that will be updated as stages complete.

    Returns:
        None after all required criterion and kill identities are present.
    """
    criterion_names = {
        1: "instant scan-free ATTACH with index adoption",
        2: "zero rows lost by stable identity multiset",
        3: "zero unique violations under concurrent writes",
        4: "VALIDATE throughput measured and extrapolated",
        5: "plain DETACH with DEFAULT under bounded lock retries",
        6: "deliberately non-UTC DDL with explicit-offset UTC H",
    }
    evidence.criteria = [
        CriterionResult(
            number=number,
            name=criterion_names[number],
            passed=False,
            evidence={"reason": "not exercised because the rehearsal did not reach this proof"},
        )
        for number in sorted(_REQUIRED_CRITERIA)
    ]
    evidence.kills = [
        KillResult(
            name=name,
            passed=False,
            evidence={
                "reason": "not exercised because the rehearsal did not reach this kill point"
            },
        )
        for name in (
            "during VALIDATE",
            "after DDL BEGIN before COMMIT",
            "during COMMIT (ambiguous)",
            "after COMMIT before resume",
            "orphan transaction_timeout before lease expiry",
            "writer during ACTIVE fence abort/page",
        )
    ]


def _replace_kill(evidence: RehearsalEvidence, result: KillResult) -> None:
    """Replace the required placeholder for one exercised kill point.

    Args:
        evidence: Aggregate containing the complete required matrix.
        result: Measured result with one exact required name.

    Returns:
        None after exactly one placeholder is replaced.
    """
    matching = [index for index, item in enumerate(evidence.kills) if item.name == result.name]
    _assert(len(matching) == 1, f"kill result has unknown or duplicate name: {result.name}")
    evidence.kills[matching[0]] = result


def _replace_criterion(evidence: RehearsalEvidence, result: CriterionResult) -> None:
    """Replace the required placeholder for one evaluated criterion.

    Args:
        evidence: Aggregate containing six exact criterion identities.
        result: Evaluated criterion.

    Returns:
        None after exactly one placeholder is replaced.
    """
    matching = [
        index for index, item in enumerate(evidence.criteria) if item.number == result.number
    ]
    _assert(len(matching) == 1, f"criterion result has unknown number: {result.number}")
    evidence.criteria[matching[0]] = result


def _assert(condition: bool, message: str) -> None:
    """Raise a loud rehearsal failure when an assertion is false.

    Args:
        condition: Mechanically derived condition that must be true.
        message: Failure explanation retained in the report.

    Returns:
        None.

    Raises:
        RehearsalError: If the supplied condition is false.
    """
    if not condition:
        raise RehearsalError(message)


def _horizon_literal(value: datetime) -> str:
    """Format an explicit-offset PostgreSQL TIMESTAMPTZ literal.

    Args:
        value: UTC-aware horizon or partition boundary.

    Returns:
        A TIMESTAMPTZ literal carrying an explicit ``+00:00`` offset.

    Raises:
        RehearsalError: If the value is not UTC-aware or formatting drifts.
    """
    _assert(value.tzinfo is not None, "horizon value is timezone-naive")
    utc_value = value.astimezone(UTC).replace(microsecond=0)
    literal = f"TIMESTAMPTZ '{utc_value.strftime('%Y-%m-%d %H:%M:%S')}+00:00'"
    _assert(_HORIZON_PATTERN.fullmatch(literal) is not None, f"unsafe horizon literal: {literal}")
    return literal


def _aligned_horizon() -> datetime:
    """Choose a future UTC midnight for the isolated fixture.

    Returns:
        UTC midnight fourteen days after the current date.
    """
    future = datetime.now(UTC) + timedelta(days=14)
    return future.replace(hour=0, minute=0, second=0, microsecond=0)


def _allocate_paths() -> RuntimePaths:
    """Allocate one exact report tree and a short private socket directory.

    Returns:
        Paths reserved for this rehearsal invocation.
    """
    _REPORT_ROOT.mkdir(parents=True, exist_ok=True)
    run_id = f"{datetime.now(UTC).strftime('%Y%m%dT%H%M%SZ')}-{os.getpid()}"
    run_dir = _REPORT_ROOT / run_id
    instance_dir = run_dir / "instance"
    pgdata = instance_dir / "pgdata"
    socket_dir = Path(tempfile.mkdtemp(prefix="snapper-trades-rehearsal-sock-", dir="/tmp"))
    events_dir = run_dir / "events"
    identities_dir = run_dir / "identities"
    archive_dir = run_dir / "archive"
    instance_dir.mkdir(parents=True)
    events_dir.mkdir()
    identities_dir.mkdir()
    archive_dir.mkdir()
    return RuntimePaths(
        run_id=run_id,
        run_dir=run_dir,
        instance_dir=instance_dir,
        pgdata=pgdata,
        socket_dir=socket_dir,
        postgres_log=run_dir / "postgres.log",
        initdb_log=run_dir / "initdb.log",
        report_json=run_dir / "report.json",
        report_markdown=run_dir / "report.md",
        events_dir=events_dir,
        identities_dir=identities_dir,
        archive_dir=archive_dir,
        quality_log=run_dir / "quality.log",
    )


def _validate_allocated_path(path: Path, expected_parent: Path, expected_name: str) -> None:
    """Prove a cleanup target is the exact allocated child, never a broad path.

    Args:
        path: Candidate path that may be removed.
        expected_parent: Exact parent allocated by the harness.
        expected_name: Exact final path component.

    Returns:
        None.

    Raises:
        RehearsalError: If path identity is broader or different.
    """
    _assert(path.name == expected_name, f"refusing unexpected cleanup target {path}")
    _assert(
        path.parent.resolve() == expected_parent.resolve(), f"refusing broad cleanup target {path}"
    )


def _niced_parent_bound_command(argv: Sequence[str]) -> list[str]:
    """Install the parent-death signal before lowering a child process priority.

    Args:
        argv: Exact executable and arguments to run.

    Returns:
        Command prefixed with parent binding, nice 19, and idle I/O scheduling.
    """
    return [
        "setpriv",
        "--pdeathsig",
        "KILL",
        "nice",
        "-n",
        "19",
        "ionice",
        "-c",
        "3",
        *argv,
    ]


def _lower_process_priority() -> dict[str, object]:
    """Lower this process to nice 19 and idle I/O scheduling.

    Returns:
        Observed CPU nice value and ionice description.
    """
    os.setpriority(os.PRIO_PROCESS, 0, 19)
    change = subprocess.run(
        ["ionice", "-c", "3", "-p", str(os.getpid())],
        check=False,
        capture_output=True,
        text=True,
        timeout=10,
    )
    _assert(change.returncode == 0, f"failed to set idle I/O class: {change.stderr.strip()}")
    observed = subprocess.run(
        ["ionice", "-p", str(os.getpid())],
        check=False,
        capture_output=True,
        text=True,
        timeout=10,
    )
    _assert(observed.returncode == 0, "failed to read process I/O class")
    return {
        "ionice": observed.stdout.strip(),
        "nice": os.getpriority(os.PRIO_PROCESS, 0),
    }


def _host_safety_preflight(config: RunConfig, paths: RuntimePaths) -> dict[str, object]:
    """Refuse unsafe load, disk, binary, or production-port conditions.

    Args:
        config: Requested fixture and resource bounds.
        paths: Exact run paths on the rehearsal disk.

    Returns:
        Host measurements used by the safety gate.
    """
    version = subprocess.run(
        [str(_POSTGRES), "--version"],
        check=False,
        capture_output=True,
        text=True,
        timeout=10,
    )
    _assert(version.returncode == 0, f"PostgreSQL binary unavailable: {_POSTGRES}")
    _assert(
        f"PostgreSQL) {_EXPECTED_VERSION}" in version.stdout,
        f"expected PostgreSQL {_EXPECTED_VERSION}, observed {version.stdout.strip()}",
    )
    _assert(shutil.which("zstd") is not None, "zstd is required for deletion-grade archive proof")
    _assert(_SLEEP.is_file(), f"sleep binary is unavailable: {_SLEEP}")
    _assert(
        shutil.which("setpriv") is not None,
        "setpriv is required to bind postmaster lifetime to the supervisor",
    )
    _assert(_PORT != _PRODUCTION_PORT, "rehearsal port equals production port")
    cpu_count = os.cpu_count() or 1
    load_one = os.getloadavg()[0]
    load_per_cpu = load_one / cpu_count
    _assert(
        load_per_cpu <= config.load_limit_per_cpu,
        (
            f"host load safety gate closed: {load_per_cpu:.2f} per CPU exceeds "
            f"{config.load_limit_per_cpu:.2f}"
        ),
    )
    usage = shutil.disk_usage(paths.run_dir)
    required_bytes = max(40 * 1024**3, config.rows * 4_096)
    _assert(
        usage.free >= required_bytes,
        f"insufficient rehearsal disk: need {required_bytes}, have {usage.free}",
    )
    _assert(config.rows >= config.days * 100, "fixture is too small to exercise every day")
    _assert(config.rows <= _QUALIFYING_ROWS, "fixture exceeds the host-safe 10M-row ceiling")
    _assert(config.days == 56, "fixture must cover exactly the production 56-day span")
    _assert(config.writers >= 2, "at least two publishers are required for reconciler competition")
    _assert(config.writers <= _MAX_WRITERS, "publisher count exceeds the host-safe ceiling")
    _assert(
        _MIN_WRITER_RATE <= config.writer_rate <= _MAX_WRITER_RATE,
        "writer rate is outside the bounded 1-80 rows/s range",
    )
    _assert(
        0 < config.load_limit_per_cpu <= _MAX_LOAD_PER_CPU,
        "load limit may be made stricter but cannot exceed the fixed safety ceiling",
    )
    _assert(
        _MIN_GUARD_SECONDS <= config.guard_seconds <= 2.0,
        "catalog stability guard must remain between 0.5 and 2 seconds",
    )
    _assert(4.0 <= config.lease_seconds <= 10.0, "lease must remain between 4 and 10 seconds")
    _assert(
        1.0 <= config.transaction_timeout_seconds <= 5.0,
        "transaction timeout must remain between 1 and 5 seconds",
    )
    _assert(
        0.5 <= config.cleanup_margin_seconds <= 2.0,
        "cleanup margin must remain between 0.5 and 2 seconds",
    )
    _assert(
        config.transaction_timeout_seconds + config.cleanup_margin_seconds < config.lease_seconds,
        "transaction_timeout and cleanup margin do not fit inside the publisher lease",
    )
    return {
        "cpu_count": cpu_count,
        "disk_free_bytes": usage.free,
        "load_one": load_one,
        "load_per_cpu": load_per_cpu,
        "postgres_binary": str(_POSTGRES),
        "postgres_version": version.stdout.strip(),
        "required_free_bytes": required_bytes,
    }


def _run_logged(
    argv: Sequence[str],
    log_path: Path,
    timeout: float,
) -> subprocess.CompletedProcess[str]:
    """Run one subprocess and persist its complete output.

    Args:
        argv: Exact executable and arguments.
        log_path: Evidence file that receives stdout and stderr.
        timeout: Maximum child runtime in seconds.

    Returns:
        Completed process with captured text.
    """
    try:
        completed = subprocess.run(
            list(argv),
            check=False,
            capture_output=True,
            text=True,
            timeout=timeout,
        )
    except subprocess.TimeoutExpired as exc:
        stdout = (
            exc.stdout.decode(errors="replace") if isinstance(exc.stdout, bytes) else exc.stdout
        )
        stderr = (
            exc.stderr.decode(errors="replace") if isinstance(exc.stderr, bytes) else exc.stderr
        )
        log_path.write_text((stdout or "") + (stderr or ""), encoding="utf-8")
        raise RehearsalError(f"subprocess exceeded {timeout:.0f}s: {argv[0]}") from exc
    log_path.write_text(completed.stdout + completed.stderr, encoding="utf-8")
    return completed


def _start_instance(
    paths: RuntimePaths,
    source_snapshot: SourceSnapshot,
) -> tuple[InstanceRef, subprocess.Popen[bytes]]:
    """Initialize and start the private niced PostgreSQL cluster.

    Args:
        paths: Exact PGDATA, socket, and evidence paths.
        source_snapshot: Initial script and test digests for every child.

    Returns:
        Coordinates and owned process handle of the started postmaster.
    """
    user = subprocess.run(
        ["id", "-un"],
        check=True,
        capture_output=True,
        text=True,
        timeout=10,
    ).stdout.strip()
    init = _run_logged(
        _niced_parent_bound_command(
            [
                str(_INITDB),
                "-D",
                str(paths.pgdata),
                "--auth=trust",
                "--encoding=UTF8",
                "--no-locale",
                f"--username={user}",
            ]
        ),
        paths.initdb_log,
        _INITDB_TIMEOUT_SECONDS,
    )
    _assert(init.returncode == 0, f"initdb failed; see {paths.initdb_log}")
    settings = (
        "unix_socket_permissions=0700",
        "shared_buffers=64MB",
        "max_connections=16",
        "autovacuum=off",
        "jit=off",
        "huge_pages=off",
        "max_parallel_workers_per_gather=0",
        "max_parallel_maintenance_workers=0",
        "max_worker_processes=2",
        "maintenance_work_mem=64MB",
        "work_mem=4MB",
        "temp_file_limit=4GB",
        "wal_level=minimal",
        "max_wal_senders=0",
        "max_wal_size=2GB",
        "checkpoint_timeout=30min",
        "track_io_timing=on",
        "log_min_messages=info",
        "log_line_prefix=%m [%p] %a ",
    )
    server_arguments = [
        str(_POSTGRES),
        "-D",
        str(paths.pgdata),
        "-k",
        str(paths.socket_dir),
        "-h",
        "",
        "-p",
        str(_PORT),
    ]
    for setting in settings:
        server_arguments.extend(("-c", setting))
    with paths.postgres_log.open("ab") as log_handle:
        postmaster = subprocess.Popen(
            _niced_parent_bound_command(server_arguments),
            stdout=log_handle,
            stderr=subprocess.STDOUT,
            start_new_session=True,
        )
    try:
        _assert(
            os.getsid(postmaster.pid) == postmaster.pid, "postmaster did not start a new session"
        )
        _assert(
            os.getpgid(postmaster.pid) == postmaster.pid,
            "postmaster is not leader of its private process group",
        )
        (paths.instance_dir / "postmaster.session").write_text(
            f"{postmaster.pid}\n",
            encoding="utf-8",
        )
        deadline = time.monotonic() + 30.0
        while time.monotonic() < deadline:
            returncode = postmaster.poll()
            _assert(
                returncode is None,
                f"throwaway postmaster exited during startup: {returncode}",
            )
            pid_file = paths.pgdata / "postmaster.pid"
            if pid_file.exists():
                pid_text = pid_file.read_text(encoding="utf-8").splitlines()[0]
                _assert(int(pid_text) == postmaster.pid, "direct postmaster PID file mismatch")
                ready = subprocess.run(
                    [
                        str(_PG_ISREADY),
                        "-h",
                        str(paths.socket_dir),
                        "-p",
                        str(_PORT),
                        "-U",
                        user,
                        "-d",
                        "postgres",
                    ],
                    check=False,
                    capture_output=True,
                    text=True,
                    timeout=10,
                )
                if ready.returncode == 0:
                    break
            time.sleep(0.05)
        else:
            raise RehearsalError("throwaway postmaster readiness timeout")
        postmaster_pid = postmaster.pid
        _assert(postmaster_pid > 1, "invalid throwaway postmaster PID")
        return (
            InstanceRef(
                pgdata=paths.pgdata.resolve(),
                socket_dir=paths.socket_dir.resolve(),
                port=_PORT,
                user=user,
                postmaster_pid=postmaster_pid,
                source_snapshot=source_snapshot,
            ),
            postmaster,
        )
    except BaseException as exc:
        _signal_process_session(postmaster.pid, signal.SIGKILL)
        try:
            postmaster.wait(timeout=5)
        except subprocess.TimeoutExpired:
            _signal_process_session(postmaster.pid, signal.SIGKILL)
            with contextlib.suppress(subprocess.TimeoutExpired):
                postmaster.wait(timeout=5)
        remaining = _kill_owned_process_session(postmaster.pid)
        if remaining:
            raise RehearsalError(
                f"postmaster startup cleanup left session processes: {remaining}"
            ) from exc
        raise


def _instance_priority(instance: InstanceRef) -> dict[str, object]:
    """Read back the postmaster CPU and I/O scheduling classes.

    Args:
        instance: Started private instance whose PID was read from PGDATA.

    Returns:
        Observed nice value and I/O scheduling description.
    """
    nice_value = int(
        subprocess.run(
            ["ps", "-o", "ni=", "-p", str(instance.postmaster_pid)],
            check=True,
            capture_output=True,
            text=True,
            timeout=10,
        ).stdout.strip()
    )
    io_class = subprocess.run(
        ["ionice", "-p", str(instance.postmaster_pid)],
        check=True,
        capture_output=True,
        text=True,
        timeout=10,
    ).stdout.strip()
    _assert(nice_value == 19, f"throwaway PostgreSQL nice value is {nice_value}, not 19")
    _assert("idle" in io_class.lower(), f"throwaway PostgreSQL is not idle-I/O class: {io_class}")
    return {"ionice": io_class, "nice": nice_value}


async def _instance_limits(instance: InstanceRef) -> dict[str, str]:
    """Read back every resource setting that bounds the throwaway server.

    Args:
        instance: Started private instance.

    Returns:
        Observed PostgreSQL resource settings after exact-value assertions.
    """
    connection = await _connect(instance, "instance-limits")
    try:
        names = (
            "autovacuum",
            "maintenance_work_mem",
            "max_connections",
            "max_parallel_maintenance_workers",
            "max_parallel_workers_per_gather",
            "shared_buffers",
            "temp_file_limit",
            "work_mem",
        )
        settings = {name: cast(str, await connection.fetchval(f"SHOW {name}")) for name in names}
        expected = {
            "autovacuum": "off",
            "maintenance_work_mem": "64MB",
            "max_connections": "16",
            "max_parallel_maintenance_workers": "0",
            "max_parallel_workers_per_gather": "0",
            "shared_buffers": "64MB",
            "temp_file_limit": "4GB",
            "work_mem": "4MB",
        }
        _assert(settings == expected, f"throwaway PostgreSQL resource settings drift: {settings}")
        return settings
    finally:
        await _close_connection_bounded(connection)


def _processes_in_session(session_id: int) -> tuple[int, ...]:
    """Find every live process in one dedicated postmaster session.

    Args:
        session_id: Session created by the owned postmaster process.

    Returns:
        Sorted process IDs that still belong to the exact session.
    """
    matches: list[int] = []
    if session_id <= 1:
        return ()
    for entry in Path("/proc").iterdir():
        if not entry.name.isdigit():
            continue
        pid = int(entry.name)
        try:
            observed_session = os.getsid(pid)
        except OSError:
            continue
        if observed_session == session_id:
            matches.append(pid)
    return tuple(sorted(matches))


def _signal_process_session(session_id: int, signal_number: signal.Signals) -> None:
    """Signal every process group still owned by one private session.

    Args:
        session_id: Exact dedicated postmaster session identifier.
        signal_number: Signal delivered to each group in the session.

    Returns:
        None.
    """
    process_groups: set[int] = set()
    for pid in _processes_in_session(session_id):
        try:
            if os.getsid(pid) == session_id:
                process_groups.add(os.getpgid(pid))
        except OSError:
            continue
    for process_group in sorted(process_groups):
        _assert(process_group > 1, "refusing to signal a broad process group")
        with contextlib.suppress(ProcessLookupError):
            os.killpg(process_group, signal_number)


def _kill_owned_process_session(session_id: int) -> tuple[int, ...]:
    """Kill and recheck every descendant in the exact postmaster session.

    Args:
        session_id: Dedicated session created for the throwaway postmaster.

    Returns:
        Process IDs that remain after the bounded SIGKILL cleanup.
    """
    live_processes = _processes_in_session(session_id)
    deadline = time.monotonic() + _PROCESS_SESSION_CLEANUP_SECONDS
    while live_processes and time.monotonic() < deadline:
        _signal_process_session(session_id, signal.SIGKILL)
        time.sleep(0.05)
        live_processes = _processes_in_session(session_id)
    return live_processes


def _owned_session_identity(
    paths: RuntimePaths,
    instance: InstanceRef | None,
    postmaster_process: subprocess.Popen[bytes] | None,
) -> tuple[int | None, list[str]]:
    """Resolve the exact owned session from independent lifecycle identities.

    Args:
        paths: Allocated instance directory containing the session record.
        instance: Verified runtime identity when startup completed.
        postmaster_process: Direct child handle retained by the supervisor.

    Returns:
        Trusted session identifier and any identity inconsistencies.
    """
    errors: list[str] = []
    session_path = paths.instance_dir / "postmaster.session"
    recorded: int | None = None
    if session_path.exists():
        try:
            recorded = int(session_path.read_text(encoding="utf-8").strip())
        except (OSError, ValueError) as exc:
            errors.append(f"could not verify postmaster session record: {exc}")
    elif instance is not None or postmaster_process is not None:
        errors.append("postmaster session record is missing")
    process_session = postmaster_process.pid if postmaster_process is not None else None
    instance_session = instance.postmaster_pid if instance is not None else None
    trusted = process_session or instance_session or recorded
    if trusted is not None and trusted <= 1:
        errors.append(f"invalid postmaster session identifier: {trusted}")
        trusted = None
    candidates = {
        value for value in (recorded, process_session, instance_session) if value is not None
    }
    if len(candidates) > 1:
        errors.append(f"postmaster session identities disagree: {sorted(candidates)}")
    return trusted, errors


def _pid_file_error(paths: RuntimePaths, instance: InstanceRef | None) -> str | None:
    """Return a PID-file identity error without using its PID for signaling.

    Args:
        paths: Exact allocated PGDATA path.
        instance: Started instance identity, if startup completed.

    Returns:
        Error text when the PID file is malformed or mismatched, otherwise None.
    """
    pid_file = paths.pgdata / "postmaster.pid"
    if not pid_file.exists():
        return None
    try:
        pid_text = pid_file.read_text(encoding="utf-8").splitlines()[0]
        pid = int(pid_text)
    except (IndexError, OSError, ValueError) as exc:
        return f"could not verify postmaster PID file: {exc}"
    expected_pid = instance.postmaster_pid if instance is not None else None
    if expected_pid is not None and pid != expected_pid:
        return f"postmaster PID changed before cleanup: {pid} != {expected_pid}"
    return None


def _stop_owned_postmaster(
    instance: InstanceRef | None,
    postmaster_process: subprocess.Popen[bytes] | None,
    session_id: int | None,
) -> tuple[int | None, str | None, list[str]]:
    """Stop and reap only the supervisor's unreleased direct child process.

    Args:
        instance: Started instance identity, if startup completed.
        postmaster_process: Exact process handle returned by direct startup.
        session_id: Dedicated process session created with the postmaster.

    Returns:
        Return code, final signal label, and cleanup errors.
    """
    if postmaster_process is None:
        return None, None, []
    errors: list[str] = []
    stop_signal: str | None = None
    if instance is not None and postmaster_process.pid != instance.postmaster_pid:
        errors.append("owned postmaster process handle does not match the instance PID")
    if postmaster_process.poll() is None:
        postmaster_process.send_signal(signal.SIGQUIT)
        stop_signal = "SIGQUIT"
        try:
            stop_returncode = postmaster_process.wait(timeout=30)
        except subprocess.TimeoutExpired:
            if session_id is None:
                postmaster_process.kill()
            else:
                _signal_process_session(session_id, signal.SIGKILL)
            stop_signal = "SIGKILL"
            try:
                stop_returncode = postmaster_process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                errors.append("owned postmaster process did not exit after SIGKILL")
                stop_returncode = None
    else:
        stop_returncode = postmaster_process.returncode
    if stop_returncode is None:
        try:
            stop_returncode = postmaster_process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            errors.append("owned postmaster process could not be reaped")
    return stop_returncode, stop_signal, errors


def _stop_instance(
    paths: RuntimePaths,
    instance: InstanceRef | None,
    postmaster_process: subprocess.Popen[bytes] | None,
) -> dict[str, object]:
    """Stop the exact throwaway postmaster and remove only allocated data.

    Args:
        paths: Exact allocated run paths.
        instance: Started instance coordinates, if startup completed.
        postmaster_process: Owned direct postmaster process handle, if started.

    Returns:
        Lifecycle evidence for shutdown and deletion.
    """
    cleanup_errors: list[str] = []
    pid_error = _pid_file_error(paths, instance)
    if pid_error is not None:
        cleanup_errors.append(pid_error)
    session_id, session_errors = _owned_session_identity(paths, instance, postmaster_process)
    cleanup_errors.extend(session_errors)
    stop_returncode, stop_signal, stop_errors = _stop_owned_postmaster(
        instance,
        postmaster_process,
        session_id,
    )
    cleanup_errors.extend(stop_errors)
    live_processes = _kill_owned_process_session(session_id) if session_id is not None else ()
    session_proven = session_id is not None or (
        instance is None
        and postmaster_process is None
        and not (paths.instance_dir / "postmaster.session").exists()
    )
    if live_processes:
        cleanup_errors.append(
            f"refusing cleanup while postmaster session processes remain: {live_processes}"
        )
    if not session_proven:
        cleanup_errors.append("refusing cleanup without a verified postmaster session identity")
    _validate_allocated_path(paths.instance_dir, paths.run_dir, "instance")
    _validate_allocated_path(paths.socket_dir, Path("/tmp"), paths.socket_dir.name)
    _assert(
        paths.socket_dir.name.startswith("snapper-trades-rehearsal-sock-"),
        "socket cleanup target lacks rehearsal prefix",
    )
    removable = session_proven and not live_processes
    if paths.instance_dir.exists() and removable:
        shutil.rmtree(paths.instance_dir)
    if paths.socket_dir.exists() and removable:
        shutil.rmtree(paths.socket_dir)
    return {
        "cleanup_errors": cleanup_errors,
        "cleanup_ok": not cleanup_errors,
        "instance_directory_removed": not paths.instance_dir.exists(),
        "live_postmasters_after_cleanup": list(live_processes),
        "postmaster_session_id": session_id,
        "socket_directory_removed": not paths.socket_dir.exists(),
        "stop_returncode": stop_returncode,
        "stop_signal": stop_signal,
    }


def _validate_instance_ref(instance: InstanceRef) -> None:
    """Reject any connection coordinates outside the allocated private shape.

    Args:
        instance: Candidate coordinates passed to a connection helper.

    Returns:
        None.
    """
    _assert(instance.port != _PRODUCTION_PORT, "refusing production PostgreSQL port")
    _assert(
        instance.socket_dir.parent == Path("/tmp")
        and instance.socket_dir.name.startswith("snapper-trades-rehearsal-sock-"),
        f"refusing non-rehearsal socket directory {instance.socket_dir}",
    )
    _assert(
        _REPORT_ROOT.resolve() in instance.pgdata.parents,
        f"refusing PGDATA outside rehearsal reports: {instance.pgdata}",
    )
    pid_path = instance.pgdata / "postmaster.pid"
    _assert(pid_path.exists(), f"throwaway postmaster PID file missing: {pid_path}")
    pid_text = pid_path.read_text(encoding="utf-8").splitlines()[0]
    _assert(int(pid_text) == instance.postmaster_pid, "throwaway postmaster identity mismatch")
    try:
        session_id = os.getsid(instance.postmaster_pid)
        process_group = os.getpgid(instance.postmaster_pid)
    except OSError as exc:
        raise RehearsalError("throwaway postmaster process identity disappeared") from exc
    _assert(
        session_id == instance.postmaster_pid and process_group == instance.postmaster_pid,
        "throwaway postmaster left its dedicated process session",
    )


async def _connect(
    instance: InstanceRef,
    application_name: str,
    command_timeout: float | None = _DEFAULT_SQL_TIMEOUT_SECONDS,
) -> Connection:
    """Connect only to the verified private Unix socket and assert identity.

    Args:
        instance: Verified throwaway connection coordinates.
        application_name: Unique label used by independent fault observers.
        command_timeout: Optional client-side bound for every SQL operation.

    Returns:
        An asyncpg connection whose default session timezone is non-UTC.
    """
    _validate_instance_ref(instance)
    connection = await asyncpg.connect(
        user=instance.user,
        database="postgres",
        host=str(instance.socket_dir),
        port=instance.port,
        timeout=_CONNECT_TIMEOUT_SECONDS,
        command_timeout=command_timeout,
        server_settings={
            "application_name": application_name,
            "timezone": _NON_UTC_TIMEZONE,
        },
    )
    row = await connection.fetchrow("""
        SELECT
            current_setting('data_directory') AS data_directory,
            current_setting('port')::integer AS port,
            current_setting('server_version') AS server_version,
            current_setting('timezone') AS timezone,
            inet_server_addr() IS NULL AS unix_socket
        """)
    _assert(row is not None, "private instance identity query returned no row")
    _assert(
        Path(cast(str, row["data_directory"])).resolve() == instance.pgdata,
        "connected PostgreSQL data_directory is not the allocated throwaway PGDATA",
    )
    _assert(cast(int, row["port"]) == instance.port, "connected PostgreSQL port mismatch")
    _assert(
        cast(str, row["server_version"]).startswith(_EXPECTED_VERSION),
        "connected PostgreSQL version mismatch",
    )
    _assert(cast(str, row["timezone"]) == _NON_UTC_TIMEZONE, "session did not start non-UTC")
    _assert(cast(bool, row["unix_socket"]), "connection is not using a Unix socket")
    return connection


async def _close_connection_bounded(connection: Connection) -> None:
    """Close one private database connection or terminate it after a fixed bound.

    Args:
        connection: Asyncpg connection owned by the current task.

    Returns:
        None after the connection is closed or forcibly terminated.
    """
    if connection.is_closed():
        return
    try:
        await connection.close(timeout=_TASK_CANCEL_TIMEOUT_SECONDS)
    except TimeoutError:
        connection.terminate()


async def _execute_utc_ddl(connection: Connection, statement: str) -> None:
    """Execute one separately committed DDL statement under a UTC pin.

    Args:
        connection: Verified throwaway database connection.
        statement: DDL statement to execute.

    Returns:
        None.
    """
    initial_timezone = await connection.fetchval("SHOW timezone")
    _assert(initial_timezone == _NON_UTC_TIMEZONE, "DDL session was not deliberately non-UTC")
    async with connection.transaction():
        await connection.execute("SET LOCAL timezone = 'UTC'")
        pinned_timezone = await connection.fetchval("SHOW timezone")
        _assert(pinned_timezone == "UTC", "DDL transaction timezone pin failed")
        await connection.execute(statement)


def _base_table_sql() -> str:
    """Return the production-fidelity trades table DDL.

    Returns:
        Sequence and table DDL with the migration-level sequence CHECK.
    """
    return """
        CREATE SEQUENCE trades_id_seq AS bigint;
        CREATE TABLE trades (
            id bigint NOT NULL DEFAULT nextval('trades_id_seq'::regclass),
            public_id uuid NOT NULL,
            instrument_public_id uuid NOT NULL,
            trade_id character varying(64),
            price double precision NOT NULL,
            size double precision NOT NULL,
            side character varying(4) NOT NULL,
            executed_at timestamp with time zone,
            session_id uuid NOT NULL,
            sequence_id integer NOT NULL,
            "timestamp" timestamp with time zone NOT NULL,
            known_to timestamp with time zone NOT NULL,
            CONSTRAINT trades_pkey PRIMARY KEY (id),
            CONSTRAINT uq_trade_instrument_trade_id
                UNIQUE (instrument_public_id, trade_id),
            CONSTRAINT ck_trades_sequence_id CHECK (sequence_id > 0)
        );
        ALTER SEQUENCE trades_id_seq OWNED BY trades.id;
    """


def _base_index_sql() -> tuple[str, ...]:
    """Return every legacy index outside the initial PK and U2 constraints.

    Returns:
        Exact production indexes plus the standalone three-column arbiter.
    """
    return (
        """
        CREATE INDEX ix_trade_instrument_ts
        ON trades (instrument_public_id, "timestamp")
        """,
        'CREATE INDEX ix_trades_timestamp ON trades ("timestamp")',
        "CREATE INDEX ix_trades_executed_at ON trades (executed_at)",
        """
        CREATE UNIQUE INDEX ix_trades_public_id
        ON trades (public_id)
        WHERE known_to = TIMESTAMPTZ '9999-12-31 23:59:59+00:00'
        """,
        """
        CREATE UNIQUE INDEX uq_trade_instr_tid_exec
        ON trades (instrument_public_id, trade_id, executed_at)
        """,
    )


def _control_schema_sql() -> str:
    """Return durable harness-only lease, event, and commit-probe tables.

    Returns:
        DDL for the isolated control schema.
    """
    return """
        CREATE SCHEMA rehearsal;
        CREATE TABLE rehearsal.topology_epoch (
            singleton boolean PRIMARY KEY DEFAULT true CHECK (singleton),
            topology text NOT NULL CHECK (
                topology IN ('ORDINARY', 'PARTITIONED', 'U3_PENDING', 'U3')
            )
        );
        INSERT INTO rehearsal.topology_epoch(singleton, topology)
        VALUES (true, 'ORDINARY');
        CREATE TABLE rehearsal.fence_generations (
            generation bigint PRIMARY KEY,
            token uuid NOT NULL,
            opened_at timestamp with time zone NOT NULL,
            lease_expires_at timestamp with time zone NOT NULL,
            cleanup_margin_ms integer NOT NULL,
            transaction_timeout_ms integer NOT NULL,
            ddl_application text NOT NULL,
            ddl_pid integer,
            resolution text CHECK (
                resolution IS NULL OR
                resolution IN ('PRE_ABORTED', 'POST_COMMITTED')
            ),
            resolver text,
            resolved_fingerprint text,
            resolved_at timestamp with time zone
        );
        CREATE TABLE rehearsal.publisher_acks (
            generation bigint NOT NULL REFERENCES rehearsal.fence_generations(generation),
            publisher_id text NOT NULL,
            retention_generation bigint NOT NULL,
            acknowledged_at timestamp with time zone NOT NULL,
            session_closed boolean NOT NULL,
            inflight integer NOT NULL,
            PRIMARY KEY (generation, publisher_id)
        );
        CREATE TABLE rehearsal.publisher_events (
            id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
            generation bigint,
            publisher_id text NOT NULL,
            event text NOT NULL,
            mode text,
            fingerprint text,
            occurred_at timestamp with time zone NOT NULL DEFAULT clock_timestamp()
        );
        CREATE TABLE rehearsal.expected_identity (
            public_id uuid NOT NULL,
            instrument_public_id uuid NOT NULL,
            trade_id character varying(64) NOT NULL,
            executed_at timestamp with time zone NOT NULL
        );
        CREATE UNLOGGED TABLE rehearsal.fixture_identity (
            public_id uuid NOT NULL,
            instrument_public_id uuid NOT NULL,
            trade_id character varying(64) NOT NULL,
            executed_at timestamp with time zone NOT NULL
        );
        CREATE UNIQUE INDEX fixture_identity_public_id
        ON rehearsal.fixture_identity (public_id);
        CREATE TABLE rehearsal.conflict_quarantine (
            id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
            public_id uuid NOT NULL,
            instrument_public_id uuid NOT NULL,
            trade_id character varying(64) NOT NULL,
            executed_at timestamp with time zone NOT NULL,
            reason text NOT NULL,
            quarantined_at timestamp with time zone NOT NULL DEFAULT clock_timestamp(),
            resolved_at timestamp with time zone
        );
        CREATE TABLE rehearsal.commit_marker (
            singleton boolean PRIMARY KEY DEFAULT true CHECK (singleton),
            generation bigint NOT NULL
        );
        INSERT INTO rehearsal.commit_marker(singleton, generation)
        VALUES (true, 0);
        CREATE FUNCTION rehearsal.delay_commit() RETURNS trigger
        LANGUAGE plpgsql AS $$
        BEGIN
            PERFORM pg_sleep(1.5);
            RETURN NEW;
        END
        $$;
        CREATE CONSTRAINT TRIGGER rehearsal_commit_delay
        AFTER UPDATE ON rehearsal.commit_marker
        DEFERRABLE INITIALLY DEFERRED
        FOR EACH ROW EXECUTE FUNCTION rehearsal.delay_commit();
        CREATE TABLE rehearsal.publisher_runtime (
            session_id uuid PRIMARY KEY,
            publisher_id text NOT NULL,
            generation bigint NOT NULL REFERENCES rehearsal.fence_generations(generation),
            state text NOT NULL CHECK (state IN ('ACTIVE', 'INCIDENT', 'RECOVERED')),
            held_obligations integer NOT NULL CHECK (held_obligations >= 0),
            started_at timestamp with time zone NOT NULL DEFAULT clock_timestamp(),
            recovered_at timestamp with time zone
        );
        CREATE TABLE rehearsal.fence_pages (
            generation bigint PRIMARY KEY REFERENCES rehearsal.fence_generations(generation),
            publisher_id text NOT NULL,
            reason text NOT NULL,
            paged_at timestamp with time zone NOT NULL DEFAULT clock_timestamp()
        );
        CREATE TABLE rehearsal.retention_generation (
            singleton boolean PRIMARY KEY DEFAULT true CHECK (singleton),
            generation bigint NOT NULL,
            latched_at timestamp with time zone
        );
        INSERT INTO rehearsal.retention_generation(singleton, generation)
        VALUES (true, 0);
        CREATE TABLE rehearsal.retention_blocked_batches (
            batch_id uuid PRIMARY KEY,
            obligation_generation bigint NOT NULL,
            state text NOT NULL CHECK (state IN ('BLOCKED', 'DRAINED')),
            public_id uuid NOT NULL,
            instrument_public_id uuid NOT NULL,
            trade_id character varying(64) NOT NULL,
            price double precision NOT NULL,
            size double precision NOT NULL,
            side character varying(4) NOT NULL,
            executed_at timestamp with time zone NOT NULL,
            session_id uuid NOT NULL,
            sequence_id integer NOT NULL,
            "timestamp" timestamp with time zone NOT NULL,
            known_to timestamp with time zone NOT NULL,
            drained_at timestamp with time zone
        );
        CREATE TABLE rehearsal.retention_monitor_worklog (
            work_id uuid PRIMARY KEY,
            obligation_generation bigint NOT NULL,
            public_id uuid NOT NULL,
            state text NOT NULL CHECK (state IN ('PENDING', 'DRAINED')),
            drained_at timestamp with time zone
        );
        CREATE TABLE rehearsal.retention_monitor_lag (
            singleton boolean PRIMARY KEY DEFAULT true CHECK (singleton),
            earliest_unsafe timestamp with time zone NOT NULL,
            resolved_at timestamp with time zone
        );
        CREATE TABLE rehearsal.retention_monitor_scans (
            monitor text PRIMARY KEY CHECK (monitor IN ('M1', 'M2')),
            fixed_safe_cutoff timestamp with time zone NOT NULL,
            scanned_through timestamp with time zone NOT NULL,
            rows_seen bigint NOT NULL,
            backscan_rows bigint NOT NULL,
            backscan_complete boolean NOT NULL,
            completed_at timestamp with time zone NOT NULL DEFAULT clock_timestamp()
        );
        CREATE TABLE rehearsal.retention_resolution_winners (
            generation bigint PRIMARY KEY REFERENCES rehearsal.fence_generations(generation),
            resolver text NOT NULL,
            resolved_at timestamp with time zone NOT NULL DEFAULT clock_timestamp()
        );
        CREATE TABLE rehearsal.staged_replay (
            work_id uuid PRIMARY KEY,
            obligation_generation bigint NOT NULL,
            state text NOT NULL CHECK (state IN ('PENDING', 'APPLIED')),
            public_id uuid NOT NULL,
            instrument_public_id uuid NOT NULL,
            trade_id character varying(64),
            price double precision NOT NULL,
            size double precision NOT NULL,
            side character varying(4) NOT NULL,
            executed_at timestamp with time zone NOT NULL,
            session_id uuid NOT NULL,
            sequence_id integer NOT NULL,
            "timestamp" timestamp with time zone NOT NULL,
            known_to timestamp with time zone NOT NULL,
            staged_at timestamp with time zone NOT NULL DEFAULT clock_timestamp()
        );
        CREATE TABLE rehearsal.obligation_worklog (
            work_id uuid PRIMARY KEY,
            obligation_generation bigint NOT NULL,
            public_id uuid NOT NULL,
            destination text NOT NULL CHECK (destination IN ('LIVE', 'STAGED')),
            committed_at timestamp with time zone NOT NULL DEFAULT clock_timestamp(),
            settled_at timestamp with time zone
        );
        CREATE TABLE rehearsal.replay_worklog (
            work_id uuid PRIMARY KEY REFERENCES rehearsal.staged_replay(work_id),
            obligation_generation bigint NOT NULL,
            public_id uuid NOT NULL,
            applied_at timestamp with time zone NOT NULL DEFAULT clock_timestamp()
        );
    """


async def _create_fixture(
    instance: InstanceRef,
    config: RunConfig,
    horizon: datetime,
) -> dict[str, object]:
    """Create the real trades shape and load the scaled deterministic fixture.

    Args:
        instance: Verified private PostgreSQL instance.
        config: Fixture row and day counts.
        horizon: Exclusive upper boundary for every base row.

    Returns:
        Measured fixture shape and load duration.
    """
    connection = await _connect(instance, "rehearsal-fixture")
    started = time.perf_counter()
    try:
        await connection.execute(_base_table_sql())
        await connection.execute(_control_schema_sql())
        fixture_initial_timezone = cast(str, await connection.fetchval("SHOW timezone"))
        _assert(
            fixture_initial_timezone == _NON_UTC_TIMEZONE,
            "fixture horizon session did not begin deliberately non-UTC",
        )
        await connection.execute("SET timezone = 'UTC'")
        _assert(
            await connection.fetchval("SHOW timezone") == "UTC",
            "fixture horizon session failed to pin UTC",
        )
        horizon_sql = _horizon_literal(horizon)
        await connection.execute(f"""
            INSERT INTO trades (
                public_id,
                instrument_public_id,
                trade_id,
                price,
                size,
                side,
                executed_at,
                session_id,
                sequence_id,
                "timestamp",
                known_to
            )
            SELECT
                lpad(to_hex(g), 32, '0')::uuid,
                lpad(to_hex(1000000000000 + (g % 16)), 32, '0')::uuid,
                'fixture-' || g::text,
                10000.0 + ((g % 1000)::double precision / 10.0),
                0.001 + ((g % 100)::double precision / 100000.0),
                CASE WHEN g % 2 = 0 THEN 'buy' ELSE 'sell' END,
                {horizon_sql}
                    - INTERVAL '{config.days} days'
                    + (((g - 1)::double precision / {config.rows}::double precision)
                        * INTERVAL '{config.days} days'),
                '00000000-0000-7000-8000-00000000c703'::uuid,
                ((g - 1) % 2147483646 + 1)::integer,
                {horizon_sql}
                    - INTERVAL '{config.days} days'
                    + (((g - 1)::double precision / {config.rows}::double precision)
                        * INTERVAL '{config.days} days')
                    + INTERVAL '1 millisecond',
                TIMESTAMPTZ '9999-12-31 23:59:59+00:00'
            FROM generate_series(1, {config.rows}) AS generated(g)
            """)
        await connection.execute(f"""
            INSERT INTO rehearsal.fixture_identity (
                public_id,
                instrument_public_id,
                trade_id,
                executed_at
            )
            SELECT
                lpad(to_hex(g), 32, '0')::uuid,
                lpad(to_hex(1000000000000 + (g % 16)), 32, '0')::uuid,
                'fixture-' || g::text,
                {_horizon_literal(horizon)}
                    - INTERVAL '{config.days} days'
                    + (((g - 1)::double precision / {config.rows}::double precision)
                        * INTERVAL '{config.days} days')
            FROM generate_series(1, {config.rows}) AS generated(g)
            """)
        for statement in _base_index_sql():
            await connection.execute(statement)
        await connection.execute("ANALYZE trades")
        elapsed = time.perf_counter() - started
        row = await connection.fetchrow("""
            SELECT
                count(*)::bigint AS rows,
                count(DISTINCT (executed_at AT TIME ZONE 'UTC')::date)::integer AS days,
                min(executed_at) AS minimum,
                max(executed_at) AS maximum,
                pg_relation_size('trades'::regclass)::bigint AS heap_bytes
            FROM trades
            """)
        _assert(row is not None, "fixture shape query returned no row")
        index_names = {cast(str, record["relname"]) for record in await connection.fetch("""
                SELECT c.relname
                FROM pg_index i
                JOIN pg_class c ON c.oid = i.indexrelid
                WHERE i.indrelid = 'trades'::regclass
                """)}
        columns = tuple(
            (
                cast(str, record["attname"]),
                cast(str, record["data_type"]),
                cast(bool, record["attnotnull"]),
            )
            for record in await connection.fetch("""
                SELECT
                    attname,
                    format_type(atttypid, atttypmod) AS data_type,
                    attnotnull
                FROM pg_attribute
                WHERE attrelid = 'trades'::regclass
                  AND attnum > 0
                  AND NOT attisdropped
                ORDER BY attnum
                """)
        )
        constraints = {
            (cast(str, record["conname"]), cast(str, record["contype"]))
            for record in await connection.fetch("""
                SELECT conname, contype::text
                FROM pg_constraint
                WHERE conrelid = 'trades'::regclass
                """)
        }
        async with connection.transaction():
            await connection.execute("SET LOCAL timezone = 'UTC'")
            index_records = await connection.fetch("""
                SELECT
                    c.relname,
                    i.indisunique,
                    i.indisprimary,
                    ARRAY(
                        SELECT a.attname
                        FROM unnest(i.indkey) WITH ORDINALITY AS idxkey(attnum, position)
                        JOIN pg_attribute a
                          ON a.attrelid = i.indrelid
                         AND a.attnum = idxkey.attnum
                        WHERE idxkey.position <= i.indnkeyatts
                        ORDER BY idxkey.position
                    ) AS columns,
                    i.indpred IS NOT NULL AS has_predicate,
                    pg_get_expr(i.indpred, i.indrelid) AS predicate,
                    pg_get_indexdef(i.indexrelid) AS definition
                FROM pg_index i
                JOIN pg_class c ON c.oid = i.indexrelid
                WHERE i.indrelid = 'trades'::regclass
                """)
            constraint_records = await connection.fetch("""
                SELECT conname, pg_get_constraintdef(oid, true) AS definition
                FROM pg_constraint
                WHERE conrelid = 'trades'::regclass
                """)
        index_definitions = {
            cast(str, record["relname"]): cast(str, record["definition"])
            for record in index_records
        }
        index_shape = {
            (
                cast(str, record["relname"]),
                cast(bool, record["indisunique"]),
                cast(bool, record["indisprimary"]),
                tuple(cast(list[str], record["columns"])),
                cast(bool, record["has_predicate"]),
            )
            for record in index_records
        }
        predicates = {
            cast(str, record["relname"]): cast(str | None, record["predicate"])
            for record in index_records
        }
        constraint_definitions = {
            cast(str, record["conname"]): cast(str, record["definition"])
            for record in constraint_records
        }
        _assert(index_names == _EXPECTED_LEGACY_INDEXES, f"legacy index manifest: {index_names}")
        _assert(columns == _EXPECTED_COLUMN_SHAPE, f"legacy column shape mismatch: {columns}")
        _assert(index_shape == _EXPECTED_INDEX_SHAPE, f"legacy index shape mismatch: {index_shape}")
        _assert(
            constraints == _EXPECTED_CONSTRAINTS,
            f"legacy constraint manifest mismatch: {constraints}",
        )
        public_predicate = predicates["ix_trades_public_id"] or ""
        _assert(
            "known_to" in public_predicate and "9999-12-31 23:59:59+00" in public_predicate,
            f"legacy active-public-id predicate mismatch: {public_predicate}",
        )
        _assert(
            "sequence_id > 0" in constraint_definitions["ck_trades_sequence_id"],
            "legacy sequence CHECK expression mismatch",
        )
        _assert(cast(int, row["rows"]) == config.rows, "fixture row cardinality mismatch")
        oracle_rows = cast(
            int,
            await connection.fetchval("SELECT count(*)::bigint FROM rehearsal.fixture_identity"),
        )
        _assert(oracle_rows == config.rows, "fixture identity oracle cardinality mismatch")
        _assert(cast(int, row["days"]) == config.days, "fixture day cardinality mismatch")
        _assert(cast(datetime, row["maximum"]) < horizon, "fixture crosses H boundary")
        return {
            "columns": [list(item) for item in columns],
            "constraints": [list(item) for item in sorted(constraints)],
            "constraint_definitions": constraint_definitions,
            "days": cast(int, row["days"]),
            "heap_bytes": cast(int, row["heap_bytes"]),
            "index_definitions": index_definitions,
            "index_shape": [
                [name, unique, primary, list(key_columns), predicate]
                for name, unique, primary, key_columns, predicate in sorted(index_shape)
            ],
            "indexes": sorted(index_names),
            "load_seconds": elapsed,
            "maximum_executed_at": cast(datetime, row["maximum"]).isoformat(),
            "minimum_executed_at": cast(datetime, row["minimum"]).isoformat(),
            "identity_oracle_rows": oracle_rows,
            "horizon_session_initial_timezone": fixture_initial_timezone,
            "horizon_session_pinned_timezone": "UTC",
            "rows": cast(int, row["rows"]),
        }
    finally:
        await _close_connection_bounded(connection)


def _range_constraint_sql(horizon: datetime) -> str:
    """Build the NOT VALID legacy range constraint with explicit UTC H.

    Args:
        horizon: Exclusive legacy upper bound.

    Returns:
        ADD CONSTRAINT DDL using both required implication conjuncts.
    """
    return f"""
        ALTER TABLE trades
        ADD CONSTRAINT ck_trades_legacy_range
        CHECK (
            executed_at IS NOT NULL
            AND executed_at < {_horizon_literal(horizon)}
        )
        NOT VALID
    """


def _parent_table_sql() -> str:
    """Return the minimal partitioned parent DDL.

    Returns:
        A parent with real columns/defaults and no PK, U2, or partial index.
    """
    return """
        CREATE TABLE trades_parent (
            id bigint NOT NULL DEFAULT nextval('trades_id_seq'::regclass),
            public_id uuid NOT NULL,
            instrument_public_id uuid NOT NULL,
            trade_id character varying(64),
            price double precision NOT NULL,
            size double precision NOT NULL,
            side character varying(4) NOT NULL,
            executed_at timestamp with time zone NOT NULL,
            session_id uuid NOT NULL,
            sequence_id integer NOT NULL,
            "timestamp" timestamp with time zone NOT NULL,
            known_to timestamp with time zone NOT NULL,
            CONSTRAINT ck_trades_sequence_id CHECK (sequence_id > 0)
        )
        PARTITION BY RANGE (executed_at)
    """


def _parent_index_sql() -> tuple[str, ...]:
    """Return the exact four standalone parent index definitions.

    Returns:
        Standalone unique arbiter and three non-unique read indexes.
    """
    return (
        """
        CREATE UNIQUE INDEX trades_p_uq_instr_tid_exec
        ON trades_parent (instrument_public_id, trade_id, executed_at)
        """,
        """
        CREATE INDEX trades_p_ix_instr_ts
        ON trades_parent (instrument_public_id, "timestamp")
        """,
        'CREATE INDEX trades_p_ix_ts ON trades_parent ("timestamp")',
        "CREATE INDEX trades_p_ix_exec ON trades_parent (executed_at)",
    )


async def _create_parent_preludes(
    instance: InstanceRef,
    horizon: datetime,
) -> dict[str, object]:
    """Create the minimal parent, fourteen daily leaves, and empty DEFAULT.

    Args:
        instance: Verified private PostgreSQL instance.
        horizon: First daily partition lower bound.

    Returns:
        Exact parent and leaf catalog evidence.
    """
    connection = await _connect(instance, "rehearsal-preludes")
    try:
        await _execute_utc_ddl(connection, _parent_table_sql())
        for statement in _parent_index_sql():
            await _execute_utc_ddl(connection, statement)
        partition_names: list[str] = []
        for offset in range(14):
            lower = horizon + timedelta(days=offset)
            upper = lower + timedelta(days=1)
            name = f"trades_d{lower.strftime('%Y%m%d')}"
            partition_names.append(name)
            await _execute_utc_ddl(
                connection,
                f"""
                CREATE TABLE {name}
                PARTITION OF trades_parent
                FOR VALUES FROM ({_horizon_literal(lower)})
                TO ({_horizon_literal(upper)})
                """,
            )
            await _execute_utc_ddl(
                connection,
                f"ALTER TABLE {name} ADD CONSTRAINT {name}_pkey PRIMARY KEY (id)",
            )
            await _execute_utc_ddl(
                connection,
                f"""
                CREATE UNIQUE INDEX {name}_public_id
                ON {name} (public_id)
                WHERE known_to = TIMESTAMPTZ '9999-12-31 23:59:59+00:00'
                """,
            )
        await _execute_utc_ddl(
            connection,
            "CREATE TABLE trades_default PARTITION OF trades_parent DEFAULT",
        )
        await _execute_utc_ddl(
            connection,
            "ALTER TABLE trades_default ADD CONSTRAINT trades_default_pkey PRIMARY KEY (id)",
        )
        await _execute_utc_ddl(
            connection,
            """
            CREATE UNIQUE INDEX trades_default_public_id
            ON trades_default (public_id)
            WHERE known_to = TIMESTAMPTZ '9999-12-31 23:59:59+00:00'
            """,
        )
        parent_indexes = {cast(str, row["relname"]) for row in await connection.fetch("""
                SELECT c.relname
                FROM pg_index i
                JOIN pg_class c ON c.oid = i.indexrelid
                WHERE i.indrelid = 'trades_parent'::regclass
                """)}
        standalone = await connection.fetchval("""
            SELECT NOT EXISTS (
                SELECT 1
                FROM pg_constraint
                WHERE conindid = 'trades_p_uq_instr_tid_exec'::regclass
            )
            """)
        default_rows = await connection.fetchval("SELECT count(*)::bigint FROM trades_default")
        _assert(parent_indexes == _EXPECTED_PARENT_INDEXES, "parent index manifest mismatch")
        _assert(cast(bool, standalone), "parent arbiter is backed by a UNIQUE constraint")
        _assert(cast(int, default_rows) == 0, "DEFAULT partition is not empty before ATTACH")
        return {
            "daily_partitions": partition_names,
            "default_partition": "trades_default",
            "default_rows": cast(int, default_rows),
            "parent_indexes": sorted(parent_indexes),
            "standalone_unique_arbiter": cast(bool, standalone),
        }
    finally:
        await _close_connection_bounded(connection)


async def _parent_unique_constraint_negative_control(
    instance: InstanceRef,
    horizon: datetime,
) -> dict[str, object]:
    """Prove a parent UNIQUE constraint builds a new scaled child index.

    Args:
        instance: Verified private PostgreSQL instance with validated legacy CHECK.
        horizon: Explicit UTC upper bound for the temporary parent.

    Returns:
        Exact before/during/after index OID multisets and attach duration.
    """
    connection = await _connect(instance, "parent-unique-negative")
    try:
        parent_sql = _parent_table_sql().replace("trades_parent", "trades_constraint_parent")
        await _execute_utc_ddl(connection, parent_sql)
        await _execute_utc_ddl(
            connection,
            """
            ALTER TABLE trades_constraint_parent
            ADD CONSTRAINT trades_constraint_parent_uq
            UNIQUE (instrument_public_id, trade_id, executed_at)
            """,
        )
        before = await _legacy_index_oids(connection, "trades")
        fixture_rows = cast(
            int,
            await connection.fetchval("SELECT count(*)::bigint FROM rehearsal.fixture_identity"),
        )
        transaction = connection.transaction()
        await transaction.start()
        during: tuple[int, ...] = ()
        attach_seconds = 0.0
        try:
            await connection.execute("SET LOCAL timezone = 'UTC'")
            started = time.perf_counter()
            await connection.execute(f"""
                ALTER TABLE trades_constraint_parent
                ATTACH PARTITION trades
                FOR VALUES FROM (MINVALUE) TO ({_horizon_literal(horizon)})
                """)
            attach_seconds = time.perf_counter() - started
            during = await _legacy_index_oids(connection, "trades")
        finally:
            await transaction.rollback()
        after = await _legacy_index_oids(connection, "trades")
        await _execute_utc_ddl(connection, "DROP TABLE trades_constraint_parent")
        _assert(
            len(before) == 7,
            "negative parent control did not begin with seven child indexes",
        )
        _assert(
            len(during) == 8,
            "parent UNIQUE constraint did not build an eighth child index",
        )
        _assert(
            set(before).issubset(during),
            "parent UNIQUE control replaced an existing child index",
        )
        _assert(
            after == before,
            "parent UNIQUE control rollback changed child index identities",
        )
        return {
            "attach_seconds": attach_seconds,
            "child_index_oids_after_rollback": list(after),
            "child_index_oids_before": list(before),
            "child_index_oids_during_attach": list(during),
            "new_child_index_count": len(during) - len(before),
            "parent_definition": "UNIQUE constraint",
            "fixture_rows": fixture_rows,
            "proved_at_fixture_scale": fixture_rows >= _QUALIFYING_ROWS,
        }
    finally:
        await _close_connection_bounded(connection)


async def _legacy_index_oids(connection: Connection, table_name: str) -> tuple[int, ...]:
    """Read the exact sorted index OID multiset for one relation.

    Args:
        connection: Verified throwaway connection.
        table_name: Safe internal relation name.

    Returns:
        Sorted index OIDs.
    """
    _assert(table_name in {"trades", "trades_legacy"}, "unexpected index-OID relation")
    rows = await connection.fetch(f"""
        SELECT indexrelid::oid::bigint AS oid
        FROM pg_index
        WHERE indrelid = '{table_name}'::regclass
        ORDER BY indexrelid
        """)
    return tuple(cast(int, row["oid"]) for row in rows)


async def _index_inheritance_pairs(connection: Connection) -> tuple[tuple[str, str], ...]:
    """Read direct legacy-to-parent index inheritance pairs.

    Args:
        connection: Verified throwaway connection.

    Returns:
        Ordered child-index and parent-index name pairs.
    """
    rows = await connection.fetch("""
        SELECT child.relname AS child_name, parent.relname AS parent_name
        FROM pg_inherits h
        JOIN pg_class child ON child.oid = h.inhrelid
        JOIN pg_class parent ON parent.oid = h.inhparent
        JOIN pg_index child_index ON child_index.indexrelid = child.oid
        JOIN pg_index parent_index ON parent_index.indexrelid = parent.oid
        WHERE child_index.indrelid = to_regclass('trades_legacy')
          AND parent_index.indrelid = to_regclass('trades')
        ORDER BY child.relname, parent.relname
        """)
    return tuple((cast(str, row["child_name"]), cast(str, row["parent_name"])) for row in rows)


async def _catalog_state(connection: Connection, horizon: datetime) -> CatalogState:
    """Probe the total cutover decision function from current catalog truth.

    Args:
        connection: Autocommit READ COMMITTED observer.
        horizon: Expected legacy upper partition bound.

    Returns:
        PRE, POST, or INCOHERENT state with every deciding fact.
    """
    row = await connection.fetchrow("""
        SELECT
            (SELECT relkind::text FROM pg_class WHERE oid = to_regclass('trades'))
                AS trades_kind,
            (SELECT relkind::text FROM pg_class WHERE oid = to_regclass('trades_parent'))
                AS staged_parent_kind,
            (SELECT relkind::text FROM pg_class WHERE oid = to_regclass('trades_legacy'))
                AS legacy_kind,
            (SELECT topology FROM rehearsal.topology_epoch WHERE singleton)
                AS topology_epoch,
            EXISTS (
                SELECT 1
                FROM pg_inherits
                WHERE inhrelid = to_regclass('trades_legacy')
                  AND inhparent = to_regclass('trades')
            ) AS legacy_attached,
            EXISTS (
                SELECT 1
                FROM pg_depend dependency
                JOIN pg_class sequence_class ON sequence_class.oid = dependency.objid
                JOIN pg_attribute attribute
                  ON attribute.attrelid = dependency.refobjid
                 AND attribute.attnum = dependency.refobjsubid
                WHERE sequence_class.relname = 'trades_id_seq'
                  AND dependency.deptype = 'a'
                  AND dependency.refobjid = to_regclass('trades')
                  AND attribute.attname = 'id'
            ) AS sequence_owned,
            (
                SELECT pg_get_expr(relpartbound, oid, true)
                FROM pg_class
                WHERE oid = to_regclass('trades_legacy')
            ) AS legacy_bound
        """)
    _assert(row is not None, "catalog state query returned no row")
    pairs = await _index_inheritance_pairs(connection)
    trades_kind = cast(str | None, row["trades_kind"])
    staged_kind = cast(str | None, row["staged_parent_kind"])
    legacy_kind = cast(str | None, row["legacy_kind"])
    epoch = cast(str, row["topology_epoch"])
    attached = cast(bool, row["legacy_attached"])
    sequence_owned = cast(bool, row["sequence_owned"])
    bound = cast(str | None, row["legacy_bound"])
    pre = (
        trades_kind == "r"
        and staged_kind == "p"
        and legacy_kind is None
        and epoch == "ORDINARY"
        and not attached
        and sequence_owned
        and not pairs
    )
    post = (
        trades_kind == "p"
        and staged_kind is None
        and legacy_kind == "r"
        and epoch == "PARTITIONED"
        and attached
        and sequence_owned
        and set(pairs) == _EXPECTED_INDEX_PAIRS
        and bound is not None
        and "MINVALUE" in bound
        and _bound_matches_horizon(bound, horizon)
    )
    pending = (
        trades_kind == "p"
        and staged_kind is None
        and legacy_kind == "r"
        and epoch == "U3_PENDING"
        and attached
        and sequence_owned
        and set(pairs) == _EXPECTED_INDEX_PAIRS
        and bound is not None
        and "MINVALUE" in bound
        and _bound_matches_horizon(bound, horizon)
    )
    retained = (
        trades_kind == "p"
        and staged_kind is None
        and legacy_kind == "r"
        and epoch == "U3"
        and not attached
        and sequence_owned
        and not pairs
        and bound is None
    )
    purged = (
        trades_kind == "p"
        and staged_kind is None
        and legacy_kind is None
        and epoch == "U3"
        and not attached
        and sequence_owned
        and not pairs
        and bound is None
    )
    mode = (
        "PRE"
        if pre
        else (
            "POST"
            if post
            else (
                "PENDING"
                if pending
                else "RETAINED" if retained else "PURGED" if purged else "INCOHERENT"
            )
        )
    )
    return CatalogState(
        mode=mode,
        trades_kind=trades_kind,
        staged_parent_kind=staged_kind,
        legacy_kind=legacy_kind,
        topology_epoch=epoch,
        legacy_attached=attached,
        sequence_owned=sequence_owned,
        index_pairs=pairs,
        legacy_bound=bound,
    )


def _bound_matches_horizon(bound: str, horizon: datetime) -> bool:
    """Compare a rendered partition upper bound by instant, not session text.

    Args:
        bound: ``pg_get_expr(relpartbound)`` rendered in the observer timezone.
        horizon: Expected UTC upper bound.

    Returns:
        True when the rendered TO value is the same absolute instant as H.
    """
    match = re.search(r"TO \('([^']+)'(?:::[^)]+)?\)", bound)
    if match is None:
        return False
    parsed = datetime.fromisoformat(match.group(1).replace(" ", "T"))
    return parsed.astimezone(UTC) == horizon.astimezone(UTC)


def _event_path(events_dir: Path, generation: int, name: str) -> Path:
    """Return one controller event path.

    Args:
        events_dir: Exact run event directory.
        generation: Fence generation.
        name: Controlled event label.

    Returns:
        Unique JSON event path.
    """
    _assert(re.fullmatch(r"[a-z_]+", name) is not None, f"unsafe event name {name}")
    return events_dir / f"g{generation}-{name}.json"


def _write_event(path: Path, payload: dict[str, object]) -> None:
    """Durably publish one controller event through atomic rename.

    Args:
        path: Final event path.
        payload: JSON-compatible event evidence.

    Returns:
        None.
    """
    temporary = path.with_suffix(".tmp")
    with temporary.open("w", encoding="utf-8") as handle:
        json.dump(payload, handle, sort_keys=True)
        handle.flush()
        os.fsync(handle.fileno())
    temporary.replace(path)


async def _wait_event(
    path: Path,
    timeout: float = 30.0,
    process: asyncio.subprocess.Process | None = None,
) -> dict[str, object]:
    """Wait for one durable controller event.

    Args:
        path: Expected event path.
        timeout: Maximum observation time.
        process: Optional child whose early exit must retain its output.

    Returns:
        Parsed JSON object.

    Raises:
        RehearsalError: If the child exits first or the event does not appear.
    """
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if path.exists():
            parsed = json.loads(path.read_text(encoding="utf-8"))
            _assert(isinstance(parsed, dict), f"controller event is not an object: {path}")
            return cast(dict[str, object], parsed)
        if process is not None and process.returncode is not None:
            stdout_bytes, stderr_bytes = await _communicate_process_bounded(process)
            stdout = stdout_bytes.decode("utf-8", errors="replace").strip()
            stderr = stderr_bytes.decode("utf-8", errors="replace").strip()
            raise RehearsalError(
                f"child exited before {path.name}: returncode={process.returncode}; "
                f"stdout={stdout!r}; stderr={stderr!r}"
            )
        await asyncio.sleep(0.02)
    if path.exists():
        parsed = json.loads(path.read_text(encoding="utf-8"))
        _assert(isinstance(parsed, dict), f"controller event is not an object: {path}")
        return cast(dict[str, object], parsed)
    await asyncio.sleep(0)
    if process is not None and process.returncode is not None:
        stdout_bytes, stderr_bytes = await _communicate_process_bounded(process)
        stdout = stdout_bytes.decode("utf-8", errors="replace").strip()
        stderr = stderr_bytes.decode("utf-8", errors="replace").strip()
        raise RehearsalError(
            f"child exited before {path.name}: returncode={process.returncode}; "
            f"stdout={stdout!r}; stderr={stderr!r}"
        )
    raise RehearsalError(f"controller event timeout: {path.name}")


def _child_command(spec: ControllerSpec, role: str) -> list[str]:
    """Build one exact niced parent-death-bound private child command.

    Args:
        spec: Child database, lease, and scenario parameters.
        role: Private controller or publisher role.

    Returns:
        Argument vector for asyncio subprocess creation.
    """
    _assert(role in {"controller", "publisher"}, f"unsupported private child role {role}")
    _assert_source_snapshot(spec.instance.source_snapshot)
    return _niced_parent_bound_command(
        [
            sys.executable,
            str(Path(__file__).resolve()),
            "--role",
            role,
            "--scenario",
            spec.scenario,
            "--generation",
            str(spec.generation),
            "--token",
            str(spec.token),
            "--horizon",
            spec.horizon.isoformat(),
            "--writers",
            str(spec.writers),
            "--transaction-timeout-seconds",
            str(spec.transaction_timeout_seconds),
            "--cleanup-margin-seconds",
            str(spec.cleanup_margin_seconds),
            "--pgdata",
            str(spec.instance.pgdata),
            "--socket-dir",
            str(spec.instance.socket_dir),
            "--port",
            str(spec.instance.port),
            "--user",
            spec.instance.user,
            "--postmaster-pid",
            str(spec.instance.postmaster_pid),
            "--script-sha256",
            spec.instance.source_snapshot.script_sha256,
            "--test-sha256",
            spec.instance.source_snapshot.test_sha256,
            "--events-dir",
            str(spec.events_dir),
        ]
    )


def _controller_command(spec: ControllerSpec) -> list[str]:
    """Build the exact niced self-controller command.

    Args:
        spec: Controller database, lease, and scenario parameters.

    Returns:
        Argument vector for asyncio subprocess creation.
    """
    return _child_command(spec, "controller")


async def _start_controller(spec: ControllerSpec) -> asyncio.subprocess.Process:
    """Start one killable controller as a separate niced process.

    Args:
        spec: Exact controller invocation.

    Returns:
        Running subprocess handle.
    """
    process = await asyncio.create_subprocess_exec(
        *_controller_command(spec),
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    _assert(process.pid is not None and process.pid > 1, "controller process has no valid PID")
    return process


async def _start_writer_drop_child(spec: ControllerSpec) -> asyncio.subprocess.Process:
    """Start one parent-death-bound publisher used for the ACTIVE-fence kill.

    Args:
        spec: Exact private instance and active fence generation.

    Returns:
        Running publisher subprocess handle.
    """
    process = await asyncio.create_subprocess_exec(
        *_child_command(spec, "publisher"),
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    _assert(process.pid is not None and process.pid > 1, "publisher process has no valid PID")
    return process


async def _communicate_process_bounded(
    process: asyncio.subprocess.Process,
    timeout: float = _CHILD_REAP_TIMEOUT_SECONDS,
) -> tuple[bytes, bytes]:
    """Drain and reap one owned child within a fixed wall-clock bound.

    Args:
        process: Exact subprocess created by this harness.
        timeout: Maximum seconds before forced termination.

    Returns:
        Complete stdout and stderr byte streams.
    """
    try:
        return await asyncio.wait_for(process.communicate(), timeout=timeout)
    except TimeoutError:
        if process.returncode is None:
            process.kill()
        try:
            return await asyncio.wait_for(
                process.communicate(),
                timeout=_TASK_CANCEL_TIMEOUT_SECONDS,
            )
        except TimeoutError as reap_exc:
            raise RehearsalError(
                f"owned child PID {process.pid} resisted bounded SIGKILL reap"
            ) from reap_exc


async def _kill_process_bounded(process: asyncio.subprocess.Process) -> None:
    """SIGKILL and reap one exact owned child without an unbounded wait.

    Args:
        process: Exact subprocess created by this harness.

    Returns:
        None after the process is reaped.
    """
    if process.returncode is None:
        process.kill()
    await _communicate_process_bounded(process)


async def _kill_controller(process: asyncio.subprocess.Process) -> tuple[int, str, str]:
    """SIGKILL and reap the exact controller process.

    Args:
        process: Controller subprocess handle.

    Returns:
        Return code, stdout, and stderr.
    """
    _assert(process.returncode is None, "controller exited before requested kill point")
    process.send_signal(signal.SIGKILL)
    stdout_bytes, stderr_bytes = await _communicate_process_bounded(process)
    returncode = cast(int, process.returncode)
    _assert(returncode == -signal.SIGKILL, f"controller return code is {returncode}, not SIGKILL")
    return (
        returncode,
        stdout_bytes.decode("utf-8", errors="replace"),
        stderr_bytes.decode("utf-8", errors="replace"),
    )


async def _wait_backend(
    connection: Connection,
    application_name: str,
    query_fragment: str,
    expected_wait_event_type: str | None = None,
    timeout: float = 30.0,
) -> Record:
    """Independently observe a controller backend at the requested SQL point.

    Args:
        connection: Autocommit monitoring connection.
        application_name: Exact controller application label.
        query_fragment: SQL text that must be active.
        expected_wait_event_type: Optional wait-event type that must also be visible.
        timeout: Maximum observation time.

    Returns:
        pg_stat_activity row proving the backend state.
    """
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        row = await connection.fetchrow(
            """
            SELECT pid, state, query, wait_event_type, wait_event, backend_start
            FROM pg_stat_activity
            WHERE application_name = $1
              AND state = 'active'
              AND query ILIKE '%' || $2 || '%'
            """,
            application_name,
            query_fragment,
        )
        if row is not None and (
            expected_wait_event_type is None or row["wait_event_type"] == expected_wait_event_type
        ):
            return row
        await asyncio.sleep(0.01)
    raise RehearsalError(
        f"did not independently observe {application_name} executing {query_fragment}"
    )


async def _wait_backend_present(
    connection: Connection,
    application_name: str,
    timeout: float = 30.0,
) -> Record:
    """Independently observe a controller backend in any transaction state.

    Args:
        connection: Autocommit monitoring connection.
        application_name: Exact controller application label.
        timeout: Maximum observation time.

    Returns:
        Current pg_stat_activity row for the controller backend.
    """
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        row = await connection.fetchrow(
            """
            SELECT pid, state, query, wait_event_type, wait_event, backend_start
            FROM pg_stat_activity
            WHERE application_name = $1
            """,
            application_name,
        )
        if row is not None:
            return row
        await asyncio.sleep(0.01)
    raise RehearsalError(f"controller backend was not present: {application_name}")


async def _wait_backend_gone(
    connection: Connection,
    application_name: str,
    timeout: float = 30.0,
) -> datetime:
    """Wait until a controller transaction/backend is absent.

    Args:
        connection: Autocommit monitoring connection.
        application_name: Exact controller application label.
        timeout: Maximum observation time.

    Returns:
        Database clock at first confirmed absence.
    """
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        row = await connection.fetchrow(
            """
            SELECT
                clock_timestamp() AS observed_at,
                count(*)::integer AS active
            FROM pg_stat_activity
            WHERE application_name = $1
            """,
            application_name,
        )
        _assert(row is not None, "backend absence query returned no row")
        if cast(int, row["active"]) == 0:
            return cast(datetime, row["observed_at"])
        await asyncio.sleep(0.02)
    raise RehearsalError(f"controller backend did not disappear: {application_name}")


def _private_backend_process_state(instance: InstanceRef, backend_pid: int) -> str:
    """Read one verified throwaway backend's Linux process state.

    Args:
        instance: Private postmaster that must own the backend.
        backend_pid: PostgreSQL backend PID observed through pg_stat_activity.

    Returns:
        Single-letter Linux process state.
    """
    _assert(backend_pid > 1, "refusing to signal an invalid backend PID")
    status_path = Path("/proc") / str(backend_pid) / "status"
    try:
        status = status_path.read_text(encoding="utf-8")
    except OSError as exc:
        raise RehearsalError(f"private backend PID {backend_pid} disappeared") from exc
    parent_match = re.search(r"^PPid:\s+(\d+)", status, re.MULTILINE)
    _assert(parent_match is not None, f"backend PID {backend_pid} has no parent identity")
    _assert(
        int(cast(re.Match[str], parent_match).group(1)) == instance.postmaster_pid,
        f"backend PID {backend_pid} is not owned by the throwaway postmaster",
    )
    match = re.search(r"^State:\s+([A-Z])", status, re.MULTILINE)
    _assert(match is not None, f"backend PID {backend_pid} has no readable process state")
    return cast(re.Match[str], match).group(1)


async def _freeze_private_backend(instance: InstanceRef, backend_pid: int) -> str:
    """Freeze the exact private VALIDATE backend to close observation races.

    Args:
        instance: Private postmaster that owns the backend.
        backend_pid: Independently observed active VALIDATE backend.

    Returns:
        Linux stopped state proving the backend cannot leave VALIDATE before SIGKILL.
    """
    _private_backend_process_state(instance, backend_pid)
    os.kill(backend_pid, signal.SIGSTOP)
    deadline = time.monotonic() + 2.0
    while time.monotonic() < deadline:
        state = _private_backend_process_state(instance, backend_pid)
        if state == "T":
            return state
        await asyncio.sleep(0.01)
    raise RehearsalError(f"private VALIDATE backend PID {backend_pid} did not stop")


async def _resume_private_backend(instance: InstanceRef, backend_pid: int) -> None:
    """Resume an exact private backend after a controlled proof pause.

    Args:
        instance: Private postmaster that owns the backend.
        backend_pid: Backend previously stopped by this harness.

    Returns:
        None once the backend is running or has already exited.
    """
    try:
        state = _private_backend_process_state(instance, backend_pid)
    except RehearsalError:
        return
    if state == "T":
        os.kill(backend_pid, signal.SIGCONT)
    deadline = time.monotonic() + 2.0
    while time.monotonic() < deadline:
        try:
            if _private_backend_process_state(instance, backend_pid) != "T":
                return
        except RehearsalError:
            return
        await asyncio.sleep(0.01)
    raise RehearsalError(f"private VALIDATE backend PID {backend_pid} did not resume")


async def _ddl_lock_count(connection: Connection, ddl_pid: int) -> int:
    """Count generation advisory and ACCESS EXCLUSIVE locks for one DDL PID.

    Args:
        connection: Autocommit monitoring connection.
        ddl_pid: Identified PostgreSQL backend PID.

    Returns:
        Count of locks that prove the DDL transaction is still authoritative.
    """
    value = await connection.fetchval(
        """
        SELECT count(*)::integer
        FROM pg_locks
        WHERE pid = $1
          AND granted
          AND (
              locktype = 'advisory'
              OR mode = 'AccessExclusiveLock'
          )
        """,
        ddl_pid,
    )
    return cast(int, value)


async def _open_fence(
    instance: InstanceRef,
    config: RunConfig,
    scenario: str,
) -> FenceLease:
    """Open a new database-clock publisher lease generation.

    Args:
        instance: Verified private PostgreSQL instance.
        config: Lease and timeout bounds.
        scenario: Controller scenario label.

    Returns:
        Committed fence generation and absolute database-clock expiry.
    """
    connection = await _connect(instance, f"fence-open-{scenario}")
    try:
        generation = cast(
            int,
            await connection.fetchval(
                "SELECT COALESCE(max(generation), 0)::bigint + 1 FROM rehearsal.fence_generations"
            ),
        )
        token = uuid4()
        row = await connection.fetchrow(
            """
            INSERT INTO rehearsal.fence_generations (
                generation,
                token,
                opened_at,
                lease_expires_at,
                cleanup_margin_ms,
                transaction_timeout_ms,
                ddl_application
            )
            VALUES (
                $1,
                $2,
                clock_timestamp(),
                clock_timestamp() + make_interval(secs => $3),
                $4,
                $5,
                $6
            )
            RETURNING opened_at, lease_expires_at
            """,
            generation,
            token,
            config.lease_seconds,
            round(config.cleanup_margin_seconds * 1000),
            round(config.transaction_timeout_seconds * 1000),
            f"rehearsal-controller-g{generation}-{scenario}",
        )
        _assert(row is not None, "fence generation insert returned no row")
        return FenceLease(
            generation=generation,
            token=token,
            opened_at=cast(datetime, row["opened_at"]),
            expires_at=cast(datetime, row["lease_expires_at"]),
        )
    finally:
        await _close_connection_bounded(connection)


async def _latest_fence(connection: Connection) -> Record | None:
    """Return the latest publisher lease generation.

    Args:
        connection: Publisher control connection.

    Returns:
        Latest fence row or None before the first fence.
    """
    return await connection.fetchrow("""
        SELECT
            generation,
            token,
            lease_expires_at,
            resolution,
            resolver,
            resolved_fingerprint,
            ddl_pid,
            clock_timestamp() AS database_now
        FROM rehearsal.fence_generations
        ORDER BY generation DESC
        LIMIT 1
        """)


def _writer_trade_prefix(scenario: str) -> str:
    """Return the independent trade-id provenance prefix for one scenario.

    Args:
        scenario: Stable scenario label used by every publisher.

    Returns:
        Prefix shared by all synthetic writer rows in that scenario.
    """
    scenario_digest = hashlib.sha256(scenario.encode("utf-8")).hexdigest()[:10]
    return f"r-{scenario_digest}-"


def _writer_rows(
    context: WriterContext, mode: str, first_sequence: int, size: int
) -> list[WriterRow]:
    """Generate one deterministic batch with stable independent identities.

    Args:
        context: Publisher identity and H boundary.
        mode: Ordinary pre-H or partitioned post-H routing mode.
        first_sequence: First publisher-local sequence number.
        size: Number of rows to create.

    Returns:
        Deterministic full trades rows.
    """
    rows: list[WriterRow] = []
    publisher_number = int(context.publisher_id.rsplit("-", 1)[1])
    instrument_id = UUID(int=0xC800 + publisher_number)
    session_id = uuid5(_SESSION_NAMESPACE, f"{context.scenario}:{context.publisher_id}")
    trade_prefix = _writer_trade_prefix(context.scenario)
    for offset in range(size):
        sequence_id = first_sequence + offset
        if mode == "ORDINARY_TARGETLESS":
            executed_at = context.horizon - timedelta(
                seconds=(sequence_id % 80_000) + 1,
                microseconds=publisher_number,
            )
        else:
            executed_at = context.horizon + timedelta(
                days=sequence_id % 3,
                seconds=(sequence_id * 17) % 80_000,
                microseconds=publisher_number,
            )
        public_id = uuid5(
            _IDENTITY_NAMESPACE,
            f"{context.scenario}:{context.publisher_id}:{sequence_id}",
        )
        trade_id = f"{trade_prefix}{publisher_number}-{sequence_id}"
        identity = StableIdentity(
            public_id=public_id,
            instrument_public_id=instrument_id,
            trade_id=trade_id,
            executed_at=executed_at,
        )
        rows.append(
            WriterRow(
                identity=identity,
                price=50_000.0 + (sequence_id % 1000) / 10.0,
                size=0.001 + (sequence_id % 100) / 100_000.0,
                side="buy" if sequence_id % 2 == 0 else "sell",
                session_id=session_id,
                sequence_id=sequence_id,
                timestamp=executed_at + timedelta(milliseconds=1),
            )
        )
    return rows


def _insert_sql(batch_size: int, mode: str = "ORDINARY_TARGETLESS") -> str:
    """Build a typed targetless batch INSERT with RETURNING evidence.

    Args:
        batch_size: Number of rows in the batch.
        mode: Targetless bridge mode or explicit-U3 retention mode.

    Returns:
        Parameterized INSERT using ON CONFLICT DO NOTHING.
    """
    _assert(batch_size > 0, "writer batch size must be positive")
    _assert(
        mode
        in {
            "ORDINARY_TARGETLESS",
            "PARTITIONED_TARGETLESS",
            "PARTITIONED_U3",
        },
        f"unsupported writer mode {mode}",
    )
    fields_per_row = 11
    groups: list[str] = []
    for row_number in range(batch_size):
        first = row_number * fields_per_row + 1
        groups.append(
            "("
            + ",".join(f"${position}" for position in range(first, first + fields_per_row))
            + ")"
        )
    conflict_clause = (
        "ON CONFLICT (instrument_public_id, trade_id, executed_at) DO NOTHING"
        if mode == "PARTITIONED_U3"
        else "ON CONFLICT DO NOTHING"
    )
    return f"""
        INSERT INTO trades (
            public_id,
            instrument_public_id,
            trade_id,
            price,
            size,
            side,
            executed_at,
            session_id,
            sequence_id,
            "timestamp",
            known_to
        )
        VALUES {",".join(groups)}
        {conflict_clause}
        RETURNING public_id::text
    """


def _append_expected(handle: TextIO, rows: Sequence[WriterRow], stats: WriterStats) -> None:
    """Durably append expected identities before attempting database insertion.

    Args:
        handle: Publisher-exclusive external journal.
        rows: Batch that will be sent to PostgreSQL.
        stats: Mutable expected multiset and counters.

    Returns:
        None.
    """
    for row in rows:
        handle.write(row.identity.json_line() + "\n")
    handle.flush()
    os.fsync(handle.fileno())
    stats.expected.extend(row.identity for row in rows)
    stats.sent += len(rows)


async def _insert_writer_batch(
    connection: Connection,
    rows: Sequence[WriterRow],
    stats: WriterStats,
    mode: str,
) -> None:
    """Insert one targetless batch and reject hidden conflicts.

    Args:
        connection: Fresh or current publisher data connection.
        rows: Durable expected rows to insert.
        stats: Publisher counters and failure evidence.
        mode: Current targetless or explicit-U3 conflict mode.

    Returns:
        None.

    Raises:
        RehearsalError: If targetless handling hides any conflict.
    """
    parameters: list[object] = []
    for row in rows:
        parameters.extend(row.values())
    try:
        returned = await connection.fetch(_insert_sql(len(rows), mode), *parameters)
    except asyncpg.UniqueViolationError as exc:
        stats.surfaced_unique_violations += 1
        raise RehearsalError(f"publisher surfaced unique violation: {exc}") from exc
    if len(returned) != len(rows):
        stats.hidden_conflicts += len(rows) - len(returned)
        raise RehearsalError(
            f"targetless writer hid {len(rows) - len(returned)} unexpected conflicts"
        )
    stats.committed += len(rows)
    stats.batches += 1


async def _exercise_conflict_batch(
    connection: Connection,
    rows: Sequence[WriterRow],
    mode: str,
    stats: WriterStats,
) -> None:
    """Classify one clean duplicate and one mismatched twin in one batch.

    Args:
        connection: Current publisher data connection.
        rows: Newly committed canonical rows supplying two conflict identities.
        mode: Ordinary or partitioned targetless writer mode.
        stats: Publisher classification and violation evidence.

    Returns:
        None after the duplicate is deduped and the mismatch is quarantined.
    """
    _assert(len(rows) >= 1, "conflict proof requires one canonical row")
    clean = rows[0]
    canonical = rows[-1]
    mismatched = WriterRow(
        identity=canonical.identity,
        price=canonical.price + 17.0,
        size=canonical.size,
        side=canonical.side,
        session_id=canonical.session_id,
        sequence_id=canonical.sequence_id,
        timestamp=canonical.timestamp,
    )
    parameters: list[object] = []
    for row in (clean, mismatched):
        parameters.extend(row.values())
    try:
        returned = await connection.fetch(_insert_sql(2, mode), *parameters)
    except asyncpg.UniqueViolationError as exc:
        stats.surfaced_unique_violations += 1
        raise RehearsalError(f"conflict bridge surfaced unique violation: {exc}") from exc
    if returned:
        stats.conflict_classification_failures += 1
        raise RehearsalError("conflict proof inserted a duplicate or mismatched twin")
    stored_clean = await connection.fetchrow(
        """
        SELECT instrument_public_id, trade_id, executed_at, price, size, side
        FROM trades
        WHERE public_id = $1
          AND known_to = $2
        """,
        clean.identity.public_id,
        _KNOWN_TO,
    )
    stored_mismatch = await connection.fetchrow(
        """
        SELECT instrument_public_id, trade_id, executed_at, price, size, side
        FROM trades
        WHERE public_id = $1
          AND known_to = $2
        """,
        mismatched.identity.public_id,
        _KNOWN_TO,
    )
    _assert(stored_clean is not None and stored_mismatch is not None, "conflict twin disappeared")

    def business_matches(stored: Record, attempted: WriterRow) -> bool:
        return (
            cast(UUID, stored["instrument_public_id"]) == attempted.identity.instrument_public_id
            and cast(str, stored["trade_id"]) == attempted.identity.trade_id
            and cast(datetime, stored["executed_at"]) == attempted.identity.executed_at
            and cast(float, stored["price"]) == attempted.price
            and cast(float, stored["size"]) == attempted.size
            and cast(str, stored["side"]) == attempted.side
        )

    clean_match = business_matches(stored_clean, clean)
    mismatch_match = business_matches(stored_mismatch, mismatched)
    if not clean_match or mismatch_match:
        stats.conflict_classification_failures += 1
        raise RehearsalError("conflict bridge misclassified clean duplicate or mismatched twin")
    await connection.execute(
        """
        INSERT INTO rehearsal.conflict_quarantine (
            public_id,
            instrument_public_id,
            trade_id,
            executed_at,
            reason
        )
        VALUES ($1, $2, $3, $4, 'mismatched business-payload twin')
        """,
        mismatched.identity.public_id,
        mismatched.identity.instrument_public_id,
        mismatched.identity.trade_id,
        mismatched.identity.executed_at,
    )
    stats.conflict_batches_exercised += 1
    stats.clean_duplicates_classified += 1
    stats.mismatched_twins_quarantined += 1
    stats.conflict_modes.add(mode)


async def _publisher_ack(
    connection: Connection,
    generation: int,
    publisher_id: str,
) -> None:
    """Acknowledge an atomic local generation swap after session closure.

    Args:
        connection: Publisher control connection.
        generation: Fence generation being acknowledged.
        publisher_id: Stable publisher identity.

    Returns:
        None.
    """
    await connection.execute(
        """
        INSERT INTO rehearsal.publisher_acks (
            generation,
            publisher_id,
            retention_generation,
            acknowledged_at,
            session_closed,
            inflight
        )
        SELECT $1, $2, generation, clock_timestamp(), true, 0
        FROM rehearsal.retention_generation
        WHERE singleton
        ON CONFLICT (generation, publisher_id) DO NOTHING
        """,
        generation,
        publisher_id,
    )
    await connection.execute(
        """
        INSERT INTO rehearsal.publisher_events (
            generation,
            publisher_id,
            event
        )
        VALUES ($1, $2, 'PAUSED')
        """,
        generation,
        publisher_id,
    )


async def _current_writer_mode(connection: Connection, horizon: datetime) -> str:
    """Resolve ordinary or partitioned targetless mode from current catalog.

    Args:
        connection: Publisher control connection.
        horizon: Expected partition boundary when the parent is live.

    Returns:
        Targetless writer mode for the currently visible ``trades`` relation.
    """
    relkind = cast(
        str | None,
        await connection.fetchval(
            "SELECT relkind::text FROM pg_class WHERE oid = to_regclass('trades')"
        ),
    )
    if relkind == "r":
        return "ORDINARY_TARGETLESS"
    state = await _catalog_state(connection, horizon)
    _assert(
        state.mode in {"POST", "RETAINED", "PURGED"},
        "partitioned writer saw incoherent catalog",
    )
    return "PARTITIONED_TARGETLESS" if state.mode == "POST" else "PARTITIONED_U3"


async def _attempt_reconciliation(
    connection: Connection,
    context: WriterContext,
    fence: Record,
) -> tuple[str, str] | None:
    """Compete for one expiry reconciliation CAS after DDL authority is gone.

    Args:
        connection: Publisher control connection.
        context: Publisher identity and expected H.
        fence: Latest observed fence row.

    Returns:
        Resolution and fingerprint once terminal, or None while still fenced.
    """
    generation = cast(int, fence["generation"])
    token = cast(UUID, fence["token"])
    try:
        async with connection.transaction():
            await connection.execute("SET LOCAL lock_timeout = '100ms'")
            locked = await connection.fetchrow(
                """
                SELECT
                    generation,
                    lease_expires_at,
                    resolution,
                    resolver,
                    resolved_fingerprint,
                    ddl_pid,
                    clock_timestamp() AS database_now
                FROM rehearsal.fence_generations
                WHERE generation = $1
                  AND token = $2
                FOR UPDATE
                """,
                generation,
                token,
            )
            _assert(locked is not None, "fence generation disappeared")
            if locked["resolution"] is not None:
                return (
                    cast(str, locked["resolution"]),
                    cast(str, locked["resolved_fingerprint"]),
                )
            database_now = cast(datetime, locked["database_now"])
            expires_at = cast(datetime, locked["lease_expires_at"])
            if database_now < expires_at:
                return None
            ddl_pid = cast(int | None, locked["ddl_pid"])
            if ddl_pid is not None and await _ddl_lock_count(connection, ddl_pid) != 0:
                return None
            state = await _catalog_state(connection, context.horizon)
            if state.mode == "PENDING":
                winner = await connection.fetchval(
                    """
                    INSERT INTO rehearsal.retention_resolution_winners (
                        generation,
                        resolver
                    )
                    VALUES ($1, $2)
                    ON CONFLICT (generation) DO NOTHING
                    RETURNING generation
                    """,
                    generation,
                    context.publisher_id,
                )
                _assert(winner is not None, "U3_PENDING recovery had no generation CAS winner")
                reverted = await connection.fetchval("""
                    UPDATE rehearsal.topology_epoch
                    SET topology = 'PARTITIONED'
                    WHERE singleton
                      AND topology = 'U3_PENDING'
                    RETURNING topology
                    """)
                _assert(
                    reverted == "PARTITIONED",
                    "U3_PENDING CAS winner did not restore targetless topology",
                )
                state = await _catalog_state(connection, context.horizon)
                _assert(state.mode == "POST", "U3_PENDING resolver did not reach POST")
            if state.mode == "INCOHERENT":
                raise RehearsalError("publisher observed incoherent terminal catalog")
            resolution = "PRE_ABORTED" if state.mode == "PRE" else "POST_COMMITTED"
            fingerprint = state.fingerprint()
            updated = await connection.fetchval(
                """
                UPDATE rehearsal.fence_generations
                SET
                    resolution = $2,
                    resolver = $3,
                    resolved_fingerprint = $4,
                    resolved_at = clock_timestamp()
                WHERE generation = $1
                  AND token = $5
                  AND resolution IS NULL
                RETURNING generation
                """,
                generation,
                resolution,
                context.publisher_id,
                fingerprint,
                token,
            )
            if updated is not None:
                await connection.execute(
                    """
                    INSERT INTO rehearsal.publisher_events (
                        generation,
                        publisher_id,
                        event,
                        mode,
                        fingerprint
                    )
                    VALUES ($1, $2, 'RECONCILE_WINNER', $3, $4)
                    """,
                    generation,
                    context.publisher_id,
                    resolution,
                    fingerprint,
                )
            return resolution, fingerprint
    except asyncpg.LockNotAvailableError:
        return None


async def _await_publisher_resolution(
    connection: Connection,
    context: WriterContext,
    generation: int,
) -> tuple[str, str, str]:
    """Stay fenced until controller release or watchdog reconciliation.

    Args:
        connection: Publisher control connection.
        context: Publisher identity and H boundary.
        generation: Acknowledged fence generation.

    Returns:
        Resolution, fingerprint, and resume source.
    """
    while True:
        fence = await connection.fetchrow(
            """
            SELECT
                generation,
                token,
                lease_expires_at,
                resolution,
                resolver,
                resolved_fingerprint,
                ddl_pid,
                clock_timestamp() AS database_now
            FROM rehearsal.fence_generations
            WHERE generation = $1
            """,
            generation,
        )
        _assert(fence is not None, "publisher fence row disappeared")
        resolution = cast(str | None, fence["resolution"])
        if resolution is not None:
            return (
                resolution,
                cast(str, fence["resolved_fingerprint"]),
                (
                    "controller"
                    if cast(str, fence["resolver"]) == "controller"
                    else "publisher_watchdog"
                ),
            )
        reconciled = await _attempt_reconciliation(connection, context, fence)
        if reconciled is not None:
            return reconciled[0], reconciled[1], "publisher_watchdog"
        await asyncio.sleep(0.03)


async def _wait_writer_interval(stop_event: asyncio.Event, interval: float) -> None:
    """Wait for either the next batch interval or an immediate clean stop.

    Args:
        stop_event: Publisher-local clean-stop signal.
        interval: Maximum seconds before the next batch.

    Returns:
        None after either event.
    """
    with contextlib.suppress(TimeoutError):
        await asyncio.wait_for(stop_event.wait(), timeout=interval)


async def _publisher_loop(context: WriterContext) -> None:
    """Run a synthetic writer with a publisher-local lease watchdog.

    Args:
        context: Publisher database, rate, identity, and evidence state.

    Returns:
        None after a clean requested stop.
    """
    control = await _connect(context.instance, f"publisher-control-{context.publisher_id}")
    data = await _connect(context.instance, f"publisher-data-{context.publisher_id}")
    startup_fence = await _latest_fence(control)
    last_generation = (
        cast(int, startup_fence["generation"])
        if startup_fence is not None and startup_fence["resolution"] is not None
        else 0
    )
    sequence_id = 1
    batch_size = max(1, min(8, round(context.writer_rate / context.writer_count / 10)))
    interval = batch_size * context.writer_count / context.writer_rate
    try:
        with context.ledger_path.open("a", encoding="utf-8") as ledger:
            while not context.stop_event.is_set():
                latest = await _latest_fence(control)
                if latest is not None and cast(int, latest["generation"]) > last_generation:
                    generation = cast(int, latest["generation"])
                    await _close_connection_bounded(data)
                    await _publisher_ack(control, generation, context.publisher_id)
                    resolution, fingerprint, source = await _await_publisher_resolution(
                        control,
                        context,
                        generation,
                    )
                    state = await _catalog_state(control, context.horizon)
                    expected_resolution = "PRE_ABORTED" if state.mode == "PRE" else "POST_COMMITTED"
                    _assert(
                        state.mode in {"PRE", "POST", "RETAINED", "PURGED"},
                        "publisher resume catalog is incoherent",
                    )
                    _assert(
                        resolution == expected_resolution, "publisher resolution/catalog mismatch"
                    )
                    _assert(
                        fingerprint == state.fingerprint(), "publisher resume fingerprint drift"
                    )
                    fresh_fence = await control.fetchrow(
                        """
                        SELECT ddl_pid
                        FROM rehearsal.fence_generations
                        WHERE generation = $1
                        """,
                        generation,
                    )
                    _assert(fresh_fence is not None, "publisher resume fence disappeared")
                    ddl_pid = cast(int | None, fresh_fence["ddl_pid"])
                    if ddl_pid is not None:
                        _assert(
                            await _ddl_lock_count(control, ddl_pid) == 0,
                            "publisher resumed while DDL locks remained",
                        )
                    mode = {
                        "PRE": "ORDINARY_TARGETLESS",
                        "POST": "PARTITIONED_TARGETLESS",
                        "RETAINED": "PARTITIONED_U3",
                        "PURGED": "PARTITIONED_U3",
                    }[state.mode]
                    await control.execute(
                        """
                        INSERT INTO rehearsal.publisher_events (
                            generation,
                            publisher_id,
                            event,
                            mode,
                            fingerprint
                        )
                        VALUES ($1, $2, $3, $4, $5)
                        """,
                        generation,
                        context.publisher_id,
                        f"RESUMED_{source.upper()}",
                        mode,
                        fingerprint,
                    )
                    data = await _connect(
                        context.instance,
                        f"publisher-data-{context.publisher_id}-g{generation}",
                    )
                    context.stats.resumed_generations.append(generation)
                    last_generation = generation
                mode = await _current_writer_mode(control, context.horizon)
                rows = _writer_rows(context, mode, sequence_id, batch_size)
                _append_expected(ledger, rows, context.stats)
                await _insert_writer_batch(data, rows, context.stats, mode)
                if mode not in context.stats.conflict_modes:
                    await _exercise_conflict_batch(data, rows, mode, context.stats)
                await control.execute(
                    """
                    INSERT INTO rehearsal.publisher_events (
                        generation,
                        publisher_id,
                        event,
                        mode
                    )
                    VALUES ($1, $2, 'COMMIT', $3)
                    """,
                    last_generation if last_generation > 0 else None,
                    context.publisher_id,
                    mode,
                )
                sequence_id += batch_size
                await _wait_writer_interval(context.stop_event, interval)
    except Exception as exc:
        context.stats.failures.append(f"{type(exc).__name__}: {exc}")
        raise
    finally:
        await _close_connection_bounded(data)
        await _close_connection_bounded(control)


async def _wait_writer_commits(
    stats: Sequence[WriterStats],
    minimum: int,
    timeout: float = 30.0,
) -> None:
    """Wait until every publisher has committed a minimum number of rows.

    Args:
        stats: Publisher evidence objects.
        minimum: Required committed rows per publisher.
        timeout: Maximum wait.

    Returns:
        None.
    """
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        failures = [failure for item in stats for failure in item.failures]
        if failures:
            raise RehearsalError("publisher failed before commit target: " + "; ".join(failures))
        if all(item.committed >= minimum for item in stats):
            return
        await asyncio.sleep(0.03)
    raise RehearsalError(f"publishers did not each commit {minimum} rows")


async def _wait_writer_fence_ready(
    stats: Sequence[WriterStats],
    minimum: int,
    timeout: float = 30.0,
) -> None:
    """Wait until publishers finish their first conflict proof before fencing.

    Args:
        stats: Publisher counters and failure evidence.
        minimum: Required committed rows per publisher.
        timeout: Maximum readiness wait.

    Returns:
        None once every publisher is outside its initial conflict probe.
    """
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        failures = [failure for item in stats for failure in item.failures]
        if failures:
            raise RehearsalError("publisher failed before fence readiness: " + "; ".join(failures))
        if all(
            item.committed >= minimum and item.conflict_batches_exercised >= 1 for item in stats
        ):
            return
        await asyncio.sleep(0.03)
    raise RehearsalError(
        f"publishers did not each commit {minimum} rows and finish the conflict proof"
    )


def _require_live_fence_window(
    resolution: str | None,
    remaining_seconds: float,
    required_seconds: float,
) -> None:
    """Reject a terminal or too-short lease before a controller is spawned.

    Args:
        resolution: Durable generation resolution, if already terminal.
        remaining_seconds: Database-clock lease lifetime still available.
        required_seconds: Transaction timeout plus backend cleanup margin.

    Returns:
        None only for an ACTIVE generation with sufficient remaining lifetime.
    """
    _assert(resolution is None, "publisher acknowledgement generation is already terminal")
    _assert(
        remaining_seconds > required_seconds,
        (
            f"publisher acknowledgement left {remaining_seconds:.6f}s, "
            f"not more than the required {required_seconds:.6f}s lease window"
        ),
    )


async def _wait_fence_acks(
    instance: InstanceRef,
    lease: FenceLease,
    writer_count: int,
    required_seconds: float,
    expected_retention_generation: int | None = None,
) -> list[dict[str, object]]:
    """Wait for every publisher to close its session and acknowledge the fence.

    Args:
        instance: Verified private PostgreSQL instance.
        lease: Fence generation awaiting acknowledgements.
        writer_count: Exact configured publisher count.
        required_seconds: Minimum remaining transaction and cleanup window.
        expected_retention_generation: Required atomic local generation swap.

    Returns:
        Acknowledgement evidence dictionaries.
    """
    connection = await _connect(instance, f"ack-observer-g{lease.generation}")
    try:
        deadline = time.monotonic() + 30.0
        expected_publishers = {f"publisher-{number}" for number in range(1, writer_count + 1)}
        while time.monotonic() < deadline:
            fence = await connection.fetchrow(
                """
                SELECT
                    token,
                    resolution,
                    EXTRACT(
                        EPOCH FROM (lease_expires_at - clock_timestamp())
                    )::double precision AS remaining_seconds
                FROM rehearsal.fence_generations
                WHERE generation = $1
                """,
                lease.generation,
            )
            _assert(fence is not None, "publisher acknowledgement generation disappeared")
            observed_fence_token = cast(UUID, fence["token"])
            _assert(
                observed_fence_token == lease.token,
                "publisher acknowledgement fence token changed",
            )
            _require_live_fence_window(
                cast(str | None, fence["resolution"]),
                cast(float, fence["remaining_seconds"]),
                required_seconds,
            )
            rows = await connection.fetch(
                """
                SELECT
                    publisher_id,
                    retention_generation,
                    session_closed,
                    inflight,
                    acknowledged_at
                FROM rehearsal.publisher_acks
                WHERE generation = $1
                ORDER BY publisher_id
                """,
                lease.generation,
            )
            if len(rows) >= writer_count:
                observed_publishers = {cast(str, row["publisher_id"]) for row in rows}
                _assert(
                    observed_publishers == expected_publishers,
                    "publisher acknowledgement identities do not match the configured set",
                )
                evidence: list[dict[str, object]] = []
                for row in rows:
                    publisher_id = cast(str, row["publisher_id"])
                    _assert(
                        cast(bool, row["session_closed"]), "publisher ack kept data session open"
                    )
                    _assert(
                        cast(int, row["inflight"]) == 0, "publisher ack retained in-flight rows"
                    )
                    acknowledged_generation = cast(int, row["retention_generation"])
                    if expected_retention_generation is not None:
                        _assert(
                            acknowledged_generation == expected_retention_generation,
                            "publisher acknowledged the fence before its generation swap",
                        )
                    data_sessions = cast(
                        int,
                        await connection.fetchval(
                            """
                            SELECT count(*)::integer
                            FROM pg_stat_activity
                            WHERE application_name LIKE $1
                            """,
                            f"publisher-data-{publisher_id}%",
                        ),
                    )
                    _assert(
                        data_sessions == 0,
                        f"publisher {publisher_id} acknowledged with a live data session",
                    )
                    evidence.append(
                        {
                            "acknowledged_at": cast(datetime, row["acknowledged_at"]).isoformat(),
                            "data_sessions_observed": data_sessions,
                            "fence_generation": lease.generation,
                            "fence_token": str(observed_fence_token),
                            "inflight": cast(int, row["inflight"]),
                            "publisher_id": publisher_id,
                            "retention_generation": acknowledged_generation,
                            "session_closed": cast(bool, row["session_closed"]),
                        }
                    )
                return evidence
            await asyncio.sleep(0.03)
        raise RehearsalError(f"publisher acknowledgement timeout for G{lease.generation}")
    finally:
        await _close_connection_bounded(connection)


async def _identity_multiset(
    instance: InstanceRef,
    scenario: str,
    identities: Sequence[StableIdentity],
) -> IdentityEvidence:
    """Compare expected and stored stable identity multisets bidirectionally.

    Args:
        instance: Verified private PostgreSQL instance.
        scenario: Scenario label for evidence.
        identities: External writer-expected multiset.

    Returns:
        Counts and samples from both EXCEPT ALL differences.
    """
    _assert(len(identities) > 0, f"identity oracle for {scenario} has no expected rows")
    trade_pattern = "r-%" if scenario == "all_writers" else f"{_writer_trade_prefix(scenario)}%"
    connection = await _connect(
        instance,
        f"identity-oracle-{scenario}",
        command_timeout=_IDENTITY_ORACLE_TIMEOUT_SECONDS,
    )
    try:
        legacy_detached = cast(
            bool,
            await connection.fetchval("""
                SELECT
                    to_regclass('trades_legacy') IS NOT NULL
                    AND NOT EXISTS (
                        SELECT 1
                        FROM pg_inherits
                        WHERE inhrelid = to_regclass('trades_legacy')
                          AND inhparent = to_regclass('trades')
                    )
                """),
        )
        legacy_union = (
            """
            UNION ALL
            SELECT public_id, instrument_public_id, trade_id, executed_at
            FROM trades_legacy
        """
            if legacy_detached
            else ""
        )
        restore_exists = cast(
            bool,
            await connection.fetchval("SELECT to_regclass('rehearsal.trades_restore') IS NOT NULL"),
        )
        restore_union = (
            """
            UNION ALL
            SELECT public_id, instrument_public_id, trade_id, executed_at
            FROM rehearsal.trades_restore
            """
            if restore_exists
            else ""
        )
        await connection.execute(f"""
            CREATE TEMP VIEW rehearsal_stored_identity AS
            SELECT public_id, instrument_public_id, trade_id, executed_at
            FROM trades
            {legacy_union}
            {restore_union}
            """)
        await connection.execute("TRUNCATE rehearsal.expected_identity")
        await connection.copy_records_to_table(
            "expected_identity",
            schema_name="rehearsal",
            records=[identity.copy_record() for identity in identities],
            columns=(
                "public_id",
                "instrument_public_id",
                "trade_id",
                "executed_at",
            ),
        )
        missing_rows = await connection.fetch(
            """
            SELECT public_id::text, instrument_public_id::text, trade_id, executed_at
            FROM (
                SELECT public_id, instrument_public_id, trade_id, executed_at
                FROM rehearsal.expected_identity
                EXCEPT ALL
                SELECT public_id, instrument_public_id, trade_id, executed_at
                FROM rehearsal_stored_identity stored
                WHERE stored.trade_id LIKE $1
            ) difference
            ORDER BY public_id
            LIMIT 10
            """,
            trade_pattern,
        )
        extra_rows = await connection.fetch(
            """
            SELECT public_id::text, instrument_public_id::text, trade_id, executed_at
            FROM (
                SELECT public_id, instrument_public_id, trade_id, executed_at
                FROM rehearsal_stored_identity stored
                WHERE stored.trade_id LIKE $1
                EXCEPT ALL
                SELECT public_id, instrument_public_id, trade_id, executed_at
                FROM rehearsal.expected_identity
            ) difference
            ORDER BY public_id
            LIMIT 10
            """,
            trade_pattern,
        )
        missing = cast(
            int,
            await connection.fetchval(
                """
                SELECT count(*)::bigint
                FROM (
                    SELECT public_id, instrument_public_id, trade_id, executed_at
                    FROM rehearsal.expected_identity
                    EXCEPT ALL
                    SELECT public_id, instrument_public_id, trade_id, executed_at
                    FROM rehearsal_stored_identity stored
                    WHERE stored.trade_id LIKE $1
                ) difference
                """,
                trade_pattern,
            ),
        )
        extra = cast(
            int,
            await connection.fetchval(
                """
                SELECT count(*)::bigint
                FROM (
                    SELECT public_id, instrument_public_id, trade_id, executed_at
                    FROM rehearsal_stored_identity stored
                    WHERE stored.trade_id LIKE $1
                    EXCEPT ALL
                    SELECT public_id, instrument_public_id, trade_id, executed_at
                    FROM rehearsal.expected_identity
                ) difference
                """,
                trade_pattern,
            ),
        )
        return IdentityEvidence(
            scenario=scenario,
            expected=len(identities),
            missing=missing,
            extra_or_changed=extra,
            missing_samples=tuple(_identity_sample(row) for row in missing_rows),
            extra_samples=tuple(_identity_sample(row) for row in extra_rows),
        )
    finally:
        await _close_connection_bounded(connection)


async def _fixture_identity_multiset(instance: InstanceRef) -> IdentityEvidence:
    """Compare all intended fixture identities with the adopted legacy rows.

    Args:
        instance: Verified private PostgreSQL instance in coherent POST state.

    Returns:
        Exact bidirectional EXCEPT ALL evidence for the scaled legacy fixture.
    """
    connection = await _connect(
        instance,
        "identity-oracle-fixture",
        command_timeout=_IDENTITY_ORACLE_TIMEOUT_SECONDS,
    )
    try:
        expected = cast(
            int,
            await connection.fetchval("SELECT count(*)::bigint FROM rehearsal.fixture_identity"),
        )
        restore_exists = cast(
            bool,
            await connection.fetchval("SELECT to_regclass('rehearsal.trades_restore') IS NOT NULL"),
        )
        restore_union = (
            """
            UNION ALL
            SELECT public_id, instrument_public_id, trade_id, executed_at
            FROM rehearsal.trades_restore
            WHERE trade_id LIKE 'fixture-%'
            """
            if restore_exists
            else ""
        )
        await connection.execute(f"""
            CREATE TEMP VIEW rehearsal_fixture_stored AS
            SELECT public_id, instrument_public_id, trade_id, executed_at
            FROM trades
            WHERE trade_id LIKE 'fixture-%'
            {restore_union}
            """)
        missing = cast(
            int,
            await connection.fetchval("""
                SELECT count(*)::bigint
                FROM (
                    SELECT public_id, instrument_public_id, trade_id, executed_at
                    FROM rehearsal.fixture_identity
                    EXCEPT ALL
                    SELECT public_id, instrument_public_id, trade_id, executed_at
                    FROM rehearsal_fixture_stored
                ) difference
                """),
        )
        extra = cast(
            int,
            await connection.fetchval("""
                SELECT count(*)::bigint
                FROM (
                    SELECT public_id, instrument_public_id, trade_id, executed_at
                    FROM rehearsal_fixture_stored
                    EXCEPT ALL
                    SELECT public_id, instrument_public_id, trade_id, executed_at
                    FROM rehearsal.fixture_identity
                ) difference
                """),
        )
        missing_rows: Sequence[Record] = ()
        extra_rows: Sequence[Record] = ()
        if missing:
            missing_rows = await connection.fetch("""
                SELECT public_id::text, instrument_public_id::text, trade_id, executed_at
                FROM (
                    SELECT public_id, instrument_public_id, trade_id, executed_at
                    FROM rehearsal.fixture_identity
                    EXCEPT ALL
                    SELECT public_id, instrument_public_id, trade_id, executed_at
                    FROM rehearsal_fixture_stored
                ) difference
                ORDER BY public_id
                LIMIT 10
                """)
        if extra:
            extra_rows = await connection.fetch("""
                SELECT public_id::text, instrument_public_id::text, trade_id, executed_at
                FROM (
                    SELECT public_id, instrument_public_id, trade_id, executed_at
                    FROM rehearsal_fixture_stored
                    EXCEPT ALL
                    SELECT public_id, instrument_public_id, trade_id, executed_at
                    FROM rehearsal.fixture_identity
                ) difference
                ORDER BY public_id
                LIMIT 10
                """)
        return IdentityEvidence(
            scenario="fixture",
            expected=expected,
            missing=missing,
            extra_or_changed=extra,
            missing_samples=tuple(_identity_sample(row) for row in missing_rows),
            extra_samples=tuple(_identity_sample(row) for row in extra_rows),
        )
    finally:
        await _close_connection_bounded(connection)


def _ledger_identities(paths: RuntimePaths) -> list[StableIdentity]:
    """Load the durable all-scenario writer multiset from external JSONL files.

    Args:
        paths: Run paths containing publisher-exclusive identity journals.

    Returns:
        Every identity durably recorded before a database insert attempt.
    """
    identities: list[StableIdentity] = []
    for ledger in sorted(paths.identities_dir.glob("*.jsonl")):
        for line in ledger.read_text(encoding="utf-8").splitlines():
            parsed: object = json.loads(line)
            _assert(isinstance(parsed, dict), f"identity ledger row is not an object: {ledger}")
            row = cast(dict[str, object], parsed)
            identities.append(
                StableIdentity(
                    public_id=UUID(cast(str, row["public_id"])),
                    instrument_public_id=UUID(cast(str, row["instrument_public_id"])),
                    trade_id=cast(str, row["trade_id"]),
                    executed_at=datetime.fromisoformat(cast(str, row["executed_at"])),
                )
            )
    _assert(bool(identities), "all-writer identity journals are empty")
    return identities


def _identity_sample(row: Record) -> str:
    """Render one identity difference sample.

    Args:
        row: Four-column difference row.

    Returns:
        Compact stable evidence string.
    """
    return "|".join(
        (
            cast(str, row["public_id"]),
            cast(str, row["instrument_public_id"]),
            cast(str, row["trade_id"]),
            cast(datetime, row["executed_at"]).isoformat(),
        )
    )


async def _stop_publishers(
    tasks: Sequence[asyncio.Task[None]],
    stop_event: asyncio.Event,
) -> None:
    """Request publisher stop and propagate every publisher exception.

    Args:
        tasks: Publisher tasks.
        stop_event: Shared clean-stop signal.

    Returns:
        None after every publisher closes.
    """
    stop_event.set()
    failures = await _bounded_publisher_results(tasks)
    if failures:
        raise RehearsalError(
            "publisher task failures: "
            + "; ".join(f"{type(item).__name__}: {item}" for item in failures)
        )


async def _bounded_publisher_results(
    tasks: Sequence[asyncio.Task[None]],
) -> list[BaseException]:
    """Collect publisher tasks, cancelling stragglers within fixed bounds.

    Args:
        tasks: Publisher tasks that own private database connections.

    Returns:
        Exceptions raised by tasks that completed within the bound.
    """
    if not tasks:
        return []
    done, pending = await asyncio.wait(tasks, timeout=_PUBLISHER_STOP_TIMEOUT_SECONDS)
    timed_out = bool(pending)
    for task in pending:
        task.cancel()
    if pending:
        cancelled, pending = await asyncio.wait(pending, timeout=_TASK_CANCEL_TIMEOUT_SECONDS)
        done.update(cancelled)
    if pending:
        identifiers = sorted(id(task) for task in pending)
        raise RehearsalError(f"publisher tasks resisted bounded cancellation: {identifiers}")
    if timed_out:
        raise RehearsalError("publisher tasks exceeded the bounded stop interval")
    failures: list[BaseException] = []
    for task in done:
        if task.cancelled():
            failures.append(RehearsalError("publisher task cancelled unexpectedly"))
        elif task.exception() is not None:
            failures.append(cast(BaseException, task.exception()))
    return failures


async def _settle_publishers(
    tasks: Sequence[asyncio.Task[None]],
    stop_event: asyncio.Event,
) -> None:
    """Retrieve every publisher result during success or exception cleanup.

    Args:
        tasks: Publisher tasks that may already be complete.
        stop_event: Shared clean-stop signal.

    Returns:
        None after every result or exception has been retrieved.
    """
    stop_event.set()
    await _bounded_publisher_results(tasks)


def _writer_contexts(
    instance: InstanceRef,
    config: RunConfig,
    paths: RuntimePaths,
    horizon: datetime,
    scenario: str,
) -> tuple[list[WriterContext], asyncio.Event]:
    """Build independent publisher contexts and external ledgers.

    Args:
        instance: Verified private PostgreSQL instance.
        config: Writer count and rate.
        paths: Exact evidence paths.
        horizon: Routing boundary.
        scenario: Unique scenario label.

    Returns:
        Publisher contexts and their shared stop event.
    """
    stop_event = asyncio.Event()
    contexts = [
        WriterContext(
            instance=instance,
            publisher_id=f"publisher-{number}",
            scenario=scenario,
            horizon=horizon,
            writer_count=config.writers,
            writer_rate=config.writer_rate,
            ledger_path=paths.identities_dir / f"{scenario}-publisher-{number}.jsonl",
            stop_event=stop_event,
            stats=WriterStats(),
        )
        for number in range(1, config.writers + 1)
    ]
    return contexts, stop_event


async def _constraint_validated(connection: Connection) -> bool:
    """Return the validation state of the legacy range constraint.

    Args:
        connection: Verified throwaway connection.

    Returns:
        True only when PostgreSQL marks the range constraint validated.
    """
    value = await connection.fetchval("""
        SELECT convalidated
        FROM pg_constraint
        WHERE conrelid = 'trades'::regclass
          AND conname = 'ck_trades_legacy_range'
        """)
    return cast(bool, value)


async def _one_implication_negative_control(
    connection: Connection,
    horizon: datetime,
    constraint_name: str,
    expression: str,
) -> dict[str, object]:
    """Run one temporary CHECK that must fail the ATTACH implication proof.

    Args:
        connection: Private DDL connection with an empty temporary parent.
        horizon: Exact partition upper bound.
        constraint_name: Harness-only temporary CHECK name.
        expression: Deliberately insufficient or mismatched CHECK expression.

    Returns:
        Timing, DEBUG absence, and exact index-identity evidence.
    """
    messages: list[str] = []

    def listener(_connection: Connection, message: PostgresLogMessage) -> None:
        messages.append(str(message))

    connection.add_log_listener(listener)
    before = await _legacy_index_oids(connection, "trades")
    transaction = connection.transaction()
    await transaction.start()
    during: tuple[int, ...] = ()
    attach_seconds = 0.0
    try:
        await connection.execute("SET LOCAL timezone = 'UTC'")
        await connection.execute("SET LOCAL client_min_messages = 'debug1'")
        await connection.execute("ALTER TABLE trades DROP CONSTRAINT ck_trades_legacy_range")
        await connection.execute(f"""
            ALTER TABLE trades
            ADD CONSTRAINT {constraint_name}
            CHECK ({expression})
            NOT VALID
            """)
        await connection.execute(f"ALTER TABLE trades VALIDATE CONSTRAINT {constraint_name}")
        messages.clear()
        started = time.perf_counter()
        await connection.execute(f"""
            ALTER TABLE trades_implication_parent
            ATTACH PARTITION trades
            FOR VALUES FROM (MINVALUE) TO ({_horizon_literal(horizon)})
            """)
        attach_seconds = time.perf_counter() - started
        during = await _legacy_index_oids(connection, "trades")
    finally:
        await transaction.rollback()
    after = await _legacy_index_oids(connection, "trades")
    expected_debug = 'partition constraint for table "trades" is implied by existing constraints'
    debug_observed = any(expected_debug in message for message in messages)
    verification_observed = any('verifying table "trades"' in message for message in messages)
    _assert(not debug_observed, f"{constraint_name} unexpectedly implied the partition bound")
    _assert(
        verification_observed,
        f"{constraint_name} did not emit PostgreSQL table-scan verification DEBUG",
    )
    _assert(during == before and after == before, f"{constraint_name} changed child indexes")
    _assert(
        await _constraint_validated(connection),
        f"{constraint_name} rollback did not restore the strong validated CHECK",
    )
    return {
        "attach_seconds": attach_seconds,
        "child_index_oids_after": list(after),
        "child_index_oids_before": list(before),
        "child_index_oids_during": list(during),
        "debug_implication_observed": debug_observed,
        "debug_implication_message": expected_debug,
        "scan_verification_observed": verification_observed,
        "temporary_check": expression,
    }


async def _bare_date_attach_negative_control(
    connection: Connection,
    horizon: datetime,
) -> dict[str, object]:
    """Require a non-UTC bare-date partition bound to reject the scaled child.

    Args:
        connection: Private DDL connection in its deliberate non-UTC default.
        horizon: UTC horizon whose bare date resolves to a different instant.

    Returns:
        SQLSTATE, message, session timezone, and unchanged index identities.
    """
    session_timezone = cast(str, await connection.fetchval("SHOW timezone"))
    _assert(session_timezone == _NON_UTC_TIMEZONE, "bare-date ATTACH session was not non-UTC")
    before = await _legacy_index_oids(connection, "trades")
    transaction = connection.transaction()
    await transaction.start()
    sqlstate = ""
    message = ""
    try:
        try:
            await connection.execute(f"""
                ALTER TABLE trades_implication_parent
                ATTACH PARTITION trades
                FOR VALUES FROM (MINVALUE) TO ('{horizon.date().isoformat()}'::date)
                """)
        except asyncpg.PostgresError as exc:
            sqlstate = exc.sqlstate or ""
            message = str(exc)
    finally:
        await transaction.rollback()
    after = await _legacy_index_oids(connection, "trades")
    _assert(sqlstate == "23514", f"bare-date ATTACH failed with unexpected SQLSTATE {sqlstate}")
    _assert(
        "partition constraint" in message.lower(),
        "bare-date ATTACH did not fail on the parsed partition bound",
    )
    _assert(after == before, "bare-date ATTACH failure changed child index identities")
    return {
        "child_index_oids_after": list(after),
        "child_index_oids_before": list(before),
        "error": message,
        "session_timezone": session_timezone,
        "sqlstate": sqlstate,
        "unsafe_literal_confined_to_negative_control": True,
    }


async def _implication_negative_controls(
    connection: Connection,
    horizon: datetime,
) -> dict[str, object]:
    """Prove missing non-null and mismatched-bound CHECKs force ATTACH scans.

    Args:
        connection: Private DDL connection after strong CHECK validation.
        horizon: Exact UTC partition upper bound.

    Returns:
        Two scaled negative controls that lack PostgreSQL implication DEBUG.
    """
    session_timezone = cast(str, await connection.fetchval("SHOW timezone"))
    _assert(
        session_timezone == _NON_UTC_TIMEZONE,
        "bare-date negative control did not begin in the non-UTC session",
    )
    bare_date_instant = cast(
        datetime,
        await connection.fetchval(
            "SELECT $1::date::timestamp with time zone",
            horizon.date(),
        ),
    )
    _assert(
        bare_date_instant != horizon,
        "non-UTC bare date unexpectedly represented the UTC horizon",
    )
    parent_sql = _parent_table_sql().replace("trades_parent", "trades_implication_parent")
    parent_sql = parent_sql.replace(
        "executed_at timestamp with time zone NOT NULL",
        "executed_at timestamp with time zone",
    )
    await _execute_utc_ddl(connection, parent_sql)
    await _execute_utc_ddl(
        connection,
        """
        CREATE UNIQUE INDEX trades_implication_parent_uq
        ON trades_implication_parent (instrument_public_id, trade_id, executed_at)
        """,
    )
    horizon_sql = _horizon_literal(horizon)
    missing_non_null = await _one_implication_negative_control(
        connection,
        horizon,
        "ck_rehearsal_missing_non_null",
        f"executed_at < {horizon_sql}",
    )
    mismatch_sql = f"TIMESTAMPTZ '{horizon.strftime('%Y-%m-%d %H:%M:%S')}-01:00'"
    mismatch_instant = datetime.fromisoformat(
        mismatch_sql.removeprefix("TIMESTAMPTZ '").removesuffix("'").replace(" ", "T")
    ).astimezone(UTC)
    _assert(
        mismatch_instant == horizon + timedelta(hours=1),
        "mismatched-offset negative control did not move the H instant",
    )
    timezone_mismatch = await _one_implication_negative_control(
        connection,
        horizon,
        "ck_rehearsal_mismatched_horizon",
        f"executed_at IS NOT NULL AND executed_at < {mismatch_sql}",
    )
    timezone_mismatch["check_literal"] = mismatch_sql
    timezone_mismatch["check_instant"] = mismatch_instant.isoformat()
    timezone_mismatch["partition_bound_literal"] = horizon_sql
    bare_date_attach = await _bare_date_attach_negative_control(
        connection,
        horizon,
    )
    await _execute_utc_ddl(connection, "DROP TABLE trades_implication_parent")
    return {
        "bare_date_parse": {
            "bare_date_instant": bare_date_instant.isoformat(),
            "explicit_utc_horizon": horizon.isoformat(),
            "same_instant": bare_date_instant == horizon,
            "session_timezone": session_timezone,
        },
        "bare_date_attach": bare_date_attach,
        "missing_is_not_null": missing_non_null,
        "timezone_instant_mismatch": timezone_mismatch,
    }


async def _prove_set_not_null_skip(connection: Connection) -> tuple[float, float, bool]:
    """Contrast a forced NOT NULL scan with the validated-CHECK fast path.

    Args:
        connection: Verified throwaway DDL connection after CHECK validation.

    Returns:
        Fast-path seconds, scan-control seconds, and scan-skip proof result.
    """
    messages: list[str] = []

    def listener(_connection: Connection, message: PostgresLogMessage) -> None:
        messages.append(str(message))

    connection.add_log_listener(listener)
    control = connection.transaction()
    await control.start()
    try:
        await connection.execute("SET LOCAL timezone = 'UTC'")
        await connection.execute("SET LOCAL client_min_messages = 'debug1'")
        await connection.execute("ALTER TABLE trades ALTER COLUMN known_to DROP NOT NULL")
        messages.clear()
        control_started = time.perf_counter()
        await connection.execute("ALTER TABLE trades ALTER COLUMN known_to SET NOT NULL")
        control_seconds = time.perf_counter() - control_started
        control_verified = any('verifying table "trades"' in message for message in messages)
    finally:
        await control.rollback()
    messages.clear()
    fast = connection.transaction()
    await fast.start()
    try:
        await connection.execute("SET LOCAL timezone = 'UTC'")
        await connection.execute("SET LOCAL client_min_messages = 'debug1'")
        fast_started = time.perf_counter()
        await connection.execute("ALTER TABLE trades ALTER COLUMN executed_at SET NOT NULL")
        fast_seconds = time.perf_counter() - fast_started
        fast_verified = any('verifying table "trades"' in message for message in messages)
        await fast.commit()
    except Exception:
        await fast.rollback()
        raise
    not_null = cast(
        bool,
        await connection.fetchval("""
            SELECT attnotnull
            FROM pg_attribute
            WHERE attrelid = 'trades'::regclass
              AND attname = 'executed_at'
            """),
    )
    _assert(not_null, "executed_at SET NOT NULL did not take effect")
    skipped = control_verified and not fast_verified and fast_seconds < control_seconds
    _assert(control_verified, "NOT NULL negative control did not emit a table-verification scan")
    _assert(not fast_verified, "validated CHECK did not suppress SET NOT NULL table verification")
    _assert(
        fast_seconds < control_seconds,
        "validated-CHECK SET NOT NULL was not faster than the scan control",
    )
    return fast_seconds, control_seconds, skipped


async def _active_validate_pid(
    observer: Connection,
    backend_pid: int,
    application_name: str,
) -> bool:
    """Recheck that the exact frozen backend remains inside VALIDATE.

    Args:
        observer: Independent pg_stat_activity connection.
        backend_pid: Frozen backend PID.
        application_name: Exact measurement session label.

    Returns:
        True only for the same active VALIDATE backend.
    """
    row = await observer.fetchrow(
        """
        SELECT pid
        FROM pg_stat_activity
        WHERE pid = $1
          AND application_name = $2
          AND state = 'active'
          AND query ILIKE '%VALIDATE CONSTRAINT%'
        """,
        backend_pid,
        application_name,
    )
    return row is not None


async def _cancel_validation_task(task: asyncio.Task[str] | None) -> None:
    """Cancel one unfinished VALIDATE client task within a fixed bound.

    Args:
        task: Optional asyncpg execute task.

    Returns:
        None after no unfinished client task remains.
    """
    if task is None or task.done():
        return
    task.cancel()
    with contextlib.suppress(asyncio.CancelledError, TimeoutError):
        await asyncio.wait_for(task, timeout=_TASK_CANCEL_TIMEOUT_SECONDS)


async def _measure_validate_statement(
    connection: Connection,
    instance: InstanceRef,
    stats: Sequence[WriterStats],
) -> ValidationStatementEvidence:
    """Measure exact VALIDATE while a writer commits against a frozen active PID.

    Args:
        connection: Verified throwaway measurement connection.
        instance: Private postmaster used to verify and pause the exact backend.
        stats: Active publisher counters sampled around the heap validation.

    Returns:
        Shape, net timing, raw timing, backend, and overlap evidence.
    """
    _assert(
        await connection.fetchval("SHOW timezone") == _NON_UTC_TIMEZONE,
        "VALIDATE session was not non-UTC",
    )
    application_name = cast(
        str,
        await connection.fetchval("SELECT current_setting('application_name')"),
    )
    backend_pid = cast(int, await connection.fetchval("SELECT pg_backend_pid()"))
    observer = await _connect(instance, "validate-measure-observer")
    shape: Record | None = None
    validation_task: asyncio.Task[str] | None = None
    backend_frozen = False
    controlled_pause_seconds = 0.0
    raw_seconds = 0.0
    active_recheck = False
    commits_during = 0
    try:
        async with connection.transaction(isolation="repeatable_read"):
            await connection.execute("SET LOCAL timezone = 'UTC'")
            pinned_timezone = await connection.fetchval("SHOW timezone")
            _assert(pinned_timezone == "UTC", "measured VALIDATE failed to pin UTC")
            shape = await connection.fetchrow("""
                SELECT
                    count(*)::bigint AS rows,
                    pg_relation_size('trades'::regclass)::bigint AS heap_bytes
                FROM trades
                """)
            _assert(shape is not None, "VALIDATE snapshot shape query returned no row")
            commits_before = sum(item.committed for item in stats)
            started = time.perf_counter()
            validation_task = asyncio.create_task(
                connection.execute("ALTER TABLE trades VALIDATE CONSTRAINT ck_trades_legacy_range")
            )
            observed = await _wait_backend(
                observer,
                application_name,
                "VALIDATE CONSTRAINT",
            )
            _assert(
                cast(int, observed["pid"]) == backend_pid,
                "measured VALIDATE observer found a different backend PID",
            )
            _assert(
                observed["wait_event_type"] != "Lock",
                "measured VALIDATE was waiting for its table lock instead of scanning",
            )
            pause_started = time.perf_counter()
            stopped_state = await _freeze_private_backend(instance, backend_pid)
            backend_frozen = True
            _assert(stopped_state == "T", "measured VALIDATE backend was not stopped")
            target = max(item.committed for item in stats) + 1
            await _wait_writer_commits(stats, target)
            commits_during = sum(item.committed for item in stats) - commits_before
            active_recheck = await _active_validate_pid(
                observer,
                backend_pid,
                application_name,
            )
            _assert(
                active_recheck,
                "measured writer overlap did not retain the exact active VALIDATE PID",
            )
            controlled_pause_seconds = time.perf_counter() - pause_started
            await _resume_private_backend(instance, backend_pid)
            backend_frozen = False
            await validation_task
            raw_seconds = time.perf_counter() - started
    finally:
        if backend_frozen:
            await _resume_private_backend(instance, backend_pid)
        await _cancel_validation_task(validation_task)
        await _close_connection_bounded(observer)
    seconds = raw_seconds - controlled_pause_seconds
    _assert(seconds > 0, "controlled VALIDATE pause consumed the complete timing interval")
    _assert(await _constraint_validated(connection), "VALIDATE retry did not validate constraint")
    _assert(commits_during > 0, "no writer committed during the measured active VALIDATE")
    _assert(shape is not None, "VALIDATE snapshot evidence was lost")
    return ValidationStatementEvidence(
        shape=shape,
        seconds=seconds,
        raw_seconds=raw_seconds,
        commits_during=commits_during,
        backend_pid=backend_pid,
        controlled_pause_seconds=controlled_pause_seconds,
        active_recheck=active_recheck,
    )


async def _retry_validate_and_measure(
    connection: Connection,
    instance: InstanceRef,
    stats: Sequence[WriterStats],
    horizon: datetime,
) -> ValidationMetrics:
    """Retry VALIDATE, tighten NOT NULL, and compute both production ETAs.

    Args:
        connection: Verified throwaway measurement connection.
        instance: Private postmaster used to verify and pause the exact backend.
        stats: Active publisher counters sampled around the heap validation.
        horizon: Exact UTC legacy upper bound for implication controls.

    Returns:
        Timed row/heap throughput and conservative production extrapolation.
    """
    measurement = await _measure_validate_statement(connection, instance, stats)
    implication_controls = await _implication_negative_controls(connection, horizon)
    set_not_null_seconds, scan_control_seconds, scan_skipped = await _prove_set_not_null_skip(
        connection
    )
    fixture_rows = cast(int, measurement.shape["rows"])
    heap_bytes = cast(int, measurement.shape["heap_bytes"])
    rows_per_second = fixture_rows / measurement.seconds
    bytes_per_second = heap_bytes / measurement.seconds
    production_rows_eta = 377_000_000 / rows_per_second
    production_heap_eta = (59 * 1024**3) / bytes_per_second
    return ValidationMetrics(
        fixture_rows=fixture_rows,
        heap_bytes=heap_bytes,
        seconds=measurement.seconds,
        raw_seconds=measurement.raw_seconds,
        rows_per_second=rows_per_second,
        bytes_per_second=bytes_per_second,
        production_rows_eta_seconds=production_rows_eta,
        production_heap_eta_seconds=production_heap_eta,
        conservative_eta_seconds=max(production_rows_eta, production_heap_eta),
        writer_commits_during=measurement.commits_during,
        writer_overlap_backend_pid=measurement.backend_pid,
        controlled_pause_seconds=measurement.controlled_pause_seconds,
        active_recheck=measurement.active_recheck,
        cache_condition="warm post-load filesystem cache",
        set_not_null_seconds=set_not_null_seconds,
        set_not_null_scan_control_seconds=scan_control_seconds,
        set_not_null_scan_skipped=scan_skipped,
        implication_negative_controls=implication_controls,
    )


async def _wait_validate_writer_overlap(
    monitor: Connection,
    backend_pid: int,
    instance: InstanceRef,
    paths: RuntimePaths,
    horizon: datetime,
) -> tuple[Record, WriterStats, float]:
    """Commit one journaled write while the exact VALIDATE PID remains active.

    Args:
        monitor: Independent pg_stat_activity observer.
        backend_pid: Exact VALIDATE backend PID.
        instance: Verified private PostgreSQL instance.
        paths: External identity-ledger directory.
        horizon: Boundary used to construct one ordinary targetless row.

    Returns:
        Final active recheck, writer evidence, and commit latency.
    """
    stats = WriterStats()
    context = WriterContext(
        instance=instance,
        publisher_id="publisher-99",
        scenario="validate_kill",
        horizon=horizon,
        writer_count=1,
        writer_rate=1.0,
        ledger_path=paths.identities_dir / "validate_kill-overlap-publisher-99.jsonl",
        stop_event=asyncio.Event(),
        stats=stats,
    )
    rows = _writer_rows(context, "ORDINARY_TARGETLESS", 1, 1)
    connection = await _connect(instance, "validate-overlap-writer")
    try:
        with context.ledger_path.open("a", encoding="utf-8") as ledger:
            _append_expected(ledger, rows, stats)
        started = time.perf_counter()
        await _insert_writer_batch(connection, rows, stats, "ORDINARY_TARGETLESS")
        commit_seconds = time.perf_counter() - started
        final_active = await monitor.fetchrow(
            """
            SELECT pid, state, query, wait_event_type, wait_event, backend_start
            FROM pg_stat_activity
            WHERE application_name = 'rehearsal-controller-validate'
              AND pid = $1
              AND state = 'active'
              AND query ILIKE '%VALIDATE CONSTRAINT%'
              AND wait_event_type IS DISTINCT FROM 'Lock'
            """,
            backend_pid,
        )
    finally:
        await _close_connection_bounded(connection)
    _assert(
        final_active is not None,
        "journaled writer did not commit while the same VALIDATE PID was active",
    )
    _assert(stats.committed == 1, "VALIDATE overlap writer did not commit exactly one row")
    return final_active, stats, commit_seconds


def _validate_kill_passed(
    evidence: dict[str, object],
    identity: IdentityEvidence,
) -> bool:
    """Bind the VALIDATE kill row to the exact stopped active backend.

    Args:
        evidence: Independently observed kill and constraint facts.
        identity: Bidirectional writer multiset result after retry.

    Returns:
        True only when SIGKILL occurred while the same VALIDATE PID was frozen.
    """
    backend_pid = int(cast(int, evidence.get("observed_backend_pid", 0)))
    return (
        int(cast(int, evidence.get("controller_returncode", 0))) == -signal.SIGKILL
        and backend_pid > 1
        and int(cast(int, evidence.get("final_active_backend_pid", 0))) == backend_pid
        and cast(str, evidence.get("observed_backend_process_state", "")) == "T"
        and cast(bool, evidence.get("controller_killed_while_backend_stopped", False))
        and cast(bool, evidence.get("final_active_recheck", False))
        and cast(str, evidence.get("constraint_after_kill", "")) == "NOT VALID"
        and cast(str, evidence.get("constraint_after_retry", "")) == "VALID"
        and "VALIDATE CONSTRAINT" in cast(str, evidence.get("observed_query", ""))
        and int(cast(int, evidence.get("writer_commits_during_active_validate", 0))) > 0
        and identity.passed()
    )


async def _run_validate_kill(
    instance: InstanceRef,
    config: RunConfig,
    paths: RuntimePaths,
    horizon: datetime,
) -> tuple[KillResult, ValidationMetrics, IdentityEvidence, list[WriterStats]]:
    """Kill a controller during active VALIDATE, then retry and measure it.

    Args:
        instance: Verified private PostgreSQL instance.
        config: Writer and timeout bounds.
        paths: Controller event and identity evidence paths.
        horizon: Legacy upper bound.

    Returns:
        Kill result, throughput metrics, identity proof, and writer counters.
    """
    setup = await _connect(instance, "validate-setup")
    try:
        await _execute_utc_ddl(setup, _range_constraint_sql(horizon))
    finally:
        await _close_connection_bounded(setup)
    contexts, stop_event = _writer_contexts(
        instance,
        config,
        paths,
        horizon,
        "validate_kill",
    )
    tasks = [asyncio.create_task(_publisher_loop(context)) for context in contexts]
    stats = [context.stats for context in contexts]
    monitor = await _connect(instance, "validate-monitor")
    process: asyncio.subprocess.Process | None = None
    writes_during = 0
    overlap_commit_seconds = 0.0
    observed: Record | None = None
    backend_frozen = False
    try:
        await _wait_writer_commits(stats, 8)
        spec = ControllerSpec(
            instance=instance,
            scenario="validate",
            generation=0,
            token=uuid4(),
            horizon=horizon,
            writers=config.writers,
            transaction_timeout_seconds=config.transaction_timeout_seconds,
            cleanup_margin_seconds=config.cleanup_margin_seconds,
            events_dir=paths.events_dir,
        )
        process = await _start_controller(spec)
        observed = await _wait_backend(
            monitor,
            "rehearsal-controller-validate",
            "VALIDATE CONSTRAINT",
        )
        _assert(
            observed["wait_event_type"] != "Lock",
            "VALIDATE kill observation occurred during lock acquisition, not heap scanning",
        )
        backend_pid = cast(int, observed["pid"])
        stopped_state = await _freeze_private_backend(instance, backend_pid)
        backend_frozen = True
        final_active, overlap_stats, overlap_commit_seconds = await _wait_validate_writer_overlap(
            monitor,
            backend_pid,
            instance,
            paths,
            horizon,
        )
        writes_during = overlap_stats.committed
        returncode, stdout, stderr = await _kill_controller(process)
        controller_killed_while_stopped = (
            _private_backend_process_state(instance, backend_pid) == "T"
        )
        await _resume_private_backend(instance, backend_pid)
        backend_frozen = False
        await _wait_backend_gone(monitor, "rehearsal-controller-validate")
        _assert(not await _constraint_validated(monitor), "killed VALIDATE committed unexpectedly")
        metrics = await _retry_validate_and_measure(
            monitor,
            instance,
            stats,
            horizon,
        )
        await _wait_writer_commits(stats, max(item.committed for item in stats) + 1)
        await _stop_publishers(tasks, stop_event)
        stats.append(overlap_stats)
        identities = [identity for item in stats for identity in item.expected]
        identity = await _identity_multiset(instance, "validate_kill", identities)
        _assert(identity.passed(), "identity multiset failed after killed VALIDATE")
        kill_evidence: dict[str, object] = {
            "constraint_after_kill": "NOT VALID",
            "constraint_after_retry": "VALID",
            "controller_killed_while_backend_stopped": controller_killed_while_stopped,
            "controller_returncode": returncode,
            "controller_stderr": stderr.strip(),
            "controller_stdout": stdout.strip(),
            "final_active_backend_pid": cast(int, final_active["pid"]),
            "final_active_recheck": final_active is not None,
            "observed_backend_pid": backend_pid,
            "observed_backend_process_state": stopped_state,
            "observed_query": cast(str, observed["query"]),
            "observed_wait_event": cast(str | None, observed["wait_event"]),
            "observed_wait_event_type": cast(str | None, observed["wait_event_type"]),
            "overlap_writer_commit_seconds": overlap_commit_seconds,
            "writer_commits_during_active_validate": writes_during,
        }
        passed = _validate_kill_passed(kill_evidence, identity)
        return (
            KillResult(
                name="during VALIDATE",
                passed=passed,
                evidence=kill_evidence,
            ),
            metrics,
            identity,
            stats,
        )
    finally:
        if backend_frozen and observed is not None:
            await _resume_private_backend(instance, cast(int, observed["pid"]))
        if process is not None and process.returncode is None:
            await _kill_process_bounded(process)
        await _settle_publishers(tasks, stop_event)
        await _close_connection_bounded(monitor)


async def _lock_fence_authority(
    connection: Connection,
    generation: int,
) -> Record:
    """Lock one generation before any database-clock authority measurement.

    Args:
        connection: Connection already inside the authority transaction.
        generation: Exact durable fence generation to serialize.

    Returns:
        Locked fence row containing its absolute expiry.
    """
    row = await connection.fetchrow(
        """
        SELECT
            generation,
            token,
            lease_expires_at,
            transaction_timeout_ms,
            cleanup_margin_ms,
            resolution
        FROM rehearsal.fence_generations
        WHERE generation = $1
        FOR UPDATE
        """,
        generation,
    )
    _assert(row is not None, "fence generation disappeared while acquiring authority")
    return row


async def _remaining_lease_seconds(
    connection: Connection,
    lease_expires_at: datetime,
) -> float:
    """Measure one locked lease against a fresh database clock read.

    Args:
        connection: Authority transaction already holding the generation row lock.
        lease_expires_at: Absolute expiry fetched from that locked generation.

    Returns:
        Database-clock seconds remaining at this post-lock statement.
    """
    return cast(
        float,
        await connection.fetchval(
            """
            SELECT EXTRACT(
                EPOCH FROM ($1::timestamptz - clock_timestamp())
            )::double precision
            """,
            lease_expires_at,
        ),
    )


async def _authorize_controller(
    connection: Connection,
    spec: ControllerSpec,
) -> tuple[int, float, str, str]:
    """Acquire generation authority and derive timeout inside the DDL txn.

    Args:
        connection: Controller DDL connection inside BEGIN.
        spec: Exact generation, token, and margin bounds.

    Returns:
        Backend PID, remaining lease, initial timezone, and pinned timezone.
    """
    initial_timezone = cast(str, await connection.fetchval("SHOW timezone"))
    _assert(
        initial_timezone == _NON_UTC_TIMEZONE,
        "atomic DDL transaction did not begin in the deliberate non-UTC timezone",
    )
    await connection.execute("SET LOCAL timezone = 'UTC'")
    pinned_timezone = cast(str, await connection.fetchval("SHOW timezone"))
    _assert(pinned_timezone == "UTC", "atomic DDL transaction failed to pin UTC")
    await connection.execute("SET LOCAL client_min_messages = 'debug1'")
    await connection.execute("SET LOCAL lock_timeout = '0'")
    await connection.execute("SET LOCAL statement_timeout = '0'")
    await connection.execute("SET LOCAL idle_in_transaction_session_timeout = '0'")
    row = await _lock_fence_authority(connection, spec.generation)
    _assert(cast(UUID, row["token"]) == spec.token, "controller fence token mismatch")
    _assert(row["resolution"] is None, "controller generation is already terminal")
    ack_count = cast(
        int,
        await connection.fetchval(
            """
            SELECT count(*)::integer
            FROM rehearsal.publisher_acks
            WHERE generation = $1
              AND session_closed
              AND inflight = 0
            """,
            spec.generation,
        ),
    )
    _assert(ack_count == spec.writers, "controller entered DDL without every publisher ack")
    remaining = await _remaining_lease_seconds(
        connection,
        cast(datetime, row["lease_expires_at"]),
    )
    timeout_ms = cast(int, row["transaction_timeout_ms"])
    cleanup_ms = cast(int, row["cleanup_margin_ms"])
    _assert(
        timeout_ms + cleanup_ms < remaining * 1000,
        "remaining database-clock lease cannot authorize transaction_timeout",
    )
    await connection.execute(f"SET LOCAL transaction_timeout = '{timeout_ms}ms'")
    await connection.fetchval(
        "SELECT pg_advisory_xact_lock($1, $2)",
        _ADVISORY_NAMESPACE,
        spec.generation,
    )
    backend_pid = cast(int, await connection.fetchval("SELECT pg_backend_pid()"))
    return backend_pid, remaining, initial_timezone, pinned_timezone


async def _perform_cutover(
    connection: Connection,
    spec: ControllerSpec,
    messages: list[str],
) -> float:
    """Execute the settled rename, ATTACH, sequence, and epoch transaction body.

    Args:
        connection: Authorized DDL transaction.
        spec: H boundary and generation.
        messages: PostgreSQL log messages captured by the session listener.

    Returns:
        ATTACH wall-clock duration in seconds.
    """
    await connection.execute("ALTER TABLE trades RENAME TO trades_legacy")
    await connection.execute("ALTER TABLE trades_parent RENAME TO trades")
    messages.clear()
    started = time.perf_counter()
    await connection.execute(f"""
        ALTER TABLE trades
        ATTACH PARTITION trades_legacy
        FOR VALUES FROM (MINVALUE) TO ({_horizon_literal(spec.horizon)})
        """)
    attach_seconds = time.perf_counter() - started
    await connection.execute("ALTER SEQUENCE trades_id_seq OWNED BY trades.id")
    await connection.execute(
        "UPDATE rehearsal.topology_epoch SET topology = 'PARTITIONED' WHERE singleton"
    )
    return attach_seconds


async def _controller_release(
    instance: InstanceRef,
    spec: ControllerSpec,
    state: CatalogState,
) -> None:
    """Release a successful normal fence from a separate committed transaction.

    Args:
        instance: Verified private PostgreSQL instance.
        spec: Completed generation.
        state: Exact committed POST catalog state.

    Returns:
        None.
    """
    connection = await _connect(instance, f"controller-release-g{spec.generation}")
    try:
        updated = await connection.fetchval(
            """
            UPDATE rehearsal.fence_generations
            SET
                resolution = 'POST_COMMITTED',
                resolver = 'controller',
                resolved_fingerprint = $2,
                resolved_at = clock_timestamp()
            WHERE generation = $1
              AND token = $3
              AND resolution IS NULL
            RETURNING generation
            """,
            spec.generation,
            state.fingerprint(),
            spec.token,
        )
        _assert(updated is not None, "normal controller could not release publisher fence")
    finally:
        await _close_connection_bounded(connection)


async def _run_validate_controller(spec: ControllerSpec) -> int:
    """Run the killable VALIDATE-only controller transaction.

    Args:
        spec: Private instance and transaction-timeout bounds.

    Returns:
        Zero if validation commits before the supervisor kills this process.
    """
    connection = await _connect(spec.instance, "rehearsal-controller-validate")
    try:
        await connection.execute("BEGIN")
        timeout_ms = round(spec.transaction_timeout_seconds * 1000)
        await connection.execute(f"SET LOCAL transaction_timeout = '{timeout_ms}ms'")
        await connection.execute("SET LOCAL timezone = 'UTC'")
        pinned_timezone = await connection.fetchval("SHOW timezone")
        _assert(pinned_timezone == "UTC", "VALIDATE controller failed to pin UTC")
        await connection.execute("ALTER TABLE trades VALIDATE CONSTRAINT ck_trades_legacy_range")
        await connection.execute("COMMIT")
        return 0
    finally:
        await _close_connection_bounded(connection)


async def _run_writer_drop_child(spec: ControllerSpec) -> int:
    """Pre-arm an obligation marker, acknowledge an ACTIVE fence, and park.

    Args:
        spec: Private writer-drop generation and instance coordinates.

    Returns:
        Nonzero only if the supervisor never delivers the required SIGKILL.
    """
    application = f"rehearsal-writer-drop-g{spec.generation}"
    connection = await _connect(spec.instance, application)
    session_id = uuid5(_SESSION_NAMESPACE, f"writer-drop:{spec.generation}")
    try:
        async with connection.transaction():
            fence = await connection.fetchrow(
                """
                SELECT token, resolution, clock_timestamp() < lease_expires_at AS active
                FROM rehearsal.fence_generations
                WHERE generation = $1
                FOR SHARE
                """,
                spec.generation,
            )
            _assert(fence is not None, "writer-drop fence generation disappeared")
            _assert(cast(UUID, fence["token"]) == spec.token, "writer-drop fence token mismatch")
            _assert(fence["resolution"] is None, "writer-drop fence was already resolved")
            _assert(cast(bool, fence["active"]), "writer-drop fence was not ACTIVE")
            await connection.execute(
                """
                INSERT INTO rehearsal.publisher_runtime (
                    session_id,
                    publisher_id,
                    generation,
                    state,
                    held_obligations
                )
                VALUES ($1, 'publisher-kill', $2, 'ACTIVE', 1)
                """,
                session_id,
                spec.generation,
            )
            await connection.execute(
                """
                INSERT INTO rehearsal.publisher_acks (
                    generation,
                    publisher_id,
                    retention_generation,
                    acknowledged_at,
                    session_closed,
                    inflight
                )
                SELECT $1, 'publisher-kill', generation, clock_timestamp(), true, 0
                FROM rehearsal.retention_generation
                WHERE singleton
                """,
                spec.generation,
            )
        _write_event(
            _event_path(spec.events_dir, spec.generation, "writer_active"),
            {
                "application_name": application,
                "generation": spec.generation,
                "held_obligations": 1,
                "session_id": str(session_id),
            },
        )
        await asyncio.sleep(3600)
        return 6
    finally:
        await _close_connection_bounded(connection)


async def _park_after_begin(
    connection: Connection,
    spec: ControllerSpec,
    backend_pid: int,
) -> int:
    """Perform one uncommitted catalog mutation and park for SIGKILL.

    Args:
        connection: Authorized controller transaction.
        spec: Generation and event paths.
        backend_pid: Independently observable DDL backend.

    Returns:
        Nonzero only if the required SIGKILL never arrives.
    """
    await connection.execute("ALTER TABLE trades RENAME TO trades_legacy")
    internal_legacy = await connection.fetchval("SELECT to_regclass('trades_legacy')::text")
    _write_event(
        _event_path(spec.events_dir, spec.generation, "after_begin_mutation"),
        {
            "backend_pid": backend_pid,
            "internal_legacy_relation": internal_legacy,
            "scenario": spec.scenario,
        },
    )
    await asyncio.sleep(3600)
    return 3


def _connection_socket_fd(connection: Connection) -> int:
    """Resolve the exact asyncpg Unix-socket descriptor for orphan simulation.

    Args:
        connection: Authorized private DDL connection.

    Returns:
        Positive live socket descriptor duplicated into the holder process.
    """
    transport = cast(
        _TransportAccess,
        connection._transport,
    )
    socket_value = transport.get_extra_info("socket")
    _assert(socket_value is not None, "asyncpg transport exposed no socket")
    descriptor = cast(_FileDescriptorAccess, socket_value).fileno()
    _assert(descriptor > 2, "asyncpg transport returned an unsafe socket descriptor")
    return descriptor


def _process_start_ticks(process_id: int) -> int:
    """Read one Linux process start identity for PID-reuse protection.

    Args:
        process_id: Exact positive process identifier.

    Returns:
        Kernel start-time ticks from procfs.
    """
    _assert(process_id > 1, "refusing an invalid process identity")
    fields = (Path("/proc") / str(process_id) / "stat").read_text(encoding="utf-8").split()
    _assert(len(fields) > 21, f"process PID {process_id} has an incomplete procfs identity")
    return int(fields[21])


def _orphan_holder_alive(process_id: int, start_ticks: int) -> bool:
    """Return whether the exact socket holder remains a live sleep process.

    Args:
        process_id: Holder PID published before controller death.
        start_ticks: Procfs start identity published with that PID.

    Returns:
        True only for the same live harness-owned sleep process.
    """
    try:
        if _process_start_ticks(process_id) != start_ticks:
            return False
        stat_fields = (Path("/proc") / str(process_id) / "stat").read_text(encoding="utf-8").split()
        command = (Path("/proc") / str(process_id) / "cmdline").read_bytes().replace(b"\0", b" ")
    except OSError:
        return False
    return stat_fields[2] != "Z" and str(_SLEEP).encode() in command


def _start_orphan_socket_holder(
    connection: Connection,
    hold_seconds: float,
) -> tuple[subprocess.Popen[bytes], int]:
    """Duplicate the DDL socket into a passive process that survives controller death.

    Args:
        connection: Authorized DDL connection whose socket must remain open.
        hold_seconds: Upper bound after which the holder exits on its own.

    Returns:
        Owned holder process and its procfs start identity.
    """
    _assert(hold_seconds > 10, "orphan socket hold interval is too short")
    descriptor = _connection_socket_fd(connection)
    holder = subprocess.Popen(
        [
            "nice",
            "-n",
            "19",
            "ionice",
            "-c",
            "3",
            str(_SLEEP),
            f"{hold_seconds:.3f}",
        ],
        stdin=subprocess.DEVNULL,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        pass_fds=(descriptor,),
    )
    _assert(holder.pid > 1, "orphan socket holder has an invalid PID")
    start_ticks = _process_start_ticks(holder.pid)
    _assert(
        _orphan_holder_alive(holder.pid, start_ticks),
        "orphan socket holder did not retain its process identity",
    )
    return holder, start_ticks


def _terminate_popen_bounded(process: subprocess.Popen[bytes]) -> None:
    """Terminate one exact synchronous child within fixed bounds.

    Args:
        process: Holder process created by the current controller.

    Returns:
        None after the process is reaped.
    """
    if process.poll() is not None:
        return
    process.terminate()
    try:
        process.wait(timeout=2)
    except subprocess.TimeoutExpired:
        process.kill()
        try:
            process.wait(timeout=_TASK_CANCEL_TIMEOUT_SECONDS)
        except subprocess.TimeoutExpired as exc:
            raise RehearsalError(
                f"orphan socket holder PID {process.pid} resisted SIGKILL"
            ) from exc


async def _terminate_orphan_holder(process_id: int, start_ticks: int) -> bool:
    """Terminate the exact reparented socket holder after backend timeout.

    Args:
        process_id: PID published by the killed controller.
        start_ticks: Procfs identity protecting against PID reuse.

    Returns:
        True only when the exact holder is no longer live.
    """
    _assert(
        _orphan_holder_alive(process_id, start_ticks),
        "orphan holder identity changed before bounded cleanup",
    )
    os.kill(process_id, signal.SIGTERM)
    deadline = time.monotonic() + 2.0
    while time.monotonic() < deadline:
        if not _orphan_holder_alive(process_id, start_ticks):
            return True
        await asyncio.sleep(0.02)
    os.kill(process_id, signal.SIGKILL)
    deadline = time.monotonic() + _TASK_CANCEL_TIMEOUT_SECONDS
    while time.monotonic() < deadline:
        if not _orphan_holder_alive(process_id, start_ticks):
            return True
        await asyncio.sleep(0.02)
    return False


async def _controller_main(spec: ControllerSpec) -> int:
    """Run one killable controller role.

    Args:
        spec: Scenario, generation, H, and private instance coordinates.

    Returns:
        Zero only for a completed normal controller.
    """
    if spec.scenario == "validate":
        return await _run_validate_controller(spec)
    application_name = f"rehearsal-controller-g{spec.generation}-{spec.scenario}"
    connection = await _connect(spec.instance, application_name)
    messages: list[str] = []
    orphan_holder: subprocess.Popen[bytes] | None = None

    def listener(_connection: Connection, message: PostgresLogMessage) -> None:
        messages.append(str(message))

    connection.add_log_listener(listener)
    try:
        backend_pid = cast(int, await connection.fetchval("SELECT pg_backend_pid()"))
        await connection.execute("SET statement_timeout = '1000ms'")
        ddl_pid_rows = _command_row_count(
            await connection.execute(
                """
            UPDATE rehearsal.fence_generations
            SET ddl_pid = $2
            WHERE generation = $1
              AND token = $3
              AND resolution IS NULL
                """,
                spec.generation,
                backend_pid,
                spec.token,
            )
        )
        await connection.execute("RESET statement_timeout")
        _assert(ddl_pid_rows == 1, "controller could not register its DDL backend")
        await connection.execute("BEGIN")
        timeout_ms = round(spec.transaction_timeout_seconds * 1000)
        await connection.execute(f"SET LOCAL transaction_timeout = '{timeout_ms}ms'")
        authorized_pid, remaining, initial_timezone, pinned_timezone = await _authorize_controller(
            connection, spec
        )
        _assert(authorized_pid == backend_pid, "controller backend changed during authorization")
        _write_event(
            _event_path(spec.events_dir, spec.generation, "ddl_authorized"),
            {
                "backend_pid": backend_pid,
                "remaining_lease_seconds": remaining,
                "scenario": spec.scenario,
            },
        )
        if spec.scenario == "after_begin":
            return await _park_after_begin(connection, spec, backend_pid)
        attach_seconds = await _perform_cutover(connection, spec, messages)
        _write_event(
            _event_path(spec.events_dir, spec.generation, "attach_complete"),
            {
                "attach_seconds": attach_seconds,
                "debug_messages": messages,
            },
        )
        if spec.scenario == "orphan_timeout":
            orphan_holder, holder_start_ticks = _start_orphan_socket_holder(
                connection,
                remaining + 30.0,
            )
            _write_event(
                _event_path(spec.events_dir, spec.generation, "orphan_active"),
                {
                    "application_name": application_name,
                    "backend_pid": backend_pid,
                    "holder_pid": orphan_holder.pid,
                    "holder_start_ticks": holder_start_ticks,
                    "socket_fd_held": _connection_socket_fd(connection),
                },
            )
            await connection.execute("SELECT pg_sleep(30)")
            return 4
        if spec.scenario == "during_commit":
            await connection.execute(
                """
                UPDATE rehearsal.commit_marker
                SET generation = $1
                WHERE singleton
                """,
                spec.generation,
            )
            _write_event(
                _event_path(spec.events_dir, spec.generation, "commit_enter"),
                {"backend_pid": backend_pid},
            )
        await connection.execute("COMMIT")
        state = await _catalog_state(connection, spec.horizon)
        _assert(
            state.mode == "POST",
            f"controller COMMIT did not produce exact POST state: {state}",
        )
        result_payload: dict[str, object] = {
            "attach_seconds": attach_seconds,
            "catalog_fingerprint": state.fingerprint(),
            "debug_messages": messages,
            "initial_timezone": initial_timezone,
            "transaction_timezone": pinned_timezone,
        }
        _write_event(
            _event_path(spec.events_dir, spec.generation, "commit_complete"),
            result_payload,
        )
        if spec.scenario == "after_commit":
            await asyncio.sleep(3600)
            return 5
        if spec.scenario == "normal":
            await _controller_release(spec.instance, spec, state)
            return 0
        return 0
    finally:
        if orphan_holder is not None:
            _terminate_popen_bounded(orphan_holder)
        await _close_connection_bounded(connection)


async def _wait_resolution(
    connection: Connection,
    generation: int,
    timeout: float = 30.0,
) -> Record:
    """Wait until publisher watchdogs or controller resolve a fence.

    Args:
        connection: Autocommit monitoring connection.
        generation: Fence generation.
        timeout: Maximum wait.

    Returns:
        Terminal fence row.
    """
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        row = await connection.fetchrow(
            """
            SELECT
                resolution,
                resolver,
                resolved_fingerprint,
                resolved_at,
                lease_expires_at,
                ddl_pid
            FROM rehearsal.fence_generations
            WHERE generation = $1
            """,
            generation,
        )
        _assert(row is not None, "resolution fence row missing")
        if row["resolution"] is not None:
            return row
        await asyncio.sleep(0.03)
    raise RehearsalError(f"fence G{generation} did not reach terminal resolution")


async def _wait_resumes(
    connection: Connection,
    generation: int,
    writers: int,
    stats: Sequence[WriterStats],
    timeout: float = 30.0,
) -> list[Record]:
    """Wait for every configured publisher-local resume event.

    Args:
        connection: Autocommit monitoring connection.
        generation: Fence generation.
        writers: Exact configured publisher count.
        stats: Publisher failure evidence checked while waiting.
        timeout: Maximum wait.

    Returns:
        Ordered resume event rows.
    """
    deadline = time.monotonic() + timeout
    expected_publishers = {f"publisher-{number}" for number in range(1, writers + 1)}
    while time.monotonic() < deadline:
        failures = [failure for item in stats for failure in item.failures]
        if failures:
            raise RehearsalError("publisher failed before resume: " + "; ".join(failures))
        rows = await connection.fetch(
            """
            SELECT publisher_id, event, mode, fingerprint, occurred_at
            FROM rehearsal.publisher_events
            WHERE generation = $1
              AND event LIKE 'RESUMED_%'
            ORDER BY publisher_id
            """,
            generation,
        )
        publisher_ids = {cast(str, row["publisher_id"]) for row in rows}
        if len(rows) == writers and publisher_ids == expected_publishers:
            return list(rows)
        await asyncio.sleep(0.03)
    raise RehearsalError(f"not every publisher resumed for G{generation}")


async def _deny_stale_authority(
    connection: Connection,
    spec: ControllerSpec,
) -> dict[str, object]:
    """Run the real controller authorizer against a terminal generation.

    Args:
        connection: Autocommit monitoring connection.
        spec: Exact stale generation, token, acknowledgement count, and bounds.

    Returns:
        Actual-authorizer rejection and unchanged-catalog evidence.
    """
    before = await _catalog_state(connection, spec.horizon)
    rejection = ""
    authorized = False
    transaction = connection.transaction()
    await transaction.start()
    try:
        try:
            await _authorize_controller(connection, spec)
            authorized = True
        except RehearsalError as exc:
            rejection = str(exc)
    finally:
        await transaction.rollback()
    after = await _catalog_state(connection, spec.horizon)
    denied = (
        not authorized
        and rejection == "controller generation is already terminal"
        and before.fingerprint() == after.fingerprint()
    )
    return {
        "actual_authorizer_attempted": True,
        "catalog_unchanged": before.fingerprint() == after.fingerprint(),
        "denied": denied,
        "rejection": rejection,
    }


async def _reset_to_pre(instance: InstanceRef, horizon: datetime) -> None:
    """Return a committed POST cutover to the reusable exact PRE topology.

    Args:
        instance: Verified private PostgreSQL instance.
        horizon: Expected catalog boundary used for state validation.

    Returns:
        None after PRE is re-established and future leaves are empty.
    """
    connection = await _connect(instance, "rehearsal-reset")
    try:
        state = await _catalog_state(connection, horizon)
        if state.mode == "POST":
            async with connection.transaction():
                await connection.execute("SET LOCAL timezone = 'UTC'")
                await connection.execute("ALTER TABLE trades DETACH PARTITION trades_legacy")
                await connection.execute("ALTER TABLE trades RENAME TO trades_parent")
                await connection.execute("ALTER TABLE trades_legacy RENAME TO trades")
                await connection.execute("ALTER SEQUENCE trades_id_seq OWNED BY trades.id")
                await connection.execute(
                    "UPDATE rehearsal.topology_epoch SET topology = 'ORDINARY' WHERE singleton"
                )
        final_state = await _catalog_state(connection, horizon)
        _assert(final_state.mode == "PRE", f"reset did not restore PRE: {final_state}")
    finally:
        await _close_connection_bounded(connection)


async def _normal_attach_evidence(
    instance: InstanceRef,
    process: asyncio.subprocess.Process,
    spec: ControllerSpec,
    before_oids: tuple[int, ...],
) -> dict[str, object]:
    """Collect exact successful ATTACH evidence after normal controller exit.

    Args:
        instance: Verified private PostgreSQL instance.
        process: Normal controller process.
        spec: Normal generation and event paths.
        before_oids: Exact child index OIDs before ATTACH.

    Returns:
        Timing, DEBUG1, OID, inheritance, and boundary evidence.
    """
    stdout_bytes, stderr_bytes = await _communicate_process_bounded(process, 30.0)
    stdout = stdout_bytes.decode("utf-8", errors="replace")
    stderr = stderr_bytes.decode("utf-8", errors="replace")
    _assert(
        process.returncode == 0,
        f"normal controller did not exit successfully: {stdout}{stderr}",
    )
    commit = await _wait_event(_event_path(spec.events_dir, spec.generation, "commit_complete"))
    connection = await _connect(instance, "normal-attach-evidence")
    try:
        after_oids = await _legacy_index_oids(connection, "trades_legacy")
        pairs = await _index_inheritance_pairs(connection)
        debug_messages_value = commit.get("debug_messages")
        _assert(isinstance(debug_messages_value, list), "normal controller omitted DEBUG messages")
        debug_messages = [str(message) for message in cast(list[object], debug_messages_value)]
        debug_observed = any(_DEBUG_IMPLICATION in message for message in debug_messages)
        attach_seconds = float(cast(float | int, commit["attach_seconds"]))
        initial_timezone = cast(str, commit.get("initial_timezone"))
        transaction_timezone = cast(str, commit.get("transaction_timezone"))
        _assert(attach_seconds < 1.0, f"ATTACH took {attach_seconds:.6f}s")
        _assert(before_oids == after_oids, "legacy index OID multiset changed across ATTACH")
        _assert(len(before_oids) == 7, "legacy did not have exactly seven pre-ATTACH indexes")
        _assert(set(pairs) == _EXPECTED_INDEX_PAIRS, f"index inheritance mismatch: {pairs}")
        _assert(debug_observed, "PostgreSQL DEBUG1 implication proof was not observed")
        _assert(
            initial_timezone == _NON_UTC_TIMEZONE,
            f"atomic DDL initial timezone was {initial_timezone}",
        )
        _assert(transaction_timezone == "UTC", "atomic DDL did not observe UTC after SET LOCAL")
        boundary = await _exercise_h_boundary(connection, spec.horizon)
        return {
            "attach_seconds": attach_seconds,
            "child_index_count_after": len(after_oids),
            "child_index_count_before": len(before_oids),
            "child_index_oids_after": list(after_oids),
            "child_index_oids_before": list(before_oids),
            "controller_stderr": stderr.strip(),
            "controller_stdout": stdout.strip(),
            "ddl_session_initial_timezone": initial_timezone,
            "ddl_transaction_timezone": transaction_timezone,
            "debug_implication_message": _DEBUG_IMPLICATION,
            "debug_implication_observed": debug_observed,
            "h_boundary_routes": boundary,
            "index_inheritance_pairs": [list(pair) for pair in pairs],
        }
    finally:
        await _close_connection_bounded(connection)


async def _exercise_h_boundary(
    connection: Connection,
    horizon: datetime,
) -> dict[str, str]:
    """Insert exact H-edge sentinels and prove their physical partitions.

    Args:
        connection: POST-cutover connection.
        horizon: Partition boundary.

    Returns:
        Expected labels mapped to physical table names.
    """
    sentinels = (
        ("h_minus_1us", horizon - timedelta(microseconds=1), "trades_legacy"),
        ("h_exact", horizon, f"trades_d{horizon.strftime('%Y%m%d')}"),
        (
            "h_plus_1d",
            horizon + timedelta(days=1),
            f"trades_d{(horizon + timedelta(days=1)).strftime('%Y%m%d')}",
        ),
    )
    routes: dict[str, str] = {}
    for number, (label, executed_at, expected_table) in enumerate(sentinels, start=1):
        public_id = uuid5(_IDENTITY_NAMESPACE, f"boundary:{label}")
        instrument_id = UUID(int=0xCA00 + number)
        trade_id = f"boundary-{label}"
        table_name = cast(
            str,
            await connection.fetchval(
                """
            INSERT INTO trades (
                public_id,
                instrument_public_id,
                trade_id,
                price,
                size,
                side,
                executed_at,
                session_id,
                sequence_id,
                "timestamp",
                known_to
            )
            VALUES (
                $1, $2, $3, 1.0, 1.0, 'buy', $4, $5, $6, $7,
                TIMESTAMPTZ '9999-12-31 23:59:59+00:00'
            )
            RETURNING tableoid::regclass::text
            """,
                public_id,
                instrument_id,
                trade_id,
                executed_at,
                UUID("00000000-0000-7000-8000-00000000ca01"),
                number,
                executed_at + timedelta(milliseconds=1),
            ),
        )
        _assert(bool(table_name), f"{label} INSERT returned no physical route")
        _assert(
            table_name == expected_table, f"{label} routed to {table_name}, not {expected_table}"
        )
        routes[label] = table_name
    return routes


async def _run_normal_cutover(
    instance: InstanceRef,
    config: RunConfig,
    paths: RuntimePaths,
    horizon: datetime,
) -> tuple[dict[str, object], IdentityEvidence, list[WriterStats]]:
    """Run the successful end-to-end adoption while publishers are active.

    Args:
        instance: Verified private PostgreSQL instance.
        config: Lease and writer bounds.
        paths: Evidence paths.
        horizon: Legacy/daily boundary.

    Returns:
        Exact ATTACH evidence, identity proof, and writer counters.
    """
    contexts, stop_event = _writer_contexts(instance, config, paths, horizon, "normal")
    tasks = [asyncio.create_task(_publisher_loop(context)) for context in contexts]
    stats = [context.stats for context in contexts]
    monitor = await _connect(instance, "normal-monitor")
    process: asyncio.subprocess.Process | None = None
    try:
        await _wait_writer_fence_ready(stats, 4)
        before_oids = await _legacy_index_oids(monitor, "trades")
        lease = await _open_fence(instance, config, "normal")
        await _wait_fence_acks(
            instance,
            lease,
            config.writers,
            config.transaction_timeout_seconds + config.cleanup_margin_seconds,
        )
        spec = ControllerSpec(
            instance=instance,
            scenario="normal",
            generation=lease.generation,
            token=lease.token,
            horizon=horizon,
            writers=config.writers,
            transaction_timeout_seconds=config.transaction_timeout_seconds,
            cleanup_margin_seconds=config.cleanup_margin_seconds,
            events_dir=paths.events_dir,
        )
        process = await _start_controller(spec)
        attach = await _normal_attach_evidence(instance, process, spec, before_oids)
        resolution = await _wait_resolution(monitor, lease.generation)
        _assert(resolution["resolver"] == "controller", "normal fence was not controller-released")
        await _wait_resumes(monitor, lease.generation, config.writers, stats)
        target = max(item.committed for item in stats) + 1
        await _wait_writer_commits(stats, target)
        await _stop_publishers(tasks, stop_event)
        identities = [identity for item in stats for identity in item.expected]
        identity = await _identity_multiset(instance, "normal", identities)
        _assert(identity.passed(), "normal cutover identity multiset mismatch")
        return attach, identity, stats
    finally:
        if process is not None and process.returncode is None:
            await _kill_process_bounded(process)
        await _settle_publishers(tasks, stop_event)
        await _close_connection_bounded(monitor)


async def _wait_catalog_stable(
    connection: Connection,
    horizon: datetime,
    expected: CatalogState,
    seconds: float,
) -> bool:
    """Require the resume catalog fingerprint to remain unchanged.

    Args:
        connection: Autocommit observer.
        horizon: Expected partition boundary.
        expected: Catalog state captured at resume.
        seconds: Stability guard duration.

    Returns:
        True only when every sample matches.
    """
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        current = await _catalog_state(connection, horizon)
        if current.fingerprint() != expected.fingerprint():
            return False
        await asyncio.sleep(0.05)
    return True


async def _observe_fault_and_kill(
    monitor: Connection,
    process: asyncio.subprocess.Process,
    spec: ControllerSpec,
) -> tuple[int, dict[str, object]]:
    """Independently observe a scenario's exact fault point and SIGKILL it.

    Args:
        monitor: Autocommit pg_stat_activity observer.
        process: Killable controller.
        spec: Scenario and event paths.

    Returns:
        Controller return code and independent observation evidence.
    """
    application = f"rehearsal-controller-g{spec.generation}-{spec.scenario}"
    event: dict[str, object]
    observed: Record
    if spec.scenario == "after_begin":
        event = await _wait_event(
            _event_path(spec.events_dir, spec.generation, "after_begin_mutation"),
            process=process,
        )
        observed = await _wait_backend_present(monitor, application)
        pid = int(cast(int | str, event["backend_pid"]))
        _assert(cast(int, observed["pid"]) == pid, "authorized backend PID changed")
        _assert(
            cast(str, observed["state"]) == "idle in transaction",
            "after-BEGIN backend was not idle in its open transaction",
        )
        _assert(await _ddl_lock_count(monitor, pid) > 0, "authorized DDL held no generation lock")
        _assert(
            event.get("internal_legacy_relation") == "trades_legacy",
            "after-BEGIN controller did not perform an uncommitted catalog mutation",
        )
    elif spec.scenario == "during_commit":
        event = await _wait_event(
            _event_path(spec.events_dir, spec.generation, "commit_enter"),
            process=process,
        )
        observed = await _wait_backend(monitor, application, "COMMIT", "Timeout")
        _assert(observed["wait_event"] == "PgSleep", "COMMIT delay trigger was not active")
    elif spec.scenario == "after_commit":
        event = await _wait_event(
            _event_path(spec.events_dir, spec.generation, "commit_complete"),
            process=process,
        )
        observed_state = await _catalog_state(monitor, spec.horizon)
        _assert(observed_state.mode == "POST", "after-COMMIT observer did not see POST")
        resume_count = cast(
            int,
            await monitor.fetchval(
                """
                SELECT count(*)::integer
                FROM rehearsal.publisher_events
                WHERE generation = $1
                  AND event LIKE 'RESUMED_%'
                """,
                spec.generation,
            ),
        )
        _assert(resume_count == 0, "publisher resumed before after-COMMIT controller kill")
        observed = await _wait_backend_present(monitor, application)
        _assert(
            cast(str, observed["state"]) == "idle",
            "after-COMMIT controller backend still held a transaction",
        )
    elif spec.scenario == "orphan_timeout":
        event = await _wait_event(
            _event_path(spec.events_dir, spec.generation, "orphan_active"),
            process=process,
        )
        observed = await _wait_backend(monitor, application, "pg_sleep", "Timeout")
    else:
        raise RehearsalError(f"unsupported fault scenario {spec.scenario}")
    pre_kill_state = await _catalog_state(monitor, spec.horizon)
    returncode, stdout, stderr = await _kill_controller(process)
    post_kill_backend_active = False
    holder_alive_after_controller_kill = False
    if spec.scenario == "orphan_timeout":
        holder_pid = int(cast(int | str, event["holder_pid"]))
        holder_start_ticks = int(cast(int | str, event["holder_start_ticks"]))
        holder_alive_after_controller_kill = _orphan_holder_alive(
            holder_pid,
            holder_start_ticks,
        )
        _assert(
            holder_alive_after_controller_kill,
            "controller SIGKILL closed the orphan socket holder",
        )
        post_kill = await _wait_backend(
            monitor,
            application,
            "pg_sleep",
            "Timeout",
        )
        post_kill_backend_active = (
            cast(int, post_kill["pid"]) == cast(int, observed["pid"])
            and cast(str, post_kill["state"]) == "active"
        )
        _assert(
            post_kill_backend_active,
            "orphan backend did not remain active after controller SIGKILL",
        )
    return (
        returncode,
        {
            "controller_stderr": stderr.strip(),
            "controller_stdout": stdout.strip(),
            "event": event,
            "observed_backend_pid": cast(int, observed["pid"]),
            "observed_query": cast(str, observed["query"]),
            "observed_state_before_kill": pre_kill_state.mode,
            "observed_wait_event": cast(str | None, observed["wait_event"]),
            "observed_wait_event_type": cast(str | None, observed["wait_event_type"]),
            "holder_alive_after_controller_kill": holder_alive_after_controller_kill,
            "post_kill_backend_active": post_kill_backend_active,
        },
    )


async def _verify_watchdog_recovery(
    connection: Connection,
    lease: FenceLease,
    spec: ControllerSpec,
    config: RunConfig,
    stats: Sequence[WriterStats],
) -> RecoveryResult:
    """Verify terminal catalog, one CAS winner, resumes, and stability.

    Args:
        connection: Autocommit supervisor connection.
        lease: Expired controller authority generation.
        spec: Exact controller authority that must now be rejected.
        config: Publisher count and stability guard.
        stats: Publisher failure evidence checked while waiting.

    Returns:
        Complete watchdog reconciliation evidence.
    """
    resolution_row = await _wait_resolution(connection, lease.generation)
    resumes = tuple(await _wait_resumes(connection, lease.generation, config.writers, stats))
    terminal = await _catalog_state(connection, spec.horizon)
    _assert(terminal.mode in {"PRE", "POST"}, "kill recovery produced incoherent catalog")
    expected_resolution = "PRE_ABORTED" if terminal.mode == "PRE" else "POST_COMMITTED"
    resolution = cast(str, resolution_row["resolution"])
    _assert(resolution == expected_resolution, "kill recovery resolution/catalog mismatch")
    _assert(
        cast(str, resolution_row["resolved_fingerprint"]) == terminal.fingerprint(),
        "winning reconciler stored a stale catalog fingerprint",
    )
    for resume in resumes:
        _assert(
            cast(str, resume["event"]) == "RESUMED_PUBLISHER_WATCHDOG",
            "kill scenario did not resume from publisher-local watchdog",
        )
        _assert(
            cast(datetime, resume["occurred_at"]) >= lease.expires_at,
            "publisher resumed before absolute database-clock lease expiry",
        )
        _assert(
            cast(str, resume["fingerprint"]) == terminal.fingerprint(),
            "publisher resume fingerprint differs from terminal catalog",
        )
    winner_count = cast(
        int,
        await connection.fetchval(
            """
            SELECT count(*)::integer
            FROM rehearsal.publisher_events
            WHERE generation = $1
              AND event = 'RECONCILE_WINNER'
            """,
            lease.generation,
        ),
    )
    _assert(winner_count == 1, "competing watchdogs did not produce exactly one CAS winner")
    stable = await _wait_catalog_stable(
        connection,
        spec.horizon,
        terminal,
        config.guard_seconds,
    )
    _assert(stable, "catalog changed after publisher resume")
    stale_authority = await _deny_stale_authority(connection, spec)
    stale_denied = cast(bool, stale_authority["denied"])
    _assert(stale_denied, "expired generation reacquired DDL authority")
    return RecoveryResult(
        resolution_row=resolution_row,
        resumes=resumes,
        terminal=terminal,
        winner_count=winner_count,
        stable=stable,
        stale_denied=stale_denied,
        stale_authority=stale_authority,
    )


async def _complete_orphan_timeout(
    monitor: Connection,
    context: OrphanTimeoutContext,
) -> dict[str, object]:
    """Prove the post-SIGKILL backend dies from its PID-bound transaction timeout.

    Args:
        monitor: Independent private catalog and activity observer.
        context: Log, lease, controller, and fault-observation evidence.

    Returns:
        Timeout causality, early-resume, and socket-holder cleanup evidence.
    """
    event = cast(dict[str, object], context.observation["event"])
    holder_pid = int(cast(int | str, event["holder_pid"]))
    holder_start_ticks = int(cast(int | str, event["holder_start_ticks"]))
    application_name = f"rehearsal-controller-g{context.lease.generation}-{context.spec.scenario}"
    backend_pid = cast(int, context.observation["observed_backend_pid"])
    backend_gone_at = await _wait_backend_gone(monitor, application_name)
    timeout_log_observed = await _wait_log_message(
        context.log_path,
        context.log_offset,
        _TRANSACTION_TIMEOUT_LOG,
        backend_pid,
        application_name,
    )
    _assert(timeout_log_observed, "orphan was not killed by transaction_timeout")
    _assert(
        backend_gone_at
        < context.lease.expires_at - timedelta(seconds=context.spec.cleanup_margin_seconds),
        "orphan DDL survived beyond lease cleanup margin",
    )
    early_resumes = cast(
        int,
        await monitor.fetchval(
            """
            SELECT count(*)::integer
            FROM rehearsal.publisher_events
            WHERE generation = $1
              AND event LIKE 'RESUMED_%'
            """,
            context.lease.generation,
        ),
    )
    _assert(early_resumes == 0, "publisher resumed before orphan lease expiry")
    holder_terminated = await _terminate_orphan_holder(holder_pid, holder_start_ticks)
    _assert(
        holder_terminated,
        "orphan socket holder survived bounded post-timeout cleanup",
    )
    return {
        "backend_gone_at": backend_gone_at.isoformat(),
        "early_resumes_before_timeout": early_resumes,
        "holder_terminated_after_timeout": holder_terminated,
        "timeout_log_application": application_name,
        "timeout_log_backend_pid": backend_pid,
        "timeout_log_observed": timeout_log_observed,
    }


async def _cleanup_kill_scenario(resources: KillScenarioCleanup) -> None:
    """Bound holder, controller, publisher, and monitor cleanup after one kill.

    Args:
        resources: Exact resources owned by the current scenario.

    Returns:
        None after every owned process and connection is settled.
    """
    holder_pid = resources.holder_pid
    holder_start_ticks = resources.holder_start_ticks
    if (
        resources.scenario == "orphan_timeout"
        and holder_pid is None
        and resources.lease is not None
    ):
        event_path = _event_path(
            resources.paths.events_dir,
            resources.lease.generation,
            "orphan_active",
        )
        if event_path.exists():
            event = json.loads(event_path.read_text(encoding="utf-8"))
            if isinstance(event, dict):
                holder_pid = int(cast(int | str, event["holder_pid"]))
                holder_start_ticks = int(cast(int | str, event["holder_start_ticks"]))
    if (
        holder_pid is not None
        and holder_start_ticks is not None
        and _orphan_holder_alive(holder_pid, holder_start_ticks)
    ):
        cleanup_succeeded = await _terminate_orphan_holder(
            holder_pid,
            holder_start_ticks,
        )
        _assert(cleanup_succeeded, "orphan socket holder cleanup failed")
    if resources.process is not None and resources.process.returncode is None:
        await _kill_process_bounded(resources.process)
    await _settle_publishers(resources.tasks, resources.stop_event)
    await _close_connection_bounded(resources.monitor)


async def _run_kill_scenario(
    instance: InstanceRef,
    config: RunConfig,
    paths: RuntimePaths,
    horizon: datetime,
    scenario: str,
) -> ScenarioResult:
    """Run one fenced controller-kill scenario through watchdog recovery.

    Args:
        instance: Verified private PostgreSQL instance.
        config: Lease, writer, and guard bounds.
        paths: Evidence paths.
        horizon: Cutover boundary.
        scenario: Exact kill point.

    Returns:
        Terminal topology, CAS, resume, and identity evidence.
    """
    _assert(
        scenario in {"after_begin", "during_commit", "after_commit", "orphan_timeout"},
        f"unsupported kill scenario {scenario}",
    )
    await _reset_to_pre(instance, horizon)
    contexts, stop_event = _writer_contexts(instance, config, paths, horizon, scenario)
    tasks = [asyncio.create_task(_publisher_loop(context)) for context in contexts]
    stats = [context.stats for context in contexts]
    monitor = await _connect(instance, f"supervisor-{scenario}")
    process: asyncio.subprocess.Process | None = None
    orphan_holder_pid: int | None = None
    orphan_holder_start_ticks: int | None = None
    lease: FenceLease | None = None
    log_offset = paths.postgres_log.stat().st_size
    try:
        await _wait_writer_fence_ready(stats, 4)
        lease = await _open_fence(instance, config, scenario)
        await _wait_fence_acks(
            instance,
            lease,
            config.writers,
            config.transaction_timeout_seconds + config.cleanup_margin_seconds,
        )
        spec = ControllerSpec(
            instance=instance,
            scenario=scenario,
            generation=lease.generation,
            token=lease.token,
            horizon=horizon,
            writers=config.writers,
            transaction_timeout_seconds=config.transaction_timeout_seconds,
            cleanup_margin_seconds=config.cleanup_margin_seconds,
            events_dir=paths.events_dir,
        )
        process = await _start_controller(spec)
        returncode, observation = await _observe_fault_and_kill(monitor, process, spec)
        orphan_evidence: dict[str, object] = {}
        if scenario == "orphan_timeout":
            event = cast(dict[str, object], observation["event"])
            orphan_holder_pid = int(cast(int | str, event["holder_pid"]))
            orphan_holder_start_ticks = int(cast(int | str, event["holder_start_ticks"]))
            orphan_evidence = await _complete_orphan_timeout(
                monitor,
                OrphanTimeoutContext(
                    log_path=paths.postgres_log,
                    log_offset=log_offset,
                    lease=lease,
                    spec=spec,
                    observation=observation,
                ),
            )
        recovery = await _verify_watchdog_recovery(
            monitor,
            lease,
            spec,
            config,
            stats,
        )
        resolution = cast(str, recovery.resolution_row["resolution"])
        target = max(item.committed for item in stats) + 1
        await _wait_writer_commits(stats, target)
        await _stop_publishers(tasks, stop_event)
        identities = [identity for item in stats for identity in item.expected]
        identity = await _identity_multiset(instance, scenario, identities)
        _assert(identity.passed(), f"identity multiset mismatch after {scenario}")
        surfaced = sum(item.surfaced_unique_violations for item in stats)
        hidden = sum(item.hidden_conflicts for item in stats)
        _assert(surfaced == 0, f"{scenario} surfaced unique violations")
        _assert(hidden == 0, f"{scenario} hid targetless conflicts")
        observation.update(
            {
                **orphan_evidence,
                "backend_gone_at": orphan_evidence.get("backend_gone_at"),
                "lease_expires_at": lease.expires_at.isoformat(),
                "resolution": resolution,
                "resolver": cast(str, recovery.resolution_row["resolver"]),
                "resume_count": len(recovery.resumes),
                "stale_authority_denied": recovery.stale_denied,
                "stale_authority": recovery.stale_authority,
                "terminal_catalog": recovery.terminal.mode,
                "terminal_fingerprint": recovery.terminal.fingerprint(),
                "timeout_log_observed": orphan_evidence.get(
                    "timeout_log_observed",
                    False,
                ),
                "winner_count": recovery.winner_count,
            }
        )
        return ScenarioResult(
            name=scenario,
            terminal_state=recovery.terminal,
            controller_returncode=returncode,
            resolution=resolution,
            resolver=cast(str, recovery.resolution_row["resolver"]),
            resume_count=len(recovery.resumes),
            winner_count=recovery.winner_count,
            stable_after_resume=recovery.stable,
            stale_authority_denied=recovery.stale_denied,
            identity=identity,
            surfaced_unique_violations=surfaced,
            hidden_conflicts=hidden,
            fault_evidence=observation,
        )
    finally:
        await _cleanup_kill_scenario(
            KillScenarioCleanup(
                scenario=scenario,
                paths=paths,
                lease=lease,
                holder_pid=orphan_holder_pid,
                holder_start_ticks=orphan_holder_start_ticks,
                process=process,
                tasks=tuple(tasks),
                stop_event=stop_event,
                monitor=monitor,
            )
        )


def _writer_drop_kill_passed(
    evidence: dict[str, object],
    identity: IdentityEvidence,
    stats: WriterStats,
) -> bool:
    """Bind the ACTIVE-fence writer kill row to durable incident evidence.

    Args:
        evidence: Backend, lease, page, resume, and actual-authorizer facts.
        identity: Resumed writer's bidirectional identity result.
        stats: Resumed writer conflict and failure counters.

    Returns:
        True only when detection, page, recovery, and resumed ingestion all pass.
    """
    active_event = evidence.get("active_event")
    stale_authority = evidence.get("stale_authority")
    if not isinstance(active_event, dict) or not isinstance(stale_authority, dict):
        return False
    try:
        lease_expires_at = datetime.fromisoformat(cast(str, evidence.get("lease_expires_at", "")))
        backend_gone_at = datetime.fromisoformat(cast(str, evidence.get("backend_gone_at", "")))
        durable_page_at = datetime.fromisoformat(cast(str, evidence.get("durable_page_at", "")))
        ingestion_resumed_at = datetime.fromisoformat(
            cast(str, evidence.get("ingestion_resumed_at", ""))
        )
    except (TypeError, ValueError):
        return False
    generation = int(cast(int, evidence.get("fence_generation", 0)))
    return (
        int(cast(int, evidence.get("controller_returncode", 0))) == -signal.SIGKILL
        and int(cast(int, evidence.get("observed_backend_pid", 0))) > 1
        and generation > 0
        and int(cast(int, active_event.get("generation", 0))) == generation
        and int(cast(int, active_event.get("held_obligations", 0))) == 1
        and cast(bool, evidence.get("marker_prearmed", False))
        and cast(bool, evidence.get("fence_aborted", False))
        and cast(str, evidence.get("durable_page_reason", ""))
        == "publisher died with held obligations"
        and backend_gone_at < lease_expires_at
        and durable_page_at < lease_expires_at
        and ingestion_resumed_at < lease_expires_at
        and cast(bool, evidence.get("stale_authority_denied", False))
        and cast(bool, stale_authority.get("denied", False))
        and cast(bool, stale_authority.get("actual_authorizer_attempted", False))
        and cast(bool, stale_authority.get("catalog_unchanged", False))
        and cast(str, stale_authority.get("rejection", ""))
        == "controller generation is already terminal"
        and identity.passed()
        and stats.surfaced_unique_violations == 0
        and stats.hidden_conflicts == 0
        and not stats.failures
    )


async def _run_writer_drop_kill(
    instance: InstanceRef,
    config: RunConfig,
    paths: RuntimePaths,
    horizon: datetime,
) -> tuple[KillResult, IdentityEvidence, WriterStats]:
    """Kill a pre-armed publisher during an ACTIVE fence and recover immediately.

    Args:
        instance: Verified private ordinary-table instance.
        config: Fence lease bounds.
        paths: Durable event and identity journal paths.
        horizon: Pre-H timestamp boundary for the resumed ingestion probe.

    Returns:
        Kill-matrix result, resumed-row identity proof, and writer counters.
    """
    lease = await _open_fence(instance, config, "writer_drop")
    spec = ControllerSpec(
        instance=instance,
        scenario="writer_drop",
        generation=lease.generation,
        token=lease.token,
        horizon=horizon,
        writers=1,
        transaction_timeout_seconds=config.transaction_timeout_seconds,
        cleanup_margin_seconds=config.cleanup_margin_seconds,
        events_dir=paths.events_dir,
    )
    process = await _start_writer_drop_child(spec)
    monitor = await _connect(instance, "writer-drop-supervisor")
    stats = WriterStats()
    try:
        active_event = await _wait_event(
            _event_path(paths.events_dir, lease.generation, "writer_active"),
            process=process,
        )
        backend = await _wait_backend_present(
            monitor,
            f"rehearsal-writer-drop-g{lease.generation}",
        )
        marker = await monitor.fetchrow(
            """
            SELECT session_id, state, held_obligations
            FROM rehearsal.publisher_runtime
            WHERE generation = $1
            """,
            lease.generation,
        )
        _assert(marker is not None, "writer-drop pre-armed marker is missing")
        _assert(
            cast(str, marker["state"]) == "ACTIVE" and cast(int, marker["held_obligations"]) == 1,
            "writer-drop marker was not active with a held obligation",
        )
        killed_at = cast(datetime, await monitor.fetchval("SELECT clock_timestamp()"))
        _assert(killed_at < lease.expires_at, "publisher kill missed the ACTIVE fence window")
        returncode, stdout, stderr = await _kill_controller(process)
        backend_gone_at = await _wait_backend_gone(
            monitor,
            f"rehearsal-writer-drop-g{lease.generation}",
        )
        state = await _catalog_state(monitor, horizon)
        _assert(state.mode == "PRE", "writer-drop fence did not preserve the PRE catalog")
        async with monitor.transaction():
            fence = await monitor.fetchrow(
                """
                SELECT resolution, clock_timestamp() AS database_now, lease_expires_at
                FROM rehearsal.fence_generations
                WHERE generation = $1
                  AND token = $2
                FOR UPDATE
                """,
                lease.generation,
                lease.token,
            )
            _assert(fence is not None, "writer-drop recovery fence disappeared")
            _assert(fence["resolution"] is None, "writer-drop fence resolved before detection")
            _assert(
                cast(datetime, fence["database_now"]) < cast(datetime, fence["lease_expires_at"]),
                "writer-drop recovery occurred after the ACTIVE lease",
            )
            await monitor.execute(
                """
                INSERT INTO rehearsal.fence_pages (
                    generation,
                    publisher_id,
                    reason
                )
                VALUES ($1, 'publisher-kill', 'publisher died with held obligations')
                """,
                lease.generation,
            )
            await monitor.execute(
                """
                UPDATE rehearsal.publisher_runtime
                SET state = 'INCIDENT'
                WHERE generation = $1
                """,
                lease.generation,
            )
            await monitor.execute(
                """
                UPDATE rehearsal.fence_generations
                SET
                    resolution = 'PRE_ABORTED',
                    resolver = 'publisher-supervisor',
                    resolved_fingerprint = $2,
                    resolved_at = clock_timestamp()
                WHERE generation = $1
                  AND token = $3
                  AND resolution IS NULL
                """,
                lease.generation,
                state.fingerprint(),
                lease.token,
            )
        page = await monitor.fetchrow(
            """
            SELECT paged_at, reason
            FROM rehearsal.fence_pages
            WHERE generation = $1
            """,
            lease.generation,
        )
        _assert(page is not None, "writer-drop recovery did not create a durable page")
        context = WriterContext(
            instance=instance,
            publisher_id="publisher-1",
            scenario="writer_drop_resume",
            horizon=horizon,
            writer_count=1,
            writer_rate=config.writer_rate,
            ledger_path=paths.identities_dir / "writer_drop_resume-publisher-1.jsonl",
            stop_event=asyncio.Event(),
            stats=stats,
        )
        rows = _writer_rows(context, "ORDINARY_TARGETLESS", 1, 1)
        with context.ledger_path.open("a", encoding="utf-8") as ledger:
            _append_expected(ledger, rows, stats)
        await _insert_writer_batch(monitor, rows, stats, "ORDINARY_TARGETLESS")
        await _exercise_conflict_batch(
            monitor,
            rows,
            "ORDINARY_TARGETLESS",
            stats,
        )
        resume_at = cast(datetime, await monitor.fetchval("SELECT clock_timestamp()"))
        await monitor.execute(
            """
            UPDATE rehearsal.publisher_runtime
            SET
                state = 'RECOVERED',
                held_obligations = 0,
                recovered_at = clock_timestamp()
            WHERE generation = $1
            """,
            lease.generation,
        )
        identity = await _identity_multiset(
            instance,
            "writer_drop_resume",
            stats.expected,
        )
        stale_authority = await _deny_stale_authority(monitor, spec)
        stale_denied = cast(bool, stale_authority["denied"])
        kill_evidence: dict[str, object] = {
            "active_event": active_event,
            "backend_gone_at": backend_gone_at.isoformat(),
            "controller_returncode": returncode,
            "controller_stderr": stderr.strip(),
            "controller_stdout": stdout.strip(),
            "durable_page_reason": cast(str, page["reason"]),
            "durable_page_at": cast(datetime, page["paged_at"]).isoformat(),
            "fence_aborted": True,
            "fence_generation": lease.generation,
            "ingestion_resumed_at": resume_at.isoformat(),
            "lease_expires_at": lease.expires_at.isoformat(),
            "marker_prearmed": True,
            "observed_backend_pid": cast(int, backend["pid"]),
            "stale_authority": stale_authority,
            "stale_authority_denied": stale_denied,
        }
        passed = _writer_drop_kill_passed(kill_evidence, identity, stats)
        return (
            KillResult(
                name="writer during ACTIVE fence abort/page",
                passed=passed,
                evidence=kill_evidence,
            ),
            identity,
            stats,
        )
    finally:
        if process.returncode is None:
            await _kill_process_bounded(process)
        await _close_connection_bounded(monitor)


async def _wait_log_message(
    path: Path,
    offset: int,
    message: str,
    backend_pid: int,
    application_name: str,
) -> bool:
    """Wait for one PID/application-bound PostgreSQL log line.

    Args:
        path: Private instance server log.
        offset: Scenario start offset.
        message: Required PostgreSQL text.
        backend_pid: Exact independently observed backend.
        application_name: Exact independently observed session label.

    Returns:
        True when the exact backend emits the message.
    """
    _assert(backend_pid > 1, "invalid backend PID for bound log observation")
    prefix = f"[{backend_pid}] {application_name} "
    deadline = time.monotonic() + 15.0
    while time.monotonic() < deadline:
        with path.open("r", encoding="utf-8", errors="replace") as handle:
            handle.seek(offset)
            lines = handle.read().splitlines()
            if any(prefix in line and message in line for line in lines):
                return True
        await asyncio.sleep(0.05)
    return False


async def _ensure_post(
    instance: InstanceRef, config: RunConfig, paths: RuntimePaths, horizon: datetime
) -> None:
    """Ensure the reusable fixture is in POST before retention DETACH.

    Args:
        instance: Verified private PostgreSQL instance.
        config: Lease bounds.
        paths: Controller event paths.
        horizon: Cutover boundary.

    Returns:
        None after exact POST is visible.
    """
    connection = await _connect(instance, "ensure-post")
    try:
        state = await _catalog_state(connection, horizon)
        if state.mode == "POST":
            return
    finally:
        await _close_connection_bounded(connection)
    contexts, stop_event = _writer_contexts(instance, config, paths, horizon, "detach_prepare")
    tasks = [asyncio.create_task(_publisher_loop(context)) for context in contexts]
    stats = [context.stats for context in contexts]
    process: asyncio.subprocess.Process | None = None
    try:
        await _wait_writer_fence_ready(stats, 2)
        lease = await _open_fence(instance, config, "normal")
        await _wait_fence_acks(
            instance,
            lease,
            config.writers,
            config.transaction_timeout_seconds + config.cleanup_margin_seconds,
        )
        spec = ControllerSpec(
            instance=instance,
            scenario="normal",
            generation=lease.generation,
            token=lease.token,
            horizon=horizon,
            writers=config.writers,
            transaction_timeout_seconds=config.transaction_timeout_seconds,
            cleanup_margin_seconds=config.cleanup_margin_seconds,
            events_dir=paths.events_dir,
        )
        process = await _start_controller(spec)
        stdout_bytes, stderr_bytes = await _communicate_process_bounded(process, 30.0)
        _assert(
            process.returncode == 0,
            (
                "detach preparation cutover failed: "
                + stdout_bytes.decode(errors="replace")
                + stderr_bytes.decode(errors="replace")
            ),
        )
        await _stop_publishers(tasks, stop_event)
        identities = [identity for item in stats for identity in item.expected]
        identity = await _identity_multiset(instance, "detach_prepare", identities)
        _assert(identity.passed(), "detach preparation identity multiset mismatch")
    finally:
        if process is not None and process.returncode is None:
            await _kill_process_bounded(process)
        await _settle_publishers(tasks, stop_event)


async def _insert_live_retention_row(
    connection: Connection,
    horizon: datetime,
    work_id: UUID,
    generation: int,
) -> tuple[UUID, str]:
    """Commit one pre-generation row and its obligation worklog atomically.

    Args:
        connection: Direct-mutator transaction holding the generation row lock.
        horizon: Legacy upper bound used to make the committed row pre-H.
        work_id: Durable obligation identity.
        generation: Generation observed under the same database lock.

    Returns:
        Stable public ID and physical legacy relation from the one SQL statement.
    """
    public_id = uuid5(_IDENTITY_NAMESPACE, f"retention-live:{work_id}")
    executed_at = horizon - timedelta(hours=3)
    row = await connection.fetchrow(
        """
        WITH inserted AS (
            INSERT INTO trades (
                public_id,
                instrument_public_id,
                trade_id,
                price,
                size,
                side,
                executed_at,
                session_id,
                sequence_id,
                "timestamp",
                known_to
            )
            VALUES (
                $3, $4, 'retention-live-old', 62001.0, 0.0201, 'buy',
                $5, $6, 1, $7, $8
            )
            RETURNING public_id, tableoid::regclass::text AS physical_table
        ),
        logged AS (
            INSERT INTO rehearsal.obligation_worklog (
                work_id,
                obligation_generation,
                public_id,
                destination
            )
            SELECT $1, $2, public_id, 'LIVE'
            FROM inserted
            RETURNING work_id
        )
        SELECT inserted.public_id, inserted.physical_table
        FROM inserted
        CROSS JOIN logged
        """,
        work_id,
        generation,
        public_id,
        UUID(int=0xCB01),
        executed_at,
        UUID("00000000-0000-7000-8000-00000000cb01"),
        executed_at + timedelta(milliseconds=1),
        _KNOWN_TO,
    )
    _assert(row is not None, "pre-generation row/worklog statement returned no row")
    return cast(UUID, row["public_id"]), cast(str, row["physical_table"])


async def _insert_retention_barrier_fixtures(
    connection: Connection,
    horizon: datetime,
    generation: int,
    live_public_id: UUID,
) -> dict[str, str]:
    """Create real pre-generation blocked, worklog, and monitor-lag obligations.

    Args:
        connection: Direct-mutator transaction holding the generation row lock.
        horizon: Fixed retention cutoff.
        generation: Pre-latch obligation generation.
        live_public_id: Existing legacy identity the monitor must reconcile.

    Returns:
        Durable fixture identities used by the rejecting barrier.
    """
    batch_id = uuid5(_IDENTITY_NAMESPACE, "retention-blocked-batch")
    blocked_public_id = uuid5(_IDENTITY_NAMESPACE, "retention-blocked-public")
    monitor_work_id = uuid5(_IDENTITY_NAMESPACE, "retention-monitor-work")
    executed_at = horizon - timedelta(hours=4)
    await connection.execute(
        """
        INSERT INTO rehearsal.retention_blocked_batches (
            batch_id,
            obligation_generation,
            state,
            public_id,
            instrument_public_id,
            trade_id,
            price,
            size,
            side,
            executed_at,
            session_id,
            sequence_id,
            "timestamp",
            known_to
        )
        VALUES (
            $1, $2, 'BLOCKED', $3, $4, 'retention-blocked-batch',
            62002.0, 0.0202, 'sell', $5, $6, 2, $7, $8
        )
        """,
        batch_id,
        generation,
        blocked_public_id,
        UUID(int=0xCB55),
        executed_at,
        UUID("00000000-0000-7000-8000-00000000cb55"),
        executed_at + timedelta(milliseconds=1),
        _KNOWN_TO,
    )
    await connection.execute(
        """
        INSERT INTO rehearsal.retention_monitor_worklog (
            work_id,
            obligation_generation,
            public_id,
            state
        )
        VALUES ($1, $2, $3, 'PENDING')
        """,
        monitor_work_id,
        generation,
        live_public_id,
    )
    await connection.execute(
        """
        INSERT INTO rehearsal.retention_monitor_lag (
            singleton,
            earliest_unsafe
        )
        VALUES (true, $1)
        """,
        horizon - timedelta(days=1),
    )
    return {
        "blocked_batch_id": str(batch_id),
        "blocked_public_id": str(blocked_public_id),
        "monitor_work_id": str(monitor_work_id),
    }


async def _insert_staged_replay(
    connection: Connection,
    horizon: datetime,
    work_id: UUID,
    generation: int,
    number: int,
) -> UUID:
    """Insert one staged pre-H row and obligation worklog under the generation lock.

    Args:
        connection: Direct-mutator transaction holding the generation row lock.
        horizon: Legacy upper bound used to make the replay row pre-H.
        work_id: Durable idempotency identity.
        generation: Generation observed under the same database lock.
        number: Deterministic payload discriminator.

    Returns:
        Stable public ID committed by the atomic row/worklog statement.
    """
    public_id = uuid5(_IDENTITY_NAMESPACE, f"staged-replay:{work_id}")
    executed_at = horizon - timedelta(hours=2, seconds=number)
    inserted = await connection.fetchval(
        """
        WITH inserted AS (
            INSERT INTO rehearsal.staged_replay (
                work_id,
                obligation_generation,
                state,
                public_id,
                instrument_public_id,
                trade_id,
                price,
                size,
                side,
                executed_at,
                session_id,
                sequence_id,
                "timestamp",
                known_to
            )
            VALUES (
                $1, $2, 'PENDING', $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13
            )
            RETURNING work_id, obligation_generation, public_id
        ),
        logged AS (
            INSERT INTO rehearsal.obligation_worklog (
                work_id,
                obligation_generation,
                public_id,
                destination
            )
            SELECT work_id, obligation_generation, public_id, 'STAGED'
            FROM inserted
            RETURNING work_id
        )
        SELECT inserted.public_id
        FROM inserted
        CROSS JOIN logged
        """,
        work_id,
        generation,
        public_id,
        UUID(int=0xCB00 + number),
        f"staged-replay-{number}",
        61_000.0 + number,
        0.01 + number / 10_000,
        "buy" if number % 2 == 0 else "sell",
        executed_at,
        UUID("00000000-0000-7000-8000-00000000cb01"),
        number,
        executed_at + timedelta(milliseconds=1),
        _KNOWN_TO,
    )
    _assert(cast(UUID | None, inserted) == public_id, "staged row/worklog statement lost identity")
    return public_id


async def _advance_retention_generation(connection: Connection) -> tuple[int, datetime]:
    """Advance the retention latch in one serialized committed transaction.

    Args:
        connection: Independent latch connection that must wait on old mutators.

    Returns:
        New generation and database-clock latch timestamp.
    """
    async with connection.transaction():
        row = await connection.fetchrow("""
            UPDATE rehearsal.retention_generation
            SET
                generation = generation + 1,
                latched_at = clock_timestamp()
            WHERE singleton
            RETURNING generation, latched_at
            """)
    _assert(row is not None, "retention generation latch updated no row")
    return cast(int, row["generation"]), cast(datetime, row["latched_at"])


async def _settle_generation_latch(
    task: asyncio.Task[tuple[int, datetime]] | None,
    connection: Connection,
) -> None:
    """Finish or cancel the generation latch task within fixed cleanup bounds.

    Args:
        task: Optional latch task blocked behind the old generation mutator.
        connection: Connection owned exclusively by the latch task.

    Returns:
        None after the task result is retrieved.
    """
    if task is None:
        return
    done, pending = await asyncio.wait({task}, timeout=_TASK_CANCEL_TIMEOUT_SECONDS)
    if pending:
        task.cancel()
        connection.terminate()
        cancelled, pending = await asyncio.wait(pending, timeout=_TASK_CANCEL_TIMEOUT_SECONDS)
        done.update(cancelled)
    if pending:
        raise RehearsalError("generation latch task resisted bounded cancellation")
    for completed in done:
        if not completed.cancelled():
            completed.exception()


async def _prove_generation_serialization(
    instance: InstanceRef,
    horizon: datetime,
) -> dict[str, object]:
    """Force a direct mutator to serialize before the retention generation latch.

    Args:
        instance: Verified private POST instance.
        horizon: Boundary used to construct durable pre-H staged rows.

    Returns:
        Lock, routing, exact generation-bound worklog, and staging evidence.
    """
    mutator = await _connect(
        instance,
        "retention-old-generation-mutator",
        command_timeout=_PUBLISHER_STOP_TIMEOUT_SECONDS,
    )
    latch = await _connect(
        instance,
        "retention-generation-latch",
        command_timeout=_PUBLISHER_STOP_TIMEOUT_SECONDS,
    )
    monitor = await _connect(
        instance,
        "retention-generation-monitor",
        command_timeout=_PUBLISHER_STOP_TIMEOUT_SECONDS,
    )
    latch_task: asyncio.Task[tuple[int, datetime]] | None = None
    old_work_id = uuid5(_IDENTITY_NAMESPACE, "retention-generation-old")
    new_work_id = uuid5(_IDENTITY_NAMESPACE, "retention-generation-new")
    old_public_id: UUID | None = None
    new_public_id: UUID | None = None
    try:
        await mutator.execute("BEGIN")
        old_generation = cast(
            int,
            await mutator.fetchval(
                "SELECT generation FROM rehearsal.retention_generation WHERE singleton FOR SHARE"
            ),
        )
        old_public_id, old_physical_table = await _insert_live_retention_row(
            mutator,
            horizon,
            old_work_id,
            old_generation,
        )
        barrier_fixtures = await _insert_retention_barrier_fixtures(
            mutator,
            horizon,
            old_generation,
            old_public_id,
        )
        latch_task = asyncio.create_task(_advance_retention_generation(latch))
        lock_observation = await _wait_backend(
            monitor,
            "retention-generation-latch",
            "UPDATE rehearsal.retention_generation",
            "Lock",
        )
        _assert(not latch_task.done(), "generation latch did not serialize behind old mutator")
        await mutator.execute("COMMIT")
        new_generation, latched_at = await asyncio.wait_for(
            latch_task,
            timeout=_PUBLISHER_STOP_TIMEOUT_SECONDS,
        )
        async with mutator.transaction():
            observed_generation = cast(
                int,
                await mutator.fetchval("""
                    SELECT generation
                    FROM rehearsal.retention_generation
                    WHERE singleton
                    FOR SHARE
                    """),
            )
            new_public_id = await _insert_staged_replay(
                mutator,
                horizon,
                new_work_id,
                observed_generation,
                2,
            )
        _assert(new_generation == old_generation + 1, "retention generation did not advance once")
        _assert(
            observed_generation == new_generation,
            "post-latch mutator did not observe the new generation",
        )
        staged_live_rows = cast(
            int,
            await monitor.fetchval(
                "SELECT count(*)::integer FROM trades WHERE public_id = $1",
                new_public_id,
            ),
        )
        _assert(
            old_physical_table == "trades_legacy",
            "pre-generation direct mutation did not commit into attached legacy",
        )
        _assert(
            staged_live_rows == 0,
            "post-generation pre-H staged replay leaked into the mutable legacy table",
        )
        staged_row = await monitor.fetchrow(
            """
            SELECT obligation_generation, state
            FROM rehearsal.staged_replay
            WHERE work_id = $1
            """,
            new_work_id,
        )
        _assert(staged_row is not None, "post-generation staged replay row is missing")
        _assert(
            cast(int, staged_row["obligation_generation"]) == new_generation
            and cast(str, staged_row["state"]) == "PENDING",
            "post-generation replay was not durably staged under the new generation",
        )
        worklog_rows = await monitor.fetch(
            """
            SELECT work_id, obligation_generation, public_id, destination
            FROM rehearsal.obligation_worklog
            WHERE work_id = ANY($1::uuid[])
            ORDER BY work_id
            """,
            [old_work_id, new_work_id],
        )
        observed_worklog = {
            cast(UUID, row["work_id"]): (
                cast(int, row["obligation_generation"]),
                cast(UUID, row["public_id"]),
                cast(str, row["destination"]),
            )
            for row in worklog_rows
        }
        expected_worklog = {
            old_work_id: (old_generation, old_public_id, "LIVE"),
            new_work_id: (new_generation, new_public_id, "STAGED"),
        }
        worklog_matches = observed_worklog == expected_worklog
        _assert(
            worklog_matches,
            "direct-mutator obligation worklog generations or identities do not match",
        )
        return {
            "latch_backend_pid": cast(int, lock_observation["pid"]),
            "latch_wait_event": cast(str | None, lock_observation["wait_event"]),
            "latch_wait_event_type": cast(str | None, lock_observation["wait_event_type"]),
            "latched_at": latched_at.isoformat(),
            "barrier_fixtures": barrier_fixtures,
            "new_generation": new_generation,
            "new_work_id": str(new_work_id),
            "old_generation": old_generation,
            "old_row_physical_table": old_physical_table,
            "old_work_id": str(old_work_id),
            "obligation_worklog_matches": worklog_matches,
            "obligation_worklog_rows": len(worklog_rows),
            "post_latch_mutator_generation": observed_generation,
            "post_generation_row_absent_from_live": staged_live_rows == 0,
            "post_generation_row_staged_pending": cast(str, staged_row["state"]) == "PENDING",
            "serialized_before_latch": True,
        }
    finally:
        try:
            if mutator.is_in_transaction():
                await mutator.execute("ROLLBACK")
        finally:
            try:
                await _settle_generation_latch(latch_task, latch)
            finally:
                await _close_connection_bounded(monitor)
                await _close_connection_bounded(latch)
                await _close_connection_bounded(mutator)


async def _concurrent_detach_negative(connection: Connection) -> tuple[str, str]:
    """Require DEFAULT to reject DETACH CONCURRENTLY for its exact reason.

    Args:
        connection: POST-cutover autocommit connection.

    Returns:
        PostgreSQL SQLSTATE and message from the required rejection.
    """
    sqlstate = ""
    message = ""
    try:
        await connection.execute("ALTER TABLE trades DETACH PARTITION trades_legacy CONCURRENTLY")
    except asyncpg.PostgresError as exc:
        sqlstate = exc.sqlstate or ""
        message = str(exc)
    _assert(sqlstate == "55000", "concurrent DETACH failed with wrong SQLSTATE")
    _assert(
        "default partition" in message.lower(),
        "concurrent DETACH did not fail for the DEFAULT-specific reason",
    )
    still_attached = cast(
        bool,
        await connection.fetchval("""
            SELECT EXISTS (
                SELECT 1
                FROM pg_inherits
                WHERE inhrelid = 'trades_legacy'::regclass
                  AND inhparent = 'trades'::regclass
            )
            """),
    )
    _assert(still_attached, "failed concurrent DETACH changed attachment")
    return sqlstate, message


async def _set_u3_pending(
    connection: Connection,
    horizon: datetime,
    lease: FenceLease,
    minimum_remaining_seconds: float,
) -> CatalogState:
    """Commit U3_PENDING while locking and revalidating the exact active fence.

    Args:
        connection: POST-cutover autocommit connection.
        horizon: Exact attached legacy upper bound.
        lease: Exact publisher fence generation and token.
        minimum_remaining_seconds: Required database-clock lifetime after the transition.

    Returns:
        Coherent attached and U3_PENDING catalog state.
    """
    async with connection.transaction():
        await connection.execute("SET LOCAL transaction_timeout = '1s'")
        fence = await _lock_fence_authority(connection, lease.generation)
        _assert(cast(UUID, fence["token"]) == lease.token, "U3_PENDING fence token mismatch")
        _assert(fence["resolution"] is None, "U3_PENDING fence is already terminal")
        remaining = await _remaining_lease_seconds(
            connection,
            cast(datetime, fence["lease_expires_at"]),
        )
        _assert(
            remaining > minimum_remaining_seconds,
            "U3_PENDING fence lacks the required database-clock lifetime",
        )
        updated = await connection.fetchval("""
            UPDATE rehearsal.topology_epoch
            SET topology = 'U3_PENDING'
            WHERE singleton
              AND topology = 'PARTITIONED'
            RETURNING topology
            """)
    _assert(updated == "U3_PENDING", "could not enter U3_PENDING from PARTITIONED")
    state = await _catalog_state(connection, horizon)
    _assert(state.mode == "PENDING", "U3_PENDING did not produce its exact attached quadrant")
    return state


async def _retention_barrier_snapshot(
    connection: Connection,
    retention_generation: int,
    lease: FenceLease,
    config: RunConfig,
    horizon: datetime,
) -> dict[str, object]:
    """Read every day-30 precondition from one machine-queryable snapshot.

    Args:
        connection: Control connection while publishers are fenced.
        retention_generation: Closed-set generation selected by the latch.
        lease: Exact active publisher fence.
        config: Publisher population, transaction timeout, and cleanup margin.
        horizon: Fixed safe cutoff required from both monitor cursors.

    Returns:
        Counts and positive cutoff evidence used by the rejecting gate.
    """
    row = await connection.fetchrow(
        """
        SELECT
            (
                SELECT count(*)::integer
                FROM rehearsal.retention_blocked_batches
                WHERE state = 'BLOCKED'
                  AND obligation_generation < $1
            ) AS blocked_batches,
            (
                SELECT count(*)::integer
                FROM rehearsal.obligation_worklog
                WHERE settled_at IS NULL
                  AND obligation_generation < $1
            ) AS pre_generation_obligations,
            (
                SELECT count(*)::integer
                FROM rehearsal.retention_monitor_worklog
                WHERE state = 'PENDING'
                  AND obligation_generation < $1
            ) AS worklog_pending,
            (
                SELECT CASE
                    WHEN count(*) = 2
                     AND min(fixed_safe_cutoff) = max(fixed_safe_cutoff)
                    THEN min(fixed_safe_cutoff)
                END
                FROM rehearsal.retention_monitor_scans
            ) AS fixed_safe_cutoff,
            (
                SELECT scanned_through
                FROM rehearsal.retention_monitor_scans
                WHERE monitor = 'M1'
            ) AS m1_cursor,
            (
                SELECT scanned_through
                FROM rehearsal.retention_monitor_scans
                WHERE monitor = 'M2'
            ) AS m2_cursor,
            EXISTS (
                SELECT 1
                FROM rehearsal.retention_monitor_lag
            ) AS monitor_lag_breach,
            (
                EXISTS (
                    SELECT 1
                    FROM rehearsal.retention_monitor_lag
                    WHERE resolved_at IS NOT NULL
                )
                AND (
                    SELECT count(*)::integer
                    FROM rehearsal.retention_monitor_scans
                    WHERE backscan_complete
                      AND scanned_through >= $3
                ) = 2
            ) AS backscan_complete,
            (
                SELECT count(*)::integer
                FROM rehearsal.conflict_quarantine
                WHERE resolved_at IS NULL
            ) AS unresolved_quarantine,
            (
                SELECT count(*)::integer
                FROM rehearsal.staged_replay
                WHERE state = 'PENDING'
                  AND obligation_generation < $1
            ) AS actual_pre_generation_pending,
            (
                SELECT count(*)::integer
                FROM rehearsal.publisher_acks
                WHERE generation = $2
                  AND session_closed
                  AND inflight = 0
            ) AS fence_acknowledgements,
            (
                SELECT count(*)::integer
                FROM rehearsal.publisher_acks
                WHERE generation = $2
                  AND retention_generation <> $1
            ) AS wrong_generation_acknowledgements,
            (
                SELECT EXTRACT(
                    EPOCH FROM (lease_expires_at - clock_timestamp())
                )::double precision
                FROM rehearsal.fence_generations
                WHERE generation = $2
            ) AS remaining_lease_seconds,
            (
                SELECT resolution
                FROM rehearsal.fence_generations
                WHERE generation = $2
            ) AS fence_resolution,
            (
                SELECT token::text
                FROM rehearsal.fence_generations
                WHERE generation = $2
            ) AS fence_token
        """,
        retention_generation,
        lease.generation,
        horizon,
    )
    _assert(row is not None, "retention barrier state disappeared")
    _assert(cast(str, row["fence_token"]) == str(lease.token), "barrier fence token changed")
    minimum_remaining_seconds = config.transaction_timeout_seconds + config.cleanup_margin_seconds
    return {
        "actual_pre_generation_pending": cast(int, row["actual_pre_generation_pending"]),
        "backscan_complete": cast(bool, row["backscan_complete"]),
        "blocked_batches": cast(int, row["blocked_batches"]),
        "expected_fence_acknowledgements": config.writers,
        "fence_acknowledgements": cast(int, row["fence_acknowledgements"]),
        "fence_generation": lease.generation,
        "fence_resolution": cast(str | None, row["fence_resolution"]),
        "fence_token": cast(str, row["fence_token"]),
        "fixed_safe_cutoff": (
            cast(datetime, row["fixed_safe_cutoff"]).isoformat()
            if row["fixed_safe_cutoff"] is not None
            else None
        ),
        "horizon": horizon.isoformat(),
        "m1_cursor": (
            cast(datetime, row["m1_cursor"]).isoformat() if row["m1_cursor"] is not None else None
        ),
        "m2_cursor": (
            cast(datetime, row["m2_cursor"]).isoformat() if row["m2_cursor"] is not None else None
        ),
        "monitor_lag_breach": cast(bool, row["monitor_lag_breach"]),
        "pre_generation_obligations": cast(int, row["pre_generation_obligations"]),
        "required_lease_seconds": minimum_remaining_seconds,
        "remaining_lease_seconds": cast(float, row["remaining_lease_seconds"]),
        "retention_generation": retention_generation,
        "unresolved_quarantine": cast(int, row["unresolved_quarantine"]),
        "worklog_pending": cast(int, row["worklog_pending"]),
        "wrong_generation_acknowledgements": cast(
            int,
            row["wrong_generation_acknowledgements"],
        ),
    }


def _retention_barrier_blockers(snapshot: dict[str, object]) -> list[str]:
    """Return exact failing day-30 preconditions for one snapshot.

    Args:
        snapshot: Values returned by the retention barrier query.

    Returns:
        Empty only when DETACH is mechanically authorized.
    """
    blockers: list[str] = []
    zero_fields = (
        "actual_pre_generation_pending",
        "blocked_batches",
        "pre_generation_obligations",
        "unresolved_quarantine",
        "worklog_pending",
        "wrong_generation_acknowledgements",
    )
    for field_name in zero_fields:
        if int(cast(int, snapshot[field_name])) != 0:
            blockers.append(field_name)
    if snapshot["fence_acknowledgements"] != snapshot["expected_fence_acknowledgements"]:
        blockers.append("fence_acknowledgements")
    if snapshot["fence_resolution"] is not None:
        blockers.append("fence_resolution")
    if float(cast(float | int, snapshot["remaining_lease_seconds"])) <= float(
        cast(float | int, snapshot["required_lease_seconds"])
    ):
        blockers.append("lease_window")
    horizon = snapshot["horizon"]
    if snapshot["fixed_safe_cutoff"] != horizon:
        blockers.append("fixed_safe_cutoff")
    if snapshot["m1_cursor"] != horizon:
        blockers.append("m1_cursor")
    if snapshot["m2_cursor"] != horizon:
        blockers.append("m2_cursor")
    if cast(bool, snapshot["monitor_lag_breach"]) and not cast(
        bool,
        snapshot["backscan_complete"],
    ):
        blockers.append("backscan_complete")
    return blockers


def _command_row_count(command_tag: str) -> int:
    """Parse the affected-row count from an asyncpg command tag.

    Args:
        command_tag: PostgreSQL command status such as ``UPDATE 3``.

    Returns:
        Nonnegative affected-row count.
    """
    count = int(command_tag.rsplit(" ", 1)[-1])
    _assert(count >= 0, "PostgreSQL command tag returned a negative row count")
    return count


def _require_retention_barrier(snapshot: dict[str, object]) -> None:
    """Reject DETACH whenever any acknowledged barrier fact is missing.

    Args:
        snapshot: One atomic barrier evidence dictionary.

    Returns:
        None only for a fully closed pre-generation set.
    """
    blockers = _retention_barrier_blockers(snapshot)
    _assert(not blockers, "retention barrier blocked DETACH: " + ", ".join(blockers))


async def _drain_retention_obligations(
    connection: Connection,
    retention_generation: int,
) -> dict[str, object]:
    """Drain real pre-generation batch and worklog records under one transaction.

    Args:
        connection: Control connection after the closed generation is latched.
        retention_generation: Closed-set generation selected by the latch.

    Returns:
        Insert routing and exact affected-row evidence.
    """
    async with connection.transaction():
        blocked = await connection.fetchrow(
            """
            SELECT *
            FROM rehearsal.retention_blocked_batches
            WHERE state = 'BLOCKED'
              AND obligation_generation < $1
            FOR UPDATE
            """,
            retention_generation,
        )
        _assert(blocked is not None, "retention fixture has no real blocked batch to drain")
        inserted = await connection.fetchrow(
            """
            INSERT INTO trades (
                public_id,
                instrument_public_id,
                trade_id,
                price,
                size,
                side,
                executed_at,
                session_id,
                sequence_id,
                "timestamp",
                known_to
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
            RETURNING public_id, tableoid::regclass::text AS physical_table
            """,
            blocked["public_id"],
            blocked["instrument_public_id"],
            blocked["trade_id"],
            blocked["price"],
            blocked["size"],
            blocked["side"],
            blocked["executed_at"],
            blocked["session_id"],
            blocked["sequence_id"],
            blocked["timestamp"],
            blocked["known_to"],
        )
        _assert(inserted is not None, "blocked batch drain inserted no legacy row")
        await connection.execute(
            """
            INSERT INTO rehearsal.obligation_worklog (
                work_id,
                obligation_generation,
                public_id,
                destination,
                settled_at
            )
            VALUES ($1, $2, $3, 'LIVE', clock_timestamp())
            """,
            blocked["batch_id"],
            blocked["obligation_generation"],
            blocked["public_id"],
        )
        drained_batches = _command_row_count(
            await connection.execute(
                """
                UPDATE rehearsal.retention_blocked_batches
                SET state = 'DRAINED', drained_at = clock_timestamp()
                WHERE batch_id = $1
                  AND state = 'BLOCKED'
                """,
                blocked["batch_id"],
            )
        )
        settled_obligations = _command_row_count(
            await connection.execute(
                """
                UPDATE rehearsal.obligation_worklog work
                SET settled_at = clock_timestamp()
                WHERE work.obligation_generation < $1
                  AND work.destination = 'LIVE'
                  AND work.settled_at IS NULL
                  AND EXISTS (
                      SELECT 1
                      FROM trades_legacy legacy
                      WHERE legacy.public_id = work.public_id
                        AND legacy.known_to = $2
                  )
                """,
                retention_generation,
                _KNOWN_TO,
            )
        )
        drained_worklog = _command_row_count(
            await connection.execute(
                """
                UPDATE rehearsal.retention_monitor_worklog work
                SET state = 'DRAINED', drained_at = clock_timestamp()
                WHERE work.obligation_generation < $1
                  AND work.state = 'PENDING'
                  AND EXISTS (
                      SELECT 1
                      FROM trades_legacy legacy
                      WHERE legacy.public_id = work.public_id
                        AND legacy.known_to = $2
                  )
                """,
                retention_generation,
                _KNOWN_TO,
            )
        )
    _assert(
        cast(str, inserted["physical_table"]) == "trades_legacy",
        "blocked pre-generation batch did not drain into attached legacy",
    )
    _assert(drained_batches == 1, "barrier did not drain exactly one blocked batch")
    _assert(settled_obligations == 1, "barrier did not settle exactly one live obligation")
    _assert(drained_worklog == 1, "barrier did not drain exactly one monitor work item")
    return {
        "blocked_batch_physical_table": cast(str, inserted["physical_table"]),
        "drained_blocked_batches": drained_batches,
        "drained_monitor_worklog": drained_worklog,
        "settled_live_obligations": settled_obligations,
    }


async def _record_retention_scans(
    connection: Connection,
    horizon: datetime,
) -> dict[str, int]:
    """Execute and persist real M1/M2 scans plus the required lag backscan.

    Args:
        connection: Control connection while the legacy partition is immutable.
        horizon: Fixed safe cutoff both monitors must reach.

    Returns:
        Rows observed by each monitor and each backscan.
    """
    results: dict[str, int] = {}
    async with connection.transaction():
        earliest_unsafe_value = await connection.fetchval("""
            SELECT earliest_unsafe
            FROM rehearsal.retention_monitor_lag
            WHERE singleton
              AND resolved_at IS NULL
            FOR UPDATE
            """)
        _assert(
            isinstance(earliest_unsafe_value, datetime),
            "retention lag fixture has no unresolved earliest-unsafe instant",
        )
        earliest_unsafe = cast(datetime, earliest_unsafe_value)
        for monitor in ("M1", "M2"):
            scan = await connection.fetchrow(
                """
                SELECT
                    count(*)::bigint AS rows_seen,
                    count(*) FILTER (
                        WHERE executed_at >= $1
                          AND executed_at < $2
                    )::bigint AS backscan_rows
                FROM trades_legacy
                WHERE executed_at < $2
                """,
                earliest_unsafe,
                horizon,
            )
            _assert(scan is not None, f"{monitor} retention scan returned no evidence")
            await connection.execute(
                """
                INSERT INTO rehearsal.retention_monitor_scans (
                    monitor,
                    fixed_safe_cutoff,
                    scanned_through,
                    rows_seen,
                    backscan_rows,
                    backscan_complete
                )
                VALUES ($1, $2, $2, $3, $4, true)
                """,
                monitor,
                horizon,
                scan["rows_seen"],
                scan["backscan_rows"],
            )
            results[f"{monitor.lower()}_rows_seen"] = cast(int, scan["rows_seen"])
            results[f"{monitor.lower()}_backscan_rows"] = cast(int, scan["backscan_rows"])
        resolved = _command_row_count(await connection.execute("""
                UPDATE rehearsal.retention_monitor_lag
                SET resolved_at = clock_timestamp()
                WHERE singleton
                  AND resolved_at IS NULL
                """))
    _assert(resolved == 1, "monitor-lag breach did not resolve through the persisted backscan")
    _assert(
        all(value > 0 for value in results.values()),
        "M1/M2 safe-cutoff or backscan query exercised no fixture rows",
    )
    return results


async def _reject_retention_barrier(
    context: RetentionBarrierContext,
    lease: FenceLease,
) -> dict[str, object]:
    """Require one fenced day-30 attempt to reject durable blockers.

    Args:
        context: Instance, config, connection, generation, and horizon.
        lease: Active negative-attempt publisher fence.

    Returns:
        Initial blocker snapshot and exact rejecting error.
    """
    initial = await _retention_barrier_snapshot(
        context.connection,
        context.retention_generation,
        lease,
        context.config,
        context.horizon,
    )
    initial_blockers = _retention_barrier_blockers(initial)
    _assert(bool(initial_blockers), "negative retention barrier fixture had no reason to fail")
    _assert(
        int(cast(int, initial["blocked_batches"])) == 1
        and int(cast(int, initial["pre_generation_obligations"])) == 1
        and int(cast(int, initial["worklog_pending"])) == 1
        and int(cast(int, initial["unresolved_quarantine"])) > 0,
        "retention barrier negative control lacks real durable blockers",
    )
    rejection = ""
    try:
        _require_retention_barrier(initial)
    except RehearsalError as exc:
        rejection = str(exc)
    _assert(bool(rejection), "retention barrier negative control did not reject DETACH")
    return {
        "initial_blockers": initial_blockers,
        "initial_snapshot": initial,
        "negative_rejection": rejection,
    }


async def _release_rejected_retention_fence(
    connection: Connection,
    lease: FenceLease,
    writer_count: int,
    horizon: datetime,
    stats: Sequence[WriterStats],
) -> dict[str, object]:
    """Release a rejected attempt and prove ingestion resumes before cleanup.

    Args:
        connection: Autocommit retention control connection.
        lease: Rejected active publisher fence.
        writer_count: Exact configured publisher population.
        horizon: Expected unchanged POST catalog boundary.
        stats: Live publisher counters used for the resume proof.

    Returns:
        Controller resolution, catalog fingerprint, and resume evidence.
    """
    state = await _catalog_state(connection, horizon)
    _assert(state.mode == "POST", "rejected retention barrier changed the POST catalog")
    released = await connection.fetchrow(
        """
        UPDATE rehearsal.fence_generations
        SET
            resolution = 'POST_COMMITTED',
            resolver = 'controller',
            resolved_fingerprint = $2,
            resolved_at = clock_timestamp()
        WHERE generation = $1
          AND token = $3
          AND resolution IS NULL
        RETURNING generation, token, resolved_at
        """,
        lease.generation,
        state.fingerprint(),
        lease.token,
    )
    _assert(released is not None, "rejected retention fence could not be released")
    released_row = cast(Record, released)
    resumes = await _wait_resumes(connection, lease.generation, writer_count, stats)
    _assert(
        all(cast(str, resume["event"]) == "RESUMED_CONTROLLER" for resume in resumes),
        "rejected retention fence did not resume through controller release",
    )
    commits_before = {
        f"publisher-{number}": item.committed for number, item in enumerate(stats, start=1)
    }
    target = max(commits_before.values()) + 1
    await _wait_writer_commits(stats, target)
    commits_after = {
        f"publisher-{number}": item.committed for number, item in enumerate(stats, start=1)
    }
    last_resume_at = max(cast(datetime, resume["occurred_at"]) for resume in resumes)
    return {
        "catalog_fingerprint": state.fingerprint(),
        "fence_generation": lease.generation,
        "fence_token": str(cast(UUID, released_row["token"])),
        "last_resume_at": last_resume_at.isoformat(),
        "resolution": "POST_COMMITTED",
        "resolved_at": cast(datetime, released_row["resolved_at"]).isoformat(),
        "resume_count": len(resumes),
        "writer_commit_target": target,
        "writer_commits_after": commits_after,
        "writer_commits_before": commits_before,
    }


async def _clear_retention_barrier(
    connection: Connection,
    retention_generation: int,
    horizon: datetime,
) -> dict[str, object]:
    """Clear the latched finite set between bounded fence attempts.

    Args:
        connection: Control connection while publishers use the new generation.
        retention_generation: Closed pre-generation set being drained.
        horizon: Fixed cutoff for both real monitor scans.

    Returns:
        Disposition, obligation drain, and M1/M2 scan evidence.
    """
    remediation_started_at = cast(
        datetime,
        await connection.fetchval("SELECT clock_timestamp()"),
    )
    active_before = cast(
        int,
        await connection.fetchval("""
            SELECT count(*)::integer
            FROM rehearsal.fence_generations
            WHERE resolution IS NULL
              AND clock_timestamp() < lease_expires_at
            """),
    )
    _assert(active_before == 0, "barrier clearance began under an active publisher fence")
    disposition_count = _command_row_count(await connection.execute("""
            UPDATE rehearsal.conflict_quarantine
            SET resolved_at = clock_timestamp()
            WHERE resolved_at IS NULL
            """))
    obligation_drain = await _drain_retention_obligations(
        connection,
        retention_generation,
    )
    monitor_scans = await _record_retention_scans(connection, horizon)
    active_after = cast(
        int,
        await connection.fetchval("""
            SELECT count(*)::integer
            FROM rehearsal.fence_generations
            WHERE resolution IS NULL
              AND clock_timestamp() < lease_expires_at
            """),
    )
    _assert(active_after == 0, "publisher fence became active during barrier clearance")
    remediation_completed_at = cast(
        datetime,
        await connection.fetchval("SELECT clock_timestamp()"),
    )
    historical_overlaps = cast(
        int,
        await connection.fetchval(
            """
            SELECT count(*)::integer
            FROM rehearsal.fence_generations
            WHERE opened_at < $2
              AND LEAST(
                    lease_expires_at,
                    COALESCE(resolved_at, lease_expires_at)
                  ) > $1
            """,
            remediation_started_at,
            remediation_completed_at,
        ),
    )
    _assert(
        historical_overlaps == 0,
        "publisher fence interval overlapped barrier clearance",
    )
    _assert(disposition_count > 0, "barrier did not disposition a real quarantine row")
    return {
        "active_fences_after_clearance": active_after,
        "active_fences_before_clearance": active_before,
        "cleared_retention_generation": retention_generation,
        "dispositioned_quarantine_rows": disposition_count,
        "historical_fence_overlaps": historical_overlaps,
        "monitor_scans": monitor_scans,
        "obligation_drain": obligation_drain,
        "remediation_completed_at": remediation_completed_at.isoformat(),
        "remediation_started_at": remediation_started_at.isoformat(),
    }


async def _prepare_retention_barrier(
    context: RetentionBarrierContext,
) -> dict[str, object]:
    """Reject one bounded attempt and clear its closed generation after resume.

    Args:
        context: Instance, config, controller, generation, H, and publishers.

    Returns:
        Negative-attempt, controller-resume, and durable clearance evidence.
    """
    required_seconds = (
        context.config.transaction_timeout_seconds + context.config.cleanup_margin_seconds
    )
    negative_lease = await _open_fence(
        context.instance,
        context.config,
        "retention-barrier-negative",
    )
    negative_acknowledgements = await _wait_fence_acks(
        context.instance,
        negative_lease,
        context.config.writers,
        required_seconds,
        context.retention_generation,
    )
    rejection = await _reject_retention_barrier(
        context,
        negative_lease,
    )
    retry = await _release_rejected_retention_fence(
        context.connection,
        negative_lease,
        context.config.writers,
        context.horizon,
        context.stats,
    )
    clearance = await _clear_retention_barrier(
        context.connection,
        context.retention_generation,
        context.horizon,
    )
    return {
        **clearance,
        **rejection,
        "negative_fence_acknowledgements": negative_acknowledgements,
        "negative_fence_generation": negative_lease.generation,
        "negative_fence_token": str(negative_lease.token),
        "negative_retry": retry,
    }


async def _finalize_retention_barrier(
    context: RetentionBarrierContext,
    preparation: dict[str, object],
) -> tuple[FenceLease, list[dict[str, object]], dict[str, object]]:
    """Acquire a fresh final fence and rerun every persisted barrier predicate.

    Args:
        context: Instance, config, controller, generation, H, and publishers.
        preparation: Negative-attempt and scale-scan evidence.

    Returns:
        Fresh final lease, exact acknowledgements, and merged passing evidence.
    """
    required_seconds = (
        context.config.transaction_timeout_seconds + context.config.cleanup_margin_seconds
    )
    final_lease = await _open_fence(context.instance, context.config, "retention")
    final_acknowledgements = await _wait_fence_acks(
        context.instance,
        final_lease,
        context.config.writers,
        required_seconds,
        context.retention_generation,
    )
    final = await _retention_barrier_snapshot(
        context.connection,
        context.retention_generation,
        final_lease,
        context.config,
        context.horizon,
    )
    _require_retention_barrier(final)
    return (
        final_lease,
        final_acknowledgements,
        {
            **preparation,
            "final_blockers": _retention_barrier_blockers(final),
            "final_fence_generation": final_lease.generation,
            "final_lease_opened_at": final_lease.opened_at.isoformat(),
            "final_fence_token": str(final_lease.token),
            "final_snapshot": final,
            "passed": True,
        },
    )


async def _prove_pending_reconciliation(
    instance: InstanceRef,
    config: RunConfig,
    connection: Connection,
    horizon: datetime,
    stats: Sequence[WriterStats],
) -> dict[str, object]:
    """Expire an attached U3_PENDING fence and require one targetless CAS resolver.

    Args:
        instance: Verified private PostgreSQL instance.
        config: Fence lease bounds and publisher count.
        connection: Autocommit catalog observer.
        horizon: Exact attached legacy upper bound.
        stats: Live publisher counters used to prove resumed ingestion.

    Returns:
        Pending state, exact CAS winner, acknowledgements, and resume evidence.
    """
    retention_generation = cast(
        int,
        await connection.fetchval(
            "SELECT generation FROM rehearsal.retention_generation WHERE singleton"
        ),
    )
    lease = await _open_fence(instance, config, "retention-pending-recovery")
    acknowledgements = await _wait_fence_acks(
        instance,
        lease,
        config.writers,
        config.transaction_timeout_seconds + config.cleanup_margin_seconds,
        retention_generation,
    )
    pending_state = await _set_u3_pending(connection, horizon, lease, 0.5)
    resumes = await _wait_resumes(
        connection,
        lease.generation,
        config.writers,
        stats,
    )
    commits_before = {
        f"publisher-{number}": item.committed for number, item in enumerate(stats, start=1)
    }
    target = max(commits_before.values()) + 1
    await _wait_writer_commits(stats, target)
    commits_after = {
        f"publisher-{number}": item.committed for number, item in enumerate(stats, start=1)
    }
    terminal = await _catalog_state(connection, horizon)
    _assert(terminal.mode == "POST", "expired U3_PENDING fence did not restore POST")
    winner_rows = await connection.fetch(
        """
        SELECT resolver
        FROM rehearsal.retention_resolution_winners
        WHERE generation = $1
        """,
        lease.generation,
    )
    _assert(len(winner_rows) == 1, "U3_PENDING generation did not have exactly one CAS winner")
    fence = await connection.fetchrow(
        """
        SELECT resolution, resolver, token, resolved_at
        FROM rehearsal.fence_generations
        WHERE generation = $1
        """,
        lease.generation,
    )
    _assert(fence is not None, "U3_PENDING recovery fence disappeared")
    _assert(fence["resolution"] == "POST_COMMITTED", "U3_PENDING resolved incorrectly")
    last_resume_at = max(cast(datetime, resume["occurred_at"]) for resume in resumes)
    stale_rejection = ""
    try:
        await _set_u3_pending(connection, horizon, lease, 0.0)
    except RehearsalError as exc:
        stale_rejection = str(exc)
    _assert(bool(stale_rejection), "expired U3_PENDING setter was not rejected")
    stable_terminal = await _catalog_state(connection, horizon)
    _assert(stable_terminal.mode == "POST", "stale U3_PENDING setter changed terminal POST")
    return {
        "acknowledgements": acknowledgements,
        "cas_winner": cast(str, winner_rows[0]["resolver"]),
        "fence_generation": lease.generation,
        "fence_token": str(lease.token),
        "last_resume_at": last_resume_at.isoformat(),
        "observed_fence_token": str(cast(UUID, fence["token"])),
        "opened_at": lease.opened_at.isoformat(),
        "pending_fingerprint": pending_state.fingerprint(),
        "resolution": cast(str, fence["resolution"]),
        "resolved_at": cast(datetime, fence["resolved_at"]).isoformat(),
        "resolver": cast(str, fence["resolver"]),
        "resume_count": len(resumes),
        "stale_pending_rejection": stale_rejection,
        "stale_pending_setter_denied": True,
        "terminal_catalog": terminal.mode,
        "winner_count": len(winner_rows),
        "writer_commit_target": target,
        "writer_commits_after": commits_after,
        "writer_commits_before": commits_before,
    }


async def _authorized_detach_attempt(
    connection: Connection,
    spec: ControllerSpec,
    lock_timeout: str,
    authorization_evidence: dict[str, object],
) -> dict[str, object]:
    """Run one DETACH attempt under the same locked lease row as reconciliation.

    Args:
        connection: Deliberately non-UTC DDL connection.
        spec: Exact fence generation and token authority.
        lock_timeout: PostgreSQL duration for this bounded attempt.
        authorization_evidence: Mutable facts retained even after rollback.

    Returns:
        In-transaction database-clock lease and timezone evidence.
    """
    transaction = connection.transaction()
    await transaction.start()
    try:
        backend_pid, remaining, initial_timezone, pinned_timezone = await _authorize_controller(
            connection, spec
        )
        recorded = await connection.fetchval(
            """
            UPDATE rehearsal.fence_generations
            SET ddl_pid = $3
            WHERE generation = $1
              AND token = $2
              AND resolution IS NULL
            RETURNING generation
            """,
            spec.generation,
            spec.token,
            backend_pid,
        )
        _assert(recorded is not None, "DETACH authority could not record its DDL backend")
        authorization_evidence.update(
            {
                "backend_pid": backend_pid,
                "cleanup_margin_seconds": spec.cleanup_margin_seconds,
                "fence_generation": spec.generation,
                "fence_token": str(spec.token),
                "initial_timezone": initial_timezone,
                "pinned_timezone": pinned_timezone,
                "remaining_lease_seconds": remaining,
                "transaction_timeout_seconds": spec.transaction_timeout_seconds,
            }
        )
        await connection.execute(f"SET LOCAL lock_timeout = '{lock_timeout}'")
        await connection.execute(
            "UPDATE rehearsal.topology_epoch SET topology = 'U3' WHERE singleton"
        )
        await connection.execute("ALTER TABLE trades DETACH PARTITION trades_legacy")
        await transaction.commit()
        return dict(authorization_evidence)
    except Exception:
        await transaction.rollback()
        raise


async def _plain_detach_with_retry(
    connection: Connection,
    blocker: Connection,
    spec: ControllerSpec,
) -> dict[str, object]:
    """Force one lock timeout, then complete lease-authorized plain DETACH.

    Args:
        connection: DDL connection.
        blocker: Independent ACCESS SHARE blocker connection.
        spec: Exact retention generation lease authority.

    Returns:
        Retry, lease, timezone, and atomic U3_PENDING rollback evidence.
    """
    await blocker.execute("BEGIN")
    await blocker.execute("LOCK TABLE trades IN ACCESS SHARE MODE")
    first_sqlstate = ""
    first_authorization: dict[str, object] = {}
    try:
        await _authorized_detach_attempt(connection, spec, "100ms", first_authorization)
    except asyncpg.PostgresError as exc:
        first_sqlstate = exc.sqlstate or ""
    _assert(first_sqlstate == "55P03", "first plain DETACH did not exercise lock timeout")
    epoch_after_timeout = cast(
        str,
        await connection.fetchval("SELECT topology FROM rehearsal.topology_epoch WHERE singleton"),
    )
    still_attached = cast(
        bool,
        await connection.fetchval("""
            SELECT EXISTS (
                SELECT 1
                FROM pg_inherits
                WHERE inhrelid = 'trades_legacy'::regclass
                  AND inhparent = 'trades'::regclass
            )
            """),
    )
    _assert(
        epoch_after_timeout == "U3_PENDING" and still_attached,
        "failed DETACH did not atomically restore its U3_PENDING quadrant",
    )
    await blocker.execute("ROLLBACK")
    attempts = 1
    while attempts < 3:
        attempts += 1
        successful_authorization: dict[str, object] = {}
        try:
            authorization = await _authorized_detach_attempt(
                connection,
                spec,
                "1s",
                successful_authorization,
            )
            return {
                "attempts": attempts,
                "epoch_after_failed_attempt": epoch_after_timeout,
                "failed_attempt_still_attached": still_attached,
                "first_authorization": first_authorization,
                "first_sqlstate": first_sqlstate,
                "successful_authorization": authorization,
                "successful_epoch": "U3",
            }
        except asyncpg.LockNotAvailableError:
            await asyncio.sleep(0.1 * attempts)
    raise RehearsalError("plain DETACH exhausted bounded retries")


def _archive_column_sql() -> str:
    """Return the immutable explicit archive column list.

    Returns:
        Comma-separated SQL identifiers in archive format order.
    """
    return ", ".join(
        f'"{column}"' if column == "timestamp" else column for column in _ARCHIVE_COLUMNS
    )


def _sha256_path(path: Path) -> str:
    """Hash one immutable evidence or source file without materializing it.

    Args:
        path: Immutable file to hash.

    Returns:
        Lowercase SHA-256 digest.
    """
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while chunk := handle.read(1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def _capture_source_snapshot(
    script_path: Path = _REHEARSAL_SCRIPT,
    test_path: Path = _REHEARSAL_TEST,
) -> SourceSnapshot:
    """Capture exact script and test digests for one complete rehearsal.

    Args:
        script_path: Harness source whose bytes must remain stable.
        test_path: Targeted test source whose bytes must remain stable.

    Returns:
        Lowercase SHA-256 digests for both required source files.
    """
    return SourceSnapshot(
        script_sha256=_sha256_path(script_path),
        test_sha256=_sha256_path(test_path),
    )


def _assert_source_snapshot(
    expected: SourceSnapshot,
    script_path: Path = _REHEARSAL_SCRIPT,
    test_path: Path = _REHEARSAL_TEST,
) -> None:
    """Fail when either rehearsal source changes after the initial capture.

    Args:
        expected: Initial digests carried through the supervisor and children.
        script_path: Harness source to compare with the initial script digest.
        test_path: Targeted test source to compare with the initial test digest.

    Returns:
        None when both current digests exactly match the initial snapshot.
    """
    _assert(
        _SHA256_PATTERN.fullmatch(expected.script_sha256) is not None,
        "invalid expected rehearsal script SHA-256",
    )
    _assert(
        _SHA256_PATTERN.fullmatch(expected.test_sha256) is not None,
        "invalid expected rehearsal test SHA-256",
    )
    observed = _capture_source_snapshot(script_path, test_path)
    _assert(
        observed == expected,
        (
            "rehearsal source drift detected: "
            f"expected script={expected.script_sha256} test={expected.test_sha256}; "
            f"observed script={observed.script_sha256} test={observed.test_sha256}"
        ),
    )


def _fsync_file(path: Path) -> None:
    """Flush one completed archive artifact to its backing filesystem.

    Args:
        path: Existing regular file.

    Returns:
        None after fsync succeeds.
    """
    with path.open("rb") as handle:
        os.fsync(handle.fileno())


def _fsync_directory(path: Path) -> None:
    """Flush archive directory entries before an irreversible DROP.

    Args:
        path: Existing archive directory.

    Returns:
        None after the directory fsync succeeds.
    """
    descriptor = os.open(path, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _assert_archive_headroom(paths: RuntimePaths, stage: str) -> int:
    """Require free space before every archive operation that can grow disk use.

    Args:
        paths: Run paths on the shared rehearsal filesystem.
        stage: Exact operation label retained in failure evidence.

    Returns:
        Observed free bytes before the stage.
    """
    free_bytes = shutil.disk_usage(paths.archive_dir).free
    _assert(
        free_bytes >= _ARCHIVE_STAGE_FREE_BYTES,
        f"archive stage {stage} has only {free_bytes} free bytes",
    )
    return free_bytes


async def _terminate_async_process(process: asyncio.subprocess.Process) -> None:
    """Terminate and reap one failed archive subprocess within a fixed bound.

    Args:
        process: zstd compressor or decompressor that must not outlive failure.

    Returns:
        None after the child has been reaped.
    """
    if process.returncode is not None:
        await asyncio.wait_for(process.wait(), timeout=_TASK_CANCEL_TIMEOUT_SECONDS)
        return
    process.terminate()
    try:
        await asyncio.wait_for(process.wait(), timeout=2.0)
    except TimeoutError:
        process.kill()
        try:
            await asyncio.wait_for(process.wait(), timeout=_TASK_CANCEL_TIMEOUT_SECONDS)
        except TimeoutError as reap_exc:
            raise RehearsalError(
                f"archive child PID {process.pid} resisted bounded SIGKILL reap"
            ) from reap_exc


async def _export_archive_day(
    connection: Connection,
    paths: RuntimePaths,
    lower: datetime,
    upper: datetime,
) -> dict[str, object]:
    """Stream one UTC legacy day through single-threaded zstd.

    Args:
        connection: Private throwaway database connection.
        paths: Run archive directory.
        lower: Inclusive UTC day bound.
        upper: Exclusive UTC day bound.

    Returns:
        Per-day row count, byte size, SHA-256, and explicit bounds.
    """
    day = lower.strftime("%Y-%m-%d")
    archive_path = paths.archive_dir / f"trades-{day}.csv.zst"
    _assert(not archive_path.exists(), f"archive path already exists: {archive_path}")
    compressor = await asyncio.create_subprocess_exec(
        *_niced_parent_bound_command(
            [
                "zstd",
                "-T1",
                "-3",
                "-q",
                "-o",
                str(archive_path),
            ]
        ),
        stdin=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    compressor_input = compressor.stdin
    if compressor_input is None:
        await _terminate_async_process(compressor)
        raise RehearsalError("zstd compressor stdin is unavailable")

    async def write_chunk(chunk: bytes) -> None:
        compressor_input.write(chunk)
        await compressor_input.drain()

    query = f"""
        SELECT {_archive_column_sql()}
        FROM trades_legacy
        WHERE executed_at >= {_horizon_literal(lower)}
          AND executed_at < {_horizon_literal(upper)}
        ORDER BY executed_at, id
    """
    try:
        status = await connection.copy_from_query(
            query,
            output=write_chunk,
            timeout=_ARCHIVE_SQL_TIMEOUT_SECONDS,
            format="csv",
            header=True,
        )
    except BaseException:
        compressor_input.close()
        await _terminate_async_process(compressor)
        raise
    compressor_input.close()
    try:
        await asyncio.wait_for(
            compressor_input.wait_closed(),
            timeout=_TASK_CANCEL_TIMEOUT_SECONDS,
        )
    except TimeoutError as exc:
        await _terminate_async_process(compressor)
        raise RehearsalError("zstd archive input pipe did not close") from exc
    except (BrokenPipeError, ConnectionResetError):
        await _terminate_async_process(compressor)
        raise
    try:
        stderr_bytes = await asyncio.wait_for(
            compressor.stderr.read() if compressor.stderr is not None else asyncio.sleep(0, b""),
            timeout=_TASK_CANCEL_TIMEOUT_SECONDS,
        )
        returncode = await asyncio.wait_for(
            compressor.wait(),
            timeout=_TASK_CANCEL_TIMEOUT_SECONDS,
        )
    except TimeoutError as exc:
        await _terminate_async_process(compressor)
        raise RehearsalError("zstd archive compression did not terminate") from exc
    stderr = stderr_bytes.decode("utf-8", errors="replace")
    _assert(returncode == 0, f"zstd archive compression failed: {stderr}")
    _fsync_file(archive_path)
    match = re.fullmatch(r"COPY (\d+)", status)
    if match is None:
        raise RehearsalError(f"unexpected archive COPY status: {status}")
    rows = int(match.group(1))
    return {
        "bytes": archive_path.stat().st_size,
        "day": day,
        "file": archive_path.name,
        "lower": lower.isoformat(),
        "rows": rows,
        "sha256": _sha256_path(archive_path),
        "upper": upper.isoformat(),
    }


async def _restore_archive_day(
    connection: Connection,
    archive_path: Path,
) -> int:
    """Stream one compressed archive day into the unconstrained restore table.

    Args:
        connection: Private throwaway restore connection.
        archive_path: Manifest-addressed zstd CSV.

    Returns:
        Number of rows PostgreSQL reports copied.
    """
    decompressor = await asyncio.create_subprocess_exec(
        *_niced_parent_bound_command(
            [
                "zstd",
                "-dc",
                str(archive_path),
            ]
        ),
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    decompressor_output = decompressor.stdout
    if decompressor_output is None:
        await _terminate_async_process(decompressor)
        raise RehearsalError("zstd decompressor stdout is unavailable")

    async def read_chunks() -> AsyncIterator[bytes]:
        while chunk := await decompressor_output.read(1024 * 1024):
            yield chunk

    try:
        status = await connection.copy_to_table(
            "trades_restore",
            schema_name="rehearsal",
            source=read_chunks(),
            columns=_ARCHIVE_COLUMNS,
            timeout=_ARCHIVE_SQL_TIMEOUT_SECONDS,
            format="csv",
            header=True,
        )
    except BaseException:
        await _terminate_async_process(decompressor)
        raise
    try:
        stderr_bytes = await asyncio.wait_for(
            (
                decompressor.stderr.read()
                if decompressor.stderr is not None
                else asyncio.sleep(0, b"")
            ),
            timeout=_TASK_CANCEL_TIMEOUT_SECONDS,
        )
        returncode = await asyncio.wait_for(
            decompressor.wait(),
            timeout=_TASK_CANCEL_TIMEOUT_SECONDS,
        )
    except TimeoutError as exc:
        await _terminate_async_process(decompressor)
        raise RehearsalError("zstd archive decompression did not terminate") from exc
    stderr = stderr_bytes.decode("utf-8", errors="replace")
    _assert(returncode == 0, f"zstd archive decompression failed: {stderr}")
    match = re.fullmatch(r"COPY (\d+)", status)
    if match is None:
        raise RehearsalError(f"unexpected restore COPY status: {status}")
    return int(match.group(1))


def _restore_table_sql() -> str:
    """Return the unconstrained full-column archive restore target.

    Returns:
        Schema-identical column DDL without uniqueness constraints.
    """
    return """
        CREATE UNLOGGED TABLE rehearsal.trades_restore (
            public_id uuid NOT NULL,
            "timestamp" timestamp with time zone NOT NULL,
            known_to timestamp with time zone NOT NULL,
            session_id uuid NOT NULL,
            sequence_id integer NOT NULL,
            instrument_public_id uuid NOT NULL,
            trade_id character varying(64),
            executed_at timestamp with time zone,
            price double precision NOT NULL,
            size double precision NOT NULL,
            side character varying(4) NOT NULL,
            id bigint NOT NULL
        )
    """


def _paper_replay_sql(target: str, source: str) -> str:
    """Build a deterministic 1-minute paper replay from one trade relation.

    Args:
        target: Harness-only aggregate table name.
        source: Legacy or restored source relation.

    Returns:
        CREATE TABLE AS SQL for exact OHLCV replay comparison.
    """
    _assert(
        target in {"paper_original", "paper_restored"},
        f"invalid paper replay target {target}",
    )
    _assert(
        source in {"trades_legacy", "rehearsal.trades_restore"},
        f"invalid paper replay source {source}",
    )
    return f"""
        CREATE UNLOGGED TABLE rehearsal.{target} AS
        SELECT
            instrument_public_id,
            date_trunc('minute', executed_at) AS bucket,
            (array_agg(price ORDER BY executed_at, id))[1] AS open,
            max(price) AS high,
            min(price) AS low,
            (array_agg(price ORDER BY executed_at DESC, id DESC))[1] AS close,
            sum(size ORDER BY executed_at, id) AS volume,
            count(*)::bigint AS trades
        FROM {source}
        GROUP BY instrument_public_id, date_trunc('minute', executed_at)
    """


async def _archive_equality(
    connection: Connection,
) -> tuple[int, int, int, int]:
    """Compare full restored rows and paper replay outputs in both directions.

    Args:
        connection: Private archive verification connection.

    Returns:
        Full-row missing/extra and paper replay missing/extra counts.
    """
    columns = _archive_column_sql()
    full_missing = cast(
        int,
        await connection.fetchval(f"""
            SELECT count(*)::bigint
            FROM (
                SELECT {columns} FROM trades_legacy
                EXCEPT ALL
                SELECT {columns} FROM rehearsal.trades_restore
            ) difference
            """),
    )
    full_extra = cast(
        int,
        await connection.fetchval(f"""
            SELECT count(*)::bigint
            FROM (
                SELECT {columns} FROM rehearsal.trades_restore
                EXCEPT ALL
                SELECT {columns} FROM trades_legacy
            ) difference
            """),
    )
    replay_columns = "instrument_public_id, bucket, open, high, low, close, volume, trades"
    replay_missing = cast(
        int,
        await connection.fetchval(f"""
            SELECT count(*)::bigint
            FROM (
                SELECT {replay_columns} FROM rehearsal.paper_original
                EXCEPT ALL
                SELECT {replay_columns} FROM rehearsal.paper_restored
            ) difference
            """),
    )
    replay_extra = cast(
        int,
        await connection.fetchval(f"""
            SELECT count(*)::bigint
            FROM (
                SELECT {replay_columns} FROM rehearsal.paper_restored
                EXCEPT ALL
                SELECT {replay_columns} FROM rehearsal.paper_original
            ) difference
            """),
    )
    return full_missing, full_extra, replay_missing, replay_extra


def _persist_archive_manifest(
    paths: RuntimePaths,
    horizon: datetime,
    manifests: Sequence[dict[str, object]],
) -> tuple[Path, str, list[dict[str, object]], int]:
    """Persist, fsync, reread, and structurally validate the archive manifest.

    Args:
        paths: Exact retained archive directory.
        horizon: Exclusive upper bound of the 56 archived days.
        manifests: Generated day records after every archive file is fsynced.

    Returns:
        Manifest path, digest, validated day records, and total rows.
    """
    manifest_rows = sum(int(cast(int, item["rows"])) for item in manifests)
    manifest_path = paths.archive_dir / "manifest.json"
    manifest_payload: dict[str, object] = {
        "columns": list(_ARCHIVE_COLUMNS),
        "days": list(manifests),
        "rows": manifest_rows,
        "table": "trades_legacy",
    }
    with manifest_path.open("x", encoding="utf-8") as handle:
        handle.write(json.dumps(manifest_payload, indent=2, sort_keys=True) + "\n")
        handle.flush()
        os.fsync(handle.fileno())
    _fsync_directory(paths.archive_dir)
    manifest_sha = _sha256_path(manifest_path)
    persisted_value: object = json.loads(manifest_path.read_text(encoding="utf-8"))
    _assert(isinstance(persisted_value, dict), "persisted archive manifest is not an object")
    persisted_manifest = cast(dict[str, object], persisted_value)
    _assert(
        persisted_manifest == manifest_payload,
        "persisted archive manifest differs from the generated manifest",
    )
    persisted_days_value = persisted_manifest["days"]
    _assert(isinstance(persisted_days_value, list), "manifest days are not a list")
    persisted_days_raw = cast(list[object], persisted_days_value)
    _assert(
        len(persisted_days_raw) == 56
        and all(isinstance(item, dict) for item in persisted_days_raw),
        "manifest does not contain exactly 56 day objects",
    )
    persisted_days = [cast(dict[str, object], item) for item in persisted_days_raw]
    lower_start = horizon - timedelta(days=56)
    for offset, item in enumerate(persisted_days):
        lower = lower_start + timedelta(days=offset)
        expected_file = f"trades-{lower.strftime('%Y-%m-%d')}.csv.zst"
        _assert(cast(str, item["file"]) == expected_file, "archive manifest file sequence drift")
        _assert(cast(str, item["lower"]) == lower.isoformat(), "archive lower bound drift")
        _assert(
            cast(str, item["upper"]) == (lower + timedelta(days=1)).isoformat(),
            "archive upper bound drift",
        )
        _assert(int(cast(int, item["rows"])) > 0, "archive manifest contains an empty day")
        _assert(int(cast(int, item["bytes"])) > 0, "archive manifest contains an empty file")
        _assert(
            re.fullmatch(r"[0-9a-f]{64}", cast(str, item["sha256"])) is not None,
            "archive manifest contains an invalid SHA-256",
        )
    persisted_rows = int(cast(int, persisted_manifest["rows"]))
    _assert(
        sum(int(cast(int, item["rows"])) for item in persisted_days) == persisted_rows,
        "persisted archive day rows do not sum to the manifest total",
    )
    return manifest_path, manifest_sha, persisted_days, persisted_rows


async def _restore_manifest_days(
    connection: Connection,
    paths: RuntimePaths,
    persisted_days: Sequence[dict[str, object]],
) -> int:
    """Restore every manifest-addressed file after checking path and digest.

    Args:
        connection: Private archive verification connection.
        paths: Exact retained archive directory.
        persisted_days: Validated day entries reread from the durable manifest.

    Returns:
        Sum of PostgreSQL COPY row counts.
    """
    _assert_archive_headroom(paths, "restore table creation")
    await connection.execute(_restore_table_sql())
    restored_from_files = 0
    for day_number, item in enumerate(persisted_days, start=1):
        _assert_archive_headroom(paths, f"restore day {day_number}")
        archive_path = paths.archive_dir / cast(str, item["file"])
        _assert(
            archive_path.parent.resolve() == paths.archive_dir.resolve(),
            "archive manifest escaped the allocated archive directory",
        )
        _assert(
            _sha256_path(archive_path) == cast(str, item["sha256"]),
            f"archive SHA-256 mismatch: {archive_path.name}",
        )
        restored_from_files += await _restore_archive_day(connection, archive_path)
    return restored_from_files


async def _archive_restore_gate(
    instance: InstanceRef,
    paths: RuntimePaths,
    horizon: datetime,
) -> dict[str, object]:
    """Export, manifest, restore, compare, and replay before legacy DROP.

    Args:
        instance: Verified private PostgreSQL instance.
        paths: Retained archive and manifest paths.
        horizon: Exclusive upper bound of the 56-day legacy fixture.

    Returns:
        Deletion-gate evidence that must be fully green before DROP.
    """
    free_before_archive = shutil.disk_usage(paths.archive_dir).free
    _assert(
        free_before_archive >= 24 * 1024**3,
        "archive gate requires at least 24 GiB free before export and restore",
    )
    connection = await _connect(
        instance,
        "archive-restore-gate",
        command_timeout=_ARCHIVE_SQL_TIMEOUT_SECONDS,
    )
    try:
        archive_initial_timezone = cast(str, await connection.fetchval("SHOW timezone"))
        _assert(
            archive_initial_timezone == _NON_UTC_TIMEZONE,
            "archive horizon session did not begin deliberately non-UTC",
        )
        await connection.execute("SET timezone = 'UTC'")
        await connection.execute("SET statement_timeout = '30min'")
        await connection.execute("SET lock_timeout = '5s'")
        _assert(
            await connection.fetchval("SHOW timezone") == "UTC",
            "archive horizon session failed to pin UTC",
        )
        _assert_archive_headroom(paths, "original paper replay")
        await connection.execute(_paper_replay_sql("paper_original", "trades_legacy"))
        manifests: list[dict[str, object]] = []
        lower_start = horizon - timedelta(days=56)
        for offset in range(56):
            lower = lower_start + timedelta(days=offset)
            _assert_archive_headroom(paths, f"export day {offset + 1}")
            manifests.append(
                await _export_archive_day(
                    connection,
                    paths,
                    lower,
                    lower + timedelta(days=1),
                )
            )
        manifest_path, manifest_sha, persisted_days, persisted_rows = _persist_archive_manifest(
            paths, horizon, manifests
        )
        legacy_rows = cast(
            int,
            await connection.fetchval("SELECT count(*)::bigint FROM trades_legacy"),
        )
        _assert(persisted_rows == legacy_rows, "archive manifest total differs from legacy rows")
        restored_from_files = await _restore_manifest_days(connection, paths, persisted_days)
        restore_rows = cast(
            int,
            await connection.fetchval("SELECT count(*)::bigint FROM rehearsal.trades_restore"),
        )
        _assert(restored_from_files == persisted_rows, "restored COPY totals differ from manifest")
        _assert(restore_rows == persisted_rows, "scratch restore row total differs from manifest")
        _assert_archive_headroom(paths, "restored paper replay")
        await connection.execute(_paper_replay_sql("paper_restored", "rehearsal.trades_restore"))
        _assert_archive_headroom(paths, "full-column and paper comparison")
        full_missing, full_extra, replay_missing, replay_extra = await _archive_equality(connection)
        _assert(full_missing == 0 and full_extra == 0, "archive full-column round trip differs")
        _assert(
            replay_missing == 0 and replay_extra == 0,
            "paper replay differs between original and restored trades",
        )
        original_candles = cast(
            int,
            await connection.fetchval("SELECT count(*)::bigint FROM rehearsal.paper_original"),
        )
        restored_candles = cast(
            int,
            await connection.fetchval("SELECT count(*)::bigint FROM rehearsal.paper_restored"),
        )
        free_after_verification = shutil.disk_usage(paths.archive_dir).free
        _assert(
            free_after_verification >= _ARCHIVE_EMERGENCY_FREE_BYTES,
            "archive verification breached the 8 GiB emergency free-space floor",
        )
        return {
            "archive_columns": list(_ARCHIVE_COLUMNS),
            "archive_days": len(persisted_days),
            "archive_files": [cast(str, item["file"]) for item in persisted_days],
            "archive_manifest": str(manifest_path),
            "archive_manifest_rows": persisted_rows,
            "archive_manifest_sha256": manifest_sha,
            "archive_session_initial_timezone": archive_initial_timezone,
            "archive_session_pinned_timezone": "UTC",
            "full_column_extra": full_extra,
            "full_column_missing": full_missing,
            "free_bytes_after_verification": free_after_verification,
            "free_bytes_before_archive": free_before_archive,
            "legacy_rows": legacy_rows,
            "paper_original_candles": original_candles,
            "paper_replay_extra": replay_extra,
            "paper_replay_missing": replay_missing,
            "paper_restored_candles": restored_candles,
            "restore_rows": restore_rows,
        }
    finally:
        await _close_connection_bounded(connection)


async def _apply_staged_work(
    connection: Connection,
    work_id: UUID,
    commit: bool,
) -> bool:
    """Apply one pending staged row and its worklog in the same transaction.

    Args:
        connection: Private retained-topology replay connection.
        work_id: Durable idempotency key.
        commit: Whether to commit or deliberately abort the complete unit.

    Returns:
        True when pending work was attempted, or False when already applied.
    """
    transaction = connection.transaction()
    await transaction.start()
    try:
        row = await connection.fetchrow(
            "SELECT state FROM rehearsal.staged_replay WHERE work_id = $1 FOR UPDATE",
            work_id,
        )
        _assert(row is not None, f"staged replay work disappeared: {work_id}")
        if cast(str, row["state"]) == "APPLIED":
            await transaction.rollback()
            return False
        inserted = await connection.fetchval(
            """
            INSERT INTO trades (
                public_id,
                instrument_public_id,
                trade_id,
                price,
                size,
                side,
                executed_at,
                session_id,
                sequence_id,
                "timestamp",
                known_to
            )
            SELECT
                public_id,
                instrument_public_id,
                trade_id,
                price,
                size,
                side,
                executed_at,
                session_id,
                sequence_id,
                "timestamp",
                known_to
            FROM rehearsal.staged_replay
            WHERE work_id = $1
            ON CONFLICT (instrument_public_id, trade_id, executed_at) DO NOTHING
            RETURNING public_id
            """,
            work_id,
        )
        _assert(inserted is not None, f"pending staged replay conflicted unexpectedly: {work_id}")
        await connection.execute(
            """
            INSERT INTO rehearsal.replay_worklog (
                work_id,
                obligation_generation,
                public_id
            )
            SELECT work_id, obligation_generation, public_id
            FROM rehearsal.staged_replay
            WHERE work_id = $1
            """,
            work_id,
        )
        await connection.execute(
            "UPDATE rehearsal.staged_replay SET state = 'APPLIED' WHERE work_id = $1",
            work_id,
        )
        if commit:
            await transaction.commit()
        else:
            await transaction.rollback()
        return True
    except BaseException:
        if connection.is_in_transaction():
            await transaction.rollback()
        raise


async def _prove_staged_replay(
    connection: Connection,
    generation_evidence: dict[str, object],
) -> dict[str, object]:
    """Prove abort-pending atomicity and idempotent post-day-30 replay.

    Args:
        connection: Private connection after legacy DROP and U3 epoch commit.
        generation_evidence: Post-generation staged work ID created after the latch.

    Returns:
        Rollback, commit, rerun, worklog, routing, and full-row evidence.
    """
    new_work = UUID(cast(str, generation_evidence["new_work_id"]))
    abort_attempted = await _apply_staged_work(connection, new_work, False)
    abort_row = await connection.fetchrow(
        """
        SELECT
            state,
            EXISTS (
                SELECT 1 FROM rehearsal.replay_worklog WHERE work_id = $1
            ) AS worklogged,
            EXISTS (
                SELECT 1
                FROM trades
                WHERE public_id = (
                    SELECT public_id FROM rehearsal.staged_replay WHERE work_id = $1
                )
            ) AS applied
        FROM rehearsal.staged_replay
        WHERE work_id = $1
        """,
        new_work,
    )
    _assert(abort_row is not None, "aborted staged work disappeared")
    abort_left_pending = (
        cast(str, abort_row["state"]) == "PENDING"
        and not cast(bool, abort_row["worklogged"])
        and not cast(bool, abort_row["applied"])
    )
    _assert(abort_attempted and abort_left_pending, "aborted replay left a half-applied unit")
    first_applications = [await _apply_staged_work(connection, new_work, True)]
    idempotent_reruns = [await _apply_staged_work(connection, new_work, True)]
    counts = await connection.fetchrow("""
        SELECT
            (SELECT count(*)::integer
             FROM rehearsal.staged_replay
             WHERE state = 'APPLIED') AS applied_work,
            (SELECT count(*)::integer FROM rehearsal.replay_worklog) AS worklog_rows,
            (SELECT count(*)::integer
             FROM trades
             WHERE trade_id LIKE 'staged-replay-%') AS trade_rows,
            (SELECT count(*)::integer
             FROM trades_default
             WHERE trade_id LIKE 'staged-replay-%') AS default_rows
        """)
    _assert(counts is not None, "staged replay count query returned no row")
    differences = await connection.fetchrow("""
        SELECT
            (
                SELECT count(*)::integer
                FROM (
                    SELECT
                        public_id, instrument_public_id, trade_id, price, size, side,
                        executed_at, session_id, sequence_id, "timestamp", known_to
                    FROM rehearsal.staged_replay
                    EXCEPT ALL
                    SELECT
                        public_id, instrument_public_id, trade_id, price, size, side,
                        executed_at, session_id, sequence_id, "timestamp", known_to
                    FROM trades
                    WHERE trade_id LIKE 'staged-replay-%'
                ) difference
            ) AS missing,
            (
                SELECT count(*)::integer
                FROM (
                    SELECT
                        public_id, instrument_public_id, trade_id, price, size, side,
                        executed_at, session_id, sequence_id, "timestamp", known_to
                    FROM trades
                    WHERE trade_id LIKE 'staged-replay-%'
                    EXCEPT ALL
                    SELECT
                        public_id, instrument_public_id, trade_id, price, size, side,
                        executed_at, session_id, sequence_id, "timestamp", known_to
                    FROM rehearsal.staged_replay
                ) difference
            ) AS extra
        """)
    _assert(differences is not None, "staged replay difference query returned no row")
    missing = cast(int, differences["missing"])
    extra = cast(int, differences["extra"])
    passed = (
        abort_left_pending
        and first_applications == [True]
        and idempotent_reruns == [False]
        and cast(int, counts["applied_work"]) == 1
        and cast(int, counts["worklog_rows"]) == 1
        and cast(int, counts["trade_rows"]) == 1
        and cast(int, counts["default_rows"]) == 1
        and missing == 0
        and extra == 0
    )
    _assert(passed, "staged replay durability/idempotence proof failed")
    return {
        "abort_left_pending_without_row_or_worklog": abort_left_pending,
        "applied_work": cast(int, counts["applied_work"]),
        "default_rows": cast(int, counts["default_rows"]),
        "first_applications": first_applications,
        "full_row_extra": extra,
        "full_row_missing": missing,
        "idempotent_reruns": idempotent_reruns,
        "row_and_worklog_same_transaction": True,
        "trade_rows": cast(int, counts["trade_rows"]),
        "worklog_rows": cast(int, counts["worklog_rows"]),
    }


async def _prepare_retention_detach(
    instance: InstanceRef,
    config: RunConfig,
    connection: Connection,
    horizon: datetime,
    stats: Sequence[WriterStats],
) -> RetentionPreparation:
    """Latch generation, exercise recovery, and clear the rejecting barrier.

    Args:
        instance: Verified private PostgreSQL instance.
        config: Fence and publisher bounds.
        connection: Autocommit retention controller connection.
        horizon: Exact legacy partition upper bound.
        stats: Live publisher counters.

    Returns:
        Fully evidenced state immediately before lease-authorized DETACH.
    """
    await _wait_writer_fence_ready(stats, 4)
    generation = await _prove_generation_serialization(instance, horizon)
    retention_generation = int(cast(int, generation["new_generation"]))
    default_attached = cast(
        bool,
        await connection.fetchval("""
            SELECT EXISTS (
                SELECT 1
                FROM pg_inherits
                WHERE inhrelid = 'trades_default'::regclass
                  AND inhparent = 'trades'::regclass
            )
            """),
    )
    _assert(default_attached, "DEFAULT partition missing before DETACH proof")
    default_rows = cast(
        int,
        await connection.fetchval("SELECT count(*)::bigint FROM trades_default"),
    )
    _assert(default_rows == 0, "DEFAULT partition was not drained before retention")
    concurrent_sqlstate, concurrent_message = await _concurrent_detach_negative(connection)
    barrier_context = RetentionBarrierContext(
        instance=instance,
        config=config,
        connection=connection,
        retention_generation=retention_generation,
        horizon=horizon,
        stats=stats,
    )
    barrier_preparation = await _prepare_retention_barrier(barrier_context)
    pending_reconciliation = await _prove_pending_reconciliation(
        instance,
        config,
        connection,
        horizon,
        stats,
    )
    lease, acknowledgements, barrier = await _finalize_retention_barrier(
        barrier_context,
        barrier_preparation,
    )
    pending_state = await _set_u3_pending(
        connection,
        horizon,
        lease,
        config.transaction_timeout_seconds + config.cleanup_margin_seconds,
    )
    return RetentionPreparation(
        generation=generation,
        lease=lease,
        acknowledgements=acknowledgements,
        barrier=barrier,
        pending_reconciliation=pending_reconciliation,
        pending_state=pending_state,
        default_attached=default_attached,
        default_rows=default_rows,
        concurrent_sqlstate=concurrent_sqlstate,
        concurrent_message=concurrent_message,
    )


async def _drop_legacy_with_bounds(connection: Connection) -> None:
    """Drop the detached legacy table under server and client time limits.

    Args:
        connection: Private retention controller connection.

    Returns:
        None after the bounded transaction commits.
    """
    async with connection.transaction():
        await connection.execute("SET LOCAL timezone = 'UTC'")
        await connection.execute("SET LOCAL lock_timeout = '2s'")
        await connection.execute("SET LOCAL statement_timeout = '10s'")
        await connection.execute("SET LOCAL transaction_timeout = '12s'")
        await connection.execute("DROP TABLE trades_legacy")


async def _archive_and_drop_legacy(
    instance: InstanceRef,
    connection: Connection,
    paths: RuntimePaths,
    horizon: datetime,
) -> tuple[dict[str, object], int]:
    """Complete the bounded archive gate before the bounded legacy DROP.

    Args:
        instance: Verified private PostgreSQL instance.
        connection: Retention controller connection.
        paths: Archive and manifest paths.
        horizon: Exclusive legacy upper bound.

    Returns:
        Archive evidence and intentionally retired legacy row count.
    """
    archive = await asyncio.wait_for(
        _archive_restore_gate(instance, paths, horizon),
        timeout=_ARCHIVE_GATE_TIMEOUT_SECONDS,
    )
    legacy_rows = int(cast(int, archive["legacy_rows"]))
    await asyncio.wait_for(
        _drop_legacy_with_bounds(connection),
        timeout=_RETENTION_DROP_TIMEOUT_SECONDS,
    )
    return archive, legacy_rows


async def _exercise_detach(
    instance: InstanceRef,
    config: RunConfig,
    paths: RuntimePaths,
    horizon: datetime,
) -> tuple[
    dict[str, object],
    IdentityEvidence,
    IdentityEvidence,
    IdentityEvidence,
    list[WriterStats],
]:
    """Prove DEFAULT forbids concurrent detach and plain detach retries safely.

    Args:
        instance: Verified private PostgreSQL instance.
        config: Used to create a final POST state if needed.
        paths: Controller evidence paths.
        horizon: Cutover boundary.

    Returns:
        DETACH, retention, terminal fixture, global writer, and writer evidence.
    """
    await _ensure_post(instance, config, paths, horizon)
    connection = await _connect(
        instance,
        "detach-proof",
        command_timeout=_PUBLISHER_STOP_TIMEOUT_SECONDS,
    )
    blocker = await _connect(
        instance,
        "detach-blocker",
        command_timeout=_PUBLISHER_STOP_TIMEOUT_SECONDS,
    )
    contexts, stop_event = _writer_contexts(
        instance,
        config,
        paths,
        horizon,
        "retention",
    )
    tasks = [asyncio.create_task(_publisher_loop(context)) for context in contexts]
    stats = [context.stats for context in contexts]
    try:
        preparation = await _prepare_retention_detach(
            instance,
            config,
            connection,
            horizon,
            stats,
        )
        spec = ControllerSpec(
            instance=instance,
            scenario="retention",
            generation=preparation.lease.generation,
            token=preparation.lease.token,
            horizon=horizon,
            writers=config.writers,
            transaction_timeout_seconds=config.transaction_timeout_seconds,
            cleanup_margin_seconds=config.cleanup_margin_seconds,
            events_dir=paths.events_dir,
        )
        retry = await _plain_detach_with_retry(connection, blocker, spec)
        retained_state = await _catalog_state(connection, horizon)
        _assert(
            retained_state.mode == "RETAINED",
            f"DETACH+epoch did not commit one coherent RETAINED state: {retained_state}",
        )
        released = await connection.fetchrow(
            """
            UPDATE rehearsal.fence_generations
            SET
                resolution = 'POST_COMMITTED',
                resolver = 'controller',
                resolved_fingerprint = $2,
                resolved_at = clock_timestamp()
            WHERE generation = $1
              AND token = $3
              AND resolution IS NULL
            RETURNING generation, token
            """,
            preparation.lease.generation,
            retained_state.fingerprint(),
            preparation.lease.token,
        )
        _assert(released is not None, "retention controller could not release its fence")
        released_row = cast(Record, released)
        resumes = await _wait_resumes(
            connection,
            preparation.lease.generation,
            config.writers,
            stats,
        )
        target = max(item.committed for item in stats) + 1
        await _wait_writer_commits(stats, target)
        await _stop_publishers(tasks, stop_event)
        retention_identities = [identity for item in stats for identity in item.expected]
        retention_identity = await _identity_multiset(
            instance,
            "retention",
            retention_identities,
        )
        _assert(retention_identity.passed(), "retention writer identity multiset mismatch")
        archive, legacy_rows = await _archive_and_drop_legacy(
            instance,
            connection,
            paths,
            horizon,
        )
        legacy_absent = await connection.fetchval("SELECT to_regclass('trades_legacy') IS NULL")
        default_still_attached = cast(
            bool,
            await connection.fetchval("""
                SELECT EXISTS (
                    SELECT 1
                    FROM pg_inherits
                    WHERE inhrelid = 'trades_default'::regclass
                      AND inhparent = 'trades'::regclass
                )
                """),
        )
        _assert(cast(bool, legacy_absent), "legacy DROP did not remove detached table")
        _assert(default_still_attached, "plain DETACH disturbed DEFAULT partition")
        purged_state = await _catalog_state(connection, horizon)
        _assert(
            purged_state.mode == "PURGED",
            f"legacy DROP did not produce a coherent PURGED catalog: {purged_state}",
        )
        staged_replay = await _prove_staged_replay(connection, preparation.generation)
        post_replay_state = await _catalog_state(connection, horizon)
        _assert(
            post_replay_state.fingerprint() == purged_state.fingerprint(),
            "staged replay changed the terminal catalog topology",
        )
        all_writer_identity = await _identity_multiset(
            instance,
            "all_writers",
            _ledger_identities(paths),
        )
        _assert(all_writer_identity.passed(), "terminal all-writer identity multiset mismatch")
        fixture_identity = await _fixture_identity_multiset(instance)
        _assert(fixture_identity.passed(), "terminal fixture/archive identity multiset mismatch")
        return (
            {
                "archive_gate": archive,
                "archive_gate_passed": True,
                "bounded_attempt_limit": 3,
                "concurrent_detach_message": preparation.concurrent_message,
                "concurrent_detach_sqlstate": preparation.concurrent_sqlstate,
                "default_attached_after_plain_detach": default_still_attached,
                "default_attached_before": preparation.default_attached,
                "default_rows_before_detach": preparation.default_rows,
                "detached_legacy_rows_intentionally_retired": legacy_rows,
                "failed_attempt_epoch_rolled_back": (
                    retry["epoch_after_failed_attempt"] == "U3_PENDING"
                    and cast(bool, retry["failed_attempt_still_attached"])
                ),
                "first_plain_attempt_sqlstate": retry["first_sqlstate"],
                "generation_serialization": preparation.generation,
                "legacy_drop_exercised": cast(bool, legacy_absent),
                "pending_reconciliation": preparation.pending_reconciliation,
                "pending_state_before_detach": preparation.pending_state.mode,
                "plain_detach_attempts": retry["attempts"],
                "plain_detach_succeeded": True,
                "retention_barrier": preparation.barrier,
                "retention_acknowledgements": preparation.acknowledgements,
                "retention_authorization": retry["successful_authorization"],
                "retention_first_authorization": retry["first_authorization"],
                "retention_epoch_after_commit": retained_state.topology_epoch,
                "retention_fence_generation": preparation.lease.generation,
                "retention_release_generation": cast(int, released_row["generation"]),
                "retention_release_token": str(cast(UUID, released_row["token"])),
                "retention_resumes": len(resumes),
                "staged_replay": staged_replay,
                "terminal_catalog_after_drop": purged_state.mode,
                "terminal_fingerprint_after_drop": purged_state.fingerprint(),
            },
            retention_identity,
            fixture_identity,
            all_writer_identity,
            stats,
        )
    finally:
        try:
            await _settle_publishers(tasks, stop_event)
        finally:
            try:
                if blocker.is_in_transaction():
                    await blocker.execute("ROLLBACK")
            finally:
                await _close_connection_bounded(blocker)
                await _close_connection_bounded(connection)


def _scenario_fault_passed(result: ScenarioResult) -> bool:
    """Bind each generic kill row to its scenario-specific fault observation.

    Args:
        result: Full scenario evidence returned by the recovery exercise.

    Returns:
        True only when the reported fault and terminal resolution agree exactly.
    """
    fault = result.fault_evidence
    event = fault.get("event")
    stale_authority = fault.get("stale_authority")
    expected_resolution = "PRE_ABORTED" if result.terminal_state.mode == "PRE" else "POST_COMMITTED"
    common = (
        isinstance(event, dict)
        and isinstance(stale_authority, dict)
        and int(cast(int, fault.get("observed_backend_pid", 0))) > 1
        and bool(cast(str, fault.get("observed_query", "")))
        and cast(str, fault.get("resolution", "")) == result.resolution == expected_resolution
        and cast(str, fault.get("resolver", "")) == result.resolver
        and cast(str, fault.get("terminal_catalog", "")) == result.terminal_state.mode
        and cast(str, fault.get("terminal_fingerprint", "")) == result.terminal_state.fingerprint()
        and int(cast(int, fault.get("winner_count", 0))) == result.winner_count == 1
        and int(cast(int, fault.get("resume_count", 0))) == result.resume_count
        and cast(bool, fault.get("stale_authority_denied", False))
        and cast(bool, cast(dict[str, object], stale_authority).get("denied", False))
        and cast(
            bool,
            cast(dict[str, object], stale_authority).get(
                "actual_authorizer_attempted",
                False,
            ),
        )
        and cast(
            bool,
            cast(dict[str, object], stale_authority).get("catalog_unchanged", False),
        )
        and cast(str, cast(dict[str, object], stale_authority).get("rejection", ""))
        == "controller generation is already terminal"
    )
    if not common:
        return False
    query = cast(str, fault["observed_query"])
    observed_state = cast(str, fault.get("observed_state_before_kill", ""))
    event_dict = cast(dict[str, object], event)
    if result.name == "after_begin":
        return (
            observed_state == "PRE"
            and cast(str, event_dict.get("scenario", "")) == "after_begin"
            and "to_regclass" in query
        )
    if result.name == "during_commit":
        return (
            observed_state == "PRE"
            and query.strip() == "COMMIT"
            and int(cast(int, event_dict.get("backend_pid", 0))) > 1
        )
    if result.name == "after_commit":
        messages = event_dict.get("debug_messages")
        return (
            observed_state == "POST"
            and isinstance(messages, list)
            and any(_DEBUG_IMPLICATION in str(message) for message in cast(list[object], messages))
            and float(cast(float | int, event_dict.get("attach_seconds", 2.0))) < 1.0
        )
    return (
        result.name == "orphan_timeout"
        and observed_state == "PRE"
        and "pg_sleep" in query
        and int(cast(int, event_dict.get("backend_pid", 0)))
        == int(cast(int, fault.get("observed_backend_pid", 0)))
        and int(cast(int, event_dict.get("holder_pid", 0))) > 1
        and int(cast(int, event_dict.get("holder_start_ticks", 0))) > 0
        and int(cast(int, event_dict.get("socket_fd_held", 0))) > 2
        and cast(bool, fault.get("holder_alive_after_controller_kill", False))
        and cast(bool, fault.get("post_kill_backend_active", False))
        and cast(bool, fault.get("holder_terminated_after_timeout", False))
        and int(cast(int, fault.get("early_resumes_before_timeout", -1))) == 0
        and cast(bool, fault.get("timeout_log_observed", False))
        and int(cast(int, fault.get("timeout_log_backend_pid", 0)))
        == int(cast(int, fault.get("observed_backend_pid", 0)))
        and cast(str, fault.get("timeout_log_application", ""))
        == cast(str, event_dict.get("application_name", ""))
        and cast(str, fault.get("backend_gone_at", ""))
        < cast(str, fault.get("lease_expires_at", ""))
    )


def _scenario_kill_result(result: ScenarioResult) -> KillResult:
    """Convert one cutover scenario into the requested kill-matrix row.

    Args:
        result: Full scenario evidence.

    Returns:
        PASS/FAIL kill result with terminal recovery facts.
    """
    labels = {
        "after_begin": "after DDL BEGIN before COMMIT",
        "during_commit": "during COMMIT (ambiguous)",
        "after_commit": "after COMMIT before resume",
        "orphan_timeout": "orphan transaction_timeout before lease expiry",
    }
    passed = (
        result.controller_returncode == -signal.SIGKILL
        and result.terminal_state.mode in {"PRE", "POST"}
        and result.resume_count >= 2
        and result.winner_count == 1
        and result.stable_after_resume
        and result.stale_authority_denied
        and result.identity.passed()
        and result.surfaced_unique_violations == 0
        and result.hidden_conflicts == 0
        and _scenario_fault_passed(result)
    )
    return KillResult(
        name=labels[result.name],
        passed=passed,
        evidence={
            "controller_returncode": result.controller_returncode,
            "hidden_targetless_conflicts": result.hidden_conflicts,
            "identity_expected": result.identity.expected,
            "identity_extra_or_changed": result.identity.extra_or_changed,
            "identity_missing": result.identity.missing,
            "reconciler": result.resolver,
            "resolution": result.resolution,
            "resume_count": result.resume_count,
            "stable_after_resume": result.stable_after_resume,
            "stale_authority_denied": result.stale_authority_denied,
            "surfaced_unique_violations": result.surfaced_unique_violations,
            "terminal_catalog": result.terminal_state.mode,
            "terminal_fingerprint": result.terminal_state.fingerprint(),
            "winner_count": result.winner_count,
            "fault_observation": result.fault_evidence,
        },
    )


def _duration_text(seconds: float) -> str:
    """Render a compact duration for reports.

    Args:
        seconds: Duration in seconds.

    Returns:
        Human-readable hours, minutes, and seconds.
    """
    total = max(0, round(seconds))
    hours, remainder = divmod(total, 3600)
    minutes, final_seconds = divmod(remainder, 60)
    return f"{hours}h {minutes}m {final_seconds}s"


def _lease_authorization_passed(
    evidence: dict[str, object],
    expected_generation: int,
    expected_token: object,
) -> bool:
    """Validate one in-transaction database-clock DDL authorization.

    Args:
        evidence: Facts captured after locking the exact fence generation row.
        expected_generation: Final retention fence generation.
        expected_token: Final retention fence token.

    Returns:
        True only when timeout and cleanup fit within the remaining lease.
    """
    remaining = float(cast(float | int, evidence.get("remaining_lease_seconds", 0)))
    timeout = float(cast(float | int, evidence.get("transaction_timeout_seconds", 0)))
    margin = float(cast(float | int, evidence.get("cleanup_margin_seconds", 0)))
    return (
        int(cast(int, evidence.get("backend_pid", 0))) > 1
        and int(cast(int, evidence.get("fence_generation", 0))) == expected_generation
        and evidence.get("fence_token") == expected_token
        and _valid_uuid_text(expected_token)
        and cast(str, evidence.get("initial_timezone", "")) == _NON_UTC_TIMEZONE
        and cast(str, evidence.get("pinned_timezone", "")) == "UTC"
        and timeout > 0
        and margin > 0
        and remaining > timeout + margin
    )


def _ordered_evidence_times(values: Sequence[object]) -> bool:
    """Require a strictly ordered series of timezone-aware evidence instants.

    Args:
        values: Serialized database-clock instants in expected event order.

    Returns:
        True only when every value parses with a timezone and increases strictly.
    """
    if not values or not all(isinstance(value, str) for value in values):
        return False
    try:
        instants = [datetime.fromisoformat(cast(str, value)) for value in values]
    except ValueError:
        return False
    return all(instant.tzinfo is not None for instant in instants) and all(
        earlier < later for earlier, later in itertools.pairwise(instants)
    )


def _valid_uuid_text(value: object) -> bool:
    """Validate one serialized UUID without accepting non-string coercions.

    Args:
        value: Candidate evidence value.

    Returns:
        True only for a canonical UUID string.
    """
    if not isinstance(value, str):
        return False
    try:
        parsed = UUID(value)
    except ValueError:
        return False
    return str(parsed) == value


def _retention_acknowledgements_passed(
    acknowledgements: Sequence[dict[str, object]],
    expected_publishers: int,
    retention_generation: int,
    fence_generation: int,
    fence_token: object,
) -> bool:
    """Validate one complete generation-bound publisher acknowledgement set.

    Args:
        acknowledgements: Persisted publisher session-closure evidence.
        expected_publishers: Exact configured publisher population.
        retention_generation: Closed retention obligation generation.
        fence_generation: Exact phase-specific publisher fence generation.
        fence_token: Exact phase-specific publisher fence token.

    Returns:
        True only when every expected publisher closed cleanly on the generation.
    """
    expected_identities = {f"publisher-{number}" for number in range(1, expected_publishers + 1)}
    observed_identities = {
        cast(str, acknowledgement.get("publisher_id", "")) for acknowledgement in acknowledgements
    }
    return (
        expected_publishers > 0
        and fence_generation > 0
        and _valid_uuid_text(fence_token)
        and len(acknowledgements) == expected_publishers
        and observed_identities == expected_identities
        and all(
            acknowledgement.get("session_closed") is True
            and int(cast(int, acknowledgement.get("fence_generation", 0))) == fence_generation
            and acknowledgement.get("fence_token") == fence_token
            and int(cast(int, acknowledgement.get("inflight", -1))) == 0
            and int(cast(int, acknowledgement.get("data_sessions_observed", -1))) == 0
            and int(cast(int, acknowledgement.get("retention_generation", -1)))
            == retention_generation
            for acknowledgement in acknowledgements
        )
    )


def _retention_snapshot_window_passed(
    snapshot: dict[str, object],
    fence_generation: int,
    retention_generation: int,
    required_seconds: float,
    expected_publishers: int,
) -> bool:
    """Validate identity, generation, and live-window facts from one fence snapshot.

    Args:
        snapshot: Coherent database snapshot captured after publisher acknowledgements.
        fence_generation: Exact fence generation expected in the snapshot.
        retention_generation: Exact closed retention generation.
        required_seconds: Transaction timeout plus cleanup margin.
        expected_publishers: Exact configured publisher population.

    Returns:
        True only for the active exact fence with a strictly sufficient lease.
    """
    if not _valid_uuid_text(snapshot.get("fence_token")):
        return False
    remaining = float(cast(float | int, snapshot.get("remaining_lease_seconds", 0)))
    recorded_required = float(cast(float | int, snapshot.get("required_lease_seconds", 0)))
    return (
        fence_generation > 0
        and retention_generation > 0
        and required_seconds > 0
        and math.isclose(recorded_required, required_seconds, rel_tol=1e-9)
        and remaining > recorded_required
        and snapshot.get("fence_resolution", "missing") is None
        and int(cast(int, snapshot.get("fence_generation", 0))) == fence_generation
        and int(cast(int, snapshot.get("retention_generation", 0))) == retention_generation
        and int(cast(int, snapshot.get("expected_fence_acknowledgements", -1)))
        == expected_publishers
        and int(cast(int, snapshot.get("fence_acknowledgements", -1))) == expected_publishers
    )


def _writer_commit_resume_passed(
    retry: dict[str, object],
    acknowledgements: Sequence[dict[str, object]],
) -> bool:
    """Validate committed ingestion progress after a rejected fence is released.

    Args:
        retry: Controller release, resume, and publisher commit evidence.
        acknowledgements: Exact publisher set expected to make progress.

    Returns:
        True only when every resumed publisher advances beyond its baseline.
    """
    expected_publishers = {
        cast(str, acknowledgement.get("publisher_id", "")) for acknowledgement in acknowledgements
    }
    before = cast(dict[str, int], retry.get("writer_commits_before", {}))
    after = cast(dict[str, int], retry.get("writer_commits_after", {}))
    if not expected_publishers or set(before) != expected_publishers:
        return False
    if set(after) != expected_publishers:
        return False
    target = int(cast(int, retry.get("writer_commit_target", 0)))
    return target == max(before.values()) + 1 and all(
        after[publisher_id] >= target and after[publisher_id] > before[publisher_id]
        for publisher_id in expected_publishers
    )


def _retention_phase_order_passed(
    barrier: dict[str, object],
    pending_reconciliation: dict[str, object],
    retention_generation: int,
    terminal_fence_generation: int,
) -> bool:
    """Validate negative release, lease-free clearance, and fresh final-fence order.

    Args:
        barrier: Combined negative, remediation, and final barrier evidence.
        pending_reconciliation: Separate expired U3_PENDING fence evidence.
        retention_generation: Closed obligation generation cleared by remediation.
        terminal_fence_generation: Fence authorizing the successful DETACH.

    Returns:
        True only when all three fence generations and remediation are ordered.
    """
    negative_generation = int(cast(int, barrier.get("negative_fence_generation", 0)))
    pending_generation = int(cast(int, pending_reconciliation.get("fence_generation", 0)))
    final_generation = int(cast(int, barrier.get("final_fence_generation", 0)))
    fence_tokens = {
        cast(str, barrier.get("negative_fence_token", "")),
        cast(str, pending_reconciliation.get("fence_token", "")),
        cast(str, barrier.get("final_fence_token", "")),
    }
    retry = cast(dict[str, object], barrier.get("negative_retry", {}))
    negative_acknowledgements = cast(
        list[dict[str, object]],
        barrier.get("negative_fence_acknowledgements", []),
    )
    pending_acknowledgements = cast(
        list[dict[str, object]],
        pending_reconciliation.get("acknowledgements", []),
    )
    return (
        0 < negative_generation < pending_generation < final_generation
        and final_generation == terminal_fence_generation
        and len(fence_tokens) == 3
        and all(_valid_uuid_text(token) for token in fence_tokens)
        and pending_reconciliation.get("fence_token")
        == pending_reconciliation.get("observed_fence_token")
        and _writer_commit_resume_passed(
            pending_reconciliation,
            pending_acknowledgements,
        )
        and int(cast(int, retry.get("fence_generation", 0))) == negative_generation
        and retry.get("fence_token") == barrier.get("negative_fence_token")
        and cast(str, retry.get("resolution", "")) == "POST_COMMITTED"
        and int(cast(int, retry.get("resume_count", 0))) == len(negative_acknowledgements)
        and _writer_commit_resume_passed(retry, negative_acknowledgements)
        and int(cast(int, barrier.get("active_fences_before_clearance", -1))) == 0
        and int(cast(int, barrier.get("active_fences_after_clearance", -1))) == 0
        and int(cast(int, barrier.get("historical_fence_overlaps", -1))) == 0
        and int(cast(int, barrier.get("cleared_retention_generation", -1))) == retention_generation
        and _ordered_evidence_times(
            (
                retry.get("resolved_at"),
                retry.get("last_resume_at"),
                barrier.get("remediation_started_at"),
                barrier.get("remediation_completed_at"),
                pending_reconciliation.get("opened_at"),
                pending_reconciliation.get("resolved_at"),
                pending_reconciliation.get("last_resume_at"),
                barrier.get("final_lease_opened_at"),
            )
        )
    )


def _criteria(inputs: AcceptanceInputs) -> list[CriterionResult]:
    """Build all six hard acceptance results from measured evidence.

    Args:
        inputs: All measured acceptance inputs.

    Returns:
        Exactly six falsifiable criterion results.
    """
    terminal_identities = [
        item for item in inputs.identities if item.scenario in {"fixture", "all_writers"}
    ]
    identity_expected = sum(item.expected for item in terminal_identities)
    identity_missing = sum(item.missing for item in terminal_identities)
    identity_extra = sum(item.extra_or_changed for item in terminal_identities)
    identity_scopes = {item.scenario for item in terminal_identities}
    surfaced = sum(item.surfaced_unique_violations for item in inputs.writer_stats)
    surfaced += sum(item.surfaced_unique_violations for item in inputs.scenario_results)
    hidden = sum(item.hidden_conflicts for item in inputs.writer_stats)
    hidden += sum(item.hidden_conflicts for item in inputs.scenario_results)
    writer_failures = [failure for item in inputs.writer_stats for failure in item.failures]
    conflict_batches = sum(item.conflict_batches_exercised for item in inputs.writer_stats)
    clean_duplicates = sum(item.clean_duplicates_classified for item in inputs.writer_stats)
    quarantined_twins = sum(item.mismatched_twins_quarantined for item in inputs.writer_stats)
    classification_failures = sum(
        item.conflict_classification_failures for item in inputs.writer_stats
    )
    conflict_modes = {mode for item in inputs.writer_stats for mode in item.conflict_modes}
    parent_unique_negative = cast(
        dict[str, object],
        inputs.attach["parent_unique_constraint_negative_control"],
    )
    implication_controls = cast(
        dict[str, object],
        inputs.attach["implication_negative_controls"],
    )
    missing_non_null_control = cast(
        dict[str, object],
        implication_controls["missing_is_not_null"],
    )
    timezone_mismatch_control = cast(
        dict[str, object],
        implication_controls["timezone_instant_mismatch"],
    )
    bare_date_control = cast(
        dict[str, object],
        implication_controls["bare_date_parse"],
    )
    bare_date_attach_control = cast(
        dict[str, object],
        implication_controls["bare_date_attach"],
    )
    archive_gate = cast(
        dict[str, object],
        inputs.detach.get("archive_gate", {}),
    )
    generation_serialization = cast(
        dict[str, object],
        inputs.detach.get("generation_serialization", {}),
    )
    staged_replay = cast(
        dict[str, object],
        inputs.detach.get("staged_replay", {}),
    )
    retention_acks = cast(
        list[dict[str, object]],
        inputs.detach.get("retention_acknowledgements", []),
    )
    retention_barrier = cast(
        dict[str, object],
        inputs.detach.get("retention_barrier", {}),
    )
    barrier_final = cast(
        dict[str, object],
        retention_barrier.get("final_snapshot", {}),
    )
    barrier_initial = cast(
        dict[str, object],
        retention_barrier.get("initial_snapshot", {}),
    )
    computed_initial_blockers, computed_final_blockers = (
        (_retention_barrier_blockers(barrier_initial) if barrier_initial else ["unexercised"]),
        _retention_barrier_blockers(barrier_final) if barrier_final else ["unexercised"],
    )
    reported_initial_blockers, reported_final_blockers = (
        cast(list[str], retention_barrier.get("initial_blockers", [])),
        cast(
            list[str],
            retention_barrier.get("final_blockers", ["unexercised"]),
        ),
    )
    barrier_obligation_drain = cast(
        dict[str, object],
        retention_barrier.get("obligation_drain", {}),
    )
    barrier_monitor_scans = cast(
        dict[str, object],
        retention_barrier.get("monitor_scans", {}),
    )
    barrier_negative_acks = cast(
        list[dict[str, object]],
        retention_barrier.get("negative_fence_acknowledgements", []),
    )
    pending_reconciliation = cast(
        dict[str, object],
        inputs.detach.get("pending_reconciliation", {}),
    )
    pending_acknowledgements = cast(
        list[dict[str, object]],
        pending_reconciliation.get("acknowledgements", []),
    )
    retention_authorization = cast(
        dict[str, object],
        inputs.detach.get("retention_authorization", {}),
    )
    retention_first_authorization = cast(
        dict[str, object],
        inputs.detach.get("retention_first_authorization", {}),
    )
    (
        retention_generation,
        expected_retention_publishers,
        terminal_retention_fence_generation,
        terminal_retention_fence_token,
    ) = (
        int(cast(int, generation_serialization.get("new_generation", -1))),
        int(cast(int, barrier_final.get("expected_fence_acknowledgements", -1))),
        int(cast(int, inputs.detach.get("retention_fence_generation", -1))),
        retention_barrier.get("final_fence_token"),
    )
    required_retention_seconds = float(
        cast(float | int, retention_authorization.get("transaction_timeout_seconds", 0))
    ) + float(cast(float | int, retention_authorization.get("cleanup_margin_seconds", 0)))
    attach_seconds = float(cast(float | int, inputs.attach["attach_seconds"]))
    negative_scans_observed = all(
        not cast(bool, control["debug_implication_observed"])
        and cast(bool, control["scan_verification_observed"])
        and float(cast(float | int, control["attach_seconds"])) >= 0.05
        and float(cast(float | int, control["attach_seconds"])) > attach_seconds * 5
        for control in (missing_non_null_control, timezone_mismatch_control)
    )
    expected_row_eta = (
        377_000_000 / inputs.metrics.rows_per_second
        if inputs.metrics.rows_per_second > 0
        else math.inf
    )
    expected_heap_eta = (
        (59 * 1024**3) / inputs.metrics.bytes_per_second
        if inputs.metrics.bytes_per_second > 0
        else math.inf
    )
    measured_rows_per_second = (
        inputs.metrics.fixture_rows / inputs.metrics.seconds
        if inputs.metrics.seconds > 0
        else math.inf
    )
    measured_bytes_per_second = (
        inputs.metrics.heap_bytes / inputs.metrics.seconds
        if inputs.metrics.seconds > 0
        else math.inf
    )
    metrics_finite = all(
        math.isfinite(value) and value > 0
        for value in (
            inputs.metrics.seconds,
            inputs.metrics.raw_seconds,
            inputs.metrics.controlled_pause_seconds,
            inputs.metrics.rows_per_second,
            inputs.metrics.bytes_per_second,
            inputs.metrics.production_rows_eta_seconds,
            inputs.metrics.production_heap_eta_seconds,
            inputs.metrics.conservative_eta_seconds,
            inputs.metrics.set_not_null_seconds,
            inputs.metrics.set_not_null_scan_control_seconds,
        )
    )
    eta_consistent = (
        math.isclose(
            inputs.metrics.rows_per_second,
            measured_rows_per_second,
            rel_tol=1e-9,
        )
        and math.isclose(
            inputs.metrics.bytes_per_second,
            measured_bytes_per_second,
            rel_tol=1e-9,
        )
        and math.isclose(
            inputs.metrics.production_rows_eta_seconds,
            expected_row_eta,
            rel_tol=1e-9,
        )
        and math.isclose(
            inputs.metrics.production_heap_eta_seconds,
            expected_heap_eta,
            rel_tol=1e-9,
        )
        and math.isclose(
            inputs.metrics.conservative_eta_seconds,
            max(expected_row_eta, expected_heap_eta),
            rel_tol=1e-9,
        )
    )
    attach_passed = (
        attach_seconds < 1.0
        and inputs.attach["child_index_oids_before"] == inputs.attach["child_index_oids_after"]
        and cast(bool, inputs.attach["debug_implication_observed"])
        and {
            tuple(pair) for pair in cast(list[list[str]], inputs.attach["index_inheritance_pairs"])
        }
        == _EXPECTED_INDEX_PAIRS
        and int(cast(int, parent_unique_negative["new_child_index_count"])) == 1
        and parent_unique_negative["child_index_oids_after_rollback"]
        == parent_unique_negative["child_index_oids_before"]
        and len(
            cast(
                list[int],
                parent_unique_negative["child_index_oids_during_attach"],
            )
        )
        == 8
        and cast(bool, parent_unique_negative["proved_at_fixture_scale"])
        and int(cast(int, parent_unique_negative["fixture_rows"])) >= _QUALIFYING_ROWS
        and negative_scans_observed
    )
    return [
        CriterionResult(
            number=1,
            name="instant scan-free ATTACH with index adoption",
            passed=attach_passed,
            evidence=inputs.attach,
        ),
        CriterionResult(
            number=2,
            name="zero rows lost by stable identity multiset",
            passed=(
                identity_missing == 0
                and identity_extra == 0
                and identity_expected > inputs.configured_rows
                and identity_scopes == {"fixture", "all_writers"}
            ),
            evidence={
                "comparison_operator": "EXCEPT ALL in both directions",
                "expected_identity_rows": identity_expected,
                "extra_or_changed_identity_rows": identity_extra,
                "missing_identity_rows": identity_missing,
                "terminal_scopes": sorted(identity_scopes),
            },
        ),
        CriterionResult(
            number=3,
            name="zero unique violations under concurrent writes",
            passed=(
                surfaced == 0
                and hidden == 0
                and not writer_failures
                and conflict_batches >= 2
                and clean_duplicates == conflict_batches
                and quarantined_twins == conflict_batches
                and classification_failures == 0
                and conflict_modes
                == {
                    "ORDINARY_TARGETLESS",
                    "PARTITIONED_TARGETLESS",
                    "PARTITIONED_U3",
                }
            ),
            evidence={
                "clean_duplicates_classified": clean_duplicates,
                "conflict_batches_exercised": conflict_batches,
                "conflict_classification_failures": classification_failures,
                "conflict_modes": sorted(conflict_modes),
                "hidden_targetless_conflicts": hidden,
                "mismatched_twins_quarantined": quarantined_twins,
                "publisher_failures": writer_failures,
                "surfaced_unique_violations": surfaced,
            },
        ),
        CriterionResult(
            number=4,
            name="VALIDATE throughput measured and extrapolated",
            passed=(
                inputs.configured_rows == _QUALIFYING_ROWS
                and inputs.metrics.fixture_rows >= _QUALIFYING_ROWS
                and metrics_finite
                and eta_consistent
                and inputs.metrics.writer_commits_during > 0
                and inputs.metrics.writer_overlap_backend_pid > 1
                and inputs.metrics.controlled_pause_seconds > 0
                and inputs.metrics.raw_seconds
                > inputs.metrics.seconds + inputs.metrics.controlled_pause_seconds * 0.9
                and inputs.metrics.active_recheck
                and inputs.metrics.set_not_null_scan_skipped
            ),
            evidence={
                "cache_condition": inputs.metrics.cache_condition,
                "configured_fixture_rows": inputs.configured_rows,
                "fixture_heap_bytes": inputs.metrics.heap_bytes,
                "fixture_rows": inputs.metrics.fixture_rows,
                "production_377m_row_eta": _duration_text(
                    inputs.metrics.production_rows_eta_seconds
                ),
                "production_59gib_heap_eta": _duration_text(
                    inputs.metrics.production_heap_eta_seconds
                ),
                "production_conservative_eta": _duration_text(
                    inputs.metrics.conservative_eta_seconds
                ),
                "throughput_bytes_per_second": inputs.metrics.bytes_per_second,
                "throughput_rows_per_second": inputs.metrics.rows_per_second,
                "validate_seconds": inputs.metrics.seconds,
                "validate_raw_seconds": inputs.metrics.raw_seconds,
                "controlled_pause_seconds": inputs.metrics.controlled_pause_seconds,
                "writer_overlap_backend_pid": inputs.metrics.writer_overlap_backend_pid,
                "active_validate_pid_rechecked": inputs.metrics.active_recheck,
                "writer_commits_during_validate": inputs.metrics.writer_commits_during,
                "set_not_null_scan_control_seconds": (
                    inputs.metrics.set_not_null_scan_control_seconds
                ),
                "set_not_null_scan_skipped": inputs.metrics.set_not_null_scan_skipped,
                "set_not_null_seconds": inputs.metrics.set_not_null_seconds,
            },
        ),
        CriterionResult(
            number=5,
            name="plain DETACH with DEFAULT under bounded lock retries",
            passed=(
                cast(bool, inputs.detach["default_attached_before"])
                and cast(bool, inputs.detach["plain_detach_succeeded"])
                and cast(str, inputs.detach["first_plain_attempt_sqlstate"]) == "55P03"
                and cast(str, inputs.detach.get("concurrent_detach_sqlstate", "")) == "55000"
                and int(cast(int, inputs.detach["plain_detach_attempts"])) >= 2
                and int(cast(int, inputs.detach["plain_detach_attempts"])) <= 3
                and int(cast(int, inputs.detach["default_rows_before_detach"])) == 0
                and cast(bool, inputs.detach.get("default_attached_after_plain_detach", False))
                and cast(bool, inputs.detach.get("failed_attempt_epoch_rolled_back", False))
                and cast(str, inputs.detach.get("retention_epoch_after_commit", "")) == "U3"
                and cast(str, inputs.detach.get("terminal_catalog_after_drop", "")) == "PURGED"
                and int(cast(int, inputs.detach.get("retention_resumes", 0)))
                == expected_retention_publishers
                and _retention_acknowledgements_passed(
                    retention_acks,
                    expected_retention_publishers,
                    retention_generation,
                    terminal_retention_fence_generation,
                    terminal_retention_fence_token,
                )
                and _retention_acknowledgements_passed(
                    barrier_negative_acks,
                    expected_retention_publishers,
                    retention_generation,
                    int(cast(int, retention_barrier.get("negative_fence_generation", 0))),
                    retention_barrier.get("negative_fence_token"),
                )
                and _retention_acknowledgements_passed(
                    pending_acknowledgements,
                    expected_retention_publishers,
                    retention_generation,
                    int(cast(int, pending_reconciliation.get("fence_generation", 0))),
                    pending_reconciliation.get("fence_token"),
                )
                and _retention_snapshot_window_passed(
                    barrier_initial,
                    int(cast(int, retention_barrier.get("negative_fence_generation", 0))),
                    retention_generation,
                    required_retention_seconds,
                    expected_retention_publishers,
                )
                and _retention_snapshot_window_passed(
                    barrier_final,
                    int(cast(int, retention_barrier.get("final_fence_generation", 0))),
                    retention_generation,
                    required_retention_seconds,
                    expected_retention_publishers,
                )
                and barrier_initial.get("fence_token")
                == retention_barrier.get("negative_fence_token")
                and barrier_final.get("fence_token") == retention_barrier.get("final_fence_token")
                and _retention_phase_order_passed(
                    retention_barrier,
                    pending_reconciliation,
                    retention_generation,
                    terminal_retention_fence_generation,
                )
                and int(cast(int, inputs.detach.get("retention_release_generation", -1)))
                == terminal_retention_fence_generation
                and inputs.detach.get("retention_release_token") == terminal_retention_fence_token
                and cast(bool, retention_barrier.get("passed", False))
                and reported_initial_blockers == computed_initial_blockers
                and set(computed_initial_blockers)
                >= {
                    "backscan_complete",
                    "blocked_batches",
                    "fixed_safe_cutoff",
                    "m1_cursor",
                    "m2_cursor",
                    "pre_generation_obligations",
                    "unresolved_quarantine",
                    "worklog_pending",
                }
                and reported_final_blockers == computed_final_blockers == []
                and int(cast(int, barrier_final.get("actual_pre_generation_pending", -1))) == 0
                and int(cast(int, barrier_final.get("blocked_batches", -1))) == 0
                and int(cast(int, barrier_final.get("pre_generation_obligations", -1))) == 0
                and int(cast(int, barrier_final.get("unresolved_quarantine", -1))) == 0
                and int(cast(int, barrier_final.get("worklog_pending", -1))) == 0
                and int(cast(int, barrier_final.get("wrong_generation_acknowledgements", -1))) == 0
                and barrier_final.get("fixed_safe_cutoff") == barrier_final.get("horizon")
                and barrier_final.get("m1_cursor") == barrier_final.get("horizon")
                and barrier_final.get("m2_cursor") == barrier_final.get("horizon")
                and cast(bool, barrier_final.get("monitor_lag_breach", False))
                and cast(bool, barrier_final.get("backscan_complete", False))
                and int(cast(int, retention_barrier.get("dispositioned_quarantine_rows", 0))) > 0
                and cast(str, retention_barrier.get("negative_rejection", ""))
                == "retention barrier blocked DETACH: " + ", ".join(computed_initial_blockers)
                and cast(str, inputs.detach.get("pending_state_before_detach", "")) == "PENDING"
                and int(cast(int, pending_reconciliation.get("winner_count", 0))) == 1
                and cast(str, pending_reconciliation.get("terminal_catalog", "")) == "POST"
                and cast(str, pending_reconciliation.get("resolution", "")) == "POST_COMMITTED"
                and int(cast(int, pending_reconciliation.get("resume_count", 0)))
                == expected_retention_publishers
                and cast(
                    bool,
                    pending_reconciliation.get("stale_pending_setter_denied", False),
                )
                and bool(
                    cast(
                        str,
                        pending_reconciliation.get("stale_pending_rejection", ""),
                    )
                )
                and cast(
                    str,
                    barrier_obligation_drain.get("blocked_batch_physical_table", ""),
                )
                == "trades_legacy"
                and int(cast(int, barrier_obligation_drain.get("drained_blocked_batches", 0))) == 1
                and int(cast(int, barrier_obligation_drain.get("drained_monitor_worklog", 0))) == 1
                and int(cast(int, barrier_obligation_drain.get("settled_live_obligations", 0))) == 1
                and all(
                    int(cast(int, barrier_monitor_scans.get(field_name, 0))) > 0
                    for field_name in (
                        "m1_backscan_rows",
                        "m1_rows_seen",
                        "m2_backscan_rows",
                        "m2_rows_seen",
                    )
                )
                and _lease_authorization_passed(
                    retention_first_authorization,
                    terminal_retention_fence_generation,
                    terminal_retention_fence_token,
                )
                and _lease_authorization_passed(
                    retention_authorization,
                    terminal_retention_fence_generation,
                    terminal_retention_fence_token,
                )
                and cast(bool, generation_serialization.get("serialized_before_latch", False))
                and cast(str, generation_serialization.get("old_row_physical_table", ""))
                == "trades_legacy"
                and cast(
                    bool,
                    generation_serialization.get("post_generation_row_absent_from_live", False),
                )
                and cast(
                    bool,
                    generation_serialization.get("post_generation_row_staged_pending", False),
                )
                and cast(
                    bool,
                    generation_serialization.get("obligation_worklog_matches", False),
                )
                and int(cast(int, generation_serialization.get("obligation_worklog_rows", 0))) == 2
                and cast(str, generation_serialization.get("latch_wait_event_type", "")) == "Lock"
                and cast(bool, inputs.detach["archive_gate_passed"])
                and int(cast(int, archive_gate.get("archive_days", 0))) == 56
                and int(cast(int, archive_gate.get("legacy_rows", -1)))
                == int(cast(int, archive_gate.get("archive_manifest_rows", -2)))
                == int(cast(int, archive_gate.get("restore_rows", -3)))
                and int(cast(int, archive_gate.get("full_column_missing", -1))) == 0
                and int(cast(int, archive_gate.get("full_column_extra", -1))) == 0
                and int(cast(int, archive_gate.get("paper_replay_missing", -1))) == 0
                and int(cast(int, archive_gate.get("paper_replay_extra", -1))) == 0
                and cast(bool, inputs.detach["legacy_drop_exercised"])
                and cast(
                    bool,
                    staged_replay.get(
                        "abort_left_pending_without_row_or_worklog",
                        False,
                    ),
                )
                and cast(bool, staged_replay.get("row_and_worklog_same_transaction", False))
                and staged_replay.get("first_applications") == [True]
                and staged_replay.get("idempotent_reruns") == [False]
                and int(cast(int, staged_replay.get("worklog_rows", 0))) == 1
                and int(cast(int, staged_replay.get("trade_rows", 0))) == 1
                and int(cast(int, staged_replay.get("full_row_missing", -1))) == 0
                and int(cast(int, staged_replay.get("full_row_extra", -1))) == 0
            ),
            evidence=inputs.detach,
        ),
        CriterionResult(
            number=6,
            name="deliberately non-UTC DDL with explicit-offset UTC H",
            passed=(
                cast(str, inputs.attach["ddl_session_initial_timezone"]) == _NON_UTC_TIMEZONE
                and cast(str, inputs.attach["ddl_transaction_timezone"]) == "UTC"
                and not cast(
                    bool,
                    timezone_mismatch_control["debug_implication_observed"],
                )
                and cast(
                    bool,
                    timezone_mismatch_control["scan_verification_observed"],
                )
                and float(cast(float | int, timezone_mismatch_control["attach_seconds"])) >= 0.05
                and cast(str, timezone_mismatch_control["check_literal"]).endswith("-01:00'")
                and cast(str, timezone_mismatch_control["check_instant"])
                == (inputs.horizon + timedelta(hours=1)).isoformat()
                and cast(str, timezone_mismatch_control["partition_bound_literal"])
                == _horizon_literal(inputs.horizon)
                and cast(str, bare_date_control["session_timezone"]) == _NON_UTC_TIMEZONE
                and not cast(bool, bare_date_control["same_instant"])
                and cast(str, bare_date_attach_control["sqlstate"]) == "23514"
                and cast(str, bare_date_attach_control["session_timezone"]) == _NON_UTC_TIMEZONE
                and _HORIZON_PATTERN.fullmatch(_horizon_literal(inputs.horizon)) is not None
                and inputs.attach["h_boundary_routes"]
                == {
                    "h_exact": f"trades_d{inputs.horizon.strftime('%Y%m%d')}",
                    "h_minus_1us": "trades_legacy",
                    "h_plus_1d": (
                        f"trades_d{(inputs.horizon + timedelta(days=1)).strftime('%Y%m%d')}"
                    ),
                }
            ),
            evidence={
                "ddl_session_initial_timezone": inputs.attach["ddl_session_initial_timezone"],
                "ddl_transaction_timezone": inputs.attach["ddl_transaction_timezone"],
                "h_boundary_routes": inputs.attach["h_boundary_routes"],
                "h_literal": _horizon_literal(inputs.horizon),
                "literal_pattern_asserted": _HORIZON_PATTERN.pattern,
                "bare_date_attach_negative_control": bare_date_attach_control,
                "bare_date_negative_control": bare_date_control,
                "timezone_mismatch_negative_control": timezone_mismatch_control,
            },
        ),
    ]


async def _run_rehearsal(
    instance: InstanceRef,
    config: RunConfig,
    paths: RuntimePaths,
    evidence: RehearsalEvidence,
) -> None:
    """Execute fixture, acceptance flow, kill matrix, and retention DETACH.

    Args:
        instance: Verified private PostgreSQL instance.
        config: Resource and protocol bounds.
        paths: Retained report and controller event paths.
        evidence: Mutable aggregate report evidence.

    Returns:
        None after every required assertion has run.
    """
    horizon = _aligned_horizon()
    evidence.fixture = await _create_fixture(instance, config, horizon)
    validate_kill, metrics, validate_identity, validate_stats = await _run_validate_kill(
        instance,
        config,
        paths,
        horizon,
    )
    evidence.validate = metrics
    evidence.issues.append(
        "VALIDATE ETA is a warm post-load filesystem-cache extrapolation; no host-wide cache "
        "eviction was attempted on the production trading/router host."
    )
    _replace_kill(evidence, validate_kill)
    evidence.identity_checks.append(validate_identity)
    parent_unique_negative = await _parent_unique_constraint_negative_control(
        instance,
        horizon,
    )
    evidence.fixture["parent_unique_constraint_negative_control"] = parent_unique_negative
    preludes = await _create_parent_preludes(instance, horizon)
    evidence.fixture["preludes"] = preludes
    writer_drop_kill, writer_drop_identity, writer_drop_stats = await _run_writer_drop_kill(
        instance,
        config,
        paths,
        horizon,
    )
    _replace_kill(evidence, writer_drop_kill)
    evidence.identity_checks.append(writer_drop_identity)
    attach, normal_identity, normal_stats = await _run_normal_cutover(
        instance,
        config,
        paths,
        horizon,
    )
    attach["parent_unique_constraint_negative_control"] = parent_unique_negative
    attach["implication_negative_controls"] = metrics.implication_negative_controls
    evidence.identity_checks.append(normal_identity)
    all_writer_stats = [writer_drop_stats, *validate_stats, *normal_stats]
    scenario_results: list[ScenarioResult] = []
    for scenario in ("after_begin", "during_commit", "after_commit", "orphan_timeout"):
        scenario_result = await _run_kill_scenario(
            instance,
            config,
            paths,
            horizon,
            scenario,
        )
        scenario_results.append(scenario_result)
        _replace_kill(evidence, _scenario_kill_result(scenario_result))
        evidence.identity_checks.append(scenario_result.identity)
    await _ensure_post(instance, config, paths, horizon)
    fixture_identity = await _fixture_identity_multiset(instance)
    pending_detach: dict[str, object] = {
        "archive_gate_passed": False,
        "default_attached_before": False,
        "default_rows_before_detach": -1,
        "first_plain_attempt_sqlstate": "",
        "legacy_drop_exercised": False,
        "plain_detach_attempts": 4,
        "plain_detach_succeeded": False,
    }
    before_detach = _criteria(
        AcceptanceInputs(
            attach=attach,
            identities=evidence.identity_checks,
            writer_stats=all_writer_stats,
            scenario_results=scenario_results,
            metrics=metrics,
            detach=pending_detach,
            horizon=horizon,
            configured_rows=config.rows,
        )
    )
    for result in before_detach:
        if result.number in {1, 4, 6}:
            _replace_criterion(evidence, result)
    _assert(
        fixture_identity.passed(),
        "terminal fixture identity multiset mismatch before retention",
    )
    try:
        (
            detach,
            retention_identity,
            terminal_fixture_identity,
            all_writer_identity,
            retention_stats,
        ) = await _exercise_detach(instance, config, paths, horizon)
        evidence.identity_checks.extend(
            (retention_identity, terminal_fixture_identity, all_writer_identity)
        )
        all_writer_stats.extend(retention_stats)
    except Exception as exc:
        _replace_criterion(
            evidence,
            CriterionResult(
                number=5,
                name="plain DETACH with DEFAULT under bounded lock retries",
                passed=False,
                evidence={
                    "error": f"{type(exc).__name__}: {exc}",
                    "reason": "DETACH/archive/DROP stage failed or remained incomplete",
                },
            ),
        )
        raise
    results = _criteria(
        AcceptanceInputs(
            attach=attach,
            identities=evidence.identity_checks,
            writer_stats=all_writer_stats,
            scenario_results=scenario_results,
            metrics=metrics,
            detach=detach,
            horizon=horizon,
            configured_rows=config.rows,
        )
    )
    for result in results:
        _replace_criterion(evidence, result)


def _repository_verification_commands() -> tuple[tuple[str, tuple[str, ...]], ...]:
    """Return the exact eleven constrained repository verification commands.

    Returns:
        Ordered names and argument vectors for every required targeted check.
    """
    python = str(_VENV_BIN / "python")
    source_files = (
        "scripts/trades_partition_rehearsal.py",
        "tests/scripts/test_trades_partition_rehearsal.py",
    )
    commands: tuple[tuple[str, tuple[str, ...]], ...] = (
        (
            "targeted pytest",
            (
                str(_VENV_BIN / "pytest"),
                "-q",
                "-W",
                "error",
                "tests/scripts/test_trades_partition_rehearsal.py",
            ),
        ),
        ("ruff", (str(_VENV_BIN / "ruff"), "check", *source_files)),
        ("black", (str(_VENV_BIN / "black"), "--check", *source_files)),
        ("isort", (str(_VENV_BIN / "isort"), "--check-only", *source_files)),
        ("targeted mypy", (str(_VENV_BIN / "mypy"), *source_files)),
        ("complexity", (python, "scripts/check_complexity_ratchet.py")),
        ("no comments", (python, "scripts/check_no_comments.py", "--strict")),
        (
            "docstrings",
            (
                python,
                "scripts/check_docstrings.py",
                "--strict",
                "--verbose",
                "--enforce-bdd",
                "--enforce-google-sections",
            ),
        ),
        (
            "coverage exclusions",
            (python, "scripts/check_coverage_exclusions.py", "--strict"),
        ),
        ("main guard", (python, "scripts/check_main_guard.py", "--strict")),
        ("init files", (python, "scripts/check_init_files.py", "--strict")),
    )
    _assert(
        tuple(name for name, argv in commands if argv) == _REQUIRED_VERIFICATION_NAMES,
        "repository verification manifest is incomplete or reordered",
    )
    return commands


def _run_repository_verification(
    paths: RuntimePaths,
    source_snapshot: SourceSnapshot,
) -> list[dict[str, object]]:
    """Run the constrained project quality gate and retain its actual output.

    Args:
        paths: Run directory receiving the combined quality log.
        source_snapshot: Initial source digests that every check must preserve.

    Returns:
        Command, exit code, output, and PASS/FAIL for every required check.
    """
    commands = _repository_verification_commands()
    environment = dict(os.environ)
    environment.pop("DATABASE_URL", None)
    environment["DB_URL"] = "sqlite+aiosqlite:///./data/dev.db"
    results: list[dict[str, object]] = []
    log_lines: list[str] = []
    _assert_source_snapshot(source_snapshot)
    for name, argv in commands:
        _assert_source_snapshot(source_snapshot)
        command_text = " ".join(argv)
        try:
            completed = subprocess.run(
                argv,
                cwd=_ROOT,
                env=environment,
                check=False,
                capture_output=True,
                text=True,
                timeout=180,
            )
            returncode = completed.returncode
            output = (completed.stdout + completed.stderr).strip() or "(no output)"
        except (OSError, subprocess.TimeoutExpired) as exc:
            returncode = -1
            output = f"{type(exc).__name__}: {exc}"
        _assert_source_snapshot(source_snapshot)
        passed = returncode == 0
        results.append(
            {
                "command": command_text,
                "name": name,
                "output": output,
                "returncode": returncode,
                "status": "PASS" if passed else "FAIL",
            }
        )
        log_lines.extend((f"{name}: {'PASS' if passed else 'FAIL'}", command_text, output, ""))
    _assert_source_snapshot(source_snapshot)
    paths.quality_log.write_text("\n".join(log_lines), encoding="utf-8")
    return results


def _report_json(
    evidence: RehearsalEvidence,
    config: RunConfig,
    paths: RuntimePaths,
    status: str,
) -> dict[str, object]:
    """Build the retained machine-readable report.

    Args:
        evidence: All completed acceptance and kill evidence.
        config: Requested fixture and protocol bounds.
        paths: Run identity and evidence locations.
        status: PASS or FAIL.

    Returns:
        JSON-compatible report dictionary.
    """
    planned_reports = {paths.report_json.resolve(), paths.report_markdown.resolve()}
    existing_files = {
        candidate.resolve() for candidate in paths.run_dir.rglob("*") if candidate.is_file()
    }
    files_created = [str(candidate) for candidate in sorted(planned_reports | existing_files)]
    validate: dict[str, object] | None = None
    if evidence.validate is not None:
        validate = {
            "bytes_per_second": evidence.validate.bytes_per_second,
            "conservative_eta_seconds": evidence.validate.conservative_eta_seconds,
            "fixture_heap_bytes": evidence.validate.heap_bytes,
            "fixture_rows": evidence.validate.fixture_rows,
            "production_heap_eta_seconds": evidence.validate.production_heap_eta_seconds,
            "production_rows_eta_seconds": evidence.validate.production_rows_eta_seconds,
            "rows_per_second": evidence.validate.rows_per_second,
            "seconds": evidence.validate.seconds,
            "raw_seconds": evidence.validate.raw_seconds,
            "controlled_pause_seconds": evidence.validate.controlled_pause_seconds,
            "writer_overlap_backend_pid": evidence.validate.writer_overlap_backend_pid,
            "active_recheck": evidence.validate.active_recheck,
            "cache_condition": evidence.validate.cache_condition,
            "implication_negative_controls": (evidence.validate.implication_negative_controls),
            "set_not_null_scan_control_seconds": (
                evidence.validate.set_not_null_scan_control_seconds
            ),
            "set_not_null_scan_skipped": evidence.validate.set_not_null_scan_skipped,
            "set_not_null_seconds": evidence.validate.set_not_null_seconds,
            "writer_commits_during": evidence.validate.writer_commits_during,
        }
    return {
        "acceptance_criteria": [result.as_json() for result in evidence.criteria],
        "config": {
            "cleanup_margin_seconds": config.cleanup_margin_seconds,
            "days": config.days,
            "guard_seconds": config.guard_seconds,
            "lease_seconds": config.lease_seconds,
            "rows": config.rows,
            "transaction_timeout_seconds": config.transaction_timeout_seconds,
            "writer_rate": config.writer_rate,
            "writers": config.writers,
        },
        "files_created": files_created,
        "fixture": evidence.fixture,
        "generated_at": datetime.now(UTC).isoformat(),
        "identity_checks": [
            {
                "expected": item.expected,
                "extra_or_changed": item.extra_or_changed,
                "missing": item.missing,
                "scenario": item.scenario,
            }
            for item in evidence.identity_checks
        ],
        "instance_lifecycle": evidence.lifecycle,
        "issues": evidence.issues,
        "kill_matrix": [result.as_json() for result in evidence.kills],
        "report_directory": str(paths.run_dir),
        "run_id": paths.run_id,
        "status": status,
        "validate": validate,
        "verification": evidence.verification,
    }


def _markdown_report(report: dict[str, object]) -> str:
    """Render the operator decision report in the requested section order.

    Args:
        report: Machine-readable report dictionary.

    Returns:
        Markdown report text.
    """
    status = cast(str, report["status"])
    fixture = cast(dict[str, object], report["fixture"])
    lifecycle = cast(dict[str, object], report["instance_lifecycle"])
    criteria = cast(list[dict[str, object]], report["acceptance_criteria"])
    kills = cast(list[dict[str, object]], report["kill_matrix"])
    validate = cast(dict[str, object] | None, report["validate"])
    verification = cast(list[dict[str, object]], report["verification"])
    files_created = cast(list[str], report["files_created"])
    issues = cast(list[str], report["issues"])
    lines = [
        f"# Trades partition adoption rehearsal: {status}",
        "",
        "## Summary",
        "",
        (
            "The harness rehearsed the settled CHECK → VALIDATE → NOT NULL → "
            "standalone-index parent → atomic ATTACH sequence on a private PostgreSQL 18.4 "
            f"instance. Overall result: **{status}**."
        ),
        "",
        "## Instance lifecycle",
        "",
        f"- Created: {lifecycle.get('created')}",
        f"- PostgreSQL: {cast(dict[str, object], lifecycle.get('host_safety', {})).get('postgres_version')}",
        f"- Data directory: {lifecycle.get('data_directory')}",
        f"- Unix-socket-only listener: {lifecycle.get('listen_addresses') == ''}",
        f"- Port: {lifecycle.get('port')}",
        f"- Supervisor nice/I/O: {lifecycle.get('nice')} / {lifecycle.get('ionice')}",
        (
            f"- Postmaster nice/I/O: {lifecycle.get('postmaster_nice')} / "
            f"{lifecycle.get('postmaster_ionice')}"
        ),
        f"- Parent-death signal for owned children: {lifecycle.get('parent_death_signal')}",
        f"- Harness script SHA-256: {lifecycle.get('script_sha256')}",
        f"- Targeted test SHA-256: {lifecycle.get('test_sha256')}",
        f"- PostgreSQL resource limits: {lifecycle.get('resource_settings')}",
        f"- PGDATA removed: {lifecycle.get('instance_directory_removed')}",
        f"- Unix socket directory removed: {lifecycle.get('socket_directory_removed')}",
        f"- PostgreSQL stop return code: {lifecycle.get('stop_returncode')}",
        "",
        "## Fixture shape",
        "",
        f"- Rows: {fixture.get('rows')}",
        f"- UTC days: {fixture.get('days')}",
        f"- Heap bytes: {fixture.get('heap_bytes')}",
        f"- Columns: {fixture.get('columns')}",
        f"- Constraints: {fixture.get('constraints')}",
        f"- Existing index manifest: {fixture.get('indexes')}",
        f"- Existing index definitions: {fixture.get('index_definitions')}",
        f"- Parent preludes: {fixture.get('preludes')}",
        "",
        "## Acceptance criteria",
        "",
    ]
    for criterion in criteria:
        lines.extend(
            (
                (
                    f"- **{criterion.get('status')} — {criterion.get('number')}. "
                    f"{criterion.get('name')}**"
                ),
                f"  Evidence: `{json.dumps(criterion.get('evidence'), sort_keys=True)}`",
            )
        )
    lines.extend(("", "## VALIDATE throughput", ""))
    if validate is None:
        lines.append("- FAIL: VALIDATE was not measured.")
    else:
        lines.extend(
            (
                f"- Measured seconds: {validate.get('seconds')}",
                f"- Raw seconds: {validate.get('raw_seconds')}",
                f"- Controlled pause seconds: {validate.get('controlled_pause_seconds')}",
                f"- Active VALIDATE PID rechecked: {validate.get('active_recheck')}",
                f"- VALIDATE backend PID: {validate.get('writer_overlap_backend_pid')}",
                f"- Cache condition: {validate.get('cache_condition')}",
                f"- Writer commits during VALIDATE: {validate.get('writer_commits_during')}",
                f"- Rows/s: {validate.get('rows_per_second')}",
                f"- Bytes/s: {validate.get('bytes_per_second')}",
                (
                    "- 377M-row ETA: "
                    f"{_duration_text(float(cast(float, validate['production_rows_eta_seconds'])))}"
                ),
                (
                    "- 59 GiB heap ETA: "
                    f"{_duration_text(float(cast(float, validate['production_heap_eta_seconds'])))}"
                ),
                (
                    "- Conservative production ETA: "
                    f"{_duration_text(float(cast(float, validate['conservative_eta_seconds'])))}"
                ),
                f"- SET NOT NULL seconds: {validate.get('set_not_null_seconds')}",
                (
                    "- SET NOT NULL forced-scan control seconds: "
                    f"{validate.get('set_not_null_scan_control_seconds')}"
                ),
                f"- SET NOT NULL scan skipped: {validate.get('set_not_null_scan_skipped')}",
            )
        )
    lines.extend(("", "## Kill matrix", ""))
    for kill in kills:
        lines.extend(
            (
                f"- **{kill.get('status')} — {kill.get('name')}**",
                f"  Evidence: `{json.dumps(kill.get('evidence'), sort_keys=True)}`",
            )
        )
    lines.extend(
        (
            "",
            "## Files created",
            "",
        )
    )
    lines.extend(f"- `{created_file}`" for created_file in files_created)
    lines.extend(("", "## Verification", ""))
    if verification:
        for result in verification:
            lines.extend(
                (
                    (
                        f"- **{result.get('status')} — {result.get('name')}** "
                        f"(exit {result.get('returncode')})"
                    ),
                    f"  Command: `{result.get('command')}`",
                    "",
                    "  ```text",
                    str(result.get("output")),
                    "  ```",
                )
            )
    else:
        lines.append("- FAIL: repository verification was not executed.")
    lines.extend(("", "## Issues", ""))
    if issues:
        lines.extend(f"- {issue}" for issue in issues)
    else:
        lines.append("- None.")
    lines.append("")
    return "\n".join(lines)


def _write_report(paths: RuntimePaths, report: dict[str, object]) -> None:
    """Persist JSON and Markdown reports without deleting earlier evidence.

    Args:
        paths: Exact retained report paths.
        report: Complete report dictionary.

    Returns:
        None.
    """
    paths.report_json.write_text(
        json.dumps(report, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    paths.report_markdown.write_text(_markdown_report(report), encoding="utf-8")


def _parse_supervisor_config(argv: Sequence[str] | None) -> RunConfig:
    """Parse the public supervisor-only command line.

    Args:
        argv: Optional argument list.

    Returns:
        Validated run configuration.
    """
    parser = argparse.ArgumentParser(
        description="Rehearse trades partition adoption on a private throwaway PostgreSQL 18.4"
    )
    parser.add_argument("--rows", type=int, default=10_000_000)
    parser.add_argument("--days", type=int, default=56)
    parser.add_argument("--writers", type=int, default=2)
    parser.add_argument("--writer-rate", type=float, default=80.0)
    parser.add_argument("--lease-seconds", type=float, default=10.0)
    parser.add_argument("--transaction-timeout-seconds", type=float, default=3.0)
    parser.add_argument("--cleanup-margin-seconds", type=float, default=1.0)
    parser.add_argument("--guard-seconds", type=float, default=0.5)
    parser.add_argument("--load-limit-per-cpu", type=float, default=1.5)
    args = parser.parse_args(argv)
    return RunConfig(
        rows=cast(int, args.rows),
        days=cast(int, args.days),
        writers=cast(int, args.writers),
        writer_rate=cast(float, args.writer_rate),
        lease_seconds=cast(float, args.lease_seconds),
        transaction_timeout_seconds=cast(float, args.transaction_timeout_seconds),
        cleanup_margin_seconds=cast(float, args.cleanup_margin_seconds),
        guard_seconds=cast(float, args.guard_seconds),
        load_limit_per_cpu=cast(float, args.load_limit_per_cpu),
    )


def _controller_parser() -> argparse.ArgumentParser:
    """Build the private self-controller argument parser.

    Returns:
        Parser for supervisor-created controller subprocesses.
    """
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument("--role", required=True)
    parser.add_argument("--scenario", required=True)
    parser.add_argument("--generation", required=True, type=int)
    parser.add_argument("--token", required=True)
    parser.add_argument("--horizon", required=True)
    parser.add_argument("--writers", required=True, type=int)
    parser.add_argument("--transaction-timeout-seconds", required=True, type=float)
    parser.add_argument("--cleanup-margin-seconds", required=True, type=float)
    parser.add_argument("--pgdata", required=True, type=Path)
    parser.add_argument("--socket-dir", required=True, type=Path)
    parser.add_argument("--port", required=True, type=int)
    parser.add_argument("--user", required=True)
    parser.add_argument("--postmaster-pid", required=True, type=int)
    parser.add_argument("--script-sha256", required=True)
    parser.add_argument("--test-sha256", required=True)
    parser.add_argument("--events-dir", required=True, type=Path)
    return parser


def _parse_private_child(argv: Sequence[str]) -> tuple[str, ControllerSpec]:
    """Parse and validate a supervisor-created private child command.

    Args:
        argv: Full controller or publisher argument list.

    Returns:
        Private role and exact child specification.
    """
    args = _controller_parser().parse_args(argv)
    role = cast(str, args.role)
    scenario = cast(str, args.scenario)
    valid_scenario = (
        role == "controller"
        and scenario in _CONTROLLER_SCENARIOS
        or role == "publisher"
        and scenario in _PUBLISHER_SCENARIOS
    )
    _assert(valid_scenario, f"invalid private role/scenario {role}/{scenario}")
    source_snapshot = SourceSnapshot(
        script_sha256=cast(str, args.script_sha256),
        test_sha256=cast(str, args.test_sha256),
    )
    _assert_source_snapshot(source_snapshot)
    instance = InstanceRef(
        pgdata=cast(Path, args.pgdata).resolve(),
        socket_dir=cast(Path, args.socket_dir).resolve(),
        port=cast(int, args.port),
        user=cast(str, args.user),
        postmaster_pid=cast(int, args.postmaster_pid),
        source_snapshot=source_snapshot,
    )
    _validate_instance_ref(instance)
    return (
        role,
        ControllerSpec(
            instance=instance,
            scenario=scenario,
            generation=cast(int, args.generation),
            token=UUID(cast(str, args.token)),
            horizon=datetime.fromisoformat(cast(str, args.horizon)),
            writers=cast(int, args.writers),
            transaction_timeout_seconds=cast(float, args.transaction_timeout_seconds),
            cleanup_margin_seconds=cast(float, args.cleanup_margin_seconds),
            events_dir=cast(Path, args.events_dir),
        ),
    )


async def _supervisor(config: RunConfig) -> tuple[int, Path]:
    """Own instance lifecycle and always write a truthful terminal report.

    Args:
        config: Public rehearsal configuration.

    Returns:
        Exit code and retained report path.
    """
    paths = _allocate_paths()
    evidence = RehearsalEvidence()
    _initialize_result_matrix(evidence)
    source_snapshot = _capture_source_snapshot()
    evidence.lifecycle = {
        "script_sha256": source_snapshot.script_sha256,
        "test_sha256": source_snapshot.test_sha256,
    }
    instance: InstanceRef | None = None
    postmaster_process: subprocess.Popen[bytes] | None = None
    status = "FAIL"
    try:
        priority = _lower_process_priority()
        safety = _host_safety_preflight(config, paths)
        instance, postmaster_process = _start_instance(paths, source_snapshot)
        postmaster_priority = _instance_priority(instance)
        resource_settings = await _instance_limits(instance)
        evidence.lifecycle.update(
            {
                "created": True,
                "data_directory": str(instance.pgdata),
                "host_safety": safety,
                "ionice": priority["ionice"],
                "listen_addresses": "",
                "nice": priority["nice"],
                "port": instance.port,
                "postmaster_pid": instance.postmaster_pid,
                "postmaster_ionice": postmaster_priority["ionice"],
                "postmaster_nice": postmaster_priority["nice"],
                "postmaster_session_id": instance.postmaster_pid,
                "parent_death_signal": "SIGKILL",
                "private_process_session": True,
                "resource_settings": resource_settings,
                "socket_directory": str(instance.socket_dir),
            }
        )
        await _run_rehearsal(instance, config, paths, evidence)
        _assert_source_snapshot(source_snapshot)
        status = "PASS" if evidence.passed() else "FAIL"
        if status == "FAIL":
            evidence.issues.append("one or more hard acceptance or kill-matrix assertions failed")
    except Exception as exc:
        evidence.issues.append(f"{type(exc).__name__}: {exc}")
        evidence.issues.append(traceback.format_exc())
    finally:
        try:
            lifecycle = _stop_instance(paths, instance, postmaster_process)
            evidence.lifecycle.update(lifecycle)
            _assert(
                cast(bool, lifecycle["cleanup_ok"]),
                f"throwaway cleanup reported errors: {lifecycle['cleanup_errors']}",
            )
        except Exception as cleanup_exc:
            evidence.issues.append(f"cleanup failure: {type(cleanup_exc).__name__}: {cleanup_exc}")
            status = "FAIL"
        try:
            evidence.verification = _run_repository_verification(paths, source_snapshot)
            failed_verification = [
                result for result in evidence.verification if cast(str, result["status"]) != "PASS"
            ]
            if failed_verification:
                evidence.issues.append(
                    f"{len(failed_verification)} repository verification commands failed"
                )
                status = "FAIL"
        except Exception as verification_exc:
            evidence.issues.append(
                f"verification failure: {type(verification_exc).__name__}: {verification_exc}"
            )
            status = "FAIL"
        report = _report_json(evidence, config, paths, status)
        _write_report(paths, report)
    return (0 if status == "PASS" else 1), paths.report_markdown


def main(argv: Sequence[str] | None = None) -> int:
    """Run either the public supervisor or a private killable controller.

    Args:
        argv: Optional command-line arguments.

    Returns:
        Zero only when every hard assertion passed.
    """
    arguments = list(sys.argv[1:] if argv is None else argv)
    if "--role" in arguments:
        role, spec = _parse_private_child(arguments)
        if role == "publisher":
            return asyncio.run(_run_writer_drop_child(spec))
        return asyncio.run(_controller_main(spec))
    config = _parse_supervisor_config(arguments)
    returncode, report_path = asyncio.run(_supervisor(config))
    print(f"Trades partition rehearsal report: {report_path}")
    return returncode


if __name__ == "__main__":
    raise SystemExit(main())
