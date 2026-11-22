"""Read-only production probe comparing cold replay vs in-memory checkpoint resume.

Usage:
    PYTHONPATH=src python scripts/pnl_checkpoint_resume_probe.py \
        --wallet <WALLET_PUBLIC_ID> --mode live --valuation-ccy USD --window-minutes 60

Executes exclusively over read paths with SQLite ``PRAGMA query_only = ON`` and
PostgreSQL ``READ ONLY`` session characteristics, SQL DML/DDL statement checks,
and ORM flush/commit guards so no database write can ever commit.
"""

import argparse
import asyncio
import math
import re
import sys
import time
from collections.abc import Iterator
from collections.abc import Sequence
from contextlib import contextmanager
from dataclasses import dataclass
from dataclasses import fields
from dataclasses import is_dataclass
from datetime import datetime
from datetime import timedelta
from typing import Protocol
from typing import cast

from sqlalchemy import event
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session

from snapper.application.portfolio.fx_conversion_shadow import POINTWISE_FX_ELECTION_POLICY_VERSION
from snapper.application.portfolio.pnl_anchor_identity import normalize_portfolio_pnl_valuation_ccy
from snapper.application.portfolio.pnl_anchor_identity import (
    normalize_portfolio_pnl_wallet_public_id,
)
from snapper.application.portfolio.pnl_opening_checkpoint_capture import PnlCheckpointCaptureRequest
from snapper.application.portfolio.pnl_opening_checkpoint_certificate import (
    execution_manifest_digest,
)
from snapper.application.portfolio.pnl_opening_checkpoint_verifier import PnlCheckpointFallback
from snapper.application.portfolio.pnl_opening_checkpoint_verifier import PnlCheckpointResume
from snapper.application.portfolio.pnl_opening_checkpoint_verifier import (
    PnlCheckpointResumeEligibility,
)
from snapper.application.portfolio.pnl_opening_checkpoint_verifier import (
    verify_checkpoint_for_resume,
)
from snapper.application.portfolio.pnl_snapshot_planner import FINALIZATION_LAG
from snapper.application.portfolio.pnl_snapshotter_checkpoint import checkpoint_anchor_identity
from snapper.application.portfolio.pnl_timeline_service import PNL_TIMELINE_CALC_VERSION
from snapper.application.portfolio.pnl_timeline_service import PnlSeriesReadPolicy
from snapper.application.portfolio.pnl_timeline_service import PnlSeriesReplayOptions
from snapper.application.portfolio.pnl_timeline_service import PnlWalletSeriesResult
from snapper.application.portfolio.pnl_timeline_service import build_wallet_pnl_series
from snapper.config.settings import get_bootstrap_settings
from snapper.core.json_types import JsonValue
from snapper.data.pnl_correction_state import PnlCorrectionFrontier
from snapper.data.pnl_correction_state import PnlOpeningCheckpointRecord
from snapper.data.repository import PortfolioPnlSampleScope
from snapper.data.repository import Repository
from snapper.data.repository import SQLAlchemyRepository
from snapper.data.repository_types import PnlTimelineExecutionPrefix
from snapper.data.repository_types import PnlTimelineFillWitnessCut
from snapper.data.repository_types import PortfolioPnlAnchorRow

_MINUTE = timedelta(minutes=1)
_SQL_NOISE_RE = re.compile(r"'(?:''|[^'])*'|--[^\n]*|/\*[\s\S]*?\*/")
_READ_ONLY_VERBS = frozenset(
    {
        "SELECT",
        "SHOW",
        "VALUES",
        "EXPLAIN",
        "BEGIN",
        "COMMIT",
        "END",
        "ROLLBACK",
        "SAVEPOINT",
        "RELEASE",
    }
)
_WRITE_KEYWORDS = frozenset(
    {
        "INSERT",
        "UPDATE",
        "DELETE",
        "REPLACE",
        "MERGE",
        "UPSERT",
        "CREATE",
        "ALTER",
        "DROP",
        "TRUNCATE",
        "VACUUM",
        "ATTACH",
        "DETACH",
        "GRANT",
        "REVOKE",
        "COPY",
        "CALL",
        "DO",
        "LOCK",
    }
)


class CheckpointResumeProbeError(RuntimeError):
    """Explicit failure when read-only invariants or resume equivalence fail."""


class _CursorProtocol(Protocol):
    """Minimal DBAPI cursor surface used for read-only connection setup."""

    def execute(self, statement: str) -> object:
        """Execute a session read-only statement on the connection."""
        ...

    def close(self) -> object:
        """Close the temporary cursor."""
        ...


class _DbapiConnectionProtocol(Protocol):
    """Minimal DBAPI connection surface providing a cursor."""

    def cursor(self) -> _CursorProtocol:
        """Open a DBAPI cursor for connection initialization."""
        ...


@dataclass(frozen=True)
class CheckpointResumeProbeConfig:
    """Operator parameters selecting one wallet/scope window and repository URL."""

    wallet_public_id: str
    mode: str = "live"
    valuation_ccy: str = "USD"
    window_minutes: int = 60
    db_url: str | None = None


@dataclass(frozen=True)
class CheckpointResumeProbeReport:
    """Measured timings and bit-exact comparison verdict for one probed window."""

    wallet_public_id: str
    mode: str
    valuation_ccy: str
    calc_version: str
    checkpoint_minute: datetime
    frontier_tip: datetime
    point_count: int
    rate_source_count: int
    cold_seconds: float
    capture_seconds: float
    verify_seconds: float
    resume_seconds: float
    matched: bool
    mismatch_detail: str | None = None


@dataclass(frozen=True)
class _PreparedScope:
    """Validated anchor, frontier, and checkpoint/suffix window bounds."""

    scope: PortfolioPnlSampleScope
    anchor: PortfolioPnlAnchorRow
    frontier: PnlCorrectionFrontier
    checkpoint_minute: datetime
    suffix_from: datetime
    capture_as_of: datetime


def _require(condition: bool, reason: str) -> None:
    """Raise ``CheckpointResumeProbeError`` unless ``condition`` holds.

    Args:
        condition: Invariant condition required for safe probing.
        reason: Stable failure code included in the raised exception.

    Raises:
        CheckpointResumeProbeError: When ``condition`` evaluates to ``False``.
    """
    if not condition:
        raise CheckpointResumeProbeError(reason)


def _is_read_only_pragma(normalized: str) -> bool:
    """Return whether a SQLite ``PRAGMA`` statement performs no mutation.

    Args:
        normalized: Whitespace-collapsed uppercase SQL fragment starting with ``PRAGMA``.

    Returns:
        ``True`` only for read-only introspection pragmas or ``PRAGMA QUERY_ONLY = ON/1/TRUE``.
    """
    if "=" not in normalized:
        return True
    compact = normalized.replace(" ", "")
    return compact in {
        "PRAGMAQUERY_ONLY=ON",
        "PRAGMAQUERY_ONLY=1",
        "PRAGMAQUERY_ONLY=TRUE",
    }


def _is_read_only_set(normalized: str) -> bool:
    """Return whether a PostgreSQL ``SET`` statement pins read-only execution.

    Args:
        normalized: Whitespace-collapsed uppercase SQL fragment starting with ``SET``.

    Returns:
        ``True`` only for read-only session or transaction characteristics.
    """
    if "READ WRITE" in normalized:
        return False
    return normalized == "SET SESSION CHARACTERISTICS AS TRANSACTION READ ONLY" or (
        normalized.startswith("SET TRANSACTION ") and normalized.endswith("READ ONLY")
    )


def _assert_fragment_read_only(fragment: str) -> None:
    """Reject any SQL fragment capable of mutating database state or settings.

    Args:
        fragment: Comment- and literal-stripped SQL fragment without semicolons.

    Raises:
        CheckpointResumeProbeError: When the fragment is not strictly read-only.
    """
    normalized = " ".join(fragment.split()).upper()
    tokens = re.findall(r"[A-Z_]+", normalized)
    if not tokens:
        return
    verb = tokens[0]
    if verb in _READ_ONLY_VERBS:
        return
    if verb == "WITH" and not (_WRITE_KEYWORDS & set(tokens)):
        return
    if verb == "PRAGMA" and _is_read_only_pragma(normalized):
        return
    if verb == "SET" and _is_read_only_set(normalized):
        return
    raise CheckpointResumeProbeError(f"write_sql_refused:{verb}")


def _assert_read_only_sql(statement: str) -> None:
    """Validate every semicolon-delimited statement in ``statement`` is read-only.

    Args:
        statement: Raw SQL string submitted to SQLAlchemy cursor execution.
    """
    cleaned = _SQL_NOISE_RE.sub(" ", statement)
    for fragment in cleaned.split(";"):
        _assert_fragment_read_only(fragment)


def _enforce_connection_read_only(dbapi_connection: object, dialect_name: str) -> None:
    """Enable read-only transaction enforcement on a DBAPI connection.

    Args:
        dbapi_connection: Raw driver connection opened or checked out by SQLAlchemy.
        dialect_name: SQLAlchemy dialect name (``sqlite`` or ``postgresql``).

    Raises:
        CheckpointResumeProbeError: For unsupported database dialects.
    """
    conn = cast(_DbapiConnectionProtocol, dbapi_connection)
    cursor = conn.cursor()
    try:
        if dialect_name == "sqlite":
            cursor.execute("PRAGMA query_only = ON")
        elif dialect_name == "postgresql":
            cursor.execute("SET SESSION CHARACTERISTICS AS TRANSACTION READ ONLY")
        else:
            raise CheckpointResumeProbeError(f"unsupported_dialect:{dialect_name}")
    finally:
        cursor.close()


def _on_before_cursor_execute(
    _conn: object,
    _cursor: object,
    statement: str,
    *_rest: object,
) -> None:
    """Refuse non-read-only SQL before cursor execution reaches the driver.

    Args:
        _conn: SQLAlchemy connection (unused).
        _cursor: DBAPI cursor (unused).
        statement: SQL statement text about to execute.
        *_rest: Remaining SQLAlchemy listener positional arguments.
    """
    _assert_read_only_sql(statement)


def _guard_session_write(session: object, *_args: object) -> None:
    """Refuse ORM flushes or commits whenever any pending mutations are staged.

    Args:
        session: SQLAlchemy ORM session entering ``before_flush`` or ``before_commit``.
        *_args: Additional SQLAlchemy event hook arguments (unused).

    Raises:
        CheckpointResumeProbeError: When ``session`` carries new, dirty, or deleted state.
    """
    if isinstance(session, Session) and (session.new or session.dirty or session.deleted):
        raise CheckpointResumeProbeError("write_commit_detected")


@contextmanager
def _read_only_guards(repo: Repository) -> Iterator[SQLAlchemyRepository]:
    """Attach DBAPI read-only mode, SQL filters, and ORM commit guards to ``repo``.

    Args:
        repo: Candidate repository instance supplied to the probe.

    Yields:
        Verified ``SQLAlchemyRepository`` with active read-only event guards.

    Raises:
        CheckpointResumeProbeError: If ``repo`` is not a ``SQLAlchemyRepository``.
    """
    _require(isinstance(repo, SQLAlchemyRepository), "sqlalchemy_repository_required")
    sa_repo = cast(SQLAlchemyRepository, repo)
    sync_engine = sa_repo.engine.sync_engine
    dialect_name = sa_repo.dialect_name

    def _on_connect(dbapi_connection: object, _connection_record: object) -> None:
        """Apply DBAPI read-only mode on newly opened connections."""
        _enforce_connection_read_only(dbapi_connection, dialect_name)

    def _on_checkout(
        dbapi_connection: object,
        _connection_record: object,
        _connection_proxy: object,
    ) -> None:
        """Re-assert DBAPI read-only mode when borrowing pooled connections."""
        _enforce_connection_read_only(dbapi_connection, dialect_name)

    event.listen(sync_engine, "connect", _on_connect)
    event.listen(sync_engine, "checkout", _on_checkout)
    event.listen(sync_engine, "before_cursor_execute", _on_before_cursor_execute)
    event.listen(Session, "before_flush", _guard_session_write)
    event.listen(Session, "before_commit", _guard_session_write)
    try:
        yield sa_repo
    finally:
        event.remove(Session, "before_commit", _guard_session_write)
        event.remove(Session, "before_flush", _guard_session_write)
        event.remove(sync_engine, "before_cursor_execute", _on_before_cursor_execute)
        event.remove(sync_engine, "checkout", _on_checkout)
        event.remove(sync_engine, "connect", _on_connect)


async def _preflight_read_only(
    repo: SQLAlchemyRepository,
    policy: PnlSeriesReadPolicy,
) -> None:
    """Verify read policy, SQL/ORM hooks, and live connection read-only status.

    Args:
        repo: Guarded SQLAlchemy repository instance.
        policy: Series read policy that will govern all probe service calls.

    Raises:
        CheckpointResumeProbeError: When any write hazard or missing guard is detected.
    """
    _require(not policy.allow_anchor_creation, "write_policy_allow_anchor_creation")
    sync_engine = repo.engine.sync_engine
    _require(
        event.contains(sync_engine, "before_cursor_execute", _on_before_cursor_execute)
        and event.contains(Session, "before_commit", _guard_session_write),
        "read_only_guard_missing",
    )
    async with repo.session() as session:
        if repo.dialect_name == "sqlite":
            value = (await session.execute(text("PRAGMA query_only"))).scalar_one()
            _require(value == 1, "database_write_would_commit")
        elif repo.dialect_name == "postgresql":
            value = (await session.execute(text("SHOW transaction_read_only"))).scalar_one()
            _require(str(value).lower() == "on", "database_write_would_commit")
        else:
            raise CheckpointResumeProbeError(f"unsupported_dialect:{repo.dialect_name}")


def _validate_config(config: CheckpointResumeProbeConfig) -> tuple[str, str, str]:
    """Validate and normalize operator wallet, trading mode, and valuation currency.

    Args:
        config: Raw operator configuration parameters.

    Returns:
        Normalized ``(wallet_public_id, mode, valuation_ccy)`` tuple.
    """
    wallet = normalize_portfolio_pnl_wallet_public_id(config.wallet_public_id)
    _require(config.mode in ("live", "paper"), f"invalid_mode:{config.mode}")
    ccy = normalize_portfolio_pnl_valuation_ccy(config.valuation_ccy)
    _require(config.window_minutes >= 1, f"invalid_window_minutes:{config.window_minutes}")
    return wallet, config.mode, ccy


async def _load_prepared_scope(
    repo: SQLAlchemyRepository,
    config: CheckpointResumeProbeConfig,
) -> _PreparedScope:
    """Load and validate anchor, correction frontier, and window bounds for probing.

    Args:
        repo: Guarded read-only repository.
        config: Validated operator probe parameters.

    Returns:
        Prepared scope, anchor, frontier, and checkpoint/suffix minute bounds.
    """
    wallet, mode, ccy = _validate_config(config)
    anchor = await repo.get_portfolio_pnl_anchor(wallet, mode, ccy, None)
    _require(anchor is not None, "missing_active_anchor")
    assert anchor is not None
    t0 = anchor["point_time"]
    scope = PortfolioPnlSampleScope(
        wallet_public_id=wallet,
        mode=mode,
        valuation_ccy=ccy,
        epoch_public_id=anchor["epoch_public_id"],
        anchor_point_time=t0,
    )
    frontier = await repo.get_portfolio_pnl_correction_frontier(scope)
    _require(frontier is not None, "missing_correction_frontier")
    assert frontier is not None
    _require(frontier.has_fill_gap is False, "frontier_has_fill_gap")
    _require(
        frontier.raw_execution_watermarks is not None
        and frontier.execution_manifest_digest is not None
        and frontier.expected_venues is not None
        and frontier.venue_event_watermarks is not None
        and frontier.fill_witness_digest is not None,
        "frontier_incomplete_cut",
    )
    earliest_checkpoint = t0 + _MINUTE
    checkpoint_minute = max(
        earliest_checkpoint,
        frontier.tip - timedelta(minutes=config.window_minutes),
    )
    _require(frontier.tip > checkpoint_minute, "frontier_window_too_short")
    capture_as_of = min(frontier.as_of, checkpoint_minute + FINALIZATION_LAG)
    return _PreparedScope(
        scope=scope,
        anchor=anchor,
        frontier=frontier,
        checkpoint_minute=checkpoint_minute,
        suffix_from=checkpoint_minute + _MINUTE,
        capture_as_of=capture_as_of,
    )


def _frontier_witness_cut(frontier: PnlCorrectionFrontier) -> PnlTimelineFillWitnessCut:
    """Extract the typed frozen fill-witness cut from ``frontier``.

    Args:
        frontier: Frontier previously validated to contain complete cut metadata.

    Returns:
        Frozen fill-witness cut mapping for replay options.
    """
    assert frontier.venue_event_watermarks is not None
    assert frontier.fill_witness_digest is not None
    return {
        "watermarks": dict(frontier.venue_event_watermarks),
        "digest": frontier.fill_witness_digest,
    }


async def _capture_and_verify_in_memory(
    repo: SQLAlchemyRepository,
    prepared: _PreparedScope,
    read_policy: PnlSeriesReadPolicy,
) -> tuple[PnlCheckpointResume, float, float]:
    """Capture a checkpoint at C purely in memory and verify it against frontier T.

    Args:
        repo: Guarded read-only repository.
        prepared: Prepared scope, frontier, and window bounds.
        read_policy: Read policy with ``allow_anchor_creation=False``.

    Returns:
        Tuple of verified resume state, capture seconds, and verification seconds.
    """
    scope, anchor, frontier = prepared.scope, prepared.anchor, prepared.frontier
    capture_prefix: PnlTimelineExecutionPrefix = await repo.get_pnl_timeline_execution_prefix(
        scope.wallet_public_id,
        scope.mode,
        prepared.capture_as_of,
        capture_fill_witness_cut=True,
    )
    _require(
        not await repo.pnl_timeline_scope_has_fill_gap(
            scope.wallet_public_id,
            scope.mode,
            prepared.capture_as_of,
            capture_prefix,
        ),
        "capture_fill_gap",
    )
    witness = capture_prefix.get("fill_witness_cut")
    _require(witness is not None, "missing_capture_fill_witness_cut")
    assert witness is not None
    assert frontier.expected_venues is not None
    anchor_public_id, anchor_digest = checkpoint_anchor_identity(anchor)
    capture_request = PnlCheckpointCaptureRequest(
        key=frontier.key,
        source_generation=frontier.generation,
        raw_execution_watermarks=dict(capture_prefix["watermarks"]),
        execution_manifest_digest=execution_manifest_digest(capture_prefix["annulments"]),
        expected_venues=frontier.expected_venues,
        venue_event_watermarks=dict(witness["watermarks"]),
        fill_witness_digest=witness["digest"],
        anchor_public_id=anchor_public_id,
        anchor_digest=anchor_digest,
        calc_version=frontier.key.calc_version,
    )
    capture_started = time.perf_counter()
    capture_result = await build_wallet_pnl_series(
        repo,
        scope.wallet_public_id,
        scope.mode,
        anchor["point_time"] + _MINUTE,
        prepared.checkpoint_minute,
        "1m",
        prepared.capture_as_of,
        valuation_ccy=scope.valuation_ccy,
        policy=read_policy,
        options=PnlSeriesReplayOptions(
            baseline_watermarks=dict(frontier.baseline_watermarks),
            frozen_execution_watermarks=capture_prefix["watermarks"],
            frozen_execution_manifest_digest=capture_request.execution_manifest_digest,
            frozen_has_fill_gap=False,
            frozen_fill_witness_cut=witness,
            replay_generation_tip=prepared.checkpoint_minute,
            pointwise_exact_fx=True,
            checkpoint_capture=capture_request,
        ),
    )
    capture_seconds = time.perf_counter() - capture_started
    _require(
        capture_result.checkpoint_write is not None,
        f"checkpoint_capture_refused:{capture_result.checkpoint_capture_refusal}",
    )
    assert capture_result.checkpoint_write is not None
    write = capture_result.checkpoint_write
    record = PnlOpeningCheckpointRecord(
        certificate=write.certificate,
        state_json=write.state_json,
        updated_at=prepared.capture_as_of,
    )
    verify_started = time.perf_counter()
    verified = await verify_checkpoint_for_resume(
        repo,
        record,
        scope,
        frontier.as_of,
        PnlCheckpointResumeEligibility(
            mode="active",
            calc_version=frontier.key.calc_version,
            timeline_calc_version=PNL_TIMELINE_CALC_VERSION,
            fx_policy_version=POINTWISE_FX_ELECTION_POLICY_VERSION,
            expected_anchor_public_id=anchor_public_id,
            expected_anchor_digest=anchor_digest,
            pointwise_exact_fx=True,
            replay_generation_tip=frontier.tip,
            metadata_only=False,
            from_time=prepared.suffix_from,
            frontier_tip=frontier.tip,
            has_fill_gap=False,
            expected_execution_manifest_digest=frontier.execution_manifest_digest,
        ),
    )
    verify_seconds = time.perf_counter() - verify_started
    if isinstance(verified, PnlCheckpointFallback):
        raise CheckpointResumeProbeError(f"checkpoint_verify_fallback:{verified.reason}")
    return verified, capture_seconds, verify_seconds


async def _run_window_series(
    repo: SQLAlchemyRepository,
    prepared: _PreparedScope,
    read_policy: PnlSeriesReadPolicy,
    resume: PnlCheckpointResume | None,
) -> tuple[PnlWalletSeriesResult, float]:
    """Execute one timed cold or resumed replay over ``[suffix_from, frontier.tip]``.

    Args:
        repo: Guarded read-only repository.
        prepared: Prepared scope, frontier, and window bounds.
        read_policy: Read policy with ``allow_anchor_creation=False``.
        resume: Verified opening-checkpoint resume state, or ``None`` for cold replay.

    Returns:
        Series result paired with elapsed wall-clock seconds.
    """
    scope, frontier = prepared.scope, prepared.frontier
    options = PnlSeriesReplayOptions(
        baseline_watermarks=dict(frontier.baseline_watermarks),
        frozen_execution_watermarks=frontier.raw_execution_watermarks,
        frozen_execution_manifest_digest=frontier.execution_manifest_digest,
        frozen_has_fill_gap=False,
        frozen_fill_witness_cut=_frontier_witness_cut(frontier),
        replay_generation_tip=frontier.tip,
        pointwise_exact_fx=True,
        checkpoint_resume=resume,
    )
    started = time.perf_counter()
    result = await build_wallet_pnl_series(
        repo,
        scope.wallet_public_id,
        scope.mode,
        prepared.suffix_from,
        frontier.tip,
        "1m",
        frontier.as_of,
        valuation_ccy=scope.valuation_ccy,
        policy=read_policy,
        options=options,
    )
    elapsed = time.perf_counter() - started
    return result, elapsed


def _canonical(value: object) -> JsonValue:
    """Convert point or rate-source values to float-hex, NaN-aware JSON primitives.

    Args:
        value: Scalar, datetime, tuple, or frozen dataclass field value.

    Returns:
        Canonical JSON structure preserving float bit representations.
    """
    if value is None or isinstance(value, (str, bool, int)):
        return value
    if isinstance(value, float):
        return "nan" if math.isnan(value) else value.hex()
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, tuple):
        return [_canonical(item) for item in value]
    if is_dataclass(value) and not isinstance(value, type):
        return {item.name: _canonical(getattr(value, item.name)) for item in fields(value)}
    raise TypeError(f"Unsupported probe comparison value: {type(value).__name__}")


def _compare_sequences(
    label: str,
    left: Sequence[object],
    right: Sequence[object],
) -> str | None:
    """Compare two sequences element-wise using bit-exact float serialization.

    Args:
        label: Result field group name (``points`` or ``rate_sources``).
        left: Cold series sequence.
        right: Resumed series sequence.

    Returns:
        Mismatch descriptor string, or ``None`` when bit-identical.
    """
    if len(left) != len(right):
        return f"{label}.length:{len(left)}!={len(right)}"
    for index, (left_item, right_item) in enumerate(zip(left, right, strict=True)):
        if _canonical(left_item) != _canonical(right_item):
            return f"{label}[{index}]"
    return None


def _compare_series(
    cold: PnlWalletSeriesResult,
    resumed: PnlWalletSeriesResult,
) -> str | None:
    """Compare ``points`` and ``rate_sources`` bit-for-bit across cold and resumed results.

    Args:
        cold: Cold anchor replay result over ``[suffix_from, frontier.tip]``.
        resumed: Checkpoint-resumed replay result over ``[suffix_from, frontier.tip]``.

    Returns:
        First mismatch location, or ``None`` when both ``points`` and ``rate_sources`` match.
    """
    points_mismatch = _compare_sequences("points", cold.points, resumed.points)
    if points_mismatch is not None:
        return points_mismatch
    return _compare_sequences("rate_sources", cold.rate_sources, resumed.rate_sources)


async def probe_with_repository(
    repo: Repository,
    config: CheckpointResumeProbeConfig,
    *,
    read_policy: PnlSeriesReadPolicy | None = None,
) -> CheckpointResumeProbeReport:
    """Run read-only preflight, cold replay, in-memory capture, resume, and comparison.

    Args:
        repo: Database repository (must be ``SQLAlchemyRepository``).
        config: Operator wallet, mode, currency, and suffix-window parameters.
        read_policy: Optional override read policy (defaults to non-mutating policy).

    Returns:
        Probe timings and bit-exact comparison report.

    Raises:
        CheckpointResumeProbeError: When any read-only check, capture, or resume refuses.
    """
    policy = read_policy or PnlSeriesReadPolicy(
        allow_anchor_creation=False,
        current_truth_horizon=False,
    )
    with _read_only_guards(repo) as sa_repo:
        await _preflight_read_only(sa_repo, policy)
        prepared = await _load_prepared_scope(sa_repo, config)
        cold_result, cold_seconds = await _run_window_series(sa_repo, prepared, policy, None)
        verified, capture_seconds, verify_seconds = await _capture_and_verify_in_memory(
            sa_repo,
            prepared,
            policy,
        )
        resumed_result, resume_seconds = await _run_window_series(
            sa_repo,
            prepared,
            policy,
            verified,
        )
        _require(
            resumed_result.checkpoint_resume_fallback is None,
            f"checkpoint_resume_fallback:{resumed_result.checkpoint_resume_fallback}",
        )
    mismatch = _compare_series(cold_result, resumed_result)
    return CheckpointResumeProbeReport(
        wallet_public_id=prepared.scope.wallet_public_id,
        mode=prepared.scope.mode,
        valuation_ccy=prepared.scope.valuation_ccy,
        calc_version=prepared.frontier.key.calc_version,
        checkpoint_minute=prepared.checkpoint_minute,
        frontier_tip=prepared.frontier.tip,
        point_count=len(cold_result.points),
        rate_source_count=len(cold_result.rate_sources),
        cold_seconds=cold_seconds,
        capture_seconds=capture_seconds,
        verify_seconds=verify_seconds,
        resume_seconds=resume_seconds,
        matched=mismatch is None,
        mismatch_detail=mismatch,
    )


async def probe_wallet_checkpoint_resume(
    config: CheckpointResumeProbeConfig,
) -> CheckpointResumeProbeReport:
    """Open an isolated repository for ``config`` and run the read-only resume probe.

    Args:
        config: Operator probe parameters including optional explicit database URL.

    Returns:
        Probe timings and bit-exact comparison report.
    """
    url = config.db_url or get_bootstrap_settings().db_url
    repo = SQLAlchemyRepository(url)
    try:
        return await probe_with_repository(repo, config)
    finally:
        await repo.engine.dispose()


def _format_report(report: CheckpointResumeProbeReport) -> str:
    """Format probe timings and comparison verdict as operator-readable stdout lines.

    Args:
        report: Completed probe report returned by :func:`probe_wallet_checkpoint_resume`.

    Returns:
        Multi-line summary containing scope, timings, counts, and equivalence status.
    """
    return "\n".join(
        (
            (
                f"wallet={report.wallet_public_id} mode={report.mode} "
                f"ccy={report.valuation_ccy} calc_version={report.calc_version}"
            ),
            (
                f"checkpoint_minute={report.checkpoint_minute.isoformat()} "
                f"frontier_tip={report.frontier_tip.isoformat()}"
            ),
            f"points={report.point_count} rate_sources={report.rate_source_count}",
            (
                f"cold_seconds={report.cold_seconds:.6f} "
                f"resume_seconds={report.resume_seconds:.6f} "
                f"capture_seconds={report.capture_seconds:.6f} "
                f"verify_seconds={report.verify_seconds:.6f}"
            ),
            f"bit_identical={report.matched} mismatch_detail={report.mismatch_detail or 'none'}",
        )
    )


def build_parser() -> argparse.ArgumentParser:
    """Build the CLI argument parser for the read-only checkpoint resume probe.

    Returns:
        Configured argument parser accepting wallet, scope, window, and optional URL.
    """
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--wallet", required=True, help="Wallet public ID to probe.")
    parser.add_argument("--mode", default="live", choices=("live", "paper"))
    parser.add_argument("--valuation-ccy", default="USD")
    parser.add_argument("--window-minutes", type=int, default=60)
    parser.add_argument("--db-url", default=None)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    """Run the read-only checkpoint resume probe CLI and print timings and verdict.

    Args:
        argv: Optional command-line arguments overriding ``sys.argv[1:]``.

    Returns:
        Zero when cold and resumed results match bit-for-bit, one on mismatch, two on error.
    """
    arguments = build_parser().parse_args(list(argv) if argv is not None else None)
    config = CheckpointResumeProbeConfig(
        wallet_public_id=arguments.wallet,
        mode=arguments.mode,
        valuation_ccy=arguments.valuation_ccy,
        window_minutes=arguments.window_minutes,
        db_url=arguments.db_url,
    )
    try:
        report = asyncio.run(probe_wallet_checkpoint_resume(config))
    except (CheckpointResumeProbeError, ValueError, OSError, SQLAlchemyError) as failure:
        print(f"checkpoint resume probe refused: {failure}", file=sys.stderr)
        return 2
    print(_format_report(report))
    return 0 if report.matched else 1


if __name__ == "__main__":
    raise SystemExit(main())
