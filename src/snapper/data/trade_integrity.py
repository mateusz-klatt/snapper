"""Bounded database primitives for the two trade-integrity monitors."""

from dataclasses import dataclass
from datetime import datetime
from datetime import timedelta
from typing import Final
from typing import TypedDict
from typing import cast

from sqlalchemy import and_
from sqlalchemy import delete
from sqlalchemy import literal_column
from sqlalchemy import or_
from sqlalchemy import select
from sqlalchemy import text
from sqlalchemy import tuple_
from sqlalchemy import update
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import aliased
from sqlalchemy.sql.elements import ColumnElement

from snapper.data.models import Trade
from snapper.data.models import TradeIntegrityMonitorCursor
from snapper.data.models import TradeIntegrityWorkItem
from snapper.data.repository_types import TradeIntegrityFinding
from snapper.data.repository_types import TradeIntegrityMonitor
from snapper.data.repository_types import TradeIntegrityRunResult

TRADE_INTEGRITY_OVERLAP: Final = timedelta(hours=6)
"""Fixed replay interval, 4.2 times the measured mean full-queue residence.

The 400,000-row writer queue represents about 86 minutes at 6.7 million
trades per day. Six hours keeps ordinary queue and retry delay inside a
wide operational margin. Restore/import/replay obligations do not rely
on this bound because they enter the durable worklog. The live writer
has no absolute retry-age ceiling, so this remains an operational bound;
the half-overlap lag warning exposes erosion before the full margin is
spent.
"""

TRADE_INTEGRITY_SETTLEMENT_GRACE: Final = timedelta(minutes=5)
"""Recent interval withheld from the scan horizon for ordinary commit lag."""

TRADE_INTEGRITY_LAG_THRESHOLD: Final = TRADE_INTEGRITY_OVERLAP / 2
"""Completed-coverage lag that produces a monitor health warning."""

TRADE_INTEGRITY_SWEEP_LIMIT: Final = 25_000
"""Hard maximum recent-window rows returned by one monitor pass."""

TRADE_INTEGRITY_WORKLOG_LIMIT: Final = 2_000
"""Hard maximum durable work obligations returned by one monitor pass."""

_FINDING_LIMIT: Final = 20
_CANDIDATE_CHUNK_SIZE: Final = 1_000


@dataclass(frozen=True, slots=True)
class TradeIntegrityPassRequest:
    """Inputs controlling one bounded repository pass.

    Attributes:
        monitor: Monitor key.
        now: Reference time shared by cursor and lag calculations.
        sweep_limit: Maximum recent trade rows read.
        worklog_limit: Maximum outstanding restore obligations read.
    """

    monitor: TradeIntegrityMonitor
    now: datetime
    sweep_limit: int
    worklog_limit: int


@dataclass(frozen=True, slots=True)
class _M1Candidate:
    """One exact venue identity and execution time to cross-check."""

    instrument_public_id: str
    trade_id: str
    executed_at: datetime | None


class _M1PostgresParams(TypedDict):
    """Bound arrays for one PostgreSQL M1 candidate chunk."""

    instrument_public_ids: list[str]
    trade_ids: list[str]
    executed_ats: list[datetime | None]
    finding_limit: int


class _M2PostgresParams(TypedDict):
    """Bound array for one PostgreSQL M2 candidate chunk."""

    public_ids: list[str]
    finding_limit: int


@dataclass(frozen=True, slots=True)
class _PassOutcome:
    """Counts and findings used to assemble one public pass result."""

    findings: tuple[TradeIntegrityFinding, ...]
    sweep_rows: int
    worklog_rows: int
    pass_completed: bool


_M1_POSTGRES_QUERY = text("""
    SELECT
        candidate.instrument_public_id::text AS instrument_public_id,
        candidate.trade_id,
        candidate.executed_at AS expected_executed_at,
        conflicting.executed_at AS conflicting_executed_at
    FROM unnest(
        CAST(:instrument_public_ids AS text[])::uuid[],
        CAST(:trade_ids AS text[]),
        CAST(:executed_ats AS timestamptz[])
    ) AS candidate(instrument_public_id, trade_id, executed_at)
    CROSS JOIN LATERAL (
        SELECT matched.executed_at
        FROM trades AS matched
        WHERE matched.instrument_public_id = candidate.instrument_public_id
          AND matched.trade_id = candidate.trade_id
          AND matched.executed_at IS DISTINCT FROM candidate.executed_at
        LIMIT 1
    ) AS conflicting
    LIMIT :finding_limit
    """)

_M2_POSTGRES_QUERY = text("""
    SELECT
        candidate.public_id::text AS public_id,
        active_rows.active_count
    FROM unnest(
        CAST(:public_ids AS text[])::uuid[]
    ) AS candidate(public_id)
    CROSS JOIN LATERAL (
        SELECT count(*)::integer AS active_count
        FROM (
            SELECT 1
            FROM trades AS matched
            WHERE matched.public_id = candidate.public_id
              AND matched.known_to =
                  TIMESTAMPTZ '9999-12-31 23:59:59+00'
            LIMIT 2
        ) AS bounded_active_rows
    ) AS active_rows
    WHERE active_rows.active_count > 1
    LIMIT :finding_limit
    """)


def _settled_horizon(now: datetime) -> datetime:
    """Return the newest bus time eligible for a sweep."""
    return now - TRADE_INTEGRITY_SETTLEMENT_GRACE


async def _load_cursor(
    session: AsyncSession,
    request: TradeIntegrityPassRequest,
) -> TradeIntegrityMonitorCursor:
    """Lock or initialize one monitor's durable paginated cursor."""
    statement = (
        select(TradeIntegrityMonitorCursor)
        .where(TradeIntegrityMonitorCursor.monitor == request.monitor)
        .with_for_update()
    )
    state = (await session.execute(statement)).scalar_one_or_none()
    if state is not None:
        return state
    settled_horizon = _settled_horizon(request.now)
    scan_start = settled_horizon - TRADE_INTEGRITY_OVERLAP
    state = TradeIntegrityMonitorCursor(
        monitor=request.monitor,
        covered_through=scan_start,
        scan_cursor_timestamp=scan_start,
        scan_cursor_id=0,
        scan_end=settled_horizon,
        updated_at=request.now,
    )
    session.add(state)
    await session.flush()
    return state


async def _load_worklog(
    session: AsyncSession,
    dialect_name: str,
    request: TradeIntegrityPassRequest,
) -> list[TradeIntegrityWorkItem]:
    """Lock a bounded batch of outstanding obligations for one monitor."""
    pending = (
        TradeIntegrityWorkItem.m1_pending
        if request.monitor == "m1"
        else TradeIntegrityWorkItem.m2_pending
    )
    pending_filter = (
        pending == literal_column("1") if dialect_name == "sqlite" else pending.is_(True)
    )
    statement = (
        select(TradeIntegrityWorkItem)
        .where(pending_filter)
        .order_by(TradeIntegrityWorkItem.id)
        .limit(request.worklog_limit)
        .with_for_update(skip_locked=True)
    )
    return list((await session.scalars(statement)).all())


async def _load_sweep(
    session: AsyncSession,
    state: TradeIntegrityMonitorCursor,
    limit: int,
) -> list[Trade]:
    """Read the next bounded page from the current fixed scan pass."""
    after_cursor = or_(
        Trade.timestamp > state.scan_cursor_timestamp,
        and_(
            Trade.timestamp == state.scan_cursor_timestamp,
            Trade.id > state.scan_cursor_id,
        ),
    )
    statement = (
        select(Trade)
        .where(
            Trade.timestamp >= state.scan_cursor_timestamp,
            after_cursor,
            Trade.timestamp <= state.scan_end,
        )
        .order_by(Trade.timestamp, Trade.id)
        .limit(limit)
    )
    return list((await session.scalars(statement)).all())


def _m1_candidates(
    sweep: list[Trade],
    worklog: list[TradeIntegrityWorkItem],
) -> list[_M1Candidate]:
    """Deduplicate non-null venue identities from both monitor inputs."""
    candidates: dict[tuple[str, str, datetime | None], _M1Candidate] = {}
    for trade in sweep:
        if trade.trade_id is None:
            continue
        candidate = _M1Candidate(
            instrument_public_id=trade.instrument_public_id,
            trade_id=trade.trade_id,
            executed_at=trade.executed_at,
        )
        candidates[(candidate.instrument_public_id, candidate.trade_id, candidate.executed_at)] = (
            candidate
        )
    for item in worklog:
        if item.trade_id is None:
            continue
        candidate = _M1Candidate(
            instrument_public_id=item.instrument_public_id,
            trade_id=item.trade_id,
            executed_at=item.executed_at,
        )
        candidates[(candidate.instrument_public_id, candidate.trade_id, candidate.executed_at)] = (
            candidate
        )
    return list(candidates.values())


def _m2_candidates(
    sweep: list[Trade],
    worklog: list[TradeIntegrityWorkItem],
) -> list[str]:
    """Deduplicate public identities from both monitor inputs."""
    return list({trade.public_id for trade in sweep} | {item.public_id for item in worklog})


def _chunks[T](values: list[T]) -> list[list[T]]:
    """Split candidates into planner-stable bounded chunks."""
    return [
        values[start : start + _CANDIDATE_CHUNK_SIZE]
        for start in range(0, len(values), _CANDIDATE_CHUNK_SIZE)
    ]


async def _find_m1_postgresql(
    session: AsyncSession,
    candidates: list[_M1Candidate],
) -> tuple[TradeIntegrityFinding, ...]:
    """Probe candidate venue identities through the U2 or U3 index."""
    findings: dict[tuple[str, str], TradeIntegrityFinding] = {}
    for chunk in _chunks(candidates):
        remaining = _FINDING_LIMIT - len(findings)
        if remaining <= 0:
            break
        params = _M1PostgresParams(
            instrument_public_ids=[item.instrument_public_id for item in chunk],
            trade_ids=[item.trade_id for item in chunk],
            executed_ats=[item.executed_at for item in chunk],
            finding_limit=remaining,
        )
        rows = (await session.execute(_M1_POSTGRES_QUERY, params)).mappings().all()
        for row in rows:
            instrument_public_id = cast(str, row["instrument_public_id"])
            trade_id = cast(str, row["trade_id"])
            findings[(instrument_public_id, trade_id)] = TradeIntegrityFinding(
                monitor="m1",
                public_id=None,
                instrument_public_id=instrument_public_id,
                trade_id=trade_id,
                expected_executed_at=cast(datetime | None, row["expected_executed_at"]),
                conflicting_executed_at=cast(datetime | None, row["conflicting_executed_at"]),
                active_count=None,
            )
    return tuple(findings.values())


async def _find_m1_sqlite(
    session: AsyncSession,
    candidates: list[_M1Candidate],
) -> tuple[TradeIntegrityFinding, ...]:
    """Run the M1 mutation probe with SQLite-native tuple predicates."""
    candidate_trade = aliased(Trade)
    conflicting_trade = aliased(Trade)
    findings: dict[tuple[str, str], TradeIntegrityFinding] = {}
    for chunk in _chunks(candidates):
        remaining = _FINDING_LIMIT - len(findings)
        if remaining <= 0:
            break
        non_null = [
            (item.instrument_public_id, item.trade_id, item.executed_at)
            for item in chunk
            if item.executed_at is not None
        ]
        null_times = [
            (item.instrument_public_id, item.trade_id) for item in chunk if item.executed_at is None
        ]
        candidate_filters: list[ColumnElement[bool]] = []
        if non_null:
            candidate_filters.append(
                tuple_(
                    candidate_trade.instrument_public_id,
                    candidate_trade.trade_id,
                    candidate_trade.executed_at,
                ).in_(non_null)
            )
        if null_times:
            candidate_filters.append(
                and_(
                    tuple_(
                        candidate_trade.instrument_public_id,
                        candidate_trade.trade_id,
                    ).in_(null_times),
                    candidate_trade.executed_at.is_(None),
                )
            )
        statement = (
            select(
                candidate_trade.instrument_public_id,
                candidate_trade.trade_id,
                candidate_trade.executed_at,
                conflicting_trade.executed_at,
            )
            .join(
                conflicting_trade,
                and_(
                    conflicting_trade.instrument_public_id == candidate_trade.instrument_public_id,
                    conflicting_trade.trade_id == candidate_trade.trade_id,
                    conflicting_trade.executed_at.is_distinct_from(candidate_trade.executed_at),
                ),
            )
            .where(or_(*candidate_filters))
            .limit(remaining)
        )
        rows = (await session.execute(statement)).all()
        for row in rows:
            instrument_public_id = row[0]
            trade_id = cast(str, row[1])
            findings[(instrument_public_id, trade_id)] = TradeIntegrityFinding(
                monitor="m1",
                public_id=None,
                instrument_public_id=instrument_public_id,
                trade_id=trade_id,
                expected_executed_at=cast(datetime | None, row[2]),
                conflicting_executed_at=cast(datetime | None, row[3]),
                active_count=None,
            )
    return tuple(findings.values())


async def _find_m2_postgresql(
    session: AsyncSession,
    public_ids: list[str],
) -> tuple[TradeIntegrityFinding, ...]:
    """Probe active public identities through the literal partial index."""
    findings: dict[str, TradeIntegrityFinding] = {}
    for chunk in _chunks(public_ids):
        remaining = _FINDING_LIMIT - len(findings)
        if remaining <= 0:
            break
        params = _M2PostgresParams(public_ids=chunk, finding_limit=remaining)
        rows = (await session.execute(_M2_POSTGRES_QUERY, params)).mappings().all()
        for row in rows:
            public_id = cast(str, row["public_id"])
            findings[public_id] = TradeIntegrityFinding(
                monitor="m2",
                public_id=public_id,
                instrument_public_id=None,
                trade_id=None,
                expected_executed_at=None,
                conflicting_executed_at=None,
                active_count=cast(int, row["active_count"]),
            )
    return tuple(findings.values())


async def _find_m2_sqlite(
    session: AsyncSession,
    public_ids: list[str],
) -> tuple[TradeIntegrityFinding, ...]:
    """Run the M2 mutation probe with the SQLite partial-index literal."""
    first = aliased(Trade)
    second = aliased(Trade)
    active: ColumnElement[str] = literal_column("'9999-12-31 23:59:59.000000'")
    findings: dict[str, TradeIntegrityFinding] = {}
    for chunk in _chunks(public_ids):
        remaining = _FINDING_LIMIT - len(findings)
        if remaining <= 0:
            break
        statement = (
            select(first.public_id)
            .join(
                second,
                and_(
                    second.public_id == first.public_id,
                    second.id > first.id,
                    second.known_to == active,
                ),
            )
            .where(
                first.public_id.in_(chunk),
                first.known_to == active,
            )
            .limit(remaining)
        )
        for public_id in (await session.scalars(statement)).all():
            findings[public_id] = TradeIntegrityFinding(
                monitor="m2",
                public_id=public_id,
                instrument_public_id=None,
                trade_id=None,
                expected_executed_at=None,
                conflicting_executed_at=None,
                active_count=2,
            )
    return tuple(findings.values())


async def _find_violations(
    session: AsyncSession,
    dialect_name: str,
    request: TradeIntegrityPassRequest,
    sweep: list[Trade],
    worklog: list[TradeIntegrityWorkItem],
) -> tuple[TradeIntegrityFinding, ...]:
    """Dispatch one monitor to its dialect-specific bounded query."""
    if request.monitor == "m1":
        candidates = _m1_candidates(sweep, worklog)
        if not candidates:
            return ()
        if dialect_name == "postgresql":
            return await _find_m1_postgresql(session, candidates)
        return await _find_m1_sqlite(session, candidates)
    public_ids = _m2_candidates(sweep, worklog)
    if not public_ids:
        return ()
    if dialect_name == "postgresql":
        return await _find_m2_postgresql(session, public_ids)
    return await _find_m2_sqlite(session, public_ids)


async def _ack_worklog(
    session: AsyncSession,
    monitor: TradeIntegrityMonitor,
    worklog: list[TradeIntegrityWorkItem],
) -> None:
    """Clear this monitor's exact obligations and remove fully drained rows."""
    if not worklog:
        return
    item_ids = [item.id for item in worklog]
    pending_column = "m1_pending" if monitor == "m1" else "m2_pending"
    await session.execute(
        update(TradeIntegrityWorkItem)
        .where(TradeIntegrityWorkItem.id.in_(item_ids))
        .values(**{pending_column: False})
    )
    await session.execute(
        delete(TradeIntegrityWorkItem).where(
            TradeIntegrityWorkItem.id.in_(item_ids),
            TradeIntegrityWorkItem.m1_pending.is_(False),
            TradeIntegrityWorkItem.m2_pending.is_(False),
        )
    )


def _advance_cursor(
    state: TradeIntegrityMonitorCursor,
    sweep: list[Trade],
    request: TradeIntegrityPassRequest,
) -> bool:
    """Advance one page or start the next complete overlap pass."""
    if len(sweep) >= request.sweep_limit:
        last = sweep[-1]
        state.scan_cursor_timestamp = last.timestamp
        state.scan_cursor_id = last.id
        state.updated_at = request.now
        return False
    state.covered_through = state.scan_end
    next_end = max(state.scan_end, _settled_horizon(request.now))
    state.scan_cursor_timestamp = state.scan_end - TRADE_INTEGRITY_OVERLAP
    state.scan_cursor_id = 0
    state.scan_end = next_end
    state.updated_at = request.now
    return True


def _result(
    state: TradeIntegrityMonitorCursor,
    request: TradeIntegrityPassRequest,
    outcome: _PassOutcome,
) -> TradeIntegrityRunResult:
    """Build the immutable result and completed-coverage lag signal."""
    lag = max(timedelta(0), _settled_horizon(request.now) - state.covered_through)
    return TradeIntegrityRunResult(
        monitor=request.monitor,
        findings=outcome.findings,
        sweep_rows=outcome.sweep_rows,
        worklog_rows=outcome.worklog_rows,
        cursor_timestamp=state.scan_cursor_timestamp,
        cursor_id=state.scan_cursor_id,
        covered_through=state.covered_through,
        lag_seconds=int(lag.total_seconds()),
        lagged=lag > TRADE_INTEGRITY_LAG_THRESHOLD,
        pass_completed=outcome.pass_completed,
    )


async def run_trade_integrity_monitor(
    session: AsyncSession,
    dialect_name: str,
    request: TradeIntegrityPassRequest,
) -> TradeIntegrityRunResult:
    """Run and durably checkpoint one bounded M1 or M2 pass.

    A finding commits only first-use cursor initialization. It never
    advances the sweep page or acknowledges worklog rows, so the same
    evidence remains level-triggered after process restart.
    """
    if not 0 < request.sweep_limit <= TRADE_INTEGRITY_SWEEP_LIMIT:
        raise ValueError(
            f"trade integrity sweep limit must be within 1..{TRADE_INTEGRITY_SWEEP_LIMIT}"
        )
    if not 0 < request.worklog_limit <= TRADE_INTEGRITY_WORKLOG_LIMIT:
        raise ValueError(
            f"trade integrity worklog limit must be within 1..{TRADE_INTEGRITY_WORKLOG_LIMIT}"
        )
    if dialect_name not in {"postgresql", "sqlite"}:
        raise ValueError(f"unsupported trade integrity dialect: {dialect_name}")
    state = await _load_cursor(session, request)
    worklog = await _load_worklog(session, dialect_name, request)
    sweep = await _load_sweep(session, state, request.sweep_limit)
    findings = await _find_violations(
        session,
        dialect_name,
        request,
        sweep,
        worklog,
    )
    if findings:
        await session.commit()
        return _result(
            state,
            request,
            _PassOutcome(
                findings=findings,
                sweep_rows=len(sweep),
                worklog_rows=len(worklog),
                pass_completed=False,
            ),
        )
    await _ack_worklog(session, request.monitor, worklog)
    pass_completed = _advance_cursor(state, sweep, request)
    await session.commit()
    return _result(
        state,
        request,
        _PassOutcome(
            findings=(),
            sweep_rows=len(sweep),
            worklog_rows=len(worklog),
            pass_completed=pass_completed,
        ),
    )
