"""Bounded database primitives for the two trade-integrity monitors.

M1 looks for one venue identity (instrument and trade id) stored with two
execution times; M2 looks for more than one active row per public id. Sweep
candidates are probed only within :data:`TRADE_INTEGRITY_PROBE_WINDOW` of
their own ``executed_at``, so that each probe reads only the daily
``trades`` partitions around the candidate. Durable worklog obligations
(restore, import and replay) and candidates without an execution time are
still probed exactly, over every partition.
"""

from collections.abc import Sequence
from dataclasses import dataclass
from dataclasses import field
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
from sqlalchemy.engine import RowMapping
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

TRADE_INTEGRITY_PROBE_WINDOW: Final = timedelta(days=7)
"""Inclusive distance around a sweep candidate's ``executed_at`` that its probe reads.

For a candidate routed to a windowed probe, a stored row can contribute to a
finding only if its ``executed_at`` differs from the candidate's by at most
this interval. The bound lets PostgreSQL prune the daily ``trades``
partitions per candidate, so a probe's cost no longer grows with the number
of partitions. A conflict further apart is not detected for sweep
candidates; worklog obligations keep the exact check over every partition.
PostgreSQL sessions run with the time zone UTC, so the interval arithmetic of
the windowed statements adds exactly this duration.
"""

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


@dataclass(frozen=True, slots=True)
class _M1WindowCandidate:
    """One sweep venue identity probed only around its execution time."""

    instrument_public_id: str
    trade_id: str
    executed_at: datetime


@dataclass(frozen=True, slots=True)
class _M2WindowCandidate:
    """One sweep public identity probed only around its execution time."""

    public_id: str
    executed_at: datetime


@dataclass(frozen=True, slots=True)
class _M1Route:
    """M1 candidates of one page, split between windowed and exact probes."""

    windowed: list[_M1WindowCandidate]
    exact: list[_M1Candidate]


@dataclass(frozen=True, slots=True)
class _M2Route:
    """M2 candidates of one page, split between windowed and exact probes."""

    windowed: list[_M2WindowCandidate]
    exact: list[str]


@dataclass(slots=True)
class _FindingCollector:
    """Deduplicated findings of one monitor page under one shared cap."""

    by_key: dict[tuple[str, ...], TradeIntegrityFinding] = field(default_factory=dict)

    def remaining(self) -> int:
        """Return how many more findings this page may record."""
        return _FINDING_LIMIT - len(self.by_key)

    def add(self, key: tuple[str, ...], finding: TradeIntegrityFinding) -> None:
        """Record one finding under its identity key, replacing an earlier one."""
        self.by_key[key] = finding

    def findings(self) -> tuple[TradeIntegrityFinding, ...]:
        """Return the recorded findings in first-recorded order."""
        return tuple(self.by_key.values())


class _M1PostgresParams(TypedDict):
    """Bound arrays for one PostgreSQL M1 candidate chunk."""

    instrument_public_ids: list[str]
    trade_ids: list[str]
    executed_ats: list[datetime | None]
    finding_limit: int


class _M1WindowedPostgresParams(TypedDict):
    """Bound arrays and window for one windowed PostgreSQL M1 chunk."""

    instrument_public_ids: list[str]
    trade_ids: list[str]
    executed_ats: list[datetime]
    probe_window: timedelta
    finding_limit: int


class _M2PostgresParams(TypedDict):
    """Bound array for one PostgreSQL M2 candidate chunk."""

    public_ids: list[str]
    finding_limit: int


class _M2WindowedPostgresParams(TypedDict):
    """Bound arrays and window for one windowed PostgreSQL M2 chunk."""

    public_ids: list[str]
    executed_ats: list[datetime]
    probe_window: timedelta
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

_M1_WINDOWED_POSTGRES_QUERY = text("""
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
          AND matched.executed_at >= candidate.executed_at - CAST(:probe_window AS interval)
          AND matched.executed_at <= candidate.executed_at + CAST(:probe_window AS interval)
          AND matched.executed_at IS DISTINCT FROM candidate.executed_at
        LIMIT 1
    ) AS conflicting
    LIMIT :finding_limit
    """)

_M2_WINDOWED_POSTGRES_QUERY = text("""
    SELECT
        candidate.public_id::text AS public_id,
        active_rows.active_count
    FROM unnest(
        CAST(:public_ids AS text[])::uuid[],
        CAST(:executed_ats AS timestamptz[])
    ) AS candidate(public_id, executed_at)
    CROSS JOIN LATERAL (
        SELECT count(*)::integer AS active_count
        FROM (
            SELECT 1
            FROM trades AS matched
            WHERE matched.public_id = candidate.public_id
              AND matched.known_to =
                  TIMESTAMPTZ '9999-12-31 23:59:59+00'
              AND matched.executed_at >= candidate.executed_at - CAST(:probe_window AS interval)
              AND matched.executed_at <= candidate.executed_at + CAST(:probe_window AS interval)
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


def _route_m1(
    sweep: list[Trade],
    worklog: list[TradeIntegrityWorkItem],
) -> _M1Route:
    """Split venue identities between exact worklog and windowed sweep probes.

    Every worklog obligation with a trade id is an exact candidate, including
    one without an execution time. Sweep rows always carry an execution time,
    because ``trades.executed_at`` is the non-nullable partition key, so they
    are windowed candidates, except that an identity which also occurs among
    the exact candidates is probed only exactly, with the exact candidates'
    values. Rows without a trade id stay out of M1.
    """
    exact: dict[tuple[str, str, datetime | None], _M1Candidate] = {}
    for item in worklog:
        if item.trade_id is not None:
            exact[(item.instrument_public_id, item.trade_id, item.executed_at)] = _M1Candidate(
                instrument_public_id=item.instrument_public_id,
                trade_id=item.trade_id,
                executed_at=item.executed_at,
            )
    exact_identities = {(item.instrument_public_id, item.trade_id) for item in exact.values()}
    windowed: dict[tuple[str, str, datetime], _M1WindowCandidate] = {}
    for trade in sweep:
        if trade.trade_id is None:
            continue
        if (trade.instrument_public_id, trade.trade_id) in exact_identities:
            continue
        windowed[(trade.instrument_public_id, trade.trade_id, trade.executed_at)] = (
            _M1WindowCandidate(
                instrument_public_id=trade.instrument_public_id,
                trade_id=trade.trade_id,
                executed_at=trade.executed_at,
            )
        )
    return _M1Route(windowed=list(windowed.values()), exact=list(exact.values()))


def _route_m2(
    sweep: list[Trade],
    worklog: list[TradeIntegrityWorkItem],
) -> _M2Route:
    """Split public identities between exact worklog and windowed sweep probes.

    Every worklog obligation makes its public id exact, and sweep rows of that
    public id are then not probed separately. Each other sweep row is a
    windowed candidate centred on its own execution time, so one public id
    can have several.
    """
    exact = list(dict.fromkeys(item.public_id for item in worklog))
    exact_ids = set(exact)
    windowed: dict[tuple[str, datetime], _M2WindowCandidate] = {}
    for trade in sweep:
        if trade.public_id in exact_ids:
            continue
        windowed[(trade.public_id, trade.executed_at)] = _M2WindowCandidate(
            public_id=trade.public_id,
            executed_at=trade.executed_at,
        )
    return _M2Route(windowed=list(windowed.values()), exact=exact)


def _chunks[T](values: list[T]) -> list[list[T]]:
    """Split candidates into planner-stable bounded chunks."""
    return [
        values[start : start + _CANDIDATE_CHUNK_SIZE]
        for start in range(0, len(values), _CANDIDATE_CHUNK_SIZE)
    ]


def _record_m1_rows(rows: Sequence[RowMapping], collector: _FindingCollector) -> None:
    """Map PostgreSQL M1 result rows into findings keyed by venue identity."""
    for row in rows:
        instrument_public_id = cast(str, row["instrument_public_id"])
        trade_id = cast(str, row["trade_id"])
        collector.add(
            (instrument_public_id, trade_id),
            TradeIntegrityFinding(
                monitor="m1",
                public_id=None,
                instrument_public_id=instrument_public_id,
                trade_id=trade_id,
                expected_executed_at=cast(datetime | None, row["expected_executed_at"]),
                conflicting_executed_at=cast(datetime | None, row["conflicting_executed_at"]),
                active_count=None,
            ),
        )


def _record_m2_rows(rows: Sequence[RowMapping], collector: _FindingCollector) -> None:
    """Map PostgreSQL M2 result rows into findings keyed by public identity."""
    for row in rows:
        public_id = cast(str, row["public_id"])
        collector.add((public_id,), _m2_finding(public_id, cast(int, row["active_count"])))


def _m2_finding(public_id: str, active_count: int) -> TradeIntegrityFinding:
    """Build one M2 finding for a public identity and its bounded active count."""
    return TradeIntegrityFinding(
        monitor="m2",
        public_id=public_id,
        instrument_public_id=None,
        trade_id=None,
        expected_executed_at=None,
        conflicting_executed_at=None,
        active_count=active_count,
    )


async def _find_m1_postgresql(
    session: AsyncSession,
    candidates: list[_M1Candidate],
    collector: _FindingCollector | None = None,
) -> tuple[TradeIntegrityFinding, ...]:
    """Probe exact venue identities over every partition through the U2 or U3 index."""
    collector = collector if collector is not None else _FindingCollector()
    for chunk in _chunks(candidates):
        remaining = collector.remaining()
        if remaining <= 0:
            break
        params = _M1PostgresParams(
            instrument_public_ids=[item.instrument_public_id for item in chunk],
            trade_ids=[item.trade_id for item in chunk],
            executed_ats=[item.executed_at for item in chunk],
            finding_limit=remaining,
        )
        rows = (await session.execute(_M1_POSTGRES_QUERY, params)).mappings().all()
        _record_m1_rows(rows, collector)
    return collector.findings()


async def _find_m1_windowed_postgresql(
    session: AsyncSession,
    candidates: list[_M1WindowCandidate],
    collector: _FindingCollector,
) -> tuple[TradeIntegrityFinding, ...]:
    """Probe sweep venue identities only within the window around each execution time."""
    for chunk in _chunks(candidates):
        remaining = collector.remaining()
        if remaining <= 0:
            break
        params = _M1WindowedPostgresParams(
            instrument_public_ids=[item.instrument_public_id for item in chunk],
            trade_ids=[item.trade_id for item in chunk],
            executed_ats=[item.executed_at for item in chunk],
            probe_window=TRADE_INTEGRITY_PROBE_WINDOW,
            finding_limit=remaining,
        )
        rows = (await session.execute(_M1_WINDOWED_POSTGRES_QUERY, params)).mappings().all()
        _record_m1_rows(rows, collector)
    return collector.findings()


async def _sqlite_m1_times(
    session: AsyncSession,
    identities: list[tuple[str, str]],
) -> dict[tuple[str, str], list[datetime | None]]:
    """Read every stored execution time of the given venue identities."""
    statement = (
        select(
            Trade.instrument_public_id.label("instrument_public_id"),
            Trade.trade_id.label("trade_id"),
            Trade.executed_at.label("executed_at"),
        )
        .where(
            tuple_(Trade.instrument_public_id, Trade.trade_id).in_(list(dict.fromkeys(identities)))
        )
        .order_by(Trade.id)
    )
    times: dict[tuple[str, str], list[datetime | None]] = {}
    for row in (await session.execute(statement)).mappings().all():
        key = (cast(str, row["instrument_public_id"]), cast(str, row["trade_id"]))
        times.setdefault(key, []).append(cast(datetime | None, row["executed_at"]))
    return times


def _m1_sqlite_finding(
    instrument_public_id: str,
    trade_id: str,
    expected: datetime | None,
    conflicting: datetime | None,
) -> TradeIntegrityFinding:
    """Build one M1 finding from supplied candidate values and a stored time."""
    return TradeIntegrityFinding(
        monitor="m1",
        public_id=None,
        instrument_public_id=instrument_public_id,
        trade_id=trade_id,
        expected_executed_at=expected,
        conflicting_executed_at=conflicting,
        active_count=None,
    )


async def _find_m1_sqlite(
    session: AsyncSession,
    candidates: list[_M1Candidate],
    collector: _FindingCollector | None = None,
) -> tuple[TradeIntegrityFinding, ...]:
    """Evaluate exact M1 candidates from their supplied values, as PostgreSQL does.

    A stored time conflicts when it is distinct from the candidate's time in
    the sense of ``IS DISTINCT FROM``, so a candidate without a time conflicts
    with any stored row of its identity that has one.
    """
    collector = collector if collector is not None else _FindingCollector()
    for chunk in _chunks(candidates):
        if collector.remaining() <= 0:
            break
        times = await _sqlite_m1_times(
            session, [(item.instrument_public_id, item.trade_id) for item in chunk]
        )
        for item in chunk:
            distinct = [
                stored
                for stored in times.get((item.instrument_public_id, item.trade_id), [])
                if stored != item.executed_at
            ]
            if distinct and collector.remaining() > 0:
                collector.add(
                    (item.instrument_public_id, item.trade_id),
                    _m1_sqlite_finding(
                        item.instrument_public_id, item.trade_id, item.executed_at, distinct[0]
                    ),
                )
    return collector.findings()


async def _find_m1_windowed_sqlite(
    session: AsyncSession,
    candidates: list[_M1WindowCandidate],
    collector: _FindingCollector,
) -> tuple[TradeIntegrityFinding, ...]:
    """Evaluate sweep M1 candidates only within the window around each execution time."""
    for chunk in _chunks(candidates):
        if collector.remaining() <= 0:
            break
        times = await _sqlite_m1_times(
            session, [(item.instrument_public_id, item.trade_id) for item in chunk]
        )
        for item in chunk:
            lower = item.executed_at - TRADE_INTEGRITY_PROBE_WINDOW
            upper = item.executed_at + TRADE_INTEGRITY_PROBE_WINDOW
            distinct = [
                stored
                for stored in times.get((item.instrument_public_id, item.trade_id), [])
                if stored is not None and lower <= stored <= upper and stored != item.executed_at
            ]
            if distinct and collector.remaining() > 0:
                collector.add(
                    (item.instrument_public_id, item.trade_id),
                    _m1_sqlite_finding(
                        item.instrument_public_id, item.trade_id, item.executed_at, distinct[0]
                    ),
                )
    return collector.findings()


async def _find_m2_postgresql(
    session: AsyncSession,
    public_ids: list[str],
    collector: _FindingCollector | None = None,
) -> tuple[TradeIntegrityFinding, ...]:
    """Probe exact public identities over every partition through the literal partial index."""
    collector = collector if collector is not None else _FindingCollector()
    for chunk in _chunks(public_ids):
        remaining = collector.remaining()
        if remaining <= 0:
            break
        params = _M2PostgresParams(public_ids=chunk, finding_limit=remaining)
        rows = (await session.execute(_M2_POSTGRES_QUERY, params)).mappings().all()
        _record_m2_rows(rows, collector)
    return collector.findings()


async def _find_m2_windowed_postgresql(
    session: AsyncSession,
    candidates: list[_M2WindowCandidate],
    collector: _FindingCollector,
) -> tuple[TradeIntegrityFinding, ...]:
    """Probe sweep public identities only within the window around each execution time."""
    for chunk in _chunks(candidates):
        remaining = collector.remaining()
        if remaining <= 0:
            break
        params = _M2WindowedPostgresParams(
            public_ids=[item.public_id for item in chunk],
            executed_ats=[item.executed_at for item in chunk],
            probe_window=TRADE_INTEGRITY_PROBE_WINDOW,
            finding_limit=remaining,
        )
        rows = (await session.execute(_M2_WINDOWED_POSTGRES_QUERY, params)).mappings().all()
        _record_m2_rows(rows, collector)
    return collector.findings()


async def _find_m2_sqlite(
    session: AsyncSession,
    public_ids: list[str],
    collector: _FindingCollector | None = None,
) -> tuple[TradeIntegrityFinding, ...]:
    """Run the exact M2 mutation probe with the SQLite partial-index literal."""
    collector = collector if collector is not None else _FindingCollector()
    first = aliased(Trade)
    second = aliased(Trade)
    active: ColumnElement[str] = literal_column("'9999-12-31 23:59:59.000000'")
    for chunk in _chunks(public_ids):
        remaining = collector.remaining()
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
            collector.add((public_id,), _m2_finding(public_id, 2))
    return collector.findings()


async def _find_m2_windowed_sqlite(
    session: AsyncSession,
    candidates: list[_M2WindowCandidate],
    collector: _FindingCollector,
) -> tuple[TradeIntegrityFinding, ...]:
    """Count active rows of each public id only within the window around its candidate.

    Both counted rows are compared with the candidate's time, not with each
    other, so two rows can be up to twice the window apart. The count is
    bounded at two, as in the PostgreSQL probe.
    """
    active: ColumnElement[str] = literal_column("'9999-12-31 23:59:59.000000'")
    for chunk in _chunks(candidates):
        if collector.remaining() <= 0:
            break
        statement = select(
            Trade.public_id.label("public_id"), Trade.executed_at.label("executed_at")
        ).where(
            Trade.public_id.in_(sorted({item.public_id for item in chunk})),
            Trade.known_to == active,
        )
        times: dict[str, list[datetime | None]] = {}
        for row in (await session.execute(statement)).mappings().all():
            times.setdefault(cast(str, row["public_id"]), []).append(
                cast(datetime | None, row["executed_at"])
            )
        for item in chunk:
            lower = item.executed_at - TRADE_INTEGRITY_PROBE_WINDOW
            upper = item.executed_at + TRADE_INTEGRITY_PROBE_WINDOW
            count = sum(
                1
                for stored in times.get(item.public_id, [])
                if stored is not None and lower <= stored <= upper
            )
            if count > 1 and collector.remaining() > 0:
                collector.add((item.public_id,), _m2_finding(item.public_id, min(count, 2)))
    return collector.findings()


async def _find_m1_violations(
    session: AsyncSession,
    dialect_name: str,
    route: _M1Route,
) -> tuple[TradeIntegrityFinding, ...]:
    """Run the exact and then the windowed M1 probes under one finding cap."""
    collector = _FindingCollector()
    if dialect_name == "postgresql":
        if route.exact:
            await _find_m1_postgresql(session, route.exact, collector)
        if route.windowed:
            await _find_m1_windowed_postgresql(session, route.windowed, collector)
        return collector.findings()
    if route.exact:
        await _find_m1_sqlite(session, route.exact, collector)
    if route.windowed:
        await _find_m1_windowed_sqlite(session, route.windowed, collector)
    return collector.findings()


async def _find_m2_violations(
    session: AsyncSession,
    dialect_name: str,
    route: _M2Route,
) -> tuple[TradeIntegrityFinding, ...]:
    """Run the exact and then the windowed M2 probes under one finding cap."""
    collector = _FindingCollector()
    if dialect_name == "postgresql":
        if route.exact:
            await _find_m2_postgresql(session, route.exact, collector)
        if route.windowed:
            await _find_m2_windowed_postgresql(session, route.windowed, collector)
        return collector.findings()
    if route.exact:
        await _find_m2_sqlite(session, route.exact, collector)
    if route.windowed:
        await _find_m2_windowed_sqlite(session, route.windowed, collector)
    return collector.findings()


async def _find_violations(
    session: AsyncSession,
    dialect_name: str,
    request: TradeIntegrityPassRequest,
    sweep: list[Trade],
    worklog: list[TradeIntegrityWorkItem],
) -> tuple[TradeIntegrityFinding, ...]:
    """Route one monitor's page to its windowed and exact dialect probes."""
    if request.monitor == "m1":
        return await _find_m1_violations(session, dialect_name, _route_m1(sweep, worklog))
    return await _find_m2_violations(session, dialect_name, _route_m2(sweep, worklog))


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
