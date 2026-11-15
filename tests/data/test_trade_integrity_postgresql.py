"""Dialect routing tests for incremental trade-integrity probes."""

from collections.abc import Callable
from datetime import UTC
from datetime import datetime
from datetime import timedelta
from typing import Literal
from typing import cast
from unittest.mock import AsyncMock
from unittest.mock import MagicMock

import pytest
from sqlalchemy.ext.asyncio import AsyncEngine
from sqlalchemy.ext.asyncio import AsyncSession

from snapper.data import repository as repository_module
from snapper.data import trade_integrity as integrity_module
from snapper.data.models import Trade
from snapper.data.models import TradeIntegrityWorkItem
from snapper.data.repository import SQLAlchemyRepository
from snapper.data.repository_types import TradeIntegrityFinding
from snapper.data.repository_types import TradeIntegrityRunResult
from snapper.data.trade_integrity import _M1_WINDOWED_POSTGRES_QUERY
from snapper.data.trade_integrity import _M2_WINDOWED_POSTGRES_QUERY
from snapper.data.trade_integrity import TRADE_INTEGRITY_PROBE_WINDOW
from snapper.data.trade_integrity import TradeIntegrityPassRequest
from snapper.data.trade_integrity import _find_m1_postgresql
from snapper.data.trade_integrity import _find_m1_sqlite
from snapper.data.trade_integrity import _find_m1_windowed_postgresql
from snapper.data.trade_integrity import _find_m1_windowed_sqlite
from snapper.data.trade_integrity import _find_m2_postgresql
from snapper.data.trade_integrity import _find_m2_windowed_postgresql
from snapper.data.trade_integrity import _find_m2_windowed_sqlite
from snapper.data.trade_integrity import _find_violations
from snapper.data.trade_integrity import _FindingCollector
from snapper.data.trade_integrity import _M1Candidate
from snapper.data.trade_integrity import _M1Route
from snapper.data.trade_integrity import _M1WindowCandidate
from snapper.data.trade_integrity import _M2Route
from snapper.data.trade_integrity import _M2WindowCandidate
from snapper.data.trade_integrity import _route_m1
from snapper.data.trade_integrity import _route_m2
from snapper.data.trade_integrity import run_trade_integrity_monitor

_NOW = datetime(2026, 7, 29, 12, 0, tzinfo=UTC)

type _Probe = Callable[..., object]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("dialect", "timeout_seconds", "expected_timeout"),
    [
        ("postgresql", 43.0, "43000ms"),
        ("postgresql", 1.234, "1234ms"),
        ("postgresql", 0.0, "1000ms"),
        ("sqlite", 43.0, None),
    ],
)
async def test_repository_bounds_postgresql_statements_before_monitor_pass(
    monkeypatch: pytest.MonkeyPatch,
    dialect: str,
    timeout_seconds: float,
    expected_timeout: str | None,
) -> None:
    """Apply transaction-local statement bounds only on PostgreSQL.

    Given: A monitor repository with a selected database dialect and time budget.
    When: The repository starts a monitor pass.
    Then: PostgreSQL receives its millisecond timeout before any monitor work,
        while SQLite runs the pass without a PostgreSQL setting.
    """
    session = AsyncMock(spec=AsyncSession)
    engine = MagicMock()
    engine.url.get_dialect.return_value.name = dialect
    repository = SQLAlchemyRepository.__new__(SQLAlchemyRepository)
    repository.engine = cast(AsyncEngine, engine)
    repository.session_factory = MagicMock(return_value=session)
    pass_result = MagicMock(spec=TradeIntegrityRunResult)
    monitor_pass = AsyncMock(return_value=pass_result)
    operations = MagicMock()
    operations.attach_mock(session.execute, "execute")
    operations.attach_mock(monitor_pass, "run_pass")
    monkeypatch.setattr(repository_module, "run_trade_integrity_monitor_pass", monitor_pass)
    request = TradeIntegrityPassRequest(
        monitor="m1",
        now=_NOW,
        sweep_limit=500,
        worklog_limit=100,
    )

    result = await repository.run_trade_integrity_monitor(
        monitor="m1",
        now=_NOW,
        sweep_limit=500,
        worklog_limit=100,
        statement_timeout_seconds=timeout_seconds,
    )

    assert result is pass_result
    monitor_pass.assert_awaited_once_with(session, dialect, request)
    if expected_timeout is None:
        session.execute.assert_not_awaited()
        assert [operation[0] for operation in operations.mock_calls] == ["run_pass"]
    else:
        session.execute.assert_awaited_once()
        assert str(session.execute.await_args.args[0]) == (
            f"SET LOCAL statement_timeout = '{expected_timeout}'"
        )
        assert [operation[0] for operation in operations.mock_calls] == ["execute", "run_pass"]


def _session_with_rows(rows: list[dict[str, object]]) -> AsyncSession:
    """Build an async session boundary returning mapped result rows.

    Args:
        rows: Mapping rows returned by the database boundary.

    Returns:
        Session mock with an asynchronous execute method.
    """
    result = MagicMock()
    result.mappings.return_value.all.return_value = rows
    session = MagicMock()
    session.execute = AsyncMock(return_value=result)
    return cast(AsyncSession, session)


def _m1_finding(
    trade_id: str,
    expected: datetime | None,
    conflicting: datetime | None,
) -> TradeIntegrityFinding:
    """Build the M1 finding expected for one venue identity of ``instrument-1``.

    Args:
        trade_id: Venue trade identity.
        expected: Candidate execution time.
        conflicting: Stored execution time that conflicts with it.

    Returns:
        The structured M1 finding.
    """
    return TradeIntegrityFinding(
        monitor="m1",
        public_id=None,
        instrument_public_id="instrument-1",
        trade_id=trade_id,
        expected_executed_at=expected,
        conflicting_executed_at=conflicting,
        active_count=None,
    )


def _m2_finding(public_id: str, active_count: int) -> TradeIntegrityFinding:
    """Build the M2 finding expected for one public identity.

    Args:
        public_id: Logical row identity.
        active_count: Bounded number of active rows.

    Returns:
        The structured M2 finding.
    """
    return TradeIntegrityFinding(
        monitor="m2",
        public_id=public_id,
        instrument_public_id=None,
        trade_id=None,
        expected_executed_at=None,
        conflicting_executed_at=None,
        active_count=active_count,
    )


def _trade(public_id: str, trade_id: str | None, executed_at: datetime) -> Trade:
    """Build one detached swept trade.

    Args:
        public_id: Logical row identity.
        trade_id: Venue trade identity, or ``None``.
        executed_at: Venue execution time.

    Returns:
        A trade that is never added to a session.
    """
    return Trade(
        public_id=public_id,
        instrument_public_id="instrument-1",
        trade_id=trade_id,
        executed_at=executed_at,
    )


def _work_item(
    public_id: str,
    trade_id: str | None,
    executed_at: datetime | None,
) -> TradeIntegrityWorkItem:
    """Build one detached worklog obligation.

    Args:
        public_id: Logical row identity.
        trade_id: Venue trade identity, or ``None``.
        executed_at: Obligation execution time, or ``None``.

    Returns:
        An obligation that is never added to a session.
    """
    return TradeIntegrityWorkItem(
        public_id=public_id,
        instrument_public_id="instrument-1",
        trade_id=trade_id,
        executed_at=executed_at,
    )


@pytest.mark.asyncio
async def test_postgresql_m1_probe_maps_structured_findings() -> None:
    """Map PostgreSQL M1 evidence into a deduplicated finding.

    Given: One venue identity candidate and one PostgreSQL conflict row.
    When: The PostgreSQL M1 probe executes.
    Then: It preserves both execution times and the venue identity.
    """
    expected = _NOW
    conflicting = _NOW.replace(minute=1)
    session = _session_with_rows(
        [
            {
                "instrument_public_id": "instrument-1",
                "trade_id": "trade-1",
                "expected_executed_at": expected,
                "conflicting_executed_at": conflicting,
            }
        ]
    )

    findings = await _find_m1_postgresql(
        session,
        [_M1Candidate("instrument-1", "trade-1", expected)],
    )

    assert findings == (_m1_finding("trade-1", expected, conflicting),)


@pytest.mark.asyncio
async def test_postgresql_m2_probe_maps_active_count() -> None:
    """Map PostgreSQL M2 evidence into an active-identity finding.

    Given: One public identity candidate with three active placements.
    When: The PostgreSQL M2 probe executes.
    Then: It reports the public identity and its active count.
    """
    session = _session_with_rows([{"public_id": "public-1", "active_count": 3}])

    findings = await _find_m2_postgresql(session, ["public-1"])

    assert findings == (_m2_finding("public-1", 3),)


@pytest.mark.asyncio
async def test_postgresql_windowed_m1_probe_binds_the_window_per_candidate() -> None:
    """Send each windowed M1 candidate's time with the fixed probe window.

    Given: One swept venue identity and one PostgreSQL conflict row.
    When: The windowed PostgreSQL M1 probe executes.
    Then: It sends the windowed statement with the candidate arrays, the
        seven-day window and the full cap, and maps the row into a finding.
    """
    conflicting = _NOW + timedelta(days=6)
    session = _session_with_rows(
        [
            {
                "instrument_public_id": "instrument-1",
                "trade_id": "trade-1",
                "expected_executed_at": _NOW,
                "conflicting_executed_at": conflicting,
            }
        ]
    )
    collector = _FindingCollector()

    findings = await _find_m1_windowed_postgresql(
        session,
        [_M1WindowCandidate("instrument-1", "trade-1", _NOW)],
        collector,
    )

    assert findings == (_m1_finding("trade-1", _NOW, conflicting),)
    cast(AsyncMock, session.execute).assert_awaited_once_with(
        _M1_WINDOWED_POSTGRES_QUERY,
        {
            "instrument_public_ids": ["instrument-1"],
            "trade_ids": ["trade-1"],
            "executed_ats": [_NOW],
            "probe_window": TRADE_INTEGRITY_PROBE_WINDOW,
            "finding_limit": 20,
        },
    )


@pytest.mark.asyncio
async def test_postgresql_windowed_m2_probe_binds_the_window_per_candidate() -> None:
    """Send each windowed M2 candidate's time with the fixed probe window.

    Given: Two windowed candidates of one public id and one PostgreSQL row.
    When: The windowed PostgreSQL M2 probe executes.
    Then: It sends both candidate times with the seven-day window and maps
        the row into a finding.
    """
    session = _session_with_rows([{"public_id": "public-1", "active_count": 2}])
    later = _NOW + timedelta(days=3)

    findings = await _find_m2_windowed_postgresql(
        session,
        [_M2WindowCandidate("public-1", _NOW), _M2WindowCandidate("public-1", later)],
        _FindingCollector(),
    )

    assert findings == (_m2_finding("public-1", 2),)
    cast(AsyncMock, session.execute).assert_awaited_once_with(
        _M2_WINDOWED_POSTGRES_QUERY,
        {
            "public_ids": ["public-1", "public-1"],
            "executed_ats": [_NOW, later],
            "probe_window": TRADE_INTEGRITY_PROBE_WINDOW,
            "finding_limit": 20,
        },
    )


@pytest.mark.asyncio
async def test_probe_limits_skip_database_work(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Stop every dialect probe before querying after its finding cap.

    Given: A zero finding limit and non-empty exact and windowed candidates.
    When: Each PostgreSQL and SQLite probe begins a candidate chunk.
    Then: No database query executes and every probe returns no findings.
    """
    session = _session_with_rows([])
    candidate = _M1Candidate("instrument-1", "trade-1", _NOW)
    m1_window = _M1WindowCandidate("instrument-1", "trade-1", _NOW)
    m2_window = _M2WindowCandidate("public-1", _NOW)
    monkeypatch.setattr(integrity_module, "_FINDING_LIMIT", 0)

    results = (
        await _find_m1_postgresql(session, [candidate]),
        await _find_m1_sqlite(session, [candidate]),
        await _find_m2_postgresql(session, ["public-1"]),
        await integrity_module._find_m2_sqlite(session, ["public-1"]),
        await _find_m1_windowed_postgresql(session, [m1_window], _FindingCollector()),
        await _find_m1_windowed_sqlite(session, [m1_window], _FindingCollector()),
        await _find_m2_windowed_postgresql(session, [m2_window], _FindingCollector()),
        await _find_m2_windowed_sqlite(session, [m2_window], _FindingCollector()),
    )

    assert results == ((), (), (), (), (), (), (), ())
    cast(AsyncMock, session.execute).assert_not_awaited()


@pytest.mark.asyncio
async def test_sqlite_m1_probe_reports_any_time_for_a_candidate_without_one() -> None:
    """Treat a stored time as distinct from a candidate without a time.

    Given: An exact M1 candidate without an execution time and one stored row
        of its venue identity.
    When: The SQLite M1 probe evaluates the supplied candidate.
    Then: The stored row is a finding without an expected time, as
        ``IS DISTINCT FROM`` reports it on PostgreSQL.
    """
    stored = _NOW - timedelta(days=30)
    session = _session_with_rows(
        [{"instrument_public_id": "instrument-1", "trade_id": "trade-1", "executed_at": stored}]
    )

    findings = await _find_m1_sqlite(
        session,
        [_M1Candidate("instrument-1", "trade-1", None)],
    )

    assert findings == (_m1_finding("trade-1", None, stored),)
    cast(AsyncMock, session.execute).assert_awaited_once()


@pytest.mark.asyncio
async def test_sqlite_probes_stop_at_the_cap_inside_one_chunk(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Record no finding beyond the cap from later candidates of the same chunk.

    Given: A cap of one finding and two candidates in one chunk, both with
        stored conflicts inside the window, for each SQLite probe.
    When: Each SQLite probe evaluates its chunk.
    Then: Each records only the first candidate's finding.
    """
    monkeypatch.setattr(integrity_module, "_FINDING_LIMIT", 1)
    conflicting = _NOW + timedelta(days=1)
    m1_rows: list[dict[str, object]] = [
        {"instrument_public_id": "instrument-1", "trade_id": trade_id, "executed_at": conflicting}
        for trade_id in ("trade-1", "trade-2")
    ]
    m2_rows: list[dict[str, object]] = [
        {"public_id": public_id, "executed_at": executed_at}
        for public_id in ("public-1", "public-2")
        for executed_at in (_NOW, conflicting)
    ]

    exact = await _find_m1_sqlite(
        _session_with_rows(m1_rows),
        [_M1Candidate("instrument-1", trade_id, _NOW) for trade_id in ("trade-1", "trade-2")],
    )
    windowed = await _find_m1_windowed_sqlite(
        _session_with_rows(m1_rows),
        [_M1WindowCandidate("instrument-1", trade_id, _NOW) for trade_id in ("trade-1", "trade-2")],
        _FindingCollector(),
    )
    active = await _find_m2_windowed_sqlite(
        _session_with_rows(m2_rows),
        [_M2WindowCandidate(public_id, _NOW) for public_id in ("public-1", "public-2")],
        _FindingCollector(),
    )

    assert exact == (_m1_finding("trade-1", _NOW, conflicting),)
    assert windowed == (_m1_finding("trade-1", _NOW, conflicting),)
    assert active == (_m2_finding("public-1", 2),)


@pytest.mark.asyncio
async def test_sqlite_windowed_probes_ignore_rows_without_a_time() -> None:
    """Never count a stored row without an execution time inside a window.

    Given: Windowed M1 and M2 candidates whose identities have stored rows
        without an execution time besides rows at the candidate's own time.
    When: The windowed SQLite probes evaluate them.
    Then: Neither reports a finding.
    """
    m1_rows: list[dict[str, object]] = [
        {"instrument_public_id": "instrument-1", "trade_id": "trade-1", "executed_at": None},
        {"instrument_public_id": "instrument-1", "trade_id": "trade-1", "executed_at": _NOW},
    ]
    m2_rows: list[dict[str, object]] = [
        {"public_id": "public-1", "executed_at": None},
        {"public_id": "public-1", "executed_at": _NOW},
    ]

    windowed = await _find_m1_windowed_sqlite(
        _session_with_rows(m1_rows),
        [_M1WindowCandidate("instrument-1", "trade-1", _NOW)],
        _FindingCollector(),
    )
    active = await _find_m2_windowed_sqlite(
        _session_with_rows(m2_rows),
        [_M2WindowCandidate("public-1", _NOW)],
        _FindingCollector(),
    )

    assert (windowed, active) == ((), ())


def test_route_m1_skips_rows_without_a_trade_identity() -> None:
    """Exclude sweep rows and worklog obligations that lack a venue trade identity.

    Given: A swept trade and a worklog obligation, both without a trade id.
    When: M1 candidates are routed.
    Then: Neither probe receives a candidate.
    """
    route = _route_m1(
        [_trade("public-1", None, _NOW)],
        [_work_item("public-2", None, _NOW)],
    )

    assert route == _M1Route(windowed=[], exact=[])


def test_route_m1_gives_owed_identities_exact_precedence() -> None:
    """Probe an identity that is both swept and owed only exactly.

    Given: Two swept rows of one owed identity, one swept row of another
        identity twice, and an owed identity without an execution time.
    When: M1 candidates are routed.
    Then: The owed identities are exact with their worklog values, and only
        the other identity is windowed, once.
    """
    owed_time = _NOW - timedelta(days=9)
    route = _route_m1(
        [
            _trade("public-1", "owed", _NOW),
            _trade("public-2", "owed", _NOW + timedelta(days=1)),
            _trade("public-3", "free", _NOW),
            _trade("public-3", "free", _NOW),
        ],
        [
            _work_item("public-1", "owed", owed_time),
            _work_item("public-4", "untimed", None),
        ],
    )

    assert route == _M1Route(
        windowed=[_M1WindowCandidate("instrument-1", "free", _NOW)],
        exact=[
            _M1Candidate("instrument-1", "owed", owed_time),
            _M1Candidate("instrument-1", "untimed", None),
        ],
    )


def test_route_m2_keeps_distinct_execution_times_of_one_public_id_windowed() -> None:
    """Keep each distinct execution time of a public id as its own candidate.

    Given: Swept rows of one public id at two times, one of them twice, and a
        swept row of an owed public id.
    When: M2 candidates are routed.
    Then: The owed public id is exact only, and the other public id has one
        windowed candidate per distinct execution time.
    """
    later = _NOW + timedelta(days=10)
    route = _route_m2(
        [
            _trade("public-1", "trade-1", _NOW),
            _trade("public-1", "trade-2", later),
            _trade("public-1", "trade-3", later),
            _trade("public-2", "trade-4", _NOW),
        ],
        [_work_item("public-2", "trade-4", _NOW), _work_item("public-2", "trade-4", None)],
    )

    assert route == _M2Route(
        windowed=[_M2WindowCandidate("public-1", _NOW), _M2WindowCandidate("public-1", later)],
        exact=["public-2"],
    )


def _recording_probe(
    calls: list[tuple[str, object]],
    name: str,
    key: tuple[str, ...],
    finding: TradeIntegrityFinding,
) -> AsyncMock:
    """Build a probe double that records its call and adds one finding.

    Args:
        calls: Shared call log of probe names and candidate lists.
        name: Name recorded for this probe.
        key: Collector key of the finding it adds.
        finding: Finding it adds to the shared collector.

    Returns:
        An asynchronous probe double.
    """

    async def probe(
        session: AsyncSession,
        candidates: object,
        collector: _FindingCollector,
    ) -> tuple[TradeIntegrityFinding, ...]:
        """Record the call and add the finding to the shared collector."""
        calls.append((name, candidates))
        collector.add(key, finding)
        return collector.findings()

    return AsyncMock(side_effect=probe)


_PROBE_NAMES: dict[tuple[str, str], tuple[str, str]] = {
    ("m1", "postgresql"): ("_find_m1_postgresql", "_find_m1_windowed_postgresql"),
    ("m1", "sqlite"): ("_find_m1_sqlite", "_find_m1_windowed_sqlite"),
    ("m2", "postgresql"): ("_find_m2_postgresql", "_find_m2_windowed_postgresql"),
    ("m2", "sqlite"): ("_find_m2_sqlite", "_find_m2_windowed_sqlite"),
}


@pytest.mark.asyncio
@pytest.mark.parametrize(("monitor", "dialect"), list(_PROBE_NAMES))
async def test_dispatch_runs_exact_then_windowed_probes_with_one_collector(
    monkeypatch: pytest.MonkeyPatch,
    monitor: Literal["m1", "m2"],
    dialect: str,
) -> None:
    """Route a page to the exact probe first and the windowed probe second.

    Given: One swept trade and one owed obligation of different identities.
    When: Violation discovery runs for the selected monitor and dialect.
    Then: The exact probe receives the obligation, the windowed probe the
        swept row, and both findings come back from one shared collector.
    """
    exact_name, windowed_name = _PROBE_NAMES[(monitor, dialect)]
    calls: list[tuple[str, object]] = []
    exact_finding = _m2_finding("exact", 2)
    windowed_finding = _m2_finding("windowed", 2)
    monkeypatch.setattr(
        integrity_module,
        exact_name,
        _recording_probe(calls, "exact", ("exact",), exact_finding),
    )
    monkeypatch.setattr(
        integrity_module,
        windowed_name,
        _recording_probe(calls, "windowed", ("windowed",), windowed_finding),
    )
    request = TradeIntegrityPassRequest(monitor=monitor, now=_NOW, sweep_limit=1, worklog_limit=1)

    findings = await _find_violations(
        MagicMock(),
        dialect,
        request,
        [_trade("public-1", "trade-1", _NOW)],
        [_work_item("public-2", "trade-2", _NOW)],
    )

    assert findings == (exact_finding, windowed_finding)
    expected_exact: object = (
        [_M1Candidate("instrument-1", "trade-2", _NOW)] if monitor == "m1" else ["public-2"]
    )
    expected_windowed: object = (
        [_M1WindowCandidate("instrument-1", "trade-1", _NOW)]
        if monitor == "m1"
        else [_M2WindowCandidate("public-1", _NOW)]
    )
    assert calls == [("exact", expected_exact), ("windowed", expected_windowed)]


@pytest.mark.asyncio
@pytest.mark.parametrize(("monitor", "dialect"), list(_PROBE_NAMES))
async def test_dispatch_short_circuits_without_candidates(
    monkeypatch: pytest.MonkeyPatch,
    monitor: Literal["m1", "m2"],
    dialect: str,
) -> None:
    """Skip every database probe when the page has no candidate identity.

    Given: A pass with no sweep rows or pending worklog obligations.
    When: Violation discovery routes the page for the selected monitor and dialect.
    Then: It returns no finding without invoking either probe.
    """
    probes = {name: AsyncMock() for name in _PROBE_NAMES[(monitor, dialect)]}
    for name, probe in probes.items():
        monkeypatch.setattr(integrity_module, name, probe)
    request = TradeIntegrityPassRequest(monitor=monitor, now=_NOW, sweep_limit=1, worklog_limit=1)

    findings = await _find_violations(MagicMock(), dialect, request, [], [])

    assert findings == ()
    for probe in probes.values():
        probe.assert_not_awaited()


@pytest.mark.asyncio
async def test_monitor_rejects_unsupported_dialect() -> None:
    """Reject a monitor pass for a database without a supported probe.

    Given: Valid pass limits and an unsupported database dialect.
    When: A trade-integrity monitor pass starts.
    Then: It raises before issuing database work.
    """
    request = TradeIntegrityPassRequest(
        monitor="m1",
        now=_NOW,
        sweep_limit=1,
        worklog_limit=1,
    )

    s5778_value_1 = MagicMock()
    with pytest.raises(ValueError, match="unsupported trade integrity dialect"):
        await run_trade_integrity_monitor(s5778_value_1, "oracle", request)
