"""Dialect routing tests for incremental trade-integrity probes."""

from datetime import UTC
from datetime import datetime
from typing import Literal
from typing import cast
from unittest.mock import AsyncMock
from unittest.mock import MagicMock

import pytest
from sqlalchemy.ext.asyncio import AsyncSession

from snapper.data import trade_integrity as integrity_module
from snapper.data.models import Trade
from snapper.data.models import TradeIntegrityWorkItem
from snapper.data.repository_types import TradeIntegrityFinding
from snapper.data.trade_integrity import TradeIntegrityPassRequest
from snapper.data.trade_integrity import _find_m1_postgresql
from snapper.data.trade_integrity import _find_m1_sqlite
from snapper.data.trade_integrity import _find_m2_postgresql
from snapper.data.trade_integrity import _find_violations
from snapper.data.trade_integrity import _m1_candidates
from snapper.data.trade_integrity import _M1Candidate
from snapper.data.trade_integrity import run_trade_integrity_monitor

_NOW = datetime(2026, 7, 29, 12, 0, tzinfo=UTC)


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

    assert findings == (
        TradeIntegrityFinding(
            monitor="m1",
            public_id=None,
            instrument_public_id="instrument-1",
            trade_id="trade-1",
            expected_executed_at=expected,
            conflicting_executed_at=conflicting,
            active_count=None,
        ),
    )


@pytest.mark.asyncio
async def test_postgresql_m2_probe_maps_active_count() -> None:
    """Map PostgreSQL M2 evidence into an active-identity finding.

    Given: One public identity candidate with three active placements.
    When: The PostgreSQL M2 probe executes.
    Then: It reports the public identity and its active count.
    """
    session = _session_with_rows([{"public_id": "public-1", "active_count": 3}])

    findings = await _find_m2_postgresql(session, ["public-1"])

    assert findings == (
        TradeIntegrityFinding(
            monitor="m2",
            public_id="public-1",
            instrument_public_id=None,
            trade_id=None,
            expected_executed_at=None,
            conflicting_executed_at=None,
            active_count=3,
        ),
    )


@pytest.mark.asyncio
async def test_probe_limits_skip_database_work(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Stop every dialect probe before querying after its finding cap.

    Given: A zero finding limit and non-empty candidates.
    When: Each PostgreSQL and SQLite probe begins a candidate chunk.
    Then: No database query executes and every probe returns no findings.
    """
    session = _session_with_rows([])
    candidate = _M1Candidate("instrument-1", "trade-1", _NOW)
    monkeypatch.setattr(integrity_module, "_FINDING_LIMIT", 0)

    results = (
        await _find_m1_postgresql(session, [candidate]),
        await _find_m1_sqlite(session, [candidate]),
        await _find_m2_postgresql(session, ["public-1"]),
        await integrity_module._find_m2_sqlite(session, ["public-1"]),
    )

    assert results == ((), (), (), ())
    cast(AsyncMock, session.execute).assert_not_awaited()


@pytest.mark.asyncio
async def test_sqlite_m1_probe_accepts_null_execution_candidates() -> None:
    """Use the nullable SQLite predicate for an unknown execution time.

    Given: An M1 candidate whose execution time is unavailable.
    When: The SQLite probe builds and executes its candidate query.
    Then: The query completes through the null-time predicate.
    """
    session = _session_with_rows([])

    findings = await _find_m1_sqlite(
        session,
        [_M1Candidate("instrument-1", "trade-1", None)],
    )

    assert findings == ()
    cast(AsyncMock, session.execute).assert_awaited_once()


def test_m1_candidates_skip_null_worklog_trade_identity() -> None:
    """Exclude worklog obligations that lack a venue trade identity.

    Given: A worklog item whose trade ID is null.
    When: M1 candidates are assembled.
    Then: The incomplete identity is omitted.
    """
    item = MagicMock(trade_id=None)

    assert _m1_candidates([], [cast(TradeIntegrityWorkItem, item)]) == []


@pytest.mark.asyncio
@pytest.mark.parametrize("monitor", ["m1", "m2"])
async def test_postgresql_dispatch_uses_monitor_specific_probe(
    monkeypatch: pytest.MonkeyPatch,
    monitor: Literal["m1", "m2"],
) -> None:
    """Route PostgreSQL candidates to the selected monitor probe.

    Given: A PostgreSQL pass with one candidate for the selected monitor.
    When: Violation discovery dispatches by monitor.
    Then: It returns the finding produced by that monitor's PostgreSQL probe.
    """
    finding = MagicMock()
    m1_probe = AsyncMock(return_value=(finding,))
    m2_probe = AsyncMock(return_value=(finding,))
    monkeypatch.setattr(integrity_module, "_find_m1_postgresql", m1_probe)
    monkeypatch.setattr(integrity_module, "_find_m2_postgresql", m2_probe)
    request = TradeIntegrityPassRequest(
        monitor=monitor,
        now=_NOW,
        sweep_limit=1,
        worklog_limit=1,
    )
    trade = MagicMock(
        public_id="public-1",
        instrument_public_id="instrument-1",
        trade_id="trade-1",
        executed_at=_NOW,
    )

    findings = await _find_violations(
        MagicMock(),
        "postgresql",
        request,
        [cast(Trade, trade)],
        [],
    )

    assert findings == (finding,)
    selected = m1_probe if monitor == "m1" else m2_probe
    selected.assert_awaited_once()


@pytest.mark.asyncio
async def test_m2_dispatch_short_circuits_without_candidates(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Skip every M2 database probe when the pass has no candidate identity.

    Given: An M2 pass with no sweep rows or pending worklog obligations,
    When: Violation discovery assembles its public-ID candidates,
    Then: It returns no finding without invoking either dialect probe.
    """
    postgresql_probe = AsyncMock()
    sqlite_probe = AsyncMock()
    monkeypatch.setattr(integrity_module, "_find_m2_postgresql", postgresql_probe)
    monkeypatch.setattr(integrity_module, "_find_m2_sqlite", sqlite_probe)
    request = TradeIntegrityPassRequest(
        monitor="m2",
        now=_NOW,
        sweep_limit=1,
        worklog_limit=1,
    )

    findings = await _find_violations(
        MagicMock(),
        "postgresql",
        request,
        [],
        [],
    )

    assert findings == ()
    postgresql_probe.assert_not_awaited()
    sqlite_probe.assert_not_awaited()


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

    with pytest.raises(ValueError, match="unsupported trade integrity dialect"):
        await run_trade_integrity_monitor(MagicMock(), "oracle", request)
