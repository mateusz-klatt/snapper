"""Integration, R1, and determinism pins for PR-1 repair-obligation liveness."""

import logging
import math
import os
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import replace
from datetime import datetime
from datetime import timedelta
from pathlib import Path
from typing import cast
from uuid import uuid4

import pytest
from sqlalchemy.engine import make_url

from snapper.application.portfolio import pnl_snapshotter
from snapper.application.portfolio import pnl_timeline
from snapper.application.portfolio.average_cost import FLAT_EPSILON
from snapper.application.portfolio.pnl_correction_planner import PnlCorrectionPlanInputs
from snapper.application.portfolio.pnl_correction_planner import plan_correction_chunk
from snapper.application.portfolio.pnl_faithful_recovery import MAX_RECOVERY_KEY_VISITS
from snapper.application.portfolio.pnl_snapshot_planner import MinuteInputs
from snapper.application.portfolio.pnl_snapshotter import PortfolioPnlSnapshotter
from snapper.application.portfolio.pnl_timeline import MarkMap
from snapper.application.portfolio.pnl_timeline import PnlIncompletenessReason
from snapper.application.portfolio.pnl_timeline import PnlIncompletenessReasonEntry
from snapper.application.portfolio.pnl_timeline import PnlTimelineWorkBudgetError
from snapper.application.portfolio.pnl_timeline import TimelineExecution
from snapper.application.portfolio.pnl_timeline import TimelineExecutionLineage
from snapper.application.portfolio.pnl_timeline import build_pnl_timeline
from snapper.application.portfolio.pnl_timeline_service import PnlSeriesReadPolicy
from snapper.application.portfolio.pnl_timeline_service import PnlSeriesReplayMetadata
from snapper.application.portfolio.pnl_timeline_service import PnlSeriesReplayOptions
from snapper.application.portfolio.pnl_timeline_service import PnlWalletSeriesResult
from snapper.application.portfolio.pnl_timeline_service import build_wallet_pnl_series
from snapper.data.models import KNOWN_TO_MAX
from snapper.data.models import Execution
from snapper.data.models import PortfolioPnlPoint
from snapper.data.models import VenueEvent
from snapper.data.models import WalletCredential
from snapper.data.pnl_correction_state import PnlCorrectionChunk
from snapper.data.pnl_correction_state import PnlCorrectionEdit
from snapper.data.pnl_correction_state import PnlCorrectionEvidence
from snapper.data.pnl_correction_state import PnlCorrectionFrontier
from snapper.data.pnl_correction_state import PnlCorrectionKey
from snapper.data.repository import PortfolioPnlSampleQuery
from snapper.data.repository import PortfolioPnlSampleScope
from snapper.data.repository import SQLAlchemyRepository
from snapper.data.repository_types import PNL_SAMPLE_CALC_VERSION
from snapper.data.repository_types import PortfolioPnlSampleRow
from tests.application.portfolio import test_pnl_snapshot_planner as planner_fixtures
from tests.application.portfolio import test_pnl_snapshotter as series_fixtures
from tests.application.portfolio import test_pnl_timeline as timeline_fixtures
from tests.application.portfolio import test_pnl_timeline_service as service_fixtures
from tests.data import test_pnl_crypto_usd_plane_repository as prices
from tests.data import test_pnl_timeline_execution_prefix_repository as prefix
from tests.data import test_pnl_timeline_sample_repository as samples

_MINUTE = timedelta(minutes=1)
_T0 = samples._T0
_M1, _M2, _M3, _M4 = (
    _T0 + _MINUTE,
    _T0 + 2 * _MINUTE,
    _T0 + 3 * _MINUTE,
    _T0 + 4 * _MINUTE,
)
_MANIFEST = "a" * 64
_WITNESS_DIGEST = "c" * 64


@pytest.fixture(params=("sqlite", "postgresql"))
async def db_url(
    request: pytest.FixtureRequest,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> AsyncIterator[str]:
    """Provide an isolated SQLite DB or opt-in disposable PostgreSQL scratch DB."""
    monkeypatch.delenv("PNL_FX_SHADOW_PINNING_ENABLED", raising=False)
    if request.param == "sqlite":
        yield f"sqlite+aiosqlite:///{(tmp_path / 'repair-liveness.db').as_posix()}"
        return
    configured = os.environ.get("PNL_TEST_POSTGRES_URL")
    if configured is None:
        pytest.skip("requires isolated PNL_TEST_POSTGRES_URL")
    base = make_url(configured)
    assert base.get_backend_name() == "postgresql"
    database_name = f"pnl_repair_liveness_{uuid4().hex}"
    maintenance = SQLAlchemyRepository(configured)
    created = False
    try:
        async with maintenance.engine.connect() as connection:
            admin = await connection.execution_options(isolation_level="AUTOCOMMIT")
            await admin.exec_driver_sql(f'CREATE DATABASE "{database_name}"')
            created = True
        yield base.set(database=database_name).render_as_string(hide_password=False)
    finally:
        if created:
            async with maintenance.engine.connect() as connection:
                admin = await connection.execution_options(isolation_level="AUTOCOMMIT")
                await admin.exec_driver_sql(f'DROP DATABASE IF EXISTS "{database_name}"')
        await maintenance.engine.dispose()


@asynccontextmanager
async def _repository(url: str) -> AsyncIterator[SQLAlchemyRepository]:
    """Open and dispose a dedicated repository pool."""
    repo = SQLAlchemyRepository(url)
    try:
        yield repo
    finally:
        await repo.engine.dispose()


async def _seed_scope(url: str) -> PortfolioPnlSampleScope:
    """Seed anchor, kraken credential, observations, and one matched execution for reads."""
    anchor = service_fixtures._empty_anchor(samples._WALLET, "live", "USD", _T0)
    async with _repository(url) as repo:
        await repo.create_all()
        async with repo.session() as session:
            session.add(PortfolioPnlPoint(**anchor, known_to=KNOWN_TO_MAX))
            session.add(
                WalletCredential(
                    public_id="00000000-0000-7000-8000-000000000071",
                    wallet_public_id=samples._WALLET,
                    exchange="kraken",
                    credential_type="api_key_secret",
                    encrypted_payload="repair-liveness-fixture",
                    timestamp=_T0,
                    session_id=samples._SESSION,
                    sequence_id=1,
                    known_to=KNOWN_TO_MAX,
                )
            )
            session.add_all((prefix._symbol(), prefix._instrument()))
            session.add(
                prices._spec(prefix._INSTRUMENT, ver=prices._Ver(timestamp=prefix._EARLIER))
            )
            order = prefix._order()
            order.wallet_public_id = samples._WALLET
            execution: Execution = prefix._execution(
                "kraken", 1, _M1, order.public_id, wallet_public_id=samples._WALLET
            )
            event: VenueEvent = prefix._fill_event(
                prefix._CLIENT_ORDER,
                prefix._KRAKEN_SHARD,
                wallet_public_id=samples._WALLET,
                sequence_id=1,
            )
            event.timestamp = event.received_at = event.venue_timestamp = _M1
            session.add_all((order, execution, event))
            for idx, minute in enumerate((_M1, _M2, _M3, _M4), start=1):
                obs = samples._observation(
                    timestamp=minute,
                    balances_json=f'[{{"currency":"USD","total":{2500.0 if minute == _M2 else 1000.0}}}]',
                )
                obs.public_id = f"00000000-0000-7000-8000-0000000b{idx:04x}"
                obs.sequence_id = idx
                session.add(obs)
                session.add(
                    prices._candle(
                        prefix._INSTRUMENT,
                        minute - _MINUTE,
                        100.0,
                        spec=prices._CandleSpec(
                            timestamp=minute,
                            public_id=f"00000000-0000-7000-8000-0000000c{idx:04x}",
                        ),
                    )
                )
            await session.commit()
    return samples._scope(epoch=anchor["epoch_public_id"])


def _scripted_point(minute: datetime, reason: PnlIncompletenessReason | None) -> object:
    """Build either a complete point or an untrusted point with one causal reason."""
    if reason is None:
        return series_fixtures._complete_point(minute)
    return replace(
        series_fixtures._untrusted_point(minute),
        incompleteness_reasons=(
            PnlIncompletenessReasonEntry(
                reason=reason,
                withholding_tier="untrusted",
                withholding_scope="global",
                trigger_instrument_public_id=None,
            ),
        ),
    )


def _install_scripted_series(
    monkeypatch: pytest.MonkeyPatch,
    state: dict[str, object],
    reads: list[tuple[datetime, datetime, bool]],
) -> None:
    """Patch build_wallet_pnl_series so replay writes scripted points without touching current reads."""

    async def fake_series(*args: object, **kwargs: object) -> PnlWalletSeriesResult:
        """Serve scripted replay outputs while recording replay windows."""
        start, end = cast(datetime, args[3]), cast(datetime, args[4])
        options = cast(PnlSeriesReplayOptions, kwargs["options"])
        reads.append((start, end, options.metadata_only))
        seq = cast(int, state["seq"])
        affected = cast(datetime | None, state.get("affected"))
        reason_at_m2 = cast(PnlIncompletenessReason | None, state.get("m2_reason"))
        points = tuple(
            _scripted_point(
                minute,
                reason_at_m2 if minute == _M2 else None,
            )
            for minute in (_M1, _M2, _M3, _M4)
            if start <= minute <= end
        )
        metadata = PnlSeriesReplayMetadata(
            max_scope_sequence_by_exchange={"kraken": seq},
            earliest_affected_minute=affected,
            detection_watermarks={"kraken": seq},
            raw_execution_watermarks={"kraken": seq + 100},
            execution_manifest_digest=_MANIFEST,
            has_fill_gap=False,
            fill_witness_cut={"watermarks": {"kraken:spot": 10}, "digest": _WITNESS_DIGEST},
        )
        return PnlWalletSeriesResult(
            points=cast(tuple[pnl_timeline.PnlTimelinePoint, ...], points),
            granularity="1m",
            valuation_ccy="USD",
            rate_sources=(),
            replay_metadata=metadata,
        )

    monkeypatch.setattr(pnl_snapshotter, "build_wallet_pnl_series", fake_series)


async def _frontier(
    repo: SQLAlchemyRepository, scope: PortfolioPnlSampleScope
) -> PnlCorrectionFrontier:
    """Load the persisted frontier and assert it exists."""
    frontier = await repo.get_portfolio_pnl_correction_frontier(scope)
    assert frontier is not None
    return frontier


def _query(scope: PortfolioPnlSampleScope, *, require_reconciled: bool) -> PortfolioPnlSampleQuery:
    """Build a sample query for the test scope."""
    return PortfolioPnlSampleQuery(
        wallet_public_id=scope.wallet_public_id,
        mode=scope.mode,
        valuation_ccy=scope.valuation_ccy,
        epoch_public_id=scope.epoch_public_id,
        calc_version=PNL_SAMPLE_CALC_VERSION,
        require_reconciled=require_reconciled,
    )


async def test_settled_frontier_clears_repair_from_for_absent_and_retracted_exempt_withholds(
    db_url: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Settle repair_from=None for absent and retracted exempt withholds on SQLite and PG.

    Given: A scope seeded in SQLite or opt-in PostgreSQL.
    When: M2 withholds with instrument_reconciliation_failed first while absent, then later
        retracting an active M2 row, followed by an idle tick at M4.
    Then: In both cases the frontier settles with repair_from=None, baseline_watermarks
        advances, require_reconciled=True hides M2 consistently, and M4 never restarts at M2.
    """
    scope = await _seed_scope(db_url)
    reads: list[tuple[datetime, datetime, bool]] = []
    state: dict[str, object] = {
        "seq": 1,
        "affected": None,
        "m2_reason": "instrument_reconciliation_failed",
    }
    _install_scripted_series(monkeypatch, state, reads)

    async with _repository(db_url) as repo:
        await PortfolioPnlSnapshotter(
            repo=repo, disabled=False, clock=lambda: _M3 + timedelta(minutes=2)
        )._tick_once()
        g1 = await _frontier(repo, scope)
        assert g1.next_minute is None
        assert g1.repair_from is None
        assert g1.baseline_watermarks == {"kraken": 1}
        reconciled_g1 = await repo.get_portfolio_pnl_samples(
            _query(scope, require_reconciled=True), _M1, _M3
        )
        assert [row["point_time"] for row in reconciled_g1] == [_M1, _M3]

        state.update(seq=2, affected=_M2, m2_reason=None)
        await PortfolioPnlSnapshotter(
            repo=repo, disabled=False, clock=lambda: _M3 + timedelta(minutes=2, seconds=10)
        )._tick_once()
        g2 = await _frontier(repo, scope)
        assert g2.repair_from is None
        assert g2.baseline_watermarks == {"kraken": 2}
        assert [
            row["point_time"]
            for row in await repo.get_portfolio_pnl_samples(
                _query(scope, require_reconciled=True), _M1, _M3
            )
        ] == [_M1, _M2, _M3]

        state.update(seq=3, affected=_M2, m2_reason="instrument_reconciliation_failed")
        await PortfolioPnlSnapshotter(
            repo=repo, disabled=False, clock=lambda: _M3 + timedelta(minutes=2, seconds=20)
        )._tick_once()
        g3 = await _frontier(repo, scope)
        assert g3.next_minute is None
        assert g3.repair_from is None
        assert g3.baseline_watermarks == {"kraken": 3}
        assert [
            row["point_time"]
            for row in await repo.get_portfolio_pnl_samples(
                _query(scope, require_reconciled=True), _M1, _M3
            )
        ] == [_M1, _M3]

        reads.clear()
        state.update(seq=3, affected=None, m2_reason=None)
        await PortfolioPnlSnapshotter(
            repo=repo, disabled=False, clock=lambda: _M4 + timedelta(minutes=2)
        )._tick_once()
        g4 = await _frontier(repo, scope)
        assert g4.repair_from is None
        assert g4.tip == _M4
        assert [entry for entry in reads if not entry[2]] == [(_M4, _M4, False)]


async def test_r1_retained_sum_unrepresentable_pins_repair_from_and_gates_current_series_overlay(
    db_url: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Retain repair_from and suppress current equity overlay while R1 preserves a row.

    Given: A settled 5C.5 scope in SQLite or opt-in PostgreSQL with complete M1..M3 rows.
    When: M2 becomes pure instrument_sum_unrepresentable (preserving M2), then resolves.
    Then: repair_from stays pinned at M2, require_reconciled and current-truth overlay reads
        hide [M2..M3] before settlement, and expose all 3 consistent rows after settlement.
    """
    scope = await _seed_scope(db_url)
    reads: list[tuple[datetime, datetime, bool]] = []
    state: dict[str, object] = {"seq": 1, "affected": None, "m2_reason": None}
    _install_scripted_series(monkeypatch, state, reads)

    async with _repository(db_url) as repo:
        await PortfolioPnlSnapshotter(
            repo=repo, disabled=False, clock=lambda: _M3 + timedelta(minutes=2)
        )._tick_once()
        assert (await _frontier(repo, scope)).repair_from is None

        state.update(seq=2, affected=_M2, m2_reason="instrument_sum_unrepresentable")
        await PortfolioPnlSnapshotter(
            repo=repo, disabled=False, clock=lambda: _M3 + timedelta(minutes=2, seconds=10)
        )._tick_once()
        blocked = await _frontier(repo, scope)
        assert blocked.next_minute is None
        assert blocked.repair_from == _M2
        assert blocked.baseline_watermarks == {"kraken": 1}
        assert [
            row["point_time"]
            for row in await repo.get_portfolio_pnl_samples(
                _query(scope, require_reconciled=True), _M1, _M3
            )
        ] == [_M1]

        overlay_blocked = await build_wallet_pnl_series(
            repo,
            samples._WALLET,
            "live",
            _M1,
            _M3,
            "1m",
            _M3 + timedelta(minutes=2, seconds=10),
            valuation_ccy="USD",
            policy=PnlSeriesReadPolicy(allow_anchor_creation=False, current_truth=True),
        )
        assert overlay_blocked.equity_overlay_at(_M1).equity == 1000.0
        assert overlay_blocked.equity_overlay_at(_M2).equity is None
        assert overlay_blocked.equity_overlay_at(_M3).equity is None

        state.update(seq=3, affected=_M2, m2_reason=None)
        await PortfolioPnlSnapshotter(
            repo=repo, disabled=False, clock=lambda: _M3 + timedelta(minutes=2, seconds=20)
        )._tick_once()
        settled = await _frontier(repo, scope)
        assert settled.next_minute is None
        assert settled.repair_from is None
        assert settled.baseline_watermarks == {"kraken": 3}

        overlay_settled = await build_wallet_pnl_series(
            repo,
            samples._WALLET,
            "live",
            _M1,
            _M3,
            "1m",
            _M3 + timedelta(minutes=2, seconds=20),
            valuation_ccy="USD",
            policy=PnlSeriesReadPolicy(allow_anchor_creation=False, current_truth=True),
        )
        assert overlay_settled.equity_overlay_at(_M1).equity == 1000.0
        assert overlay_settled.equity_overlay_at(_M2).equity == 2500.0
        assert overlay_settled.equity_overlay_at(_M3).equity == 1000.0


async def _seed_foreign_sample(
    repo: SQLAlchemyRepository,
    scope: PortfolioPnlSampleScope,
    minute: datetime,
    calc_version: str,
) -> None:
    """Persist an active complete sample of a foreign calculation version."""
    foreign_row: PortfolioPnlSampleRow = samples._mutate(
        samples._complete_sample(minute),
        epoch_public_id=scope.epoch_public_id,
        calc_version=calc_version,
        cash_usd=2500.0,
        position_value_usd=0.0,
        drawdown=0.0,
    )
    async with repo.session() as session:
        session.add(
            PortfolioPnlPoint(
                **repo._portfolio_pnl_sample_orm_kwargs(foreign_row),
                known_to=KNOWN_TO_MAX,
            )
        )
        await session.commit()


async def test_foreign_calc_version_sum_unrepresentable_retracts_and_settles_repair_from(
    db_url: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Retract a foreign calc_version row on sum_unrepresentable and settle repair_from.

    Given: An active complete 5C.4 sample seeded at M2 in SQLite or opt-in PostgreSQL.
    When: A correction generation replays M1..M3 with M2 withheld purely as
        instrument_sum_unrepresentable and commits through the repository fence.
    Then: The fence accepts the retraction of that exact 5C.4 version, the foreign row is
        no longer active, M2 is in unrepairable_withheld_minutes, and repair_from settles
        to None without borrowing the foreign equity peak or re-replaying M2 at M4.
    """
    scope = await _seed_scope(db_url)
    reads: list[tuple[datetime, datetime, bool]] = []
    state: dict[str, object] = {
        "seq": 1,
        "affected": None,
        "m2_reason": "instrument_sum_unrepresentable",
    }
    _install_scripted_series(monkeypatch, state, reads)

    async with _repository(db_url) as repo:
        await _seed_foreign_sample(repo, scope, _M2, "5C.4")
        observed_before = await repo.get_portfolio_pnl_correction_evidence(scope, _M2, _M2)
        (foreign_version,) = observed_before.versions
        assert foreign_version.row["calc_version"] == "5C.4"

        committed_chunks: list[PnlCorrectionChunk] = []
        original_commit = repo.commit_portfolio_pnl_correction_chunk

        async def record_commit(
            commit_scope: PortfolioPnlSampleScope,
            current: PnlCorrectionFrontier,
            chunk: PnlCorrectionChunk,
        ) -> PnlCorrectionFrontier:
            """Forward chunk commit to the repository fence while recording the plan."""
            committed_chunks.append(chunk)
            return await original_commit(commit_scope, current, chunk)

        monkeypatch.setattr(repo, "commit_portfolio_pnl_correction_chunk", record_commit)
        await PortfolioPnlSnapshotter(
            repo=repo, disabled=False, clock=lambda: _M3 + timedelta(minutes=2)
        )._tick_once()

        (chunk,) = committed_chunks
        assert chunk.withheld_minutes == (_M2,)
        assert chunk.unrepairable_withheld_minutes == (_M2,)
        assert PnlCorrectionEdit(_M2, foreign_version, None) in chunk.edits
        after_commit = await repo.get_portfolio_pnl_correction_evidence(scope, _M2, _M2)
        assert after_commit.versions == ()
        foreign_query = replace(_query(scope, require_reconciled=False), calc_version="5C.4")
        assert await repo.get_portfolio_pnl_samples(foreign_query, _M1, _M3) == []

        settled = await _frontier(repo, scope)
        assert settled.next_minute is None
        assert settled.repair_from is None
        assert settled.baseline_watermarks == {"kraken": 1}
        assert settled.prior_peak == 1000.0
        reconciled = await repo.get_portfolio_pnl_samples(
            _query(scope, require_reconciled=True), _M1, _M3
        )
        assert [row["point_time"] for row in reconciled] == [_M1, _M3]
        assert [row["drawdown"] for row in reconciled] == [0.0, 0.0]

        reads.clear()
        state.update(seq=1, affected=None, m2_reason=None)
        await PortfolioPnlSnapshotter(
            repo=repo, disabled=False, clock=lambda: _M4 + timedelta(minutes=2)
        )._tick_once()
        advanced = await _frontier(repo, scope)
        assert advanced.repair_from is None
        assert advanced.tip == _M4
        assert [entry for entry in reads if not entry[2]] == [(_M4, _M4, False)]


def _unrepresentable_fee_constellation(
    *,
    minute: int,
    attribution_only: bool,
) -> tuple[tuple[TimelineExecution, ...], dict[str, TimelineExecutionLineage]]:
    """Build exact-unit fee fills producing *_sum_unrepresentable deterministically."""
    if attribution_only:
        executions = (
            timeline_fixtures._exec("I1", 101, minute, "buy", 0.0, 100.0, fee=-1e20),
            timeline_fixtures._exec("I1", 102, minute, "buy", 0.0, 100.0, fee=1e20),
            timeline_fixtures._exec("I1", 103, minute, "buy", 0.0, 100.0, fee=-1.0),
        )
        lineage = {
            executions[0].order_public_id: TimelineExecutionLineage(
                "rest", None, None, "live", None
            ),
            executions[1].order_public_id: TimelineExecutionLineage(
                "strategy", "plan-1", "signal-1", "live", "dca"
            ),
            executions[2].order_public_id: TimelineExecutionLineage(
                "strategy", "plan-1", "signal-1", "live", "dca"
            ),
        }
        return executions, lineage
    executions = (
        timeline_fixtures._exec("I1", 101, minute, "buy", 0.0, 100.0, fee=-1e20),
        timeline_fixtures._exec("I2", 102, minute, "buy", 0.0, 100.0, fee=1e20),
        timeline_fixtures._exec("I2", 103, minute, "buy", 0.0, 100.0, fee=-1.0),
    )
    return executions, {}


@pytest.mark.parametrize(
    ("attribution_only", "expected_reason"),
    [
        (False, "instrument_sum_unrepresentable"),
        (True, "attribution_sum_unrepresentable"),
    ],
)
def test_sum_unrepresentable_is_identical_across_1m_and_1440m_chunks(
    attribution_only: bool,
    expected_reason: PnlIncompletenessReason,
) -> None:
    """Pin determinism of *_sum_unrepresentable across 1-minute and 1,440-minute chunks.

    Given: A 1,440-minute window whose final minute hits *_sum_unrepresentable after
        an earlier minute consumes faithful recovery work.
    When: Valued with recovery_budget_per_point=True in a 1-minute slice and in a 1,440-minute chunk.
    Then: Both produce the identical withheld point and unrepairable classification.
    """
    prior_execs, prior_lineage = _faithful_fee_constellation(minute=1)
    target_execs, target_lineage = _unrepresentable_fee_constellation(
        minute=1439, attribution_only=attribution_only
    )
    executions = (*prior_execs, *target_execs)
    lineage = {**prior_lineage, **target_lineage}
    marks: MarkMap = {}

    wide_window = replace(timeline_fixtures._window(0, 1439), recovery_budget_per_point=True)
    narrow_window = replace(timeline_fixtures._window(1439, 1439), recovery_budget_per_point=True)
    wide = build_pnl_timeline(executions, (), marks, wide_window, lineage=lineage)
    narrow = build_pnl_timeline(executions, (), marks, narrow_window, lineage=lineage)

    assert len(wide.points) == 1440
    assert wide.points[-1] == narrow.points[0]
    assert narrow.points[0].valuation_status == "incomplete"
    assert [entry.reason for entry in narrow.points[0].incompleteness_reasons] == [expected_reason]

    point_time = narrow.points[0].point_time
    evidence = PnlCorrectionEvidence(
        key=PnlCorrectionKey(
            planner_fixtures._WALLET,
            "live",
            "USD",
            planner_fixtures._EPOCH,
            PNL_SAMPLE_CALC_VERSION,
        ),
        start=point_time,
        end=point_time,
        versions=(),
        prior_peak=None,
    )
    planned = plan_correction_chunk(
        PnlCorrectionPlanInputs(
            evidence=evidence,
            minute_inputs=(
                MinuteInputs(
                    narrow.points[0],
                    {"kraken": planner_fixtures._attempt(minute=point_time)},
                    planner_fixtures._crypto_evidence(),
                ),
            ),
            expected_venues=frozenset({"kraken"}),
            position_versions=(),
            make_row=planner_fixtures._to_row,
        )
    )
    assert planned.chunk.unrepairable_withheld_minutes == (point_time,)


def _faithful_fee_constellation(
    *, minute: int
) -> tuple[tuple[TimelineExecution, ...], dict[str, TimelineExecutionLineage]]:
    """Build a three-owner faithful recovery state at the given grid minute offset."""
    unit = math.ulp(20.0)
    deltas = (32.0, 0.875 * unit, 120.0, 1.875 * unit, -132.0, 2.875 * unit)
    executions = tuple(
        timeline_fixtures._exec("I3", index, minute, "buy", 0.0, 100.0, fee=-delta)
        for index, delta in enumerate(deltas, start=1)
    )
    owners = (
        TimelineExecutionLineage("rest", None, None, "live", None),
        TimelineExecutionLineage("strategy", "plan-2", "signal-2", "live", "dca"),
        TimelineExecutionLineage("strategy", None, "signal-3", "live", "rebalance"),
    )
    lineage = {
        execution.order_public_id: owners[idx // 2] for idx, execution in enumerate(executions)
    }
    return executions, lineage


def test_shared_replay_recovery_work_exhaustion_raises_instead_of_withholding(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Prove shared _ReplayRecoveryWork exhaustion raises PnlTimelineWorkBudgetError.

    Given: Two recovery-requiring minutes whose per-point FaithfulRecoveryBudget fits (22 visits)
        while the chunk-shared _ReplayRecoveryWork reserve permits only 25 visits across the chunk.
    When: build_pnl_timeline evaluates both minutes under recovery_budget_per_point=True.
    Then: The multi-minute chunk raises PnlTimelineWorkBudgetError instead of withholding, while
        each minute succeeds individually in a 1-minute chunk.
    """
    monkeypatch.setattr(pnl_timeline, "MAX_REPLAY_RECOVERY_KEY_VISITS", 25)
    assert MAX_RECOVERY_KEY_VISITS >= 22
    first, first_lineage = _faithful_fee_constellation(minute=0)
    second_raw, second_lineage = _faithful_fee_constellation(minute=1)
    second = tuple(
        replace(
            item,
            scope_sequence=item.scope_sequence + 10,
            order_public_id=f"second-{item.order_public_id}",
        )
        for item in second_raw
    )
    lineage = {
        **first_lineage,
        **{
            later.order_public_id: second_lineage[earlier.order_public_id]
            for earlier, later in zip(second_raw, second, strict=True)
        },
    }
    executions = (*first, *second)
    with pytest.raises(PnlTimelineWorkBudgetError):
        build_pnl_timeline(
            executions,
            (),
            {},
            replace(timeline_fixtures._window(0, 1), recovery_budget_per_point=True),
            lineage=lineage,
        )
    single_first = build_pnl_timeline(
        executions,
        (),
        {},
        replace(timeline_fixtures._window(0, 0), recovery_budget_per_point=True),
        lineage=lineage,
    )
    single_second = build_pnl_timeline(
        executions,
        (),
        {},
        replace(timeline_fixtures._window(1, 1), recovery_budget_per_point=True),
        lineage=lineage,
    )
    assert single_first.points[0].valuation_status == "complete"
    assert single_second.points[0].valuation_status == "complete"
    assert abs(single_second.points[0].fee_pnl or 0.0) > FLAT_EPSILON


def _lag_records(caplog: pytest.LogCaptureFixture) -> list[logging.LogRecord]:
    """Return structured repair_from_lag log records."""
    return [record for record in caplog.records if hasattr(record, "repair_from_lag")]


def _seed_frontier_with_repair(
    repo: series_fixtures._FakeRepo,
    repair_from: datetime,
    tip: datetime,
    *,
    wallet: str = series_fixtures._WALLET,
    next_minute: datetime | None = None,
) -> PnlCorrectionFrontier:
    """Install a frontier with an explicit repair_from and tip."""
    scope = series_fixtures._sample_scope(wallet)
    key = repo._correction_key(scope)
    frontier = PnlCorrectionFrontier(
        key=key,
        generation=1,
        revision=2,
        window_start=_M1,
        next_minute=next_minute,
        tip=tip,
        as_of=tip + timedelta(minutes=2),
        baseline_watermarks={"kraken": 1},
        pending_watermarks={"kraken": 4},
        prior_peak=None,
        repair_from=repair_from,
        raw_execution_watermarks={"kraken": 1},
        execution_manifest_digest=_MANIFEST,
        has_fill_gap=False,
        expected_venues=frozenset({"kraken"}),
        venue_event_watermarks={"kraken:spot": 10},
        fill_witness_digest=_WITNESS_DIGEST,
    )
    repo._frontiers[key] = frontier
    return frontier


async def test_repair_from_lag_emits_once_above_24h_and_rearms_per_distinct_value(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Emit one structured warning per scope and distinct repair_from beyond 24 hours.

    Given: A scope whose repair_from lags frontier.tip by 24 h, then >24 h across
        multiple tips, then moves to another repair_from >24 h behind, then clears.
    When: _sample_scope inspects each frontier across ticks and _reset_scope_logs runs.
    Then: Exactly 24 h emits nothing; >24 h emits once per distinct repair_from
        carrying repair_from_lag and scope extras, and clears when repair_from resolves.
    """
    repo = series_fixtures._FakeRepo(
        credentials=[series_fixtures._cred("kraken")],
        anchor=series_fixtures._anchor(),
    )
    boundary_tip = _M1 + timedelta(hours=24)
    _seed_frontier_with_repair(repo, _M1, boundary_tip)
    now = boundary_tip + timedelta(minutes=2)
    snap = PortfolioPnlSnapshotter(
        repo=cast(SQLAlchemyRepository, repo),
        interval_seconds=60,
        disabled=False,
        clock=lambda: now,
    )
    series_fixtures._install_series(monkeypatch, first=series_fixtures._meta(seq=4))
    with caplog.at_level("WARNING", logger=pnl_snapshotter.__name__):
        await snap._tick_once()
        assert _lag_records(caplog) == []

        now = boundary_tip + timedelta(minutes=3)
        _seed_frontier_with_repair(repo, _M1, boundary_tip + timedelta(minutes=1))
        await snap._tick_once()
        records = _lag_records(caplog)
        assert len(records) == 1
        assert getattr(records[0], "repair_from_lag") == timedelta(hours=24, minutes=1)
        assert getattr(records[0], "wallet_public_id") == series_fixtures._WALLET
        assert getattr(records[0], "repair_from") == _M1

        snap._reset_scope_logs(series_fixtures._WALLET)
        now = boundary_tip + timedelta(minutes=4)
        await snap._tick_once()
        assert len(_lag_records(caplog)) == 1

        now = boundary_tip + timedelta(minutes=5)
        _seed_frontier_with_repair(repo, _M2, boundary_tip + timedelta(minutes=3))
        await snap._tick_once()
        records = _lag_records(caplog)
        assert len(records) == 2
        assert getattr(records[1], "repair_from") == _M2
        assert getattr(records[1], "repair_from_lag") == timedelta(hours=24, minutes=2)

        now = boundary_tip + timedelta(minutes=6)
        frontier = repo.frontier()
        repo._frontiers[frontier.key] = replace(frontier, repair_from=None)
        await snap._tick_once()
        assert snap._repair_from_lag_logged == {}


def test_repair_from_lag_rearms_after_dropping_to_the_threshold_at_the_same_repair_from() -> None:
    """Log again once a lag that fell back to 24 h exceeds it at the same repair_from.

    Given: One scope whose repair_from stays at M1 while frontier.tip moves to 24 h 1 m,
        then back to exactly 24 h, then to 24 h 2 m behind it.
    When: _log_repair_from_lag inspects each frontier in turn.
    Then: The first breach logs, the drop to the threshold clears the scope entry
        without logging, and the second breach logs again for the same repair_from.
    """
    repo = series_fixtures._FakeRepo(
        credentials=[series_fixtures._cred("kraken")],
        anchor=series_fixtures._anchor(),
    )
    as_of = _M1 + timedelta(hours=24, minutes=10)
    snap = series_fixtures._snapshotter(repo, as_of)
    ctx = snap._build_scope_context(
        series_fixtures._WALLET,
        series_fixtures._anchor(),
        frozenset({"kraken"}),
        as_of,
    )
    scope_key = snap._scope_streak_key(ctx.scope)
    lags = (
        timedelta(hours=24, minutes=1),
        timedelta(hours=24),
        timedelta(hours=24, minutes=2),
    )
    logged: list[bool] = []
    for lag in lags:
        before = dict(snap._repair_from_lag_logged)
        frontier = _seed_frontier_with_repair(repo, _M1, _M1 + lag)
        snap._log_repair_from_lag(ctx, frontier)
        logged.append(scope_key in snap._repair_from_lag_logged and scope_key not in before)
    assert logged == [True, False, True]
    assert snap._repair_from_lag_logged == {scope_key: _M1}


def test_repair_from_lag_log_drops_entries_of_a_superseded_epoch() -> None:
    """Forget a re-anchored scope's earlier-epoch lag entry while keeping other scopes.

    Given: Lag entries for this wallet under an earlier epoch and for another wallet.
    When: _log_repair_from_lag runs for this wallet's current epoch, first without a
        repair_from and then with a lag beyond 24 h.
    Then: The earlier-epoch entry is dropped at once, the other wallet's entry
        survives, and only the current epoch's entry is recorded for this wallet.
    """
    repo = series_fixtures._FakeRepo(
        credentials=[series_fixtures._cred("kraken")],
        anchor=series_fixtures._anchor(),
    )
    as_of = _M1 + timedelta(hours=24, minutes=10)
    snap = series_fixtures._snapshotter(repo, as_of)
    ctx = snap._build_scope_context(
        series_fixtures._WALLET,
        series_fixtures._anchor(),
        frozenset({"kraken"}),
        as_of,
    )
    current_key = snap._scope_streak_key(ctx.scope)
    wallet, mode, ccy, epoch = current_key
    stale_key = (wallet, mode, ccy, "0000dead-0000-7000-8000-0000000000e1")
    other_key = ("0000face-0000-7000-8000-0000000000b9", mode, ccy, epoch)
    snap._repair_from_lag_logged = {stale_key: _M1, other_key: _M2}

    snap._log_repair_from_lag(ctx, None)
    assert snap._repair_from_lag_logged == {other_key: _M2}

    snap._repair_from_lag_logged[stale_key] = _M1
    frontier = _seed_frontier_with_repair(repo, _M1, _M1 + timedelta(hours=24, minutes=1))
    snap._log_repair_from_lag(ctx, frontier)
    assert snap._repair_from_lag_logged == {other_key: _M2, current_key: _M1}


async def test_repair_from_lag_fires_on_advance_replay_commit_and_prunes_absent_scopes(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Fire when an in-flight chunk creates a >24 h lag and prune absent scopes.

    Given: Two wallets where an active chunk withholds M1 inside a >24 h window.
    When: _advance_replay commits the chunk and _prune_absent_scopes drops a wallet.
    Then: The committed chunk emits one lag warning and absent scopes are pruned.
    """
    second_wallet = "0000face-0000-7000-8000-0000000000b9"
    tip = _M1 + timedelta(hours=24, minutes=5)
    repo = series_fixtures._FakeRepo(
        credentials=[series_fixtures._cred("kraken")],
        anchor=series_fixtures._anchor(),
    )
    frontier = _seed_frontier_with_repair(repo, _M1, tip, next_minute=_M1)
    repo._frontiers[frontier.key] = replace(frontier, repair_from=None)
    as_of = tip + timedelta(minutes=2)
    snap = series_fixtures._snapshotter(repo, as_of)
    ctx = snap._build_scope_context(
        series_fixtures._WALLET,
        series_fixtures._anchor(),
        frozenset({"kraken"}),
        as_of,
    )

    async def untrusted_series(*args: object, **kwargs: object) -> PnlWalletSeriesResult:
        """Return an untrusted window starting at M1."""
        del kwargs
        from_time, to_time = cast(datetime, args[3]), cast(datetime, args[4])
        return series_fixtures._series(
            from_time,
            to_time,
            metadata=series_fixtures._meta(seq=4),
            untrusted=True,
        )

    monkeypatch.setattr(pnl_snapshotter, "build_wallet_pnl_series", untrusted_series)
    with caplog.at_level("WARNING", logger=pnl_snapshotter.__name__):
        await snap._advance_replay(cast(SQLAlchemyRepository, repo), ctx, repo.frontier())
    assert len(_lag_records(caplog)) == 1
    snap._repair_from_lag_logged[(second_wallet, "live", "USD", series_fixtures._EPOCH)] = _M1
    snap._prune_absent_scopes({series_fixtures._WALLET})
    expected_key = snap._scope_streak_key(series_fixtures._sample_scope())
    assert set(snap._repair_from_lag_logged) == {expected_key}
    snap._prune_absent_scopes(set())
    assert snap._repair_from_lag_logged == {}
