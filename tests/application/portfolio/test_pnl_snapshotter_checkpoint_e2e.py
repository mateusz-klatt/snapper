"""Real-SQLite snapshotter checkpoint differentials across off, shadow, and active."""

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime
from datetime import timedelta
from pathlib import Path
from typing import Literal

import pytest
from sqlalchemy import update

from snapper.application.portfolio import pnl_engine_options
from snapper.application.portfolio import pnl_snapshotter
from snapper.application.portfolio import pnl_snapshotter_checkpoint as dispatch
from snapper.application.portfolio.pnl_engine_options import PnlEngineOptions
from snapper.application.portfolio.pnl_opening_checkpoint_metrics import checkpoint_metrics
from snapper.application.portfolio.pnl_opening_checkpoint_metrics import reset_checkpoint_metrics
from snapper.application.portfolio.pnl_snapshot_planner import ChunkWindow
from snapper.application.portfolio.pnl_snapshotter import PortfolioPnlSnapshotter
from snapper.application.portfolio.pnl_snapshotter import _CheckpointReplayOptions
from snapper.application.portfolio.pnl_snapshotter import _ScopeContext
from snapper.application.portfolio.pnl_timeline_service import PnlWalletSeriesResult
from snapper.data.models import Execution
from snapper.data.models import PortfolioPnlOpeningCheckpoint
from snapper.data.models import VenueEvent
from snapper.data.pnl_correction_state import PnlCorrectionFrontier
from snapper.data.pnl_correction_state import PnlOpeningCheckpointRecord
from snapper.data.repository import PortfolioPnlSampleQuery
from snapper.data.repository import PortfolioPnlSampleScope
from snapper.data.repository import Repository
from snapper.data.repository import SQLAlchemyRepository
from snapper.data.repository_types import PNL_SAMPLE_CALC_VERSION
from snapper.data.repository_types import PnlTimelineExecutionPrefix
from snapper.data.repository_types import PnlTimelineFillWitnessCut
from snapper.data.repository_types import PortfolioPnlSampleRow
from tests.application.portfolio import test_pnl_snapshotter_continuation as continuation
from tests.application.portfolio import test_pnl_snapshotter_lower_id_rewind as idless
from tests.data import test_pnl_opening_checkpoint_repository as checkpoint_repository
from tests.data import test_pnl_timeline_execution_prefix_repository as prefix
from tests.data import test_pnl_timeline_sample_repository as samples

type _Scenario = Literal[
    "quiet", "buy_sell", "deferred", "multi_chunk", "idless", "invalidate", "age", "suffix_size"
]

_MINUTE = timedelta(minutes=1)
_TICK1_AS_OF = samples._T0 + 3 * _MINUTE
_TICK2_AS_OF = continuation._AS_OF
_ROW_PROVENANCE = frozenset({"public_id", "session_id", "sequence_id", "timestamp"})


@dataclass(frozen=True)
class _RunOutcome:
    """Committed rows and per-tick replay/prefix-read counts from one real run."""

    rows: tuple[dict[str, object], ...]
    replay_calls: tuple[int, ...]
    prefix_reads: tuple[int, ...]


def _permit_capture(monkeypatch: pytest.MonkeyPatch) -> None:
    """Enable the prospective 5C.6 engine behavior through the test-only seam."""
    monkeypatch.setattr(
        pnl_engine_options,
        "_engine_options_table",
        lambda: {PNL_SAMPLE_CALC_VERSION: PnlEngineOptions(True, True, True)},
    )


def _query(scope: PortfolioPnlSampleScope) -> PortfolioPnlSampleQuery:
    """Build the exact read query for one seeded anchor scope."""
    return PortfolioPnlSampleQuery(
        scope.wallet_public_id,
        scope.mode,
        scope.valuation_ccy,
        scope.epoch_public_id,
        PNL_SAMPLE_CALC_VERSION,
    )


def _canonical_rows(rows: list[PortfolioPnlSampleRow]) -> tuple[dict[str, object], ...]:
    """Project committed rows to exact financial values, reasons, and watermarks."""
    return tuple(
        {key: value for key, value in row.items() if key not in _ROW_PROVENANCE} for row in rows
    )


def _matched_execution(
    sequence: int,
    minute: int,
    side: str,
    effective_minute: int | None = None,
) -> tuple[Execution, VenueEvent]:
    """Build one direct-order execution and its identity-bound fill witness."""
    execution = prefix._execution("kraken", sequence, samples._T0 + minute * _MINUTE)
    execution.side = side
    if effective_minute is not None:
        execution.executed_at = samples._T0 + effective_minute * _MINUTE
    event = prefix._fill_event(
        prefix._CLIENT_ORDER,
        prefix._KRAKEN_SHARD,
        sequence_id=sequence,
        execution_scope_sequence=sequence,
    )
    event.side = side
    event.exec_id = execution.exec_id
    event.trade_id = execution.trade_id
    event.timestamp = event.received_at = execution.timestamp
    event.venue_timestamp = execution.executed_at or execution.timestamp
    return execution, event


async def _add_prefix_deferred(repo: SQLAlchemyRepository) -> None:
    """Seed a known execution before tick one whose effective event crosses C."""
    execution, event = _matched_execution(2, 1, "buy", 4)
    async with repo.session() as session:
        session.add_all((execution, event))
        await session.commit()


async def _age_checkpoint(repo: SQLAlchemyRepository) -> None:
    """Age only the stored certificate header so the next tick recaptures before verification."""
    async with repo.session() as session:
        await session.execute(
            update(PortfolioPnlOpeningCheckpoint).values(
                checkpoint_minute=samples._T0
                + _MINUTE
                - timedelta(minutes=dispatch.RECAPTURE_AGE_MINUTES)
            )
        )
        await session.commit()


async def _add_suffix(
    repo: SQLAlchemyRepository,
    scenario: _Scenario,
) -> None:
    """Mutate real suffix evidence or a certified prefix accrual between ticks."""
    async with repo.session() as session:
        if scenario in ("buy_sell", "suffix_size"):
            for sequence, minute, side in ((2, 2, "buy"), (3, 3, "sell")):
                session.add_all(_matched_execution(sequence, minute, side))
        elif scenario == "idless":
            order = prefix._order(
                public_id="00000000-0000-7000-8000-000000000c02",
                client_order_id="client-kraken-suffix",
            )
            execution, event = idless._fill(2)
            execution.order_public_id = order.public_id
            event.client_order_id = event.venue_client_id = order.client_order_id
            session.add(order)
            session.add_all((execution, event))
        elif scenario == "invalidate":
            accrual = checkpoint_repository._accrual_orm(
                samples._T0 + _MINUTE,
                1,
                timestamp=_TICK1_AS_OF,
            )
            accrual.instrument_public_id = prefix._INSTRUMENT
            session.add(accrual)
        await session.commit()


async def _tick_with_spies(
    repo: SQLAlchemyRepository,
    as_of: datetime,
    monkeypatch: pytest.MonkeyPatch,
) -> tuple[int, int]:
    """Run one real tick while counting replay calls and reads inside replay."""
    original_prefix = repo.get_pnl_timeline_execution_prefix_at_watermarks
    calls = 0
    prefix_reads = 0
    in_replay = False

    async def prefix_read(
        wallet_public_id: str,
        read_mode: str,
        watermarks: Mapping[str, int],
        read_as_of: datetime,
        *,
        fill_witness_cut: PnlTimelineFillWitnessCut | Literal["capture"] | None = None,
    ) -> PnlTimelineExecutionPrefix:
        """Count full-prefix reads only while the replay engine is active."""
        nonlocal prefix_reads
        if in_replay:
            prefix_reads += 1
        return await original_prefix(
            wallet_public_id,
            read_mode,
            watermarks,
            read_as_of,
            fill_witness_cut=fill_witness_cut,
        )

    async def replay(
        replay_repo: Repository,
        ctx: _ScopeContext,
        chunk: ChunkWindow,
        frontier: PnlCorrectionFrontier,
        checkpoint: _CheckpointReplayOptions | None = None,
    ) -> tuple[ChunkWindow, PnlWalletSeriesResult]:
        """Count replay calls and delimit their internal prefix reads."""
        nonlocal calls, in_replay
        calls += 1
        in_replay = True
        try:
            return await original_replay(replay_repo, ctx, chunk, frontier, checkpoint)
        finally:
            in_replay = False

    snap = PortfolioPnlSnapshotter(repo=repo, disabled=False, clock=lambda: as_of)
    original_replay = snap._replay_series
    monkeypatch.setattr(repo, "get_pnl_timeline_execution_prefix_at_watermarks", prefix_read)
    monkeypatch.setattr(snap, "_replay_series", replay)
    await snap._tick_once()
    return calls, prefix_reads


def _assert_checkpoint(
    record: object,
    mode: str,
    scenario: _Scenario,
    initial: bool,
) -> None:
    """Assert absence off and the expected certificate minute in enabled modes."""
    if mode == "off":
        assert record is None
        return
    assert isinstance(record, PnlOpeningCheckpointRecord)
    recaptured = scenario in ("invalidate", "age", "suffix_size")
    expected = samples._T0 + _MINUTE if initial or not recaptured else continuation._TIP
    assert record.certificate.checkpoint_minute == expected


async def _run_mode(
    mode: str,
    path: Path,
    monkeypatch: pytest.MonkeyPatch,
    scenario: _Scenario,
) -> _RunOutcome:
    """Run real ticks with per-tick replay and internal prefix-read spies."""
    if scenario == "multi_chunk":
        monkeypatch.setattr(pnl_snapshotter, "MAX_CORRECTION_CHUNK_MINUTES", 2)
    if scenario == "suffix_size":
        monkeypatch.setattr(dispatch, "RECAPTURE_SUFFIX_FILLS", 1)
    path.parent.mkdir(parents=True, exist_ok=True)
    url = f"sqlite+aiosqlite:///{path.as_posix()}"
    scope = await idless._seed(url, complete=False)
    monkeypatch.setenv("PNL_SNAPSHOTTER_CHECKPOINT_MODE", mode)
    monkeypatch.delenv("PNL_FX_SHADOW_PINNING_ENABLED", raising=False)
    async with idless._repository(url) as repo:
        if scenario == "deferred":
            await _add_prefix_deferred(repo)
        replay_calls: list[int] = []
        prefix_reads: list[int] = []
        calls, reads = await _tick_with_spies(repo, _TICK1_AS_OF, monkeypatch)
        replay_calls.append(calls)
        prefix_reads.append(reads)
        initial = await repo.get_portfolio_pnl_opening_checkpoint(scope)
        _assert_checkpoint(initial, mode, scenario, True)
        if scenario == "age" and mode != "off":
            await _age_checkpoint(repo)
        await _add_suffix(repo, scenario)
        for _ in range(4):
            calls, reads = await _tick_with_spies(repo, _TICK2_AS_OF, monkeypatch)
            replay_calls.append(calls)
            prefix_reads.append(reads)
        rows = await repo.get_portfolio_pnl_samples(
            _query(scope),
            samples._T0 + _MINUTE,
            continuation._TIP,
        )
        record = await repo.get_portfolio_pnl_opening_checkpoint(scope)
        _assert_checkpoint(record, mode, scenario, False)
        return _RunOutcome(_canonical_rows(rows), tuple(replay_calls), tuple(prefix_reads))


async def test_real_quiet_resume_matches_off_and_uses_one_replay(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Prove real resumed and recaptured rows equal off-mode committed rows.

    Given: A real anchored SQLite scope with one prefix fill and no suffix fills.
    When: Tick one captures and tick two advances the quiet generation in each mode.
    Then: Every committed financial field is identical and active resume succeeds.

    Args:
        tmp_path: Private test directory.
        monkeypatch: Environment and engine-options seam.
    """
    _permit_capture(monkeypatch)
    baseline: tuple[dict[str, object], ...] | None = None
    for mode in ("off", "shadow", "active"):
        reset_checkpoint_metrics()
        outcome = await _run_mode(mode, tmp_path / f"{mode}.db", monkeypatch, "quiet")
        rows = outcome.rows
        assert len(rows) == 5
        if baseline is None:
            baseline = rows
        else:
            assert rows == baseline
        if mode == "active":
            assert outcome.replay_calls == (1, 1, 0, 0, 0)
            assert outcome.prefix_reads[1] == 0
            metrics = checkpoint_metrics()
            assert metrics.captures_committed == 1
            assert metrics.resumes_attempted == 1
            assert metrics.resumes_succeeded == 1
            assert metrics.recaptures == {"absent": 1}


async def _assert_differential(
    scenario: _Scenario,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Run identical fresh seeds through off, shadow, and active and compare rows."""
    _permit_capture(monkeypatch)
    baseline: tuple[dict[str, object], ...] | None = None
    for mode in ("off", "shadow", "active"):
        reset_checkpoint_metrics()
        outcome = await _run_mode(
            mode,
            tmp_path / scenario / f"{mode}.db",
            monkeypatch,
            scenario,
        )
        rows = outcome.rows
        assert len(rows) == 5
        if baseline is None:
            baseline = rows
        else:
            assert rows == baseline


@pytest.mark.parametrize("scenario", ["buy_sell", "deferred", "multi_chunk", "idless"])
async def test_real_suffix_scenarios_match_off(
    scenario: _Scenario,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Compare real ordinary, deferred, chunked, and ID-less suffix replay.

    Given: Identical real SQLite evidence for one suffix scenario.
    When: Off, shadow, and active run tick one and the forward generation.
    Then: All committed points, reasons, audit, and watermarks are identical.

    Args:
        scenario: Real evidence mutation to exercise.
        tmp_path: Private scenario directory.
        monkeypatch: Engine seam, mode, and chunk-limit controls.
    """
    await _assert_differential(scenario, tmp_path, monkeypatch)


async def test_real_prefix_accrual_change_falls_back_deletes_and_matches_off(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Retire a checkpoint when a prefix accrual changes after certification.

    Given: Tick one captured C and a new real accrual at C arrives before tick two.
    When: Active verifies the next generation and replays from the anchor.
    Then: Fallback and fenced deletion counters advance and rows equal off mode.

    Args:
        tmp_path: Private scenario directory.
        monkeypatch: Engine seam and checkpoint-mode controls.
    """
    await _assert_differential("invalidate", tmp_path, monkeypatch)
    metrics = checkpoint_metrics()
    assert metrics.fallbacks == {
        "checkpoint_absent": 1,
        "checkpoint_accrual_prefix_changed": 1,
    }
    assert metrics.invalidations_deleted == 1
    assert metrics.recaptures == {"absent": 1, "fallback": 1}


@pytest.mark.parametrize(("scenario", "reason"), [("age", "age"), ("suffix_size", "suffix_size")])
async def test_real_recapture_triggers_use_one_anchor_replay_and_match_off(
    scenario: _Scenario,
    reason: str,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Recapture by age or suffix size through one real anchor replay.

    Given: An initially captured checkpoint that crosses the selected recapture bound.
    When: Off, shadow, and active process the same next generation.
    Then: Rows match off and active records one recapture with a new tip certificate.

    Args:
        scenario: Real age or suffix-size trigger fixture.
        reason: Expected closed recapture metric key.
        tmp_path: Private scenario directory.
        monkeypatch: Engine seam and trigger controls.
    """
    await _assert_differential(scenario, tmp_path, monkeypatch)
    metrics = checkpoint_metrics()
    assert metrics.recaptures == {"absent": 1, reason: 1}
    assert metrics.captures_committed == 2
    assert metrics.resumes_attempted == 0
