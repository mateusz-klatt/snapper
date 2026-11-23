"""Real-SQLite snapshotter checkpoint differentials across off, shadow, and active."""

import logging
import math
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime
from datetime import timedelta
from pathlib import Path
from typing import Literal

import pytest
from sqlalchemy import update
from sqlalchemy.exc import SQLAlchemyError

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
from snapper.data.pnl_correction_state import IdAggregate
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
from tests.data import test_pnl_crypto_usd_plane_repository as prices
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
    assert metrics.invalidations_deleted == {"generation": 1}
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


async def _add_age_frontier_evidence(repo: SQLAlchemyRepository, age_tip: datetime) -> None:
    """Advance real market and observation evidence to the recapture age horizon."""
    async with repo.session() as session:
        observation = samples._observation(
            timestamp=age_tip,
            balances_json='[{"currency":"USD","total":1000},{"currency":"BTC","total":2}]',
        )
        observation.sequence_id = 10
        observation.public_id = "00000000-0000-7000-8000-000000000999"
        session.add(observation)
        session.add(
            prices._candle(
                prefix._INSTRUMENT,
                age_tip - _MINUTE,
                115.0,
                spec=prices._CandleSpec(
                    timestamp=age_tip,
                    public_id="00000000-0000-7000-8000-000000000998",
                ),
            )
        )
        await session.commit()


async def _run_three_tick_lifecycle(
    mode: Literal["off", "active"],
    path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> tuple[dict[str, object], ...]:
    """Execute tick 1 capture, tick 2 suffix resume, and tick 3 age recapture on SQLite."""
    path.parent.mkdir(parents=True, exist_ok=True)
    url = f"sqlite+aiosqlite:///{path.as_posix()}"
    scope = await idless._seed(url, complete=False)
    monkeypatch.setenv("PNL_SNAPSHOTTER_CHECKPOINT_MODE", mode)
    monkeypatch.delenv("PNL_FX_SHADOW_PINNING_ENABLED", raising=False)
    capture_tip = samples._T0 + _MINUTE
    age_tip = capture_tip + timedelta(minutes=dispatch.RECAPTURE_AGE_MINUTES)
    age_as_of = age_tip + 2 * _MINUTE
    async with idless._repository(url) as repo:
        calls1, reads1 = await _tick_with_spies(repo, _TICK1_AS_OF, monkeypatch)
        assert calls1 == 1
        assert reads1 == (1 if mode == "off" else 2)
        await _add_suffix(repo, "buy_sell")
        calls2, reads2 = await _tick_with_spies(repo, _TICK2_AS_OF, monkeypatch)
        assert calls2 == 1
        assert reads2 == (1 if mode == "off" else 0)
        if mode == "active":
            c1 = await repo.get_portfolio_pnl_opening_checkpoint(scope)
            assert c1 is not None and c1.certificate.checkpoint_minute == capture_tip
        await _add_age_frontier_evidence(repo, age_tip)
        calls3, reads3 = await _tick_with_spies(repo, age_as_of, monkeypatch)
        assert calls3 == 1
        assert reads3 == (1 if mode == "off" else 2)
        final_record = await repo.get_portfolio_pnl_opening_checkpoint(scope)
        if mode == "off":
            assert final_record is None
        else:
            assert final_record is not None
            assert final_record.certificate.checkpoint_minute == age_tip
        rows = await repo.get_portfolio_pnl_samples(_query(scope), capture_tip, age_tip)
    return _canonical_rows(rows)


async def test_real_sqlite_three_tick_capture_resume_and_age_recapture_equal_cold(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Prove 3-tick real SQLite capture, resume, and age recapture match pure cold runs.

    Given: A real SQLite scope where tick 1 anchors and captures, tick 2 adds suffix
        fills and resumes, and tick 3 crosses the unpatched age bound for recapture.
    When: Both ``off`` and ``active`` modes run all three ticks with only permit and
        mode constants patched in test scope.
    Then: Active resumes cleanly on tick 2, recaptures on tick 3 via age, and every
        persisted sample row across all 1,441 minutes equals the pure-cold run.

    Args:
        tmp_path: Private temporary directory for isolated SQLite files.
        monkeypatch: Pytest fixture patching only engine permit and mode constants.
    """
    _permit_capture(monkeypatch)
    reset_checkpoint_metrics()
    off_rows = await _run_three_tick_lifecycle("off", tmp_path / "cold.db", monkeypatch)
    reset_checkpoint_metrics()
    active_rows = await _run_three_tick_lifecycle("active", tmp_path / "active.db", monkeypatch)
    assert len(off_rows) == dispatch.RECAPTURE_AGE_MINUTES + 1
    assert active_rows == off_rows
    metrics = checkpoint_metrics()
    assert metrics.captures_committed == 2
    assert metrics.resumes_attempted == 1
    assert metrics.resumes_succeeded == 1
    assert metrics.recaptures == {"absent": 1, "age": 1}


def _assert_value_bit_equal(expected: object, actual: object) -> None:
    """Assert two canonical row field values are bit-identical including NaN and -0.0."""
    if isinstance(expected, float) and isinstance(actual, float):
        if math.isnan(expected) or math.isnan(actual):
            assert math.isnan(expected) and math.isnan(actual)
            return
        assert expected.hex() == actual.hex()
        return
    assert expected == actual


def _assert_rows_bit_equal(
    expected: tuple[dict[str, object], ...],
    actual: tuple[dict[str, object], ...],
) -> None:
    """Assert two projected sample sequences match bit-for-bit at every minute and field."""
    assert len(actual) == len(expected)
    for expected_row, actual_row in zip(expected, actual, strict=True):
        assert actual_row.keys() == expected_row.keys()
        for key, expected_value in expected_row.items():
            _assert_value_bit_equal(expected_value, actual_row[key])


async def _add_valuation_minutes(
    repo: SQLAlchemyRepository,
    start_offset: int,
    end_offset: int,
) -> None:
    """Append continuous BTC/USD observations and candles across minute offsets."""
    async with repo.session() as session:
        for offset in range(start_offset, end_offset + 1):
            minute = samples._T0 + offset * _MINUTE
            observation = samples._observation(
                timestamp=minute,
                balances_json='[{"currency":"USD","total":1000},{"currency":"BTC","total":2}]',
            )
            observation.sequence_id = offset
            observation.public_id = f"00000000-0000-7000-8000-{offset + 300:012x}"
            session.add(observation)
            session.add(
                prices._candle(
                    prefix._INSTRUMENT,
                    minute - _MINUTE,
                    100.0 + offset,
                    spec=prices._CandleSpec(
                        timestamp=minute,
                        public_id=f"00000000-0000-7000-8000-{offset + 400:012x}",
                    ),
                )
            )
        await session.commit()


async def _add_flip_suffix(repo: SQLAlchemyRepository) -> None:
    """Append valuation minutes 7..10 plus suffix buy and sell fills after checkpoint C."""
    await _add_valuation_minutes(repo, 7, 10)
    async with repo.session() as session:
        for sequence, minute, side in ((2, 7, "buy"), (3, 9, "sell")):
            session.add_all(_matched_execution(sequence, minute, side))
        await session.commit()


async def _run_catchup_flip_sequence(
    path: Path,
    monkeypatch: pytest.MonkeyPatch,
    *,
    flip_active: bool,
) -> _RunOutcome:
    """Run two cold chunks, optional active flip on chunk three, and two suffix chunks."""
    monkeypatch.setattr(pnl_snapshotter, "MAX_CORRECTION_CHUNK_MINUTES", 2)
    monkeypatch.delenv("PNL_FX_SHADOW_PINNING_ENABLED", raising=False)
    path.parent.mkdir(parents=True, exist_ok=True)
    url = f"sqlite+aiosqlite:///{path.as_posix()}"
    scope = await idless._seed(url, complete=False)
    monkeypatch.setenv("PNL_SNAPSHOTTER_CHECKPOINT_MODE", "off")
    replay_calls: list[int] = []
    prefix_reads: list[int] = []
    as_of_phase1 = samples._T0 + 8 * _MINUTE
    as_of_phase2 = samples._T0 + 12 * _MINUTE
    async with idless._repository(url) as repo:
        await _add_valuation_minutes(repo, 6, 6)
        for _ in range(2):
            calls, reads = await _tick_with_spies(repo, as_of_phase1, monkeypatch)
            replay_calls.append(calls)
            prefix_reads.append(reads)
        assert await repo.get_portfolio_pnl_opening_checkpoint(scope) is None
        if flip_active:
            monkeypatch.setenv("PNL_SNAPSHOTTER_CHECKPOINT_MODE", "active")
        calls, reads = await _tick_with_spies(repo, as_of_phase1, monkeypatch)
        replay_calls.append(calls)
        prefix_reads.append(reads)
        checkpoint_after_flip = await repo.get_portfolio_pnl_opening_checkpoint(scope)
        if flip_active:
            assert isinstance(checkpoint_after_flip, PnlOpeningCheckpointRecord)
            assert checkpoint_after_flip.certificate.checkpoint_minute == samples._T0 + 6 * _MINUTE
        else:
            assert checkpoint_after_flip is None
        await _add_flip_suffix(repo)
        for _ in range(2):
            calls, reads = await _tick_with_spies(repo, as_of_phase2, monkeypatch)
            replay_calls.append(calls)
            prefix_reads.append(reads)
        rows = await repo.get_portfolio_pnl_samples(
            _query(scope),
            samples._T0 + _MINUTE,
            samples._T0 + 10 * _MINUTE,
        )
    return _RunOutcome(_canonical_rows(rows), tuple(replay_calls), tuple(prefix_reads))


async def test_real_sqlite_mid_catchup_flip_captures_and_resumes_bit_equal_to_cold(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Prove mid-catch-up flip under unpatched 5C.6 matches pure-cold bit-for-bit.

    Given: An unpatched 5C.6 SQLite scope that advances two chunks in ``off`` mode.
    When: ``PNL_SNAPSHOTTER_CHECKPOINT_MODE`` flips to ``active`` before chunk three at C,
        and two suffix chunks follow after appending fills and candles.
    Then: Chunk three captures at C, both suffix chunks resume without prefix reads,
        and all 10 persisted sample rows equal a pure-cold run bit-for-bit.

    Args:
        tmp_path: Private temporary directory for isolated SQLite databases.
        monkeypatch: Pytest fixture managing chunk size and mode environment variables.
    """
    reset_checkpoint_metrics()
    cold = await _run_catchup_flip_sequence(
        tmp_path / "cold.db",
        monkeypatch,
        flip_active=False,
    )
    assert checkpoint_metrics().captures_committed == 0
    assert checkpoint_metrics().resumes_attempted == 0

    reset_checkpoint_metrics()
    flipped = await _run_catchup_flip_sequence(
        tmp_path / "flipped.db",
        monkeypatch,
        flip_active=True,
    )
    assert len(cold.rows) == 10
    _assert_rows_bit_equal(cold.rows, flipped.rows)
    assert flipped.replay_calls == (1, 1, 1, 1, 1)
    assert flipped.prefix_reads == (1, 1, 2, 0, 0)
    metrics = checkpoint_metrics()
    assert metrics.captures_committed == 1
    assert metrics.resumes_attempted == 2
    assert metrics.resumes_succeeded == 2
    assert metrics.capture_refusals == {}


async def test_real_sqlite_shadow_mode_compares_persists_cold_and_reports_zero_mismatches(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Prove unpatched shadow mode compares suffix resumes against cold with zero mismatches.

    Given: Identical real SQLite histories with suffix buy/sell fills across two ticks.
    When: One runs under unpatched ``off`` mode and the other under unpatched ``shadow`` mode.
    Then: Shadow compares and commits without error, reports ``shadow_mismatches == 0``,
        and persists sample rows bit-identical to pure cold.

    Args:
        tmp_path: Private temporary directory for isolated SQLite databases.
        monkeypatch: Pytest fixture setting the checkpoint mode environment variable.
        caplog: Log capture verifying no errors or shadow mismatches are emitted.
    """
    monkeypatch.delenv("PNL_FX_SHADOW_PINNING_ENABLED", raising=False)
    reset_checkpoint_metrics()
    cold_outcome = await _run_mode("off", tmp_path / "shadow_cold.db", monkeypatch, "buy_sell")
    reset_checkpoint_metrics()
    with caplog.at_level(logging.WARNING, logger="snapper"):
        shadow_outcome = await _run_mode(
            "shadow",
            tmp_path / "shadow_live.db",
            monkeypatch,
            "buy_sell",
        )
    assert len(cold_outcome.rows) == 5
    _assert_rows_bit_equal(cold_outcome.rows, shadow_outcome.rows)
    metrics = checkpoint_metrics()
    assert metrics.captures_committed == 1
    assert metrics.resumes_attempted >= 1
    assert metrics.resumes_succeeded == metrics.resumes_attempted
    assert metrics.shadow_comparisons >= 1
    assert metrics.shadow_mismatches == 0
    assert metrics.shadow_skipped == {}
    assert not [record for record in caplog.records if record.levelno >= logging.ERROR]


async def test_real_sqlite_capture_db_error_rolls_back_and_commits_anchor(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Keep the authoritative commit usable after a capture evidence DB failure.

    Given: A real active-mode capture whose accrual fingerprint read raises SQLAlchemyError.
    When: The capture session rolls back and the snapshotter commits its anchor plan.
    Then: Samples and frontier persist without a checkpoint, and the refusal is counted.

    Args:
        tmp_path: Private test directory for the real SQLite database.
        monkeypatch: Environment, capture-permission, and repository failure seams.
    """
    _permit_capture(monkeypatch)
    reset_checkpoint_metrics()
    url = f"sqlite+aiosqlite:///{(tmp_path / 'capture_db_error.db').as_posix()}"
    scope = await idless._seed(url, complete=False)
    monkeypatch.setenv("PNL_SNAPSHOTTER_CHECKPOINT_MODE", "active")
    monkeypatch.delenv("PNL_FX_SHADOW_PINNING_ENABLED", raising=False)
    async with idless._repository(url) as repo:

        async def fail_fingerprint(
            wallet_public_id: str,
            mode: str,
            after_exclusive: datetime,
            through: datetime,
            as_of: datetime,
        ) -> IdAggregate:
            """Simulate a database failure that invalidates only this read transaction."""
            del wallet_public_id, mode, after_exclusive, through, as_of
            raise SQLAlchemyError("capture evidence transaction aborted")

        monkeypatch.setattr(repo, "get_pnl_accrual_prefix_fingerprint", fail_fingerprint)
        snap = PortfolioPnlSnapshotter(repo=repo, disabled=False, clock=lambda: _TICK1_AS_OF)
        await snap._tick_once()

        rows = await repo.get_portfolio_pnl_samples(
            _query(scope),
            samples._T0 + _MINUTE,
            samples._T0 + _MINUTE,
        )
        frontier = await repo.get_portfolio_pnl_correction_frontier(scope)
        assert len(rows) == 1
        assert frontier is not None
        assert frontier.next_minute is None
        assert await repo.get_portfolio_pnl_opening_checkpoint(scope) is None
        assert checkpoint_metrics().capture_refusals == {"capture_evidence_error": 1}
