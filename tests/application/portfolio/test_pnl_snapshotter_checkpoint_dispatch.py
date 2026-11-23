"""Adversarial dispatch tests for snapshotter opening-checkpoint modes."""

import asyncio
import logging
import time
from collections.abc import Awaitable
from collections.abc import Callable
from dataclasses import dataclass
from dataclasses import replace
from datetime import datetime
from datetime import timedelta
from typing import cast
from unittest.mock import AsyncMock

import pytest
from sqlalchemy.exc import IntegrityError
from sqlalchemy.exc import OperationalError
from sqlalchemy.exc import SQLAlchemyError

from snapper.application.portfolio import pnl_engine_options
from snapper.application.portfolio import pnl_snapshotter
from snapper.application.portfolio import pnl_snapshotter_checkpoint as dispatch
from snapper.application.portfolio import pnl_snapshotter_config
from snapper.application.portfolio import pnl_timeline_service
from snapper.application.portfolio.pnl_correction_planner import PlannedPnlCorrection
from snapper.application.portfolio.pnl_engine_options import PnlEngineOptions
from snapper.application.portfolio.pnl_opening_checkpoint_bundle import bundle_from_json
from snapper.application.portfolio.pnl_opening_checkpoint_metrics import (
    CHECKPOINT_SUMMARY_EVERY_TICKS,
)
from snapper.application.portfolio.pnl_opening_checkpoint_metrics import checkpoint_metrics
from snapper.application.portfolio.pnl_opening_checkpoint_metrics import checkpoint_metrics_by_scope
from snapper.application.portfolio.pnl_opening_checkpoint_metrics import checkpoint_scope_key
from snapper.application.portfolio.pnl_opening_checkpoint_metrics import checkpoint_wallet_alias
from snapper.application.portfolio.pnl_opening_checkpoint_metrics import reset_checkpoint_metrics
from snapper.application.portfolio.pnl_opening_checkpoint_verifier import PnlCheckpointResume
from snapper.application.portfolio.pnl_snapshot_planner import ChunkWindow
from snapper.application.portfolio.pnl_snapshotter import PortfolioPnlSnapshotter
from snapper.application.portfolio.pnl_snapshotter import _CheckpointReplayOptions
from snapper.application.portfolio.pnl_snapshotter import _PlanBasis
from snapper.application.portfolio.pnl_snapshotter import _ScopeContext
from snapper.application.portfolio.pnl_snapshotter_checkpoint import PnlCheckpointDecision
from snapper.application.portfolio.pnl_snapshotter_checkpoint import PnlCheckpointResumeRequest
from snapper.application.portfolio.pnl_timeline import PnlTimelineResult
from snapper.application.portfolio.pnl_timeline import PnlTimelineWorkBudgetError
from snapper.application.portfolio.pnl_timeline_service import PnlFxRateSource
from snapper.application.portfolio.pnl_timeline_service import PnlWalletSeriesResult
from snapper.data.pnl_correction_state import PnlCorrectionChunk
from snapper.data.pnl_correction_state import PnlCorrectionFrontier
from snapper.data.pnl_correction_state import PnlOpeningCheckpointCertificate
from snapper.data.pnl_correction_state import PnlOpeningCheckpointWrite
from snapper.data.repository import PortfolioPnlSampleScope
from snapper.data.repository import Repository
from snapper.data.repository_types import PNL_SAMPLE_CALC_VERSION
from snapper.data.repository_types import PortfolioPnlAnchorRow
from tests.application.portfolio import test_pnl_opening_checkpoint_verifier as verifier
from tests.application.portfolio import test_pnl_snapshotter as fixtures
from tests.application.portfolio import test_pnl_snapshotter_checkpoint_shadow as shadow_cases


def _permit_capture(monkeypatch: pytest.MonkeyPatch) -> None:
    """Permit the production version only inside an individual capture test."""
    monkeypatch.setattr(
        pnl_engine_options,
        "_engine_options_table",
        lambda: {PNL_SAMPLE_CALC_VERSION: PnlEngineOptions(True, False, False)},
    )


def _enable_checkpoint_modes(monkeypatch: pytest.MonkeyPatch) -> None:
    """Widen code-enabled checkpoint modes inside tests exercising shadow or active."""
    monkeypatch.setattr(
        pnl_snapshotter_config,
        "CHECKPOINT_MODES_ENABLED",
        frozenset({"off", "shadow", "active"}),
    )


def _restrict_checkpoint_modes_to_off(monkeypatch: pytest.MonkeyPatch) -> None:
    """Narrow code-enabled checkpoint modes to ``off`` when testing the kill switch."""
    monkeypatch.setattr(
        pnl_snapshotter_config,
        "CHECKPOINT_MODES_ENABLED",
        frozenset({"off"}),
    )


def _disallow_capture(monkeypatch: pytest.MonkeyPatch) -> None:
    """Disallow checkpoint capture for the production calc version under test."""
    monkeypatch.setattr(
        pnl_engine_options,
        "_engine_options_table",
        lambda: {PNL_SAMPLE_CALC_VERSION: PnlEngineOptions(False, True, True)},
    )


def _pending(repo: fixtures._FakeRepo) -> PnlCorrectionFrontier:
    """Return a clean one-minute settling generation."""
    repo.seed_completed_replay(fixtures._minute(2))
    frontier = replace(
        repo.frontier(),
        next_minute=fixtures._minute(3),
        tip=fixtures._minute(3),
    )
    repo._frontiers[frontier.key] = frontier
    return frontier


def _context(
    snap: PortfolioPnlSnapshotter,
    frontier: PnlCorrectionFrontier,
) -> _ScopeContext:
    """Bind the pending generation to the fixture's active anchor."""
    return snap._build_scope_context(
        fixtures._WALLET,
        fixtures._anchor(),
        frozenset({"kraken"}),
        frontier.as_of,
    )


async def _write(
    monkeypatch: pytest.MonkeyPatch,
    frontier: PnlCorrectionFrontier,
) -> PnlOpeningCheckpointWrite:
    """Rebind a real service capture to the snapshotter fixture generation."""
    record, _, _ = await verifier._valid_capture(monkeypatch)
    certificate = replace(
        record.certificate,
        key=frontier.key,
        anchor_public_id=fixtures._anchor()["public_id"],
        source_generation=frontier.generation,
        checkpoint_minute=frontier.tip,
        as_of=frontier.as_of,
        cutoff_execution_watermarks=frontier.raw_execution_watermarks or {},
        execution_manifest_digest=frontier.execution_manifest_digest or "a" * 64,
        source_expected_venues=frontier.expected_venues or frozenset({"kraken"}),
        source_venue_event_watermarks=frontier.venue_event_watermarks or {},
        source_fill_witness_digest=frontier.fill_witness_digest or "c" * 64,
    )
    return PnlOpeningCheckpointWrite(certificate, record.state_json)


@dataclass(frozen=True)
class _ReplayBehavior:
    """Synthetic write and divergence controls for the replay double."""

    write: PnlOpeningCheckpointWrite | None = None
    divergent: bool = False
    split_once: bool = False
    resume_fallback: bool = False
    capture_refusal: str | None = None


def _install_replay(
    monkeypatch: pytest.MonkeyPatch,
    snap: PortfolioPnlSnapshotter,
    frontier: PnlCorrectionFrontier,
    behavior: _ReplayBehavior,
) -> list[str]:
    """Install a deterministic replay double and return its event log."""
    events: list[str] = []
    split = {"pending": behavior.split_once}
    write = behavior.write
    divergent = behavior.divergent
    resume_fallback = behavior.resume_fallback

    async def replay(
        repo: Repository,
        ctx: _ScopeContext,
        chunk: ChunkWindow,
        current: PnlCorrectionFrontier,
        checkpoint: _CheckpointReplayOptions | None = None,
    ) -> tuple[ChunkWindow, PnlWalletSeriesResult]:
        """Return anchor and resumed values with optional capture or divergence."""
        del repo, ctx, current
        checkpoint = checkpoint or _CheckpointReplayOptions()
        capture, resume = checkpoint.capture, checkpoint.resume
        if split["pending"]:
            split["pending"] = False
            chunk = ChunkWindow(chunk.start, chunk.start)
        if capture is not None:
            events.append("resume_capture" if resume is not None else "capture")
        elif resume is not None:
            events.append("resume")
        else:
            events.append("anchor")
        result = fixtures._series(
            chunk.start,
            chunk.end,
            metadata=fixtures._meta(seq=4),
        )
        if resume is not None and divergent:
            result = replace(
                result,
                points=(replace(result.points[0], realized_pnl=9.0), *result.points[1:]),
            )
        refusal = behavior.capture_refusal if capture is not None else None
        staged = write if capture is not None and refusal is None else None
        if capture is not None and (divergent or chunk.end != frontier.tip):
            staged = None
            refusal = "capture_value_error" if divergent else "capture_not_at_tip"
        return chunk, replace(
            result,
            checkpoint_write=staged,
            checkpoint_capture_refusal=refusal,
            checkpoint_resume_fallback=(
                "checkpoint_late_execution" if resume is not None and resume_fallback else None
            ),
        )

    monkeypatch.setattr(snap, "_replay_series", replay)
    return events


@pytest.mark.parametrize("mode", ["off", "shadow", "active"])
@pytest.mark.parametrize("capture_permitted", [False, True])
async def test_off_and_kill_switch_modes_never_read_a_checkpoint(
    monkeypatch: pytest.MonkeyPatch,
    mode: str,
    capture_permitted: bool,
) -> None:
    """Keep off query-free and kill-switch shadow/active inert regardless of capture permission.

    Given: A clean settling chunk with capture either disallowed or unpatched-permitted.
    When: Off runs, or shadow/active runs under a narrowed kill switch, or unpatched
        shadow/active runs with capture disallowed via engine options.
    Then: No checkpoint read or capture write ever occurs when effective mode is ``off``,
        while enabled non-off modes with ``capture_permitted=False`` count permission refusal.
    """
    reset_checkpoint_metrics()
    if not capture_permitted:
        _disallow_capture(monkeypatch)
    if mode != "off" and capture_permitted:
        _restrict_checkpoint_modes_to_off(monkeypatch)
    monkeypatch.setenv("PNL_SNAPSHOTTER_CHECKPOINT_MODE", mode)
    repo = fixtures._FakeRepo()
    frontier = _pending(repo)
    snap = fixtures._snapshotter(repo, frontier.as_of)
    _install_replay(monkeypatch, snap, frontier, _ReplayBehavior())
    read = AsyncMock(side_effect=AssertionError("checkpoint query forbidden"))
    monkeypatch.setattr(repo, "get_portfolio_pnl_opening_checkpoint", read)

    await snap._advance_replay(cast(Repository, repo), _context(snap, frontier), frontier)

    read.assert_not_awaited()
    assert repo._opening_checkpoints == {}
    assert checkpoint_metrics().captures_committed == 0
    if mode == "off" or capture_permitted:
        assert snap.checkpoint_mode == "off"
        assert checkpoint_metrics().capture_refusals == {}
    else:
        assert checkpoint_metrics().capture_refusals == {"capture_not_permitted": 1}
    assert len(repo.committed_chunks) == 1


async def test_tip_capture_commits_and_service_refusal_still_commits_chunk(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Commit a certified row and isolate a later capture ValueError refusal.

    Given: Capture is permitted and the real service produced a certificate.
    When: A tip commits, then a replay reports ``capture_value_error``.
    Then: Both chunks commit while only the first stores a checkpoint row.
    """
    reset_checkpoint_metrics()
    _enable_checkpoint_modes(monkeypatch)
    monkeypatch.setenv("PNL_SNAPSHOTTER_CHECKPOINT_MODE", "active")
    repo = fixtures._FakeRepo()
    frontier = _pending(repo)
    write = await _write(monkeypatch, frontier)
    _permit_capture(monkeypatch)
    snap = fixtures._snapshotter(repo, frontier.as_of)
    _install_replay(monkeypatch, snap, frontier, _ReplayBehavior(write))

    await snap._advance_replay(cast(Repository, repo), _context(snap, frontier), frontier)

    assert await repo.get_portfolio_pnl_opening_checkpoint(fixtures._sample_scope()) is not None
    assert checkpoint_metrics().captures_committed == 1
    assert len(repo.committed_chunks) == 1

    second = replace(repo.frontier(), next_minute=fixtures._minute(4), tip=fixtures._minute(4))
    repo._frontiers[second.key] = second
    _install_replay(monkeypatch, snap, second, _ReplayBehavior(divergent=True))
    await snap._advance_replay(cast(Repository, repo), _context(snap, second), second)
    assert len(repo.committed_chunks) == 2
    assert checkpoint_metrics().capture_refusals == {"capture_value_error": 1}


@pytest.mark.parametrize("gate", ["off_tip", "fill_gap", "repair_before_start", "split_left"])
async def test_capture_gates_never_commit_a_row(
    monkeypatch: pytest.MonkeyPatch,
    gate: str,
) -> None:
    """Refuse off-tip, gap, prior repair, and split-left capture attempts.

    Given: A permitted resolver and one ineligible replay shape.
    When: The chunk plans and commits.
    Then: Financial progress commits without a checkpoint row.
    """
    reset_checkpoint_metrics()
    _enable_checkpoint_modes(monkeypatch)
    monkeypatch.setenv("PNL_SNAPSHOTTER_CHECKPOINT_MODE", "active")
    repo = fixtures._FakeRepo()
    frontier = _pending(repo)
    write = await _write(monkeypatch, frontier)
    _permit_capture(monkeypatch)
    if gate == "off_tip" or gate == "split_left":
        frontier = replace(frontier, tip=fixtures._minute(4))
    elif gate == "fill_gap":
        frontier = replace(frontier, has_fill_gap=True)
    elif gate == "repair_before_start":
        frontier = replace(frontier, repair_from=fixtures._minute(1))
    repo._frontiers[frontier.key] = frontier
    snap = fixtures._snapshotter(repo, frontier.as_of)
    if gate == "off_tip":
        snap._replay_chunk_limits[fixtures._WALLET] = 1
    _install_replay(
        monkeypatch,
        snap,
        frontier,
        _ReplayBehavior(write, split_once=gate == "split_left"),
    )

    await snap._advance_replay(cast(Repository, repo), _context(snap, frontier), frontier)

    assert await repo.get_portfolio_pnl_opening_checkpoint(fixtures._sample_scope()) is None
    assert len(repo.committed_chunks) == 1


async def test_write_precheck_refusal_does_not_fail_tick(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Drop a write when planning created a repair while committing its chunk.

    Given: A syntactically valid write and a pre-check that reports ``not_settled``.
    When: Active mode commits the settling chunk.
    Then: The chunk commits with no checkpoint and the refusal is counted.
    """
    reset_checkpoint_metrics()
    _enable_checkpoint_modes(monkeypatch)
    monkeypatch.setenv("PNL_SNAPSHOTTER_CHECKPOINT_MODE", "active")
    repo = fixtures._FakeRepo()
    frontier = _pending(repo)
    write = await _write(monkeypatch, frontier)
    _permit_capture(monkeypatch)
    snap = fixtures._snapshotter(repo, frontier.as_of)
    _install_replay(monkeypatch, snap, frontier, _ReplayBehavior(write))

    def refuse(
        frontier_after: PnlCorrectionFrontier,
        chunk: PnlCorrectionChunk,
        certificate: PnlOpeningCheckpointCertificate,
    ) -> str:
        """Simulate a repair created while planning."""
        del frontier_after, chunk, certificate
        return "not_settled"

    monkeypatch.setattr(dispatch, "checkpoint_write_refusal", refuse)
    await snap._advance_replay(cast(Repository, repo), _context(snap, frontier), frontier)

    assert len(repo.committed_chunks) == 1
    assert checkpoint_metrics().capture_refusals == {"not_settled": 1}


async def _resume(monkeypatch: pytest.MonkeyPatch) -> PnlCheckpointResume:
    """Build one decoded service resume without running repository verification."""
    record, _, _ = await verifier._valid_capture(monkeypatch)
    return PnlCheckpointResume(record, bundle_from_json(record.state_json))


def _install_resume(
    monkeypatch: pytest.MonkeyPatch,
    repo: fixtures._FakeRepo,
    frontier: PnlCorrectionFrontier,
    resume: PnlCheckpointResume,
) -> None:
    """Return one prepared resume and seed its fenced stored row."""
    bound = replace(
        resume,
        record=replace(
            resume.record,
            certificate=replace(
                resume.record.certificate,
                key=frontier.key,
                source_generation=frontier.generation,
            ),
        ),
    )
    repo._opening_checkpoints[frontier.key] = bound.record

    async def prepare(request: PnlCheckpointResumeRequest) -> PnlCheckpointDecision:
        """Return the already verified resume."""
        del request
        return PnlCheckpointDecision(resume=bound)

    monkeypatch.setattr(pnl_snapshotter, "prepare_checkpoint_resume", prepare)


async def test_shadow_match_keeps_anchor_plan_without_capture(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Commit the anchor plan after an exact match and retain the row.

    Given: A verified checkpoint and identical anchor and resumed paths.
    When: Shadow compares plans on a resume-only tick.
    Then: No deletion or capture occurs, comparison counters advance, and a row remains.
    """
    reset_checkpoint_metrics()
    monkeypatch.setenv("PNL_SNAPSHOTTER_CHECKPOINT_MODE", "shadow")
    repo = fixtures._FakeRepo()
    frontier = _pending(repo)
    write = await _write(monkeypatch, frontier)
    resume = await _resume(monkeypatch)
    _permit_capture(monkeypatch)
    _install_resume(monkeypatch, repo, frontier, resume)
    snap = fixtures._snapshotter(repo, frontier.as_of)
    events = _install_replay(monkeypatch, snap, frontier, _ReplayBehavior(write))

    await snap._advance_replay(cast(Repository, repo), _context(snap, frontier), frontier)

    assert events == ["anchor", "resume"]
    assert checkpoint_metrics().shadow_comparisons == 1
    assert checkpoint_metrics().shadow_mismatches == 0
    assert await repo.get_portfolio_pnl_opening_checkpoint(fixtures._sample_scope()) is not None


@pytest.mark.parametrize(
    "failure",
    [
        SQLAlchemyError("resumed replay database failure"),
        ValueError("invalid resumed state"),
        PnlTimelineWorkBudgetError("resumed replay budget"),
    ],
)
async def test_shadow_replay_failures_still_commit_anchor_and_log_once(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
    failure: Exception,
) -> None:
    """Isolate deterministic resumed failures from two authoritative commits.

    Given: A verified checkpoint whose resumed replay raises one review-listed exception.
    When: Two consecutive shadow chunks run under the same failure episode.
    Then: Both anchor chunks commit, each failure is counted, and only one warning is logged.

    Args:
        monkeypatch: Pytest fixture installing the checkpoint and replay doubles.
        caplog: Log capture for the scope's single warning episode.
        failure: Exception raised only by the resumed replay.
    """
    reset_checkpoint_metrics()
    monkeypatch.setenv("PNL_SNAPSHOTTER_CHECKPOINT_MODE", "shadow")
    repo = fixtures._FakeRepo()
    frontier = _pending(repo)
    resume = await _resume(monkeypatch)
    _permit_capture(monkeypatch)
    _install_resume(monkeypatch, repo, frontier, resume)
    snap = fixtures._snapshotter(repo, frontier.as_of)

    async def replay(
        unused_repo: Repository,
        ctx: _ScopeContext,
        chunk: ChunkWindow,
        current: PnlCorrectionFrontier,
        checkpoint: _CheckpointReplayOptions | None = None,
    ) -> tuple[ChunkWindow, PnlWalletSeriesResult]:
        """Return the cold anchor and fail only the non-authoritative resume."""
        del unused_repo, ctx, current
        if checkpoint is not None and checkpoint.resume is not None:
            raise failure
        return chunk, fixtures._series(chunk.start, chunk.end, metadata=fixtures._meta(seq=4))

    monkeypatch.setattr(snap, "_replay_series", replay)
    with caplog.at_level(logging.WARNING, logger=pnl_snapshotter.__name__):
        await snap._advance_replay(cast(Repository, repo), _context(snap, frontier), frontier)
        second = replace(repo.frontier(), next_minute=fixtures._minute(4), tip=fixtures._minute(4))
        repo._frontiers[second.key] = second
        await snap._advance_replay(cast(Repository, repo), _context(snap, second), second)

    reason = f"shadow_error:{type(failure).__name__}"
    assert len(repo.committed_chunks) == 2
    assert len(repo.persisted) == 2
    assert repo.frontier().next_minute is None
    assert checkpoint_metrics().shadow_skipped == {reason: 2}
    assert caplog.text.count("checkpoint shadow failed") == 1
    assert type(failure).__name__ in caplog.text


async def test_shadow_cancelled_error_propagates_after_anchor_commit(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Preserve cancellation instead of converting it to an isolated shadow failure.

    Given: A verified checkpoint whose resumed replay is cancelled.
    When: Shadow dispatch reaches the resumed replay.
    Then: The anchor chunk has already committed, CancelledError propagates,
        and no shadow-error metric occurs.

    Args:
        monkeypatch: Pytest fixture installing the checkpoint and replay doubles.
    """
    reset_checkpoint_metrics()
    monkeypatch.setenv("PNL_SNAPSHOTTER_CHECKPOINT_MODE", "shadow")
    repo = fixtures._FakeRepo()
    frontier = _pending(repo)
    resume = await _resume(monkeypatch)
    _permit_capture(monkeypatch)
    _install_resume(monkeypatch, repo, frontier, resume)
    snap = fixtures._snapshotter(repo, frontier.as_of)

    async def replay(
        unused_repo: Repository,
        ctx: _ScopeContext,
        chunk: ChunkWindow,
        current: PnlCorrectionFrontier,
        checkpoint: _CheckpointReplayOptions | None = None,
    ) -> tuple[ChunkWindow, PnlWalletSeriesResult]:
        """Return the cold anchor and cancel the resumed replay."""
        del unused_repo, ctx, current
        if checkpoint is not None and checkpoint.resume is not None:
            raise asyncio.CancelledError
        return chunk, fixtures._series(chunk.start, chunk.end, metadata=fixtures._meta(seq=4))

    monkeypatch.setattr(snap, "_replay_series", replay)
    with pytest.raises(asyncio.CancelledError):
        await snap._advance_replay(cast(Repository, repo), _context(snap, frontier), frontier)
    assert len(repo.committed_chunks) == 1
    assert checkpoint_metrics().shadow_skipped == {}


def _slow_resume_replay(
    delay_seconds: float,
) -> Callable[..., Awaitable[tuple[ChunkWindow, PnlWalletSeriesResult]]]:
    """Build a replay double whose resumed side sleeps before failing loudly."""

    async def replay(
        unused_repo: Repository,
        ctx: _ScopeContext,
        chunk: ChunkWindow,
        current: PnlCorrectionFrontier,
        checkpoint: _CheckpointReplayOptions | None = None,
    ) -> tuple[ChunkWindow, PnlWalletSeriesResult]:
        """Return the cold anchor at once and stall the resumed replay."""
        del unused_repo, ctx, current
        if checkpoint is not None and checkpoint.resume is not None:
            await asyncio.sleep(delay_seconds)
            raise AssertionError("the shadow budget should have cancelled the resumed replay")
        return chunk, fixtures._series(chunk.start, chunk.end, metadata=fixtures._meta(seq=4))

    return replay


async def test_shadow_tick_budget_cancels_slow_shadow_and_still_commits_anchor(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Keep the authoritative commit alive when shadow work outlasts its own budget.

    Given: A verified checkpoint, a tick deadline leaving only 50 ms of shadow
        budget after the commit reserve, and a resumed replay that stalls for 5 s.
    When: Shadow dispatch runs the chunk.
    Then: The shadow branch is cancelled as an isolated ``TimeoutError`` failure
        and the cold anchor plan still commits before the tick deadline.

    Args:
        monkeypatch: Pytest fixture installing the checkpoint and replay doubles.
    """
    reset_checkpoint_metrics()
    monkeypatch.setenv("PNL_SNAPSHOTTER_CHECKPOINT_MODE", "shadow")
    repo = fixtures._FakeRepo()
    frontier = _pending(repo)
    resume = await _resume(monkeypatch)
    _permit_capture(monkeypatch)
    _install_resume(monkeypatch, repo, frontier, resume)
    snap = fixtures._snapshotter(repo, frontier.as_of)
    monkeypatch.setattr(snap, "_replay_series", _slow_resume_replay(5.0))
    reserve = snap.interval_seconds * pnl_snapshotter.SHADOW_TICK_RESERVE_FRACTION
    snap._tick_deadline = time.monotonic() + reserve + 0.05

    started = time.monotonic()
    await snap._advance_replay(cast(Repository, repo), _context(snap, frontier), frontier)

    assert time.monotonic() - started < 2.0
    assert len(repo.committed_chunks) == 1
    assert checkpoint_metrics().shadow_skipped == {"shadow_error:TimeoutError": 1}


async def test_shadow_without_tick_budget_skips_shadow_and_commits_anchor(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Skip shadow work entirely once the commit reserve is all that remains.

    Given: A verified checkpoint and a tick deadline already inside the commit reserve.
    When: Shadow dispatch runs the chunk.
    Then: The resumed replay never starts, ``tick_budget`` is counted, and the
        anchor plan commits.

    Args:
        monkeypatch: Pytest fixture installing the checkpoint and replay doubles.
    """
    reset_checkpoint_metrics()
    monkeypatch.setenv("PNL_SNAPSHOTTER_CHECKPOINT_MODE", "shadow")
    repo = fixtures._FakeRepo()
    frontier = _pending(repo)
    resume = await _resume(monkeypatch)
    _permit_capture(monkeypatch)
    _install_resume(monkeypatch, repo, frontier, resume)
    snap = fixtures._snapshotter(repo, frontier.as_of)
    monkeypatch.setattr(snap, "_replay_series", _slow_resume_replay(5.0))
    snap._tick_deadline = time.monotonic()

    await snap._advance_replay(cast(Repository, repo), _context(snap, frontier), frontier)

    assert len(repo.committed_chunks) == 1
    assert checkpoint_metrics().shadow_skipped == {"tick_budget": 1}


async def test_shadow_external_cancel_propagates_under_tick_budget(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Propagate a task cancellation that arrives while the shadow budget is armed.

    Given: A verified checkpoint, a generous tick deadline, and a resumed replay
        that stalls until the dispatching task is cancelled from outside.
    When: The task is cancelled during the shadow branch.
    Then: ``CancelledError`` reaches the caller after the anchor chunk has
        committed, and no shadow failure is recorded.

    Args:
        monkeypatch: Pytest fixture installing the checkpoint and replay doubles.
    """
    reset_checkpoint_metrics()
    monkeypatch.setenv("PNL_SNAPSHOTTER_CHECKPOINT_MODE", "shadow")
    repo = fixtures._FakeRepo()
    frontier = _pending(repo)
    resume = await _resume(monkeypatch)
    _permit_capture(monkeypatch)
    _install_resume(monkeypatch, repo, frontier, resume)
    snap = fixtures._snapshotter(repo, frontier.as_of)
    monkeypatch.setattr(snap, "_replay_series", _slow_resume_replay(30.0))
    snap._tick_deadline = time.monotonic() + 3600.0

    task = asyncio.create_task(
        snap._advance_replay(cast(Repository, repo), _context(snap, frontier), frontier)
    )
    await asyncio.sleep(0.05)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert len(repo.committed_chunks) == 1
    assert checkpoint_metrics().shadow_skipped == {}


async def test_shadow_uncancellable_worker_past_both_deadlines_cannot_skip_commit(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Commit the anchor even when a shielded valuation worker outlives every deadline.

    Given: A verified checkpoint, a shadow budget of 50 ms, an enclosing tick
        timeout of 150 ms, and a resumed replay whose valuation runs in the
        production shielded worker for 300 ms, which no cancellation can stop.
    When: The tick deadline expires while the worker is still running.
    Then: The tick timeout surfaces as before, yet the cold anchor chunk has
        already committed, because the commit precedes every shadow await.

    Args:
        monkeypatch: Pytest fixture installing the checkpoint and replay doubles.
    """
    reset_checkpoint_metrics()
    monkeypatch.setenv("PNL_SNAPSHOTTER_CHECKPOINT_MODE", "shadow")
    repo = fixtures._FakeRepo()
    frontier = _pending(repo)
    resume = await _resume(monkeypatch)
    _permit_capture(monkeypatch)
    _install_resume(monkeypatch, repo, frontier, resume)
    snap = fixtures._snapshotter(repo, frontier.as_of)

    def blocking_valuation() -> tuple[PnlTimelineResult, tuple[PnlFxRateSource, ...]]:
        """Hold the worker thread past both deadlines, then fail loudly."""
        time.sleep(0.3)
        raise AssertionError("the shielded worker result must never reach planning")

    async def replay(
        unused_repo: Repository,
        ctx: _ScopeContext,
        chunk: ChunkWindow,
        current: PnlCorrectionFrontier,
        checkpoint: _CheckpointReplayOptions | None = None,
    ) -> tuple[ChunkWindow, PnlWalletSeriesResult]:
        """Return the cold anchor and run the resumed valuation in the shielded worker."""
        del unused_repo, ctx, current
        if checkpoint is not None and checkpoint.resume is not None:
            await pnl_timeline_service._offload_series_valuation(blocking_valuation)
        return chunk, fixtures._series(chunk.start, chunk.end, metadata=fixtures._meta(seq=4))

    monkeypatch.setattr(snap, "_replay_series", replay)
    reserve = snap.interval_seconds * pnl_snapshotter.SHADOW_TICK_RESERVE_FRACTION
    snap._tick_deadline = time.monotonic() + reserve + 0.05

    with pytest.raises(TimeoutError):
        async with asyncio.timeout(0.15):
            await snap._advance_replay(cast(Repository, repo), _context(snap, frontier), frontier)

    assert len(repo.committed_chunks) == 1


async def test_shadow_plans_both_sides_from_one_planning_input_read(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Read planning evidence once so a racing write cannot fake a mismatch.

    Given: A verified checkpoint and identical anchor and resumed paths.
    When: Shadow plans the anchor and the resumed side for one chunk.
    Then: Observation and position evidence are read exactly once and both
        planners receive the same snapshot.

    Args:
        monkeypatch: Pytest fixture installing the checkpoint and replay doubles.
    """
    reset_checkpoint_metrics()
    monkeypatch.setenv("PNL_SNAPSHOTTER_CHECKPOINT_MODE", "shadow")
    repo = fixtures._FakeRepo()
    frontier = _pending(repo)
    write = await _write(monkeypatch, frontier)
    resume = await _resume(monkeypatch)
    _permit_capture(monkeypatch)
    _install_resume(monkeypatch, repo, frontier, resume)
    snap = fixtures._snapshotter(repo, frontier.as_of)
    _install_replay(monkeypatch, snap, frontier, _ReplayBehavior(write))
    evidence_reads = AsyncMock(wraps=snap._load_chunk_evidence)
    position_reads = AsyncMock(wraps=snap._load_position_versions)
    monkeypatch.setattr(snap, "_load_chunk_evidence", evidence_reads)
    monkeypatch.setattr(snap, "_load_position_versions", position_reads)
    seen: list[_PlanBasis] = []
    original_impl = snap._plan_chunk_impl

    async def recording_impl(
        request: pnl_snapshotter._ChunkPlanRequest, *, advance_sequence: bool
    ) -> PlannedPnlCorrection:
        """Record each planner's basis before delegating to the real planner."""
        seen.append(request.basis)
        return await original_impl(request, advance_sequence=advance_sequence)

    monkeypatch.setattr(snap, "_plan_chunk_impl", recording_impl)

    await snap._advance_replay(cast(Repository, repo), _context(snap, frontier), frontier)

    assert checkpoint_metrics().shadow_comparisons == 1
    assert evidence_reads.await_count == 1
    assert position_reads.await_count == 1
    assert len(seen) == 2
    assert seen[0].inputs is not None
    assert seen[0].inputs is seen[1].inputs


async def test_shadow_mismatch_deletes_row_and_logs_error(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Retire a divergent row without capture and commit the anchor plan.

    Given: A resumed point differs from the anchor on a resume-only tick.
    When: Shadow detects the mismatch.
    Then: Fenced deletion occurs, no row survives, and ERROR is logged.
    """
    reset_checkpoint_metrics()
    monkeypatch.setenv("PNL_SNAPSHOTTER_CHECKPOINT_MODE", "shadow")
    repo = fixtures._FakeRepo()
    frontier = _pending(repo)
    write = await _write(monkeypatch, frontier)
    resume = await _resume(monkeypatch)
    _permit_capture(monkeypatch)
    _install_resume(monkeypatch, repo, frontier, resume)
    snap = fixtures._snapshotter(repo, frontier.as_of)
    events = _install_replay(monkeypatch, snap, frontier, _ReplayBehavior(write, divergent=True))
    original_delete = repo.delete_portfolio_pnl_opening_checkpoint

    async def delete(scope: object, expected_generation: int) -> bool:
        """Record deletion in the shadow event sequence."""
        events.append("delete")
        return await original_delete(cast(PortfolioPnlSampleScope, scope), expected_generation)

    monkeypatch.setattr(repo, "delete_portfolio_pnl_opening_checkpoint", delete)
    with caplog.at_level(logging.ERROR):
        await snap._advance_replay(cast(Repository, repo), _context(snap, frontier), frontier)

    assert events == ["anchor", "resume", "delete"]
    assert "checkpoint_shadow_mismatch" in caplog.text
    assert checkpoint_metrics().shadow_mismatches == 1
    assert checkpoint_metrics().invalidations_deleted == 1
    assert await repo.get_portfolio_pnl_opening_checkpoint(fixtures._sample_scope()) is None


async def test_active_resume_commits_resumed_result(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Use the resumed path directly after verification.

    Given: A verified stored checkpoint and bit-identical suffix replay.
    When: Active advances on a resume-only tick.
    Then: No anchor replay occurs and resume success is counted.
    """
    reset_checkpoint_metrics()
    _enable_checkpoint_modes(monkeypatch)
    monkeypatch.setenv("PNL_SNAPSHOTTER_CHECKPOINT_MODE", "active")
    repo = fixtures._FakeRepo()
    frontier = _pending(repo)
    write = await _write(monkeypatch, frontier)
    resume = await _resume(monkeypatch)
    _permit_capture(monkeypatch)
    _install_resume(monkeypatch, repo, frontier, resume)
    snap = fixtures._snapshotter(repo, frontier.as_of)
    events = _install_replay(monkeypatch, snap, frontier, _ReplayBehavior(write))

    await snap._advance_replay(cast(Repository, repo), _context(snap, frontier), frontier)

    assert events == ["resume"]
    assert checkpoint_metrics().resumes_attempted == 1
    assert checkpoint_metrics().resumes_succeeded == 1
    assert len(repo.committed_chunks) == 1


@pytest.mark.parametrize("mode", ["shadow", "active"])
async def test_service_resume_fallback_deletes_and_commits_anchor(
    monkeypatch: pytest.MonkeyPatch,
    mode: str,
) -> None:
    """Fall back from a late suffix execution without failing the tick.

    Given: A verified row whose suffix load reports ``checkpoint_late_execution``.
    When: Shadow or active dispatches the replay.
    Then: The old generation is deleted and the anchor chunk commits. Active
        recaptures inline; shadow commits first and defers the recapture to the
        next tick's ``absent`` path, so its shadow branch never captures.
    """
    reset_checkpoint_metrics()
    if mode != "off":
        _enable_checkpoint_modes(monkeypatch)
    monkeypatch.setenv("PNL_SNAPSHOTTER_CHECKPOINT_MODE", mode)
    repo = fixtures._FakeRepo()
    frontier = _pending(repo)
    write = await _write(monkeypatch, frontier)
    resume = await _resume(monkeypatch)
    _permit_capture(monkeypatch)
    _install_resume(monkeypatch, repo, frontier, resume)
    snap = fixtures._snapshotter(repo, frontier.as_of)
    events = _install_replay(
        monkeypatch,
        snap,
        frontier,
        _ReplayBehavior(write, resume_fallback=True),
    )

    await snap._advance_replay(cast(Repository, repo), _context(snap, frontier), frontier)

    expected = ["anchor", "resume"] if mode == "shadow" else ["resume", "capture"]
    assert events == expected
    assert checkpoint_metrics().fallbacks == {"checkpoint_late_execution": 1}
    assert checkpoint_metrics().invalidations_deleted == 1
    assert checkpoint_metrics().resumes_succeeded == 0
    assert len(repo.committed_chunks) == 1


async def test_shadow_fallback_retires_checkpoint_without_inline_recapture(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Retire a fallen-back checkpoint in shadow without any capture inside the branch.

    Given: A verified checkpoint whose resumed replay falls back, and a
        recapture that would refuse with ``capture_evidence_error`` if attempted.
    When: Shadow dispatch commits the cold anchor and then runs its branch.
    Then: Samples and frontier advance, the stored checkpoint is deleted, and
        no capture is attempted, so no capture refusal or commit is counted.

    Args:
        monkeypatch: Pytest fixture installing resume and replay behavior.
    """
    reset_checkpoint_metrics()
    _enable_checkpoint_modes(monkeypatch)
    monkeypatch.setenv("PNL_SNAPSHOTTER_CHECKPOINT_MODE", "shadow")
    repo = fixtures._FakeRepo()
    frontier = _pending(repo)
    write = await _write(monkeypatch, frontier)
    resume = await _resume(monkeypatch)
    _permit_capture(monkeypatch)
    _install_resume(monkeypatch, repo, frontier, resume)
    snap = fixtures._snapshotter(repo, frontier.as_of)
    events = _install_replay(
        monkeypatch,
        snap,
        frontier,
        _ReplayBehavior(
            write,
            resume_fallback=True,
            capture_refusal="capture_evidence_error",
        ),
    )

    await snap._advance_replay(cast(Repository, repo), _context(snap, frontier), frontier)

    assert events == ["anchor", "resume"]
    assert len(repo.committed_chunks) == 1
    assert len(repo.persisted) == 1
    assert repo.frontier().next_minute is None
    assert checkpoint_metrics().invalidations_deleted == 1
    assert checkpoint_metrics().capture_refusals == {}
    assert checkpoint_metrics().captures_committed == 0
    assert checkpoint_metrics().recaptures == {}


@pytest.mark.parametrize("mode", ["active", "shadow"])
async def test_checkpoint_mode_prior_peak_change_rewinds_without_commit(
    monkeypatch: pytest.MonkeyPatch,
    mode: str,
) -> None:
    """Rewind both dispatch shapes when the causal prefix changes during replay.

    Given: Active or shadow replay and a physical evidence peak newer than the frontier.
    When: The selected path reaches planning.
    Then: It rewinds without committing financial rows or a checkpoint.
    """
    reset_checkpoint_metrics()
    if mode != "off":
        _enable_checkpoint_modes(monkeypatch)
    monkeypatch.setenv("PNL_SNAPSHOTTER_CHECKPOINT_MODE", mode)
    repo = fixtures._FakeRepo()
    frontier = _pending(repo)
    _permit_capture(monkeypatch)
    if mode == "shadow":
        resume = await _resume(monkeypatch)
        _permit_capture(monkeypatch)
        _install_resume(monkeypatch, repo, frontier, resume)
    snap = fixtures._snapshotter(repo, frontier.as_of)
    _install_replay(monkeypatch, snap, frontier, _ReplayBehavior())
    original = repo.get_portfolio_pnl_correction_evidence

    async def changed(scope: PortfolioPnlSampleScope, start: object, end: object) -> object:
        """Return evidence whose prior peak changed during replay."""
        evidence = await original(scope, cast(datetime, start), cast(datetime, end))
        return replace(evidence, prior_peak=1.0)

    rewind = AsyncMock()
    monkeypatch.setattr(repo, "get_portfolio_pnl_correction_evidence", changed)
    monkeypatch.setattr(snap, "_rewind_replay", rewind)
    await snap._advance_replay(cast(Repository, repo), _context(snap, frontier), frontier)

    rewind.assert_awaited_once()
    assert not repo.committed_chunks


async def test_shadow_without_tip_capture_still_commits_matching_anchor(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Compare and commit a non-settling shadow chunk without a third replay.

    Given: A verified checkpoint and an off-tip one-minute chunk.
    When: Anchor and resumed paths match.
    Then: Only those two paths run and the anchor plan commits.
    """
    reset_checkpoint_metrics()
    monkeypatch.setenv("PNL_SNAPSHOTTER_CHECKPOINT_MODE", "shadow")
    repo = fixtures._FakeRepo()
    frontier = replace(_pending(repo), tip=fixtures._minute(4))
    repo._frontiers[frontier.key] = frontier
    resume = await _resume(monkeypatch)
    _permit_capture(monkeypatch)
    _install_resume(monkeypatch, repo, frontier, resume)
    snap = fixtures._snapshotter(repo, frontier.as_of)
    snap._replay_chunk_limits[fixtures._WALLET] = 1
    events = _install_replay(monkeypatch, snap, frontier, _ReplayBehavior())

    await snap._advance_replay(cast(Repository, repo), _context(snap, frontier), frontier)

    assert events == ["anchor", "resume"]
    assert len(repo.committed_chunks) == 1


async def test_off_prior_peak_change_executes_real_rewind_without_commit(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Exercise the off-mode evidence guard and actual causal-prefix rewind.

    Given: A cold off-mode replay whose physical prior peak changes before planning.
    When: The evidence guard invokes the unmocked rewind helper.
    Then: The frontier rebuilds from the activation prefix and no financial chunk commits.

    Args:
        monkeypatch: Pytest fixture installing replay and changed-evidence reads.
    """
    monkeypatch.setenv("PNL_SNAPSHOTTER_CHECKPOINT_MODE", "off")
    repo = fixtures._FakeRepo()
    frontier = _pending(repo)
    snap = fixtures._snapshotter(repo, frontier.as_of)
    _install_replay(monkeypatch, snap, frontier, _ReplayBehavior())
    original = repo.get_portfolio_pnl_correction_evidence

    async def changed(scope: PortfolioPnlSampleScope, start: object, end: object) -> object:
        """Return evidence whose causal peak differs from the durable frontier."""
        evidence = await original(scope, cast(datetime, start), cast(datetime, end))
        return replace(evidence, prior_peak=1.0)

    monkeypatch.setattr(repo, "get_portfolio_pnl_correction_evidence", changed)
    await snap._advance_replay(cast(Repository, repo), _context(snap, frontier), frontier)

    assert repo.committed_chunks == []
    assert repo.frontier() != frontier
    assert repo.frontier().next_minute == fixtures._minute(1)


def test_chunk_outcome_recovers_reduced_limits_after_fast_full_chunks(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Ignore slow or partial chunks and grow reduced replay limits after three successes.

    Given: A reduced four-minute replay limit and an eight-minute maximum.
    When: Slow, partial, then three full fast outcomes arrive, followed by a smaller limit.
    Then: The first limit resets to maximum and the second grows by one doubling step.

    Args:
        monkeypatch: Pytest fixture setting a deterministic maximum chunk width.
    """
    monkeypatch.setattr(pnl_snapshotter, "MAX_CORRECTION_CHUNK_MINUTES", 8)
    snap = fixtures._snapshotter(fixtures._FakeRepo(), fixtures._minute(5))
    wallet = fixtures._WALLET
    full = ChunkWindow(fixtures._minute(1), fixtures._minute(4))
    partial = ChunkWindow(fixtures._minute(1), fixtures._minute(2))
    snap._replay_chunk_limits[wallet] = 4
    snap._record_chunk_outcome(wallet, float(snap._interval_seconds), full)
    snap._record_chunk_outcome(wallet, 0.0, partial)
    for _ in range(3):
        snap._record_chunk_outcome(wallet, 0.0, full)
    assert snap._replay_chunk_limit(wallet) == 8

    snap._replay_chunk_limits[wallet] = 2
    for _ in range(3):
        snap._record_chunk_outcome(wallet, 0.0, partial)
    assert snap._replay_chunk_limit(wallet) == 4


def test_repair_from_lag_warning_is_logged_once_per_frontier(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Suppress duplicate lag warnings for one unchanged repair frontier.

    Given: A repair boundary more than one day behind its generation tip.
    When: The lag reporter runs twice for the same scope and repair minute.
    Then: Only the first call emits the warning.

    Args:
        caplog: Log capture for the throttled repair-lag warning.
    """
    repo = fixtures._FakeRepo()
    frontier = _pending(repo)
    lagging = replace(frontier, repair_from=frontier.tip - timedelta(hours=25))
    snap = fixtures._snapshotter(repo, frontier.as_of)
    ctx = _context(snap, lagging)
    with caplog.at_level(logging.WARNING, logger=pnl_snapshotter.__name__):
        snap._log_repair_from_lag(ctx, lagging)
        snap._log_repair_from_lag(ctx, lagging)
    assert caplog.text.count("repair_from lags frontier tip") == 1


def _financial_rows(repo: fixtures._FakeRepo) -> list[dict[str, object]]:
    """Return committed rows without per-process write provenance."""
    return [
        {
            key: value
            for key, value in row.items()
            if key not in {"public_id", "session_id", "sequence_id", "timestamp"}
        }
        for row in repo.persisted
    ]


@pytest.mark.parametrize("mode", ["off", "shadow", "active"])
async def test_modes_commit_identical_financial_rows_on_same_quiet_scenario(
    monkeypatch: pytest.MonkeyPatch,
    mode: str,
) -> None:
    """Keep committed financial rows identical across every dispatch mode.

    Given: The same quiet complete-point scenario under off, shadow, and active.
    When: Each mode plans and commits its chunk.
    Then: Every financial, audit, and watermark field is identical.
    """
    reset_checkpoint_metrics()
    if mode != "off":
        _enable_checkpoint_modes(monkeypatch)
    monkeypatch.setenv("PNL_SNAPSHOTTER_CHECKPOINT_MODE", mode)
    repo = fixtures._FakeRepo()
    frontier = _pending(repo)
    if mode != "off":
        _permit_capture(monkeypatch)
    snap = fixtures._snapshotter(repo, frontier.as_of)
    _install_replay(monkeypatch, snap, frontier, _ReplayBehavior())

    await snap._advance_replay(cast(Repository, repo), _context(snap, frontier), frontier)

    expected = [
        {
            key: value
            for key, value in fixtures._persisted_sample(
                fixtures._minute(3), status="complete"
            ).items()
            if key not in {"public_id", "session_id", "sequence_id", "timestamp"}
        }
    ]
    assert _financial_rows(repo) == [
        {
            **row,
            "realized_pnl": 1.0,
            "fee_pnl": -0.5,
            "accrual_pnl": 0.25,
            "unrealized_pnl": 5.0,
            "cash_usd": 1000.0,
            "position_value_usd": 0.0,
            "drawdown": 0.0,
            "audit_json": _financial_rows(repo)[0]["audit_json"],
            "watermarks_json": _financial_rows(repo)[0]["watermarks_json"],
        }
        for row in expected
    ]


def _summary_lines(caplog: pytest.LogCaptureFixture) -> list[str]:
    """Return all INFO checkpoint summary log messages captured during a test."""
    return [
        record.getMessage()
        for record in caplog.records
        if record.levelname == "INFO"
        and record.getMessage().startswith("checkpoint_metrics_summary")
    ]


async def test_periodic_summary_emits_every_n_ticks_and_separates_scopes(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Emit one deterministic INFO summary per scope every N active ticks.

    Given: Two independent wallet scopes in a shadow-mode snapshotter.
    When: ``_tick_once`` runs for ``CHECKPOINT_SUMMARY_EVERY_TICKS`` ticks with events in each scope.
    Then: Ticks 1..N-1 emit nothing, tick N emits one cumulative line per scope, and scopes stay isolated.
    """
    reset_checkpoint_metrics()
    monkeypatch.setenv("PNL_SNAPSHOTTER_CHECKPOINT_MODE", "shadow")
    wallet_a = "wallet-alpha"
    wallet_b = "wallet-beta"
    repo = fixtures._FakeRepo(
        credentials=[
            fixtures._cred("kraken", wallet=wallet_a),
            fixtures._cred("kraken", wallet=wallet_b),
        ],
        anchor=fixtures._anchor(),
    )

    async def anchor_for(
        wallet: str, mode: str, ccy: str, as_of: datetime | None
    ) -> PortfolioPnlAnchorRow:
        """Return an active USD anchor bound to ``wallet``."""
        del mode, ccy, as_of
        return cast(
            PortfolioPnlAnchorRow,
            {**fixtures._anchor(), "wallet_public_id": wallet},
        )

    monkeypatch.setattr(repo, "get_portfolio_pnl_anchor", anchor_for)
    snap = fixtures._snapshotter(repo, fixtures._minute(3))

    async def fake_series(
        unused_repo: object,
        ctx: _ScopeContext,
        chunk: ChunkWindow,
        options: object,
    ) -> PnlWalletSeriesResult:
        """Return a canned series per scope without DB reads."""
        del unused_repo, ctx, options
        return fixtures._series(chunk.start, chunk.end, metadata=fixtures._meta(seq=4))

    monkeypatch.setattr(snap, "_series", fake_series)

    with caplog.at_level(logging.INFO, logger=pnl_snapshotter.__name__):
        for _ in range(CHECKPOINT_SUMMARY_EVERY_TICKS - 1):
            await snap._tick_once()
        assert _summary_lines(caplog) == []
        await snap._tick_once()

    lines = _summary_lines(caplog)
    assert len(lines) == 2
    alias_a = checkpoint_wallet_alias(wallet_a)
    alias_b = checkpoint_wallet_alias(wallet_b)
    expected_lines = {
        (
            "checkpoint_metrics_summary "
            f"wallet={alias_a} mode=live ccy=USD "
            "captures_committed=0 "
            "capture_refusals={} "
            "resumes_attempted=0 resumes_succeeded=0 "
            "fallbacks={checkpoint_absent:1} invalidations_deleted=0 "
            "shadow_comparisons=0 shadow_mismatches=0 "
            "shadow_skipped={} recaptures={absent:1}"
        ),
        (
            "checkpoint_metrics_summary "
            f"wallet={alias_b} mode=live ccy=USD "
            "captures_committed=0 "
            "capture_refusals={} "
            "resumes_attempted=0 resumes_succeeded=0 "
            "fallbacks={checkpoint_absent:1} invalidations_deleted=0 "
            "shadow_comparisons=0 shadow_mismatches=0 "
            "shadow_skipped={} recaptures={absent:1}"
        ),
    }
    assert set(lines) == expected_lines
    assert wallet_a not in "\n".join(lines)
    assert wallet_b not in "\n".join(lines)
    by_scope = checkpoint_metrics_by_scope()
    assert by_scope[(alias_a, "live", "USD")].fallbacks == {"checkpoint_absent": 1}
    assert by_scope[(alias_b, "live", "USD")].fallbacks == {"checkpoint_absent": 1}
    assert by_scope[(alias_a, "live", "USD")].recaptures == {"absent": 1}
    assert by_scope[(alias_b, "live", "USD")].recaptures == {"absent": 1}
    assert checkpoint_metrics().capture_refusals == {}
    assert checkpoint_metrics().fallbacks == {"checkpoint_absent": 2}
    assert checkpoint_metrics().recaptures == {"absent": 2}


async def test_off_mode_emits_no_summary_lines_or_per_scope_accounting(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Skip summary logging and per-scope counter overhead when checkpoint mode is off.

    Given: A snapshotter configured with ``PNL_SNAPSHOTTER_CHECKPOINT_MODE=off`` and
        capture permitted on the engine options table.
    When: ``_tick_once`` runs ``CHECKPOINT_SUMMARY_EVERY_TICKS`` times across a valid scope.
    Then: Zero summary lines are logged, no checkpoint writes occur, and metrics stay empty.
    """
    reset_checkpoint_metrics()
    _permit_capture(monkeypatch)
    monkeypatch.setenv("PNL_SNAPSHOTTER_CHECKPOINT_MODE", "off")
    repo = fixtures._FakeRepo(
        credentials=[fixtures._cred("kraken")],
        anchor=fixtures._anchor(),
    )
    snap = fixtures._snapshotter(repo, fixtures._minute(3))

    async def fake_series(
        unused_repo: object,
        ctx: _ScopeContext,
        chunk: ChunkWindow,
        options: object,
    ) -> PnlWalletSeriesResult:
        """Return a canned series without DB reads."""
        del unused_repo, ctx, options
        return fixtures._series(chunk.start, chunk.end, metadata=fixtures._meta(seq=4))

    monkeypatch.setattr(snap, "_series", fake_series)

    with caplog.at_level(logging.INFO, logger=pnl_snapshotter.__name__):
        for _ in range(CHECKPOINT_SUMMARY_EVERY_TICKS):
            await snap._tick_once()

    assert _summary_lines(caplog) == []
    assert checkpoint_metrics_by_scope() == {}
    assert checkpoint_metrics().captures_committed == 0
    assert checkpoint_metrics().capture_refusals == {}


async def _setup_shadow_second_scope(
    monkeypatch: pytest.MonkeyPatch,
    repo: fixtures._FakeRepo,
    frontier_a: PnlCorrectionFrontier,
    wallet_b: str,
) -> tuple[PnlCorrectionFrontier, PnlCheckpointResume]:
    """Seed a second wallet's frontier and stored checkpoint for shadow comparison."""
    frontier_b = replace(frontier_a, key=replace(frontier_a.key, wallet_public_id=wallet_b))
    repo._frontiers[frontier_b.key] = frontier_b
    resume_b = await _resume(monkeypatch)
    b_record = replace(
        resume_b.record,
        certificate=replace(
            resume_b.record.certificate,
            key=frontier_b.key,
            source_generation=frontier_b.generation,
        ),
    )
    repo._opening_checkpoints[frontier_b.key] = b_record
    _permit_capture(monkeypatch)
    return frontier_b, replace(resume_b, record=b_record)


async def test_two_scopes_count_commits_and_resumes_independently(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Count distinct checkpoint transitions per scope and emit independent summaries.

    Given: Two wallets with capture permitted, one committing and one failing shadow comparison.
    When: ``_advance_replay`` processes each scope and a periodic summary tick fires.
    Then: Each scope carries only its own counts, summaries reflect both, and totals sum them.
    """
    reset_checkpoint_metrics()
    monkeypatch.setenv("PNL_SNAPSHOTTER_CHECKPOINT_MODE", "shadow")
    _permit_capture(monkeypatch)
    repo = fixtures._FakeRepo()
    frontier_a = _pending(repo)
    write_a = await _write(monkeypatch, frontier_a)
    wallet_b = "wallet-shadow-mismatch"
    frontier_b, resume_b = await _setup_shadow_second_scope(monkeypatch, repo, frontier_a, wallet_b)
    snap = fixtures._snapshotter(repo, frontier_a.as_of)

    async def prepare(request: PnlCheckpointResumeRequest) -> PnlCheckpointDecision:
        """Return a prepared resume for ``wallet_b`` and delegate ``wallet_a``."""
        if request.scope.wallet_public_id == wallet_b:
            return PnlCheckpointDecision(resume=resume_b)
        return await dispatch.prepare_checkpoint_resume(request)

    async def replay(
        unused_repo: Repository,
        ctx: _ScopeContext,
        chunk: ChunkWindow,
        current: PnlCorrectionFrontier,
        checkpoint: _CheckpointReplayOptions | None = None,
    ) -> tuple[ChunkWindow, PnlWalletSeriesResult]:
        """Stage a capture write for ``wallet_a`` and diverge on ``wallet_b`` resume."""
        del unused_repo, current
        checkpoint = checkpoint or _CheckpointReplayOptions()
        result = fixtures._series(chunk.start, chunk.end, metadata=fixtures._meta(seq=4))
        if ctx.wallet_public_id == wallet_b and checkpoint.resume is not None:
            return chunk, replace(
                result,
                points=(replace(result.points[0], realized_pnl=9.0), *result.points[1:]),
            )
        return chunk, replace(
            result, checkpoint_write=write_a if checkpoint.capture is not None else None
        )

    monkeypatch.setattr(pnl_snapshotter, "prepare_checkpoint_resume", prepare)
    monkeypatch.setattr(snap, "_replay_series", replay)
    ctx_a = _context(snap, frontier_a)
    ctx_b = snap._build_scope_context(
        wallet_b,
        cast(PortfolioPnlAnchorRow, {**fixtures._anchor(), "wallet_public_id": wallet_b}),
        frozenset({"kraken"}),
        frontier_b.as_of,
    )
    await snap._advance_replay(cast(Repository, repo), ctx_a, frontier_a)
    await snap._advance_replay(cast(Repository, repo), ctx_b, frontier_b)

    by_scope = checkpoint_metrics_by_scope()
    key_a = checkpoint_scope_key((fixtures._WALLET, "live", "USD"))
    key_b = checkpoint_scope_key((wallet_b, "live", "USD"))
    assert by_scope[key_a].captures_committed == 1
    assert by_scope[key_a].recaptures == {"absent": 1}
    assert by_scope[key_b].shadow_comparisons == 1
    assert by_scope[key_b].shadow_mismatches == 1
    assert by_scope[key_b].invalidations_deleted == 1
    assert checkpoint_metrics().captures_committed == 1
    assert checkpoint_metrics().shadow_mismatches == 1

    repo._credentials = [
        fixtures._cred("kraken", wallet=fixtures._WALLET),
        fixtures._cred("kraken", wallet=wallet_b),
    ]
    monkeypatch.setattr(snap, "_probe_replay", AsyncMock(return_value=None))
    snap._checkpoint_summary_ticks = CHECKPOINT_SUMMARY_EVERY_TICKS - 1
    with caplog.at_level(logging.INFO, logger=pnl_snapshotter.__name__):
        await snap._tick_once()
    lines = _summary_lines(caplog)
    assert len(lines) == 2
    expected_lines = {
        (
            "checkpoint_metrics_summary "
            f"wallet={key_a[0]} mode=live ccy=USD "
            "captures_committed=1 capture_refusals={} "
            "resumes_attempted=0 resumes_succeeded=0 "
            "fallbacks={checkpoint_absent:1} invalidations_deleted=0 "
            "shadow_comparisons=0 shadow_mismatches=0 "
            "shadow_skipped={} recaptures={absent:1}"
        ),
        (
            "checkpoint_metrics_summary "
            f"wallet={key_b[0]} mode=live ccy=USD "
            "captures_committed=0 capture_refusals={} "
            "resumes_attempted=1 resumes_succeeded=1 "
            "fallbacks={} invalidations_deleted=1 "
            "shadow_comparisons=1 shadow_mismatches=1 "
            "shadow_skipped={} recaptures={}"
        ),
    }
    assert set(lines) == expected_lines
    assert fixtures._WALLET not in "\n".join(lines)
    assert wallet_b not in "\n".join(lines)


async def test_checkpoint_upsert_error_becomes_closed_refusal_and_commits_samples(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Keep chunk samples and frontier commit intact when checkpoint upsert raises.

    Given: Active mode with a settling capture write and a repository whose checkpoint
        upsert raises ``IntegrityError``.
    When: ``_tick_once`` runs the capture chunk.
    Then: No exception escapes, the financial sample row and frontier advance commit,
        ``capture_refusals`` records ``capture_upsert_failed``, and ``captures_committed == 0``.

    Args:
        monkeypatch: Pytest monkeypatch fixture for checkpoint mode and capture permissions.
    """
    reset_checkpoint_metrics()
    _enable_checkpoint_modes(monkeypatch)
    monkeypatch.setenv("PNL_SNAPSHOTTER_CHECKPOINT_MODE", "active")
    repo = fixtures._FakeRepo(credentials=[fixtures._cred("kraken")], anchor=fixtures._anchor())
    frontier = _pending(repo)
    write = await _write(monkeypatch, frontier)
    _permit_capture(monkeypatch)
    repo.upsert_failure = IntegrityError("INSERT", {}, Exception("synthetic constraint failure"))
    snap = fixtures._snapshotter(repo, frontier.as_of)
    _install_replay(monkeypatch, snap, frontier, _ReplayBehavior(write))

    await snap._tick_once()

    assert len(repo.committed_chunks) == 1
    assert repo.persisted
    assert repo.frontier().next_minute is None
    assert await repo.get_portfolio_pnl_opening_checkpoint(fixtures._sample_scope()) is None
    assert checkpoint_metrics().captures_committed == 0
    assert checkpoint_metrics().capture_refusals == {"capture_upsert_failed": 1}


async def test_checkpoint_upsert_programming_error_propagates_without_silent_sample_commit(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Do not swallow non-DB programming errors inside the synthetic SAVEPOINT wrapper.

    Given: Active mode with a settling capture write whose synthetic upsert raises
        ``RuntimeError``.
    When: ``_advance_replay`` runs the chunk.
    Then: ``RuntimeError`` propagates instead of turning into ``capture_upsert_failed``.

    Args:
        monkeypatch: Pytest monkeypatch fixture for checkpoint mode and capture permissions.
    """
    reset_checkpoint_metrics()
    _enable_checkpoint_modes(monkeypatch)
    monkeypatch.setenv("PNL_SNAPSHOTTER_CHECKPOINT_MODE", "active")
    repo = fixtures._FakeRepo(credentials=[fixtures._cred("kraken")], anchor=fixtures._anchor())
    frontier = _pending(repo)
    write = await _write(monkeypatch, frontier)
    _permit_capture(monkeypatch)
    repo.upsert_failure = RuntimeError("synthetic programming bug")
    snap = fixtures._snapshotter(repo, frontier.as_of)
    _install_replay(monkeypatch, snap, frontier, _ReplayBehavior(write))

    with pytest.raises(RuntimeError, match="synthetic programming bug"):
        await snap._advance_replay(cast(Repository, repo), _context(snap, frontier), frontier)

    assert repo.committed_chunks == []


async def test_outer_tx_abort_on_checkpoint_write_retries_with_capture_write_failed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Engage the ``capture_write_failed`` retry when checkpoint persistence aborts the commit.

    Given: Active mode with a settling capture write whose initial commit aborts the outer
        transaction with ``OperationalError`` when a checkpoint write is attached.
    When: ``_tick_once`` commits the chunk.
    Then: ``capture_refusals`` records ``capture_write_failed``, the fallback retry commits
        with ``checkpoint=None``, and financial sample rows still persist.

    Args:
        monkeypatch: Pytest monkeypatch fixture for checkpoint mode and commit interception.
    """
    reset_checkpoint_metrics()
    _enable_checkpoint_modes(monkeypatch)
    monkeypatch.setenv("PNL_SNAPSHOTTER_CHECKPOINT_MODE", "active")
    repo = fixtures._FakeRepo(credentials=[fixtures._cred("kraken")], anchor=fixtures._anchor())
    frontier = _pending(repo)
    write = await _write(monkeypatch, frontier)
    _permit_capture(monkeypatch)
    snap = fixtures._snapshotter(repo, frontier.as_of)
    _install_replay(monkeypatch, snap, frontier, _ReplayBehavior(write))
    original_commit = repo.commit_portfolio_pnl_correction_chunk
    checkpoint_args: list[PnlOpeningCheckpointWrite | None] = []

    async def abort_once(
        scope: PortfolioPnlSampleScope,
        expected: PnlCorrectionFrontier,
        chunk: PnlCorrectionChunk,
        checkpoint: PnlOpeningCheckpointWrite | None = None,
    ) -> PnlCorrectionFrontier:
        """Abort the outer commit once with OperationalError when a checkpoint write is attached."""
        checkpoint_args.append(checkpoint)
        if checkpoint is not None:
            raise OperationalError("INSERT", {}, Exception("outer transaction aborted"))
        return await original_commit(scope, expected, chunk, checkpoint=None)

    monkeypatch.setattr(repo, "commit_portfolio_pnl_correction_chunk", abort_once)

    await snap._tick_once()

    assert len(checkpoint_args) == 2
    assert checkpoint_args[0] is not None
    assert checkpoint_args[1] is None
    assert len(repo.committed_chunks) == 1
    assert repo.persisted
    assert checkpoint_metrics().captures_committed == 0
    assert checkpoint_metrics().capture_refusals == {"capture_write_failed": 1}


async def test_verify_error_becomes_closed_refusal_and_commits_anchor(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Convert unexpected checkpoint verification exceptions to ``checkpoint_verify_error``.

    Given: Active mode with a stored record whose verification read raises ``RuntimeError``.
    When: ``_tick_once`` advances the chunk.
    Then: Verification records ``checkpoint_verify_error`` without exposing the raw wallet id,
        falls back to anchor replay, and commits the chunk cleanly.
    """
    reset_checkpoint_metrics()
    _enable_checkpoint_modes(monkeypatch)
    monkeypatch.setenv("PNL_SNAPSHOTTER_CHECKPOINT_MODE", "active")
    repo = fixtures._FakeRepo(credentials=[fixtures._cred("kraken")], anchor=fixtures._anchor())
    frontier = _pending(repo)
    write = await _write(monkeypatch, frontier)
    resume = await _resume(monkeypatch)
    _permit_capture(monkeypatch)
    repo._opening_checkpoints[frontier.key] = replace(
        resume.record,
        certificate=replace(
            resume.record.certificate,
            key=frontier.key,
            source_generation=frontier.generation,
        ),
    )
    snap = fixtures._snapshotter(repo, frontier.as_of)
    _install_replay(monkeypatch, snap, frontier, _ReplayBehavior(write))
    monkeypatch.setattr(
        dispatch,
        "verify_checkpoint_for_resume",
        AsyncMock(side_effect=RuntimeError("transient verify timeout")),
    )

    with caplog.at_level(logging.WARNING):
        await snap._tick_once()

    assert len(repo.committed_chunks) == 1
    assert checkpoint_metrics().fallbacks == {"checkpoint_verify_error": 1}
    assert fixtures._WALLET not in caplog.text


@pytest.mark.parametrize(
    ("decision", "charged"),
    [
        (PnlCheckpointDecision(recapture="fallback", verification_attempted=True), 1),
        (PnlCheckpointDecision(recapture="absent"), 0),
    ],
)
async def test_shadow_charges_comparison_slot_when_verification_runs_without_resume(
    monkeypatch: pytest.MonkeyPatch,
    decision: PnlCheckpointDecision,
    charged: int,
) -> None:
    """Charge the shadow slot for the verification work, not only for a comparison.

    Given: Shadow mode and a resume decision that either ran a refused verification
        or found no stored checkpoint at all.
    When: The chunk is dispatched.
    Then: A refused verification consumes one comparison slot while an absent
        checkpoint consumes none, and the anchor chunk commits either way.

    Args:
        monkeypatch: Pytest fixture installing the decision and replay doubles.
        decision: Decision returned by the patched resume preparation.
        charged: Expected comparison slots consumed by the dispatch.
    """
    reset_checkpoint_metrics()
    monkeypatch.setenv("PNL_SNAPSHOTTER_CHECKPOINT_MODE", "shadow")
    repo = fixtures._FakeRepo()
    frontier = _pending(repo)
    write = await _write(monkeypatch, frontier)
    _permit_capture(monkeypatch)
    snap = fixtures._snapshotter(repo, frontier.as_of)
    _install_replay(monkeypatch, snap, frontier, _ReplayBehavior(write))

    async def prepare(request: PnlCheckpointResumeRequest) -> PnlCheckpointDecision:
        """Return the parametrised pre-replay decision."""
        del request
        return decision

    monkeypatch.setattr(pnl_snapshotter, "prepare_checkpoint_resume", prepare)

    await snap._advance_replay(cast(Repository, repo), _context(snap, frontier), frontier)

    assert snap._tick_shadow_comparisons == charged
    assert len(repo.committed_chunks) == 1


async def test_shadow_planning_is_side_effect_free_on_sequence_and_cursor(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Keep shadow planning side-effect-free with respect to ``_sequence`` and frontier cursor.

    Given: Identical single-minute settling frontiers executed under ``off`` and ``shadow``
        modes with a verified checkpoint, plus direct invocation of ``_plan_shadow_chunk``.
    When: Both ticks complete and ``_plan_shadow_chunk`` plans a non-empty edit window.
    Then: ``shadow_snap._sequence`` and ``shadow_repo.frontier()`` match the ``off`` tick,
        and direct ``_plan_shadow_chunk`` leaves ``shadow_snap._sequence`` and cursor unchanged.
    """
    monkeypatch.setenv("PNL_SNAPSHOTTER_CHECKPOINT_MODE", "off")
    off_repo = fixtures._FakeRepo(credentials=[fixtures._cred("kraken")], anchor=fixtures._anchor())
    off_frontier = _pending(off_repo)
    off_snap = fixtures._snapshotter(off_repo, off_frontier.as_of)
    _install_replay(monkeypatch, off_snap, off_frontier, _ReplayBehavior())
    await off_snap._tick_once()

    reset_checkpoint_metrics()
    monkeypatch.setenv("PNL_SNAPSHOTTER_CHECKPOINT_MODE", "shadow")
    shadow_repo = fixtures._FakeRepo(
        credentials=[fixtures._cred("kraken")],
        anchor=fixtures._anchor(),
    )
    shadow_frontier = _pending(shadow_repo)
    resume = await _resume(monkeypatch)
    _permit_capture(monkeypatch)
    _install_resume(monkeypatch, shadow_repo, shadow_frontier, resume)
    shadow_snap = fixtures._snapshotter(shadow_repo, shadow_frontier.as_of)
    _install_replay(monkeypatch, shadow_snap, shadow_frontier, _ReplayBehavior())
    await shadow_snap._tick_once()

    assert shadow_snap._sequence == off_snap._sequence
    assert shadow_snap._sequence == len(off_repo.persisted)
    assert [row["sequence_id"] for row in shadow_repo.persisted] == [
        row["sequence_id"] for row in off_repo.persisted
    ]
    assert shadow_repo.frontier() == off_repo.frontier()

    seq_before = shadow_snap._sequence
    frontier_before = shadow_repo.frontier()
    empty_ctx = _context(shadow_snap, frontier_before)
    plan_window = ChunkWindow(fixtures._minute(3), fixtures._minute(3))
    evidence = await shadow_repo.get_portfolio_pnl_correction_evidence(
        empty_ctx.scope,
        plan_window.start,
        plan_window.end,
    )
    await shadow_snap._plan_shadow_chunk(
        cast(Repository, shadow_repo),
        empty_ctx,
        fixtures._series(plan_window.start, plan_window.end, metadata=fixtures._meta(seq=4)),
        _PlanBasis(evidence),
    )
    assert shadow_snap._sequence == seq_before
    assert shadow_repo.frontier() == frontier_before


async def test_shadow_budget_split_reuses_anchor_chunk_without_window_mismatch(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Compare the resumed replay over the actual split anchor window.

    Given: A two-minute request whose cold anchor replay splits to its first minute.
    When: Shadow dispatch resumes and compares the chunk.
    Then: Both calls use the split window and no mismatch or false divergence is recorded.

    Args:
        monkeypatch: Pytest fixture installing resume and split replay behavior.
    """
    reset_checkpoint_metrics()
    monkeypatch.setenv("PNL_SNAPSHOTTER_CHECKPOINT_MODE", "shadow")
    repo = fixtures._FakeRepo()
    frontier = replace(_pending(repo), tip=fixtures._minute(4))
    repo._frontiers[frontier.key] = frontier
    resume = await _resume(monkeypatch)
    _permit_capture(monkeypatch)
    _install_resume(monkeypatch, repo, frontier, resume)
    snap = fixtures._snapshotter(repo, frontier.as_of)
    events = _install_replay(
        monkeypatch,
        snap,
        frontier,
        _ReplayBehavior(split_once=True),
    )
    chunks: list[ChunkWindow] = []
    original_replay = snap._replay_series

    async def recording_replay(
        unused_repo: Repository,
        ctx: _ScopeContext,
        chunk: ChunkWindow,
        current: PnlCorrectionFrontier,
        checkpoint: _CheckpointReplayOptions | None = None,
    ) -> tuple[ChunkWindow, PnlWalletSeriesResult]:
        """Record requested windows around the deterministic replay double."""
        chunks.append(chunk)
        return await original_replay(unused_repo, ctx, chunk, current, checkpoint)

    monkeypatch.setattr(snap, "_replay_series", recording_replay)
    await snap._advance_replay(cast(Repository, repo), _context(snap, frontier), frontier)

    assert events == ["anchor", "resume"]
    assert chunks == [
        ChunkWindow(fixtures._minute(3), fixtures._minute(4)),
        ChunkWindow(fixtures._minute(3), fixtures._minute(3)),
    ]
    assert checkpoint_metrics().shadow_comparisons == 1
    assert checkpoint_metrics().shadow_skipped == {}
    assert checkpoint_metrics().shadow_mismatches == 0


async def test_shadow_dispatch_skips_window_mismatch_without_false_divergence(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Record ``window_mismatch`` and keep checkpoint when anchor and resumed windows differ.

    Given: Shadow mode with a verified checkpoint where the resumed branch serves a different
        chunk end than the anchor branch and diverges inside the series.
    When: ``_advance_replay`` runs shadow comparison.
    Then: Comparison skips with ``window_mismatch``, zero mismatches are counted, and the
        stored checkpoint survives without deletion.
    """
    reset_checkpoint_metrics()
    monkeypatch.setenv("PNL_SNAPSHOTTER_CHECKPOINT_MODE", "shadow")
    repo = fixtures._FakeRepo()
    frontier = replace(_pending(repo), tip=fixtures._minute(4))
    repo._frontiers[frontier.key] = frontier
    resume = await _resume(monkeypatch)
    _permit_capture(monkeypatch)
    _install_resume(monkeypatch, repo, frontier, resume)
    repo._opening_checkpoints[frontier.key] = replace(
        resume.record,
        certificate=replace(
            resume.record.certificate,
            key=frontier.key,
            source_generation=frontier.generation,
        ),
    )
    snap = fixtures._snapshotter(repo, frontier.as_of)

    async def mismatching_replay(
        unused_repo: Repository,
        ctx: _ScopeContext,
        chunk: ChunkWindow,
        current: PnlCorrectionFrontier,
        checkpoint: _CheckpointReplayOptions | None = None,
    ) -> tuple[ChunkWindow, PnlWalletSeriesResult]:
        """Return distinct chunk windows and values for resumed vs anchor branch."""
        del unused_repo, ctx, current
        checkpoint = checkpoint or _CheckpointReplayOptions()
        if checkpoint.resume is not None:
            smaller = ChunkWindow(chunk.start, fixtures._minute(2))
            base = fixtures._series(smaller.start, chunk.start, metadata=fixtures._meta(seq=4))
            return smaller, replace(
                base,
                points=(replace(base.points[0], realized_pnl=777.0),),
            )
        return chunk, fixtures._series(chunk.start, chunk.end, metadata=fixtures._meta(seq=4))

    monkeypatch.setattr(snap, "_replay_series", mismatching_replay)
    await snap._advance_replay(cast(Repository, repo), _context(snap, frontier), frontier)

    assert checkpoint_metrics().shadow_skipped == {"window_mismatch": 1}
    assert checkpoint_metrics().shadow_mismatches == 0
    assert checkpoint_metrics().shadow_comparisons == 0
    assert await repo.get_portfolio_pnl_opening_checkpoint(fixtures._sample_scope()) is not None


async def test_disappearing_scope_prunes_metrics_and_hides_raw_wallet_in_logs(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Prune disappeared per-scope metrics and keep raw wallet ids out of checkpoint logs.

    Given: Two active wallets generating checkpoint fallback logs, followed by a tick where the
        second wallet leaves active credentials.
    When: Both ticks run through ``_tick_once``.
    Then: The removed wallet's metrics bucket is pruned and neither raw wallet id appears in logs.
    """
    reset_checkpoint_metrics()
    _enable_checkpoint_modes(monkeypatch)
    monkeypatch.setenv("PNL_SNAPSHOTTER_CHECKPOINT_MODE", "active")
    _permit_capture(monkeypatch)
    wallet_a = fixtures._WALLET
    wallet_b = "0000face-0000-7000-8000-0000000000b1"
    repo = fixtures._FakeRepo(
        credentials=[
            fixtures._cred("kraken", wallet=wallet_a),
            fixtures._cred("kraken", wallet=wallet_b),
        ],
        anchor=fixtures._anchor(),
    )
    frontier_a = _pending(repo)
    frontier_b = replace(frontier_a, key=replace(frontier_a.key, wallet_public_id=wallet_b))
    repo._frontiers[frontier_b.key] = frontier_b
    write_a = await _write(monkeypatch, frontier_a)
    write_b = replace(
        write_a,
        certificate=replace(write_a.certificate, key=frontier_b.key),
    )

    async def anchor_for(
        wallet: str, mode: str, ccy: str, as_of: datetime | None
    ) -> PortfolioPnlAnchorRow:
        """Return matching active USD anchor per wallet."""
        del mode, ccy, as_of
        return cast(PortfolioPnlAnchorRow, {**fixtures._anchor(), "wallet_public_id": wallet})

    monkeypatch.setattr(repo, "get_portfolio_pnl_anchor", anchor_for)
    snap = fixtures._snapshotter(repo, frontier_a.as_of)

    async def fake_series(
        unused_repo: object,
        ctx: _ScopeContext,
        chunk: ChunkWindow,
        options: object,
    ) -> PnlWalletSeriesResult:
        """Return a canned series without DB reads for probe and replay."""
        del unused_repo, ctx, options
        return fixtures._series(chunk.start, chunk.end, metadata=fixtures._meta(seq=4))

    async def replay(
        unused_repo: Repository,
        ctx: _ScopeContext,
        chunk: ChunkWindow,
        current: PnlCorrectionFrontier,
        checkpoint: _CheckpointReplayOptions | None = None,
    ) -> tuple[ChunkWindow, PnlWalletSeriesResult]:
        """Return staged captures for both wallets."""
        del unused_repo, current, checkpoint
        write = write_a if ctx.wallet_public_id == wallet_a else write_b
        return chunk, replace(
            fixtures._series(chunk.start, chunk.end, metadata=fixtures._meta(seq=4)),
            checkpoint_write=write,
        )

    monkeypatch.setattr(snap, "_series", fake_series)
    monkeypatch.setattr(snap, "_replay_series", replay)
    with caplog.at_level(logging.WARNING, logger="snapper.application.portfolio"):
        await snap._tick_once()
        key_a = checkpoint_scope_key((wallet_a, "live", "USD"))
        key_b = checkpoint_scope_key((wallet_b, "live", "USD"))
        assert set(checkpoint_metrics_by_scope()) == {key_a, key_b}
        repo._credentials = [fixtures._cred("kraken", wallet=wallet_a)]
        await snap._tick_once()

    assert set(checkpoint_metrics_by_scope()) == {key_a}
    assert wallet_a not in caplog.text
    assert wallet_b not in caplog.text


async def test_tick_recaptures_are_bounded_by_max_recaptures_per_tick(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Bound anchor recaptures over ``(cutoff, frontier]`` per tick to ``MAX_RECAPTURES_PER_TICK``.

    Given: ``MAX_RECAPTURES_PER_TICK`` set to ``1`` and three wallets needing recapture in one tick.
    When: A single ``_tick_once`` processes all three wallets.
    Then: At most ``MAX_RECAPTURES_PER_TICK`` capture occurs while all three chunks still commit.
    """
    reset_checkpoint_metrics()
    _enable_checkpoint_modes(monkeypatch)
    monkeypatch.setenv("PNL_SNAPSHOTTER_CHECKPOINT_MODE", "active")
    _permit_capture(monkeypatch)
    assert isinstance(dispatch.MAX_RECAPTURES_PER_TICK, int)
    assert dispatch.MAX_RECAPTURES_PER_TICK >= 1
    monkeypatch.setattr(dispatch, "MAX_RECAPTURES_PER_TICK", 1)

    wallets = (
        "0000face-0000-7000-8000-0000000000c1",
        "0000face-0000-7000-8000-0000000000c2",
        "0000face-0000-7000-8000-0000000000c3",
    )
    repo = fixtures._FakeRepo(
        credentials=[fixtures._cred("kraken", wallet=w) for w in wallets],
        anchor=fixtures._anchor(),
    )
    base_frontier = _pending(repo)
    repo._frontiers.clear()
    writes_by_wallet: dict[str, PnlOpeningCheckpointWrite] = {}
    base_write = await _write(monkeypatch, base_frontier)
    for wallet in wallets:
        wf = replace(base_frontier, key=replace(base_frontier.key, wallet_public_id=wallet))
        repo._frontiers[wf.key] = wf
        writes_by_wallet[wallet] = replace(
            base_write,
            certificate=replace(base_write.certificate, key=wf.key),
        )

    async def anchor_for(
        wallet: str, mode: str, ccy: str, as_of: datetime | None
    ) -> PortfolioPnlAnchorRow:
        """Return matching active USD anchor per wallet."""
        del mode, ccy, as_of
        return cast(PortfolioPnlAnchorRow, {**fixtures._anchor(), "wallet_public_id": wallet})

    monkeypatch.setattr(repo, "get_portfolio_pnl_anchor", anchor_for)
    snap = fixtures._snapshotter(repo, base_frontier.as_of)

    async def fake_series(
        unused_repo: object,
        ctx: _ScopeContext,
        chunk: ChunkWindow,
        options: object,
    ) -> PnlWalletSeriesResult:
        """Return a canned series without DB reads for probe and replay."""
        del unused_repo, ctx, options
        return fixtures._series(chunk.start, chunk.end, metadata=fixtures._meta(seq=4))

    async def replay(
        unused_repo: Repository,
        ctx: _ScopeContext,
        chunk: ChunkWindow,
        current: PnlCorrectionFrontier,
        checkpoint: _CheckpointReplayOptions | None = None,
    ) -> tuple[ChunkWindow, PnlWalletSeriesResult]:
        """Emit capture writes only when capture was permitted by tick budget."""
        del unused_repo, current
        checkpoint = checkpoint or _CheckpointReplayOptions()
        return chunk, replace(
            fixtures._series(chunk.start, chunk.end, metadata=fixtures._meta(seq=4)),
            checkpoint_write=(
                writes_by_wallet[ctx.wallet_public_id] if checkpoint.capture is not None else None
            ),
        )

    monkeypatch.setattr(snap, "_series", fake_series)
    monkeypatch.setattr(snap, "_replay_series", replay)
    await snap._tick_once()

    assert len(repo.committed_chunks) == 3
    assert checkpoint_metrics().captures_committed == 1
    assert sum(checkpoint_metrics().recaptures.values()) == 1
    assert checkpoint_metrics().capture_refusals == {"capture_recapture_budget_exceeded": 2}


def test_exhausted_recapture_budget_records_only_tip_capture_refusal(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Report a bounded recapture omission only for an otherwise capturable tip chunk.

    Given: An exhausted tick recapture budget and a complete pending frontier.
    When: Tip and non-tip chunks request recapture.
    Then: Only the tip request records the closed budget-exceeded refusal.

    Args:
        monkeypatch: Pytest monkeypatch fixture for capture permission.
    """
    reset_checkpoint_metrics()
    _permit_capture(monkeypatch)
    repo = fixtures._FakeRepo()
    frontier = replace(_pending(repo), tip=fixtures._minute(4))
    snap = fixtures._snapshotter(repo, frontier.as_of)
    snap._tick_recaptures = dispatch.MAX_RECAPTURES_PER_TICK
    start = frontier.next_minute
    assert start is not None
    request = pnl_snapshotter._CheckpointDispatchRequest(
        cast(Repository, repo),
        _context(snap, frontier),
        frontier,
        ChunkWindow(start, frontier.tip),
        0.0,
        None,
        None,
    )
    assert snap._consume_recapture(request, frontier.tip) is None
    assert snap._consume_recapture(request, start) is None
    assert checkpoint_metrics().capture_refusals == {"capture_recapture_budget_exceeded": 1}


async def test_commit_without_checkpoint_reraises_database_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Do not convert an ordinary financial commit failure into a checkpoint refusal.

    Given: A commit plan with no staged checkpoint write.
    When: The financial commit raises a SQLAlchemy failure.
    Then: The original failure propagates without a checkpoint-only retry.

    Args:
        monkeypatch: Pytest monkeypatch fixture for the failing repository commit.
    """
    repo = fixtures._FakeRepo()
    frontier = _pending(repo)
    snap = fixtures._snapshotter(repo, frontier.as_of)
    start = frontier.next_minute
    assert start is not None
    chunk = ChunkWindow(start, frontier.tip)
    request = pnl_snapshotter._CheckpointDispatchRequest(
        cast(Repository, repo),
        _context(snap, frontier),
        frontier,
        chunk,
        0.0,
        None,
        None,
    )
    monkeypatch.setattr(
        repo,
        "commit_portfolio_pnl_correction_chunk",
        AsyncMock(side_effect=OperationalError("UPDATE", {}, Exception("commit failed"))),
    )
    with pytest.raises(OperationalError, match="commit failed"):
        await snap._commit_checkpoint_plan(
            request,
            chunk,
            fixtures._series(start, frontier.tip),
            shadow_cases._plan(frontier),
        )


@pytest.mark.parametrize("mode", ["active", "shadow"])
async def test_resume_fallback_without_recapture_avoids_second_anchor_replay(
    monkeypatch: pytest.MonkeyPatch,
    mode: str,
) -> None:
    """Reuse the anchor result already returned by the resumed fallback when not recapturing.

    Given: Active or shadow mode with a verified checkpoint, a non-tip chunk where recapture
        is ineligible, and a resumed call whose service fallback returns the anchor series.
    When: ``_advance_replay`` dispatches the chunk.
    Then: No second anchor replay runs, the stale checkpoint is invalidated, and the chunk commits.

    Args:
        monkeypatch: Pytest monkeypatch fixture for checkpoint mode and service stubbing.
        mode: Checkpoint dispatch mode under test.
    """
    reset_checkpoint_metrics()
    _enable_checkpoint_modes(monkeypatch)
    monkeypatch.setenv("PNL_SNAPSHOTTER_CHECKPOINT_MODE", mode)
    repo = fixtures._FakeRepo()
    frontier = replace(_pending(repo), tip=fixtures._minute(4))
    repo._frontiers[frontier.key] = frontier
    write = await _write(monkeypatch, frontier)
    resume = await _resume(monkeypatch)
    _permit_capture(monkeypatch)
    _install_resume(monkeypatch, repo, frontier, resume)
    snap = fixtures._snapshotter(repo, frontier.as_of)
    snap._replay_chunk_limits[fixtures._WALLET] = 1
    events = _install_replay(
        monkeypatch,
        snap,
        frontier,
        _ReplayBehavior(write, resume_fallback=True),
    )

    await snap._advance_replay(cast(Repository, repo), _context(snap, frontier), frontier)

    assert events == (["resume"] if mode == "active" else ["anchor", "resume"])
    assert checkpoint_metrics().fallbacks == {"checkpoint_late_execution": 1}
    assert checkpoint_metrics().invalidations_deleted == 1
    assert checkpoint_metrics().resumes_succeeded == 0
    assert len(repo.committed_chunks) == 1


async def test_shadow_comparison_budget_rotates_and_over_cap_wallets_run_off(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Cap verification and resumed replay while rotating fairly across full ticks.

    Given: Three wallets with verified checkpoints and one comparison slot per tick.
    When: Three ticks advance finalized time.
    Then: Only one wallet verifies and resumes per tick in cyclic order, while every
        wallet still commits its cold anchor chunk.

    Args:
        monkeypatch: Pytest fixture installing wallet resumes and the comparison cap.
    """
    reset_checkpoint_metrics()
    _enable_checkpoint_modes(monkeypatch)
    monkeypatch.setenv("PNL_SNAPSHOTTER_CHECKPOINT_MODE", "shadow")
    _permit_capture(monkeypatch)
    monkeypatch.setattr(pnl_snapshotter, "MAX_SHADOW_COMPARISONS_PER_TICK", 1)
    wallets = (
        "0000face-0000-7000-8000-0000000000d1",
        "0000face-0000-7000-8000-0000000000d2",
        "0000face-0000-7000-8000-0000000000d3",
    )
    repo = fixtures._FakeRepo(
        credentials=[fixtures._cred("kraken", wallet=wallet) for wallet in wallets],
        anchor=fixtures._anchor(),
    )
    base_frontier = _pending(repo)
    repo._frontiers.clear()
    base_resume = await _resume(monkeypatch)
    resumes: dict[str, PnlCheckpointResume] = {}
    for wallet in wallets:
        frontier = replace(
            base_frontier,
            key=replace(base_frontier.key, wallet_public_id=wallet),
        )
        repo._frontiers[frontier.key] = frontier
        bound = replace(
            base_resume,
            record=replace(
                base_resume.record,
                certificate=replace(
                    base_resume.record.certificate,
                    key=frontier.key,
                    source_generation=frontier.generation,
                ),
            ),
        )
        resumes[wallet] = bound
        repo._opening_checkpoints[frontier.key] = bound.record

    async def anchor_for(
        wallet: str,
        mode: str,
        ccy: str,
        as_of: datetime | None,
    ) -> PortfolioPnlAnchorRow:
        """Return an active anchor bound to each scheduled wallet."""
        del mode, ccy, as_of
        return cast(PortfolioPnlAnchorRow, {**fixtures._anchor(), "wallet_public_id": wallet})

    verified_by_tick: list[list[str]] = []
    resumed_by_tick: list[list[str]] = []
    current_verified: list[str] = []
    current_resumed: list[str] = []

    async def fake_series(
        unused_repo: object,
        ctx: _ScopeContext,
        chunk: ChunkWindow,
        options: object,
    ) -> PnlWalletSeriesResult:
        """Return one settled probe without exercising the anchor fixture parser."""
        del unused_repo, ctx, options
        return fixtures._series(chunk.start, chunk.end, metadata=fixtures._meta(seq=4))

    async def prepare(request: PnlCheckpointResumeRequest) -> PnlCheckpointDecision:
        """Return the wallet's verified resume and record each verification slot."""
        current_verified.append(request.scope.wallet_public_id)
        return PnlCheckpointDecision(resume=resumes[request.scope.wallet_public_id])

    async def replay(
        unused_repo: Repository,
        ctx: _ScopeContext,
        chunk: ChunkWindow,
        current: PnlCorrectionFrontier,
        checkpoint: _CheckpointReplayOptions | None = None,
    ) -> tuple[ChunkWindow, PnlWalletSeriesResult]:
        """Return matching series and record only resumed wallet work."""
        del unused_repo, current
        if checkpoint is not None and checkpoint.resume is not None:
            current_resumed.append(ctx.wallet_public_id)
        return chunk, fixtures._series(chunk.start, chunk.end, metadata=fixtures._meta(seq=4))

    monkeypatch.setattr(repo, "get_portfolio_pnl_anchor", anchor_for)
    monkeypatch.setattr(pnl_snapshotter, "prepare_checkpoint_resume", prepare)
    clock_state = {"as_of": base_frontier.as_of}
    snap = fixtures._snapshotter(repo, base_frontier.as_of)
    snap._clock = lambda: clock_state["as_of"]
    monkeypatch.setattr(snap, "_series", fake_series)
    monkeypatch.setattr(snap, "_replay_series", replay)
    for step in range(3):
        clock_state["as_of"] = (
            base_frontier.tip + pnl_snapshotter.FINALIZATION_LAG + step * pnl_snapshotter._MINUTE
        )
        current_verified = []
        current_resumed = []
        await snap._tick_once()
        verified_by_tick.append(current_verified)
        resumed_by_tick.append(current_resumed)

    assert (verified_by_tick, resumed_by_tick) == (
        [[wallet] for wallet in wallets],
        [[wallet] for wallet in wallets],
    )
    assert (len(repo.committed_chunks), checkpoint_metrics().shadow_comparisons) == (9, 3)


async def test_recapture_budget_rotates_fairly_across_ticks_including_on_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Rotate recapture turn across ticks so failing earlier scopes cannot starve peers.

    Given: ``MAX_RECAPTURES_PER_TICK == 1`` and three wallets needing recapture across three
        ticks as finalized time advances, where the first two wallets fail whenever they
        consume recapture budget.
    When: Three consecutive ticks run via ``_tick_once``.
    Then: Each tick gives recapture turn to the next wallet in cyclic order (``c1``, then
        ``c2``, then ``c3``) while non-failing scopes still advance and commit samples.

    Args:
        monkeypatch: Pytest monkeypatch fixture for checkpoint mode and recapture budget.
    """
    reset_checkpoint_metrics()
    _enable_checkpoint_modes(monkeypatch)
    monkeypatch.setenv("PNL_SNAPSHOTTER_CHECKPOINT_MODE", "active")
    _permit_capture(monkeypatch)
    monkeypatch.setattr(dispatch, "MAX_RECAPTURES_PER_TICK", 1)

    wallets = (
        "0000face-0000-7000-8000-0000000000c1",
        "0000face-0000-7000-8000-0000000000c2",
        "0000face-0000-7000-8000-0000000000c3",
    )
    failing_recaptures = {wallets[0], wallets[1]}
    repo = fixtures._FakeRepo(
        credentials=[fixtures._cred("kraken", wallet=w) for w in wallets],
        anchor=fixtures._anchor(),
    )
    base_frontier = _pending(repo)
    repo._frontiers.clear()
    writes_by_wallet: dict[str, PnlOpeningCheckpointWrite] = {}
    base_write = await _write(monkeypatch, base_frontier)
    for wallet in wallets:
        wf = replace(base_frontier, key=replace(base_frontier.key, wallet_public_id=wallet))
        repo._frontiers[wf.key] = wf
        writes_by_wallet[wallet] = replace(
            base_write,
            certificate=replace(base_write.certificate, key=wf.key),
        )

    async def anchor_for(
        wallet: str, mode: str, ccy: str, as_of: datetime | None
    ) -> PortfolioPnlAnchorRow:
        """Return matching active USD anchor per wallet."""
        del mode, ccy, as_of
        return cast(PortfolioPnlAnchorRow, {**fixtures._anchor(), "wallet_public_id": wallet})

    monkeypatch.setattr(repo, "get_portfolio_pnl_anchor", anchor_for)
    clock_state = {"as_of": base_frontier.as_of}
    snap = fixtures._snapshotter(repo, base_frontier.as_of)
    snap._clock = lambda: clock_state["as_of"]
    captured_by_tick: list[list[str]] = []
    current_tick: list[str] = []

    async def fake_series(
        unused_repo: object,
        ctx: _ScopeContext,
        chunk: ChunkWindow,
        options: object,
    ) -> PnlWalletSeriesResult:
        """Return a valid probe series when settled scopes advance to a new tip."""
        del unused_repo, ctx, options
        return fixtures._series(chunk.start, chunk.end, metadata=fixtures._meta(seq=4))

    async def replay(
        unused_repo: Repository,
        ctx: _ScopeContext,
        chunk: ChunkWindow,
        current: PnlCorrectionFrontier,
        checkpoint: _CheckpointReplayOptions | None = None,
    ) -> tuple[ChunkWindow, PnlWalletSeriesResult]:
        """Track capture attempts and fail recapturing scopes configured to error."""
        del unused_repo
        checkpoint = checkpoint or _CheckpointReplayOptions()
        if checkpoint.capture is not None:
            current_tick.append(ctx.wallet_public_id)
            if ctx.wallet_public_id in failing_recaptures:
                raise RuntimeError(f"synthetic recapture failure for {ctx.wallet_public_id}")
        write = replace(
            writes_by_wallet[ctx.wallet_public_id],
            certificate=replace(
                writes_by_wallet[ctx.wallet_public_id].certificate,
                key=current.key,
                source_generation=current.generation,
                checkpoint_minute=current.tip,
                as_of=current.as_of,
            ),
        )
        return chunk, replace(
            fixtures._series(chunk.start, chunk.end, metadata=fixtures._meta(seq=4)),
            checkpoint_write=write if checkpoint.capture is not None else None,
        )

    monkeypatch.setattr(snap, "_series", fake_series)
    monkeypatch.setattr(snap, "_replay_series", replay)
    for step in range(3):
        clock_state["as_of"] = (
            base_frontier.tip + pnl_snapshotter.FINALIZATION_LAG + step * pnl_snapshotter._MINUTE
        )
        current_tick = []
        await snap._tick_once()
        captured_by_tick.append(current_tick)

    assert captured_by_tick == [[wallets[0]], [wallets[1]], [wallets[2]]]
    assert checkpoint_metrics().captures_committed == 1
    assert (
        await repo.get_portfolio_pnl_opening_checkpoint(
            replace(fixtures._sample_scope(), wallet_public_id=wallets[2])
        )
        is not None
    )


def test_post_commit_cancellation_keeps_the_replay_chunk_limit(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Back off replay work only for wallets whose authoritative chunk did not commit.

    Given: One wallet marked as committed this iteration and another that was not.
    When: A tick cancellation reaches each wallet's handler.
    Then: The committed wallet keeps its replay chunk limit and logs the shadow
        overrun, while the uncommitted wallet's limit is halved as before.

    Args:
        caplog: Log capture for the shadow overrun warning.
    """
    repo = fixtures._FakeRepo()
    snap = fixtures._snapshotter(repo, fixtures._minute(5))
    full = snap._replay_chunk_limit("committed")
    snap._post_commit_wallet = "committed"
    with caplog.at_level(logging.WARNING, logger=pnl_snapshotter.__name__):
        snap._reduce_replay_chunk_after_cancel("committed")
    assert snap._replay_chunk_limit("committed") == full
    assert "replay chunk limit kept" in caplog.text
    snap._reduce_replay_chunk_after_cancel("pending")
    assert snap._replay_chunk_limit("pending") == max(1, full // 2)
