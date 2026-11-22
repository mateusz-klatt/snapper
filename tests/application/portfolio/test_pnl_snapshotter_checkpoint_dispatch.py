"""Adversarial dispatch tests for snapshotter opening-checkpoint modes."""

import logging
from dataclasses import dataclass
from dataclasses import replace
from datetime import datetime
from typing import cast
from unittest.mock import AsyncMock

import pytest
from sqlalchemy.exc import IntegrityError
from sqlalchemy.exc import OperationalError

from snapper.application.portfolio import pnl_engine_options
from snapper.application.portfolio import pnl_snapshotter
from snapper.application.portfolio import pnl_snapshotter_checkpoint as dispatch
from snapper.application.portfolio import pnl_snapshotter_config
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
from snapper.application.portfolio.pnl_snapshotter import _ScopeContext
from snapper.application.portfolio.pnl_snapshotter_checkpoint import PnlCheckpointDecision
from snapper.application.portfolio.pnl_snapshotter_checkpoint import PnlCheckpointResumeRequest
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
        refusal = None
        staged = write if capture is not None else None
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
async def test_off_and_production_modes_never_read_a_checkpoint(
    monkeypatch: pytest.MonkeyPatch,
    mode: str,
    capture_permitted: bool,
) -> None:
    """Keep off query-free and production shadow/active inert regardless of capture permission.

    Given: A clean settling chunk with capture either disallowed or permitted.
    When: Off runs (or shadow/active runs under the default kill switch -> effective off).
    Then: No checkpoint read or capture dispatch ever occurs when effective mode is ``off``,
        while enabled non-off modes with ``capture_permitted=False`` count permission refusal.
    """
    reset_checkpoint_metrics()
    if capture_permitted:
        _permit_capture(monkeypatch)
    if mode != "off" and not capture_permitted:
        _enable_checkpoint_modes(monkeypatch)
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
    _enable_checkpoint_modes(monkeypatch)
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
    _enable_checkpoint_modes(monkeypatch)
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
    Then: The old generation is deleted and an anchor capture commits instead.
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

    expected = ["anchor", "resume", "capture"] if mode == "shadow" else ["resume", "capture"]
    assert events == expected
    assert checkpoint_metrics().fallbacks == {"checkpoint_late_execution": 1}
    assert checkpoint_metrics().invalidations_deleted == 1
    assert checkpoint_metrics().resumes_succeeded == 0
    assert len(repo.committed_chunks) == 1


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
    _enable_checkpoint_modes(monkeypatch)
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
    _enable_checkpoint_modes(monkeypatch)
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
            "capture_refusals={capture_not_permitted:1} "
            "resumes_attempted=0 resumes_succeeded=0 "
            "fallbacks={} invalidations_deleted=0 "
            "shadow_comparisons=0 shadow_mismatches=0 "
            "shadow_skipped={} recaptures={}"
        ),
        (
            "checkpoint_metrics_summary "
            f"wallet={alias_b} mode=live ccy=USD "
            "captures_committed=0 "
            "capture_refusals={capture_not_permitted:1} "
            "resumes_attempted=0 resumes_succeeded=0 "
            "fallbacks={} invalidations_deleted=0 "
            "shadow_comparisons=0 shadow_mismatches=0 "
            "shadow_skipped={} recaptures={}"
        ),
    }
    assert set(lines) == expected_lines
    assert wallet_a not in "\n".join(lines)
    assert wallet_b not in "\n".join(lines)
    by_scope = checkpoint_metrics_by_scope()
    assert by_scope[(alias_a, "live", "USD")].capture_refusals == {"capture_not_permitted": 1}
    assert by_scope[(alias_b, "live", "USD")].capture_refusals == {"capture_not_permitted": 1}
    assert checkpoint_metrics().capture_refusals == {"capture_not_permitted": 2}


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
    _enable_checkpoint_modes(monkeypatch)
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
    _enable_checkpoint_modes(monkeypatch)
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
        evidence,
    )
    assert shadow_snap._sequence == seq_before
    assert shadow_repo.frontier() == frontier_before


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
    _enable_checkpoint_modes(monkeypatch)
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


async def test_active_resume_fallback_without_recapture_avoids_second_anchor_replay(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Reuse the anchor result already returned by the resumed service fallback when not recapturing.

    Given: Active mode with a verified checkpoint, a non-tip chunk where recapture is ineligible,
        and a resumed call whose service fallback already returns the anchor series.
    When: ``_advance_replay`` dispatches the chunk.
    Then: Only the single resumed call runs (no second anchor replay), the stale checkpoint
        is invalidated, the fallback metric increments, and the financial chunk commits.

    Args:
        monkeypatch: Pytest monkeypatch fixture for checkpoint mode and service stubbing.
    """
    reset_checkpoint_metrics()
    _enable_checkpoint_modes(monkeypatch)
    monkeypatch.setenv("PNL_SNAPSHOTTER_CHECKPOINT_MODE", "active")
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

    assert events == ["resume"]
    assert checkpoint_metrics().fallbacks == {"checkpoint_late_execution": 1}
    assert checkpoint_metrics().invalidations_deleted == 1
    assert checkpoint_metrics().resumes_succeeded == 0
    assert len(repo.committed_chunks) == 1


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
