"""Fallback, deletion, and defensive refusal tests for checkpoint dispatch helpers."""

import asyncio
from dataclasses import replace
from datetime import timedelta
from typing import cast
from unittest.mock import AsyncMock

import pytest

from snapper.application.portfolio import pnl_snapshotter_checkpoint as dispatch
from snapper.application.portfolio.pnl_opening_checkpoint_metrics import checkpoint_metrics
from snapper.application.portfolio.pnl_opening_checkpoint_metrics import reset_checkpoint_metrics
from snapper.application.portfolio.pnl_opening_checkpoint_verifier import PnlCheckpointFallback
from snapper.data.pnl_correction_state import PnlCorrectionFrontier
from snapper.data.repository import Repository
from tests.application.portfolio import test_pnl_snapshotter as fixtures
from tests.application.portfolio import test_pnl_snapshotter_checkpoint_dispatch as cases
from tests.application.portfolio import test_pnl_snapshotter_checkpoint_shadow as shadow_cases


def test_unknown_calc_version_and_incomplete_frontier_refuse_capture(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Fail closed for an unknown version and each absent frontier-cut field.

    Given: A permitted resolver after an unknown-version probe.
    When: Each required source-cut field is absent in turn.
    Then: Capture returns no request and records a ValueError refusal.
    """
    reset_checkpoint_metrics()
    assert dispatch.checkpoint_capture_permitted("unknown") is False
    repo = fixtures._FakeRepo()
    frontier = cases._pending(repo)
    cases._permit_capture(monkeypatch)
    for field in (
        "raw_execution_watermarks",
        "execution_manifest_digest",
        "expected_venues",
        "venue_event_watermarks",
        "fill_witness_digest",
    ):
        incomplete = replace(frontier, **{field: None})
        request = dispatch.capture_request_for(
            incomplete,
            fixtures._sample_scope(),
            fixtures._anchor(),
            incomplete.tip,
        )
        assert request is None
    assert checkpoint_metrics().capture_refusals == {"capture_value_error": 5}


def test_capture_request_constructor_value_error_is_soft(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Convert request validation failure to a refusal without propagating it.

    Given: A complete frontier and a constructor that raises ValueError.
    When: Capture request construction runs.
    Then: No request escapes and the refusal counter advances.
    """
    reset_checkpoint_metrics()
    repo = fixtures._FakeRepo()
    frontier = cases._pending(repo)
    cases._permit_capture(monkeypatch)

    def fail(*args: object, **kwargs: object) -> None:
        """Reject synthetic request construction."""
        del args, kwargs
        raise ValueError("invalid request")

    monkeypatch.setattr(dispatch, "PnlCheckpointCaptureRequest", fail)
    assert (
        dispatch.capture_request_for(
            frontier, fixtures._sample_scope(), fixtures._anchor(), frontier.tip
        )
        is None
    )
    assert checkpoint_metrics().capture_refusals == {"capture_value_error": 1}


async def test_prepare_resume_handles_fill_gap_and_generation_fenced_delete(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Avoid DB work on a fill gap and delete a stale verified row.

    Given: A permitted generation, first gapped then clean with a stored record.
    When: Verification returns an evidence-change fallback requiring deletion.
    Then: The gap performs no read and the stale generation is deleted and counted.
    """
    reset_checkpoint_metrics()
    cases._permit_capture(monkeypatch)
    repo = fixtures._FakeRepo()
    frontier = replace(cases._pending(repo), tip=fixtures._minute(4))
    resume = await cases._resume(monkeypatch)
    cases._permit_capture(monkeypatch)
    repo._opening_checkpoints[frontier.key] = replace(
        resume.record,
        certificate=replace(
            resume.record.certificate,
            key=frontier.key,
            source_generation=frontier.generation,
        ),
    )
    read = AsyncMock(side_effect=AssertionError("gap must avoid DB work"))
    monkeypatch.setattr(repo, "get_portfolio_pnl_opening_checkpoint", read)
    gapped = replace(frontier, has_fill_gap=True)
    request = dispatch.PnlCheckpointResumeRequest(
        cast(Repository, repo),
        fixtures._sample_scope(),
        fixtures._anchor(),
        gapped,
        fixtures._minute(3),
        "active",
    )
    decision = await dispatch.prepare_checkpoint_resume(request)
    assert decision.recapture == "fallback"
    read.assert_not_awaited()

    monkeypatch.setattr(
        repo,
        "get_portfolio_pnl_opening_checkpoint",
        repo.__class__.get_portfolio_pnl_opening_checkpoint.__get__(repo),
    )
    verify = AsyncMock(
        return_value=PnlCheckpointFallback("checkpoint_accrual_prefix_changed", True)
    )
    monkeypatch.setattr(dispatch, "verify_checkpoint_for_resume", verify)
    clean = replace(request, frontier=frontier)
    fallback = await dispatch.prepare_checkpoint_resume(clean)
    assert fallback.recapture == "fallback"
    assert await repo.get_portfolio_pnl_opening_checkpoint(fixtures._sample_scope()) is None
    assert checkpoint_metrics().invalidations_deleted == 1


async def test_prepare_resume_returns_verified_state_and_handles_start_at_tip(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Return verified state and classify a one-minute window by its stored row.

    Given: A clean generation with no row and then a verified service outcome.
    When: The start equals tip and then precedes tip.
    Then: The absent row recaptures and the verified row returns its resume state.
    """
    cases._permit_capture(monkeypatch)
    repo = fixtures._FakeRepo()
    frontier = replace(cases._pending(repo), tip=fixtures._minute(4))
    resume = await cases._resume(monkeypatch)
    cases._permit_capture(monkeypatch)
    request = dispatch.PnlCheckpointResumeRequest(
        cast(Repository, repo),
        fixtures._sample_scope(),
        fixtures._anchor(),
        frontier,
        frontier.tip,
        "active",
    )
    read = AsyncMock(return_value=None)
    monkeypatch.setattr(repo, "get_portfolio_pnl_opening_checkpoint", read)
    empty = await dispatch.prepare_checkpoint_resume(request)
    assert empty.recapture == "absent"
    assert empty.verification_attempted is False
    read.assert_awaited_once()
    read.side_effect = None
    read.return_value = replace(
        resume.record,
        certificate=replace(
            resume.record.certificate,
            key=frontier.key,
            source_generation=frontier.generation,
        ),
    )
    monkeypatch.setattr(dispatch, "verify_checkpoint_for_resume", AsyncMock(return_value=resume))
    decision = await dispatch.prepare_checkpoint_resume(replace(request, start=fixtures._minute(3)))
    assert decision.resume is resume
    assert decision.verification_attempted is True
    monkeypatch.setattr(
        dispatch,
        "verify_checkpoint_for_resume",
        AsyncMock(return_value=PnlCheckpointFallback("checkpoint_ineligible", False)),
    )
    fallback = await dispatch.prepare_checkpoint_resume(replace(request, start=fixtures._minute(3)))
    assert fallback.recapture == "fallback"
    exhausted = await dispatch.prepare_checkpoint_resume(
        replace(request, start=fixtures._minute(3), recapture_budget_exhausted=True)
    )
    assert exhausted == dispatch.PnlCheckpointDecision(verification_attempted=True)


async def test_resume_fallback_reason_and_delete_fence_miss(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Classify unexpected service fallback as corrupt and ignore a stale fence.

    Given: A stored resume whose generation no longer matches the repository row.
    When: Delete-required and non-delete service fallbacks are handled.
    Then: Corruption is counted, a fence miss is ignored, and non-delete falls back without deleting.
    """
    reset_checkpoint_metrics()
    repo = fixtures._FakeRepo()
    frontier = replace(cases._pending(repo), tip=fixtures._minute(4))
    resume = await cases._resume(monkeypatch)
    cases._permit_capture(monkeypatch)
    stale = replace(frontier, generation=frontier.generation + 1)
    await dispatch.handle_resume_fallback(
        cast(Repository, repo),
        fixtures._sample_scope(),
        stale,
        resume,
        "unexpected",
    )
    assert checkpoint_metrics().fallbacks == {"checkpoint_payload_corrupt": 1}
    assert checkpoint_metrics().invalidations_deleted == 0
    delete = AsyncMock(return_value=True)
    monkeypatch.setattr(repo, "delete_portfolio_pnl_opening_checkpoint", delete)
    await dispatch.handle_resume_fallback(
        cast(Repository, repo),
        fixtures._sample_scope(),
        stale,
        resume,
        "checkpoint_ineligible",
    )
    delete.assert_not_awaited()
    assert (
        await dispatch._suffix_fills(
            cast(Repository, repo),
            fixtures._sample_scope(),
            replace(frontier, raw_execution_watermarks=None),
            resume.record,
        )
        == 0
    )


@pytest.mark.parametrize("cancelled", [False, True])
async def test_delete_checkpoint_handles_failure_and_preserves_cancellation(
    monkeypatch: pytest.MonkeyPatch,
    cancelled: bool,
) -> None:
    """Contain deletion failures while allowing task cancellation to propagate.

    Given: A stored checkpoint whose fenced deletion raises an error or cancellation.
    When: The deletion helper awaits the repository.
    Then: Ordinary errors are contained and cancellation preserves its identity.

    Args:
        monkeypatch: Pytest monkeypatch fixture.
        cancelled: Whether the repository raises cancellation instead of an ordinary error.
    """
    reset_checkpoint_metrics()
    repo = fixtures._FakeRepo()
    resume = await cases._resume(monkeypatch)
    failure = asyncio.CancelledError() if cancelled else RuntimeError("delete failed")
    delete = AsyncMock(side_effect=failure)
    monkeypatch.setattr(repo, "delete_portfolio_pnl_opening_checkpoint", delete)
    if cancelled:
        with pytest.raises(asyncio.CancelledError):
            await dispatch.delete_checkpoint(
                cast(Repository, repo), fixtures._sample_scope(), resume.record
            )
    else:
        await dispatch.delete_checkpoint(
            cast(Repository, repo), fixtures._sample_scope(), resume.record
        )
    delete.assert_awaited_once()
    assert checkpoint_metrics().invalidations_deleted == 0


async def test_prepare_resume_propagates_cancellation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Preserve cancellation raised before stored checkpoint verification.

    Given: An eligible resume request whose checkpoint read is cancelled.
    When: Resume preparation awaits the stored row.
    Then: ``CancelledError`` propagates without logging a fallback.

    Args:
        monkeypatch: Pytest monkeypatch fixture.
    """
    cases._permit_capture(monkeypatch)
    repo = fixtures._FakeRepo()
    frontier = cases._pending(repo)
    request = dispatch.PnlCheckpointResumeRequest(
        cast(Repository, repo),
        fixtures._sample_scope(),
        fixtures._anchor(),
        frontier,
        fixtures._minute(3),
        "active",
    )
    monkeypatch.setattr(
        repo,
        "get_portfolio_pnl_opening_checkpoint",
        AsyncMock(side_effect=asyncio.CancelledError),
    )
    with pytest.raises(asyncio.CancelledError):
        await dispatch.prepare_checkpoint_resume(request)


async def test_write_precheck_value_error_is_soft(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Convert a malformed staged certificate into ``capture_value_error``.

    Given: A write whose frontier pre-check raises ValueError.
    When: The helper certifies it before commit.
    Then: No write is returned and the chunk remains safe to commit without it.
    """
    reset_checkpoint_metrics()
    repo = fixtures._FakeRepo()
    frontier = cases._pending(repo)
    write = await cases._write(monkeypatch, frontier)
    result = replace(
        fixtures._series(fixtures._minute(3), fixtures._minute(3)),
        checkpoint_write=write,
    )
    plan = shadow_cases._plan(frontier)

    def fail(
        frontier_after: PnlCorrectionFrontier,
        chunk: object,
        certificate: object,
    ) -> None:
        """Reject the synthetic staged write."""
        del frontier_after, chunk, certificate
        raise ValueError("malformed certificate")

    monkeypatch.setattr(dispatch, "checkpoint_write_refusal", fail)
    assert (
        dispatch.certified_checkpoint_write(frontier, fixtures._sample_scope(), plan, result)
        is None
    )
    assert checkpoint_metrics().capture_refusals == {"capture_value_error": 1}


async def test_stationary_persistent_fill_gap_skip_and_liveness_log(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Cover the pre-existing stationary-gap path in the changed snapshotter module.

    Given: A settled, persistently gapped frontier with no active sample rows.
    When: Replay start evaluates the same probe twice and then a nonstationary one.
    Then: The stationary gap pauses with one streak log and the other rewinds to t0.
    """
    repo = fixtures._FakeRepo()
    original = cases._pending(repo)
    frontier = replace(
        original,
        next_minute=None,
        window_start=fixtures._minute(1),
        repair_from=fixtures._minute(1),
        has_fill_gap=True,
    )
    snap = fixtures._snapshotter(repo, frontier.as_of)
    ctx = cases._context(snap, frontier)
    pending = fixtures._meta(seq=4, has_fill_gap=True)
    monkeypatch.setattr(
        repo,
        "has_active_portfolio_pnl_samples",
        AsyncMock(return_value=False),
        raising=False,
    )

    first = await snap._replay_start(
        cast(Repository, repo), ctx, frontier, fixtures._minute(4), pending
    )
    second = await snap._replay_start(
        cast(Repository, repo), ctx, frontier, fixtures._minute(4), pending
    )
    changed = await snap._replay_start(
        cast(Repository, repo),
        ctx,
        replace(frontier, next_minute=fixtures._minute(3)),
        fixtures._minute(4),
        pending,
    )

    assert first is None
    assert second is None
    assert changed == fixtures._minute(1)
    assert len(snap._persistent_fill_gap_skip_logged) == 1


async def test_every_recapture_trigger_and_forward_resume_decision(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Classify every closed recapture trigger before replay.

    Given: A certificate compatible with a forward generation.
    When: Each absence, generation, version, age, or suffix-bound trigger applies.
    Then: Its stable reason is selected and the ordinary forward case resumes.
    """
    repo = fixtures._FakeRepo()
    frontier = cases._pending(repo)
    resume = await cases._resume(monkeypatch)
    certificate = replace(
        resume.record.certificate,
        key=frontier.key,
        source_generation=frontier.generation,
        checkpoint_minute=frontier.tip - timedelta(minutes=1),
        as_of=frontier.as_of,
    )
    record = replace(resume.record, certificate=certificate)
    forward = replace(frontier, window_start=certificate.checkpoint_minute + timedelta(minutes=1))
    assert dispatch.recapture_trigger(forward, record, 0) is None
    assert dispatch.recapture_trigger(forward, None, 0) == "absent"
    assert (
        dispatch.recapture_trigger(
            replace(forward, window_start=certificate.checkpoint_minute),
            replace(
                record, certificate=replace(certificate, source_generation=frontier.generation + 1)
            ),
            0,
        )
        == "generation"
    )
    changed_key = replace(frontier.key, calc_version="other")
    assert (
        dispatch.recapture_trigger(replace(forward, key=changed_key), record, 0) == "calc_version"
    )
    aged_frontier = replace(
        forward,
        tip=frontier.tip + timedelta(minutes=dispatch.RECAPTURE_AGE_MINUTES),
    )
    assert dispatch.recapture_trigger(aged_frontier, record, 0) == "age"
    assert (
        dispatch.recapture_trigger(forward, record, dispatch.RECAPTURE_SUFFIX_FILLS)
        == "suffix_size"
    )


async def test_verification_flag_tracks_verifier_entry_not_earlier_lookup_failures(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Charge verification only once the live verifier has actually been entered.

    Given: A clean generation whose stored-row lookup raises first, and then a
        stored row whose live verifier raises.
    When: Resume preparation runs for each failure.
    Then: Both fall back as ``checkpoint_verify_error``, but only the verifier
        failure reports ``verification_attempted``.

    Args:
        monkeypatch: Pytest fixture injecting the lookup and verifier failures.
    """
    reset_checkpoint_metrics()
    cases._permit_capture(monkeypatch)
    repo = fixtures._FakeRepo()
    frontier = cases._pending(repo)
    resume = await cases._resume(monkeypatch)
    cases._permit_capture(monkeypatch)
    request = dispatch.PnlCheckpointResumeRequest(
        cast(Repository, repo),
        fixtures._sample_scope(),
        fixtures._anchor(),
        frontier,
        fixtures._minute(3),
        "shadow",
    )
    lookup = AsyncMock(side_effect=RuntimeError("checkpoint lookup timeout"))
    monkeypatch.setattr(repo, "get_portfolio_pnl_opening_checkpoint", lookup)
    before_verifier = await dispatch.prepare_checkpoint_resume(request)
    assert before_verifier.recapture == "fallback"
    assert before_verifier.verification_attempted is False

    lookup.side_effect = None
    lookup.return_value = replace(
        resume.record,
        certificate=replace(
            resume.record.certificate,
            key=frontier.key,
            source_generation=frontier.generation,
        ),
    )
    monkeypatch.setattr(
        dispatch,
        "verify_checkpoint_for_resume",
        AsyncMock(side_effect=RuntimeError("verifier timeout")),
    )
    inside_verifier = await dispatch.prepare_checkpoint_resume(request)
    assert inside_verifier.recapture == "fallback"
    assert inside_verifier.verification_attempted is True
    assert checkpoint_metrics().fallbacks == {"checkpoint_verify_error": 2}


async def test_verifier_cancellation_propagates_unwrapped(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Propagate a cancellation raised inside the live verifier unchanged.

    Given: A stored checkpoint whose live verifier raises ``CancelledError``.
    When: Resume preparation runs.
    Then: ``CancelledError`` itself reaches the caller, is not wrapped as a
        verifier failure, and no fallback is recorded.

    Args:
        monkeypatch: Pytest fixture injecting the stored row and cancelled verifier.
    """
    reset_checkpoint_metrics()
    cases._permit_capture(monkeypatch)
    repo = fixtures._FakeRepo()
    frontier = cases._pending(repo)
    resume = await cases._resume(monkeypatch)
    cases._permit_capture(monkeypatch)
    monkeypatch.setattr(
        repo,
        "get_portfolio_pnl_opening_checkpoint",
        AsyncMock(
            return_value=replace(
                resume.record,
                certificate=replace(
                    resume.record.certificate,
                    key=frontier.key,
                    source_generation=frontier.generation,
                ),
            )
        ),
    )
    monkeypatch.setattr(
        dispatch,
        "verify_checkpoint_for_resume",
        AsyncMock(side_effect=asyncio.CancelledError),
    )
    request = dispatch.PnlCheckpointResumeRequest(
        cast(Repository, repo),
        fixtures._sample_scope(),
        fixtures._anchor(),
        frontier,
        fixtures._minute(3),
        "shadow",
    )

    with pytest.raises(asyncio.CancelledError) as raised:
        await dispatch.prepare_checkpoint_resume(request)

    assert raised.value.__cause__ is None
    assert checkpoint_metrics().fallbacks == {}
