"""Capture, resume, refusal, and invalidation helpers for the P&L snapshotter."""

import asyncio
import hashlib
import json
import logging
from dataclasses import dataclass
from dataclasses import replace
from datetime import datetime
from datetime import timedelta
from typing import Final

from snapper.application.portfolio.fx_conversion_shadow import POINTWISE_FX_ELECTION_POLICY_VERSION
from snapper.application.portfolio.pnl_correction_planner import PlannedPnlCorrection
from snapper.application.portfolio.pnl_engine_options import pnl_engine_options_for
from snapper.application.portfolio.pnl_opening_checkpoint_capture import PnlCheckpointCaptureRequest
from snapper.application.portfolio.pnl_opening_checkpoint_certificate import (
    checkpoint_minute_fenced,
)
from snapper.application.portfolio.pnl_opening_checkpoint_certificate import (
    checkpoint_write_refusal,
)
from snapper.application.portfolio.pnl_opening_checkpoint_metrics import (
    PnlCheckpointRecaptureTrigger,
)
from snapper.application.portfolio.pnl_opening_checkpoint_metrics import checkpoint_wallet_alias
from snapper.application.portfolio.pnl_opening_checkpoint_metrics import (
    record_checkpoint_capture_refusal,
)
from snapper.application.portfolio.pnl_opening_checkpoint_metrics import record_checkpoint_fallback
from snapper.application.portfolio.pnl_opening_checkpoint_metrics import (
    record_checkpoint_invalidation_deleted,
)
from snapper.application.portfolio.pnl_opening_checkpoint_metrics import record_checkpoint_recapture
from snapper.application.portfolio.pnl_opening_checkpoint_verifier import PnlCheckpointFallback
from snapper.application.portfolio.pnl_opening_checkpoint_verifier import PnlCheckpointResume
from snapper.application.portfolio.pnl_opening_checkpoint_verifier import (
    PnlCheckpointResumeEligibility,
)
from snapper.application.portfolio.pnl_opening_checkpoint_verifier import service_resume_fallback
from snapper.application.portfolio.pnl_opening_checkpoint_verifier import (
    verify_checkpoint_for_resume,
)
from snapper.application.portfolio.pnl_snapshotter_config import CheckpointMode
from snapper.application.portfolio.pnl_timeline_service import PNL_TIMELINE_CALC_VERSION
from snapper.application.portfolio.pnl_timeline_service import PnlWalletSeriesResult
from snapper.data.pnl_correction_state import PnlCorrectionFrontier
from snapper.data.pnl_correction_state import PnlOpeningCheckpointRecord
from snapper.data.pnl_correction_state import PnlOpeningCheckpointWrite
from snapper.data.pnl_correction_state import advance_correction_frontier
from snapper.data.repository import PortfolioPnlSampleScope
from snapper.data.repository import Repository
from snapper.data.repository_types import PortfolioPnlAnchorRow

logger = logging.getLogger(__name__)

RECAPTURE_AGE_MINUTES: Final[int] = 1440
RECAPTURE_SUFFIX_FILLS: Final[int] = 2000
MAX_RECAPTURES_PER_TICK: Final[int] = 4
"""Maximum full anchor recaptures over ``(cutoff, frontier]`` permitted in a single tick."""
_RECAPTURE_AGE: Final[timedelta] = timedelta(minutes=RECAPTURE_AGE_MINUTES)


def checkpoint_anchor_identity(anchor: PortfolioPnlAnchorRow) -> tuple[str, str]:
    """Return the active anchor id and a canonical digest of its complete row.

    Args:
        anchor: Active anchor database row.

    Returns:
        Pair of anchor public id and SHA-256 hex digest.
    """
    payload = {
        key: value.isoformat() if isinstance(value, datetime) else value
        for key, value in anchor.items()
    }
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return anchor["public_id"], hashlib.sha256(encoded).hexdigest()


def checkpoint_capture_permitted(calc_version: str) -> bool:
    """Resolve capture permission without allowing unknown versions to escape.

    Args:
        calc_version: P&L calculation version string.

    Returns:
        Whether checkpoint capture is permitted for ``calc_version``.
    """
    try:
        return pnl_engine_options_for(calc_version).capture_permitted
    except ValueError:
        return False


def _log_refusal(scope: PortfolioPnlSampleScope, reason: str) -> None:
    """Report a soft refusal without failing the financial chunk."""
    logger.warning(
        "PortfolioPnlSnapshotter: checkpoint capture refused; wallet=%s mode=%s ccy=%s "
        "epoch=%s reason=%s",
        checkpoint_wallet_alias(scope.wallet_public_id),
        scope.mode,
        scope.valuation_ccy,
        scope.epoch_public_id,
        reason,
        extra={"checkpoint_capture_refusal": reason},
    )
    record_checkpoint_capture_refusal(reason, scope)


def record_checkpoint_write_refusal(scope: PortfolioPnlSampleScope, reason: str) -> None:
    """Log and count one repository or savepoint checkpoint write refusal.

    Args:
        scope: Active anchor-bound sample scope.
        reason: Closed refusal code describing why checkpoint persistence rolled back.
    """
    _log_refusal(scope, reason)


def capture_request_for(
    frontier: PnlCorrectionFrontier,
    scope: PortfolioPnlSampleScope,
    anchor: PortfolioPnlAnchorRow,
    end: datetime,
    *,
    self_heal_open: datetime | None = None,
) -> PnlCheckpointCaptureRequest | None:
    """Build a capture request only at a clean settling tip and isolate refusals.

    Args:
        frontier: Current durable correction frontier.
        scope: Active anchor-bound sample scope.
        anchor: Active anchor row for the scope.
        end: Inclusive end minute of the chunk being replayed.
        self_heal_open: This tick's earliest retryable incomplete minute, if any.

    Returns:
        Validated capture request, or ``None`` when ineligible or refused.
    """
    if not checkpoint_capture_permitted(frontier.key.calc_version):
        _log_refusal(scope, "capture_not_permitted")
        return None
    if checkpoint_minute_fenced(end, self_heal_open):
        _log_refusal(scope, "self_heal_open")
        return None
    start = frontier.next_minute
    if (
        start is None
        or end != frontier.tip
        or frontier.has_fill_gap is not False
        or (frontier.repair_from is not None and frontier.repair_from < start)
    ):
        return None
    raw = frontier.raw_execution_watermarks
    manifest = frontier.execution_manifest_digest
    venues = frontier.expected_venues
    witnesses = frontier.venue_event_watermarks
    witness_digest = frontier.fill_witness_digest
    if (
        raw is None
        or manifest is None
        or venues is None
        or witnesses is None
        or witness_digest is None
    ):
        _log_refusal(scope, "capture_value_error")
        return None
    try:
        anchor_public_id, anchor_digest = checkpoint_anchor_identity(anchor)
        return PnlCheckpointCaptureRequest(
            key=frontier.key,
            source_generation=frontier.generation,
            raw_execution_watermarks=raw,
            execution_manifest_digest=manifest,
            expected_venues=venues,
            venue_event_watermarks=witnesses,
            fill_witness_digest=witness_digest,
            anchor_public_id=anchor_public_id,
            anchor_digest=anchor_digest,
            calc_version=frontier.key.calc_version,
        )
    except (ValueError, TypeError):
        _log_refusal(scope, "capture_value_error")
        return None


async def delete_checkpoint(
    repo: Repository,
    scope: PortfolioPnlSampleScope,
    record: PnlOpeningCheckpointRecord,
) -> None:
    """Retire a stored row through its source-generation fence and count deletion.

    Args:
        repo: Repository used for generation-fenced deletion.
        scope: Scope owning the checkpoint row.
        record: Stored checkpoint record being retired.
    """
    try:
        deleted = await repo.delete_portfolio_pnl_opening_checkpoint(
            scope,
            record.certificate.source_generation,
        )
    except asyncio.CancelledError:
        raise
    except Exception as exc:
        logger.warning(
            "PortfolioPnlSnapshotter: checkpoint delete failed; wallet=%s generation=%s (%s)",
            checkpoint_wallet_alias(scope.wallet_public_id),
            record.certificate.source_generation,
            type(exc).__name__,
        )
        return
    if deleted:
        record_checkpoint_invalidation_deleted("generation", scope)


def _log_fallback(
    scope: PortfolioPnlSampleScope,
    frontier: PnlCorrectionFrontier,
    fallback: PnlCheckpointFallback,
) -> None:
    """Count and report one structured anchor fallback."""
    record_checkpoint_fallback(fallback.reason, scope)
    logger.warning(
        "PortfolioPnlSnapshotter: checkpoint resume fallback; wallet=%s generation=%s "
        "reason=%s delete_required=%s",
        checkpoint_wallet_alias(scope.wallet_public_id),
        frontier.generation,
        fallback.reason,
        fallback.delete_required,
        extra={"checkpoint_resume_fallback": fallback.reason},
    )


@dataclass(frozen=True)
class PnlCheckpointResumeRequest:
    """All caller-bound inputs for one no-work-first resume verification."""

    repo: Repository
    scope: PortfolioPnlSampleScope
    anchor: PortfolioPnlAnchorRow
    frontier: PnlCorrectionFrontier
    start: datetime
    mode: CheckpointMode
    recapture_budget_exhausted: bool = False


@dataclass(frozen=True)
class PnlCheckpointDecision:
    """One pre-replay choice: verified suffix resume or anchor recapture.

    ``verification_attempted`` is ``True`` whenever the expensive live
    verification of a stored checkpoint was entered, whatever its verdict and
    including an unexpected error raised inside it. A failure in the stored
    record lookup or suffix count before verification leaves it ``False``. Shadow mode charges its
    per-tick comparison budget on that flag, so a checkpoint that refuses
    late still consumes a slot.
    """

    resume: PnlCheckpointResume | None = None
    recapture: PnlCheckpointRecaptureTrigger | None = None
    verification_attempted: bool = False


def recapture_trigger(
    frontier: PnlCorrectionFrontier,
    record: PnlOpeningCheckpointRecord | None,
    suffix_fills: int,
) -> PnlCheckpointRecaptureTrigger | None:
    """Classify recapture triggers without database work or replay side effects.

    Generation recapture deliberately combines a generation mismatch with only the
    ``window_start`` comparator: a forward generation resumes an earlier checkpoint,
    while pending-minute and repair fences remain repository deletion concerns.

    Args:
        frontier: Current durable correction frontier.
        record: Stored opening checkpoint record, if any.
        suffix_fills: Number of suffix fills after the checkpoint cutoff.

    Returns:
        Recapture trigger token, or ``None`` when verification may proceed.
    """
    if record is None:
        return "absent"
    certificate = record.certificate
    if certificate.key.calc_version != frontier.key.calc_version:
        return "calc_version"
    if certificate.source_generation != frontier.generation and checkpoint_minute_fenced(
        certificate.checkpoint_minute, frontier.window_start
    ):
        return "generation"
    if frontier.tip - certificate.checkpoint_minute >= _RECAPTURE_AGE:
        return "age"
    if suffix_fills >= RECAPTURE_SUFFIX_FILLS:
        return "suffix_size"
    return None


def _recapture(
    scope: PortfolioPnlSampleScope,
    reason: PnlCheckpointRecaptureTrigger,
) -> PnlCheckpointDecision:
    """Count and return one anchor-only recapture decision."""
    record_checkpoint_recapture(reason, scope)
    return PnlCheckpointDecision(recapture=reason)


async def _suffix_fills(
    repo: Repository,
    scope: PortfolioPnlSampleScope,
    frontier: PnlCorrectionFrontier,
    record: PnlOpeningCheckpointRecord,
) -> int:
    """Count suffix rows with one grouped aggregate over indexed execution coordinates."""
    watermarks = frontier.raw_execution_watermarks
    if watermarks is None:
        return 0
    counts = await repo.get_pnl_execution_prefix_counts(
        scope.wallet_public_id,
        scope.mode,
        watermarks,
    )
    certified = record.certificate.cutoff_execution_counts
    return sum(
        max(0, counts.get(exchange, (0, 0))[0] - certified.get(exchange, 0))
        for exchange in watermarks
    )


def _exhausted_recapture_decision(scope: PortfolioPnlSampleScope) -> PnlCheckpointDecision:
    """Record per-tick recapture budget exhaustion and continue without capture."""
    _log_refusal(scope, "capture_recapture_budget_exceeded")
    return PnlCheckpointDecision()


async def _verify_stored_checkpoint(
    request: PnlCheckpointResumeRequest,
    record: PnlOpeningCheckpointRecord,
) -> PnlCheckpointDecision:
    """Verify a stored row against live generation evidence or trigger a fallback."""
    scope = request.scope
    frontier = request.frontier
    anchor_public_id, anchor_digest = checkpoint_anchor_identity(request.anchor)
    outcome = await verify_checkpoint_for_resume(
        request.repo,
        record,
        scope,
        frontier.as_of,
        PnlCheckpointResumeEligibility(
            mode=request.mode,
            calc_version=frontier.key.calc_version,
            timeline_calc_version=PNL_TIMELINE_CALC_VERSION,
            fx_policy_version=POINTWISE_FX_ELECTION_POLICY_VERSION,
            expected_anchor_public_id=anchor_public_id,
            expected_anchor_digest=anchor_digest,
            expected_execution_manifest_digest=frontier.execution_manifest_digest,
            pointwise_exact_fx=True,
            replay_generation_tip=frontier.tip,
            metadata_only=False,
            from_time=request.start,
            frontier_tip=frontier.tip,
            has_fill_gap=False,
        ),
    )
    if isinstance(outcome, PnlCheckpointResume):
        return PnlCheckpointDecision(resume=outcome)
    _log_fallback(scope, frontier, outcome)
    if outcome.delete_required:
        await delete_checkpoint(request.repo, scope, record)
    if request.recapture_budget_exhausted:
        return _exhausted_recapture_decision(scope)
    return _recapture(scope, "fallback")


class _VerifierEnteredError(Exception):
    """Wrap a failure raised after live checkpoint verification has started.

    Shadow mode charges its comparison slot only for verification work, so a
    lookup or suffix-count failure before the verifier must stay uncharged
    while a failure inside the verifier is charged.
    """


async def _prepare_with_record(
    request: PnlCheckpointResumeRequest,
) -> PnlCheckpointDecision:
    """Evaluate absence, invalidation, bounded recapture, and live verification."""
    repo = request.repo
    scope = request.scope
    frontier = request.frontier
    record = await repo.get_portfolio_pnl_opening_checkpoint(scope)
    initial = recapture_trigger(frontier, record, 0)
    if initial == "absent":
        _log_fallback(scope, frontier, PnlCheckpointFallback("checkpoint_absent", False))
        return (
            _exhausted_recapture_decision(scope)
            if request.recapture_budget_exhausted
            else _recapture(scope, "absent")
        )
    if initial is not None:
        assert record is not None
        await delete_checkpoint(repo, scope, record)
        return (
            _exhausted_recapture_decision(scope)
            if request.recapture_budget_exhausted
            else _recapture(scope, initial)
        )
    assert record is not None
    suffix = await _suffix_fills(repo, scope, frontier, record)
    bounded = recapture_trigger(frontier, record, suffix)
    if bounded is not None and not request.recapture_budget_exhausted:
        return _recapture(scope, bounded)
    try:
        verified = await _verify_stored_checkpoint(request, record)
    except asyncio.CancelledError:
        raise
    except Exception as exc:
        raise _VerifierEnteredError from exc
    return replace(verified, verification_attempted=True)


async def prepare_checkpoint_resume(
    request: PnlCheckpointResumeRequest,
) -> PnlCheckpointDecision:
    """Choose verified resume or anchor recapture before any replay begins.

    Args:
        request: Caller-bound inputs for pre-replay checkpoint verification.

    Returns:
        Decision selecting either verified suffix resume or anchor recapture.
    """
    scope = request.scope
    frontier = request.frontier
    if request.mode == "off" or not checkpoint_capture_permitted(frontier.key.calc_version):
        return PnlCheckpointDecision(recapture="absent")
    if frontier.has_fill_gap:
        _log_fallback(scope, frontier, PnlCheckpointFallback("checkpoint_fill_gap", False))
        return (
            _exhausted_recapture_decision(scope)
            if request.recapture_budget_exhausted
            else _recapture(scope, "fallback")
        )
    try:
        return await _prepare_with_record(request)
    except asyncio.CancelledError:
        raise
    except Exception as exc:
        entered = isinstance(exc, _VerifierEnteredError)
        cause = exc.__cause__ if entered and exc.__cause__ is not None else exc
        logger.warning(
            "PortfolioPnlSnapshotter: checkpoint verify failed; wallet=%s generation=%s (%s)",
            checkpoint_wallet_alias(scope.wallet_public_id),
            frontier.generation,
            type(cause).__name__,
        )
        _log_fallback(scope, frontier, PnlCheckpointFallback("checkpoint_verify_error", False))
        return replace(
            (
                _exhausted_recapture_decision(scope)
                if request.recapture_budget_exhausted
                else _recapture(scope, "fallback")
            ),
            verification_attempted=entered,
        )


async def handle_resume_fallback(
    repo: Repository,
    scope: PortfolioPnlSampleScope,
    frontier: PnlCorrectionFrontier,
    resume: PnlCheckpointResume,
    reason: str,
) -> None:
    """Count a service-detected fallback through the closed verifier verdict and retire stale state.

    The fallback itself does not count a recapture: only a caller that then
    selects one does, so shadow mode, which never recaptures inline, does not
    report a recapture it did not request.

    Args:
        repo: Repository used to retire the stored checkpoint row.
        scope: Scope owning the checkpoint row.
        frontier: Current durable correction frontier.
        resume: Verified resume state that hit a service fallback.
        reason: Fallback reason reported by the timeline service.
    """
    fallback = service_resume_fallback(reason)
    _log_fallback(scope, frontier, fallback)
    if fallback.delete_required:
        await delete_checkpoint(repo, scope, resume.record)


def certified_checkpoint_write(
    frontier: PnlCorrectionFrontier,
    scope: PortfolioPnlSampleScope,
    plan: PlannedPnlCorrection,
    result: PnlWalletSeriesResult,
) -> PnlOpeningCheckpointWrite | None:
    """Pre-check a captured write and convert every expected error to a soft refusal.

    Args:
        frontier: Current durable correction frontier before commit.
        scope: Scope owning the staged checkpoint write.
        plan: Correction plan for the chunk being committed.
        result: Replay result carrying any staged capture or service refusal.

    Returns:
        Certified checkpoint write, or ``None`` when omitted or refused.
    """
    refusal = result.checkpoint_capture_refusal
    if refusal is not None:
        _log_refusal(scope, refusal)
    write = result.checkpoint_write
    if write is None:
        return None
    try:
        refusal = checkpoint_write_refusal(
            advance_correction_frontier(frontier, plan.chunk),
            plan.chunk,
            write.certificate,
        )
    except ValueError:
        refusal = "capture_value_error"
    if refusal is not None:
        _log_refusal(scope, refusal)
        return None
    return write
