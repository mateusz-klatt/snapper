"""Capture, resume, refusal, and invalidation helpers for the P&L snapshotter."""

import hashlib
import json
import logging
from dataclasses import dataclass
from datetime import datetime
from datetime import timedelta
from typing import Final

from snapper.application.portfolio.fx_conversion_shadow import POINTWISE_FX_ELECTION_POLICY_VERSION
from snapper.application.portfolio.pnl_correction_planner import PlannedPnlCorrection
from snapper.application.portfolio.pnl_engine_options import pnl_engine_options_for
from snapper.application.portfolio.pnl_opening_checkpoint_capture import PnlCheckpointCaptureRequest
from snapper.application.portfolio.pnl_opening_checkpoint_certificate import (
    checkpoint_write_refusal,
)
from snapper.application.portfolio.pnl_opening_checkpoint_metrics import (
    PnlCheckpointRecaptureTrigger,
)
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
_RECAPTURE_AGE: Final[timedelta] = timedelta(minutes=RECAPTURE_AGE_MINUTES)


def checkpoint_anchor_identity(anchor: PortfolioPnlAnchorRow) -> tuple[str, str]:
    """Return the active anchor id and a canonical digest of its complete row."""
    payload = {
        key: value.isoformat() if isinstance(value, datetime) else value
        for key, value in anchor.items()
    }
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return anchor["public_id"], hashlib.sha256(encoded).hexdigest()


def checkpoint_capture_permitted(calc_version: str) -> bool:
    """Resolve capture permission without allowing unknown versions to escape."""
    try:
        return pnl_engine_options_for(calc_version).capture_permitted
    except ValueError:
        return False


def _log_refusal(scope: PortfolioPnlSampleScope, reason: str) -> None:
    """Report a soft refusal without failing the financial chunk."""
    logger.warning(
        "PortfolioPnlSnapshotter: checkpoint capture refused; wallet=%s mode=%s ccy=%s "
        "epoch=%s reason=%s",
        scope.wallet_public_id,
        scope.mode,
        scope.valuation_ccy,
        scope.epoch_public_id,
        reason,
        extra={"checkpoint_capture_refusal": reason},
    )
    record_checkpoint_capture_refusal(reason)


def capture_request_for(
    frontier: PnlCorrectionFrontier,
    scope: PortfolioPnlSampleScope,
    anchor: PortfolioPnlAnchorRow,
    end: datetime,
) -> PnlCheckpointCaptureRequest | None:
    """Build a capture request only at a clean settling tip and isolate refusals."""
    if not checkpoint_capture_permitted(frontier.key.calc_version):
        _log_refusal(scope, "capture_not_permitted")
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
    anchor_public_id, anchor_digest = checkpoint_anchor_identity(anchor)
    try:
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
    except ValueError:
        _log_refusal(scope, "capture_value_error")
        return None


async def delete_checkpoint(
    repo: Repository,
    scope: PortfolioPnlSampleScope,
    record: PnlOpeningCheckpointRecord,
) -> None:
    """Retire a stored row through its source-generation fence and count deletion."""
    deleted = await repo.delete_portfolio_pnl_opening_checkpoint(
        scope,
        record.certificate.source_generation,
    )
    if deleted:
        record_checkpoint_invalidation_deleted()


def _log_fallback(
    scope: PortfolioPnlSampleScope,
    frontier: PnlCorrectionFrontier,
    fallback: PnlCheckpointFallback,
) -> None:
    """Count and report one structured anchor fallback."""
    record_checkpoint_fallback(fallback.reason)
    logger.warning(
        "PortfolioPnlSnapshotter: checkpoint resume fallback; wallet=%s generation=%s "
        "reason=%s delete_required=%s",
        scope.wallet_public_id,
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


@dataclass(frozen=True)
class PnlCheckpointDecision:
    """One pre-replay choice: verified suffix resume or anchor recapture."""

    resume: PnlCheckpointResume | None = None
    recapture: PnlCheckpointRecaptureTrigger | None = None


def recapture_trigger(
    frontier: PnlCorrectionFrontier,
    record: PnlOpeningCheckpointRecord | None,
    suffix_fills: int,
) -> PnlCheckpointRecaptureTrigger | None:
    """Classify recapture triggers without database work or replay side effects."""
    if record is None:
        return "absent"
    certificate = record.certificate
    if certificate.key.calc_version != frontier.key.calc_version:
        return "calc_version"
    if (
        certificate.source_generation != frontier.generation
        and frontier.window_start <= certificate.checkpoint_minute
    ):
        return "generation"
    if frontier.tip - certificate.checkpoint_minute >= _RECAPTURE_AGE:
        return "age"
    if suffix_fills >= RECAPTURE_SUFFIX_FILLS:
        return "suffix_size"
    return None


def _recapture(reason: PnlCheckpointRecaptureTrigger) -> PnlCheckpointDecision:
    """Count and return one anchor-only recapture decision."""
    record_checkpoint_recapture(reason)
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


async def prepare_checkpoint_resume(
    request: PnlCheckpointResumeRequest,
) -> PnlCheckpointDecision:
    """Choose verified resume or anchor recapture before any replay begins."""
    repo = request.repo
    scope = request.scope
    anchor = request.anchor
    frontier = request.frontier
    start = request.start
    mode = request.mode
    if mode == "off" or not checkpoint_capture_permitted(frontier.key.calc_version):
        return PnlCheckpointDecision(recapture="absent")
    if frontier.has_fill_gap:
        _log_fallback(scope, frontier, PnlCheckpointFallback("checkpoint_fill_gap", False))
        return _recapture("fallback")
    record = await repo.get_portfolio_pnl_opening_checkpoint(scope)
    initial = recapture_trigger(frontier, record, 0)
    if initial is not None:
        if initial == "absent":
            _log_fallback(scope, frontier, PnlCheckpointFallback("checkpoint_absent", False))
        return _recapture(initial)
    assert record is not None
    suffix = await _suffix_fills(repo, scope, frontier, record)
    bounded = recapture_trigger(frontier, record, suffix)
    if bounded is not None:
        return _recapture(bounded)
    anchor_public_id, anchor_digest = checkpoint_anchor_identity(anchor)
    outcome = await verify_checkpoint_for_resume(
        repo,
        record,
        scope,
        frontier.as_of,
        PnlCheckpointResumeEligibility(
            mode=mode,
            calc_version=frontier.key.calc_version,
            timeline_calc_version=PNL_TIMELINE_CALC_VERSION,
            fx_policy_version=POINTWISE_FX_ELECTION_POLICY_VERSION,
            expected_anchor_public_id=anchor_public_id,
            expected_anchor_digest=anchor_digest,
            pointwise_exact_fx=True,
            replay_generation_tip=frontier.tip,
            metadata_only=False,
            from_time=start,
            frontier_tip=frontier.tip,
            has_fill_gap=False,
        ),
    )
    if isinstance(outcome, PnlCheckpointResume):
        return PnlCheckpointDecision(resume=outcome)
    _log_fallback(scope, frontier, outcome)
    if outcome.delete_required:
        await delete_checkpoint(repo, scope, record)
    return _recapture("fallback")


async def handle_resume_fallback(
    repo: Repository,
    scope: PortfolioPnlSampleScope,
    frontier: PnlCorrectionFrontier,
    resume: PnlCheckpointResume,
    reason: str,
) -> None:
    """Count a service-detected late execution and retire the divergent row."""
    fallback = PnlCheckpointFallback("checkpoint_late_execution", True)
    if reason != "checkpoint_late_execution":
        fallback = PnlCheckpointFallback("checkpoint_payload_corrupt", True)
    _log_fallback(scope, frontier, fallback)
    record_checkpoint_recapture("fallback")
    await delete_checkpoint(repo, scope, resume.record)


def certified_checkpoint_write(
    frontier: PnlCorrectionFrontier,
    scope: PortfolioPnlSampleScope,
    plan: PlannedPnlCorrection,
    result: PnlWalletSeriesResult,
) -> PnlOpeningCheckpointWrite | None:
    """Pre-check a captured write and convert every expected error to a soft refusal."""
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
