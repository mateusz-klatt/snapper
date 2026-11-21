"""Suffix loading and pure state preparation for verified checkpoint resume."""

from collections.abc import Mapping
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime

from snapper.application.portfolio.pnl_opening_checkpoint import checkpoint_from_payload
from snapper.application.portfolio.pnl_opening_checkpoint_verifier import PnlCheckpointResume
from snapper.application.portfolio.pnl_timeline import TimelineCheckpointOpening
from snapper.data.repository import Repository
from snapper.data.repository_types import PnlTimelineAccrualRow
from snapper.data.repository_types import PnlTimelineExecutionLineageRow
from snapper.data.repository_types import PnlTimelineExecutionSuffixRequest
from snapper.data.repository_types import PnlTimelineOpeningExecutionRow


@dataclass(frozen=True)
class PnlCheckpointSuffixEvidence:
    """Suffix executions, bounded accruals, and their exact lineage rows."""

    executions: list[PnlTimelineOpeningExecutionRow]
    accruals: list[PnlTimelineAccrualRow]
    lineage: list[PnlTimelineExecutionLineageRow]


def resume_engine_opening(resume: PnlCheckpointResume) -> TimelineCheckpointOpening:
    """Decode the verified envelope's engine opening through the existing codec.

    Args:
        resume: Verified checkpoint record and decoded envelope.

    Returns:
        The immutable engine opening captured at C.
    """
    return checkpoint_from_payload(resume.bundle.engine_state)


def merge_resume_spans(
    carried: Mapping[str, tuple[datetime, datetime]],
    suffix: Mapping[str, tuple[datetime, datetime]],
) -> dict[str, tuple[datetime, datetime]]:
    """Merge carried and suffix spans by pairwise minimum and maximum.

    Args:
        carried: Prefix spans stored in the checkpoint envelope.
        suffix: Spans derived from suffix reference activity.

    Returns:
        Combined spans keyed by instrument.
    """
    merged = dict(carried)
    for instrument_public_id, span in suffix.items():
        existing = merged.get(instrument_public_id)
        merged[instrument_public_id] = (
            span
            if existing is None
            else (
                min(existing[0], span[0]),
                max(existing[1], span[1]),
            )
        )
    return merged


def has_late_resume_execution(
    resume: PnlCheckpointResume,
    rows: Sequence[PnlTimelineOpeningExecutionRow],
    opening_t0: datetime,
) -> bool:
    """Detect suffix rows whose seeded clamp reaches C or whose stamp reaches t0.

    Args:
        resume: Verified checkpoint carrying per-pool last-effective cursors.
        rows: Suffix and deferred executions in certified order.
        opening_t0: Activation boundary.

    Returns:
        ``True`` when the caller must fall back to anchor replay.
    """
    opening = resume_engine_opening(resume)
    last_effective = dict(opening.last_effective_by_pool)
    checkpoint_minute = resume.record.certificate.checkpoint_minute
    for row in rows:
        event_time = row["timestamp"]
        if event_time <= opening_t0:
            return True
        pool_key = (row["instrument_public_id"], row["shard_key"])
        effective_time = max(last_effective.get(pool_key, event_time), event_time)
        if effective_time <= checkpoint_minute:
            return True
        last_effective[pool_key] = effective_time
    return False


async def load_checkpoint_suffix(
    repo: Repository,
    resume: PnlCheckpointResume,
    generation_watermarks: Mapping[str, int],
    reference_tip: datetime,
    as_of: datetime,
) -> PnlCheckpointSuffixEvidence:
    """Load only suffix/deferred executions, bounded accruals, and their lineage.

    Args:
        repo: Repository providing suffix-only resume reads.
        resume: Verified checkpoint record and envelope.
        generation_watermarks: Current generation's execution cut.
        reference_tip: Generation reference tip.
        as_of: Resuming generation's knowledge horizon.

    Returns:
        The complete suffix evidence without reading the full prefix.
    """
    certificate = resume.record.certificate
    executions = await repo.get_pnl_timeline_execution_suffix_at_watermarks(
        PnlTimelineExecutionSuffixRequest(
            wallet_public_id=certificate.key.wallet_public_id,
            mode=certificate.key.mode,
            cutoff_watermarks=certificate.cutoff_execution_watermarks,
            generation_watermarks=generation_watermarks,
            deferred_execution_sequences=certificate.deferred_execution_sequences,
            as_of=as_of,
        )
    )
    accruals = await repo.get_accruals_for_pnl(
        certificate.key.wallet_public_id,
        certificate.key.mode,
        as_of,
        after_exclusive=certificate.checkpoint_minute,
        through=reference_tip,
    )
    order_ids = tuple(sorted({row["order_public_id"] for row in executions}))
    lineage = await repo.get_pnl_timeline_execution_lineage(
        order_ids,
        as_of,
    )
    return PnlCheckpointSuffixEvidence(executions=executions, accruals=accruals, lineage=lineage)
