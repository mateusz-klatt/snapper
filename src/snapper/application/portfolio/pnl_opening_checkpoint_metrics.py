"""Process-local counters for durable opening-checkpoint dispatch."""

from dataclasses import dataclass
from dataclasses import field
from typing import Literal

type PnlCheckpointRecaptureTrigger = Literal[
    "absent",
    "age",
    "generation",
    "calc_version",
    "fallback",
    "suffix_size",
]


@dataclass
class PnlCheckpointMetrics:
    """Monotonic checkpoint counters with stable machine-readable reason maps."""

    captures_committed: int = 0
    capture_refusals: dict[str, int] = field(default_factory=dict)
    resumes_attempted: int = 0
    resumes_succeeded: int = 0
    fallbacks: dict[str, int] = field(default_factory=dict)
    invalidations_deleted: int = 0
    shadow_comparisons: int = 0
    shadow_mismatches: int = 0
    recaptures: dict[PnlCheckpointRecaptureTrigger, int] = field(default_factory=dict)


_METRICS = PnlCheckpointMetrics()


def checkpoint_metrics() -> PnlCheckpointMetrics:
    """Return a detached snapshot of every process-local checkpoint counter."""
    return PnlCheckpointMetrics(
        captures_committed=_METRICS.captures_committed,
        capture_refusals=dict(_METRICS.capture_refusals),
        resumes_attempted=_METRICS.resumes_attempted,
        resumes_succeeded=_METRICS.resumes_succeeded,
        fallbacks=dict(_METRICS.fallbacks),
        invalidations_deleted=_METRICS.invalidations_deleted,
        shadow_comparisons=_METRICS.shadow_comparisons,
        shadow_mismatches=_METRICS.shadow_mismatches,
        recaptures=dict(_METRICS.recaptures),
    )


def reset_checkpoint_metrics() -> None:
    """Reset all counters for isolated service tests."""
    _METRICS.captures_committed = 0
    _METRICS.capture_refusals.clear()
    _METRICS.resumes_attempted = 0
    _METRICS.resumes_succeeded = 0
    _METRICS.fallbacks.clear()
    _METRICS.invalidations_deleted = 0
    _METRICS.shadow_comparisons = 0
    _METRICS.shadow_mismatches = 0
    _METRICS.recaptures.clear()


def record_checkpoint_capture_committed() -> None:
    """Count one checkpoint upsert committed with its settling chunk."""
    _METRICS.captures_committed += 1


def record_checkpoint_capture_refusal(reason: str) -> None:
    """Count one soft capture refusal by its stable reason."""
    _METRICS.capture_refusals[reason] = _METRICS.capture_refusals.get(reason, 0) + 1


def record_checkpoint_resume_attempted() -> None:
    """Count one verified suffix replay attempt."""
    _METRICS.resumes_attempted += 1


def record_checkpoint_resume_succeeded() -> None:
    """Count one suffix replay that completed without a resume fallback."""
    _METRICS.resumes_succeeded += 1


def record_checkpoint_fallback(reason: str) -> None:
    """Count one anchor fallback by its stable reason."""
    _METRICS.fallbacks[reason] = _METRICS.fallbacks.get(reason, 0) + 1


def record_checkpoint_invalidation_deleted() -> None:
    """Count one generation-fenced stored-row deletion."""
    _METRICS.invalidations_deleted += 1


def record_checkpoint_shadow_comparison() -> None:
    """Count one completed anchor-versus-resumed comparison."""
    _METRICS.shadow_comparisons += 1


def record_checkpoint_shadow_mismatch() -> None:
    """Count one divergent shadow comparison."""
    _METRICS.shadow_mismatches += 1


def record_checkpoint_recapture(reason: PnlCheckpointRecaptureTrigger) -> None:
    """Count one pre-replay anchor recapture decision by its closed trigger."""
    _METRICS.recaptures[reason] = _METRICS.recaptures.get(reason, 0) + 1
