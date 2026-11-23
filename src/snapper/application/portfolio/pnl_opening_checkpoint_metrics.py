"""Process-local counters and summary logging for durable opening-checkpoint dispatch."""

import hashlib
import hmac
import logging
import os
from collections.abc import Mapping
from collections.abc import Set as AbstractSet
from dataclasses import dataclass
from dataclasses import field
from typing import Final
from typing import Literal
from typing import Protocol

CHECKPOINT_SUMMARY_EVERY_TICKS: Final[int] = 10
"""Number of active snapshotter ticks between per-scope checkpoint summary logs."""

_WALLET_ALIAS_KEY: Final[bytes] = os.urandom(32)
"""Process-local secret key used for keyed wallet-id aliases in logs and metrics."""

type PnlCheckpointRecaptureTrigger = Literal[
    "absent",
    "age",
    "generation",
    "calc_version",
    "fallback",
    "suffix_size",
]

type PnlCheckpointInvalidationReason = Literal["generation", "repo_fence"]
"""Closed vocabulary for opening-checkpoint deletion paths."""

type PnlCheckpointShadowSkipReason = Literal["window_mismatch", "tick_budget"]
"""Closed vocabulary for skipped checkpoint shadow comparisons."""

type PnlCheckpointScopeKey = tuple[str, str, str]
"""Process-local aliased scope identity ``(wallet_alias, mode, valuation_ccy)``."""


def checkpoint_wallet_alias(wallet_public_id: str) -> str:
    """Return a deterministic process-local keyed HMAC alias for a wallet id.

    Args:
        wallet_public_id: Raw wallet identifier from configuration or scope state.

    Returns:
        16-character hexadecimal keyed HMAC digest safe for logs and metric labels.
    """
    return hmac.new(
        _WALLET_ALIAS_KEY,
        wallet_public_id.encode("utf-8"),
        hashlib.sha256,
    ).hexdigest()[:16]


def checkpoint_scope_key(
    scope: PnlCheckpointScope | PnlCheckpointScopeKey,
) -> PnlCheckpointScopeKey:
    """Resolve one scope object or raw key tuple into an aliased metrics key.

    Args:
        scope: Protocol scope instance or ``(wallet_public_id, mode, valuation_ccy)`` tuple.

    Returns:
        Aliased ``(wallet_alias, mode, valuation_ccy)`` scope key.
    """
    if isinstance(scope, tuple):
        return (checkpoint_wallet_alias(scope[0]), scope[1], scope[2])
    return (
        checkpoint_wallet_alias(scope.wallet_public_id),
        scope.mode,
        scope.valuation_ccy,
    )


class PnlCheckpointScope(Protocol):
    """Minimal scope surface needed to attribute checkpoint counters per scope."""

    @property
    def wallet_public_id(self) -> str:
        """Return the scope's wallet public identifier.

        Returns:
            Wallet public identifier string.
        """

    @property
    def mode(self) -> str:
        """Return the scope's trading mode.

        Returns:
            Trading mode string.
        """

    @property
    def valuation_ccy(self) -> str:
        """Return the scope's valuation currency.

        Returns:
            Valuation currency code.
        """


@dataclass
class PnlCheckpointMetrics:
    """Monotonic checkpoint counters with stable machine-readable reason maps."""

    captures_committed: int = 0
    capture_refusals: dict[str, int] = field(default_factory=dict)
    resumes_attempted: int = 0
    resumes_succeeded: int = 0
    fallbacks: dict[str, int] = field(default_factory=dict)
    invalidations_deleted: dict[PnlCheckpointInvalidationReason, int] = field(default_factory=dict)
    shadow_comparisons: int = 0
    shadow_mismatches: int = 0
    shadow_skipped: dict[str, int] = field(default_factory=dict)
    recaptures: dict[PnlCheckpointRecaptureTrigger, int] = field(default_factory=dict)


_METRICS = PnlCheckpointMetrics()
_SCOPE_METRICS: dict[PnlCheckpointScopeKey, PnlCheckpointMetrics] = {}


def _snapshot(metrics: PnlCheckpointMetrics) -> PnlCheckpointMetrics:
    """Return a detached copy of one counter bucket."""
    return PnlCheckpointMetrics(
        captures_committed=metrics.captures_committed,
        capture_refusals=dict(metrics.capture_refusals),
        resumes_attempted=metrics.resumes_attempted,
        resumes_succeeded=metrics.resumes_succeeded,
        fallbacks=dict(metrics.fallbacks),
        invalidations_deleted=dict(metrics.invalidations_deleted),
        shadow_comparisons=metrics.shadow_comparisons,
        shadow_mismatches=metrics.shadow_mismatches,
        shadow_skipped=dict(metrics.shadow_skipped),
        recaptures=dict(metrics.recaptures),
    )


def _targets(
    scope: PnlCheckpointScope | PnlCheckpointScopeKey | None,
) -> tuple[PnlCheckpointMetrics, ...]:
    """Return the global bucket and, when ``scope`` is given, the matching scope bucket."""
    if scope is None:
        return (_METRICS,)
    key = checkpoint_scope_key(scope)
    scoped = _SCOPE_METRICS.get(key)
    if scoped is None:
        scoped = PnlCheckpointMetrics()
        _SCOPE_METRICS[key] = scoped
    return (_METRICS, scoped)


def checkpoint_metrics() -> PnlCheckpointMetrics:
    """Return a detached snapshot of every process-local global checkpoint counter.

    Returns:
        Detached copy of the cumulative global checkpoint metrics.
    """
    return _snapshot(_METRICS)


def checkpoint_metrics_by_scope() -> dict[PnlCheckpointScopeKey, PnlCheckpointMetrics]:
    """Return detached per-scope checkpoint counter snapshots sorted by scope key.

    Returns:
        Mapping from aliased ``(wallet_alias, mode, valuation_ccy)`` to detached
        cumulative metrics for each observed scope.
    """
    return {key: _snapshot(_SCOPE_METRICS[key]) for key in sorted(_SCOPE_METRICS)}


def prune_checkpoint_metrics(active_wallets: AbstractSet[str]) -> int:
    """Drop per-scope metrics entries whose wallet is absent from active wallets.

    Args:
        active_wallets: Currently scheduled raw wallet identifiers for this tick.

    Returns:
        Number of disappeared scope buckets pruned from memory.
    """
    active_aliases = {checkpoint_wallet_alias(wallet) for wallet in active_wallets}
    stale = [key for key in _SCOPE_METRICS if key[0] not in active_aliases]
    for key in stale:
        del _SCOPE_METRICS[key]
    return len(stale)


def reset_checkpoint_metrics() -> None:
    """Reset all global and per-scope counters for isolated service tests."""
    _METRICS.captures_committed = 0
    _METRICS.capture_refusals.clear()
    _METRICS.resumes_attempted = 0
    _METRICS.resumes_succeeded = 0
    _METRICS.fallbacks.clear()
    _METRICS.invalidations_deleted.clear()
    _METRICS.shadow_comparisons = 0
    _METRICS.shadow_mismatches = 0
    _METRICS.shadow_skipped.clear()
    _METRICS.recaptures.clear()
    _SCOPE_METRICS.clear()


def record_checkpoint_capture_committed(
    scope: PnlCheckpointScope | PnlCheckpointScopeKey | None = None,
) -> None:
    """Count one checkpoint upsert committed with its settling chunk.

    Args:
        scope: Optional scope owning the committed checkpoint.
    """
    for target in _targets(scope):
        target.captures_committed += 1


def record_checkpoint_capture_refusal(
    reason: str,
    scope: PnlCheckpointScope | PnlCheckpointScopeKey | None = None,
) -> None:
    """Count one soft capture refusal by its stable reason.

    Args:
        reason: Stable machine-readable refusal reason.
        scope: Optional scope owning the refused capture.
    """
    for target in _targets(scope):
        target.capture_refusals[reason] = target.capture_refusals.get(reason, 0) + 1


def record_checkpoint_resume_attempted(
    scope: PnlCheckpointScope | PnlCheckpointScopeKey | None = None,
) -> None:
    """Count one verified suffix replay attempt.

    Args:
        scope: Optional scope owning the resume attempt.
    """
    for target in _targets(scope):
        target.resumes_attempted += 1


def record_checkpoint_resume_succeeded(
    scope: PnlCheckpointScope | PnlCheckpointScopeKey | None = None,
) -> None:
    """Count one suffix replay that completed without a resume fallback.

    Args:
        scope: Optional scope owning the completed resume.
    """
    for target in _targets(scope):
        target.resumes_succeeded += 1


def record_checkpoint_fallback(
    reason: str,
    scope: PnlCheckpointScope | PnlCheckpointScopeKey | None = None,
) -> None:
    """Count one anchor fallback by its stable reason.

    Args:
        reason: Stable machine-readable fallback reason.
        scope: Optional scope owning the anchor fallback.
    """
    for target in _targets(scope):
        target.fallbacks[reason] = target.fallbacks.get(reason, 0) + 1


def record_checkpoint_invalidation_deleted(
    reason: PnlCheckpointInvalidationReason,
    scope: PnlCheckpointScope | PnlCheckpointScopeKey | None = None,
) -> None:
    """Count one stored-row deletion by its closed invalidation path.

    Args:
        reason: Generation fence or repository frontier fence performing the deletion.
        scope: Optional scope owning the deleted checkpoint row.
    """
    for target in _targets(scope):
        target.invalidations_deleted[reason] = target.invalidations_deleted.get(reason, 0) + 1


def record_checkpoint_shadow_comparison(
    scope: PnlCheckpointScope | PnlCheckpointScopeKey | None = None,
) -> None:
    """Count one completed anchor-versus-resumed comparison.

    Args:
        scope: Optional scope owning the shadow comparison.
    """
    for target in _targets(scope):
        target.shadow_comparisons += 1


def record_checkpoint_shadow_mismatch(
    scope: PnlCheckpointScope | PnlCheckpointScopeKey | None = None,
) -> None:
    """Count one divergent shadow comparison.

    Args:
        scope: Optional scope owning the shadow mismatch.
    """
    for target in _targets(scope):
        target.shadow_mismatches += 1


def record_checkpoint_recapture(
    reason: PnlCheckpointRecaptureTrigger,
    scope: PnlCheckpointScope | PnlCheckpointScopeKey | None = None,
) -> None:
    """Count one pre-replay anchor recapture decision by its closed trigger.

    Args:
        reason: Closed recapture trigger token.
        scope: Optional scope owning the recapture decision.
    """
    for target in _targets(scope):
        target.recaptures[reason] = target.recaptures.get(reason, 0) + 1


def _format_reason_counts[ReasonT: str](counts: Mapping[ReasonT, int]) -> str:
    """Format a reason-to-count map compactly with keys sorted ascending."""
    entries = ",".join(f"{reason}:{counts[reason]}" for reason in sorted(counts))
    return f"{{{entries}}}"


def record_checkpoint_shadow_skipped(
    reason: PnlCheckpointShadowSkipReason,
    scope: PnlCheckpointScope | PnlCheckpointScopeKey | None = None,
) -> None:
    """Count one skipped shadow comparison by closed reason code.

    Args:
        reason: Closed skip reason token (for example ``"window_mismatch"``).
        scope: Optional scope owning the skipped comparison.
    """
    for target in _targets(scope):
        target.shadow_skipped[reason] = target.shadow_skipped.get(reason, 0) + 1


def record_checkpoint_shadow_failure(
    reason: str,
    scope: PnlCheckpointScope | PnlCheckpointScopeKey | None = None,
) -> None:
    """Count one isolated shadow failure by exception-type reason.

    Args:
        reason: Stable ``shadow_error:<Type>`` failure token.
        scope: Optional scope owning the failed shadow branch.
    """
    for target in _targets(scope):
        target.shadow_skipped[reason] = target.shadow_skipped.get(reason, 0) + 1


def format_checkpoint_metrics_summary(
    scope: PnlCheckpointScope | PnlCheckpointScopeKey,
    metrics: PnlCheckpointMetrics,
) -> str:
    """Format one deterministic ``checkpoint_metrics_summary`` line for a scope.

    Args:
        scope: Protocol scope or aliased scope tuple ``(wallet_alias, mode, valuation_ccy)``.
        metrics: Cumulative counters for that scope.

    Returns:
        Single-line log string starting with ``checkpoint_metrics_summary``.
    """
    wallet_alias, mode, ccy = scope if isinstance(scope, tuple) else checkpoint_scope_key(scope)
    return (
        f"checkpoint_metrics_summary "
        f"wallet={wallet_alias} "
        f"mode={mode} "
        f"ccy={ccy} "
        f"captures_committed={metrics.captures_committed} "
        f"capture_refusals={_format_reason_counts(metrics.capture_refusals)} "
        f"resumes_attempted={metrics.resumes_attempted} "
        f"resumes_succeeded={metrics.resumes_succeeded} "
        f"fallbacks={_format_reason_counts(metrics.fallbacks)} "
        f"invalidations_deleted={_format_reason_counts(metrics.invalidations_deleted)} "
        f"shadow_comparisons={metrics.shadow_comparisons} "
        f"shadow_mismatches={metrics.shadow_mismatches} "
        f"shadow_skipped={_format_reason_counts(metrics.shadow_skipped)} "
        f"recaptures={_format_reason_counts(metrics.recaptures)}"
    )


def emit_checkpoint_metrics_summary(target_logger: logging.Logger) -> None:
    """Emit one cumulative INFO summary line per observed checkpoint scope.

    Args:
        target_logger: Logger receiving one INFO record per scope.
    """
    for scope, metrics in checkpoint_metrics_by_scope().items():
        target_logger.info(
            "%s",
            format_checkpoint_metrics_summary(scope, metrics),
            extra={
                "checkpoint_metrics_summary": True,
                "wallet_alias": scope[0],
                "mode": scope[1],
                "valuation_ccy": scope[2],
            },
        )


def advance_checkpoint_summary_tick(
    tick_count: int,
    mode: str,
    target_logger: logging.Logger,
    *,
    every_ticks: int = CHECKPOINT_SUMMARY_EVERY_TICKS,
) -> int:
    """Advance the active-mode tick counter and log per-scope summaries every N ticks.

    Args:
        tick_count: Completed non-off tick count before this tick.
        mode: Configured snapshotter checkpoint mode.
        target_logger: Logger receiving summary lines when the period elapses.
        every_ticks: Tick period between emitted summaries.

    Returns:
        Updated tick count (unchanged when ``mode == "off"``).
    """
    if mode == "off":
        return tick_count
    next_tick = tick_count + 1
    if next_tick % every_ticks == 0:
        emit_checkpoint_metrics_summary(target_logger)
    return next_tick
