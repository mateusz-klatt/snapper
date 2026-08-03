"""Operator certification of durable consumer-wide FX requirement electorates."""

import fcntl
import hashlib
import json
import os
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import cast

from snapper.application.portfolio.fx_conversion_shadow import FX_ELECTION_POLICY_VERSION
from snapper.application.portfolio.fx_conversion_shadow import FxShadowEvaluation
from snapper.application.portfolio.fx_conversion_shadow import FxShadowPinContext
from snapper.application.portfolio.fx_conversion_shadow import FxShadowPinMetrics
from snapper.application.portfolio.fx_conversion_shadow import activate_fx_shadow_context
from snapper.application.portfolio.fx_conversion_shadow import fx_shadow_evaluation_completeness
from snapper.application.portfolio.fx_conversion_shadow import fx_shadow_pin_metrics
from snapper.application.portfolio.fx_conversion_shadow import shadow_pin_fx_evaluations
from snapper.application.portfolio.pnl_timeline_service import _event_fx_minutes
from snapper.application.portfolio.pnl_timeline_service import _general_fx_minutes
from snapper.application.portfolio.pnl_timeline_service import _identity_fx_planes
from snapper.application.portfolio.pnl_timeline_service import _load_request_fx_rates
from snapper.application.portfolio.pnl_timeline_service import (
    _partition_series_execution_price_refs,
)
from snapper.data.fx_conversion_digests import build_requirement_manifest_digest
from snapper.data.repository import Repository
from snapper.data.repository_types import FxConversionRefusalQuery
from snapper.data.repository_types import FxConversionSuccessfulQuery
from snapper.data.repository_types import FxProofBackfillConsumer
from snapper.data.repository_types import PnlTimelineAccrualRow
from snapper.data.repository_types import PnlTimelineOpeningExecutionRow

FX_PROOF_BACKFILL_BATCH_SIZE = 50
_PROJECT_ROOT = Path(__file__).resolve().parents[4]
FX_PROOF_BACKFILL_CHECKPOINT = _PROJECT_ROOT / "data" / "fx-proof-backfill-checkpoint.json"


@dataclass(frozen=True, slots=True)
class FxProofBackfillRequirement:
    """One consumer-bound full per-pair electorate and its current DB state."""

    wallet_public_id: str
    valuation_ccy: str
    calculation_version: str
    knowledge_at: datetime
    evaluation: FxShadowEvaluation
    pinned: bool
    refusal_audited: bool


@dataclass(frozen=True, slots=True)
class FxProofBackfillSemanticRefusal:
    """One instrument whose conversion requirements could not be derived safely."""

    wallet_public_id: str
    valuation_ccy: str
    calculation_version: str
    instrument_public_id: str
    reason: str
    lost_requirements: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class FxProofBackfillDiscovery:
    """Requirements and instrument-granular pre-election refusals."""

    requirements: tuple[FxProofBackfillRequirement, ...]
    semantic_refusals: tuple[FxProofBackfillSemanticRefusal, ...]


@dataclass(frozen=True, slots=True)
class FxProofBackfillResult:
    """Post-run DB state, metric deltas, outcome counts, and completion state."""

    requirements: tuple[FxProofBackfillRequirement, ...]
    semantic_refusals: tuple[FxProofBackfillSemanticRefusal, ...]
    metrics: FxShadowPinMetrics
    processed_consumers: int
    proof_creations: int
    refusal_audit_creations: int
    aborted: bool
    fully_verified: bool


def _consumer_key(consumer: FxProofBackfillConsumer) -> str:
    """Return the stable union identity used by checkpoint ordering."""
    watermarks = json.dumps(consumer.watermarks, sort_keys=True, separators=(",", ":"))
    return "|".join(
        (
            consumer.wallet_public_id,
            consumer.mode,
            consumer.valuation_ccy,
            consumer.calculation_version,
            consumer.knowledge_at.isoformat(),
            watermarks,
        )
    )


def _requirement_key(requirement: FxProofBackfillRequirement) -> str:
    """Return the canonical report and deduplication identity."""
    evaluation = requirement.evaluation
    manifest = build_requirement_manifest_digest(tuple(sorted(evaluation.required_minutes)))
    return "|".join(
        (
            requirement.wallet_public_id,
            requirement.valuation_ccy,
            requirement.calculation_version,
            evaluation.scope_kind,
            evaluation.consumer_instrument_public_id or "",
            "-".join(evaluation.pair),
            manifest,
        )
    )


def _consumer_manifest(consumers: list[FxProofBackfillConsumer]) -> str:
    """Digest the ordered consumer-union universe bound to a cursor hint."""
    payload = "\n".join(_consumer_key(consumer) for consumer in consumers).encode()
    return hashlib.sha256(payload).hexdigest()


def _checkpoint_start(path: Path, digest: str, consumer_count: int) -> int:
    """Read a valid cursor hint, ignoring stale state from a changed universe."""
    if not path.exists():
        return 0
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict) or payload.get("manifest_digest") != digest:
        return 0
    cursor = payload.get("cursor")
    if not isinstance(cursor, int) or cursor < 0 or cursor >= max(consumer_count, 1):
        return 0
    return cursor


def _fsync_directory(path: Path) -> None:
    """Make a checkpoint rename or removal durable in its containing directory."""
    descriptor = os.open(path, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _write_checkpoint(path: Path, digest: str, cursor: int) -> None:
    """Atomically publish and fsync one completed-consumer cursor hint."""
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(f"{path.suffix}.tmp")
    payload = json.dumps(
        {"cursor": cursor, "manifest_digest": digest},
        sort_keys=True,
        separators=(",", ":"),
    )
    with temporary.open("w", encoding="utf-8") as stream:
        stream.write(payload)
        stream.flush()
        os.fsync(stream.fileno())
    temporary.replace(path)
    _fsync_directory(path.parent)


def reset_fx_proof_backfill_checkpoint(path: Path) -> None:
    """Remove one anchored cursor hint and durably publish its absence.

    Args:
        path: Anchored checkpoint path to remove when present.
    """
    if path.exists():
        path.unlink()
        _fsync_directory(path.parent)


@contextmanager
def fx_proof_backfill_apply_lock(checkpoint_path: Path) -> Iterator[None]:
    """Hold one nonblocking host lock for the complete apply operation.

    Args:
        checkpoint_path: Anchored cursor whose sibling lock serializes apply.

    Yields:
        Control while this process owns the exclusive host lock.
    """
    lock_path = checkpoint_path.with_suffix(f"{checkpoint_path.suffix}.lock")
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    with lock_path.open("a+", encoding="utf-8") as stream:
        try:
            fcntl.flock(stream.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as error:
            raise ValueError("another FX proof backfill apply is already running") from error
        try:
            yield
        finally:
            fcntl.flock(stream.fileno(), fcntl.LOCK_UN)


def _metric_delta(before: FxShadowPinMetrics, after: FxShadowPinMetrics) -> FxShadowPinMetrics:
    """Subtract two process-global snapshots without resetting shared counters."""
    return FxShadowPinMetrics(
        creation=after.creation - before.creation,
        reuse=after.reuse - before.reuse,
        conflict=after.conflict - before.conflict,
        upgrade_required=after.upgrade_required - before.upgrade_required,
        mismatch=after.mismatch - before.mismatch,
        failure=after.failure - before.failure,
        dropped=after.dropped - before.dropped,
    )


def _has_adverse_metric(metrics: FxShadowPinMetrics) -> bool:
    """Return whether a batch encountered any non-convergent pin outcome."""
    return any(
        (
            metrics.conflict,
            metrics.upgrade_required,
            metrics.mismatch,
            metrics.failure,
            metrics.dropped,
        )
    )


def _lost_requirements(
    instrument_id: str,
    executions: list[PnlTimelineOpeningExecutionRow],
    accruals: list[PnlTimelineAccrualRow],
) -> tuple[str, ...]:
    """Enumerate the exact durable events lost with one unprovable instrument."""
    lost = {
        f"execution:{row['public_id']}@{row['timestamp'].isoformat()}"
        for row in executions
        if row["instrument_public_id"] == instrument_id
    }
    lost.update(
        f"accrual:{row['accrual_type']}@{row['accrued_at'].isoformat()}"
        for row in accruals
        if row["instrument_public_id"] == instrument_id
    )
    return tuple(sorted(lost))


def _expected_evaluation_count(
    requirements: dict[str, dict[tuple[str, str], set[datetime]]],
    identities: dict[str, tuple[str, str, str]],
) -> int:
    """Count the shared and instrument-owned electorates F2 must construct."""
    general = len(_general_fx_minutes(requirements, identities))
    owned = 0
    for instrument_id, plane in identities.items():
        pair = tuple(sorted((plane[0], plane[1])))
        if pair in requirements.get(instrument_id, {}):
            owned += 1
    return general + owned


async def _artifact_state(
    repo: Repository,
    evaluation: FxShadowEvaluation,
    calculation_version: str,
    knowledge_at: datetime,
) -> tuple[bool, bool]:
    """Read proof and refusal-audit state at the durable consumer horizon."""
    target = evaluation.target_currency
    source = evaluation.pair[1] if evaluation.pair[0] == target else evaluation.pair[0]
    common = FxConversionSuccessfulQuery(
        scope_kind=evaluation.scope_kind,
        consumer_instrument_public_id=evaluation.consumer_instrument_public_id,
        source_currency=source,
        target_currency=target,
        unordered_pair="-".join(evaluation.pair),
        required_minutes=tuple(sorted(evaluation.required_minutes)),
        election_policy_version=FX_ELECTION_POLICY_VERSION,
        calculation_version=calculation_version,
    )
    pinned = await repo.get_latest_visible_fx_conversion_artifact(common, knowledge_at) is not None
    if fx_shadow_evaluation_completeness(evaluation) != "refused":
        return pinned, False
    reason = _canonical_refusal_reason(evaluation)
    refusal = FxConversionRefusalQuery(**common, refusal_reason_json=reason)
    return pinned, await repo.has_visible_fx_conversion_refusal(refusal, knowledge_at)


def _canonical_refusal_reason(evaluation: FxShadowEvaluation) -> str:
    """Build the same canonical missing-minute reason F2 persists."""
    missing = [minute.isoformat() for minute in sorted(evaluation.required_minutes)]
    return json.dumps(
        {"reason": "fx_conversion_unproven", "unproven_minutes": missing},
        sort_keys=True,
        separators=(",", ":"),
    )


async def _consumer_discovery(
    repo: Repository,
    consumer: FxProofBackfillConsumer,
) -> FxProofBackfillDiscovery:
    """Derive one consumer's true full per-pair electorates at its own horizon."""
    horizon = consumer.knowledge_at
    prefix = await repo.get_pnl_timeline_execution_prefix_at_watermarks(
        consumer.wallet_public_id,
        consumer.mode,
        consumer.watermarks,
        horizon,
    )
    executions = prefix["executions"]
    accruals = await repo.get_accruals_for_pnl(consumer.wallet_public_id, consumer.mode, horizon)
    instrument_ids = sorted(
        {row["instrument_public_id"] for row in executions}
        | {row["instrument_public_id"] for row in accruals}
    )
    refs = await repo.get_instrument_symbol_refs(instrument_ids, horizon)
    trusted_refs, reasons = _partition_series_execution_price_refs(executions, refs)
    trusted_ids = {ref["instrument_public_id"] for ref in trusted_refs}
    trusted_executions = [row for row in executions if row["instrument_public_id"] in trusted_ids]
    refusals = tuple(
        FxProofBackfillSemanticRefusal(
            wallet_public_id=consumer.wallet_public_id,
            valuation_ccy=consumer.valuation_ccy,
            calculation_version=consumer.calculation_version,
            instrument_public_id=instrument_id,
            reason=",".join(sorted(instrument_reasons)),
            lost_requirements=_lost_requirements(instrument_id, executions, accruals),
        )
        for instrument_id, instrument_reasons in sorted(reasons.items())
    )
    base = {ref["instrument_public_id"]: ref["base_currency"] for ref in trusted_refs}
    quote = {ref["instrument_public_id"]: cast(str, ref["quote_currency"]) for ref in trusted_refs}
    event_requirements = _event_fx_minutes(
        trusted_executions,
        accruals,
        base,
        quote,
        consumer.valuation_ccy,
    )
    identities = _identity_fx_planes(trusted_refs, event_requirements)
    context = FxShadowPinContext(consumer.calculation_version, [], manifest_limit=None)
    with activate_fx_shadow_context(context):
        await _load_request_fx_rates(
            repo,
            {},
            event_requirements,
            identities,
            horizon,
            consumer.valuation_ccy,
        )
    expected = _expected_evaluation_count(event_requirements, identities)
    if len(context.evaluations) != expected:
        refusal = FxProofBackfillSemanticRefusal(
            wallet_public_id=consumer.wallet_public_id,
            valuation_ccy=consumer.valuation_ccy,
            calculation_version=consumer.calculation_version,
            instrument_public_id="*",
            reason="fx_evaluation_count_mismatch",
            lost_requirements=tuple(sorted(_requirement_events(event_requirements))),
        )
        return FxProofBackfillDiscovery((), (*refusals, refusal))
    deduplicated = {_evaluation_key(evaluation): evaluation for evaluation in context.evaluations}
    requirements: list[FxProofBackfillRequirement] = []
    for evaluation in deduplicated.values():
        pinned, audited = await _artifact_state(
            repo,
            evaluation,
            consumer.calculation_version,
            horizon,
        )
        requirements.append(
            FxProofBackfillRequirement(
                wallet_public_id=consumer.wallet_public_id,
                valuation_ccy=consumer.valuation_ccy,
                calculation_version=consumer.calculation_version,
                knowledge_at=horizon,
                evaluation=evaluation,
                pinned=pinned,
                refusal_audited=audited,
            )
        )
    return FxProofBackfillDiscovery(
        tuple(sorted(requirements, key=_requirement_key)),
        refusals,
    )


def _evaluation_key(evaluation: FxShadowEvaluation) -> tuple[object, ...]:
    """Deduplicate shared identities without merging instrument-owned scopes."""
    return (
        evaluation.scope_kind,
        evaluation.consumer_instrument_public_id,
        evaluation.pair,
        tuple(sorted(evaluation.required_minutes)),
    )


def _requirement_events(
    requirements: dict[str, dict[tuple[str, str], set[datetime]]],
) -> set[str]:
    """Flatten lost evaluator inputs for an explicit invariant refusal."""
    return {
        f"{instrument_id}:{'-'.join(pair)}@{minute.isoformat()}"
        for instrument_id, pairs in requirements.items()
        for pair, minutes in pairs.items()
        for minute in minutes
    }


async def discover_fx_proof_backfill(repo: Repository) -> FxProofBackfillDiscovery:
    """Derive every SQL-aggregated consumer union for no-write reporting.

    Args:
        repo: Repository providing durable consumers and raw FX evidence.

    Returns:
        Full per-pair requirements and instrument-granular refusals.
    """
    requirements: list[FxProofBackfillRequirement] = []
    refusals: list[FxProofBackfillSemanticRefusal] = []
    for consumer in await repo.list_fx_proof_backfill_consumers():
        discovery = await _consumer_discovery(repo, consumer)
        requirements.extend(discovery.requirements)
        refusals.extend(discovery.semantic_refusals)
    deduplicated = {_requirement_key(item): item for item in requirements}
    return FxProofBackfillDiscovery(
        tuple(deduplicated[key] for key in sorted(deduplicated)),
        tuple(sorted(refusals, key=_semantic_refusal_key)),
    )


def _semantic_refusal_key(refusal: FxProofBackfillSemanticRefusal) -> tuple[str, ...]:
    """Return deterministic report ordering for instrument refusals."""
    return (
        refusal.wallet_public_id,
        refusal.valuation_ccy,
        refusal.calculation_version,
        refusal.instrument_public_id,
        refusal.reason,
    )


async def _refresh_requirement(
    repo: Repository,
    requirement: FxProofBackfillRequirement,
) -> FxProofBackfillRequirement:
    """Recompute current artifact state at the consumer's sealed horizon."""
    pinned, audited = await _artifact_state(
        repo,
        requirement.evaluation,
        requirement.calculation_version,
        requirement.knowledge_at,
    )
    return FxProofBackfillRequirement(
        wallet_public_id=requirement.wallet_public_id,
        valuation_ccy=requirement.valuation_ccy,
        calculation_version=requirement.calculation_version,
        knowledge_at=requirement.knowledge_at,
        evaluation=requirement.evaluation,
        pinned=pinned,
        refusal_audited=audited,
    )


async def _apply_requirement(
    repo: Repository,
    requirement: FxProofBackfillRequirement,
) -> tuple[FxProofBackfillRequirement, int, int]:
    """Let current DB state skip work, then classify a newly created outcome."""
    current = await _refresh_requirement(repo, requirement)
    if current.pinned or current.refusal_audited:
        return current, 0, 0
    before = fx_shadow_pin_metrics()
    await shadow_pin_fx_evaluations(
        repo,
        (current.evaluation,),
        current.calculation_version,
    )
    delta = _metric_delta(before, fx_shadow_pin_metrics())
    refreshed = await _refresh_requirement(repo, current)
    proof_creation = int(delta.creation > 0 and refreshed.pinned)
    refusal_creation = int(delta.creation > 0 and refreshed.refusal_audited)
    return refreshed, proof_creation, refusal_creation


def _ordered_consumers(
    consumers: list[FxProofBackfillConsumer], start: int
) -> list[tuple[int, FxProofBackfillConsumer]]:
    """Rotate by the hint while retaining every DB-decided consumer as work."""
    indexed = list(enumerate(consumers))
    return [*indexed[start:], *indexed[:start]]


async def run_fx_proof_backfill(
    repo: Repository,
    apply: bool,
    checkpoint_path: Path = FX_PROOF_BACKFILL_CHECKPOINT,
) -> FxProofBackfillResult:
    """Report or apply consumer-by-consumer with DB-authoritative recovery.

    Args:
        repo: Repository providing durable consumers and proof persistence.
        apply: Whether to persist instead of returning a no-write report.
        checkpoint_path: Anchored completed-consumer cursor hint.

    Returns:
        Post-run state, metric deltas, classified creations, and completion flags.
    """
    metrics_before = fx_shadow_pin_metrics()
    if not apply:
        discovery = await discover_fx_proof_backfill(repo)
        return FxProofBackfillResult(
            discovery.requirements,
            discovery.semantic_refusals,
            _metric_delta(metrics_before, fx_shadow_pin_metrics()),
            0,
            0,
            0,
            False,
            True,
        )
    consumers = await repo.list_fx_proof_backfill_consumers()
    digest = _consumer_manifest(consumers)
    start = _checkpoint_start(checkpoint_path, digest, len(consumers))
    requirements: list[FxProofBackfillRequirement] = []
    refusals: list[FxProofBackfillSemanticRefusal] = []
    processed = 0
    proof_creations = 0
    refusal_creations = 0
    aborted = False
    for index, consumer in _ordered_consumers(consumers, start):
        discovery = await _consumer_discovery(repo, consumer)
        refusals.extend(discovery.semantic_refusals)
        consumer_requirements: list[FxProofBackfillRequirement] = []
        consumer_metrics = fx_shadow_pin_metrics()
        for batch_start in range(0, len(discovery.requirements), FX_PROOF_BACKFILL_BATCH_SIZE):
            batch = discovery.requirements[batch_start : batch_start + FX_PROOF_BACKFILL_BATCH_SIZE]
            for requirement in batch:
                refreshed, proof_count, refusal_count = await _apply_requirement(repo, requirement)
                consumer_requirements.append(refreshed)
                proof_creations += proof_count
                refusal_creations += refusal_count
            if _has_adverse_metric(_metric_delta(consumer_metrics, fx_shadow_pin_metrics())):
                aborted = True
                break
        requirements.extend(consumer_requirements)
        if aborted:
            break
        processed += 1
        next_cursor = (index + 1) % max(len(consumers), 1)
        _write_checkpoint(checkpoint_path, digest, next_cursor)
    refreshed_requirements = [
        await _refresh_requirement(repo, requirement) for requirement in requirements
    ]
    fully_verified = (
        not aborted
        and not refusals
        and all(item.pinned or item.refusal_audited for item in refreshed_requirements)
    )
    if fully_verified:
        reset_fx_proof_backfill_checkpoint(checkpoint_path)
    return FxProofBackfillResult(
        tuple(sorted(refreshed_requirements, key=_requirement_key)),
        tuple(sorted(refusals, key=_semantic_refusal_key)),
        _metric_delta(metrics_before, fx_shadow_pin_metrics()),
        processed,
        proof_creations,
        refusal_creations,
        aborted,
        fully_verified,
    )
