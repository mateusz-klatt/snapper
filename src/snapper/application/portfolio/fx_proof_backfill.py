"""Operator-run certification of durable execution-minute FX requirements."""

import hashlib
import json
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import UTC
from datetime import datetime
from pathlib import Path
from typing import cast

from snapper.application.portfolio.fx_conversion_shadow import FxShadowEvaluation
from snapper.application.portfolio.fx_conversion_shadow import FxShadowPinContext
from snapper.application.portfolio.fx_conversion_shadow import FxShadowPinMetrics
from snapper.application.portfolio.fx_conversion_shadow import activate_fx_shadow_context
from snapper.application.portfolio.fx_conversion_shadow import fx_shadow_pin_metrics
from snapper.application.portfolio.fx_conversion_shadow import reset_fx_shadow_pin_metrics
from snapper.application.portfolio.fx_conversion_shadow import shadow_pin_fx_evaluations
from snapper.application.portfolio.pnl_timeline_service import _event_fx_minutes
from snapper.application.portfolio.pnl_timeline_service import _identity_fx_planes
from snapper.application.portfolio.pnl_timeline_service import _load_request_fx_rates
from snapper.application.portfolio.pnl_timeline_service import (
    _partition_series_execution_price_refs,
)
from snapper.data.fx_conversion_digests import build_requirement_manifest_digest
from snapper.data.repository import Repository
from snapper.data.repository_types import FxConversionSuccessfulQuery
from snapper.data.repository_types import FxProofBackfillConsumer
from snapper.data.repository_types import InstrumentSymbolRefRow
from snapper.data.repository_types import PnlTimelineOpeningExecutionRow

FX_PROOF_BACKFILL_BATCH_SIZE = 50
FX_PROOF_BACKFILL_CHECKPOINT = Path("data/fx-proof-backfill-checkpoint.json")


@dataclass(frozen=True, slots=True)
class FxProofBackfillRequirement:
    """One consumer-bound exact-minute election and its pin state."""

    wallet_public_id: str
    valuation_ccy: str
    calculation_version: str
    consumer_public_id: str
    evaluation: FxShadowEvaluation
    pinned: bool


@dataclass(frozen=True, slots=True)
class FxProofBackfillResult:
    """Stable report plus the seven shared persistence counters."""

    requirements: tuple[FxProofBackfillRequirement, ...]
    semantic_refusals: tuple[str, ...]
    metrics: FxShadowPinMetrics
    processed: int


@dataclass(frozen=True, slots=True)
class FxProofBackfillDiscovery:
    """Derived FX requirements and pre-election semantic refusals."""

    requirements: tuple[FxProofBackfillRequirement, ...]
    semantic_refusals: tuple[str, ...]


def _requirement_key(requirement: FxProofBackfillRequirement) -> str:
    """Return the deterministic checkpoint and ordering identity."""
    evaluation = requirement.evaluation
    minute_digest = build_requirement_manifest_digest(tuple(sorted(evaluation.required_minutes)))
    owner = evaluation.consumer_instrument_public_id or ""
    return "|".join(
        (
            requirement.consumer_public_id,
            requirement.wallet_public_id,
            requirement.valuation_ccy,
            requirement.calculation_version,
            evaluation.scope_kind,
            owner,
            "-".join(evaluation.pair),
            minute_digest,
        )
    )


def _manifest_digest(requirements: list[FxProofBackfillRequirement]) -> str:
    """Bind a checkpoint to the complete ordered durable requirement universe."""
    payload = "\n".join(_requirement_key(item) for item in requirements).encode()
    return hashlib.sha256(payload).hexdigest()


def _checkpoint_start(path: Path, digest: str) -> int:
    """Read a resumable cursor and refuse a checkpoint for a changed universe."""
    if not path.exists():
        return 0
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict) or payload.get("manifest_digest") != digest:
        raise ValueError("FX proof backfill checkpoint does not match current requirements")
    cursor = payload.get("cursor")
    if not isinstance(cursor, int) or cursor < 0:
        raise ValueError("FX proof backfill checkpoint cursor is invalid")
    return cursor


def _write_checkpoint(path: Path, digest: str, cursor: int) -> None:
    """Atomically publish the last fully processed bounded-batch cursor."""
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(f"{path.suffix}.tmp")
    temporary.write_text(
        json.dumps(
            {"cursor": cursor, "manifest_digest": digest},
            sort_keys=True,
            separators=(",", ":"),
        ),
        encoding="utf-8",
    )
    temporary.replace(path)


def _trusted_execution_context(
    rows: Sequence[PnlTimelineOpeningExecutionRow], refs: list[InstrumentSymbolRefRow]
) -> tuple[list[PnlTimelineOpeningExecutionRow], list[InstrumentSymbolRefRow]]:
    """Apply the timeline's complete-span denomination and venue proof."""
    trusted_refs, reasons = _partition_series_execution_price_refs(rows, refs)
    trusted_ids = {ref["instrument_public_id"] for ref in trusted_refs}
    trusted_rows = [row for row in rows if row["instrument_public_id"] in trusted_ids]
    if reasons:
        missing = ",".join(sorted(reasons))
        raise ValueError(f"execution FX requirement identity cannot be proven: {missing}")
    return trusted_rows, trusted_refs


def _union_consumers(
    consumers: list[FxProofBackfillConsumer],
) -> list[FxProofBackfillConsumer]:
    """Fold active artifacts into their wallet/valuation calculation-version unions."""
    unions: dict[tuple[str, str, str, str], FxProofBackfillConsumer] = {}
    for consumer in consumers:
        key = (
            consumer.wallet_public_id,
            consumer.mode,
            consumer.valuation_ccy,
            consumer.calculation_version,
        )
        existing = unions.get(key)
        watermarks = dict(existing.watermarks) if existing is not None else {}
        for exchange, watermark in consumer.watermarks.items():
            watermarks[exchange] = max(watermarks.get(exchange, 0), watermark)
        representative = consumer
        if existing is not None and existing.knowledge_at > consumer.knowledge_at:
            representative = existing
        unions[key] = FxProofBackfillConsumer(
            public_id=representative.public_id,
            wallet_public_id=consumer.wallet_public_id,
            mode=consumer.mode,
            valuation_ccy=consumer.valuation_ccy,
            calculation_version=consumer.calculation_version,
            point_kind=representative.point_kind,
            knowledge_at=max(
                consumer.knowledge_at,
                existing.knowledge_at if existing is not None else consumer.knowledge_at,
            ),
            watermarks=watermarks,
        )
    return [unions[key] for key in sorted(unions)]


async def _consumer_evaluations(
    repo: Repository,
    consumer: FxProofBackfillConsumer,
    as_of: datetime,
) -> list[FxShadowEvaluation]:
    """Derive singleton manifests through the production timeline election path."""
    prefix = await repo.get_pnl_timeline_execution_prefix_at_watermarks(
        consumer.wallet_public_id,
        consumer.mode,
        consumer.watermarks,
        as_of,
    )
    rows = prefix["executions"]
    instrument_ids = sorted({row["instrument_public_id"] for row in rows})
    refs = await repo.get_instrument_symbol_refs(instrument_ids, as_of)
    trusted_rows, trusted_refs = _trusted_execution_context(rows, refs)
    base = {ref["instrument_public_id"]: ref["base_currency"] for ref in trusted_refs}
    quote = {ref["instrument_public_id"]: cast(str, ref["quote_currency"]) for ref in trusted_refs}
    requirements = _event_fx_minutes(
        trusted_rows,
        (),
        base,
        quote,
        consumer.valuation_ccy,
    )
    evaluations: list[FxShadowEvaluation] = []
    for instrument_id, pairs in sorted(requirements.items()):
        for pair, minutes in sorted(pairs.items()):
            for minute in sorted(minutes):
                singleton = {instrument_id: {pair: {minute}}}
                identity = _identity_fx_planes(trusted_refs, singleton)
                context = FxShadowPinContext(consumer.calculation_version, [])
                with activate_fx_shadow_context(context):
                    await _load_request_fx_rates(
                        repo,
                        {},
                        singleton,
                        identity,
                        as_of,
                        consumer.valuation_ccy,
                    )
                evaluations.extend(context.evaluations)
    return evaluations


async def _is_pinned(
    repo: Repository, evaluation: FxShadowEvaluation, calculation_version: str
) -> bool:
    """Check for a matching visible successful singleton proof."""
    target = evaluation.target_currency
    source = evaluation.pair[1] if evaluation.pair[0] == target else evaluation.pair[0]
    query = FxConversionSuccessfulQuery(
        scope_kind=evaluation.scope_kind,
        consumer_instrument_public_id=evaluation.consumer_instrument_public_id,
        source_currency=source,
        target_currency=target,
        unordered_pair="-".join(evaluation.pair),
        required_minutes=tuple(sorted(evaluation.required_minutes)),
        election_policy_version="pnl-fiat-v1",
        calculation_version=calculation_version,
    )
    return (
        await repo.get_latest_visible_fx_conversion_artifact(query, datetime.now(UTC)) is not None
    )


async def discover_fx_proof_backfill(
    repo: Repository,
) -> FxProofBackfillDiscovery:
    """Enumerate the active durable-consumer union without extending any cut.

    Args:
        repo: Repository providing durable consumers and raw FX evidence.

    Returns:
        Deterministically ordered requirements and explicit semantic refusals.
    """
    as_of = datetime.now(UTC)
    requirements: list[FxProofBackfillRequirement] = []
    refusals: list[str] = []
    consumers = _union_consumers(await repo.list_fx_proof_backfill_consumers())
    for consumer in consumers:
        try:
            evaluations = await _consumer_evaluations(repo, consumer, as_of)
        except (RuntimeError, ValueError) as error:
            refusals.append(
                " | ".join(
                    (
                        consumer.wallet_public_id,
                        consumer.valuation_ccy,
                        consumer.calculation_version,
                        str(error),
                    )
                )
            )
            continue
        for evaluation in evaluations:
            requirements.append(
                FxProofBackfillRequirement(
                    wallet_public_id=consumer.wallet_public_id,
                    valuation_ccy=consumer.valuation_ccy,
                    calculation_version=consumer.calculation_version,
                    consumer_public_id=consumer.public_id,
                    evaluation=evaluation,
                    pinned=await _is_pinned(repo, evaluation, consumer.calculation_version),
                )
            )
    return FxProofBackfillDiscovery(
        tuple(sorted(requirements, key=_requirement_key)),
        tuple(sorted(refusals)),
    )


async def run_fx_proof_backfill(
    repo: Repository,
    apply: bool,
    checkpoint_path: Path = FX_PROOF_BACKFILL_CHECKPOINT,
) -> FxProofBackfillResult:
    """Report requirements or persist them in checkpointed constant-size batches.

    Args:
        repo: Repository receiving canonical artifacts in apply mode.
        apply: Whether to persist instead of returning a no-write report.
        checkpoint_path: Durable cursor file advanced after complete batches.

    Returns:
        The complete report, shared counters, and committed cursor position.
    """
    reset_fx_shadow_pin_metrics()
    discovery = await discover_fx_proof_backfill(repo)
    requirements = list(discovery.requirements)
    if not apply:
        return FxProofBackfillResult(
            tuple(requirements), discovery.semantic_refusals, fx_shadow_pin_metrics(), 0
        )
    digest = _manifest_digest(requirements)
    start = _checkpoint_start(checkpoint_path, digest)
    processed = start
    for batch_start in range(start, len(requirements), FX_PROOF_BACKFILL_BATCH_SIZE):
        batch = requirements[batch_start : batch_start + FX_PROOF_BACKFILL_BATCH_SIZE]
        for requirement in batch:
            await shadow_pin_fx_evaluations(
                repo,
                (requirement.evaluation,),
                requirement.calculation_version,
            )
        metrics = fx_shadow_pin_metrics()
        if any(
            (
                metrics.conflict,
                metrics.upgrade_required,
                metrics.mismatch,
                metrics.failure,
                metrics.dropped,
            )
        ):
            break
        processed = batch_start + len(batch)
        _write_checkpoint(checkpoint_path, digest, processed)
    if processed == len(requirements) and checkpoint_path.exists():
        checkpoint_path.unlink()
    return FxProofBackfillResult(
        tuple(requirements),
        discovery.semantic_refusals,
        fx_shadow_pin_metrics(),
        processed,
    )
