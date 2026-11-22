"""Ordered verification of persisted opening checkpoints before suffix resume."""

import asyncio
import logging
from collections.abc import Mapping
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime
from datetime import timedelta
from typing import Literal

from snapper.application.portfolio.execution_chain import ExecutionChainError
from snapper.application.portfolio.execution_chain import execution_chain_genesis
from snapper.application.portfolio.pnl_engine_options import pnl_engine_options_for
from snapper.application.portfolio.pnl_opening_checkpoint_bundle import PnlCheckpointStateBundle
from snapper.application.portfolio.pnl_opening_checkpoint_bundle import bundle_from_json
from snapper.application.portfolio.pnl_opening_checkpoint_bundle import (
    is_unsupported_bundle_version,
)
from snapper.application.portfolio.pnl_opening_checkpoint_certificate import (
    event_fx_exchanges_digest,
)
from snapper.application.portfolio.pnl_opening_checkpoint_certificate import (
    execution_manifest_digest,
)
from snapper.application.portfolio.pnl_opening_checkpoint_certificate import (
    partition_execution_watermarks,
)
from snapper.application.portfolio.pnl_opening_checkpoint_certificate import prefix_annulment_digest
from snapper.application.portfolio.pnl_opening_checkpoint_certificate import prefix_lineage_digest
from snapper.application.portfolio.pnl_opening_checkpoint_certificate import symbol_ref_digest
from snapper.data.pnl_correction_state import IdAggregate
from snapper.data.pnl_correction_state import PnlOpeningCheckpointCertificate
from snapper.data.pnl_correction_state import PnlOpeningCheckpointRecord
from snapper.data.repository import PortfolioPnlSampleScope
from snapper.data.repository import Repository
from snapper.data.repository_types import PnlFxRatePlane
from snapper.data.repository_types import PnlTimelineAppliedAnnulment
from snapper.data.repository_types import PnlTimelineExecutionPrefix

logger = logging.getLogger(__name__)

PnlCheckpointFallbackReason = Literal[
    "checkpoint_absent",
    "checkpoint_ineligible",
    "checkpoint_version_mismatch",
    "checkpoint_bundle_version",
    "checkpoint_fill_gap",
    "checkpoint_payload_corrupt",
    "checkpoint_verify_error",
    "checkpoint_anchor_mismatch",
    "checkpoint_execution_counts_changed",
    "checkpoint_execution_chain_changed",
    "checkpoint_prefix_annulment_changed",
    "checkpoint_execution_manifest_changed",
    "checkpoint_prefix_witness_changed",
    "checkpoint_prefix_assignments_changed",
    "checkpoint_accrual_prefix_changed",
    "checkpoint_lineage_prefix_changed",
    "checkpoint_symbol_refs_changed",
    "checkpoint_price_provenance_changed",
    "checkpoint_event_fx_exchanges_changed",
    "checkpoint_event_fx_candles_changed",
    "checkpoint_fx_election_changed",
    "checkpoint_late_execution",
    "checkpoint_verification_horizon_mismatch",
]
"""Closed fallback vocabulary for checkpoint resume refusal."""

_SERVICE_FALLBACK_REASONS: dict[str, PnlCheckpointFallbackReason] = {
    "checkpoint_absent": "checkpoint_absent",
    "checkpoint_ineligible": "checkpoint_ineligible",
    "checkpoint_version_mismatch": "checkpoint_version_mismatch",
    "checkpoint_bundle_version": "checkpoint_bundle_version",
    "checkpoint_fill_gap": "checkpoint_fill_gap",
    "checkpoint_payload_corrupt": "checkpoint_payload_corrupt",
    "checkpoint_verify_error": "checkpoint_verify_error",
    "checkpoint_anchor_mismatch": "checkpoint_anchor_mismatch",
    "checkpoint_execution_counts_changed": "checkpoint_execution_counts_changed",
    "checkpoint_execution_chain_changed": "checkpoint_execution_chain_changed",
    "checkpoint_prefix_annulment_changed": "checkpoint_prefix_annulment_changed",
    "checkpoint_execution_manifest_changed": "checkpoint_execution_manifest_changed",
    "checkpoint_prefix_witness_changed": "checkpoint_prefix_witness_changed",
    "checkpoint_prefix_assignments_changed": "checkpoint_prefix_assignments_changed",
    "checkpoint_accrual_prefix_changed": "checkpoint_accrual_prefix_changed",
    "checkpoint_lineage_prefix_changed": "checkpoint_lineage_prefix_changed",
    "checkpoint_symbol_refs_changed": "checkpoint_symbol_refs_changed",
    "checkpoint_price_provenance_changed": "checkpoint_price_provenance_changed",
    "checkpoint_event_fx_exchanges_changed": "checkpoint_event_fx_exchanges_changed",
    "checkpoint_event_fx_candles_changed": "checkpoint_event_fx_candles_changed",
    "checkpoint_fx_election_changed": "checkpoint_fx_election_changed",
    "checkpoint_late_execution": "checkpoint_late_execution",
    "checkpoint_verification_horizon_mismatch": "checkpoint_verification_horizon_mismatch",
}
"""Exact lookup mapping closed service fallback strings back to typed literals."""

_DELETE_REASONS: frozenset[str] = frozenset(
    {
        "checkpoint_bundle_version",
        "checkpoint_payload_corrupt",
        "checkpoint_anchor_mismatch",
        "checkpoint_execution_counts_changed",
        "checkpoint_execution_chain_changed",
        "checkpoint_prefix_annulment_changed",
        "checkpoint_execution_manifest_changed",
        "checkpoint_prefix_witness_changed",
        "checkpoint_prefix_assignments_changed",
        "checkpoint_accrual_prefix_changed",
        "checkpoint_lineage_prefix_changed",
        "checkpoint_symbol_refs_changed",
        "checkpoint_price_provenance_changed",
        "checkpoint_event_fx_exchanges_changed",
        "checkpoint_event_fx_candles_changed",
        "checkpoint_fx_election_changed",
        "checkpoint_late_execution",
    }
)
"""Fallbacks whose stored checkpoint must be retired with a generation fence."""


@dataclass(frozen=True)
class PnlCheckpointResume:
    """Verified persisted record, decoded service state, and prefix annulments."""

    record: PnlOpeningCheckpointRecord
    bundle: PnlCheckpointStateBundle
    prefix_annulments: tuple[PnlTimelineAppliedAnnulment, ...] = ()
    verified_fx_planes: tuple[PnlFxRatePlane, ...] | None = None
    verified_at: datetime | None = None


@dataclass(frozen=True)
class PnlCheckpointFallback:
    """Stable fallback reason and whether the stored row requires fenced deletion."""

    reason: PnlCheckpointFallbackReason
    delete_required: bool


@dataclass(frozen=True)
class PnlCheckpointResumeEligibility:
    """Caller policy, version, anchor, generation-manifest, and window facts."""

    mode: str
    calc_version: str
    timeline_calc_version: str
    fx_policy_version: str
    expected_anchor_public_id: str
    expected_anchor_digest: str
    pointwise_exact_fx: bool
    replay_generation_tip: datetime | None
    metadata_only: bool
    from_time: datetime
    frontier_tip: datetime
    has_fill_gap: bool
    expected_execution_manifest_digest: str | None = None


def _fallback(reason: PnlCheckpointFallbackReason) -> PnlCheckpointFallback:
    """Build one fallback with its deletion policy derived from the closed vocabulary."""
    return PnlCheckpointFallback(reason=reason, delete_required=reason in _DELETE_REASONS)


def service_resume_fallback(reason: str) -> PnlCheckpointFallback:
    """Resolve one service-reported fallback string into a closed fallback verdict.

    Args:
        reason: Fallback reason emitted by the timeline service or ``unexpected``.

    Returns:
        Closed :class:`PnlCheckpointFallback` preserving known closed codes and
        mapping unrecognised reasons to ``"checkpoint_payload_corrupt"``.
    """
    closed = _SERVICE_FALLBACK_REASONS.get(reason, "checkpoint_payload_corrupt")
    return _fallback(closed)


def _version_mismatch(
    certificate: PnlOpeningCheckpointCertificate,
    eligibility: PnlCheckpointResumeEligibility,
) -> bool:
    """Return whether the certificate's version triple or schema differs."""
    try:
        permitted = pnl_engine_options_for(eligibility.calc_version).capture_permitted
    except ValueError:
        return True
    return (
        not permitted
        or certificate.key.calc_version != eligibility.calc_version
        or certificate.timeline_calc_version != eligibility.timeline_calc_version
        or certificate.fx_policy_version != eligibility.fx_policy_version
        or certificate.schema_version != 1
    )


def _execution_counts_match(
    certificate: PnlOpeningCheckpointCertificate,
    counts: Mapping[str, tuple[int, int]],
) -> bool:
    """Compare count and maximum sequence over every certified exchange prefix."""
    if set(counts) != set(certificate.cutoff_execution_watermarks):
        return False
    for exchange, watermark in certificate.cutoff_execution_watermarks.items():
        count, maximum = counts[exchange]
        if count != certificate.cutoff_execution_counts[exchange]:
            return False
        if maximum != watermark:
            return False
    return True


def _oriented_pairs(pairs: Sequence[tuple[str, str]]) -> list[tuple[str, str]]:
    """Expand certified unordered pairs into both publication orientations."""
    return sorted({orientation for pair in pairs for orientation in (pair, pair[::-1])})


def _bundle_candle_aggregates(
    bundle: PnlCheckpointStateBundle,
) -> dict[PnlFxRatePlane, IdAggregate]:
    """Convert the envelope's strict per-plane payloads into domain aggregates."""
    return {
        (entry.base, entry.quote, entry.exchange): IdAggregate(
            count=entry.aggregate.count,
            id_sum=int(entry.aggregate.id_sum),
            max_id=entry.aggregate.max_id,
            max_timestamp=entry.aggregate.max_timestamp,
        )
        for entry in bundle.event_fx_candles_by_plane
    }


async def _execution_chain_matches(
    repo: Repository,
    scope: PortfolioPnlSampleScope,
    certificate: PnlOpeningCheckpointCertificate,
) -> bool:
    """Recompute every exchange's raw execution chain tip at W_C and compare to the certificate.

    Performance note: folds from genesis over ``(0, W_C]`` at every resume,
    which is an O(prefix) read. PR-C2's benchmark must measure this cost.

    Args:
        repo: Repository providing the raw execution chain fold.
        scope: Resuming anchor-bound sample scope.
        certificate: Persisted checkpoint certificate carrying expected tips.

    Returns:
        ``True`` when every exchange's recomputed tip matches the certificate.
    """
    try:
        for exchange, watermark in sorted(certificate.cutoff_execution_watermarks.items()):
            tip = await repo.get_spot_execution_chain_tip(
                scope.wallet_public_id,
                exchange,
                scope.mode,
                0,
                execution_chain_genesis(scope.wallet_public_id, exchange, scope.mode),
                watermark,
            )
            if tip != certificate.execution_chain_digests[exchange]:
                return False
    except ExecutionChainError:
        return False
    return True


def _manifest_matches(
    certificate: PnlOpeningCheckpointCertificate,
    prefix: PnlTimelineExecutionPrefix,
    expected_manifest_digest: str | None,
) -> bool:
    """Require frozen generation manifest digest to match both live frontier and live prefix."""
    live_manifest = execution_manifest_digest(prefix["annulments"])
    if certificate.execution_manifest_digest != live_manifest:
        return False
    return (
        expected_manifest_digest is None
        or certificate.execution_manifest_digest == expected_manifest_digest
    )


def _effective_times_match(
    certificate: PnlOpeningCheckpointCertificate,
    bundle: PnlCheckpointStateBundle,
    prefix: PnlTimelineExecutionPrefix,
) -> bool:
    """Verify per-pool clamped effective times agree with deferred assignments at C."""
    activation_time = bundle.engine_state.activation_time
    if activation_time is None:
        return False
    checkpoint_minute = certificate.checkpoint_minute
    deferred_set = {
        (exchange, seq)
        for exchange, sequences in certificate.deferred_execution_sequences.items()
        for seq in sequences
    }
    last_effective: dict[tuple[str, str], datetime] = {}
    ordered = sorted(
        prefix["executions"],
        key=lambda row: (row["exchange"], row["scope_sequence"]),
    )
    for row in ordered:
        is_deferred = (row["exchange"], row["scope_sequence"]) in deferred_set
        event_time = row["timestamp"]
        if event_time <= activation_time:
            if is_deferred:
                return False
            continue
        pool_key = (row["instrument_public_id"], row["shard_key"])
        effective_time = max(last_effective.get(pool_key, event_time), event_time)
        last_effective[pool_key] = effective_time
        if is_deferred != (effective_time > checkpoint_minute):
            return False
    return True


def _prefix_assignments_match(
    certificate: PnlOpeningCheckpointCertificate,
    bundle: PnlCheckpointStateBundle,
    prefix: PnlTimelineExecutionPrefix,
) -> bool:
    """Verify applied watermarks and deferred sequences against live prefix executions."""
    deferred_keys = tuple(
        (item.exchange, item.scope_sequence) for item in bundle.engine_state.deferred_execution_keys
    )
    live_applied, live_deferred = partition_execution_watermarks(
        prefix["executions"],
        deferred_keys,
    )
    cert_deferred = {
        exchange: tuple(sequences)
        for exchange, sequences in certificate.deferred_execution_sequences.items()
        if sequences
    }
    if (
        live_applied != dict(certificate.applied_watermarks_by_exchange)
        or live_deferred != cert_deferred
    ):
        return False
    return _effective_times_match(certificate, bundle, prefix)


async def _verify_execution_prefix(
    repo: Repository,
    scope: PortfolioPnlSampleScope,
    record: PnlOpeningCheckpointRecord,
    frontier_as_of: datetime,
    expected_manifest_digest: str | None = None,
) -> tuple[PnlCheckpointFallback | None, PnlTimelineExecutionPrefix | None]:
    """Verify counts, chain tips, annulments, manifest, witnesses, and live prefix rows."""
    certificate = record.certificate
    counts = await repo.get_pnl_execution_prefix_counts(
        scope.wallet_public_id,
        scope.mode,
        certificate.cutoff_execution_watermarks,
    )
    if not _execution_counts_match(certificate, counts):
        return _fallback("checkpoint_execution_counts_changed"), None
    if not await _execution_chain_matches(repo, scope, certificate):
        return _fallback("checkpoint_execution_chain_changed"), None
    prefix = await repo.get_pnl_timeline_execution_prefix_at_watermarks(
        scope.wallet_public_id,
        scope.mode,
        certificate.cutoff_execution_watermarks,
        frontier_as_of,
        fill_witness_cut={
            "watermarks": dict(certificate.prefix_venue_event_watermarks),
            "digest": certificate.prefix_fill_witness_digest,
        },
    )
    if (
        prefix_annulment_digest(
            prefix["annulments"],
            certificate.cutoff_execution_watermarks,
        )
        != certificate.prefix_annulment_digest
    ):
        return _fallback("checkpoint_prefix_annulment_changed"), None
    if not _manifest_matches(certificate, prefix, expected_manifest_digest):
        return _fallback("checkpoint_execution_manifest_changed"), None
    witness = prefix.get("fill_witness_cut")
    if (
        witness is None
        or witness["watermarks"] != certificate.prefix_venue_event_watermarks
        or witness["digest"] != certificate.prefix_fill_witness_digest
    ):
        return _fallback("checkpoint_prefix_witness_changed"), None
    return None, prefix


async def _verify_accounting_and_refs(
    repo: Repository,
    scope: PortfolioPnlSampleScope,
    record: PnlOpeningCheckpointRecord,
    bundle: PnlCheckpointStateBundle,
    frontier_as_of: datetime,
) -> PnlCheckpointFallback | None:
    """Verify accrual, lineage, and symbol-reference prefixes in order."""
    certificate = record.certificate
    accrual = await repo.get_pnl_accrual_prefix_fingerprint(
        scope.wallet_public_id,
        scope.mode,
        scope.anchor_point_time,
        certificate.checkpoint_minute,
        frontier_as_of,
    )
    if accrual != certificate.accrual_prefix_fingerprint:
        return _fallback("checkpoint_accrual_prefix_changed")
    lineage = await repo.get_pnl_timeline_execution_lineage(
        bundle.prefix_lineage_order_ids,
        frontier_as_of,
    )
    if prefix_lineage_digest(lineage) != certificate.prefix_lineage_digest:
        return _fallback("checkpoint_lineage_prefix_changed")
    refs = await repo.get_instrument_symbol_refs(bundle.engine_state.seen, frontier_as_of)
    if symbol_ref_digest(refs) != certificate.symbol_ref_digest:
        return _fallback("checkpoint_symbol_refs_changed")
    return None


async def _verify_event_fx(
    repo: Repository,
    record: PnlOpeningCheckpointRecord,
    bundle: PnlCheckpointStateBundle,
    frontier_as_of: datetime,
) -> tuple[PnlCheckpointFallback | None, tuple[PnlFxRatePlane, ...]]:
    """Verify event-FX planes and both per-plane and total candle aggregates."""
    certificate = record.certificate
    pairs = _oriented_pairs(certificate.consumed_fx_pairs)
    opens = certificate.consumed_fx_open_ats
    planes = await repo.get_pnl_fx_rate_exchanges_at(pairs, opens, frontier_as_of)
    if event_fx_exchanges_digest(planes) != certificate.event_fx_exchanges_digest:
        return _fallback("checkpoint_event_fx_exchanges_changed"), ()
    by_plane = await repo.get_pnl_event_fx_candle_fingerprints_by_plane(
        planes,
        opens,
        frontier_as_of,
    )
    total = await repo.get_pnl_event_fx_candle_fingerprint(planes, opens, frontier_as_of)
    if by_plane != _bundle_candle_aggregates(bundle):
        return _fallback("checkpoint_event_fx_candles_changed"), ()
    if total != certificate.event_fx_candles_fingerprint:
        return _fallback("checkpoint_event_fx_candles_changed"), ()
    return None, tuple(planes)


def _from_time_ineligible(from_time: datetime, checkpoint_minute: datetime) -> bool:
    """Return whether ``from_time`` falls at or before ``checkpoint_minute`` after flooring."""
    return (
        from_time.utcoffset() != timedelta(0)
        or from_time.replace(second=0, microsecond=0) <= checkpoint_minute
    )


def _checkpoint_ineligible(
    record: PnlOpeningCheckpointRecord,
    eligibility: PnlCheckpointResumeEligibility,
) -> bool:
    """Return whether mode, policy, or window forbids resume before database work."""
    return (
        eligibility.mode not in ("shadow", "active")
        or not eligibility.pointwise_exact_fx
        or eligibility.replay_generation_tip is None
        or eligibility.metadata_only
        or _from_time_ineligible(eligibility.from_time, record.certificate.checkpoint_minute)
        or record.certificate.checkpoint_minute >= eligibility.frontier_tip
    )


def _initial_fallback(
    record: PnlOpeningCheckpointRecord | None,
    eligibility: PnlCheckpointResumeEligibility,
) -> PnlCheckpointFallback | None:
    """Return absent, ineligible, version, or fill-gap fallback before decoding."""
    if record is None:
        return _fallback("checkpoint_absent")
    if _checkpoint_ineligible(record, eligibility):
        return _fallback("checkpoint_ineligible")
    if _version_mismatch(record.certificate, eligibility):
        return _fallback("checkpoint_version_mismatch")
    if eligibility.has_fill_gap:
        return _fallback("checkpoint_fill_gap")
    return None


def _certificate_matches_scope(
    certificate: PnlOpeningCheckpointCertificate,
    scope: PortfolioPnlSampleScope,
) -> bool:
    """Require certificate scope identity to match the resuming sample scope."""
    return (
        certificate.key.wallet_public_id,
        certificate.key.mode,
        certificate.key.valuation_ccy,
        certificate.key.epoch_public_id,
    ) == (
        scope.wallet_public_id,
        scope.mode,
        scope.valuation_ccy,
        scope.epoch_public_id,
    )


def _bundle_matches_certificate_and_scope(
    certificate: PnlOpeningCheckpointCertificate,
    bundle: PnlCheckpointStateBundle,
    scope: PortfolioPnlSampleScope,
) -> bool:
    """Cross-check embedded bundle identity and engine header against certificate and scope."""
    identity = bundle.identity
    engine = bundle.engine_state
    bundle_deferred = tuple(
        (item.exchange, item.scope_sequence) for item in engine.deferred_execution_keys
    )
    cert_deferred = tuple(
        (exchange, sequence)
        for exchange, sequences in sorted(certificate.deferred_execution_sequences.items())
        for sequence in sequences
    )
    return (
        identity.wallet_public_id,
        identity.mode,
        identity.valuation_ccy,
        identity.epoch_public_id,
        identity.calc_version,
        identity.source_generation,
        engine.checkpoint_minute,
        engine.activation_time,
        engine.valuation_ccy,
        bundle_deferred,
    ) == (
        certificate.key.wallet_public_id,
        certificate.key.mode,
        certificate.key.valuation_ccy,
        certificate.key.epoch_public_id,
        certificate.key.calc_version,
        certificate.source_generation,
        certificate.checkpoint_minute,
        scope.anchor_point_time,
        certificate.key.valuation_ccy,
        cert_deferred,
    )


def _decode_and_bind_bundle(
    record: PnlOpeningCheckpointRecord,
    scope: PortfolioPnlSampleScope,
) -> PnlCheckpointStateBundle | PnlCheckpointFallback:
    """Decode ``state_json`` and verify certificate, bundle, and engine identity binding."""
    certificate = record.certificate
    if not _certificate_matches_scope(certificate, scope):
        return _fallback("checkpoint_payload_corrupt")
    if is_unsupported_bundle_version(record.state_json):
        return _fallback("checkpoint_bundle_version")
    try:
        bundle = bundle_from_json(record.state_json)
    except (ValueError, TypeError, KeyError):
        return _fallback("checkpoint_payload_corrupt")
    if not _bundle_matches_certificate_and_scope(certificate, bundle, scope):
        return _fallback("checkpoint_payload_corrupt")
    return bundle


async def verify_checkpoint_for_resume(
    repo: Repository,
    record: PnlOpeningCheckpointRecord | None,
    scope: PortfolioPnlSampleScope,
    frontier_as_of: datetime,
    eligibility: PnlCheckpointResumeEligibility,
) -> PnlCheckpointResume | PnlCheckpointFallback:
    """Verify eligibility and every prefix-evidence domain before suffix resume.

    Args:
        repo: Repository providing prefix evidence at the resuming horizon.
        record: Persisted checkpoint record, or ``None`` when absent.
        scope: Anchor-bound sample scope.
        frontier_as_of: Resuming generation's knowledge horizon.
        eligibility: Exact policy, version, anchor, manifest, and window facts.

    Returns:
        Verified decoded resume state or a stable fallback with delete policy.
    """
    try:
        initial = _initial_fallback(record, eligibility)
        if initial is not None:
            return initial
        assert record is not None
        return await _verify_checkpoint_domains(
            repo,
            record,
            scope,
            frontier_as_of,
            eligibility,
        )
    except asyncio.CancelledError:
        raise
    except Exception as exc:
        logger.warning(
            "Opening checkpoint verification raised unexpectedly; falling back (%s)",
            type(exc).__name__,
        )
        return _fallback("checkpoint_verify_error")


async def _verify_checkpoint_domains(
    repo: Repository,
    record: PnlOpeningCheckpointRecord,
    scope: PortfolioPnlSampleScope,
    frontier_as_of: datetime,
    eligibility: PnlCheckpointResumeEligibility,
) -> PnlCheckpointResume | PnlCheckpointFallback:
    """Verify anchor, prefix executions, live assignments, accounting, and event FX."""
    certificate = record.certificate
    decoded = _decode_and_bind_bundle(record, scope)
    if isinstance(decoded, PnlCheckpointFallback):
        return decoded
    bundle = decoded
    if (
        certificate.anchor_public_id != eligibility.expected_anchor_public_id
        or certificate.anchor_digest != eligibility.expected_anchor_digest
    ):
        return _fallback("checkpoint_anchor_mismatch")
    execution, prefix = await _verify_execution_prefix(
        repo,
        scope,
        record,
        frontier_as_of,
        eligibility.expected_execution_manifest_digest,
    )
    if execution is not None:
        return execution
    assert prefix is not None
    if not _prefix_assignments_match(certificate, bundle, prefix):
        return _fallback("checkpoint_prefix_assignments_changed")
    accounting = await _verify_accounting_and_refs(
        repo,
        scope,
        record,
        bundle,
        frontier_as_of,
    )
    if accounting is not None:
        return accounting
    event_fx, fx_planes = await _verify_event_fx(repo, record, bundle, frontier_as_of)
    if event_fx is not None:
        return event_fx
    return PnlCheckpointResume(
        record=record,
        bundle=bundle,
        prefix_annulments=tuple(prefix["annulments"]),
        verified_fx_planes=fx_planes,
        verified_at=frontier_as_of,
    )
