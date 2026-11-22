"""Ordered verification of persisted opening checkpoints before suffix resume."""

from collections.abc import Mapping
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime
from typing import Literal

from snapper.application.portfolio.execution_chain import ExecutionChainError
from snapper.application.portfolio.execution_chain import execution_chain_genesis
from snapper.application.portfolio.pnl_engine_options import pnl_engine_options_for
from snapper.application.portfolio.pnl_opening_checkpoint_bundle import PnlCheckpointStateBundle
from snapper.application.portfolio.pnl_opening_checkpoint_bundle import bundle_from_json
from snapper.application.portfolio.pnl_opening_checkpoint_certificate import (
    event_fx_exchanges_digest,
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

PnlCheckpointFallbackReason = Literal[
    "checkpoint_absent",
    "checkpoint_ineligible",
    "checkpoint_version_mismatch",
    "checkpoint_fill_gap",
    "checkpoint_payload_corrupt",
    "checkpoint_anchor_mismatch",
    "checkpoint_execution_counts_changed",
    "checkpoint_execution_chain_changed",
    "checkpoint_prefix_annulment_changed",
    "checkpoint_prefix_witness_changed",
    "checkpoint_accrual_prefix_changed",
    "checkpoint_lineage_prefix_changed",
    "checkpoint_symbol_refs_changed",
    "checkpoint_event_fx_exchanges_changed",
    "checkpoint_event_fx_candles_changed",
    "checkpoint_late_execution",
]
"""Closed fallback vocabulary for checkpoint resume refusal."""

_DELETE_REASONS: frozenset[str] = frozenset(
    {
        "checkpoint_payload_corrupt",
        "checkpoint_anchor_mismatch",
        "checkpoint_execution_counts_changed",
        "checkpoint_execution_chain_changed",
        "checkpoint_prefix_annulment_changed",
        "checkpoint_prefix_witness_changed",
        "checkpoint_accrual_prefix_changed",
        "checkpoint_lineage_prefix_changed",
        "checkpoint_symbol_refs_changed",
        "checkpoint_event_fx_exchanges_changed",
        "checkpoint_event_fx_candles_changed",
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


@dataclass(frozen=True)
class PnlCheckpointFallback:
    """Stable fallback reason and whether the stored row requires fenced deletion."""

    reason: PnlCheckpointFallbackReason
    delete_required: bool


@dataclass(frozen=True)
class PnlCheckpointResumeEligibility:
    """Caller policy, version, anchor, window, and late-execution facts."""

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
    has_late_execution: bool = False


def _fallback(reason: PnlCheckpointFallbackReason) -> PnlCheckpointFallback:
    """Build one fallback with its deletion policy derived from the closed vocabulary."""
    return PnlCheckpointFallback(reason=reason, delete_required=reason in _DELETE_REASONS)


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


async def _verify_execution_prefix(
    repo: Repository,
    scope: PortfolioPnlSampleScope,
    record: PnlOpeningCheckpointRecord,
    frontier_as_of: datetime,
) -> tuple[PnlCheckpointFallback | None, tuple[PnlTimelineAppliedAnnulment, ...]]:
    """Verify counts, chain tips, prefix annulments, and fill-witness membership in order."""
    certificate = record.certificate
    counts = await repo.get_pnl_execution_prefix_counts(
        scope.wallet_public_id,
        scope.mode,
        certificate.cutoff_execution_watermarks,
    )
    if not _execution_counts_match(certificate, counts):
        return _fallback("checkpoint_execution_counts_changed"), ()
    if not await _execution_chain_matches(repo, scope, certificate):
        return _fallback("checkpoint_execution_chain_changed"), ()
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
        return _fallback("checkpoint_prefix_annulment_changed"), ()
    witness = prefix.get("fill_witness_cut")
    if (
        witness is None
        or witness["watermarks"] != certificate.prefix_venue_event_watermarks
        or witness["digest"] != certificate.prefix_fill_witness_digest
    ):
        return _fallback("checkpoint_prefix_witness_changed"), ()
    return None, tuple(prefix["annulments"])


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
) -> PnlCheckpointFallback | None:
    """Verify event-FX planes and both per-plane and total candle aggregates."""
    certificate = record.certificate
    pairs = _oriented_pairs(certificate.consumed_fx_pairs)
    opens = certificate.consumed_fx_open_ats
    planes = await repo.get_pnl_fx_rate_exchanges_at(pairs, opens, frontier_as_of)
    if event_fx_exchanges_digest(planes) != certificate.event_fx_exchanges_digest:
        return _fallback("checkpoint_event_fx_exchanges_changed")
    by_plane = await repo.get_pnl_event_fx_candle_fingerprints_by_plane(
        planes,
        opens,
        frontier_as_of,
    )
    total = await repo.get_pnl_event_fx_candle_fingerprint(planes, opens, frontier_as_of)
    if by_plane != _bundle_candle_aggregates(bundle):
        return _fallback("checkpoint_event_fx_candles_changed")
    if total != certificate.event_fx_candles_fingerprint:
        return _fallback("checkpoint_event_fx_candles_changed")
    return None


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
        or eligibility.from_time <= record.certificate.checkpoint_minute
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
    try:
        bundle = bundle_from_json(record.state_json)
    except ValueError:
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
        eligibility: Exact policy, version, anchor, window, and late-fill facts.

    Returns:
        Verified decoded resume state or a stable fallback with delete policy.
    """
    initial = _initial_fallback(record, eligibility)
    if initial is not None:
        return initial
    assert record is not None
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
    execution, prefix_annulments = await _verify_execution_prefix(
        repo,
        scope,
        record,
        frontier_as_of,
    )
    if execution is not None:
        return execution
    accounting = await _verify_accounting_and_refs(
        repo,
        scope,
        record,
        bundle,
        frontier_as_of,
    )
    if accounting is not None:
        return accounting
    event_fx = await _verify_event_fx(repo, record, bundle, frontier_as_of)
    if event_fx is not None:
        return event_fx
    if eligibility.has_late_execution:
        return _fallback("checkpoint_late_execution")
    return PnlCheckpointResume(
        record=record,
        bundle=bundle,
        prefix_annulments=prefix_annulments,
    )
