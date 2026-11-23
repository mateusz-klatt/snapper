"""Ordered evidence collection and strict assembly for service-side checkpoint capture."""

from collections.abc import Mapping
from collections.abc import Sequence
from dataclasses import dataclass
from dataclasses import field
from datetime import datetime
from datetime import timedelta
from typing import Literal
from typing import Protocol

from sqlalchemy.exc import SQLAlchemyError

from snapper.application.portfolio.execution_chain import ExecutionChainError
from snapper.application.portfolio.execution_chain import execution_chain_genesis
from snapper.application.portfolio.fx_conversion_shadow import POINTWISE_FX_ELECTION_POLICY_VERSION
from snapper.application.portfolio.fx_rates import FxVenueMap
from snapper.application.portfolio.pnl_engine_options import pnl_engine_options_for
from snapper.application.portfolio.pnl_opening_checkpoint import CHECKPOINT_SCHEMA_VERSION
from snapper.application.portfolio.pnl_opening_checkpoint import checkpoint_to_payload
from snapper.application.portfolio.pnl_opening_checkpoint_bundle import BUNDLE_VERSION
from snapper.application.portfolio.pnl_opening_checkpoint_bundle import (
    PnlCheckpointBundleIdentityPayload,
)
from snapper.application.portfolio.pnl_opening_checkpoint_bundle import PnlCheckpointStateBundle
from snapper.application.portfolio.pnl_opening_checkpoint_bundle import PnlEventFxCandlePlanePayload
from snapper.application.portfolio.pnl_opening_checkpoint_bundle import PnlEventFxReplayStatePayload
from snapper.application.portfolio.pnl_opening_checkpoint_bundle import PnlFxRateSourcePayload
from snapper.application.portfolio.pnl_opening_checkpoint_bundle import PnlIdAggregatePayload
from snapper.application.portfolio.pnl_opening_checkpoint_bundle import PnlMarkFxReplayStatePayload
from snapper.application.portfolio.pnl_opening_checkpoint_bundle import PnlPrefixFxCoveragePayload
from snapper.application.portfolio.pnl_opening_checkpoint_bundle import PnlPrefixFxElectionPayload
from snapper.application.portfolio.pnl_opening_checkpoint_bundle import PnlPrefixRefBindingPayload
from snapper.application.portfolio.pnl_opening_checkpoint_bundle import PnlRefBindingPayload
from snapper.application.portfolio.pnl_opening_checkpoint_bundle import bundle_to_json
from snapper.application.portfolio.pnl_opening_checkpoint_certificate import (
    event_fx_exchanges_digest,
)
from snapper.application.portfolio.pnl_opening_checkpoint_certificate import (
    partition_execution_watermarks,
)
from snapper.application.portfolio.pnl_opening_checkpoint_certificate import prefix_annulment_digest
from snapper.application.portfolio.pnl_opening_checkpoint_certificate import prefix_lineage_digest
from snapper.application.portfolio.pnl_opening_checkpoint_certificate import symbol_ref_digest
from snapper.application.portfolio.pnl_timeline import PnlIncompletenessReasonEntry
from snapper.application.portfolio.pnl_timeline import PnlTimelineResult
from snapper.application.portfolio.pnl_timeline import TimelineCheckpointOpening
from snapper.data.pnl_correction_state import IdAggregate
from snapper.data.pnl_correction_state import PnlCorrectionKey
from snapper.data.pnl_correction_state import PnlOpeningCheckpointCertificate
from snapper.data.pnl_correction_state import PnlOpeningCheckpointWrite
from snapper.data.repository import Repository
from snapper.data.repository_types import InstrumentSymbolRefRow
from snapper.data.repository_types import PnlFxRatePlane
from snapper.data.repository_types import PnlTimelineExecutionPrefix
from snapper.data.repository_types import PnlTimelineOpeningExecutionRow


class _EventFxEvidence(Protocol):
    """Structural view of the service's converted event-FX evidence."""

    @property
    def used_planes(self) -> set[tuple[tuple[str, str], PnlFxRatePlane]]:
        """Return the event planes backed by usable positive evidence.

        Returns:
            The set of pair and plane identities with usable evidence.
        """

    def venues_at(self, instrument_public_id: str, minute: datetime) -> FxVenueMap:
        """Return the venue pins for one instrument and exact event minute."""

    def has_usable_rate(self, plane: PnlFxRatePlane, minute: datetime) -> bool:
        """Return whether ``plane`` carries a positive finite close at ``minute``."""


def event_rate_source_payloads(
    requirements: Mapping[str, Mapping[tuple[str, str], set[datetime]]],
    evidence: _EventFxEvidence,
    valuation_ccy: str,
) -> tuple[PnlFxRateSourcePayload, ...]:
    """Return actual event-consumed source planes without mark-only pollution.

    Args:
        requirements: Event-only minutes grouped by instrument and pair.
        evidence: Per-minute venue pins, usable rate check, and used planes.
        valuation_ccy: Target series currency.

    Returns:
        Canonically sorted unique serialized event rate sources.
    """
    sources: dict[tuple[str, str, str, str, str], PnlFxRateSourcePayload] = {}
    for instrument_public_id, pair_requirements in requirements.items():
        for (first, second), minutes in pair_requirements.items():
            for minute in minutes:
                plane = evidence.venues_at(instrument_public_id, minute).get((first, second))
                if (
                    plane is None
                    or ((first, second), plane) not in evidence.used_planes
                    or not evidence.has_usable_rate(plane, minute)
                ):
                    continue
                payload = PnlFxRateSourcePayload(
                    source_currency=second if first == valuation_ccy else first,
                    valuation_currency=valuation_ccy,
                    base_currency=plane[0],
                    quote_currency=plane[1],
                    exchange=plane[2],
                )
                key = (
                    payload.source_currency,
                    payload.valuation_currency,
                    payload.base_currency,
                    payload.quote_currency,
                    payload.exchange,
                )
                sources[key] = payload
    return tuple(sources[key] for key in sorted(sources))


def prefix_fx_election_payloads(
    requirements: Mapping[str, Mapping[tuple[str, str], set[datetime]]],
    evidence: _EventFxEvidence,
) -> tuple[PnlPrefixFxElectionPayload, ...]:
    """Record the exact FX plane elected for every prefix event conversion minute.

    Args:
        requirements: Prefix event conversion minutes grouped by instrument and pair.
        evidence: Elected FX venue pins captured for this replay.

    Returns:
        Canonically sorted per-(instrument, pair, minute) election records.
    """
    return tuple(
        PnlPrefixFxElectionPayload(
            instrument_public_id=instrument_public_id,
            pair=pair,
            minute=minute,
            elected_plane=evidence.venues_at(instrument_public_id, minute).get(pair),
        )
        for instrument_public_id, pair_requirements in sorted(requirements.items())
        for pair, minutes in sorted(pair_requirements.items())
        for minute in sorted(minutes)
    )


def prefix_ref_binding_payloads(
    requirements: Mapping[str, Mapping[tuple[str, str], set[datetime]]],
    reference_spans: Mapping[str, tuple[datetime, datetime]],
    trusted_refs: Sequence[InstrumentSymbolRefRow],
) -> tuple[PnlPrefixRefBindingPayload, ...]:
    """Record exact trusted symbol-reference bindings for prefix-active instruments.

    Args:
        requirements: Prefix event conversion requirements grouped by instrument.
        reference_spans: Prefix execution price-reference spans by instrument.
        trusted_refs: Trusted symbol references resolved at capture time.

    Returns:
        Sorted per-instrument binding snapshots covering proven and unproven instruments.
    """
    trusted_by_instrument = {ref["instrument_public_id"]: ref for ref in trusted_refs}
    instrument_ids = sorted(set(requirements) | set(reference_spans))
    return tuple(
        PnlPrefixRefBindingPayload(
            instrument_public_id=instrument_public_id,
            binding=(
                PnlRefBindingPayload(
                    native_symbol=ref["native_symbol"],
                    exchange=ref["exchange"],
                    instrument_exchange=ref["instrument_exchange"],
                    base_currency=ref["base_currency"],
                    quote_currency=ref["quote_currency"],
                )
                if (ref := trusted_by_instrument.get(instrument_public_id)) is not None
                and ref["quote_currency"] is not None
                else None
            ),
        )
        for instrument_public_id in instrument_ids
    )


def prefix_fx_coverage_payloads(
    coverage_by_plane: Mapping[PnlFxRatePlane, set[datetime]],
) -> tuple[PnlPrefixFxCoveragePayload, ...]:
    """Encode usable per-plane FX coverage minutes observed at capture.

    Args:
        coverage_by_plane: Usable published minutes keyed by oriented FX plane.

    Returns:
        Canonically sorted coverage entries excluding empty minute sets.
    """
    return tuple(
        PnlPrefixFxCoveragePayload(
            base=plane[0],
            quote=plane[1],
            exchange=plane[2],
            minutes=tuple(sorted(minutes)),
        )
        for plane, minutes in sorted(coverage_by_plane.items())
        if minutes
    )


type PnlCheckpointCaptureRefusal = Literal[
    "self_heal_open",
    "capture_value_error",
    "capture_evidence_error",
    "capture_not_permitted",
    "capture_not_at_tip",
    "capture_execution_chain_error",
    "capture_write_failed",
    "capture_upsert_failed",
    "capture_recapture_budget_exceeded",
    "global_withhold",
    "engine_checkpoint_missing",
    "carried_event_inputs",
    "carried_inputs_on_processed_event",
    "conversion_uses_mark_evidence",
]
"""Closed refusal vocabulary emitted by service-side checkpoint capture."""


@dataclass(slots=True)
class _CaptureEvidence:
    """Mutable evidence collected for one single-use capture request."""

    active: bool = False
    refusal: PnlCheckpointCaptureRefusal | None = None
    prefix: PnlTimelineExecutionPrefix | None = None
    execution_counts: dict[str, tuple[int, int]] | None = None
    execution_chain_digests: dict[str, str] | None = None
    prefix_annulment_digest: str | None = None
    accrual_prefix_fingerprint: IdAggregate | None = None
    lineage_digest: str | None = None
    lineage_order_ids: tuple[str, ...] = ()
    symbol_ref_seen: frozenset[str] | None = None
    symbol_refs: tuple[InstrumentSymbolRefRow, ...] | None = None
    consumed_fx_pairs: tuple[tuple[str, str], ...] = ()
    consumed_fx_open_ats: tuple[datetime, ...] = ()
    event_fx_exchanges_digest: str | None = None
    event_fx_candles_by_plane: dict[PnlFxRatePlane, IdAggregate] | None = None
    event_fx_candles_fingerprint: IdAggregate | None = None
    event_walk_state: PnlEventFxReplayStatePayload | None = None
    mark_walk_state: PnlMarkFxReplayStatePayload | None = None


@dataclass(frozen=True)
class _AccrualPrefixRequest:
    """Scope and exact temporal bounds for one accrual-prefix fingerprint."""

    wallet_public_id: str
    mode: str
    after_exclusive: datetime
    through: datetime
    as_of: datetime


@dataclass(frozen=True)
class PnlCheckpointCaptureAssembly:
    """Final engine and service state needed to assemble one certified write."""

    timeline_calc_version: str
    checkpoint_minute: datetime
    as_of: datetime
    engine_state: TimelineCheckpointOpening
    price_ref_spans_by_instrument: Mapping[str, tuple[datetime, datetime]]
    display_activity_spans_by_instrument: Mapping[str, tuple[datetime, datetime]]
    rate_sources_prefix: tuple[PnlFxRateSourcePayload, ...]
    prefix_fx_elections: tuple[PnlPrefixFxElectionPayload, ...]
    prefix_ref_bindings: tuple[PnlPrefixRefBindingPayload, ...]
    prefix_fx_coverage: tuple[PnlPrefixFxCoveragePayload, ...]


@dataclass(frozen=True)
class PnlCheckpointCaptureRequest:
    """Frozen source identity and mutable evidence for one service capture attempt."""

    key: PnlCorrectionKey
    source_generation: int
    raw_execution_watermarks: Mapping[str, int]
    execution_manifest_digest: str
    expected_venues: frozenset[str]
    venue_event_watermarks: Mapping[str, int]
    fill_witness_digest: str
    anchor_public_id: str
    anchor_digest: str
    calc_version: str
    _evidence: _CaptureEvidence = field(init=False, default_factory=_CaptureEvidence, repr=False)

    def __post_init__(self) -> None:
        """Require the request calc_version to agree with the correction key.

        Raises:
            ValueError: When ``calc_version`` disagrees with ``key.calc_version``.
        """
        if self.calc_version != self.key.calc_version:
            raise ValueError("PnlCheckpointCaptureRequest calc_version must match key.calc_version")

    @property
    def active(self) -> bool:
        """Return whether this request passed capture permission and tip gates.

        Returns:
            ``True`` only when capture is permitted and C equals the generation tip.
        """
        return self._evidence.active

    @property
    def refusal(self) -> PnlCheckpointCaptureRefusal | None:
        """Return the current soft refusal, if any.

        Returns:
            The stable capture refusal reason, or ``None`` while capture remains viable.
        """
        return self._evidence.refusal

    def activate(
        self,
        to_time: datetime,
        replay_generation_tip: datetime | None,
    ) -> PnlCheckpointCaptureRefusal | None:
        """Resolve capture permission and the exact settling-tip requirement.

        Args:
            to_time: Requested checkpoint minute C.
            replay_generation_tip: Finalized generation tip supplied by replay options.

        Returns:
            A stable soft-refusal reason, or ``None`` when capture may proceed.
        """
        object.__setattr__(self, "_evidence", _CaptureEvidence())
        try:
            permitted = pnl_engine_options_for(self.calc_version).capture_permitted
        except ValueError:
            return self._refuse("capture_value_error")
        if not permitted:
            return self._refuse("capture_not_permitted")
        if replay_generation_tip != to_time:
            return self._refuse("capture_not_at_tip")
        self._evidence.active = True
        return None

    def _refuse(self, reason: PnlCheckpointCaptureRefusal) -> PnlCheckpointCaptureRefusal:
        """Stop capture and retain one stable refusal reason."""
        self._evidence.active = False
        self._evidence.refusal = reason
        return reason

    def refuse_value_error(self) -> None:
        """Convert a capture-only conversion or encoding failure to a soft refusal."""
        self._refuse("capture_value_error")

    def refuse_read_policy(self) -> None:
        """Refuse capture when the series read policy is not pointwise without carry."""
        self._refuse("capture_not_permitted")

    async def capture_execution_prefix(
        self,
        repo: Repository,
        wallet_public_id: str,
        mode: str,
        as_of: datetime,
    ) -> None:
        """Capture execution counts, chain tips, annulments, and fill witnesses first.

        Args:
            repo: Repository providing certificate and replay evidence.
            wallet_public_id: Wallet scope.
            mode: Trading mode.
            as_of: Generation knowledge horizon.
        """
        if not self.active:
            return
        try:
            watermarks = dict(sorted(self.raw_execution_watermarks.items()))
            self._evidence.execution_counts = await repo.get_pnl_execution_prefix_counts(
                wallet_public_id,
                mode,
                watermarks,
            )
            self._evidence.execution_chain_digests = {
                exchange: await repo.get_spot_execution_chain_tip(
                    wallet_public_id,
                    exchange,
                    mode,
                    0,
                    execution_chain_genesis(wallet_public_id, exchange, mode),
                    watermark,
                )
                for exchange, watermark in sorted(watermarks.items())
            }
            prefix = await repo.get_pnl_timeline_execution_prefix_at_watermarks(
                wallet_public_id,
                mode,
                watermarks,
                as_of,
                fill_witness_cut="capture",
            )
            if prefix.get("fill_witness_cut") is None:
                raise ValueError("captured execution prefix omitted its fill witness cut")
            self._evidence.prefix = prefix
            self._evidence.prefix_annulment_digest = prefix_annulment_digest(
                prefix["annulments"],
                watermarks,
            )
        except ExecutionChainError:
            self._refuse("capture_execution_chain_error")
        except ValueError:
            self._refuse("capture_value_error")
        except SQLAlchemyError:
            self._refuse("capture_evidence_error")

    async def capture_accrual_prefix(
        self,
        repo: Repository,
        request: _AccrualPrefixRequest,
    ) -> None:
        """Capture the accrual aggregate immediately before the accrual replay read.

        Args:
            repo: Repository providing prefix fingerprints.
            request: Scope and exact accrual-prefix bounds.
        """
        if not self.active:
            return
        try:
            self._evidence.accrual_prefix_fingerprint = (
                await repo.get_pnl_accrual_prefix_fingerprint(
                    request.wallet_public_id,
                    request.mode,
                    request.after_exclusive,
                    request.through,
                    request.as_of,
                )
            )
        except ValueError:
            self._refuse("capture_value_error")
        except SQLAlchemyError:
            self._refuse("capture_evidence_error")

    async def capture_prefix_lineage(
        self,
        repo: Repository,
        order_public_ids: Sequence[str],
        as_of: datetime,
    ) -> None:
        """Capture lineage rows and their digest before replay reads the same domain.

        Args:
            repo: Repository providing execution lineage.
            order_public_ids: Prefix execution order identifiers.
            as_of: Generation knowledge horizon.
        """
        if not self.active:
            return
        try:
            order_ids = tuple(sorted(set(order_public_ids)))
            rows = await repo.get_pnl_timeline_execution_lineage(order_ids, as_of)
            self._evidence.lineage_order_ids = order_ids
            self._evidence.lineage_digest = prefix_lineage_digest(rows)
        except ValueError:
            self._refuse("capture_value_error")
        except SQLAlchemyError:
            self._refuse("capture_evidence_error")

    async def capture_symbol_refs(
        self,
        repo: Repository,
        seen: Sequence[str],
        as_of: datetime,
    ) -> None:
        """Capture symbol references for engine-seen instruments before replay reads them.

        Args:
            repo: Repository providing instrument symbol references.
            seen: Engine-seen instrument identifiers.
            as_of: Generation knowledge horizon.
        """
        if not self.active:
            return
        try:
            ordered_seen = tuple(sorted(set(seen)))
            refs = await repo.get_instrument_symbol_refs(ordered_seen, as_of)
            self._evidence.symbol_ref_seen = frozenset(ordered_seen)
            self._evidence.symbol_refs = tuple(refs)
        except ValueError:
            self._refuse("capture_value_error")
        except SQLAlchemyError:
            self._refuse("capture_evidence_error")

    async def capture_event_fx(
        self,
        repo: Repository,
        event_requirements: Mapping[str, Mapping[tuple[str, str], set[datetime]]],
        as_of: datetime,
    ) -> None:
        """Capture event-only FX planes and exact candle aggregates before replay FX reads.

        Args:
            repo: Repository providing event-FX fingerprints.
            event_requirements: Event-walk minutes grouped by instrument and pair.
            as_of: Generation knowledge horizon.
        """
        if not self.active:
            return
        try:
            pairs = tuple(
                sorted(
                    {
                        pair
                        for pair_requirements in event_requirements.values()
                        for pair in pair_requirements
                    }
                )
            )
            open_ats = tuple(
                sorted(
                    {
                        minute - timedelta(minutes=1)
                        for pair_requirements in event_requirements.values()
                        for minutes in pair_requirements.values()
                        for minute in minutes
                    }
                )
            )
            oriented_pairs = sorted(
                {orientation for pair in pairs for orientation in (pair, pair[::-1])}
            )
            planes = await repo.get_pnl_fx_rate_exchanges_at(oriented_pairs, open_ats, as_of)
            self._evidence.consumed_fx_pairs = pairs
            self._evidence.consumed_fx_open_ats = open_ats
            self._evidence.event_fx_exchanges_digest = event_fx_exchanges_digest(planes)
            self._evidence.event_fx_candles_by_plane = (
                await repo.get_pnl_event_fx_candle_fingerprints_by_plane(planes, open_ats, as_of)
            )
            self._evidence.event_fx_candles_fingerprint = (
                await repo.get_pnl_event_fx_candle_fingerprint(planes, open_ats, as_of)
            )
        except ValueError:
            self._refuse("capture_value_error")
        except SQLAlchemyError:
            self._refuse("capture_evidence_error")

    def set_walk_states(
        self,
        event_walk_state: PnlEventFxReplayStatePayload,
        mark_walk_state: PnlMarkFxReplayStatePayload,
    ) -> None:
        """Retain the complete real walk states for final envelope assembly.

        Args:
            event_walk_state: Event requirement walk state at C.
            mark_walk_state: Mark requirement walk state and shadows at C.
        """
        if self.active:
            self._evidence.event_walk_state = event_walk_state
            self._evidence.mark_walk_state = mark_walk_state

    def assemble(self, assembly: PnlCheckpointCaptureAssembly) -> PnlOpeningCheckpointWrite:
        """Build the certificate and versioned envelope from captured evidence.

        Args:
            assembly: Final engine state and service-side replay payloads.

        Returns:
            A validated opening-checkpoint write ready for the frontier fence.

        Raises:
            ValueError: When captured evidence or final state violates its contract.
        """
        certificate = self._certificate(assembly)
        event_walk_state = self._require_evidence(
            self._evidence.event_walk_state,
            "event_walk_state",
        )
        mark_walk_state = self._require_evidence(
            self._evidence.mark_walk_state,
            "mark_walk_state",
        )
        by_plane = self._require_evidence(
            self._evidence.event_fx_candles_by_plane,
            "event_fx_candles_by_plane",
        )
        bundle = PnlCheckpointStateBundle(
            bundle_version=BUNDLE_VERSION,
            identity=PnlCheckpointBundleIdentityPayload(
                wallet_public_id=self.key.wallet_public_id,
                mode=self.key.mode,
                valuation_ccy=self.key.valuation_ccy,
                epoch_public_id=self.key.epoch_public_id,
                calc_version=self.key.calc_version,
                source_generation=self.source_generation,
            ),
            engine_state=checkpoint_to_payload(assembly.engine_state),
            price_ref_spans_by_instrument=dict(assembly.price_ref_spans_by_instrument),
            display_activity_spans_by_instrument=dict(
                assembly.display_activity_spans_by_instrument
            ),
            rate_sources_prefix=assembly.rate_sources_prefix,
            prefix_lineage_order_ids=self._evidence.lineage_order_ids,
            event_walk_state=event_walk_state,
            mark_walk_state=mark_walk_state,
            event_fx_candles_by_plane=tuple(
                PnlEventFxCandlePlanePayload(
                    base=plane[0],
                    quote=plane[1],
                    exchange=plane[2],
                    aggregate=_id_aggregate_payload(aggregate),
                )
                for plane, aggregate in sorted(by_plane.items())
            ),
            prefix_fx_elections=assembly.prefix_fx_elections,
            prefix_ref_bindings=assembly.prefix_ref_bindings,
            prefix_fx_coverage=assembly.prefix_fx_coverage,
        )
        return PnlOpeningCheckpointWrite(certificate=certificate, state_json=bundle_to_json(bundle))

    def _certificate(
        self, assembly: PnlCheckpointCaptureAssembly
    ) -> PnlOpeningCheckpointCertificate:
        """Construct the strict certificate from the ordered evidence reads."""
        prefix = self._require_evidence(self._evidence.prefix, "prefix")
        counts = self._require_evidence(self._evidence.execution_counts, "execution_counts")
        chain_digests = self._require_evidence(
            self._evidence.execution_chain_digests,
            "execution_chain_digests",
        )
        witness = self._require_evidence(prefix.get("fill_witness_cut"), "fill_witness_cut")
        applied, deferred = _execution_watermark_partitions(prefix, assembly.engine_state)
        return PnlOpeningCheckpointCertificate(
            key=self.key,
            timeline_calc_version=assembly.timeline_calc_version,
            fx_policy_version=POINTWISE_FX_ELECTION_POLICY_VERSION,
            schema_version=CHECKPOINT_SCHEMA_VERSION,
            anchor_public_id=self.anchor_public_id,
            anchor_digest=self.anchor_digest,
            source_generation=self.source_generation,
            checkpoint_minute=assembly.checkpoint_minute,
            as_of=assembly.as_of,
            cutoff_execution_watermarks=self.raw_execution_watermarks,
            execution_chain_digests=chain_digests,
            cutoff_execution_counts={exchange: pair[0] for exchange, pair in counts.items()},
            applied_watermarks_by_exchange=applied,
            deferred_execution_sequences=deferred,
            prefix_has_idless_executions=any(
                not (row["exec_id"] or "").strip(" \t\r\n\x0b\x0c")
                and not (row["trade_id"] or "").strip(" \t\r\n\x0b\x0c")
                for row in prefix["executions"]
            ),
            execution_manifest_digest=self.execution_manifest_digest,
            prefix_annulment_digest=self._require_evidence(
                self._evidence.prefix_annulment_digest,
                "prefix_annulment_digest",
            ),
            accrual_prefix_fingerprint=self._require_evidence(
                self._evidence.accrual_prefix_fingerprint,
                "accrual_prefix_fingerprint",
            ),
            prefix_lineage_digest=self._require_evidence(
                self._evidence.lineage_digest,
                "prefix_lineage_digest",
            ),
            prefix_fill_witness_digest=witness["digest"],
            prefix_venue_event_watermarks=witness["watermarks"],
            source_expected_venues=self.expected_venues,
            source_venue_event_watermarks=self.venue_event_watermarks,
            source_fill_witness_digest=self.fill_witness_digest,
            symbol_ref_digest=self._symbol_ref_digest(assembly.engine_state.seen),
            consumed_fx_pairs=self._evidence.consumed_fx_pairs,
            consumed_fx_open_ats=self._evidence.consumed_fx_open_ats,
            event_fx_exchanges_digest=self._require_evidence(
                self._evidence.event_fx_exchanges_digest,
                "event_fx_exchanges_digest",
            ),
            event_fx_candles_fingerprint=self._require_evidence(
                self._evidence.event_fx_candles_fingerprint,
                "event_fx_candles_fingerprint",
            ),
        )

    def _symbol_ref_digest(self, seen: Sequence[str]) -> str:
        """Compute ``symbol_ref_digest`` over exactly the engine-seen instruments."""
        captured_seen = self._require_evidence(self._evidence.symbol_ref_seen, "symbol_ref_seen")
        captured_refs = self._require_evidence(self._evidence.symbol_refs, "symbol_refs")
        engine_seen = frozenset(seen)
        if not engine_seen.issubset(captured_seen):
            raise ValueError("capture symbol references omitted an engine-seen instrument")
        return symbol_ref_digest(
            [ref for ref in captured_refs if ref["instrument_public_id"] in engine_seen]
        )

    @staticmethod
    def _require_evidence[ValueT](value: ValueT | None, label: str) -> ValueT:
        """Require one captured value without allowing ``None`` into assembly."""
        if value is None:
            raise ValueError(f"capture evidence is missing: {label}")
        return value


@dataclass(frozen=True)
class PnlCheckpointCaptureFinalization:
    """Completed valuation and service evidence needed for final capture assembly."""

    request: PnlCheckpointCaptureRequest | None
    result: PnlTimelineResult
    global_reasons: Sequence[PnlIncompletenessReasonEntry]
    event_requirements: Mapping[str, Mapping[tuple[str, str], set[datetime]]]
    fx_evidence: _EventFxEvidence
    valuation_ccy: str
    reference_execution_rows: Sequence[PnlTimelineOpeningExecutionRow]
    display_activity_spans: Mapping[str, tuple[datetime, datetime]]
    checkpoint_minute: datetime
    as_of: datetime
    timeline_calc_version: str
    trusted_refs: Sequence[InstrumentSymbolRefRow] = ()
    coverage_by_plane: Mapping[PnlFxRatePlane, set[datetime]] = field(default_factory=dict)


def capture_checkpoint_write(
    final: PnlCheckpointCaptureFinalization,
) -> tuple[PnlOpeningCheckpointWrite | None, PnlCheckpointCaptureRefusal | None]:
    """Assemble a capture write or convert every expected failure to a refusal.

    Args:
        final: Completed valuation and all final service-side capture inputs.

    Returns:
        A validated write and no refusal, or no write and its stable reason.
    """
    request = final.request
    if request is None:
        return None, None
    if not request.active:
        return None, request.refusal
    if final.global_reasons:
        return None, "global_withhold"
    engine_state = final.result.closing_checkpoint
    if engine_state is None:
        return None, final.result.closing_checkpoint_refusal or "engine_checkpoint_missing"
    reference_spans = price_ref_spans(final.reference_execution_rows)
    try:
        write = request.assemble(
            PnlCheckpointCaptureAssembly(
                timeline_calc_version=final.timeline_calc_version,
                checkpoint_minute=final.checkpoint_minute,
                as_of=final.as_of,
                engine_state=engine_state,
                price_ref_spans_by_instrument=reference_spans,
                display_activity_spans_by_instrument=final.display_activity_spans,
                rate_sources_prefix=event_rate_source_payloads(
                    final.event_requirements,
                    final.fx_evidence,
                    final.valuation_ccy,
                ),
                prefix_fx_elections=prefix_fx_election_payloads(
                    final.event_requirements,
                    final.fx_evidence,
                ),
                prefix_ref_bindings=prefix_ref_binding_payloads(
                    final.event_requirements,
                    reference_spans,
                    final.trusted_refs,
                ),
                prefix_fx_coverage=prefix_fx_coverage_payloads(final.coverage_by_plane),
            )
        )
    except (ValueError, TypeError, KeyError):
        return None, "capture_value_error"
    return write, None


def price_ref_spans(
    execution_rows: Sequence[PnlTimelineOpeningExecutionRow],
) -> dict[str, tuple[datetime, datetime]]:
    """Return exact event-time spans used by execution price-reference proofs.

    Args:
        execution_rows: Reference executions through the checkpoint boundary.

    Returns:
        Minimum and maximum event timestamp by instrument.
    """
    spans: dict[str, tuple[datetime, datetime]] = {}
    for row in execution_rows:
        instrument_public_id = row["instrument_public_id"]
        event_time = row["timestamp"]
        existing = spans.get(instrument_public_id)
        spans[instrument_public_id] = (
            (event_time, event_time)
            if existing is None
            else (min(existing[0], event_time), max(existing[1], event_time))
        )
    return spans


def _id_aggregate_payload(aggregate: IdAggregate) -> PnlIdAggregatePayload:
    """Convert one exact domain aggregate into its strict envelope payload.

    Args:
        aggregate: Repository row-identity aggregate.

    Returns:
        Strict payload with an exact decimal identity sum.
    """
    return PnlIdAggregatePayload(
        count=aggregate.count,
        id_sum=str(aggregate.id_sum),
        max_id=aggregate.max_id,
        max_timestamp=aggregate.max_timestamp,
    )


def _execution_watermark_partitions(
    prefix: PnlTimelineExecutionPrefix,
    engine_state: TimelineCheckpointOpening,
) -> tuple[dict[str, int], dict[str, tuple[int, ...]]]:
    """Partition applied and deferred prefix sequences using the engine's own keys.

    Args:
        prefix: Captured effective execution prefix through W_C.
        engine_state: Captured engine opening carrying deferred execution keys.

    Returns:
        Applied maximum watermarks and sorted deferred sequences by exchange.
    """
    return partition_execution_watermarks(
        prefix["executions"],
        engine_state.deferred_execution_keys,
    )
