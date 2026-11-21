"""Explicit, versioned continuation state for bounded P&L correction transactions.

These contracts do not enable the snapshotter. A checkpoint is independent of
sample existence: a legitimately withheld or unchanged chunk can advance it
without manufacturing a financial row. Repository transactions must fence every
transition with the checkpoint key, generation and revision.
"""

import math
from collections.abc import Mapping
from collections.abc import Sequence
from dataclasses import dataclass
from dataclasses import replace
from datetime import UTC
from datetime import datetime
from datetime import timedelta
from typing import Final

from snapper.data.repository_types import PortfolioPnlSampleRow

MAX_CORRECTION_CHUNK_MINUTES = 1440
_MINUTE = timedelta(minutes=1)
_HEX_DIGITS: Final[frozenset[str]] = frozenset("0123456789abcdef")


class PortfolioPnlCorrectionConflictError(RuntimeError):
    """A checkpoint or financial-row CAS lost and the whole chunk must roll back."""


@dataclass(frozen=True)
class PnlCorrectionKey:
    """An exact correction identity, including the anchor epoch and algorithm."""

    wallet_public_id: str
    mode: str
    valuation_ccy: str
    epoch_public_id: str
    calc_version: str


@dataclass(frozen=True)
class PnlCorrectionFrontier:
    """Durable progress and causal state for one correction generation.

    ``baseline_watermarks`` remains unchanged until the generation is reconciled.
    ``pending_watermarks`` is the evidence prefix being reconciled. A null next
    minute means scan completion; ``repair_from`` retains any unresolved work.
    ``prior_peak`` is the causal peak strictly before the next admitted minute.
    ``raw_execution_watermarks`` freezes the physical execution prefix for every
    chunk. A null cut identifies legacy checkpoints. An empty map freezes an
    empty execution prefix. ``execution_manifest_digest`` pins the authority
    manifest for that cut across restarts. ``has_fill_gap`` pins the scope's
    gap verdict at the same evidence capture. ``expected_venues`` fixes the
    spot venues used for every balance basket in the generation.
    ``venue_event_watermarks`` bounds the physical fill-witness candidates and
    ``fill_witness_digest`` detects changes inside that bound.
    """

    key: PnlCorrectionKey
    generation: int
    revision: int
    window_start: datetime
    next_minute: datetime | None
    tip: datetime
    as_of: datetime
    baseline_watermarks: Mapping[str, int]
    pending_watermarks: Mapping[str, int]
    prior_peak: float | None
    repair_from: datetime | None
    raw_execution_watermarks: Mapping[str, int] | None = None
    execution_manifest_digest: str | None = None
    has_fill_gap: bool | None = None
    expected_venues: frozenset[str] | None = None
    venue_event_watermarks: Mapping[str, int] | None = None
    fill_witness_digest: str | None = None


@dataclass(frozen=True)
class PnlCorrectionRequest:
    """An explicit invalidation, with evidence and a peak before its first minute.

    The initial baseline is used only when this exact scope has no checkpoint.
    Later invalidations retain its durable baseline, including at completion.
    The raw execution cut is separate from the effective replay watermarks,
    which can exclude annulled executions. Upstream invalidation discovery
    and deduplication remain the caller's job.
    """

    start: datetime
    tip: datetime
    as_of: datetime
    baseline_watermarks: Mapping[str, int]
    pending_watermarks: Mapping[str, int]
    prior_peak: float | None
    raw_execution_watermarks: Mapping[str, int] | None = None
    execution_manifest_digest: str | None = None
    has_fill_gap: bool | None = None
    expected_venues: frozenset[str] | None = None
    venue_event_watermarks: Mapping[str, int] | None = None
    fill_witness_digest: str | None = None


@dataclass(frozen=True)
class PnlCorrectionSampleVersion:
    """The originally observed physical SCD2 version and its immutable payload."""

    row_id: int
    row: PortfolioPnlSampleRow


@dataclass(frozen=True)
class PnlCorrectionEvidence:
    """Complete active presence/absence and causal-peak evidence for one chunk.

    Versions include every epoch and algorithm occupying this wallet/mode/ccy
    range, because the active uniqueness key is broader than the correction key.
    Absence is the complement of this complete set inside the bounded window.
    """

    key: PnlCorrectionKey
    start: datetime
    end: datetime
    versions: tuple[PnlCorrectionSampleVersion, ...]
    prior_peak: float | None


@dataclass(frozen=True)
class PnlCorrectionEdit:
    """One insert, CAS replacement or CAS retraction inside an admitted chunk.

    A missing expected row denotes an insert. A missing replacement denotes a
    retraction and requires the exact previously read row. Public ids alone are
    insufficient because SCD2 replacements deliberately reuse them.
    """

    point_time: datetime
    expected_row: PnlCorrectionSampleVersion | None
    replacement: PortfolioPnlSampleRow | None


@dataclass(frozen=True)
class PnlCorrectionChunk:
    """A bounded inclusive chunk and its resulting causal peak.

    Its start is the durable checkpoint's next minute. An empty edit tuple is a
    legitimate transaction and must still CAS-advance the explicit checkpoint.
    Repair acknowledgement is explicit; scan completion alone cannot consume
    an outstanding withheld obligation from an earlier chunk or generation.
    ``unrepairable_withheld_minutes`` names the subset of ``withheld_minutes``
    that carry no future repair obligation because they are deterministic on
    frozen evidence and leave no unproved active sample row (either absent or
    explicitly retracted in ``edits``, guard R1). The caller certifies semantic
    recomputation. The repository additionally verifies post-edit occupancy for
    the acknowledged outstanding repair suffix; presence alone does not prove
    that a retained row was correctly recomputed.
    """

    end: datetime
    edits: tuple[PnlCorrectionEdit, ...]
    prior_peak: float | None
    evidence: PnlCorrectionEvidence
    withheld_minutes: tuple[datetime, ...] = ()
    reconciles_repair: bool = False
    unrepairable_withheld_minutes: tuple[datetime, ...] = ()


def validate_correction_minute(value: datetime) -> None:
    """Require an aware exact-minute identity without silently rounding it.

    Args:
        value: An input window or edit boundary.

    Raises:
        ValueError: When the time is naive or not aligned to a minute.
    """
    if value.utcoffset() is None or value.astimezone(UTC).second or value.microsecond:
        raise ValueError("P&L correction boundaries must be aware exact minutes")


def validate_correction_peak(value: float | None) -> None:
    """Refuse a nonfinite causal peak without inventing a zero.

    Args:
        value: A causal peak, or honest absence of prior complete evidence.

    Raises:
        ValueError: When the supplied peak is nonfinite.
    """
    if value is not None and not math.isfinite(value):
        raise ValueError("P&L correction peak must be finite or absent")


def validate_correction_manifest_digest(value: str | None) -> None:
    """Require a lowercase SHA-256 manifest digest when present.

    Args:
        value: The digest of the captured execution manifest, or a legacy null.

    Raises:
        ValueError: If a supplied digest has the wrong length or alphabet.
    """
    if value is not None and (
        not isinstance(value, str)
        or len(value) != 64
        or any(character not in "0123456789abcdef" for character in value)
    ):
        raise ValueError("P&L correction manifest digest must be lowercase SHA-256")


def correction_expected_venues(value: frozenset[str] | None) -> frozenset[str] | None:
    """Validate the immutable spot venue set captured for a correction generation.

    Args:
        value: A nonempty canonical spot venue set, or a legacy null.

    Returns:
        The certified venue set, or legacy absence.

    Raises:
        ValueError: If the supplied set is empty or contains an invalid venue.
    """
    if value is not None and (
        not isinstance(value, frozenset)
        or not value
        or any(
            not isinstance(venue, str)
            or not venue
            or venue != venue.lower()
            or any(character.isspace() for character in venue)
            for venue in value
        )
    ):
        raise ValueError("P&L correction requires a nonempty canonical spot venue set")
    return value


def correction_watermarks(values: Mapping[str, int]) -> dict[str, int]:
    """Validate and copy a watermark map so caller mutation cannot alter a plan.

    Args:
        values: Complete venue-to-sequence evidence for a correction boundary.

    Returns:
        An independent deterministic map.

    Raises:
        ValueError: For blank venue names, booleans or negative sequences.
    """
    if any(
        not key
        or key != key.strip()
        or not isinstance(value, int)
        or isinstance(value, bool)
        or value < 0
        for key, value in values.items()
    ):
        raise ValueError("P&L correction watermarks must name venues and nonnegative sequences")
    return dict(sorted(values.items()))


def correction_fill_witness_watermarks(
    values: Mapping[str, int] | None, digest: str | None
) -> dict[str, int] | None:
    """Copy the venue event bounds together with their required membership proof.

    Args:
        values: Physical upper bounds, or null for a legacy checkpoint.
        digest: SHA-256 over the candidate rows, or null for a legacy checkpoint.

    Returns:
        Canonical bounds, or null when neither component is present.

    Raises:
        ValueError: When one component is missing or either is malformed.
    """
    if (values is None) != (digest is None):
        raise ValueError("P&L correction fill-witness bounds and digest must be paired")
    validate_correction_manifest_digest(digest)
    return correction_watermarks(values) if values is not None else None


def _require_watermark_progress(baseline: Mapping[str, int], pending: Mapping[str, int]) -> None:
    """Refuse evidence prefixes that forget an already observed venue sequence.

    Args:
        baseline: The durable evidence prefix which must remain represented.
        pending: The complete candidate prefix for the new generation.

    Raises:
        ValueError: If any previously observed sequence disappears or decreases.
    """
    if any(pending.get(venue, -1) < sequence for venue, sequence in baseline.items()):
        raise ValueError("P&L correction evidence watermarks must not move backwards")


def _require_retained_execution_evidence(
    current: PnlCorrectionFrontier,
    request: PnlCorrectionRequest,
    raw: Mapping[str, int] | None,
) -> None:
    """Require renewed proof for the evidence coordinates of a certified generation."""
    if current.raw_execution_watermarks is not None:
        if raw is None:
            raise ValueError("P&L correction must retain its raw execution watermarks")
        _require_watermark_progress(current.raw_execution_watermarks, raw)
    if current.execution_manifest_digest is not None and request.execution_manifest_digest is None:
        raise ValueError("P&L correction must retain its execution manifest digest")
    if current.has_fill_gap is not None and request.has_fill_gap is None:
        raise ValueError("P&L correction must retain its fill-gap verdict")
    if current.expected_venues is not None and request.expected_venues is None:
        raise ValueError("P&L correction must retain its certified spot venues")
    if current.venue_event_watermarks is not None and request.venue_event_watermarks is None:
        raise ValueError("P&L correction must retain its certified fill-witness cut")


def start_correction_frontier(
    key: PnlCorrectionKey,
    request: PnlCorrectionRequest,
    current: PnlCorrectionFrontier | None,
) -> PnlCorrectionFrontier:
    """Create or explicitly rewind a generation without advancing its baseline.

    Args:
        key: The repository's canonical scope and current calculation version.
        request: Newly discovered invalidation and its complete evidence prefix.
        current: The checkpoint read under the writer fence, or honest absence.

    Returns:
        A new generation retaining unfinished work and the durable baseline.

    Raises:
        ValueError: For invalid ranges, clocks, peaks or regressing evidence.
    """
    validate_correction_minute(request.start)
    validate_correction_minute(request.tip)
    validate_correction_peak(request.prior_peak)
    validate_correction_manifest_digest(request.execution_manifest_digest)
    expected_venues = correction_expected_venues(request.expected_venues)
    witness = correction_fill_witness_watermarks(
        request.venue_event_watermarks, request.fill_witness_digest
    )
    if request.as_of.utcoffset() is None or request.start > request.tip:
        raise ValueError("P&L correction requires an aware clock and ordered window")
    baseline = correction_watermarks(request.baseline_watermarks)
    pending = correction_watermarks(request.pending_watermarks)
    raw = (
        correction_watermarks(request.raw_execution_watermarks)
        if request.raw_execution_watermarks is not None
        else None
    )
    start, tip, peak = request.start, request.tip, request.prior_peak
    if current is not None:
        if current.key != key or request.as_of < current.as_of:
            raise ValueError("P&L correction must retain its scope and knowledge horizon")
        if current.repair_from is not None and request.start > current.repair_from:
            raise ValueError("P&L correction must retain its pending withheld repair boundary")
        baseline = dict(current.baseline_watermarks)
        _require_watermark_progress(current.pending_watermarks, pending)
        _require_retained_execution_evidence(current, request, raw)
        tip = max(tip, current.tip)
        if current.next_minute is not None and current.next_minute <= start:
            start, peak = current.next_minute, current.prior_peak
    _require_watermark_progress(baseline, pending)
    if request.as_of < tip:
        raise ValueError("P&L correction evidence clock must cover its entire target")
    return PnlCorrectionFrontier(
        key=key,
        generation=current.generation + 1 if current is not None else 1,
        revision=current.revision + 1 if current is not None else 1,
        window_start=start,
        next_minute=start,
        tip=tip,
        as_of=request.as_of,
        baseline_watermarks=baseline,
        pending_watermarks=pending,
        prior_peak=peak,
        repair_from=current.repair_from if current is not None else None,
        raw_execution_watermarks=raw,
        execution_manifest_digest=request.execution_manifest_digest,
        has_fill_gap=request.has_fill_gap,
        expected_venues=expected_venues,
        venue_event_watermarks=witness,
        fill_witness_digest=request.fill_witness_digest,
    )


def advance_correction_frontier(
    current: PnlCorrectionFrontier, chunk: PnlCorrectionChunk
) -> PnlCorrectionFrontier:
    """Advance one admitted chunk, publishing the baseline only at completion.

    Args:
        current: Actual checkpoint loaded inside the same financial transaction.
        chunk: The explicitly bounded successful plan, including a zero-write plan.

    Returns:
        The checkpoint to CAS-write with this chunk's financial mutations.

    Raises:
        ValueError: If the chunk is completed, oversized, repeated or out of range.
    """
    validate_correction_minute(chunk.end)
    validate_correction_peak(chunk.prior_peak)
    start = current.next_minute
    if start is None or not start <= chunk.end <= current.tip:
        raise ValueError("P&L correction chunk must continue the pending window")
    if (chunk.end - start) / _MINUTE >= MAX_CORRECTION_CHUNK_MINUTES:
        raise ValueError("P&L correction chunk exceeds its fixed minute bound")
    if (
        chunk.evidence.key != current.key
        or chunk.evidence.start != start
        or chunk.evidence.end != chunk.end
    ):
        raise ValueError("P&L correction evidence must bind the complete admitted chunk")
    minutes = [edit.point_time for edit in chunk.edits]
    if len(minutes) != len(set(minutes)):
        raise ValueError("P&L correction chunk must not repeat a minute")
    for edit in chunk.edits:
        validate_correction_minute(edit.point_time)
        if not start <= edit.point_time <= chunk.end:
            raise ValueError("P&L correction edit lies outside the admitted chunk")
        if edit.expected_row is None and edit.replacement is None:
            raise ValueError("P&L correction absence is not a retraction")
    _validate_correction_withheld(chunk, start)
    repair_from = _remaining_correction_repair(current, chunk, start)
    complete = chunk.end == current.tip
    raw = current.raw_execution_watermarks
    witness = current.venue_event_watermarks
    return replace(
        current,
        revision=current.revision + 1,
        next_minute=None if complete else chunk.end + _MINUTE,
        baseline_watermarks=dict(
            current.pending_watermarks
            if complete and repair_from is None
            else current.baseline_watermarks
        ),
        pending_watermarks=dict(current.pending_watermarks),
        prior_peak=chunk.prior_peak,
        repair_from=repair_from,
        raw_execution_watermarks=dict(raw) if raw is not None else None,
        venue_event_watermarks=dict(witness) if witness is not None else None,
    )


def _validate_correction_withheld(chunk: PnlCorrectionChunk, start: datetime) -> None:
    """Allow withheld minutes to close observed rows while verifying unrepairable bounds.

    Args:
        chunk: The admitted plan, including its explicit withheld classification.
        start: The actual checkpoint's next minute.

    Raises:
        ValueError: For an out-of-window minute, simultaneous insert/replacement, or
            invalid unrepairable classification.
    """
    edits = {edit.point_time: edit for edit in chunk.edits}
    withheld = set(chunk.withheld_minutes)
    for minute in chunk.withheld_minutes:
        validate_correction_minute(minute)
        edit = edits.get(minute)
        if not start <= minute <= chunk.end or (
            edit is not None and (edit.expected_row is None or edit.replacement is not None)
        ):
            raise ValueError(
                "Withheld P&L minutes must be inside the chunk and permit only observed-row retractions"
            )
    _validate_correction_unrepairable_withheld(chunk, start, withheld, edits)


def _is_unrepairable_minute_rowless(
    active_versions: Sequence[PnlCorrectionSampleVersion],
    edit: PnlCorrectionEdit | None,
) -> bool:
    """Return whether zero active rows survive at a minute after its planned edit (R1)."""
    if not active_versions:
        return True
    return (
        len(active_versions) == 1
        and edit is not None
        and edit.expected_row == active_versions[0]
        and edit.replacement is None
    )


def _validate_correction_unrepairable_withheld(
    chunk: PnlCorrectionChunk,
    start: datetime,
    withheld: set[datetime],
    edits: Mapping[datetime, PnlCorrectionEdit],
) -> None:
    """Refuse malformed, non-subset, duplicate, or row-retaining exempt minutes (R1).

    An unrepairable withheld minute relinquishes its future repair obligation, so
    no active sample row may remain at that minute in ``chunk.evidence.versions``
    after applying ``chunk.edits``. Validating this inside the pure transition
    enforces R1 at the repository fence too, because the repository revalidates
    ``chunk.evidence`` under its writer lock before commit.

    Args:
        chunk: The admitted plan, including its unrepairable subset classification.
        start: The actual checkpoint's next minute.
        withheld: All minutes withheld in this chunk.
        edits: Planned sample edits indexed by minute.

    Raises:
        ValueError: If any listed minute is invalid, duplicated, outside the chunk
            or ``withheld_minutes``, or leaves an active version unretracted.
    """
    if not chunk.unrepairable_withheld_minutes:
        return
    versions_by_minute: dict[datetime, list[PnlCorrectionSampleVersion]] = {}
    for version in chunk.evidence.versions:
        versions_by_minute.setdefault(version.row["point_time"], []).append(version)
    seen: set[datetime] = set()
    for minute in chunk.unrepairable_withheld_minutes:
        validate_correction_minute(minute)
        if minute in seen:
            raise ValueError("Unrepairable withheld P&L minutes must not repeat a minute")
        seen.add(minute)
        if not start <= minute <= chunk.end or minute not in withheld:
            raise ValueError(
                "Unrepairable withheld P&L minutes must be inside the chunk and withheld"
            )
        if not _is_unrepairable_minute_rowless(
            versions_by_minute.get(minute, ()), edits.get(minute)
        ):
            raise ValueError("Unrepairable withheld P&L minutes cannot retain an active sample row")


def _remaining_correction_repair(
    current: PnlCorrectionFrontier, chunk: PnlCorrectionChunk, start: datetime
) -> datetime | None:
    """Advance an outstanding repair suffix only with explicit successful reconciliation.

    Minutes in ``chunk.unrepairable_withheld_minutes`` are excluded from new
    repair candidates while any already-latched ``current.repair_from`` pending
    logic remains unchanged.

    Args:
        current: Durable pending repair, retained even across generation changes.
        chunk: Completed chunk and its explicit unresolved-minute classification.
        start: Actual admitted start read from the checkpoint.

    Returns:
        The earliest outstanding repair boundary, without inferring it from missing rows.
    """
    pending = current.repair_from
    if pending is not None and start <= pending <= chunk.end and chunk.reconciles_repair:
        pending = None if chunk.end == current.tip else chunk.end + _MINUTE
    unrepairable = set(chunk.unrepairable_withheld_minutes)
    repairable_withheld = (
        minute for minute in chunk.withheld_minutes if minute not in unrepairable
    )
    candidates = (*repairable_withheld, *((pending,) if pending is not None else ()))
    return min(candidates) if candidates else None


def _require_non_bool_int(value: object, label: str) -> int:
    """Require a plain integer while rejecting booleans."""
    if isinstance(value, bool) or not isinstance(value, int):
        raise ValueError(f"{label} must be an integer")
    return value


def _require_non_negative_int(value: object, label: str) -> int:
    """Require a non-negative plain integer while rejecting booleans."""
    parsed = _require_non_bool_int(value, label)
    if parsed < 0:
        raise ValueError(f"{label} must be non-negative")
    return parsed


def _require_non_blank_str(value: object, label: str) -> str:
    """Require a non-empty string without leading or trailing whitespace."""
    if not isinstance(value, str) or not value or value != value.strip():
        raise ValueError(f"{label} must be a non-empty stripped string")
    return value


def _require_sha256_digest(value: object, label: str) -> str:
    """Require a 64-character lowercase hexadecimal SHA-256 digest string."""
    if (
        not isinstance(value, str)
        or len(value) != 64
        or any(character not in _HEX_DIGITS for character in value)
    ):
        raise ValueError(f"{label} must be a 64-character lowercase hex digest")
    return value


@dataclass(frozen=True)
class IdAggregate:
    """Order-independent row-identity aggregate fingerprint over active rows.

    Stores ``(count, id_sum, max_id, max_timestamp)`` with arbitrary-precision
    Python integer arithmetic on ``id_sum`` so callers never suffer 64-bit
    overflow when comparing accrual or event-FX candle prefixes.
    """

    count: int
    id_sum: int
    max_id: int
    max_timestamp: datetime | None

    def __post_init__(self) -> None:
        """Validate integer types, non-negativity, aware timestamp, and zero coherence."""
        _require_non_negative_int(self.count, "IdAggregate.count")
        _require_non_negative_int(self.id_sum, "IdAggregate.id_sum")
        _require_non_negative_int(self.max_id, "IdAggregate.max_id")
        if self.max_timestamp is not None:
            if not isinstance(self.max_timestamp, datetime):
                raise ValueError("IdAggregate.max_timestamp must be a datetime")
            if self.max_timestamp.utcoffset() is None:
                raise ValueError("IdAggregate.max_timestamp must be timezone-aware")
        zero_flags = (
            self.count == 0,
            self.id_sum == 0,
            self.max_id == 0,
            self.max_timestamp is None,
        )
        if len(set(zero_flags)) != 1:
            raise ValueError(
                "IdAggregate fields must be coherent: count == 0 iff id_sum == 0 "
                "iff max_id == 0 iff max_timestamp is None"
            )
        if self.count > 0:
            if self.id_sum < self.count:
                raise ValueError("IdAggregate.id_sum must be at least IdAggregate.count")
            if self.id_sum < self.max_id or self.id_sum > self.count * self.max_id:
                raise ValueError(
                    "IdAggregate.id_sum must satisfy max_id <= id_sum <= count * max_id"
                )


def _validate_certificate_key(cert: PnlOpeningCheckpointCertificate) -> None:
    """Validate certificate key components, version labels, and anchor id."""
    if cert.key.mode not in ("live", "paper"):
        raise ValueError("certificate key.mode must be 'live' or 'paper'")
    _require_non_blank_str(cert.key.wallet_public_id, "certificate key.wallet_public_id")
    _require_non_blank_str(cert.key.valuation_ccy, "certificate key.valuation_ccy")
    _require_non_blank_str(cert.key.epoch_public_id, "certificate key.epoch_public_id")
    _require_non_blank_str(cert.key.calc_version, "certificate key.calc_version")
    _require_non_blank_str(cert.timeline_calc_version, "certificate timeline_calc_version")
    _require_non_blank_str(cert.fx_policy_version, "certificate fx_policy_version")
    _require_non_blank_str(cert.anchor_public_id, "certificate anchor_public_id")


def _validate_certificate_digests(cert: PnlOpeningCheckpointCertificate) -> None:
    """Validate every fixed 64-character lowercase hex digest field on a certificate."""
    digest_fields = (
        ("anchor_digest", cert.anchor_digest),
        ("execution_manifest_digest", cert.execution_manifest_digest),
        ("prefix_annulment_digest", cert.prefix_annulment_digest),
        ("prefix_lineage_digest", cert.prefix_lineage_digest),
        ("prefix_fill_witness_digest", cert.prefix_fill_witness_digest),
        ("source_fill_witness_digest", cert.source_fill_witness_digest),
        ("symbol_ref_digest", cert.symbol_ref_digest),
        ("event_fx_exchanges_digest", cert.event_fx_exchanges_digest),
    )
    for label, value in digest_fields:
        _require_sha256_digest(value, label)


def _validate_certificate_aggregates(cert: PnlOpeningCheckpointCertificate) -> None:
    """Validate both fingerprint aggregate types and horizon timestamp bounds."""
    aggregates = (
        ("accrual_prefix_fingerprint", cert.accrual_prefix_fingerprint),
        ("event_fx_candles_fingerprint", cert.event_fx_candles_fingerprint),
    )
    for field_name, aggregate in aggregates:
        if not isinstance(aggregate, IdAggregate):
            raise ValueError(f"certificate {field_name} must be IdAggregate")
        if aggregate.max_timestamp is not None and aggregate.max_timestamp > cert.as_of:
            raise ValueError(f"certificate {field_name}.max_timestamp cannot exceed as_of")


def _validate_certificate_header(cert: PnlOpeningCheckpointCertificate) -> None:
    """Validate scalar header fields, timestamps, aggregates, key, and digests."""
    schema_version = _require_non_bool_int(cert.schema_version, "certificate schema_version")
    if schema_version != 1:
        raise ValueError("certificate schema_version must equal 1")
    source_generation = _require_non_bool_int(
        cert.source_generation,
        "certificate source_generation",
    )
    if source_generation < 1:
        raise ValueError("certificate source_generation must be at least 1")
    if not isinstance(cert.prefix_has_idless_executions, bool):
        raise ValueError("certificate prefix_has_idless_executions must be a bool")
    validate_correction_minute(cert.checkpoint_minute)
    if not isinstance(cert.as_of, datetime):
        raise ValueError("certificate as_of must be a datetime")
    if cert.as_of.utcoffset() is None:
        raise ValueError("certificate as_of must be timezone-aware")
    if cert.checkpoint_minute > cert.as_of:
        raise ValueError("certificate checkpoint_minute cannot exceed as_of")
    _validate_certificate_aggregates(cert)
    _validate_certificate_key(cert)
    _validate_certificate_digests(cert)


def _normalize_chain_digests(
    digests: Mapping[str, str],
    cutoff_watermarks: Mapping[str, int],
) -> dict[str, str]:
    """Validate and canonically sort per-exchange execution chain tip digests."""
    if set(digests) != set(cutoff_watermarks):
        raise ValueError(
            "certificate execution_chain_digests exchanges must match cutoff_execution_watermarks"
        )
    return {
        exchange: _require_sha256_digest(digest, f"execution_chain_digests[{exchange!r}]")
        for exchange, digest in sorted(digests.items())
    }


def _normalize_cutoff_counts(
    counts: Mapping[str, int],
    cutoff_watermarks: Mapping[str, int],
) -> dict[str, int]:
    """Validate ``0 <= count <= W_C[ex]`` and sort per-exchange execution counts."""
    if set(counts) != set(cutoff_watermarks):
        raise ValueError(
            "certificate cutoff_execution_counts exchanges must match cutoff_execution_watermarks"
        )
    normalized: dict[str, int] = {}
    for exchange, count in sorted(counts.items()):
        validated = _require_non_negative_int(count, "cutoff_execution_counts")
        if validated > cutoff_watermarks[exchange]:
            raise ValueError(
                f"cutoff_execution_counts[{exchange!r}] cannot exceed cutoff watermark"
            )
        normalized[exchange] = validated
    return normalized


def _normalize_applied_watermarks(
    applied: Mapping[str, int],
    cutoff_watermarks: Mapping[str, int],
) -> dict[str, int]:
    """Validate ``0 <= applied <= W_C[ex]`` and return a sorted copy."""
    normalized = correction_watermarks(applied)
    for exchange, seq in normalized.items():
        if exchange not in cutoff_watermarks or seq > cutoff_watermarks[exchange]:
            raise ValueError(
                f"applied_watermarks_by_exchange[{exchange!r}] cannot exceed cutoff watermark"
            )
    return normalized


def _normalize_deferred_sequences(
    deferred: Mapping[str, Sequence[int]],
    cutoff_watermarks: Mapping[str, int],
) -> dict[str, tuple[int, ...]]:
    """Validate strictly increasing ``1..W_C[ex]`` sequences and normalize to sorted tuples."""
    normalized: dict[str, tuple[int, ...]] = {}
    for exchange, sequences in sorted(deferred.items()):
        if exchange not in cutoff_watermarks:
            raise ValueError(
                f"deferred_execution_sequences exchange {exchange!r} is not in "
                "cutoff_execution_watermarks"
            )
        limit = cutoff_watermarks[exchange]
        prev: int | None = None
        validated: list[int] = []
        for seq in sequences:
            item = _require_non_bool_int(seq, "deferred_execution_sequences")
            if item < 1 or item > limit:
                raise ValueError(
                    f"deferred_execution_sequences[{exchange!r}] sequence must lie in 1..{limit}"
                )
            if prev is not None and item <= prev:
                raise ValueError(
                    f"deferred_execution_sequences[{exchange!r}] must be strictly "
                    "increasing without duplicates"
                )
            prev = item
            validated.append(item)
        normalized[exchange] = tuple(validated)
    return normalized


def _normalize_consumed_fx_pairs(
    pairs: Sequence[tuple[str, str]],
) -> tuple[tuple[str, str], ...]:
    """Validate sorted distinct unordered currency pairs and convert to tuple."""
    normalized: list[tuple[str, str]] = []
    prev: tuple[str, str] | None = None
    for first, second in pairs:
        _require_non_blank_str(first, "consumed_fx_pairs")
        _require_non_blank_str(second, "consumed_fx_pairs")
        if first >= second:
            raise ValueError("consumed_fx_pairs entries must be lexically ordered distinct legs")
        pair = (first, second)
        if prev is not None and pair <= prev:
            raise ValueError("consumed_fx_pairs must be strictly increasing without duplicates")
        prev = pair
        normalized.append(pair)
    return tuple(normalized)


def _normalize_consumed_fx_open_ats(
    open_ats: Sequence[datetime],
    checkpoint_minute: datetime,
) -> tuple[datetime, ...]:
    """Validate strictly increasing minute-aligned open times ``<= checkpoint_minute``."""
    normalized: list[datetime] = []
    prev: datetime | None = None
    for open_at in open_ats:
        validate_correction_minute(open_at)
        if open_at > checkpoint_minute:
            raise ValueError("consumed_fx_open_ats cannot exceed checkpoint_minute")
        if prev is not None and open_at <= prev:
            raise ValueError("consumed_fx_open_ats must be strictly increasing without duplicates")
        prev = open_at
        normalized.append(open_at)
    return tuple(normalized)


def _validate_source_expected_venues(value: frozenset[str] | None) -> frozenset[str]:
    """Require a non-empty canonical spot venue set for the replayed frontier cut."""
    validated = correction_expected_venues(value)
    if validated is None:
        raise ValueError(
            "certificate source_expected_venues requires a nonempty canonical spot venue set"
        )
    return validated


def _validate_certificate_fx_fingerprint(
    fx_pairs: tuple[tuple[str, str], ...],
    fx_open_ats: tuple[datetime, ...],
    fingerprint: IdAggregate,
) -> None:
    """Require zero event-FX candle aggregate when either FX input sequence is empty."""
    if (not fx_pairs or not fx_open_ats) and fingerprint != IdAggregate(0, 0, 0, None):
        raise ValueError(
            "certificate event_fx_candles_fingerprint must be empty when "
            "consumed_fx_pairs or consumed_fx_open_ats is empty"
        )


@dataclass(frozen=True)
class PnlOpeningCheckpointCertificate:
    """Cryptographic and structural evidence certificate for an opening checkpoint.

    Binds scope identity, calculation and policy versions, schema version,
    activation anchor, source generation, boundary clock, execution cut
    watermarks/digests/counts/deferred sequences, prefix annulment and lineage
    digests, prefix and source fill-witness cuts, symbol reference digest,
    accrual fingerprint, and consumed event-FX exchange and candle fingerprints.

    Prefix vs. source witness cuts: ``prefix_*`` witness fields
    (``prefix_fill_witness_digest`` and ``prefix_venue_event_watermarks``) are
    restricted to sequences up to the checkpoint boundary and define the
    reader's recomputation target verified at the resuming generation's
    ``as_of``. By contrast, ``source_*`` fields (``source_expected_venues``,
    ``source_venue_event_watermarks``, and ``source_fill_witness_digest``) bind
    the certificate at write time to the active frontier cut under which the
    checkpoint state was replayed, matching the write gate's execution-cut check.

    Evidence-read ordering contract: reads at a fixed ``as_of`` do not share a
    database snapshot across queries, so every certificate fingerprint and
    digest must be read BEFORE the final replay chunk reads its prefix
    evidence. A concurrent commit with bus ``timestamp <= as_of`` that lands
    between the two reads enters the carried state without entering the stored
    certificate, so later verification mismatches and falls back safely; reading
    the replay evidence before the certificate would be unsafe.

    Resumption verification contract: on resume, the reader always recomputes
    every certificate fingerprint and digest at the RESUMING generation's
    ``as_of`` (never at the certificate's stored ``as_of``), so any prefix
    correction, accrual change, candle revision, lineage relabel, or late
    witness arriving after certification triggers a fail-closed fallback to full
    anchor replay.
    """

    key: PnlCorrectionKey
    timeline_calc_version: str
    fx_policy_version: str
    schema_version: int
    anchor_public_id: str
    anchor_digest: str
    source_generation: int
    checkpoint_minute: datetime
    as_of: datetime
    cutoff_execution_watermarks: Mapping[str, int]
    execution_chain_digests: Mapping[str, str]
    cutoff_execution_counts: Mapping[str, int]
    applied_watermarks_by_exchange: Mapping[str, int]
    deferred_execution_sequences: Mapping[str, Sequence[int]]
    prefix_has_idless_executions: bool
    execution_manifest_digest: str
    prefix_annulment_digest: str
    accrual_prefix_fingerprint: IdAggregate
    prefix_lineage_digest: str
    prefix_fill_witness_digest: str
    prefix_venue_event_watermarks: Mapping[str, int]
    source_expected_venues: frozenset[str]
    source_venue_event_watermarks: Mapping[str, int]
    source_fill_witness_digest: str
    symbol_ref_digest: str
    consumed_fx_pairs: Sequence[tuple[str, str]]
    consumed_fx_open_ats: Sequence[datetime]
    event_fx_exchanges_digest: str
    event_fx_candles_fingerprint: IdAggregate

    def __post_init__(self) -> None:
        """Validate every shape invariant and normalize mappings and sequences."""
        _validate_certificate_header(self)
        cutoff = correction_watermarks(self.cutoff_execution_watermarks)
        chain_digests = _normalize_chain_digests(self.execution_chain_digests, cutoff)
        counts = _normalize_cutoff_counts(self.cutoff_execution_counts, cutoff)
        applied = _normalize_applied_watermarks(self.applied_watermarks_by_exchange, cutoff)
        deferred = _normalize_deferred_sequences(self.deferred_execution_sequences, cutoff)
        witness_watermarks = correction_watermarks(self.prefix_venue_event_watermarks)
        expected_venues = _validate_source_expected_venues(self.source_expected_venues)
        if not isinstance(self.source_venue_event_watermarks, Mapping):
            raise ValueError("certificate source_venue_event_watermarks must be a mapping")
        source_witness_watermarks = correction_watermarks(self.source_venue_event_watermarks)
        fx_pairs = _normalize_consumed_fx_pairs(self.consumed_fx_pairs)
        fx_open_ats = _normalize_consumed_fx_open_ats(
            self.consumed_fx_open_ats,
            self.checkpoint_minute,
        )
        _validate_certificate_fx_fingerprint(
            fx_pairs,
            fx_open_ats,
            self.event_fx_candles_fingerprint,
        )
        object.__setattr__(self, "cutoff_execution_watermarks", cutoff)
        object.__setattr__(self, "execution_chain_digests", chain_digests)
        object.__setattr__(self, "cutoff_execution_counts", counts)
        object.__setattr__(self, "applied_watermarks_by_exchange", applied)
        object.__setattr__(self, "deferred_execution_sequences", deferred)
        object.__setattr__(self, "prefix_venue_event_watermarks", witness_watermarks)
        object.__setattr__(self, "source_expected_venues", expected_venues)
        object.__setattr__(self, "source_venue_event_watermarks", source_witness_watermarks)
        object.__setattr__(self, "consumed_fx_pairs", fx_pairs)
        object.__setattr__(self, "consumed_fx_open_ats", fx_open_ats)


@dataclass(frozen=True)
class PnlOpeningCheckpointWrite:
    """Certified opening checkpoint payload staged for a fenced chunk commit."""

    certificate: PnlOpeningCheckpointCertificate
    state_json: str

    def __post_init__(self) -> None:
        """Validate certificate type and non-blank serialized state payload."""
        if not isinstance(self.certificate, PnlOpeningCheckpointCertificate):
            raise ValueError("checkpoint write certificate must be PnlOpeningCheckpointCertificate")
        _require_non_blank_str(self.state_json, "checkpoint write state_json")


@dataclass(frozen=True)
class PnlOpeningCheckpointRecord:
    """Persisted opening checkpoint certificate, raw serialized state, and write time."""

    certificate: PnlOpeningCheckpointCertificate
    state_json: str
    updated_at: datetime

    def __post_init__(self) -> None:
        """Validate certificate, serialized state payload, and aware write timestamp."""
        if not isinstance(self.certificate, PnlOpeningCheckpointCertificate):
            raise ValueError(
                "checkpoint record certificate must be PnlOpeningCheckpointCertificate"
            )
        _require_non_blank_str(self.state_json, "checkpoint record state_json")
        if not isinstance(self.updated_at, datetime):
            raise ValueError("checkpoint record updated_at must be a datetime")
        if self.updated_at.utcoffset() is None:
            raise ValueError("checkpoint record updated_at must be timezone-aware")
