"""Explicit, versioned continuation state for bounded P&L correction transactions.

These contracts do not enable the snapshotter. A checkpoint is independent of
sample existence: a legitimately withheld or unchanged chunk can advance it
without manufacturing a financial row. Repository transactions must fence every
transition with the checkpoint key, generation and revision.
"""

import math
from collections.abc import Mapping
from dataclasses import dataclass
from dataclasses import replace
from datetime import UTC
from datetime import datetime
from datetime import timedelta

from snapper.data.repository_types import PortfolioPnlSampleRow

MAX_CORRECTION_CHUNK_MINUTES = 1440
_MINUTE = timedelta(minutes=1)


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
    The caller certifies semantic recomputation. The repository additionally
    verifies post-edit occupancy for the acknowledged outstanding repair suffix;
    presence alone does not prove that a retained row was correctly recomputed.
    """

    end: datetime
    edits: tuple[PnlCorrectionEdit, ...]
    prior_peak: float | None
    evidence: PnlCorrectionEvidence
    withheld_minutes: tuple[datetime, ...] = ()
    reconciles_repair: bool = False


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
    )


def _validate_correction_withheld(chunk: PnlCorrectionChunk, start: datetime) -> None:
    """Allow withheld minutes to close observed rows while retaining their repair obligation.

    Args:
        chunk: The admitted plan, including its explicit withheld classification.
        start: The actual checkpoint's next minute.

    Raises:
        ValueError: For an out-of-window minute or a simultaneous insert/replacement.
    """
    edits = {edit.point_time: edit for edit in chunk.edits}
    for minute in chunk.withheld_minutes:
        validate_correction_minute(minute)
        edit = edits.get(minute)
        if not start <= minute <= chunk.end or (
            edit is not None and (edit.expected_row is None or edit.replacement is not None)
        ):
            raise ValueError(
                "Withheld P&L minutes must be inside the chunk and permit only observed-row retractions"
            )


def _remaining_correction_repair(
    current: PnlCorrectionFrontier, chunk: PnlCorrectionChunk, start: datetime
) -> datetime | None:
    """Advance an outstanding repair suffix only with explicit successful reconciliation.

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
    candidates = (*chunk.withheld_minutes, *((pending,) if pending is not None else ()))
    return min(candidates) if candidates else None
