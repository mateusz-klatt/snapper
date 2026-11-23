"""Pure opening-checkpoint canonical digests, fence, and write-refusal predicates.

Provides order-independent, kind-tagged SHA-256 digests over the five replay
evidence sets that can vary across ``as_of`` reads, together with the pure
invalidation fence and write-refusal predicate evaluated around certified checkpoints.
This module performs no I/O and imports no infrastructure.
"""

import hashlib
import json
from collections.abc import Mapping
from collections.abc import Sequence
from datetime import UTC
from datetime import datetime
from typing import Final
from typing import Literal

from snapper.data.pnl_correction_state import PnlCorrectionChunk
from snapper.data.pnl_correction_state import PnlCorrectionFrontier
from snapper.data.pnl_correction_state import PnlOpeningCheckpointCertificate
from snapper.data.pnl_correction_state import correction_watermarks
from snapper.data.repository_types import InstrumentSymbolRefRow
from snapper.data.repository_types import PnlFxRatePlane
from snapper.data.repository_types import PnlTimelineAppliedAnnulment
from snapper.data.repository_types import PnlTimelineExecutionLineageRow
from snapper.data.repository_types import PnlTimelineOpeningExecutionRow

MAX_DEFERRED_EXECUTIONS: Final[int] = 256
"""Maximum deferred-execution count permitted across all exchanges in a checkpoint."""

type PnlCheckpointWriteRefusal = Literal[
    "not_settled",
    "not_at_tip",
    "generation_mismatch",
    "frontier_evidence_incomplete",
    "fill_gap",
    "cut_mismatch",
    "key_mismatch",
    "deferred_tail_too_long",
]
"""Closed vocabulary of deterministic checkpoint write-refusal reasons in precedence order."""


def opening_checkpoint_fence(frontier: PnlCorrectionFrontier) -> datetime:
    """Return the earliest boundary that invalidates an opening checkpoint.

    ``window_start`` is always set on a frontier, so the fence always exists;
    the optional pending minute and repair boundary can only move it earlier.

    Args:
        frontier: Correction state whose open window and repair boundaries form the fence.

    Returns:
        The minimum of the window start and any set next minute or repair boundary.
    """
    return min(
        (
            frontier.window_start,
            *(value for value in (frontier.next_minute, frontier.repair_from) if value is not None),
        )
    )


def checkpoint_minute_fenced(minute: datetime, fence: datetime | None) -> bool:
    """Return whether a checkpoint minute lies at or beyond an invalidation fence.

    Args:
        minute: Certified opening-checkpoint minute to test.
        fence: Earliest invalidated minute, or ``None`` for no fence.

    Returns:
        ``False`` for an absent fence or an earlier minute, otherwise ``True``.
    """
    return fence is not None and minute >= fence


class PnlCheckpointWriteRefusalError(ValueError):
    """Typed write-refusal error carrying a closed refusal code for repository writes."""

    def __init__(self, reason: PnlCheckpointWriteRefusal) -> None:
        """Bind a closed repository write refusal onto a ``ValueError`` subtype.

        Args:
            reason: Closed write-refusal reason code.
        """
        super().__init__(f"Refusing opening checkpoint write: {reason}")
        self.reason: PnlCheckpointWriteRefusal = reason


def execution_manifest_digest(annulments: Sequence[PnlTimelineAppliedAnnulment]) -> str:
    """Hash all certified correction identities in canonical order, including empty sets.

    Args:
        annulments: Applied annulment manifest entries for a generation.

    Returns:
        Lowercase 64-character SHA-256 hexadecimal digest.
    """
    identities = sorted(
        (
            annulment["public_id"],
            annulment["target_execution_public_id"],
            annulment["target_execution_digest"],
            annulment["exchange"],
            annulment["scope_sequence"],
        )
        for annulment in annulments
    )
    canonical = json.dumps(identities, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(canonical).hexdigest()


def partition_execution_watermarks(
    executions: Sequence[PnlTimelineOpeningExecutionRow],
    deferred_execution_keys: Sequence[tuple[str, int]],
) -> tuple[dict[str, int], dict[str, tuple[int, ...]]]:
    """Partition prefix execution sequences into applied watermarks and deferred tuples.

    Args:
        executions: Captured effective execution prefix rows through ``W_C``.
        deferred_execution_keys: Deferred ``(exchange, scope_sequence)`` pairs.

    Returns:
        Applied maximum watermarks and sorted deferred sequences by exchange.
    """
    deferred_set = set(deferred_execution_keys)
    applied: dict[str, int] = {}
    deferred: dict[str, list[int]] = {}
    for row in sorted(executions, key=lambda item: (item["exchange"], item["scope_sequence"])):
        exchange = row["exchange"]
        sequence = row["scope_sequence"]
        if (exchange, sequence) in deferred_set:
            deferred.setdefault(exchange, []).append(sequence)
        else:
            applied[exchange] = max(applied.get(exchange, 0), sequence)
    return applied, {exchange: tuple(sequences) for exchange, sequences in sorted(deferred.items())}


type PnlFillWitnessIdentity = tuple[int, str]
"""Canonical ``(event_id, public_id)`` identity of one fill witness row."""

type _CanonicalAtom = str | int | None


def _sort_atom(atom: _CanonicalAtom) -> tuple[int, int, str]:
    """Return a total-order sort token distinguishing None, integers, and strings."""
    if atom is None:
        return (0, 0, "")
    if isinstance(atom, int):
        return (1, atom, "")
    return (2, 0, atom)


def _canonical_digest(kind: str, records: Sequence[tuple[_CanonicalAtom, ...]]) -> str:
    """Hash canonical tuples in sorted order under a domain-identifying kind tag.

    Args:
        kind: Explicit row-kind tag ensuring distinct domains never collide.
        records: Sequence of canonical field tuples representing one evidence kind.

    Returns:
        Lowercase 64-character SHA-256 hexadecimal digest.
    """
    sorted_records = sorted(
        records,
        key=lambda record: tuple(_sort_atom(field) for field in record),
    )
    payload = {"kind": kind, "records": sorted_records}
    encoded = json.dumps(
        payload,
        allow_nan=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _canonical_instant(value: datetime, label: str) -> str:
    """Serialize a timezone-aware datetime to canonical UTC ISO-8601 text."""
    if value.utcoffset() is None:
        raise ValueError(f"{label} datetimes must be timezone-aware")
    return value.astimezone(UTC).isoformat()


def prefix_annulment_digest(
    annulments: Sequence[PnlTimelineAppliedAnnulment],
    cutoff_watermarks: Mapping[str, int],
) -> str:
    """Compute canonical SHA-256 over applied annulments targeting ``1 .. W_C``.

    Args:
        annulments: Applied annulment manifest entries for the generation.
        cutoff_watermarks: Per-exchange execution cutoff ``W_C``.

    Returns:
        Kind-tagged, order-independent 64-character lowercase hex digest.

    Raises:
        ValueError: When ``cutoff_watermarks`` or any annulment ``scope_sequence``
            is malformed.
    """
    normalized_cut = correction_watermarks(cutoff_watermarks)
    records: list[tuple[_CanonicalAtom, ...]] = []
    for annulment in annulments:
        scope_sequence = annulment["scope_sequence"]
        if isinstance(scope_sequence, bool) or not isinstance(scope_sequence, int):
            raise ValueError("annulment scope_sequence must be an integer")
        exchange = annulment["exchange"]
        if 1 <= scope_sequence <= normalized_cut.get(exchange, 0):
            records.append(
                (
                    annulment["public_id"],
                    annulment["target_execution_public_id"],
                    annulment["target_execution_digest"],
                    exchange,
                    scope_sequence,
                )
            )
    return _canonical_digest("prefix_annulments", records)


def prefix_lineage_digest(rows: Sequence[PnlTimelineExecutionLineageRow]) -> str:
    """Compute canonical SHA-256 over execution order-lineage rows.

    Args:
        rows: Order-lineage rows returned for prefix execution order ids.

    Returns:
        Kind-tagged, order-independent 64-character lowercase hex digest.
    """
    records: list[tuple[_CanonicalAtom, ...]] = [
        (
            row["order_public_id"],
            row["source_surface"],
            row["plan_public_id"],
            row["signal_public_id"],
            row["origin"],
            row["strategy_name"],
        )
        for row in rows
    ]
    return _canonical_digest("prefix_lineage", records)


def symbol_ref_digest(refs: Sequence[InstrumentSymbolRefRow]) -> str:
    """Compute canonical SHA-256 over instrument symbol-reference intervals.

    Args:
        refs: Historical instrument-symbol reference intervals active by ``as_of``.

    Returns:
        Kind-tagged, order-independent 64-character lowercase hex digest.
    """
    records: list[tuple[_CanonicalAtom, ...]] = [
        (
            ref["instrument_public_id"],
            ref["native_symbol"],
            ref["exchange"],
            ref["instrument_exchange"],
            ref["base_currency"],
            ref["quote_currency"],
            _canonical_instant(ref["valid_from"], "symbol_ref.valid_from"),
            _canonical_instant(ref["valid_to"], "symbol_ref.valid_to"),
        )
        for ref in refs
    ]
    return _canonical_digest("symbol_refs", records)


def event_fx_exchanges_digest(planes: Sequence[PnlFxRatePlane]) -> str:
    """Compute canonical SHA-256 over discovered spot-FX venue planes.

    Args:
        planes: Active ``(base, quote, exchange)`` FX planes for consumed pairs.

    Returns:
        Kind-tagged, order-independent 64-character lowercase hex digest.
    """
    records: list[tuple[_CanonicalAtom, ...]] = [
        (base, quote, exchange) for base, quote, exchange in planes
    ]
    return _canonical_digest("event_fx_exchanges", records)


def prefix_fill_witness_digest(witnesses: Sequence[PnlFillWitnessIdentity]) -> str:
    """Compute canonical SHA-256 over fill witness ``(id, public_id)`` identities.

    Args:
        witnesses: Fill witness ``(event_id, public_id)`` pairs certifying the
            prefix execution cut.

    Returns:
        Kind-tagged, order-independent 64-character lowercase hex digest.

    Raises:
        ValueError: When any witness ``event_id`` is a boolean or negative.
    """
    records: list[tuple[_CanonicalAtom, ...]] = []
    for event_id, public_id in witnesses:
        if isinstance(event_id, bool) or not isinstance(event_id, int) or event_id < 0:
            raise ValueError("fill witness event_id must be a non-negative integer")
        records.append((event_id, public_id))
    return _canonical_digest("prefix_fill_witnesses", records)


def _extract_frontier_cut(
    frontier_after: PnlCorrectionFrontier,
) -> tuple[Mapping[str, int], str, bool, frozenset[str], Mapping[str, int], str] | None:
    """Return all six frontier cut fields when completely captured, else ``None``."""
    raw_watermarks = frontier_after.raw_execution_watermarks
    manifest_digest = frontier_after.execution_manifest_digest
    has_fill_gap = frontier_after.has_fill_gap
    expected_venues = frontier_after.expected_venues
    venue_watermarks = frontier_after.venue_event_watermarks
    fill_witness_digest = frontier_after.fill_witness_digest
    if (
        raw_watermarks is None
        or manifest_digest is None
        or has_fill_gap is None
        or expected_venues is None
        or venue_watermarks is None
        or fill_witness_digest is None
    ):
        return None
    return (
        raw_watermarks,
        manifest_digest,
        has_fill_gap,
        expected_venues,
        venue_watermarks,
        fill_witness_digest,
    )


def checkpoint_write_refusal(
    frontier_after: PnlCorrectionFrontier,
    chunk: PnlCorrectionChunk,
    certificate: PnlOpeningCheckpointCertificate,
) -> PnlCheckpointWriteRefusal | None:
    """Evaluate write-eligibility gates in canonical precedence order.

    Args:
        frontier_after: Frontier state produced by advancing ``chunk``.
        chunk: Admitted correction chunk being committed.
        certificate: Candidate opening-checkpoint certificate for ``chunk.end``.

    Returns:
        The first applicable refusal reason code, or ``None`` when the write
        may proceed inside the fenced commit transaction.
    """
    if frontier_after.next_minute is not None or frontier_after.repair_from is not None:
        return "not_settled"
    if not (chunk.end == frontier_after.tip == certificate.checkpoint_minute):
        return "not_at_tip"
    if (
        certificate.source_generation != frontier_after.generation
        or certificate.as_of != frontier_after.as_of
    ):
        return "generation_mismatch"
    frontier_cut = _extract_frontier_cut(frontier_after)
    if frontier_cut is None:
        return "frontier_evidence_incomplete"
    (
        raw_watermarks,
        manifest_digest,
        has_fill_gap,
        expected_venues,
        venue_watermarks,
        fill_witness_digest,
    ) = frontier_cut
    if has_fill_gap:
        return "fill_gap"
    certificate_cut = (
        dict(certificate.cutoff_execution_watermarks),
        certificate.execution_manifest_digest,
        certificate.source_expected_venues,
        dict(certificate.source_venue_event_watermarks),
        certificate.source_fill_witness_digest,
    )
    expected_cut = (
        dict(raw_watermarks),
        manifest_digest,
        expected_venues,
        dict(venue_watermarks),
        fill_witness_digest,
    )
    if certificate_cut != expected_cut:
        return "cut_mismatch"
    if certificate.key != frontier_after.key:
        return "key_mismatch"
    deferred_count = sum(
        len(sequences) for sequences in certificate.deferred_execution_sequences.values()
    )
    if deferred_count > MAX_DEFERRED_EXECUTIONS:
        return "deferred_tail_too_long"
    return None
