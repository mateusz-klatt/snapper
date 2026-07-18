"""Pure venue-cursor range certificate for anchored walutomat spot accounts.

The module decides whether the venue ``account/history`` range
``(H_anchor, H_E]``, captured by the account observer for one anchored
reconciliation boundary, proves that the venue ledger and the local replay
agree fill-for-fill — the matched gate behind
``SpotReplayBoundary.venue_cursor_certified``. It performs no I/O, holds no
clock, and mutates no input; all arithmetic is exact ``Decimal``. Every check
runs (only absent evidence short-circuits, since nothing else is checkable
without it), the refusal union is closed and alphabetically sorted, and
anything unproven fails closed.

Attribution doctrine (production facts): venue COMMISSION rows carry NO
``orderedBy`` — their attribution is the ``orderId`` join against the read
order totals, so a COMMISSION row whose ``orderId`` is outside the totals
refuses ``venue_history_manual_unattributed``. A MARKET_FX row not covered by
the composed witness map refuses ``venue_history_manual_unattributed``
REGARDLESS of its ``orderedBy``: coverage by the fill-identity composition is
the only accepted attribution. External operations (PAYIN, PAYOUT, TRANSFER,
DIRECT_FX, anything beyond MARKET_FX/COMMISSION) and correcting entries refuse
``venue_history_unsupported_operation`` — the Phase-5 rebase owns them.
Detailed witness-composition refusals collapse to the single dispatch-layer
name ``venue_history_execution_unmapped``; the caller logs the certificate
refusals it receives.

STRADDLING-ORDER DEPENDENCY (load-bearing): the witness composition
cross-checks the in-range fill legs against ``market_fx/orders`` LIFETIME
cumulative totals. That is sound only because the anchor bootstrap's
``venue_reserved_funds_present`` refusal proves no order was open when the
anchor sealed: every order with fills inside ``(H_anchor, H_E]`` was therefore
created after the anchor, so its lifetime cumulatives equal its in-range
cumulatives. An order straddling the anchor boundary would break that
equality — the anchor refusal, not this module, is what excludes it.

WINDOWED SCOPING: every proof is scoped to the delivered window
``(H_anchor, H_E]``. Pre-anchor venue activity is unobserved by design — its
balance effect is inside the sealed anchor balances (the anchor's
``:windowed`` claim) — and genesis reach is never assumed, so rows at or below
``H_anchor`` refuse as outside the certified window rather than being
silently absorbed.
"""

import hashlib
import json
from collections.abc import Mapping
from collections.abc import Sequence
from dataclasses import dataclass
from decimal import Decimal
from typing import Literal

from snapper.application.portfolio.spot_anchor_assembly import build_witnesses_from_reads
from snapper.application.portfolio.spot_anchor_bootstrap import _VENUE_CURSOR_SCHEMES
from snapper.application.portfolio.spot_anchor_witness import witness_bijection_holds
from snapper.application.portfolio.spot_anchor_witness import witness_reverse_coverage_holds
from snapper.data.repository_types import SpotReconciliationAnchorRow
from snapper.infrastructure.exchanges.contracts import VenueAccountHistoryItem
from snapper.infrastructure.exchanges.contracts import VenueAccountHistoryTip
from snapper.infrastructure.exchanges.contracts import VenueOrderFillLegs

type HistoryCertificateRefusal = Literal[
    "venue_cursor_malformed",
    "venue_cursor_regressed",
    "venue_cursor_scheme_unregistered",
    "venue_cursor_unavailable",
    "venue_history_advanced",
    "venue_history_balance_chain_mismatch",
    "venue_history_execution_unmapped",
    "venue_history_manual_unattributed",
    "venue_history_range_incomplete",
    "venue_history_unavailable",
    "venue_history_unsupported_operation",
    "watermark_advanced",
]

_VENUE_CURSOR_KIND = "account_history_item_id"
_MARKET_FX = "MARKET_FX"
_COMMISSION = "COMMISSION"
_SUPPORTED_OPERATIONS = frozenset({_MARKET_FX, _COMMISSION})
_ZERO = Decimal(0)


@dataclass(frozen=True)
class HistoryRangeEvidence:
    """The captured venue account-history range evidence for one boundary.

    ``tip_item_id`` is the pre-watermark history tip ``H_E``;
    ``confirming_tip_item_id`` the post-evidence confirming re-read (None when
    that read failed); ``rows`` the delivered range pages in ascending item-id
    order, expected to cover exactly ``(H_anchor, H_E]``.
    """

    tip_item_id: int
    confirming_tip_item_id: int | None
    rows: tuple[VenueAccountHistoryItem, ...]


@dataclass(frozen=True)
class SpotHistoryRangeCapture:
    """One observation cycle's complete anchored-path certificate evidence.

    Assembled by the account observer inside the SAME cycle that captured the
    reconciliation boundary, so dispatch stays pure and DB-only: ``evidence``
    is the captured venue range for the certificate, ``parsed_executions`` the
    exec-id-decoded replay rows in ``(W_anchor, W_E]``, ``order_totals`` the
    per-order lifetime cumulative fill legs for the distinct in-range orders,
    and ``venue_balances`` the exact whole venue balance read of the attempt
    the boundary brackets. ``anchor_watermark`` is the sealed anchor watermark
    ``W_anchor`` the witness read was scoped from; dispatch refuses to run the
    certificate when it disagrees with the bundle's anchor epoch, so evidence
    gathered against one anchor can never certify another.
    """

    evidence: HistoryRangeEvidence
    parsed_executions: tuple[tuple[int, str, int, bool], ...]
    order_totals: Mapping[str, VenueOrderFillLegs]
    venue_balances: Mapping[str, Decimal]
    anchor_watermark: int


@dataclass(frozen=True)
class CertificateOutcome:
    """The certificate verdict: certified with a cursor, or the named refusals.

    ``venue_cursor`` is present exactly when ``certified``; ``refusals`` is the
    ordered tuple of every applicable reason (empty when certified).
    """

    certified: bool
    venue_cursor: str | None
    refusals: tuple[HistoryCertificateRefusal, ...]


def _anchor_cursor_refusals(
    anchor: SpotReconciliationAnchorRow, tip_item_id: int
) -> tuple[set[HistoryCertificateRefusal], int | None]:
    """Self-validate the anchor's sealed venue cursor against the observed tip.

    The evaluator never reads the ``venue_cursor_*`` columns, so this check is
    the only post-0031 coherence authority over the sealed cursor: the kind
    must be the registered item-id kind, the scheme must be registered, the
    value must be an ASCII-digits positive integer (unicode digits ``int``
    would accept are refused), and ``H_anchor`` must not exceed ``H_E``.
    Returns the refusals plus the parsed anchor item id (None when malformed).
    """
    refusals: set[HistoryCertificateRefusal] = set()
    if anchor["venue_cursor_kind"] != _VENUE_CURSOR_KIND:
        refusals.add("venue_cursor_malformed")
    if anchor["venue_cursor_scheme"] not in _VENUE_CURSOR_SCHEMES.values():
        refusals.add("venue_cursor_scheme_unregistered")
    value = anchor["venue_cursor_value"]
    anchor_item_id: int | None = None
    if value.isascii() and value.isdigit() and int(value) > 0:
        anchor_item_id = int(value)
    else:
        refusals.add("venue_cursor_malformed")
    if anchor_item_id is not None and anchor_item_id > tip_item_id:
        refusals.add("venue_cursor_regressed")
    return refusals, anchor_item_id


def _range_proof_refusals(
    rows: Sequence[VenueAccountHistoryItem],
    anchor_item_id: int | None,
    tip_item_id: int,
) -> set[HistoryCertificateRefusal]:
    """Return the closed-range ASC pagination-proof refusals.

    Row ids must be strictly increasing, every id must lie in
    ``(H_anchor, H_E]``, a non-empty range must reach exactly ``H_E``, and an
    advanced tip with no rows is a short delivery. The lower-bound and
    empty-range checks are skipped when the anchor cursor did not parse — the
    malformed-cursor refusal already blocks certification.
    """
    refusals: set[HistoryCertificateRefusal] = set()
    ids = [row.item_id for row in rows]
    if any(ids[index] >= ids[index + 1] for index in range(len(ids) - 1)):
        refusals.add("venue_history_range_incomplete")
    if anchor_item_id is not None and any(item_id <= anchor_item_id for item_id in ids):
        refusals.add("venue_history_range_incomplete")
    if any(item_id > tip_item_id for item_id in ids):
        refusals.add("venue_history_range_incomplete")
    if ids and ids[-1] != tip_item_id:
        refusals.add("venue_history_range_incomplete")
    if not ids and anchor_item_id is not None and tip_item_id > anchor_item_id:
        refusals.add("venue_history_range_incomplete")
    return refusals


def _taxonomy_refusals(
    rows: Sequence[VenueAccountHistoryItem],
) -> set[HistoryCertificateRefusal]:
    """Return the unsupported-operation refusals for the delivered range.

    Correcting entries and every operation type beyond MARKET_FX/COMMISSION
    (PAYIN, PAYOUT, TRANSFER, DIRECT_FX, anything else) are external flows this
    certificate cannot attribute to the replay; the Phase-5 rebase owns them.
    """
    refusals: set[HistoryCertificateRefusal] = set()
    for row in rows:
        if row.correcting_entry or row.operation_type not in _SUPPORTED_OPERATIONS:
            refusals.add("venue_history_unsupported_operation")
    return refusals


def _attribution_refusals(
    rows: Sequence[VenueAccountHistoryItem],
    witnesses: Mapping[int, frozenset[int]],
    order_totals: Mapping[str, VenueOrderFillLegs],
) -> set[HistoryCertificateRefusal]:
    """Return the manual-attribution refusals over the composed witness map.

    Coverage by the witness map is the only accepted MARKET_FX attribution: the
    witnessed item ids must be exactly the in-range MARKET_FX ids, so an
    uncovered MARKET_FX row refuses REGARDLESS of its ``orderedBy``. Venue
    COMMISSION rows carry no ``orderedBy`` at all — their attribution is the
    ``orderId`` join, so a fee row outside the read order totals refuses too.
    """
    refusals: set[HistoryCertificateRefusal] = set()
    market_fx_ids = [row.item_id for row in rows if row.operation_type == _MARKET_FX]
    if not witness_bijection_holds(witnesses, market_fx_ids):
        refusals.add("venue_history_manual_unattributed")
    for row in rows:
        if row.operation_type != _COMMISSION:
            continue
        if row.order_id is None or row.order_id not in order_totals:
            refusals.add("venue_history_manual_unattributed")
    return refusals


def _fold_refusals(
    rows: Sequence[VenueAccountHistoryItem],
    seed: Mapping[str, Decimal],
    venue_balances: Mapping[str, Decimal],
) -> set[HistoryCertificateRefusal]:
    """Return the balance-chain refusals from folding the range over the seed.

    Seeded from the anchor's sealed balances, each row's post-event balance
    must equal the running fold, and the terminal fold must equal the venue's
    WHOLE balance read: currencies absent from either side default to zero, so
    a venue-only currency or a nonzero final for a venue-missing currency both
    mismatch. An empty range therefore requires venue == anchor seed.
    """
    refusals: set[HistoryCertificateRefusal] = set()
    running = dict(seed)
    for row in rows:
        updated = running.get(row.currency, _ZERO) + row.operation_amount
        running[row.currency] = updated
        if updated != row.balance_after:
            refusals.add("venue_history_balance_chain_mismatch")
    for currency in sorted(set(running) | set(venue_balances)):
        if running.get(currency, _ZERO) != venue_balances.get(currency, _ZERO):
            refusals.add("venue_history_balance_chain_mismatch")
    return refusals


def _anchor_seed_balances(balances_json: str) -> dict[str, Decimal]:
    """Parse the anchor's sealed canonical balances into exact fold seeds."""
    parsed = json.loads(balances_json)
    return {str(currency): Decimal(str(amount)) for currency, amount in parsed.items()}


def _certified_cursor(anchor: SpotReconciliationAnchorRow, evidence: HistoryRangeEvidence) -> str:
    """Derive the certified venue cursor: scheme, tip id, and range digest.

    The digest is the SHA-256 of the compact canonical JSON of the folded
    range — ``[[item_id, currency, operation_amount, balance_after], ...]`` in
    delivered ascending order — so the persisted cursor commits to the exact
    evidence that certified it (the empty range digests ``[]``).
    """
    canonical = json.dumps(
        [
            [row.item_id, row.currency, str(row.operation_amount), str(row.balance_after)]
            for row in evidence.rows
        ],
        allow_nan=False,
        separators=(",", ":"),
    )
    digest = hashlib.sha256(canonical.encode("utf-8")).hexdigest()
    return f"{anchor['venue_cursor_scheme']}:{evidence.tip_item_id}:{digest}"


def certify_walutomat_history_range(
    evidence: HistoryRangeEvidence | None,
    anchor: SpotReconciliationAnchorRow,
    venue_balances: Mapping[str, Decimal],
    parsed_executions: Sequence[tuple[int, str, int, bool]],
    order_totals: Mapping[str, VenueOrderFillLegs],
    watermark_unchanged: bool,
    boundary_watermark: int,
) -> CertificateOutcome:
    """Certify the venue-cursor range, or name every reason it cannot be.

    Runs every check — anchor cursor self-validation, tip double-read,
    watermark stability, closed-range ASC pagination proof, operation
    taxonomy, witness composition with the shared bijection authorities, and
    the balance-chain fold — accumulating all applicable refusals (only absent
    evidence short-circuits). The refusal tuple is ordered by the closed union
    so the failure order is a stable API.

    Args:
        evidence: The captured range evidence, or None when any read failed.
        anchor: The sealed spot reconciliation anchor row.
        venue_balances: The venue's whole exact balance read at the boundary.
        parsed_executions: The exec-id-decoded replay rows in
            ``(W_anchor, W_E]`` as ``(scope_sequence, venue_order_id,
            cumulative_basis_units, is_terminal)``.
        order_totals: Per venue order id, the ``market_fx/orders`` lifetime
            cumulative totals for the distinct in-range orders.
        watermark_unchanged: Whether the committed watermark held still across
            the boundary evidence reads.
        boundary_watermark: The boundary watermark ``W_E``.

    Returns:
        The certificate outcome: certified with the derived venue cursor, or
        uncertified with the exact ordered refusal tuple.
    """
    if evidence is None:
        return CertificateOutcome(
            certified=False, venue_cursor=None, refusals=("venue_history_unavailable",)
        )
    tip_item_id = evidence.tip_item_id
    refusals, anchor_item_id = _anchor_cursor_refusals(anchor, tip_item_id)
    confirming = evidence.confirming_tip_item_id
    if confirming is None:
        refusals.add("venue_cursor_unavailable")
    else:
        if confirming < tip_item_id:
            refusals.add("venue_cursor_regressed")
        if confirming != tip_item_id:
            refusals.add("venue_history_advanced")
    if not watermark_unchanged:
        refusals.add("watermark_advanced")
    refusals |= _range_proof_refusals(evidence.rows, anchor_item_id, tip_item_id)
    refusals |= _taxonomy_refusals(evidence.rows)
    witness_outcome = build_witnesses_from_reads(
        VenueAccountHistoryTip(item_id=tip_item_id, items=evidence.rows, reached_genesis=False),
        parsed_executions,
        order_totals,
    )
    if witness_outcome.refusals:
        refusals.add("venue_history_execution_unmapped")
    expected = range(anchor["source_watermark"] + 1, boundary_watermark + 1)
    if not witness_reverse_coverage_holds(witness_outcome.witnesses, expected):
        refusals.add("venue_history_execution_unmapped")
    refusals |= _attribution_refusals(evidence.rows, witness_outcome.witnesses, order_totals)
    seed = _anchor_seed_balances(anchor["balances_json"])
    refusals |= _fold_refusals(evidence.rows, seed, venue_balances)
    if refusals:
        return CertificateOutcome(
            certified=False, venue_cursor=None, refusals=tuple(sorted(refusals))
        )
    return CertificateOutcome(
        certified=True, venue_cursor=_certified_cursor(anchor, evidence), refusals=()
    )
