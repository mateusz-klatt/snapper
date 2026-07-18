"""Pure certification authority for the spot reconciliation bootstrap anchor.

The module decides whether one observed account state may be sealed into an
immutable ``portfolio_spot_reconciliation_anchors`` row. It performs no I/O,
holds no clock, and mutates no input: the caller gathers every piece of
evidence (two balance reads, two venue history tip reads, the committed
watermark and its chain tip, the read-order instants, per-asset precision
certification, and the fill-level history witnesses) into one
:class:`SpotAnchorObservation`, and this module returns either the exact set of
named refusals or a fully certified :class:`SpotReconciliationAnchorRow`.

The venue history is consumed in NORMALIZED form
(:class:`VenueHistoryItem` / :class:`VenueHistoryTip`) so the certification
logic is independent of the venue's raw JSON shape: the adapter that reads
``account/history`` owns the raw-to-normalized mapping AND the fill-level
identity join (one venue order can carry several partial-fill executions, so
history entries are matched to local executions by history ``item_id``, never
by order id). The caller supplies which history items are ingested locally and
which witness the watermark execution; this module never collapses that
multiplicity. Every check runs (no short-circuit) so the caller sees all
reasons at once, the reason union is closed and alphabetically sorted, and
anything unproven fails closed. The obligations (O1-O8) are stated in
``plans/map_2026_07_17_s4c3_anchor_bootstrap_v2.md``; the anchor names only what
the venue reports (``inventory_status = 'venue_reported_full'``), never a
full-inventory claim.

CLAIM DOMAIN (window scoping, stated honestly): the un-ingested-fill and
manual-activity checks (O6/O7) are proven over the OBSERVED history window —
``tip_0.page`` — not over the account's whole history. Venue activity older
than the page is unobserved; its balance effect is inside the sealed balances
(replay never re-applies pre-cursor items, so the composed replay arithmetic
stays coherent), but the dedicated-account falsification does not reach it.
``history_window_reached_genesis`` records which strength was proven, and the
anchor's ``provenance`` carries ``:genesis`` (the page held the entire history)
or ``:windowed`` — a durable, per-anchor record of the claim actually made.
"""

import json
from collections.abc import Mapping
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import UTC
from datetime import datetime
from decimal import Decimal
from typing import Literal

from snapper.application.portfolio.spot_anchor_witness import witness_bijection_holds
from snapper.application.portfolio.spot_anchor_witness import witness_reverse_coverage_holds
from snapper.data.repository_types import SpotReconciliationAnchorRow

type SpotAnchorRefusal = Literal[
    "anchor_already_exists",
    "asset_precision_uncertified",
    "balance_read_not_current",
    "balance_reads_disagree",
    "balances_not_venue_raw",
    "boundary_window_inverted",
    "inventory_not_observed",
    "local_execution_not_in_venue_history",
    "margin_not_proven_cash",
    "scope_without_committed_execution",
    "venue_cursor_malformed",
    "venue_cursor_regressed",
    "venue_cursor_scheme_unregistered",
    "venue_cursor_unavailable",
    "venue_fill_not_ingested",
    "venue_history_advanced",
    "venue_history_balance_chain_mismatch",
    "venue_history_manual_unattributed",
    "venue_reserved_funds_present",
    "watermark_advanced",
]

_ANCHOR_BOUNDARY_STATUS = "cursor_certified"
_ANCHOR_INVENTORY_STATUS = "venue_reported_full"
_ANCHOR_MARGIN_STATUS = "cash"
_ANCHOR_WATERMARK_KIND = "scope_sequence"
_VENUE_CURSOR_KIND = "account_history_item_id"
_VENUE_CURSOR_SCHEMES = {"walutomat": "walutomat:api-v2.0.0:account/history:v1"}


@dataclass(frozen=True)
class VenueHistoryItem:
    """One normalized venue account-history entry (one currency leg of a fill).

    ``item_id`` is the venue's monotone, per-entry history id — the unit of the
    fill-level join, since one order carries several partial fills.
    ``is_market_fx`` marks a balance-affecting FX leg; ``is_api_attributed``
    marks it as created by our API key (``orderedBy`` prefixed ``API/``).
    ``balance_after`` is the venue's own post-event balance for ``currency``.
    """

    item_id: int
    is_market_fx: bool
    is_api_attributed: bool
    currency: str
    balance_after: Decimal


@dataclass(frozen=True)
class VenueHistoryTip:
    """The venue history tip id and its first (descending) page."""

    item_id: int
    page: tuple[VenueHistoryItem, ...]


@dataclass(frozen=True)
class SpotAnchorObservation:
    """Every piece of evidence a bootstrap decision needs, gathered by the caller.

    Instants form the ten-instant read-order chain (O4). ``balances_1`` is the
    persisted observer balance read; ``balances_2`` the reconciliation-time
    re-read. ``tip_0`` carries ``H0`` and its page; ``tip_1_item_id`` is the
    confirming ``H1``. ``precision_certified`` maps each balance asset to its
    already-evaluated precision-plane certification. ``execution_witnesses``
    maps EACH sealed-prefix ``scope_sequence`` in ``[1, source_watermark]`` to
    the venue history item ids of that execution's COMPLETE leg set, matched by
    the caller via real fill identity. The module proves reverse coverage plus a
    bijection with the attributed page (every execution witnessed, every
    attributed fill ingested), because ``scope_sequence`` is commit order, not
    venue fill order, so witnessing only the tip execution would miss an earlier
    committed but later-filled execution. The caller MUST certify each witness
    is leg-complete (base, quote, AND fee): a fill whose fee leg is still in
    flight has an incomplete effect the item-id membership alone cannot detect.
    A terminal (order-close) execution owns no new fill and shares its active
    predecessor's legs, so the witnessed item ids are deduplicated before the
    bijection while every execution still maps to a non-empty leg set.
    """

    public_id: str
    wallet_public_id: str
    exchange: str
    mode: str
    anchor_exists: bool
    balance_status: str
    balances_are_venue_raw: bool
    balance_read_bound_to_scope: bool
    venue_account_state_public_id: str
    balance_observation_id: int
    session_id: str
    sequence_id: int
    balances_1: Mapping[str, Decimal]
    balances_2: Mapping[str, Decimal]
    balances_reserved: Mapping[str, Decimal]
    source_watermark: int
    watermark_unchanged: bool
    source_chain_tip: str
    tip_0: VenueHistoryTip | None
    tip_1_item_id: int | None
    history_window_reached_genesis: bool
    precision_certified: Mapping[str, bool]
    margin_signal: bool
    execution_witnesses: Mapping[int, frozenset[int]]
    venue_cursor_requested_at: datetime
    venue_cursor_observed_at: datetime
    venue_cursor_confirmed_at: datetime
    source_watermark_requested_at: datetime
    source_watermark_captured_at: datetime
    first_request_started_at: datetime
    first_request_completed_at: datetime
    second_request_started_at: datetime
    second_request_completed_at: datetime
    timestamp: datetime


class SpotAnchorNotCertifiableError(Exception):
    """Raised by :func:`build_spot_anchor` when any refusal applies.

    Carries the exact ordered refusal tuple so the caller can log the named
    reasons and retry next cycle rather than persist a degraded anchor.
    """

    def __init__(self, refusals: tuple[SpotAnchorRefusal, ...]) -> None:
        """Store the applicable refusals and format the failure message."""
        self.refusals = refusals
        super().__init__(f"spot anchor not certifiable: {list(refusals)}")


def _decimal_string(value: Decimal) -> str:
    """Render a finite non-negative decimal canonically without exponent notation."""
    rendered = format(value, "f")
    if "." in rendered:
        rendered = rendered.rstrip("0").rstrip(".")
    return rendered


def _ordered_instants(observation: SpotAnchorObservation) -> tuple[datetime, ...]:
    """Return the ten read-order instants in the order the chain requires."""
    return (
        observation.venue_cursor_requested_at,
        observation.venue_cursor_observed_at,
        observation.source_watermark_requested_at,
        observation.source_watermark_captured_at,
        observation.first_request_started_at,
        observation.first_request_completed_at,
        observation.second_request_started_at,
        observation.second_request_completed_at,
        observation.venue_cursor_confirmed_at,
        observation.timestamp,
    )


def _boundary_window_inverted(observation: SpotAnchorObservation) -> bool:
    """Return whether the ten-instant chain is tz-naive or not monotone in UTC.

    Instants are normalized to UTC before comparison so a DST fold (where two
    wall-clock instants share one ``tzinfo`` and compare equal despite distinct
    offsets) cannot pass a wall-time comparison the storage layer would reject.
    """
    instants = _ordered_instants(observation)
    if any(instant.utcoffset() is None for instant in instants):
        return True
    utc = [instant.astimezone(UTC) for instant in instants]
    return any(utc[index] > utc[index + 1] for index in range(len(utc) - 1))


def _attributed_fills_at_or_before(
    page: Sequence[VenueHistoryItem], tip: int
) -> list[VenueHistoryItem]:
    """Return the API-attributed MARKET_FX page items with id at or below the tip."""
    return [
        item
        for item in page
        if item.is_market_fx and item.is_api_attributed and item.item_id <= tip
    ]


def _venue_history_balance_chain_mismatch(
    page: Sequence[VenueHistoryItem], balances: Mapping[str, Decimal]
) -> bool:
    """Return whether any currency's most-recent page balance disagrees with the read.

    The page is descending, so the first item seen per currency is its most
    recent. A currency present on the page whose newest ``balance_after`` does
    not equal the observed total falsifies the venue's own balance/history
    atomicity for the only window where non-atomicity is observable.
    """
    seen: set[str] = set()
    for item in page:
        if item.currency in seen:
            continue
        seen.add(item.currency)
        if balances.get(item.currency) != item.balance_after:
            return True
    return False


def spot_anchor_bootstrap_refusals(
    observation: SpotAnchorObservation,
) -> tuple[SpotAnchorRefusal, ...]:
    """Return every named reason this observation cannot be sealed, or an empty tuple.

    Runs every check; the result is ordered by the closed reason union so the
    failure order is a stable API. An empty tuple means the observation is
    certifiable and :func:`build_spot_anchor` will produce a row.

    Args:
        observation: The gathered bootstrap evidence.

    Returns:
        The ordered tuple of applicable refusal names (empty when certifiable).
    """
    refusals: set[SpotAnchorRefusal] = set()
    tip = observation.tip_0
    watermark = observation.source_watermark

    if observation.anchor_exists:
        refusals.add("anchor_already_exists")
    if any(
        not observation.precision_certified.get(asset, False) for asset in observation.balances_1
    ):
        refusals.add("asset_precision_uncertified")
    if not observation.balance_read_bound_to_scope:
        refusals.add("balance_read_not_current")
    if not observation.balances_are_venue_raw:
        refusals.add("balances_not_venue_raw")
    if _boundary_window_inverted(observation):
        refusals.add("boundary_window_inverted")
    if observation.balance_status != "observed" or not observation.balances_1:
        refusals.add("inventory_not_observed")
    if observation.margin_signal:
        refusals.add("margin_not_proven_cash")
    if watermark == 0:
        refusals.add("scope_without_committed_execution")
    if observation.exchange not in _VENUE_CURSOR_SCHEMES:
        refusals.add("venue_cursor_scheme_unregistered")
    if any(reserved != 0 for reserved in observation.balances_reserved.values()):
        refusals.add("venue_reserved_funds_present")
    if not observation.watermark_unchanged:
        refusals.add("watermark_advanced")

    if tip is None or not tip.page or observation.tip_1_item_id is None:
        refusals.add("venue_cursor_unavailable")
    if tip is not None and observation.tip_1_item_id is not None:
        if observation.tip_1_item_id < tip.item_id:
            refusals.add("venue_cursor_regressed")
        if observation.tip_1_item_id != tip.item_id:
            refusals.add("venue_history_advanced")
        elif observation.balances_1 != observation.balances_2:
            refusals.add("balance_reads_disagree")
    if tip is not None:
        if tip.item_id <= 0:
            refusals.add("venue_cursor_malformed")
        if any(item.is_market_fx and not item.is_api_attributed for item in tip.page):
            refusals.add("venue_history_manual_unattributed")
        if _venue_history_balance_chain_mismatch(tip.page, observation.balances_1):
            refusals.add("venue_history_balance_chain_mismatch")
        attributed_ids = sorted(
            item.item_id for item in _attributed_fills_at_or_before(tip.page, tip.item_id)
        )
        witnesses = observation.execution_witnesses
        if not witness_reverse_coverage_holds(witnesses, range(1, watermark + 1)):
            refusals.add("local_execution_not_in_venue_history")
        if not witness_bijection_holds(witnesses, attributed_ids):
            refusals.add("venue_fill_not_ingested")

    return tuple(sorted(refusals))


def build_spot_anchor(observation: SpotAnchorObservation) -> SpotReconciliationAnchorRow:
    """Return the certified anchor row, or raise with every applicable refusal.

    Args:
        observation: The gathered bootstrap evidence.

    Returns:
        The fully certified immutable anchor row.

    Raises:
        SpotAnchorNotCertifiableError: If any named refusal applies.
    """
    tip = observation.tip_0
    scheme = _VENUE_CURSOR_SCHEMES.get(observation.exchange)
    if tip is None or scheme is None:
        raise SpotAnchorNotCertifiableError(spot_anchor_bootstrap_refusals(observation))
    refusals = spot_anchor_bootstrap_refusals(observation)
    if refusals:
        raise SpotAnchorNotCertifiableError(refusals)
    balances_json = json.dumps(
        {asset: _decimal_string(amount) for asset, amount in observation.balances_1.items()},
        allow_nan=False,
        separators=(",", ":"),
        sort_keys=True,
    )
    window = "genesis" if observation.history_window_reached_genesis else "windowed"
    return {
        "public_id": observation.public_id,
        "wallet_public_id": observation.wallet_public_id,
        "exchange": observation.exchange,
        "mode": observation.mode,
        "venue_account_state_public_id": observation.venue_account_state_public_id,
        "balance_observation_id": observation.balance_observation_id,
        "source_watermark_kind": _ANCHOR_WATERMARK_KIND,
        "source_watermark": observation.source_watermark,
        "balances_json": balances_json,
        "first_request_started_at": observation.first_request_started_at,
        "first_request_completed_at": observation.first_request_completed_at,
        "second_request_started_at": observation.second_request_started_at,
        "second_request_completed_at": observation.second_request_completed_at,
        "boundary_status": _ANCHOR_BOUNDARY_STATUS,
        "inventory_status": _ANCHOR_INVENTORY_STATUS,
        "margin_status": _ANCHOR_MARGIN_STATUS,
        "provenance": f"spot_anchor_bootstrap:v1:{scheme}:{window}",
        "session_id": observation.session_id,
        "sequence_id": observation.sequence_id,
        "timestamp": observation.timestamp,
        "source_chain_tip": observation.source_chain_tip,
        "venue_cursor_kind": _VENUE_CURSOR_KIND,
        "venue_cursor_scheme": scheme,
        "venue_cursor_value": str(tip.item_id),
        "venue_cursor_requested_at": observation.venue_cursor_requested_at,
        "venue_cursor_observed_at": observation.venue_cursor_observed_at,
        "venue_cursor_confirmed_at": observation.venue_cursor_confirmed_at,
        "source_watermark_requested_at": observation.source_watermark_requested_at,
        "source_watermark_captured_at": observation.source_watermark_captured_at,
    }
