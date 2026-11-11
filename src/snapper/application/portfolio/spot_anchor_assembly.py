"""Pure mappers from raw venue reads to the spot-anchor witness and bootstrap.

The account observer gathers raw venue reads (an ``account/history`` tip page and
per-order fill legs) plus the exec-id-decoded sealed prefix, and needs two derived
pieces to certify an anchor: the ``execution_witnesses`` map and the normalized
:class:`VenueHistoryTip` the bootstrap folds. Both derivations are pure, so they
live here — testable without the observer's I/O — and the observer only fills the
remaining directly-observed fields of the observation.
"""

import json
from collections.abc import Mapping
from collections.abc import Sequence
from dataclasses import dataclass
from decimal import Decimal

from snapper.application.portfolio.spot_anchor_bootstrap import VenueHistoryItem
from snapper.application.portfolio.spot_anchor_bootstrap import VenueHistoryTip
from snapper.application.portfolio.spot_anchor_witness import OwnedWitnessScope
from snapper.application.portfolio.spot_anchor_witness import WitnessExecution
from snapper.application.portfolio.spot_anchor_witness import WitnessHistoryLeg
from snapper.application.portfolio.spot_anchor_witness import WitnessOrderTotals
from snapper.application.portfolio.spot_anchor_witness import WitnessOutcome
from snapper.application.portfolio.spot_anchor_witness import build_execution_witnesses
from snapper.application.portfolio.spot_anchor_witness import build_owned_execution_witnesses
from snapper.application.portfolio.spot_anchor_witness import decode_execution_cumulative
from snapper.infrastructure.exchanges.contracts import VenueAccountHistoryTip
from snapper.infrastructure.exchanges.contracts import VenueOrderFillLegs

_MARKET_FX = "MARKET_FX"
_COMMISSION = "COMMISSION"
_API_PREFIX = "API/"


def _is_api_attributed(ordered_by: str) -> bool:
    """Return whether an account-history row was created by our own API key."""
    return ordered_by.startswith(_API_PREFIX)


@dataclass(frozen=True)
class _WitnessInputs:
    """The witness builder's inputs mapped from the raw venue reads."""

    legs: list[WitnessHistoryLeg]
    executions: list[WitnessExecution]
    totals: dict[str, WitnessOrderTotals]
    commission_by_order: dict[str, Decimal]


def _witness_inputs(
    raw_tip: VenueAccountHistoryTip,
    parsed_executions: Sequence[tuple[int, str, int, bool]],
    order_totals: Mapping[str, VenueOrderFillLegs],
) -> _WitnessInputs:
    """Map the raw reads to witness legs, executions, totals, and fee sums.

    Maps the page's MARKET_FX rows to witness legs, the exec-id-decoded prefix to
    witness executions (exact cumulative), the per-order fill legs to witness
    totals, and the page's COMMISSION items to exact per-order fee sums (the
    fee-leg completeness evidence, joined to orders by ``orderId``).

    Args:
        raw_tip: The venue account-history tip and its descending page.
        parsed_executions: The sealed prefix as
            ``(scope_sequence, venue_order_id, cumulative_basis_units, is_terminal)``.
        order_totals: Per venue order id, the market_fx/orders cumulative totals.

    Returns:
        The mapped witness inputs.
    """
    legs = [
        WitnessHistoryLeg(
            item_id=item.item_id,
            order_id=item.order_id,
            transaction_id=item.transaction_id,
            currency=item.currency,
            amount=item.operation_amount,
            is_api_attributed=_is_api_attributed(item.ordered_by),
        )
        for item in raw_tip.items
        if item.operation_type == _MARKET_FX
    ]
    commission_by_order: dict[str, Decimal] = {}
    for item in raw_tip.items:
        if item.operation_type == _COMMISSION and item.order_id is not None:
            commission_by_order[item.order_id] = (
                commission_by_order.get(item.order_id, Decimal(0)) - item.operation_amount
            )
    executions = [
        WitnessExecution(
            scope_sequence=scope_sequence,
            venue_order_id=venue_order_id,
            cumulative=decode_execution_cumulative(basis_units),
            is_terminal=is_terminal,
        )
        for scope_sequence, venue_order_id, basis_units, is_terminal in parsed_executions
    ]
    totals = {
        order_id: WitnessOrderTotals(
            bought_amount=fill_legs.bought_amount,
            sold_amount=fill_legs.sold_amount,
            bought_currency=fill_legs.bought_currency,
            sold_currency=fill_legs.sold_currency,
            is_buy=fill_legs.buy_sell == "BUY",
            commission_amount=fill_legs.commission_amount,
        )
        for order_id, fill_legs in order_totals.items()
    }
    return _WitnessInputs(
        legs=legs, executions=executions, totals=totals, commission_by_order=commission_by_order
    )


def build_witnesses_from_reads(
    raw_tip: VenueAccountHistoryTip,
    parsed_executions: Sequence[tuple[int, str, int, bool]],
    order_totals: Mapping[str, VenueOrderFillLegs],
) -> WitnessOutcome:
    """Compose the execution witness map from the raw venue reads.

    The venue-cursor certificate's composition: its witnessable legs are the
    ones our own API key placed. COMMISSION rows may carry an ``orderedBy`` or
    not; the ``orderId`` join, sourced from our own order reads, is their
    attribution either way.

    Args:
        raw_tip: The venue account-history tip and its descending page.
        parsed_executions: The sealed prefix as
            ``(scope_sequence, venue_order_id, cumulative_basis_units, is_terminal)``.
        order_totals: Per venue order id, the market_fx/orders cumulative totals.

    Returns:
        The witness map, or the ordered refusals when it cannot be composed.
    """
    inputs = _witness_inputs(raw_tip, parsed_executions, order_totals)
    return build_execution_witnesses(
        inputs.executions, inputs.legs, inputs.totals, raw_tip.item_id, inputs.commission_by_order
    )


def build_owned_witnesses_from_reads(
    raw_tip: VenueAccountHistoryTip,
    parsed_executions: Sequence[tuple[int, str, int, bool]],
    order_totals: Mapping[str, VenueOrderFillLegs],
    owned_order_ids: frozenset[str],
    authoritative_order_ids: Mapping[int, str | None],
) -> WitnessOutcome:
    """Compose the anchor bootstrap's witness map from the raw venue reads.

    The bootstrap's composition: its witnessable legs are exactly those whose
    order id lies in the owned order set ``S``, each execution's decoded order
    id must be its Order's authoritative venue order id in ``S``, and every
    owned COMMISSION on the page must be proven against its order's totals and
    witnessed fills.

    Args:
        raw_tip: The seal page's tip and its descending rows.
        parsed_executions: The sealed prefix as
            ``(scope_sequence, venue_order_id, cumulative_basis_units, is_terminal)``.
        order_totals: Per venue order id, the market_fx/orders cumulative totals
            of every executed order and every owned order on the page.
        owned_order_ids: The owned order set ``S``.
        authoritative_order_ids: Per scope sequence, the execution's Order's
            authoritative venue order id, or ``None`` when it does not resolve.

    Returns:
        The witness map, or the ordered refusals when it cannot be composed.
    """
    inputs = _witness_inputs(raw_tip, parsed_executions, order_totals)
    return build_owned_execution_witnesses(
        inputs.executions,
        inputs.legs,
        inputs.totals,
        inputs.commission_by_order,
        OwnedWitnessScope(
            tip_item_id=raw_tip.item_id,
            owned_order_ids=owned_order_ids,
            authoritative_order_ids=authoritative_order_ids,
        ),
    )


def owned_page_order_ids(
    raw_tip: VenueAccountHistoryTip, owned_order_ids: frozenset[str]
) -> frozenset[str]:
    """Return every owned order id that appears on the page as MARKET_FX or COMMISSION.

    The bootstrap reads venue totals for these orders as well as for the
    executed ones, so an owned commission or fill whose order has no ingested
    execution is still checked against the venue's own totals.

    Args:
        raw_tip: The seal page's tip and its descending rows.
        owned_order_ids: The owned order set ``S``.

    Returns:
        The owned order ids present on the page.
    """
    return frozenset(
        item.order_id
        for item in raw_tip.items
        if item.operation_type in (_MARKET_FX, _COMMISSION)
        and item.order_id is not None
        and item.order_id in owned_order_ids
    )


def venue_history_tip_from_raw(raw_tip: VenueAccountHistoryTip) -> VenueHistoryTip:
    """Normalize a raw account-history tip into the bootstrap's fold input.

    Projects each raw row to the certification-plane fields the bootstrap needs:
    the MARKET_FX marker, our-key attribution (evidence only), currency, the
    venue's own post-event balance, and — verbatim — the operation type, the
    order id, and the correcting-entry flag the ownership classification reads.
    The page order (descending) is preserved.

    Args:
        raw_tip: The venue account-history tip and its descending page.

    Returns:
        The normalized tip the bootstrap folds against the local ledger.
    """
    page = tuple(
        VenueHistoryItem(
            item_id=item.item_id,
            is_market_fx=item.operation_type == _MARKET_FX,
            is_api_attributed=_is_api_attributed(item.ordered_by),
            currency=item.currency,
            balance_after=item.balance_after,
            operation_type=item.operation_type,
            order_id=item.order_id,
            correcting_entry=item.correcting_entry,
        )
        for item in raw_tip.items
    )
    return VenueHistoryTip(item_id=raw_tip.item_id, page=page)


def parse_observed_balances(
    balances_json: str,
) -> tuple[dict[str, Decimal], dict[str, Decimal], bool]:
    """Parse the observer's persisted balances into exact totals and reserved.

    The observer serializes each currency's total/free/used with an exact
    ``*_decimal`` mirror when the venue reported faithful decimals. Only those
    venue-raw entries are parsed exactly; any entry lacking the decimal mirror
    marks the whole read not-venue-raw (which the bootstrap then refuses) and is
    skipped, so a legacy-float balance can never seal an anchor.

    Args:
        balances_json: The persisted ``balances_json`` payload.

    Returns:
        The exact per-currency totals, the exact per-currency reserved amounts,
        and whether every parsed entry was venue-raw.
    """
    entries = json.loads(balances_json)
    balances: dict[str, Decimal] = {}
    reserved: dict[str, Decimal] = {}
    venue_raw = bool(entries)
    for entry in entries:
        total_decimal = entry.get("total_decimal")
        used_decimal = entry.get("used_decimal")
        if not isinstance(total_decimal, str) or not isinstance(used_decimal, str):
            venue_raw = False
            continue
        currency = str(entry["currency"])
        balances[currency] = Decimal(total_decimal)
        reserved[currency] = Decimal(used_decimal)
    return balances, reserved, venue_raw
