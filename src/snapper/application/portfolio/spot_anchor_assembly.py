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
from decimal import Decimal

from snapper.application.portfolio.spot_anchor_bootstrap import VenueHistoryItem
from snapper.application.portfolio.spot_anchor_bootstrap import VenueHistoryTip
from snapper.application.portfolio.spot_anchor_witness import WitnessExecution
from snapper.application.portfolio.spot_anchor_witness import WitnessHistoryLeg
from snapper.application.portfolio.spot_anchor_witness import WitnessOrderTotals
from snapper.application.portfolio.spot_anchor_witness import WitnessOutcome
from snapper.application.portfolio.spot_anchor_witness import build_execution_witnesses
from snapper.application.portfolio.spot_anchor_witness import decode_execution_cumulative
from snapper.infrastructure.exchanges.contracts import VenueAccountHistoryTip
from snapper.infrastructure.exchanges.contracts import VenueOrderFillLegs

_MARKET_FX = "MARKET_FX"
_API_PREFIX = "API/"


def _is_api_attributed(ordered_by: str) -> bool:
    """Return whether an account-history row was created by our own API key."""
    return ordered_by.startswith(_API_PREFIX)


def build_witnesses_from_reads(
    raw_tip: VenueAccountHistoryTip,
    parsed_executions: Sequence[tuple[int, str, int, bool]],
    order_totals: Mapping[str, VenueOrderFillLegs],
) -> WitnessOutcome:
    """Compose the execution witness map from the raw venue reads.

    Maps the tip page's MARKET_FX rows to witness legs, the exec-id-decoded
    prefix to witness executions (exact cumulative), and the per-order fill legs
    to witness totals, then defers to the pure witness builder.

    Args:
        raw_tip: The venue account-history tip and its descending page.
        parsed_executions: The sealed prefix as
            ``(scope_sequence, venue_order_id, cumulative_basis_units, is_terminal)``.
        order_totals: Per venue order id, the market_fx/orders cumulative totals.

    Returns:
        The witness map, or the ordered refusals when it cannot be composed.
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
        )
        for order_id, fill_legs in order_totals.items()
    }
    return build_execution_witnesses(executions, legs, totals, raw_tip.item_id)


def venue_history_tip_from_raw(raw_tip: VenueAccountHistoryTip) -> VenueHistoryTip:
    """Normalize a raw account-history tip into the bootstrap's fold input.

    Projects each raw row to the certification-plane fields the bootstrap needs:
    the MARKET_FX marker, our-key attribution, currency, and the venue's own
    post-event balance. The page order (descending) is preserved.

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
