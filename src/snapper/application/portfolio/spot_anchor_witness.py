"""Pure fill-identity witness builder for the spot reconciliation anchor.

The spot-anchor bootstrap (:mod:`snapper.application.portfolio.spot_anchor_bootstrap`)
needs, for every local execution in the sealed prefix ``[1, W]``, the set of venue
``account/history`` MARKET_FX item ids that constitute that execution's complete
leg set (``execution_witnesses``). Walutomat exposes NO per-fill id — only an
order's running cumulative — so this module reconstructs the mapping by
composition, and it does so PURELY: no I/O, no clock, exact ``Decimal`` only.

The composition, and why each step is sound (validated against an adversarial
review; see ``plans/map_2026_07_17_s4c3_anchor_bootstrap_v2.md`` and the S4c-3
consensus notes):

- Each local ``Execution.exec_id`` encodes ``(venue_order_id, cumulative)`` where
  ``cumulative`` is the venue's OWN reported filled amount (``boughtAmount`` for a
  buy, ``soldAmount`` for a sell — always the base-currency volume) at the instant
  the poller committed that execution. It is immutable and captured at COMMIT
  time, so it is an independent, race-free anchor for the sealed prefix — distinct
  from the account-history read taken later. The cumulative is decoded EXACTLY
  (``Decimal(basis_units) / Decimal(100_000_000)``), never through a float, so the
  cross-check stays exact.
- Per order, the attributed MARKET_FX history legs are grouped by
  ``transaction_id`` (shared across a fill's two currency legs) into fills, ordered
  by item id (chronological), and their base-currency legs accumulate a running
  cumulative. A dropped or extra venue leg makes that running cumulative miss an
  execution's boundary, so the fill would forge NEITHER a false witness map (the
  boundary check fails) NOR pass the independent ``market_fx/orders`` total
  cross-check (the base legs no longer sum to the order's reported total).
- Each execution's ``cumulative`` MUST equal a fill's running cumulative (a fill
  boundary); the fills in ``(c_prev, c_k]`` are that execution's witnesses. Because
  the cumulative is monotone, one poll snapshot legitimately spanning several venue
  fills is handled: the execution sweeps every fill in its interval.
- A terminal (``-t``) execution repeats its predecessor's cumulative (zero delta,
  an order-close marker, not a new fill); it SHARES its active predecessor's
  witness legs, and the bootstrap deduplicates the witnessed item ids so the
  sharing does not double-count.

Every check runs (no short-circuit) so the caller sees all reasons at once; the
refusal union is closed and alphabetically sorted; anything unproven fails closed
by returning refusals and no map.
"""

from collections.abc import Mapping
from collections.abc import Sequence
from dataclasses import dataclass
from decimal import Decimal
from itertools import chain
from typing import Literal

type WitnessRefusal = Literal[
    "commission_sum_disagrees_with_order_total",
    "execution_cumulative_off_fill_boundary",
    "execution_order_totals_missing",
    "history_fill_leg_incomplete",
    "history_leg_unidentified",
    "non_terminal_cumulative_collision",
    "order_base_legs_disagree_with_total",
    "unassigned_venue_fill",
]

_EXEC_ID_BASIS = Decimal(100_000_000)


@dataclass(frozen=True)
class WitnessExecution:
    """One sealed-prefix execution, decoded from its venue exec id.

    ``cumulative`` is the venue's reported base-currency filled amount at commit
    (exact ``Decimal``); ``is_terminal`` marks the disappeared-order ``-t``
    emission, which repeats the last active cumulative and owns no new fill.
    """

    scope_sequence: int
    venue_order_id: str
    cumulative: Decimal
    is_terminal: bool


@dataclass(frozen=True)
class WitnessHistoryLeg:
    """One MARKET_FX account-history leg the witness join consumes.

    ``amount`` is the signed exact-decimal balance delta for ``currency``;
    ``transaction_id`` groups a fill's two currency legs and ``order_id``
    correlates every leg of one order. ``is_api_attributed`` marks our own key
    (an ``API/`` ``orderedBy`` prefix); ids and the tip bound select the same
    attributed set the bootstrap proves the bijection over.
    """

    item_id: int
    order_id: str | None
    transaction_id: str | None
    currency: str
    amount: Decimal
    is_api_attributed: bool


@dataclass(frozen=True)
class WitnessOrderTotals:
    """One order's cumulative venue totals, the independent composition anchor.

    ``bought_amount`` / ``sold_amount`` are the exact gross cumulative legs in
    ``bought_currency`` / ``sold_currency``; ``is_buy`` selects which one
    denominates the executions' base volume (buy → bought, sell → sold).
    ``commission_amount`` is the order's cumulative fee — the independent total
    the witnessed COMMISSION history items must sum to exactly, which is how the
    fee-leg completeness obligation is DISCHARGED IN CODE (an in-flight fee item
    makes the sum fall short and the composition refuses).
    """

    bought_amount: Decimal
    sold_amount: Decimal
    bought_currency: str
    sold_currency: str
    is_buy: bool
    commission_amount: Decimal


@dataclass(frozen=True)
class WitnessOutcome:
    """The witness map, or the exact ordered reasons it could not be built.

    ``witnesses`` is empty whenever ``refusals`` is non-empty — the builder never
    emits a partial or guessed map.
    """

    witnesses: Mapping[int, frozenset[int]]
    refusals: tuple[WitnessRefusal, ...]


@dataclass(frozen=True)
class _Fill:
    """One venue fill: its running base cumulative and its leg item ids."""

    running_cumulative: Decimal
    item_ids: frozenset[int]


def _order_fills(
    order_legs: Sequence[WitnessHistoryLeg], base_currency: str, quote_currency: str
) -> tuple[tuple[_Fill, ...], bool]:
    """Group one order's legs into chronological fills with running base cumulatives.

    Returns the fills (ordered by item id) and whether every fill carried EXACTLY
    its two currency legs — one base and one quote. A fill missing either leg
    (in flight, dropped, or cut off by the page window) is a composition failure:
    certifying it would seal balances whose missing leg posts later as a
    post-cursor item the replay would double-apply.

    Args:
        order_legs: The attributed MARKET_FX legs of one order.
        base_currency: The order's base currency (buy → bought, sell → sold).
        quote_currency: The order's opposite currency (buy → sold, sell → bought).

    Returns:
        The ordered fills and a completeness flag.
    """
    legs_by_transaction: dict[str, list[WitnessHistoryLeg]] = {}
    for leg in order_legs:
        legs_by_transaction.setdefault(str(leg.transaction_id), []).append(leg)
    grouped: list[tuple[int, Decimal, frozenset[int]]] = []
    complete = True
    for transaction_legs in legs_by_transaction.values():
        base_legs = [leg for leg in transaction_legs if leg.currency == base_currency]
        quote_legs = [leg for leg in transaction_legs if leg.currency == quote_currency]
        if len(base_legs) != 1 or len(quote_legs) != 1 or len(transaction_legs) != 2:
            complete = False
            continue
        base_volume = abs(base_legs[0].amount)
        item_ids = frozenset(leg.item_id for leg in transaction_legs)
        newest_item_id = max(leg.item_id for leg in transaction_legs)
        grouped.append((newest_item_id, base_volume, item_ids))
    grouped.sort(key=lambda entry: entry[0])
    running = Decimal(0)
    fills: list[_Fill] = []
    for _newest, base_volume, item_ids in grouped:
        running += base_volume
        fills.append(_Fill(running_cumulative=running, item_ids=item_ids))
    return tuple(fills), complete


def _assign_order_witnesses(
    order_executions: Sequence[WitnessExecution],
    fills: Sequence[_Fill],
) -> tuple[dict[int, frozenset[int]], set[WitnessRefusal]]:
    """Assign one order's fills to its executions by monotone cumulative interval.

    Each active execution's cumulative must land on a fill boundary; the fills in
    ``(c_prev, c_k]`` become its witnesses. A terminal execution repeating the
    previous cumulative shares that witness set. Any fill left beyond the last
    execution is an un-ingested venue fill.

    Args:
        order_executions: The order's sealed-prefix executions.
        fills: The order's chronological fills with running base cumulatives.

    Returns:
        The per-scope witness item ids assigned, and any refusals raised.
    """
    refusals: set[WitnessRefusal] = set()
    boundaries = {fill.running_cumulative for fill in fills}
    ordered = sorted(
        order_executions, key=lambda execution: (execution.cumulative, execution.is_terminal)
    )
    witnesses: dict[int, frozenset[int]] = {}
    previous_cumulative = Decimal(0)
    previous_witness: frozenset[int] = frozenset()
    fill_index = 0
    for execution in ordered:
        if execution.cumulative > previous_cumulative:
            if execution.cumulative not in boundaries:
                refusals.add("execution_cumulative_off_fill_boundary")
                continue
            assigned: set[int] = set()
            while (
                fill_index < len(fills)
                and fills[fill_index].running_cumulative <= execution.cumulative
            ):
                assigned |= fills[fill_index].item_ids
                fill_index += 1
            witnesses[execution.scope_sequence] = frozenset(assigned)
            previous_cumulative = execution.cumulative
            previous_witness = frozenset(assigned)
        elif execution.is_terminal:
            witnesses[execution.scope_sequence] = previous_witness
        else:
            refusals.add("non_terminal_cumulative_collision")
    if fill_index < len(fills):
        refusals.add("unassigned_venue_fill")
    return witnesses, refusals


def _group_witness_inputs(
    executions: Sequence[WitnessExecution],
    legs: Sequence[WitnessHistoryLeg],
) -> tuple[dict[str, list[WitnessExecution]], dict[str, list[WitnessHistoryLeg]]]:
    """Group local executions and identified history legs by venue order."""
    executions_by_order: dict[str, list[WitnessExecution]] = {}
    for execution in executions:
        executions_by_order.setdefault(execution.venue_order_id, []).append(execution)
    legs_by_order: dict[str, list[WitnessHistoryLeg]] = {}
    for leg in legs:
        legs_by_order.setdefault(str(leg.order_id), []).append(leg)
    return executions_by_order, legs_by_order


def _build_order_witnesses(
    order_id: str,
    order_executions: Sequence[WitnessExecution],
    order_totals: Mapping[str, WitnessOrderTotals],
    order_legs: Sequence[WitnessHistoryLeg],
    commission_by_order: Mapping[str, Decimal],
) -> tuple[dict[int, frozenset[int]], set[WitnessRefusal]]:
    """Build witnesses or one fail-closed refusal set for a venue order."""
    totals = order_totals.get(order_id)
    if totals is None:
        return {}, {"execution_order_totals_missing"}
    base_currency = totals.bought_currency if totals.is_buy else totals.sold_currency
    quote_currency = totals.sold_currency if totals.is_buy else totals.bought_currency
    base_total = totals.bought_amount if totals.is_buy else totals.sold_amount
    fills, complete = _order_fills(order_legs, base_currency, quote_currency)
    if not complete:
        return {}, {"history_fill_leg_incomplete"}
    realized = fills[-1].running_cumulative if fills else Decimal(0)
    if realized != base_total:
        return {}, {"order_base_legs_disagree_with_total"}
    if commission_by_order.get(order_id, Decimal(0)) != totals.commission_amount:
        return {}, {"commission_sum_disagrees_with_order_total"}
    return _assign_order_witnesses(order_executions, fills)


def build_execution_witnesses(
    executions: Sequence[WitnessExecution],
    legs: Sequence[WitnessHistoryLeg],
    order_totals: Mapping[str, WitnessOrderTotals],
    tip_item_id: int,
    commission_by_order: Mapping[str, Decimal],
) -> WitnessOutcome:
    """Compose the sealed-prefix execution-to-history-item witness map.

    Runs every check; on any refusal returns an empty map with the exact ordered
    reasons, never a partial map. On success returns one witness item-id set per
    execution ``scope_sequence`` present in ``executions``. Leg completeness is
    ENFORCED, not assumed: every fill must carry exactly its base and quote legs,
    and every order's witnessed COMMISSION items must sum exactly to the order's
    reported cumulative commission — an in-flight quote or fee leg refuses.

    Args:
        executions: The sealed-prefix executions ``[1, W]``, exec-id decoded.
        legs: The venue MARKET_FX account-history legs (pre-filtered to MARKET_FX
            by the caller); attribution and the tip bound are applied here.
        order_totals: Per venue order id, the ``market_fx/orders`` cumulative
            totals used as the independent leg-sum cross-checks.
        tip_item_id: The venue history tip ``H0``; legs above it are excluded.
        commission_by_order: Per venue order id, the exact sum of the COMMISSION
            history items on the observed page (absent → zero).

    Returns:
        The witness map, or the ordered refusals when it cannot be composed.
    """
    refusals: set[WitnessRefusal] = set()
    attributed = [leg for leg in legs if leg.is_api_attributed and leg.item_id <= tip_item_id]
    if any(leg.order_id is None or leg.transaction_id is None for leg in attributed):
        refusals.add("history_leg_unidentified")
    identified = [
        leg for leg in attributed if leg.order_id is not None and leg.transaction_id is not None
    ]

    executions_by_order, legs_by_order = _group_witness_inputs(executions, identified)

    witnesses: dict[int, frozenset[int]] = {}
    for order_id, order_executions in executions_by_order.items():
        order_witnesses, order_refusals = _build_order_witnesses(
            order_id,
            order_executions,
            order_totals,
            legs_by_order.get(order_id, ()),
            commission_by_order,
        )
        refusals |= order_refusals
        witnesses.update(order_witnesses)

    if refusals:
        return WitnessOutcome(witnesses={}, refusals=tuple(sorted(refusals)))
    return WitnessOutcome(witnesses=witnesses, refusals=())


def witness_reverse_coverage_holds(
    witnesses: Mapping[int, frozenset[int]], expected_sequences: Sequence[int]
) -> bool:
    """Return whether every expected execution is witnessed by at least one leg.

    The reverse-coverage half of the certification bijection: every
    ``scope_sequence`` in the expected range must be present with a non-empty
    leg set — commit order is not fill order, so witnessing only the tip would
    miss an earlier-committed but later-filled execution. Shared by the anchor
    bootstrap (expected ``[1, W]``) and the venue-cursor certificate (expected
    ``(W_anchor, W_E]``): one composition authority, two callers.

    Args:
        witnesses: The per-execution witness item-id sets.
        expected_sequences: The exact scope sequences that must be witnessed.

    Returns:
        Whether the coverage holds.
    """
    return set(witnesses) == set(expected_sequences) and all(witnesses.values())


def witness_bijection_holds(
    witnesses: Mapping[int, frozenset[int]], attributed_item_ids: Sequence[int]
) -> bool:
    """Return whether the witnessed legs are exactly the attributed venue items.

    The forward half of the certification bijection: the deduplicated union of
    witnessed item ids (a terminal execution legitimately shares its
    predecessor's legs) must equal the attributed item set — an attributed
    venue fill outside the witness map is un-ingested, a witnessed id outside
    the attributed set is fabricated evidence.

    Args:
        witnesses: The per-execution witness item-id sets.
        attributed_item_ids: The attributed venue history item ids in scope.

    Returns:
        Whether the bijection holds.
    """
    witnessed = sorted(set(chain.from_iterable(witnesses.values())))
    return witnessed == sorted(attributed_item_ids)


def decode_execution_cumulative(basis_units: int) -> Decimal:
    """Decode a venue exec-id basis-unit cumulative into an exact Decimal.

    The venue exec id encodes the cumulative as ``round(cumulative * 1e8)``; this
    inverts it with strict integer/Decimal math so no float artifact enters the
    witness cross-check.

    Args:
        basis_units: The integer basis-unit cumulative from the exec id.

    Returns:
        The exact cumulative amount.
    """
    return Decimal(basis_units) / _EXEC_ID_BASIS
