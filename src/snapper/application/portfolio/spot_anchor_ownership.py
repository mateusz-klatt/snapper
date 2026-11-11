"""Pure owned-order population and unresolved-submit gate for the spot anchor bootstrap.

The bootstrap may classify a history row on its seal page as external only when
the row's order id lies outside a COMPLETE owned order set ``S``. This module
derives ``S`` from one database snapshot of the account's sentinel-tip Orders,
each left-joined to its sentinel-tip Instrument, and decides whether that
snapshot is complete enough to classify against. It performs no I/O, holds no
clock, and mutates no input.

Ownership is decided by Order lineage alone. An Order resolves only through
exactly one sentinel-tip Instrument whose exchange is a canonical venue key on
which every version of that Instrument agrees. Any other projection is an
unresolved lineage fact, which blocks the bootstrap for every exchange of the
wallet and mode; its venue order id, when it has one, is kept as a blocked id so
it can never fall through as external activity. A resolved Order of another
exchange is excluded from ``S``, and a resolved Order of the requested exchange
contributes its venue order id to ``S``. Trade commands never filter or assign
ownership.

Order persistence happens after the venue accepts an order and is best effort,
so ``S`` alone is not a proven superset of the orders this system placed. The
unresolved-submit gate therefore applies a closed table to every in-scope
``create`` or ``submit`` command. Only a terminal status (``filled``,
``cancelled``, ``expired``, ``rejected``, ``failed``) whose venue order id lies
in ``S`` and is carried by exactly one resolved Order settles the command;
every other status, including any value outside the closed status enum, leaves
it unresolved. No pre-venue proof is accepted, so a ``rejected`` or ``failed``
command that never reached the venue stays unresolved. Every sentinel-tip
Order without a venue order id is unresolved as well, whichever exchange its
Instrument resolves to, and so is every unresolved lineage fact. One unresolved
item refuses the whole bootstrap.

A venue order id is usable only when it is a non-blank string; a blank id is
treated exactly like a missing one wherever ownership is decided.
"""

from collections import Counter
from collections.abc import Mapping
from collections.abc import Sequence
from typing import Final
from typing import TypeGuard

from snapper.core.types import TradeCommandStatusEnum
from snapper.data.repository_types import SpotExecutionWitnessRow
from snapper.data.repository_types import SpotOwnedOrderLineageFact
from snapper.data.repository_types import SpotOwnedOrderLineageFactKind
from snapper.data.repository_types import SpotOwnedOrderPopulation
from snapper.data.repository_types import SpotOwnedOrderRow
from snapper.data.repository_types import SpotOwnedOrderSourceRow
from snapper.data.repository_types import SpotOwnedSubmitCommandRow

_SETTLED_COMMAND_STATUSES: Final[frozenset[str]] = frozenset(
    {
        TradeCommandStatusEnum.FILLED.value,
        TradeCommandStatusEnum.CANCELLED.value,
        TradeCommandStatusEnum.EXPIRED.value,
        TradeCommandStatusEnum.REJECTED.value,
        TradeCommandStatusEnum.FAILED.value,
    }
)
"""Command statuses that may be settled by a venue order id in ``S``.

``created``, ``dispatched``, ``direct_dispatched``, ``accepted`` and
``partially_filled`` are never settled, and neither is any value outside the
closed enum: membership here is the whole table, so an unknown status fails
closed by construction.
"""


def is_venue_order_id(value: str | None) -> TypeGuard[str]:
    """Return whether a value is a usable venue order id.

    Args:
        value: A verbatim order id from a venue history row, an Order, or a
            trade command.

    Returns:
        Whether the value is a non-blank string.
    """
    return value is not None and value.strip() != ""


def _lineage_fact(
    row: SpotOwnedOrderSourceRow,
    instrument_versions: Mapping[str, frozenset[str]],
) -> SpotOwnedOrderLineageFactKind | None:
    """Classify one Order's Instrument projection, or ``None`` when it resolves.

    Args:
        row: The sentinel-tip Order with its left-joined sentinel-tip Instruments.
        instrument_versions: The exchanges named by every Instrument version,
            keyed by instrument public id; an absent key means no version exists.

    Returns:
        The unresolved lineage fact, or ``None`` for exactly one canonical,
        version-consistent sentinel-tip Instrument.
    """
    tip_exchanges = row["instrument_exchanges"]
    versions = instrument_versions.get(row["instrument_public_id"], frozenset())
    if not tip_exchanges:
        return "instrument_dangling" if versions else "instrument_absent"
    if len(tip_exchanges) > 1:
        return "instrument_multiply_active"
    exchange = tip_exchanges[0]
    if not exchange or exchange != exchange.strip().lower() or versions != {exchange}:
        return "instrument_conflicting"
    return None


def build_owned_order_population(
    exchange: str,
    order_rows: Sequence[SpotOwnedOrderSourceRow],
    instrument_versions: Mapping[str, frozenset[str]],
    commands: Sequence[SpotOwnedSubmitCommandRow],
) -> SpotOwnedOrderPopulation:
    """Classify one snapshot's sentinel-tip Orders into the owned population.

    Args:
        exchange: The exchange whose owned order set is requested.
        order_rows: Every sentinel-tip Order of the wallet and mode, each with
            the sentinel-tip Instruments its left join matched.
        instrument_versions: The exchanges named by every version of each
            referenced Instrument, keyed by instrument public id.
        commands: The in-scope ``create`` and ``submit`` commands of the same
            snapshot.

    Returns:
        The owned order set, the resolved Orders of the exchange, every
        unresolved lineage fact with its blocked venue order ids, the resolved
        Orders of other exchanges that carry no usable venue order id, and the
        commands, unchanged.
    """
    resolved: list[SpotOwnedOrderRow] = []
    facts: list[SpotOwnedOrderLineageFact] = []
    other_exchange_unidentified: list[str] = []
    for row in order_rows:
        fact = _lineage_fact(row, instrument_versions)
        if fact is not None:
            facts.append(
                {
                    "order_public_id": row["order_public_id"],
                    "instrument_public_id": row["instrument_public_id"],
                    "exchange_order_id": row["exchange_order_id"],
                    "fact": fact,
                }
            )
        elif row["instrument_exchanges"][0] == exchange:
            resolved.append(
                {
                    "order_public_id": row["order_public_id"],
                    "exchange_order_id": row["exchange_order_id"],
                }
            )
        elif not is_venue_order_id(row["exchange_order_id"]):
            other_exchange_unidentified.append(row["order_public_id"])
    return {
        "owned_order_ids": _usable_order_ids([order["exchange_order_id"] for order in resolved]),
        "resolved_orders": tuple(resolved),
        "unresolved_lineage": tuple(facts),
        "blocked_order_ids": _usable_order_ids([fact["exchange_order_id"] for fact in facts]),
        "other_exchange_unidentified_orders": tuple(other_exchange_unidentified),
        "commands": tuple(commands),
    }


def _usable_order_ids(values: Sequence[str | None]) -> frozenset[str]:
    """Return the usable venue order ids among verbatim values.

    Args:
        values: Verbatim venue order ids, possibly missing or blank.

    Returns:
        The non-blank ids, deduplicated.
    """
    return frozenset(value for value in values if is_venue_order_id(value))


def _command_settled(
    command: SpotOwnedSubmitCommandRow,
    owned_order_ids: frozenset[str],
    carriers: Mapping[str | None, int],
) -> bool:
    """Return whether one command is settled under the closed status table.

    Args:
        command: One in-scope ``create`` or ``submit`` command.
        owned_order_ids: The owned order set ``S``.
        carriers: How many resolved Orders carry each venue order id.

    Returns:
        Whether the command has a settling status and a venue order id in
        ``S`` that exactly one resolved Order carries.
    """
    venue_order_id = command["exchange_order_id"]
    return (
        command["status"] in _SETTLED_COMMAND_STATUSES
        and is_venue_order_id(venue_order_id)
        and venue_order_id in owned_order_ids
        and carriers[venue_order_id] == 1
    )


def own_submit_unresolved(population: SpotOwnedOrderPopulation) -> bool:
    """Return whether the snapshot leaves any own submission unresolved.

    Any unresolved lineage fact (which also covers every blocked venue order
    id), any sentinel-tip Order without a usable venue order id (of the
    requested exchange or of any other), and any command the closed status
    table does not settle makes the owned order set unprovable, so the
    bootstrap must not classify history against it.

    Args:
        population: The owned-order population of one snapshot.

    Returns:
        Whether the unresolved-submit gate refuses.
    """
    if population["unresolved_lineage"] or population["other_exchange_unidentified_orders"]:
        return True
    resolved = population["resolved_orders"]
    if any(not is_venue_order_id(order["exchange_order_id"]) for order in resolved):
        return True
    carriers = Counter(order["exchange_order_id"] for order in resolved)
    owned_order_ids = population["owned_order_ids"]
    return not all(
        _command_settled(command, owned_order_ids, carriers) for command in population["commands"]
    )


def authoritative_execution_order_ids(
    witness_rows: Sequence[SpotExecutionWitnessRow],
    population: SpotOwnedOrderPopulation,
) -> dict[int, str | None]:
    """Resolve each sealed-prefix execution's Order to its authoritative venue order id.

    An execution's Order resolves only when exactly one resolved Order of the
    requested exchange carries its public id; an Order that is unresolved,
    belongs to another exchange, is missing, or appears twice resolves to
    ``None``, which the witness builder refuses as a foreign order id.

    Args:
        witness_rows: The sealed-prefix executions with their Order identities.
        population: The owned-order population of one snapshot.

    Returns:
        The authoritative venue order id per execution scope sequence.
    """
    venue_order_ids: dict[str, list[str | None]] = {}
    for order in population["resolved_orders"]:
        venue_order_ids.setdefault(order["order_public_id"], []).append(order["exchange_order_id"])
    resolved: dict[int, str | None] = {}
    for row in witness_rows:
        candidates = venue_order_ids.get(row["order_public_id"], [])
        resolved[row["scope_sequence"]] = candidates[0] if len(candidates) == 1 else None
    return resolved
