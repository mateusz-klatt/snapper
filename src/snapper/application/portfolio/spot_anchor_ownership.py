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
it unresolved. No rule accepts a pre-venue proof: a ``rejected`` or ``failed``
command without a venue order id is settled only by a reviewed never-submitted
exclusion, which pins the digest of the command's exact snapshot facts (see
:mod:`snapper.application.portfolio.never_submitted_commands`) and is rebuilt
from the snapshot on every evaluation. Every sentinel-tip Order without a venue
order id is unresolved as well, whichever exchange its Instrument resolves to,
and so is every unresolved lineage fact and every in-scope command without an
open-ended version in scope. One unresolved item refuses the whole bootstrap.

A venue order id is usable only when it is a non-blank string; a blank id is
treated exactly like a missing one wherever ownership is decided.
"""

from collections import Counter
from collections.abc import Mapping
from collections.abc import Sequence
from typing import Final
from typing import TypeGuard

from snapper.application.portfolio.never_submitted_commands import NEVER_SUBMITTED_DOCUMENT_SCHEMA
from snapper.application.portfolio.never_submitted_commands import never_submitted_digest
from snapper.core.json_types import JsonObject
from snapper.core.types import TradeCommandStatusEnum
from snapper.data.repository_types import SpotCommandVersionRow
from snapper.data.repository_types import SpotExecutionWitnessRow
from snapper.data.repository_types import SpotNeverSubmittedCandidate
from snapper.data.repository_types import SpotNeverSubmittedEventRow
from snapper.data.repository_types import SpotOwnedOrderLineageFact
from snapper.data.repository_types import SpotOwnedOrderLineageFactKind
from snapper.data.repository_types import SpotOwnedOrderPopulation
from snapper.data.repository_types import SpotOwnedOrderRow
from snapper.data.repository_types import SpotOwnedOrderSourceRow
from snapper.data.repository_types import SpotOwnedSubmitCommandRow
from snapper.data.repository_types import SpotSubmitCommandRead

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

_PRE_ACK_STATUSES: Final[frozenset[str]] = frozenset(
    {
        TradeCommandStatusEnum.CREATED.value,
        TradeCommandStatusEnum.DISPATCHED.value,
        TradeCommandStatusEnum.DIRECT_DISPATCHED.value,
    }
)
"""The only statuses a never-submitted command's earlier versions may hold."""

_NEVER_SUBMITTED_EVENT_TYPES: Final[Mapping[str, frozenset[str]]] = {
    TradeCommandStatusEnum.FAILED.value: frozenset(
        {"order_interlock_blocked", "order_breaker_open"}
    ),
    TradeCommandStatusEnum.REJECTED.value: frozenset({"order_rejected"}),
}
"""The terminal statuses a never-submitted command may hold, each with its one allowed event.

A ``failed`` command must carry exactly one pre-venue disposition event (the
live-trading interlock or the open circuit breaker), and a ``rejected`` command
exactly one ``order_rejected`` event. The pairing only narrows: it never proves
anything by itself, because ``order_rejected`` is also the executor's catch-all.
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


def _command_facts_qualify(
    command: SpotOwnedSubmitCommandRow, candidate: SpotNeverSubmittedCandidate
) -> bool:
    """Return whether a candidate's own row may describe a never-submitted command.

    Args:
        command: The in-scope sentinel-tip command.
        candidate: Its never-submitted facts from the same snapshot.

    Returns:
        Whether it is ``failed`` or ``rejected`` without a venue order id or an
        acknowledgement, carries a usable client order id equal to its venue
        client id, and no Order carries that client order id.
    """
    client_order_id = command["client_order_id"]
    return (
        command["status"] in _NEVER_SUBMITTED_EVENT_TYPES
        and command["exchange_order_id"] is None
        and command["acked_at"] is None
        and is_venue_order_id(client_order_id)
        and command["venue_client_id"] == client_order_id
        and candidate["order_row_count"] == 0
    )


def _versions_qualify(
    command: SpotOwnedSubmitCommandRow, versions: Sequence[SpotCommandVersionRow]
) -> bool:
    """Return whether a command's whole history stayed before any acknowledgement.

    Args:
        command: The in-scope sentinel-tip command.
        versions: Every version of its public id, in row-id order.

    Returns:
        Whether no version has a venue order id or an acknowledgement, the row
        ids strictly increase, the last version is the only open-ended one and
        is this command's row with its status, and every earlier version holds
        a status from before dispatch could be acknowledged.
    """
    if not versions:
        return False
    if any(
        version["exchange_order_id"] is not None or version["acked_at"] is not None
        for version in versions
    ):
        return False
    row_ids = [version["row_id"] for version in versions]
    if row_ids != sorted(set(row_ids)):
        return False
    *earlier, tip = versions
    return (
        tip["open_ended"]
        and tip["row_id"] == command["row_id"]
        and tip["status"] == command["status"]
        and not any(version["open_ended"] for version in earlier)
        and all(version["status"] in _PRE_ACK_STATUSES for version in earlier)
    )


def _events_qualify(status: str, events: Sequence[SpotNeverSubmittedEventRow]) -> bool:
    """Return whether a command's linked venue events are exactly its one disposition.

    Args:
        status: The command's terminal status.
        events: Every venue event linked to the command, from any scope.

    Returns:
        Whether there is exactly one event, it carries no venue order id, and
        its type is the one allowed for the status.
    """
    if len(events) != 1:
        return False
    event = events[0]
    allowed = _NEVER_SUBMITTED_EVENT_TYPES.get(status, frozenset())
    return event["exchange_order_id"] is None and event["event_type"] in allowed


def never_submitted_document(command: SpotOwnedSubmitCommandRow) -> JsonObject | None:
    """Render a command's never-submitted facts canonically, or ``None`` when they do not qualify.

    The document binds the command's scope and identifiers, every version's
    row id and status, every linked venue event, and the number of Orders
    carrying its client order id. It holds no datetime, and neither the audit
    record nor the events' receive times. A command whose facts break any
    condition of the never-submitted proof yields ``None`` and can never be
    excluded.

    Args:
        command: One in-scope sentinel-tip command.

    Returns:
        The canonical document, or ``None``.
    """
    candidate = command["candidate"]
    if candidate is None or not _command_facts_qualify(command, candidate):
        return None
    if not _versions_qualify(command, candidate["versions"]):
        return None
    if not _events_qualify(command["status"], candidate["venue_events"]):
        return None
    return {
        "schema": NEVER_SUBMITTED_DOCUMENT_SCHEMA,
        "command_public_id": command["public_id"],
        "command_type": command["command_type"],
        "client_order_id": command["client_order_id"],
        "venue_client_id": command["venue_client_id"],
        "exchange": command["exchange"],
        "wallet_public_id": command["wallet_public_id"],
        "mode": command["mode"],
        "status": command["status"],
        "versions": [
            {"row_id": version["row_id"], "status": version["status"]}
            for version in candidate["versions"]
        ],
        "venue_events": [
            {
                "id": event["id"],
                "event_type": event["event_type"],
                "command_public_id": event["command_public_id"],
                "client_order_id": event["client_order_id"],
                "exchange_order_id": event["exchange_order_id"],
                "status": event["status"],
                "error": event["error"],
            }
            for event in candidate["venue_events"]
        ],
        "order_row_count": candidate["order_row_count"],
    }


def _excluded(command: SpotOwnedSubmitCommandRow, pinned_digests: frozenset[str]) -> bool:
    """Return whether a reviewed never-submitted exclusion settles one command.

    Args:
        command: One in-scope sentinel-tip command.
        pinned_digests: The reviewed document digests.

    Returns:
        Whether the command's document exists and its digest is pinned.
    """
    document = never_submitted_document(command)
    return document is not None and never_submitted_digest(document) in pinned_digests


def build_owned_order_population(
    exchange: str,
    order_rows: Sequence[SpotOwnedOrderSourceRow],
    instrument_versions: Mapping[str, frozenset[str]],
    command_read: SpotSubmitCommandRead,
    pinned_digests: frozenset[str] = frozenset(),
) -> SpotOwnedOrderPopulation:
    """Classify one snapshot's sentinel-tip Orders into the owned population.

    Args:
        exchange: The exchange whose owned order set is requested.
        order_rows: Every sentinel-tip Order of the wallet and mode, each with
            the sentinel-tip Instruments its left join matched.
        instrument_versions: The exchanges named by every version of each
            referenced Instrument, keyed by instrument public id.
        command_read: The in-scope ``create`` and ``submit`` commands of the
            same snapshot, and the retired public ids among them.
        pinned_digests: The reviewed never-submitted document digests. The
            empty default excludes nothing, so it can only fail closed.

    Returns:
        The owned order set, the resolved Orders of the exchange, every
        unresolved lineage fact with its blocked venue order ids, the resolved
        Orders of other exchanges that carry no usable venue order id, the
        commands unchanged, the ids of the commands a reviewed exclusion
        settles, and the retired command public ids.
    """
    commands = command_read["commands"]
    excluded = [command for command in commands if _excluded(command, pinned_digests)]
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
        "excluded_command_public_ids": frozenset(command["public_id"] for command in excluded),
        "excluded_client_order_ids": frozenset(command["client_order_id"] for command in excluded),
        "retired_command_public_ids": tuple(command_read["retired_public_ids"]),
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
    requested exchange or of any other), any retired in-scope command, and any
    command that neither the closed status table nor a reviewed never-submitted
    exclusion settles makes the owned order set unprovable, so the bootstrap
    must not classify history against it.

    Args:
        population: The owned-order population of one snapshot.

    Returns:
        Whether the unresolved-submit gate refuses.
    """
    if (
        population["unresolved_lineage"]
        or population["other_exchange_unidentified_orders"]
        or population["retired_command_public_ids"]
    ):
        return True
    resolved = population["resolved_orders"]
    if any(not is_venue_order_id(order["exchange_order_id"]) for order in resolved):
        return True
    carriers = Counter(order["exchange_order_id"] for order in resolved)
    owned_order_ids = population["owned_order_ids"]
    excluded = population["excluded_command_public_ids"]
    return not all(
        command["public_id"] in excluded or _command_settled(command, owned_order_ids, carriers)
        for command in population["commands"]
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
