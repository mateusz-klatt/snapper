"""Venue evidence that the target order of a cancel has ended (item G).

The reconciliation loop retires a cancel command whose target order ended,
and plan recovery stops re-emitting a cancel for such a target. Both read the
same evidence, the newest qualifying venue observation of the target, which is
also its end time. A target cancelled or expired only locally, without venue
evidence, is not ended here.
"""

from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime
from typing import Literal
from typing import Protocol

from loguru import logger

from snapper.core.types import TradeCommandStatusEnum
from snapper.data.repository import SQLAlchemyRepository
from snapper.data.repository_types import TradeCommandRow
from snapper.data.repository_types import VenueEventRow

CancelHydrationOutcome = Literal["hydrated", "unresolved", "race_lost", "error"]
"""Result of attempting to resolve and CAS-hydrate one awaiting_target cancel."""


class VenueEventTimeReader(Protocol):
    """Repository read that finds a target order's venue-evidenced end."""

    async def get_latest_venue_event_received_at(
        self,
        client_order_id: str,
        event_types: Sequence[str],
        statuses: Sequence[str] | None = None,
    ) -> datetime | None:
        """Return the newest qualifying ``received_at`` of an order's venue events.

        Args:
            client_order_id: Order whose venue events are read.
            event_types: Venue event types that count.
            statuses: Optional statuses that count, compared case-insensitively.

        Returns:
            The newest qualifying ``received_at``, or ``None`` when no
            event qualifies.
        """


class CancelTargetUnresolvedError(RuntimeError):
    """Refuse blank-id cancel publish until durable evidence supplies a venue id."""


class CancelTargetIdentityReader(Protocol):
    """Repository reads used to hydrate a cancel target's venue identity."""

    async def get_exchange_order_id_for_client_order_id(
        self,
        client_order_id: str,
        as_of: datetime,
    ) -> str | None:
        """Return the active Order's venue id, if one exists.

        Args:
            client_order_id: Cancel target's client order id.
            as_of: Temporal point for active-version selection.

        Returns:
            Venue order id, or None when unavailable.
        """

    async def get_active_create_command_by_client_order_id(
        self, client_order_id: str, exchange: str
    ) -> TradeCommandRow | None:
        """Return the active create or submit command, if one exists.

        Args:
            client_order_id: Cancel target's client order id.
            exchange: Exchange scoping the command lookup.

        Returns:
            Active create or submit command, or None when unavailable.
        """

    async def get_order_lifecycle_events(self, client_order_ids: list[str]) -> list[VenueEventRow]:
        """Return lifecycle evidence for the requested client order ids.

        Args:
            client_order_ids: Client order ids whose evidence is requested.

        Returns:
            Lifecycle venue events in durable order.
        """


@dataclass(frozen=True, slots=True)
class CancelHydrationStamp:
    """Provenance stamp written onto a hydrated cancel successor row."""

    bus_time: datetime
    session_id: str
    sequence_id: int


async def cancel_target_exchange_order_id(
    reader: CancelTargetIdentityReader,
    client_order_id: str,
    exchange: str,
    as_of: datetime,
) -> str | None:
    """Resolve a venue id from Order, create command, or live ack evidence.

    Args:
        reader: Durable identity and lifecycle reader.
        client_order_id: Cancel target's client order id.
        exchange: Exchange scoping the create-command lookup.
        as_of: Temporal point for the active Order lookup.

    Returns:
        The best available venue order id, or None when unresolved.
    """
    order_id = await reader.get_exchange_order_id_for_client_order_id(client_order_id, as_of)
    if order_id:
        return order_id
    create = await reader.get_active_create_command_by_client_order_id(client_order_id, exchange)
    command_id = None if create is None else create.get("exchange_order_id")
    if command_id:
        return command_id
    events = await reader.get_order_lifecycle_events([client_order_id])
    for event in reversed(events):
        if (
            event["event_type"] in ("order_accepted", "fill_observed")
            and event["exchange_order_id"]
        ):
            return event["exchange_order_id"]
    return None


async def hydrate_awaiting_target_cancel(
    store: SQLAlchemyRepository,
    cancel: TradeCommandRow,
    exchange_order_id: str,
    stamp: CancelHydrationStamp,
    *,
    log_error: bool = True,
) -> CancelHydrationOutcome:
    """CAS-hydrate a held ``awaiting_target`` cancel to ``created`` in place.

    Args:
        store: Repository supporting the lifecycle CAS.
        cancel: Held cancel row currently expected at ``awaiting_target``.
        exchange_order_id: Newly resolved venue order identifier.
        stamp: Temporal and sequence stamp for the successor version.
        log_error: Whether unexpected exceptions should log an error.

    Returns:
        ``"hydrated"`` when the CAS succeeded, ``"race_lost"`` when the row
        moved concurrently, or ``"error"`` on a storage failure.
    """
    try:
        applied = await store.advance_trade_command_lifecycle(
            public_id=cancel["public_id"],
            expected_status=TradeCommandStatusEnum.AWAITING_TARGET,
            new_status=TradeCommandStatusEnum.CREATED,
            bus_time=stamp.bus_time,
            session_id=stamp.session_id,
            sequence_id=stamp.sequence_id,
            exchange_order_id=exchange_order_id,
        )
    except Exception as exc:
        if log_error:
            logger.error(
                "Cancel identity hydration failed for {} cid={}: {}",
                cancel["public_id"],
                cancel["client_order_id"],
                exc,
            )
        return "error"
    if not applied:
        logger.info(
            "Cancel identity hydration lost status race for {} cid={}",
            cancel["public_id"],
            cancel["client_order_id"],
        )
        return "race_lost"
    logger.info(
        "Hydrated awaiting_target cancel {} cid={} -> created (exchange_order_id={})",
        cancel["public_id"],
        cancel["client_order_id"],
        exchange_order_id,
    )
    return "hydrated"


async def resolve_and_hydrate_awaiting_target_cancel(
    store: SQLAlchemyRepository,
    cancel: TradeCommandRow,
    stamp: CancelHydrationStamp,
    *,
    log_error: bool = True,
) -> CancelHydrationOutcome:
    """Resolve a held cancel's target venue id and hydrate it in place.

    Args:
        store: Repository supporting cancel target reads and lifecycle CAS.
        cancel: Held cancel row in ``awaiting_target`` status.
        stamp: Temporal and sequence stamp used for lookup and CAS write.
        log_error: Whether lookup or hydration exceptions log an error.

    Returns:
        ``"hydrated"``, ``"unresolved"``, ``"race_lost"``, or ``"error"``.
    """
    try:
        exchange_order_id = await cancel_target_exchange_order_id(
            store,
            cancel["client_order_id"],
            cancel["exchange"],
            stamp.bus_time,
        )
    except Exception as exc:
        if log_error:
            logger.error(
                "Cancel target resolution failed for {} cid={}: {}",
                cancel["public_id"],
                cancel["client_order_id"],
                exc,
            )
        return "error"
    if not exchange_order_id:
        return "unresolved"
    return await hydrate_awaiting_target_cancel(
        store,
        cancel,
        exchange_order_id,
        stamp,
        log_error=log_error,
    )


async def target_end_time(
    reader: VenueEventTimeReader, target_status: str, client_order_id: str
) -> datetime | None:
    """Return the venue observation time of a target order's end.

    The newest qualifying ``received_at`` is both the evidence and the end
    time: ``order_terminal`` with a cancel status for a cancelled target;
    ``order_terminal`` with ``filled`` or ``closed``, otherwise
    ``fill_observed``, for a filled one; ``order_terminal`` with
    ``expired`` for an expired one. For ``fill_observed`` the newest
    observation can be a later duplicate or partial one rather than the
    completing fill, which does not change the outcome for a filled target.

    Args:
        reader: Repository read of venue event times.
        target_status: Status of the target's create or submit command.
        client_order_id: Target order's client order id.

    Returns:
        The end time, or ``None`` when the status is not an ending this
        rule recognises, or when no venue evidence exists.
    """
    if target_status == TradeCommandStatusEnum.CANCELLED:
        return await reader.get_latest_venue_event_received_at(
            client_order_id, ("order_terminal",), ("canceled", "cancelled")
        )
    if target_status == TradeCommandStatusEnum.FILLED:
        terminal = await reader.get_latest_venue_event_received_at(
            client_order_id, ("order_terminal",), ("filled", "closed")
        )
        if terminal is not None:
            return terminal
        return await reader.get_latest_venue_event_received_at(client_order_id, ("fill_observed",))
    if target_status == TradeCommandStatusEnum.EXPIRED:
        return await reader.get_latest_venue_event_received_at(
            client_order_id, ("order_terminal",), ("expired",)
        )
    return None
