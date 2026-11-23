"""Venue evidence that the target order of a cancel has ended (item G).

The reconciliation loop retires a cancel command whose target order ended,
and plan recovery stops re-emitting a cancel for such a target. Both read the
same evidence, the newest qualifying venue observation of the target, which is
also its end time. A target cancelled or expired only locally, without venue
evidence, is not ended here.
"""

from collections.abc import Sequence
from datetime import datetime
from typing import Protocol

from snapper.core.types import TradeCommandStatusEnum
from snapper.data.repository_types import TradeCommandRow
from snapper.data.repository_types import VenueEventRow


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
