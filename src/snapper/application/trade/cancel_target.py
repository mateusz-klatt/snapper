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
