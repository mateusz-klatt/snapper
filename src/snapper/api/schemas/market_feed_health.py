"""REST response schemas for the per-symbol feed-health diagnostics route.

Feed health answers the operator question: which subscribed symbols are
dark, when each last received data, and why. The rows are the persisted,
current-state projection of each publisher subprocess's in-memory
subscription-health tracker (see
:class:`snapper.data.models.InstrumentFeedHealth`).

The schemas inherit the project's :class:`PayloadResponse` envelope so
REST tracker provenance fields (``session_id`` / ``sequence_id`` /
``public_id`` / ``timestamp``) ride alongside the payload like every
other route.
"""

from datetime import datetime
from typing import Literal

from snapper.api.schemas.base import PayloadResponse
from snapper.api.schemas.base import StrictBody


class InstrumentFeedHealthRowSchema(StrictBody):
    """One current-state feed-health row.

    Attributes:
        coordinator: ``coord-<id>`` slug of the owning coordinator.
        exchange: Exchange identifier (lowercase).
        channel: Tracker channel key (e.g. ``ohlc:1m``).
        symbol: Wire-format symbol or product id.
        status: Lifecycle state (``pending`` / ``confirmed`` / ``failed``).
        requested_at: Wall-clock of the current subscribe attempt.
        confirmed_at: Wall-clock of ACK / data confirmation; ``None``
            until confirmed.
        last_seen_data_at: Wall-clock of last market data; ``None`` until
            the first datum.
        last_error: Last failure reason; ``None`` when healthy.
        retry_count: Retry attempts already consumed.
        snapshot_at: Wall-clock when the snapshot was flushed.
    """

    coordinator: str
    exchange: str
    channel: str
    symbol: str
    status: str
    requested_at: datetime
    confirmed_at: datetime | None
    last_seen_data_at: datetime | None
    last_error: str | None
    retry_count: int
    snapshot_at: datetime


class MarketFeedHealthPayload(StrictBody):
    """Inner payload for :class:`MarketFeedHealthResponse`.

    Attributes:
        rows: One :class:`InstrumentFeedHealthRowSchema` per natural key,
            ordered by ``(exchange, channel, symbol)``.
        exchange: The applied exchange filter, or ``None`` when
            unfiltered.
        fresh_within_seconds: The applied staleness filter, or ``None``
            when all rows (regardless of age) were returned.
    """

    rows: list[InstrumentFeedHealthRowSchema]
    exchange: str | None
    fresh_within_seconds: int | None


class MarketFeedHealthResponse(
    PayloadResponse[Literal["market_feed_health"], MarketFeedHealthPayload]
):
    """Wraps :class:`MarketFeedHealthPayload` with envelope provenance."""

    type: Literal["market_feed_health"] = "market_feed_health"
