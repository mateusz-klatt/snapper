"""Convert stored trades and the candles built from them into calculated one-minute rows.

The trade candle backfill and the candle repair driver fold stored trades
through the live :class:`TradeCandleBuilder`. Both use these two functions,
so a repaired minute carries exactly the bar the backfill would build from
the same trades.
"""

from datetime import UTC
from datetime import datetime
from typing import Final

from snapper.data.repository_types import CandleUpsertRow
from snapper.data.repository_types import TradeRow
from snapper.infrastructure.exchanges.contracts import CandleUpdate
from snapper.infrastructure.exchanges.contracts import TradeUpdate

CALCULATED_SOURCE: Final[str] = "calculated"
"""Source label of a candle built from stored trades."""

ONE_MINUTE: Final[str] = "1m"
"""Timeframe label of every row built here."""

_ORD_TYPE: Final[str] = "market"
"""Order type the builder records for a stored trade."""


def as_utc(value: datetime) -> datetime:
    """Normalize a datetime to timezone-aware UTC.

    Args:
        value: Naive or timezone-aware datetime; a naive value is read as UTC.

    Returns:
        Timezone-aware datetime converted to UTC.
    """
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


def trade_update(trade: TradeRow, instrument_public_id: str, event_time: datetime) -> TradeUpdate:
    """Convert a stored trade into a builder trade keyed by its instrument.

    Args:
        trade: Stored trade row.
        instrument_public_id: Instrument ID used as the builder's bucket key.
        event_time: The trade's execution time in UTC.

    Returns:
        Trade update ready for :class:`TradeCandleBuilder`.
    """
    return TradeUpdate(
        symbol=instrument_public_id,
        side=trade["side"],
        quantity=float(trade["size"]),
        price=float(trade["price"]),
        ord_type=_ORD_TYPE,
        timestamp=event_time,
        trade_id=trade["trade_id"],
    )


def calculated_row(
    candle: CandleUpdate,
    instrument_public_id: str,
    *,
    timestamp: datetime,
    session_id: str,
    sequence_id: int,
) -> CandleUpsertRow:
    """Build a calculated one-minute row from a completed builder candle.

    Args:
        candle: Completed builder candle.
        instrument_public_id: Instrument ID of the stored row.
        timestamp: Knowledge time of the row.
        session_id: Writer session of the row.
        sequence_id: Writer sequence number of the row.

    Returns:
        Complete calculated row.
    """
    return {
        "instrument_public_id": instrument_public_id,
        "open_at": as_utc(candle.interval_begin),
        "timestamp": timestamp,
        "timeframe": ONE_MINUTE,
        "open": candle.open,
        "high": candle.high,
        "low": candle.low,
        "close": candle.close,
        "volume": candle.volume,
        "vwap": candle.vwap,
        "trades": candle.trades,
        "source": CALCULATED_SOURCE,
        "complete": True,
        "session_id": session_id,
        "sequence_id": sequence_id,
    }
