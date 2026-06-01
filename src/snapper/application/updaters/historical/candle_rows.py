"""Shared candle-row builder for Polygon historical paths.

Builds :class:`CandleUpsertRow` dicts from :class:`AggregateCandle`
objects.  Lives in a dedicated module so both the download path
(``aggregates.py``) and the cache-load path (the CSV loader service)
share a single source of truth for the row mapping and stored types.

The mapping converts each candle's Decimal price/volume fields to the
``float`` storage type used by the ``candles`` table and renames the
candle's ``transactions`` attribute to the DB ``trades`` column.
"""

from collections.abc import Callable
from collections.abc import Iterable

from snapper.data.repository_types import CandleUpsertRow
from snapper.infrastructure.historical.polygon.loader import AggregateCandle

__all__ = ["build_candle_rows"]


def build_candle_rows(
    candles: Iterable[AggregateCandle],
    instrument_public_id: str,
    timeframe: str,
    session_id: str,
    sequence_id_fn: Callable[[], int],
) -> list[CandleUpsertRow]:
    """Build database row dicts from candle objects.

    Args:
        candles: Iterable of AggregateCandle objects.
        instrument_public_id: Stable public identity of the instrument.
        timeframe: Timeframe label string.
        session_id: Session identifier for provenance stamping.
        sequence_id_fn: Callable returning next sequence number per row.

    Returns:
        List of row dicts ready for database insertion.
    """
    return [
        {
            "instrument_public_id": instrument_public_id,
            "open_at": candle.timestamp,
            "timestamp": candle.timestamp,
            "timeframe": timeframe,
            "open": float(candle.open),
            "high": float(candle.high),
            "low": float(candle.low),
            "close": float(candle.close),
            "volume": float(candle.volume),
            "vwap": float(candle.vwap) if candle.vwap is not None else None,
            "trades": candle.transactions,
            "session_id": session_id,
            "sequence_id": sequence_id_fn(),
        }
        for candle in candles
    ]
