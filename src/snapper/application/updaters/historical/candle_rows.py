"""Candle-row builder for the Polygon cache-load (CSV) path.

Builds :class:`CandleUpsertRow` dicts from :class:`AggregateCandle`
objects.  Lives in a dedicated module so the cache-load path (the CSV
loader service) has a single source of truth for the row mapping and
stored types.  The download path (``aggregates.py``) writes CSV directly
and no longer builds database rows through this helper.

The mapping converts each candle's Decimal price/volume fields to the
``float`` storage type used by the ``candles`` table and renames the
candle's ``transactions`` attribute to the DB ``trades`` column.  The
caller supplies ``bus_time`` (wall-clock load time) for the row
``timestamp`` so bus-time stays distinct from the bar event-time
``open_at``.
"""

from collections.abc import Callable
from collections.abc import Iterable
from datetime import datetime

from snapper.data.repository_types import CandleUpsertRow
from snapper.infrastructure.historical.polygon.loader import AggregateCandle

__all__ = ["build_candle_rows"]


def build_candle_rows(
    candles: Iterable[AggregateCandle],
    instrument_public_id: str,
    timeframe: str,
    session_id: str,
    sequence_id_fn: Callable[[], int],
    bus_time: datetime,
) -> list[CandleUpsertRow]:
    """Build database row dicts from candle objects.

    Args:
        candles: Iterable of AggregateCandle objects.
        instrument_public_id: Stable public identity of the instrument.
        timeframe: Timeframe label string.
        session_id: Session identifier for provenance stamping.
        sequence_id_fn: Callable returning next sequence number per row.
        bus_time: Wall-clock load time stamped as each row's ``timestamp``
            (bus-time provenance), kept distinct from ``open_at`` (the bar
            event-time). On an SCD2 amend the upsert closes the prior version
            with ``known_to = row["timestamp"]``; using the load time rather
            than the bar time gives the superseded row a non-empty validity
            interval, so point-in-time/as-of reads reconstruct the value as it
            was known before the correction arrived (no lookahead bias).

    Returns:
        List of row dicts ready for database insertion.
    """
    return [
        {
            "instrument_public_id": instrument_public_id,
            "open_at": candle.timestamp,
            "timestamp": bus_time,
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
