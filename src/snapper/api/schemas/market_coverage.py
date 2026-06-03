"""REST response schemas for the market-data coverage diagnostics route.

Coverage answers a single operator question per exchange: of the
instruments that are *currently configured* (active in the bitemporal
``instruments`` table), how many are actually receiving fresh market
data versus the static configured set, and which are "dark" — gated on
for market data yet producing no recent ticks.

The schemas inherit the project's :class:`PayloadResponse` envelope so
REST tracker provenance fields (``session_id`` / ``sequence_id`` /
``public_id`` / ``timestamp``) ride alongside the payload like every
other route.
"""

from typing import Literal

from snapper.api.schemas.base import PayloadResponse
from snapper.api.schemas.base import StrictBody


class MarketDataCoverageExchange(StrictBody):
    """Per-exchange coverage aggregate row.

    Every count is taken over the active (current bitemporal)
    instruments for ``exchange``.

    Attributes:
        exchange: Exchange identifier (lowercase).
        instruments: Count of active instruments for the exchange.
        fresh_ticks: Count of those instruments having at least one
            ``ticks`` row newer than ``now - tick_window``.
        fresh_candles: Count having at least one ``candles`` row whose
            ``open_at`` is newer than ``now - candle_window``.
        gated_off: Count whose current
            ``symbol_exchange_capabilities.can_market_data`` is FALSE.
        dark: Count that are NOT ``gated_off`` and have NO fresh ticks —
            the "should be live but isn't" gap.
    """

    exchange: str
    instruments: int
    fresh_ticks: int
    fresh_candles: int
    gated_off: int
    dark: int


class MarketDataCoveragePayload(StrictBody):
    """Inner payload for :class:`MarketDataCoverageResponse`.

    Attributes:
        exchanges: One :class:`MarketDataCoverageExchange` per exchange,
            ordered by exchange.
        tick_window_seconds: Freshness window applied to ``ticks`` rows.
        candle_window_seconds: Freshness window applied to ``candles``
            rows.
    """

    exchanges: list[MarketDataCoverageExchange]
    tick_window_seconds: int
    candle_window_seconds: int


class MarketDataCoverageResponse(
    PayloadResponse[Literal["market_data_coverage"], MarketDataCoveragePayload]
):
    """Wraps :class:`MarketDataCoveragePayload` with envelope provenance."""

    type: Literal["market_data_coverage"] = "market_data_coverage"
