"""Kraken Futures exchange market data publisher.

This module provides a market data feed publisher for the Kraken Futures
exchange. It streams real-time ticks and trades via Kraken's Futures
WebSocket API and publishes normalized data to the ZMQ messaging bus.

Candle data is ingested via REST OHLCV polling (no WebSocket candle
feed available). The base class ``_candle_loop()`` drives polling
through ``subscribe_candles()``, which polls ``get_ohlcv()`` at
regular intervals.

Configuration
-------------
Symbols are configured via settings.instruments["kraken_futures"].
The publisher uses public (anonymous) WebSocket connections.
"""

from typing import Any

from loguru import logger

from snapper.application.process_manager.process_parameters import PublisherSymbolsParameters
from snapper.application.process_manager.registry import register_process
from snapper.config.settings import AppSettings
from snapper.core.types import ExchangeEnum
from snapper.core.types import MarketDataExchange
from snapper.core.types import ProcessModeEnum
from snapper.core.types import ProcessRestartPolicyEnum
from snapper.core.types import ProcessRoleEnum
from snapper.infrastructure.exchanges.implementations.kraken_futures import (
    KrakenFuturesExchangeClient,
)
from snapper.infrastructure.exchanges.kraken_sdk_patches import _CURRENT_PUBLISHER
from snapper.infrastructure.exchanges.kraken_sdk_patches import apply_kraken_futures_pool_routing
from snapper.infrastructure.symbols.functions import get_available_kraken_futures_symbols
from snapper.infrastructure.symbols.functions import native_to_kraken_futures_ws
from snapper.messaging.publishers.base import MarketDataPublisherService

apply_kraken_futures_pool_routing()


@register_process(
    "kraken_futures_feed_publisher",
    description="Kraken Futures market data feed publisher",
    priority=20,
    role=ProcessRoleEnum.CORE,
    restart_policy=ProcessRestartPolicyEnum.ALWAYS,
    tags=("market-data", "publisher", "kraken_futures"),
    parameters_model=PublisherSymbolsParameters,
    enabled=True,
    mode=ProcessModeEnum.THREAD,
)
class KrakenFuturesMarketDataPublisher(MarketDataPublisherService[KrakenFuturesExchangeClient]):
    """Kraken Futures exchange market data publisher.

    Streams real-time market data from Kraken Futures WebSocket API and
    publishes normalized messages to ZMQ. Handles symbol conversion between
    native format (BTC-USD-PERP) and Kraken Futures product IDs (PF_XBTUSD).

    Topics Published:
        - market.kraken_futures.{instrument}.ticks
        - market.kraken_futures.{instrument}.trades
        - system.heartbeats.feed.kraken_futures

    Attributes:
        Inherits all attributes from MarketDataPublisherService.
    """

    @staticmethod
    def get_default_parameters(settings: AppSettings) -> dict[str, Any]:
        """Get default parameters from settings.

        Args:
            settings: Application settings with instruments config.

        Returns:
            Dictionary with symbols list for Kraken Futures.
        """
        instruments = settings.instruments
        return {
            "symbols": instruments.get(ExchangeEnum.KRAKEN_FUTURES, []),
        }

    def _create_exchange_client(self) -> KrakenFuturesExchangeClient:
        """Create anonymous Kraken Futures client for public data.

        Returns:
            Configured KrakenFuturesExchangeClient (no API keys needed).
        """
        return KrakenFuturesExchangeClient()

    def _get_exchange_name(self) -> MarketDataExchange:
        """Get exchange identifier.

        Returns:
            "kraken_futures" exchange name.
        """
        return ExchangeEnum.KRAKEN_FUTURES

    async def start(self) -> None:
        """Start the publisher within a connector-registration context.

        Stamps ``_CURRENT_PUBLISHER`` so the patched
        ``kraken.futures.websocket.connect`` shim reads
        ``_get_exchange_name()`` ("kraken_futures") when reserving an
        egress-pool route. This lets ``egress_pool``'s
        ``allowed_exchanges`` filter pin Futures to a specific tunnel
        (e.g. alongside Equities) without affecting Spot.

        Without this override the shim falls back to the legacy
        hardcoded ``"kraken"`` tag — pool routing still works, but
        routes pinned to ``["kraken_futures"]`` would silently
        reject every Futures reservation and force fallback to a
        wildcard tunnel.

        The token is reset in ``finally`` so the ContextVar does not
        leak across publisher restarts.
        """
        token = _CURRENT_PUBLISHER.set(self)
        try:
            await super().start()
        finally:
            _CURRENT_PUBLISHER.reset(token)

    def _validate_symbols(self, symbols: list[str]) -> list[str]:
        """Validate and filter symbols for Kraken Futures.

        Converts symbols to Kraken Futures WS format to validate them.
        Invalid symbols are logged and skipped.

        Wildcard handling: when the caller passes ``["*"]``, the
        method expands to every native Kraken Futures symbol currently
        loaded by the symbol mapper. Kraken Futures' WS has no
        server-side wildcard token (unlike Kraken spot), so expansion
        happens client-side and the resulting list is then chunk-
        subscribed by :meth:`KrakenFuturesExchangeClient._subscribe_in_chunks`.

        Args:
            symbols: Input symbols in native format, or ``["*"]`` to
                subscribe to every available futures symbol.

        Returns:
            Valid symbols that can be streamed from Kraken Futures.
        """
        if symbols == ["*"]:
            expanded = get_available_kraken_futures_symbols()
            logger.info(
                f"KrakenFuturesMarketDataPublisher: wildcard expansion -> "
                f"{len(expanded)} futures symbols"
            )
            return expanded
        native_symbols: list[str] = []
        seen_symbols: set[str] = set()
        for symbol in symbols:
            try:
                native_to_kraken_futures_ws(symbol)
            except ValueError:
                logger.warning(
                    f"KrakenFuturesMarketDataPublisher: Skipping unknown native symbol {symbol}"
                )
                continue
            if symbol in seen_symbols:
                continue
            seen_symbols.add(symbol)
            native_symbols.append(symbol)
        return native_symbols

    def _get_max_symbols_per_connection(self) -> int:
        """Get Kraken Futures WebSocket symbol limit.

        Returns:
            0 (unlimited) — Kraken Futures WS does not document a per-connection limit.
        """
        return 0

    async def _attempt_liveness_recovery(self, reason: str) -> None:
        """Recover stale market data by rebuilding the public WS client."""
        logger.error("kraken_futures publisher: liveness recovery triggered ({})", reason)
        client = self._exchange_client
        if client is not None:
            await client.disconnect()
            await client._ensure_ws_connected()
