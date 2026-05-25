"""Kraken Equities (FCM Futures) market data publisher.

This module provides a market data feed publisher for FCM commodity/index
futures on Kraken's equities platform. It streams real-time ticks and trades
via the Kraken Equities WebSocket (``wss://ws-equities.kraken.com``) and
publishes normalized data to the ZMQ messaging bus.

Configuration
-------------
Symbols are configured via settings.instruments["kraken_equities"].
The publisher uses public (anonymous) WebSocket connections.
Data is delayed (~10 minutes).
"""

from datetime import UTC
from datetime import datetime
from datetime import time as datetime_time
from typing import Any

from loguru import logger

from snapper.application.process_manager.process_parameters import PublisherSymbolsParameters
from snapper.application.process_manager.registry import register_process
from snapper.config.settings import AppSettings
from snapper.core.types import ExchangeEnum
from snapper.core.types import MarketDataExchange
from snapper.core.types import ProcessModeEnum
from snapper.core.types import ProcessRoleEnum
from snapper.infrastructure.exchanges.implementations.kraken_equities import (
    KrakenEquitiesExchangeClient,
)
from snapper.infrastructure.exchanges.kraken_sdk_patches import _CURRENT_PUBLISHER
from snapper.infrastructure.exchanges.kraken_sdk_patches import (
    apply_kraken_already_subscribed_filter,
)
from snapper.infrastructure.symbols.functions import get_available_kraken_equities_symbols
from snapper.infrastructure.symbols.functions import native_to_kraken_equities_ws
from snapper.messaging.publishers.base import MarketDataPublisherService

apply_kraken_already_subscribed_filter()
"""Install the kraken-sdk Already-subscribed filter at module import.

Idempotent — safe if `snapper.messaging.publishers.kraken` has already
installed it. Required here because `KrakenEquitiesExchangeClient` reuses
`SpotWSClient` (and therefore `ConnectSpotWebsocket._manage_subscriptions`)
with a different ``ws_url``. An equities-only process that does not import
`snapper.messaging.publishers.kraken` would otherwise leave the SDK warning
flood unsuppressed for equities subscriptions.
"""

_CME_DAILY_BREAK_START = datetime_time(hour=21)
_CME_DAILY_BREAK_END = datetime_time(hour=22)


def _is_cme_closed(now_utc: datetime) -> bool:
    """Return whether CME FCM contracts are in a scheduled closure window."""
    current = now_utc if now_utc.tzinfo is not None else now_utc.replace(tzinfo=UTC)
    current = current.astimezone(UTC)
    weekday = current.weekday()
    current_time = current.time()
    if weekday == 5:
        return True
    if weekday == 6:
        return current_time < _CME_DAILY_BREAK_END
    if weekday == 4:
        return current_time >= _CME_DAILY_BREAK_END
    return _CME_DAILY_BREAK_START <= current_time < _CME_DAILY_BREAK_END


@register_process(
    "kraken_equities_feed_publisher",
    description="Kraken Equities (FCM Futures) market data feed publisher",
    priority=20,
    role=ProcessRoleEnum.CORE,
    tags=("market-data", "publisher", "kraken_equities"),
    parameters_model=PublisherSymbolsParameters,
    enabled=True,
    mode=ProcessModeEnum.THREAD,
)
class KrakenEquitiesMarketDataPublisher(
    MarketDataPublisherService[KrakenEquitiesExchangeClient],
):
    """Kraken Equities market data publisher.

    Streams real-time market data from Kraken Equities WebSocket and
    publishes normalized messages to ZMQ. Handles symbol conversion between
    native format (``CLM6-NYMEX``) and WS format (``CLM6.NYMEX``).

    Topics Published:
        - market.kraken_equities.{instrument}.ticks
        - market.kraken_equities.{instrument}.trades
        - system.heartbeats.feed.kraken_equities
    """

    @staticmethod
    def get_default_parameters(settings: AppSettings) -> dict[str, Any]:
        """Get default parameters from settings.

        Args:
            settings: Application settings with instruments config.

        Returns:
            Dictionary with symbols list for Kraken Equities.
        """
        instruments = settings.instruments
        return {
            "symbols": instruments.get(ExchangeEnum.KRAKEN_EQUITIES, []),
        }

    def _create_exchange_client(self) -> KrakenEquitiesExchangeClient:
        """Create anonymous Kraken Equities client for public data.

        Returns:
            Configured KrakenEquitiesExchangeClient (no API keys needed).
        """
        return KrakenEquitiesExchangeClient()

    def _get_exchange_name(self) -> MarketDataExchange:
        """Get exchange identifier.

        Returns:
            "kraken_equities" exchange name.
        """
        return ExchangeEnum.KRAKEN_EQUITIES

    async def start(self) -> None:
        """Start the publisher within a connector-registration context.

        Stamps ``_CURRENT_PUBLISHER`` so each ``ConnectSpotWebsocketBase``
        instance (the patched kraken-SDK class reused by Equities)
        carries the publisher's ``_get_exchange_name()`` when reserving
        an egress-pool route. This lets ``egress_pool``'s
        ``allowed_exchanges`` filter pin Equities to a specific tunnel
        (e.g. NYC) without affecting Spot or other Kraken publishers.

        Without this override the connector falls back to the
        hardcoded ``"kraken"`` tag (back-compat for Spot, which sets
        its own publisher via ``KrakenMarketDataPublisher.start``).
        The token is reset in ``finally`` to keep the ContextVar from
        leaking across publisher restarts.
        """
        token = _CURRENT_PUBLISHER.set(self)
        try:
            await super().start()
        finally:
            _CURRENT_PUBLISHER.reset(token)

    def _validate_symbols(self, symbols: list[str]) -> list[str]:
        """Validate and filter symbols for Kraken Equities.

        Converts symbols to Kraken Equities WS format to validate them.
        Invalid symbols are logged and skipped.

        Wildcard handling: ``["*"]`` expands client-side to every
        native Kraken Equities symbol currently loaded by the symbol
        mapper. The Kraken Equities WS has no server-side wildcard
        token, so expansion happens here and the underlying SDK
        subscribes per-product.

        Args:
            symbols: Input symbols in native format, or ``["*"]`` for
                subscribe-all.

        Returns:
            Valid symbols that can be streamed from Kraken Equities.
        """
        if symbols == ["*"]:
            expanded = get_available_kraken_equities_symbols()
            logger.info(
                f"KrakenEquitiesMarketDataPublisher: wildcard expansion -> "
                f"{len(expanded)} equities symbols"
            )
            return expanded
        native_symbols: list[str] = []
        seen_symbols: set[str] = set()
        for symbol in symbols:
            try:
                native_to_kraken_equities_ws(symbol)
            except ValueError:
                logger.warning(
                    f"KrakenEquitiesMarketDataPublisher: Skipping unknown native symbol {symbol}"
                )
                continue
            if symbol in seen_symbols:
                continue
            seen_symbols.add(symbol)
            native_symbols.append(symbol)
        return native_symbols

    def _get_max_symbols_per_connection(self) -> int:
        """Get WebSocket symbol limit.

        Returns:
            0 (unlimited).
        """
        return 0

    async def _candle_loop(self, symbols: list[str], timeframe: str) -> None:
        """Drive the trade-synthesized candle stream into the base loop.

        Kraken Equities has no WebSocket OHLC channel; the exchange
        client builds 1-minute candles client-side from the live
        trade stream (see
        :class:`snapper.infrastructure.exchanges._trade_candle_builder.TradeCandleBuilder`).
        REST polling per FCM contract per minute would mean hundreds
        of iapi calls and meaningful IP-ban risk; trade-based
        synthesis uses data the publisher already receives via WS
        and adds zero outbound REST traffic.

        Args:
            symbols: Native dash-separated symbols
                (e.g. ``CLM6-NYMEX``, ``MNQM6-CME``). Informational
                only — the builder emits candles for whichever
                symbols actually saw trades.
            timeframe: Snapper-style candle interval. Must be ``1m``.
        """
        await super()._candle_loop(symbols, timeframe)

    def _get_liveness_recovery_threshold_s(self) -> int:
        """Disable liveness recovery during scheduled CME closure windows."""
        if _is_cme_closed(datetime.now(UTC)):
            return 0
        return super()._get_liveness_recovery_threshold_s()

    async def _attempt_liveness_recovery(self, reason: str) -> None:
        """Recover stale market data by rebuilding the public WS client."""
        logger.error("kraken_equities publisher: liveness recovery triggered ({})", reason)
        client = self._exchange_client
        if client is not None:
            await client.disconnect()
            await client._ensure_ws_connected()
