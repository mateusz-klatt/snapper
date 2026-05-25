"""Kraken exchange market data publisher.

This module provides a market data feed publisher for the Kraken exchange.
It streams real-time ticks, trades, and candles via Kraken's WebSocket API
and publishes normalized data to the ZMQ messaging bus.

The publisher handles Kraken-specific symbol conversion and respects the
exchange's WebSocket connection limit of 20 symbols per connection.

Classes
-------
KrakenMarketDataPublisher
    RegisterableProcess for Kraken market data streaming.

Configuration
-------------
Symbols are configured via settings.instruments["kraken"].
The publisher uses public (anonymous) WebSocket connections.

Example:
-------
Register and run via process manager::

    # Configured automatically via @register_process decorator
    # or manually:
    publisher = KrakenMarketDataPublisher(symbols=["BTC-USD", "ETH-USD"])
    await publisher.start()
"""

import asyncio
import time
from collections import deque
from typing import Any
from typing import Final

from loguru import logger

from snapper.application.process_manager.process_parameters import PublisherSymbolsParameters
from snapper.application.process_manager.registry import register_process
from snapper.config.settings import AppSettings
from snapper.core.types import ExchangeEnum
from snapper.core.types import MarketDataExchange
from snapper.core.types import ProcessModeEnum
from snapper.core.types import ProcessRoleEnum
from snapper.infrastructure.exchanges.implementations.kraken import KrakenExchangeClient
from snapper.infrastructure.exchanges.kraken_sdk_patches import _CURRENT_PUBLISHER
from snapper.infrastructure.exchanges.kraken_sdk_patches import _RECONNECT_LIMIT
from snapper.infrastructure.exchanges.kraken_sdk_patches import _RECONNECT_WINDOW_S
from snapper.infrastructure.exchanges.kraken_sdk_patches import (
    apply_kraken_already_subscribed_filter,
)
from snapper.infrastructure.exchanges.kraken_sdk_patches import apply_kraken_retry_after_honoring
from snapper.infrastructure.symbols.functions import native_to_kraken_websocket
from snapper.messaging.publishers.base import MarketDataPublisherService

apply_kraken_retry_after_honoring()
apply_kraken_already_subscribed_filter()
"""Install kraken-sdk patches at module import.

Idempotent — calling multiple times is a no-op. Importing this module from
the process_manager startup path is the documented installation point per
the Phase A rollout of
``proprietary/plans/plan_2026_05_21_kraken_429_retry_after_egress_pool.md``.

* :func:`apply_kraken_retry_after_honoring` — 429 Retry-After honoring +
  reconnect watchdog (Phase A).
* :func:`apply_kraken_already_subscribed_filter` — downgrades benign
  ``Already subscribed`` race-condition warnings from the SDK to DEBUG
  so real subscription failures stay visible (Phase B).
"""

_FORCE_WS_RESTART_BACKOFF_S: Final[float] = 5.0
"""Sleep between disconnect and reconnect during in-process WS restart."""


@register_process(
    "kraken_feed_publisher",
    description="Kraken market data feed publisher",
    priority=20,
    role=ProcessRoleEnum.CORE,
    tags=("market-data", "publisher", "kraken"),
    parameters_model=PublisherSymbolsParameters,
    enabled=True,
    mode=ProcessModeEnum.THREAD,
)
class KrakenMarketDataPublisher(MarketDataPublisherService[KrakenExchangeClient]):
    """Kraken exchange market data publisher.

    Streams real-time market data from Kraken's WebSocket API and publishes
    normalized messages to ZMQ. Handles symbol conversion between native
    format (BTC-USD) and Kraken WebSocket format (XBT/USD).

    Respects Kraken's limit of 20 symbols per WebSocket connection.

    Topics Published:
        - market.kraken.{instrument}.ticks
        - market.kraken.{instrument}.trades
        - market.kraken.{instrument}.candles.{timeframe}
        - system.heartbeats.feed.kraken

    Attributes:
        Inherits all attributes from MarketDataPublisherService.

    Example:
        ::

            publisher = KrakenMarketDataPublisher(symbols=["BTC-USD"])
            await publisher.start()  # Streams until stopped
    """

    @staticmethod
    def get_default_parameters(settings: AppSettings) -> dict[str, Any]:
        """Get default parameters from settings.

        Args:
            settings: Application settings with instruments config.

        Returns:
            Dictionary with symbols list for Kraken.
        """
        instruments = settings.instruments
        kraken_symbols = instruments.get(ExchangeEnum.KRAKEN, [])
        return {
            "symbols": kraken_symbols,
        }

    def _create_exchange_client(self) -> KrakenExchangeClient:
        """Create anonymous Kraken WebSocket client.

        Returns:
            Configured KrakenExchangeClient for public data.
        """
        return KrakenExchangeClient()

    def _get_exchange_name(self) -> MarketDataExchange:
        """Get exchange identifier.

        Returns:
            "kraken" exchange name.
        """
        return ExchangeEnum.KRAKEN

    def _validate_symbols(self, symbols: list[str]) -> list[str]:
        """Validate and filter symbols for Kraken.

        Converts symbols to Kraken WebSocket format to validate them.
        Invalid symbols are logged and skipped. The wildcard ``["*"]``
        passes through untouched — Kraken's WebSocket accepts ``"*"``
        as a subscribe-all sentinel (already used by
        :class:`KrakenSnapshotUpdaterService`) so the publisher
        forwards it verbatim and bypasses both per-symbol mapping and
        the 20-symbol-per-connection limit.

        Args:
            symbols: Input symbols in native format, or ``["*"]``.

        Returns:
            Valid symbols that can be streamed from Kraken, or
            ``["*"]`` when the caller requested subscribe-all.
        """
        if symbols == ["*"]:
            return ["*"]
        native_symbols: list[str] = []
        seen_symbols: set[str] = set()
        for symbol in symbols:
            try:
                native_to_kraken_websocket(symbol)
            except ValueError:
                logger.warning(
                    f"KrakenMarketDataPublisher: Skipping unknown native symbol {symbol}"
                )
                continue
            if symbol in seen_symbols:
                continue
            seen_symbols.add(symbol)
            native_symbols.append(symbol)
        return native_symbols

    def _get_max_symbols_per_connection(self) -> int:
        """Get Kraken's WebSocket symbol limit.

        Returns:
            ``0`` when ``self.symbols == ["*"]`` (wildcard subscribe-all
            uses a single connection unconstrained by the per-symbol
            limit) or ``20`` for the explicit-symbol path.
        """
        if self.symbols == ["*"]:
            return 0
        return 20

    def __init__(self, symbols: list[str]) -> None:
        """Initialise the publisher with reconnect-storm watchdog state.

        Args:
            symbols: Native symbols to subscribe to, or ``["*"]`` for
                wildcard subscribe-all.
        """
        super().__init__(symbols)
        self._reconnect_timestamps: deque[float] = deque(maxlen=_RECONNECT_LIMIT * 2)
        self._restart_lock = asyncio.Lock()
        self._force_ws_restart_task: asyncio.Task[None] | None = None

    async def start(self) -> None:
        """Start the publisher within a connector-registration context.

        Stamps ``_CURRENT_PUBLISHER`` so each ``ConnectSpotWebsocketBase``
        instance constructed during startup is registered against this
        publisher via the patched ``__init__``. The token is reset in
        the ``finally`` clause so the ContextVar does not leak across
        publisher restarts.
        """
        token = _CURRENT_PUBLISHER.set(self)
        try:
            await super().start()
        finally:
            _CURRENT_PUBLISHER.reset(token)

    def _on_sdk_reconnect_attempt(self) -> None:
        """Hook called by the patched SDK reconnect path on every attempt.

        Records the reconnect timestamp and schedules an in-process WS
        restart when more than ``_RECONNECT_LIMIT`` attempts fall inside
        the rolling ``_RECONNECT_WINDOW_S`` window. The publisher process
        itself stays alive; only the WebSocket client is torn down and
        rebuilt via the existing
        ``KrakenExchangeClient.disconnect_websocket`` +
        ``_ensure_ws_connected`` cycle. Per the clean-signal-log rule the
        timestamp deque is cleared after a restart trigger so back-to-back
        storms do not double-fire.
        """
        now = time.monotonic()
        self._reconnect_timestamps.append(now)
        cutoff = now - _RECONNECT_WINDOW_S
        recent = [t for t in self._reconnect_timestamps if t >= cutoff]
        if len(recent) >= _RECONNECT_LIMIT:
            logger.error(
                "kraken publisher: {} reconnects in {}s — forcing WS restart",
                len(recent),
                _RECONNECT_WINDOW_S,
            )
            self._reconnect_timestamps.clear()
            self._force_ws_restart_task = asyncio.create_task(self._force_ws_restart())

    async def _force_ws_restart(self) -> None:
        """Tear down the WS client and re-establish via existing lifecycle.

        Uses ``KrakenExchangeClient.disconnect_websocket`` for teardown
        and ``_ensure_ws_connected`` for the rebuild — both are pre-existing
        methods on the exchange client, so no new public surface is
        introduced. A 5-second back-off between disconnect and reconnect
        gives the SDK a moment to settle internal state and gives
        Cloudflare time to release any in-flight 429 tracking against
        the same source IP.
        """
        async with self._restart_lock:
            client = self._exchange_client
            if client is None:
                return
            try:
                await client.disconnect_websocket()
            except Exception:
                logger.exception("kraken publisher: disconnect_websocket failed during restart")
            await asyncio.sleep(_FORCE_WS_RESTART_BACKOFF_S)
            await client._ensure_ws_connected()

    async def _attempt_liveness_recovery(self, reason: str) -> None:
        """Recover stale market data by forcing a WS restart."""
        logger.error("kraken publisher: liveness recovery triggered ({})", reason)
        await self._force_ws_restart()
