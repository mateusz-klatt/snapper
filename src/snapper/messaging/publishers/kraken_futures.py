"""Kraken Futures exchange market data publisher.

This module provides a market data feed publisher for the Kraken Futures
exchange. It streams real-time ticks and trades via Kraken's Futures
WebSocket API, derives 1-minute candles from the live trade stream, and
publishes normalized data to the ZMQ messaging bus.

Kraken Futures has no WebSocket candle channel. The exchange client
aggregates live trades into 1-minute candles; historical and
multi-interval OHLCV still use ``get_ohlcv()`` through REST/CCXT
backfill paths.

Configuration
-------------
Symbols are configured via settings.instruments["kraken_futures"].
The publisher uses public (anonymous) WebSocket connections.
"""

import asyncio
from datetime import UTC
from datetime import datetime
from typing import Any
from typing import Final
from uuid import uuid7

from loguru import logger
from sqlalchemy import select

from snapper.application.process_manager.process_parameters import PublisherSymbolsParameters
from snapper.application.process_manager.registry import register_process
from snapper.config.settings import AppSettings
from snapper.core.types import ExchangeEnum
from snapper.core.types import MarketDataExchange
from snapper.core.types import ProcessModeEnum
from snapper.core.types import ProcessRestartPolicyEnum
from snapper.core.types import ProcessRoleEnum
from snapper.data.models import SymbolMarketDataChannelCapability
from snapper.data.repository import Repository
from snapper.data.repository import close_and_insert
from snapper.infrastructure.exchanges._subscription_health import _SymbolEntry
from snapper.infrastructure.exchanges.implementations.kraken_futures import (
    KrakenFuturesExchangeClient,
)
from snapper.infrastructure.exchanges.kraken_sdk_patches import apply_kraken_futures_pool_routing
from snapper.infrastructure.network.egress_context import _CURRENT_PUBLISHER
from snapper.infrastructure.symbols.functions import get_available_kraken_futures_symbols
from snapper.infrastructure.symbols.functions import kraken_futures_ws_to_native
from snapper.infrastructure.symbols.functions import native_to_kraken_futures_ws
from snapper.infrastructure.symbols.functions import resolve_symbol_public_id
from snapper.messaging.publishers.base import MarketDataPublisherService
from snapper.messaging.schemas.data import SymbolAliasUpdateData
from snapper.messaging.topics.builders import system_topic

apply_kraken_futures_pool_routing()

_LIVENESS_RECOVERY_THRESHOLD_S: Final[int] = 60
"""Message-silence threshold (seconds) before Futures liveness recovery fires.

Lowered from the 300 s base default: the Futures trade feed aggregates the
whole perpetuals universe, which trades effectively continuously, so a 60 s
silence reliably indicates a dark feed rather than a quiet market, and a
multi-minute outage is detected and recovered in ~1 minute rather than five."""
_RUNTIME_CHANNEL_SOURCE: Final[str] = "kraken_futures_publisher_runtime"
_RUNTIME_CHANNEL_REASON: Final[str] = "Learned: trade not confirmed while ticker confirmed"
_RUNTIME_TERMINAL_TRACKER_REASON: Final[str] = "runtime learned trade channel unavailable"


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
        - market.kraken_futures.{instrument}.candles.1m
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

    def _candle_source_for(self, timeframe: str) -> str:
        """Kraken futures builds its 1m bars from the live trade stream.

        Args:
            timeframe: The candle timeframe label.

        Returns:
            ``calculated`` — these 1m bars are Snapper-computed (via
            ``TradeCandleBuilder``), not venue-precomputed OHLC.
        """
        return "calculated"

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

    def _symbols_for_trade_loop(self, symbols: list[str]) -> list[str]:
        """Filter Kraken Futures trade subscriptions by channel capability.

        Args:
            symbols: Symbol-level market-data universe selected for this
                publisher instance.

        Returns:
            Symbols whose ``trade`` channel is currently allowed.
        """
        allowed = set(get_available_kraken_futures_symbols(channel="trade"))
        return [symbol for symbol in symbols if symbol in allowed]

    def _invalidate_symbol_cache(self) -> None:
        """Refresh mapper caches and re-probe newly allowed trade symbols."""
        super()._invalidate_symbol_cache()
        if not self.running:
            return
        task = asyncio.create_task(self._reprobe_trade_symbols())
        task.add_done_callback(self._log_reprobe_task_result)

    @staticmethod
    def _log_reprobe_task_result(task: asyncio.Task[None]) -> None:
        """Log an unexpected re-probe task failure.

        Args:
            task: Completed async task.

        Returns:
            None.
        """
        try:
            task.result()
        except asyncio.CancelledError:
            return
        except Exception as exc:
            logger.warning("Kraken Futures trade re-probe task failed: {}", exc)

    async def _reprobe_trade_symbols(self) -> None:
        """Re-subscribe trade products whose channel capability reopened."""
        client = self._exchange_client
        if client is None:
            return
        reprobed = 0
        for symbol in self._symbols_for_trade_loop(self.symbols):
            try:
                product = native_to_kraken_futures_ws(symbol)
            except ValueError:
                continue
            if await client.reprobe_public_subscription("trade", product):
                reprobed += 1
        if reprobed:
            logger.info("Kraken Futures re-probed {} trade subscription(s)", reprobed)

    async def _after_feed_health_snapshot(
        self, snapshot: dict[tuple[str, str], _SymbolEntry]
    ) -> None:
        """Learn unsupported trade channels from stable subscription failures.

        Args:
            snapshot: Point-in-time subscription-health entries.

        Returns:
            None.
        """
        client = self._exchange_client
        repository = self.repository
        if client is None or repository is None:
            return
        learned = False
        for entry in snapshot.values():
            if not self._should_learn_trade_channel_false(entry, snapshot):
                continue
            try:
                native_symbol = kraken_futures_ws_to_native(entry.symbol)
                persisted = await self._persist_runtime_trade_channel_false(
                    repository,
                    native_symbol,
                )
            except Exception as exc:
                logger.warning(
                    "Kraken Futures runtime trade capability learning failed for {}: {}",
                    entry.symbol,
                    exc,
                )
                continue
            if not persisted:
                continue
            client.suppress_public_subscription(
                "trade",
                entry.symbol,
                _RUNTIME_TERMINAL_TRACKER_REASON,
            )
            learned = True
        if learned:
            await self._broadcast_runtime_capability_invalidation()

    @staticmethod
    def _should_learn_trade_channel_false(
        entry: _SymbolEntry,
        snapshot: dict[tuple[str, str], _SymbolEntry],
    ) -> bool:
        """Return True when a failed trade entry is safe to persist as false."""
        if entry.channel != "trade":
            return False
        if entry.status != "failed" or entry.last_error != "retry budget exhausted":
            return False
        if entry.slow_retry_count < 1:
            return False
        ticker = snapshot.get(("ticker", entry.symbol))
        return (
            ticker is not None
            and ticker.status == "confirmed"
            and ticker.last_seen_data_at is not None
        )

    async def _persist_runtime_trade_channel_false(
        self,
        repository: Repository,
        native_symbol: str,
    ) -> bool:
        """Persist or confirm a runtime-learned ``trade=False`` channel row.

        Args:
            repository: Async repository used by the publisher.
            native_symbol: Snapper native symbol for the failed product.

        Returns:
            True when the row exists or was written, False when the symbol
            identity cannot be resolved.
        """
        now = datetime.now(UTC)
        symbol_public_id = await resolve_symbol_public_id(repository, native_symbol, now)
        if symbol_public_id is None:
            logger.warning(
                "Kraken Futures runtime trade learning skipped unresolved symbol {}",
                native_symbol,
            )
            return False
        async with repository.session() as session:
            result = await session.execute(
                select(SymbolMarketDataChannelCapability).where(
                    SymbolMarketDataChannelCapability.symbol_public_id == symbol_public_id,
                    SymbolMarketDataChannelCapability.exchange == ExchangeEnum.KRAKEN_FUTURES,
                    SymbolMarketDataChannelCapability.channel == "trade",
                    SymbolMarketDataChannelCapability.timestamp <= now,
                    SymbolMarketDataChannelCapability.known_to > now,
                )
            )
            existing = result.scalar_one_or_none()
            if (
                existing is not None
                and existing.can_market_data is False
                and existing.source != _RUNTIME_CHANNEL_SOURCE
            ):
                return True
            if (
                existing is not None
                and existing.can_market_data is False
                and existing.source == _RUNTIME_CHANNEL_SOURCE
                and existing.reason == _RUNTIME_CHANNEL_REASON
            ):
                return True
            await close_and_insert(
                session=session,
                model=SymbolMarketDataChannelCapability,
                match_filters=[
                    SymbolMarketDataChannelCapability.symbol_public_id == symbol_public_id,
                    SymbolMarketDataChannelCapability.exchange == ExchangeEnum.KRAKEN_FUTURES,
                    SymbolMarketDataChannelCapability.channel == "trade",
                ],
                new_values={
                    "symbol_public_id": symbol_public_id,
                    "exchange": ExchangeEnum.KRAKEN_FUTURES,
                    "channel": "trade",
                    "can_market_data": False,
                    "source": _RUNTIME_CHANNEL_SOURCE,
                    "reason": _RUNTIME_CHANNEL_REASON,
                    "created_at": existing.created_at if existing is not None else now,
                    "session_id": self._tracker.session_id,
                    "sequence_id": self._tracker.next_sequence("channel_capabilities"),
                },
                bus_time=now,
            )
            await session.commit()
        logger.info(
            "Kraken Futures learned trade channel unavailable for {}",
            native_symbol,
        )
        return True

    async def _broadcast_runtime_capability_invalidation(self) -> None:
        """Broadcast the existing symbol-cache invalidation topic."""
        self._invalidate_symbol_cache()
        publisher = self.msg_publisher
        if publisher is None:
            return
        topic = system_topic("symbol_aliases")
        envelope = SymbolAliasUpdateData(
            public_id=str(uuid7()),
            timestamp=datetime.now(UTC),
            session_id=publisher.tracker.session_id,
            sequence_id=publisher.tracker.next_sequence(topic),
        )
        await publisher.send(topic, envelope)

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

    def _get_liveness_recovery_threshold_s(self) -> int:
        """Return the Futures message-silence threshold before recovery fires.

        Returns:
            ``_LIVENESS_RECOVERY_THRESHOLD_S`` seconds.
        """
        return _LIVENESS_RECOVERY_THRESHOLD_S
