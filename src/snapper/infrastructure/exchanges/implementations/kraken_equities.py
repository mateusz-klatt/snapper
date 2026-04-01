"""Kraken Equities (FCM Futures) exchange client implementation.

This module provides KrakenEquitiesExchangeClient, a market-data-only client
for traditional commodity/index futures on Kraken's equities platform. It supports:

REST API Operations:
    - Instrument metadata (via internal ``iapi.kraken.com`` API)

WebSocket Subscriptions (via SpotWSClient with overridden URL):
    - Public: tickers, trades

The Kraken Equities WebSocket uses the same v2 protocol as Kraken Spot,
with an additional ``asset_class`` field. This implementation reuses the
``SpotWSClient`` from the Kraken SDK by pointing it at
``wss://ws-equities.kraken.com``.

Phase 1 Limitations:
    - No order execution (create_order, cancel_order raise NotImplementedError)
    - No execution subscriptions (subscribe_executions raises NotImplementedError)
    - Candle subscriptions not available (use REST polling if needed)
    - supports_websocket_executions = False
    - Data is delayed (~10 minutes)
"""

import asyncio
from collections.abc import AsyncIterator
from typing import Any

import httpx
from kraken.spot import SpotWSClient
from loguru import logger

from snapper.core.types import ExchangeEnum
from snapper.data.repository import Repository
from snapper.infrastructure.exchanges.adapters.kraken_equities import (
    parse_kraken_equities_instrument,
)
from snapper.infrastructure.exchanges.adapters.kraken_equities import parse_kraken_equities_ticker
from snapper.infrastructure.exchanges.adapters.kraken_equities import parse_kraken_equities_trade
from snapper.infrastructure.exchanges.base import ExchangeClientBase
from snapper.infrastructure.exchanges.contracts import AccountBalance
from snapper.infrastructure.exchanges.contracts import CandleUpdate
from snapper.infrastructure.exchanges.contracts import ExchangeOrderRequest
from snapper.infrastructure.exchanges.contracts import ExchangeOrderSnapshot
from snapper.infrastructure.exchanges.contracts import ExecutionUpdate
from snapper.infrastructure.exchanges.contracts import InstrumentPairDescriptor
from snapper.infrastructure.exchanges.contracts import OhlcvSnapshot
from snapper.infrastructure.exchanges.contracts import OrderStatusEnum
from snapper.infrastructure.exchanges.contracts import TickerSnapshot
from snapper.infrastructure.exchanges.contracts import TickerUpdate
from snapper.infrastructure.exchanges.contracts import TradeUpdate
from snapper.infrastructure.symbols.functions import native_to_kraken_equities_ws

_NOT_IMPLEMENTED_MSG = "Order execution not available for Kraken Equities (market data only)"
_QUEUE_DRAIN_TIMEOUT = 0.1
_QUEUE_MAX_SIZE = 10000
_WS_URL = "wss://ws-equities.kraken.com"
_INSTRUMENTS_URL = "https://iapi.kraken.com/api/internal/markets/all/futures-contracts"
_INSTRUMENTS_HEADERS = {
    "accept": "application/json",
    "origin": "https://pro.kraken.com",
    "referer": "https://pro.kraken.com/",
}


def _enqueue_or_drop_oldest(queue: asyncio.Queue[Any], item: Any, label: str) -> None:
    """Put item on queue, dropping the oldest if full.

    Args:
        queue: Bounded asyncio queue.
        item: Item to enqueue.
        label: Human-readable label for the warning log.
    """
    try:
        queue.put_nowait(item)
    except asyncio.QueueFull:
        logger.warning(f"{label} queue full, dropping oldest message")
        queue.get_nowait()
        queue.put_nowait(item)


class KrakenEquitiesExchangeClient(ExchangeClientBase):
    """Kraken Equities exchange client for FCM commodity/index futures.

    Reuses SpotWSClient with ``ws_url`` pointed at ``ws-equities.kraken.com``.
    The WS v2 protocol is identical to Kraken Spot, with an additional
    ``asset_class: "futures_contract"`` param in subscriptions.

    Attributes:
        supports_websocket_executions: Always False (market data only).
    """

    supports_websocket_executions: bool = False

    def __init__(
        self,
        repository: Repository | None = None,
    ) -> None:
        """Initialize Kraken Equities exchange client.

        Args:
            repository: Database repository for logging (optional).
        """
        super().__init__(repository=repository, exchange_name=ExchangeEnum.KRAKEN_EQUITIES)
        self._ws_client: SpotWSClient | None = None
        self._tick_queue: asyncio.Queue[TickerUpdate] = asyncio.Queue(maxsize=_QUEUE_MAX_SIZE)
        self._trade_queue: asyncio.Queue[TradeUpdate] = asyncio.Queue(maxsize=_QUEUE_MAX_SIZE)

    async def connect(self) -> None:
        """Establish connection (no-op until WS subscription).

        The SpotWSClient is created lazily on first subscription.
        """
        logger.info("Kraken Equities client ready")

    async def disconnect(self) -> None:
        """Close all Kraken Equities connections."""
        if self._ws_client:
            try:
                await self._ws_client.close()
            except Exception as e:
                logger.warning(f"Error closing Kraken Equities WS: {e}")
            self._ws_client = None
        logger.info("Kraken Equities connections closed")

    async def _on_ws_message(self, message: dict[str, Any] | list[Any]) -> None:
        """Route incoming WS messages to the appropriate queue.

        This is the callback passed to SpotWSClient. It dispatches
        ticker and trade updates to their respective queues.

        Args:
            message: Parsed WebSocket message.
        """
        if isinstance(message, list) or not isinstance(message, dict):
            return
        channel = message.get("channel", "")
        msg_type = message.get("type", "")
        if channel == "ticker" and msg_type in ("snapshot", "update"):
            for item in message.get("data", []):
                try:
                    tick = parse_kraken_equities_ticker(item)
                    _enqueue_or_drop_oldest(self._tick_queue, tick, "Tick")
                except (ValueError, KeyError) as exc:
                    logger.debug(f"Skipping unparseable equities ticker: {exc}")
        elif channel == "trade" and msg_type in ("snapshot", "update"):
            for item in message.get("data", []):
                try:
                    trade = parse_kraken_equities_trade(item)
                    _enqueue_or_drop_oldest(self._trade_queue, trade, "Trade")
                except (ValueError, KeyError) as exc:
                    logger.debug(f"Skipping unparseable equities trade: {exc}")

    async def _ensure_ws_connected(self) -> None:
        """Connect the SpotWSClient if not already connected."""
        if self._ws_client is not None:
            return
        self._ws_client = SpotWSClient(
            ws_url=_WS_URL,
            callback=self._on_ws_message,
            no_public=False,
        )
        await self._ws_client.start()
        logger.info("Kraken Equities WebSocket connected")

    async def get_ticker(self, symbol: str) -> TickerSnapshot:
        """Fetch current ticker (not implemented for equities REST).

        Args:
            symbol: Symbol (unused).

        Returns:
            Never returns; always raises.

        Raises:
            NotImplementedError: Always. Use WS subscription instead.
        """
        raise NotImplementedError("Use WebSocket ticker subscription for Kraken Equities")

    async def get_ohlcv(
        self,
        symbol: str,
        timeframe: str = "1m",
        since: int | None = None,
        limit: int | None = None,
    ) -> list[OhlcvSnapshot]:
        """Fetch OHLCV candles (not implemented for equities).

        Args:
            symbol: Contract symbol (unused).
            timeframe: Candle interval (unused).
            since: Start timestamp (unused).
            limit: Maximum candles (unused).

        Returns:
            Never returns; always raises.

        Raises:
            NotImplementedError: Always.
        """
        raise NotImplementedError("OHLCV not available for Kraken Equities")

    async def create_order(self, request: ExchangeOrderRequest) -> ExchangeOrderSnapshot:
        """Submit a new order (not available).

        Args:
            request: Order parameters (unused).

        Returns:
            Never returns; always raises.

        Raises:
            NotImplementedError: Always.
        """
        raise NotImplementedError(_NOT_IMPLEMENTED_MSG)

    async def cancel_order(self, order_id: str, symbol: str | None = None) -> ExchangeOrderSnapshot:
        """Cancel an existing order (not available).

        Args:
            order_id: Exchange order ID (unused).
            symbol: Symbol filter (unused).

        Returns:
            Never returns; always raises.

        Raises:
            NotImplementedError: Always.
        """
        raise NotImplementedError(_NOT_IMPLEMENTED_MSG)

    async def get_order(self, order_id: str, symbol: str | None = None) -> ExchangeOrderSnapshot:
        """Fetch order details (not available).

        Args:
            order_id: Exchange order ID (unused).
            symbol: Symbol filter (unused).

        Returns:
            Never returns; always raises.

        Raises:
            NotImplementedError: Always.
        """
        raise NotImplementedError(_NOT_IMPLEMENTED_MSG)

    async def get_orders(
        self,
        symbol: str | None = None,
        status: OrderStatusEnum | None = None,
        limit: int | None = None,
    ) -> list[ExchangeOrderSnapshot]:
        """Fetch orders (not available).

        Args:
            symbol: Symbol filter (unused).
            status: Status filter (unused).
            limit: Maximum results (unused).

        Returns:
            Never returns; always raises.

        Raises:
            NotImplementedError: Always.
        """
        raise NotImplementedError(_NOT_IMPLEMENTED_MSG)

    async def get_balance(self, currency: str | None = None) -> dict[str, AccountBalance]:
        """Fetch account balance (not available).

        Args:
            currency: Currency filter (unused).

        Returns:
            Never returns; always raises.

        Raises:
            NotImplementedError: Always.
        """
        raise NotImplementedError(_NOT_IMPLEMENTED_MSG)

    def subscribe_ticks(self, symbols: list[str]) -> AsyncIterator[TickerUpdate]:
        """Subscribe to real-time ticker updates via WebSocket.

        Args:
            symbols: Native symbols (e.g., ``CLM6-NYMEX``) -- converted
                to Kraken Equities format internally.

        Returns:
            AsyncIterator yielding TickerUpdate for each price change.
        """
        return self._subscribe_ticks_impl(symbols)

    async def _subscribe_ticks_impl(self, symbols: list[str]) -> AsyncIterator[TickerUpdate]:
        """Implement ticker subscription via SpotWSClient.

        Args:
            symbols: Native symbols to subscribe.

        Yields:
            TickerUpdate for each price change.
        """
        await self._ensure_ws_connected()
        if self._ws_client is None:
            raise RuntimeError("WebSocket client not connected")
        ws_symbols = [native_to_kraken_equities_ws(s) for s in symbols]
        await self._ws_client.subscribe(
            params={
                "channel": "ticker",
                "symbol": ws_symbols,
                "snapshot": True,
                "throttle": 1000,
                "asset_class": "futures_contract",
            }
        )
        logger.info(f"Subscribed to Kraken Equities tickers: {symbols} -> {ws_symbols}")
        try:
            while True:
                try:
                    message = await asyncio.wait_for(
                        self._tick_queue.get(), timeout=_QUEUE_DRAIN_TIMEOUT
                    )
                    yield message
                except TimeoutError:
                    await asyncio.sleep(0.01)
        finally:
            if self._ws_client:
                try:
                    await self._ws_client.subscribe(
                        params={
                            "channel": "ticker",
                            "symbol": ws_symbols,
                            "snapshot": True,
                            "throttle": 1000,
                            "asset_class": "futures_contract",
                        }
                    )
                except Exception:
                    logger.debug("Failed to unsubscribe from equities tickers on cleanup")

    def subscribe_candles(
        self,
        symbols: list[str],
        timeframe: str = "1m",
    ) -> AsyncIterator[CandleUpdate]:
        """Subscribe to candle updates (not available for Kraken Equities).

        Args:
            symbols: Contract symbols (unused).
            timeframe: Candle interval (unused).

        Returns:
            Never returns; always raises.

        Raises:
            NotImplementedError: Always.
        """
        raise NotImplementedError("Kraken Equities candle subscription not implemented")

    def subscribe_trades(self, symbols: list[str]) -> AsyncIterator[TradeUpdate]:
        """Subscribe to real-time trade updates via WebSocket.

        Args:
            symbols: Native symbols (e.g., ``CLM6-NYMEX``).

        Returns:
            AsyncIterator yielding TradeUpdate for each trade.
        """
        return self._subscribe_trades_impl(symbols)

    async def _subscribe_trades_impl(self, symbols: list[str]) -> AsyncIterator[TradeUpdate]:
        """Implement trade subscription via SpotWSClient.

        Args:
            symbols: Native symbols to subscribe.

        Yields:
            TradeUpdate for each execution.
        """
        await self._ensure_ws_connected()
        if self._ws_client is None:
            raise RuntimeError("WebSocket client not connected")
        ws_symbols = [native_to_kraken_equities_ws(s) for s in symbols]
        await self._ws_client.subscribe(
            params={
                "channel": "trade",
                "symbol": ws_symbols,
                "snapshot": True,
                "throttle": 1000,
                "asset_class": "futures_contract",
            }
        )
        logger.info(f"Subscribed to Kraken Equities trades: {symbols} -> {ws_symbols}")
        try:
            while True:
                try:
                    message = await asyncio.wait_for(
                        self._trade_queue.get(), timeout=_QUEUE_DRAIN_TIMEOUT
                    )
                    yield message
                except TimeoutError:
                    await asyncio.sleep(0.01)
        finally:
            if self._ws_client:
                try:
                    await self._ws_client.subscribe(
                        params={
                            "channel": "trade",
                            "symbol": ws_symbols,
                            "snapshot": True,
                            "throttle": 1000,
                            "asset_class": "futures_contract",
                        }
                    )
                except Exception:
                    logger.debug("Failed to unsubscribe from equities trades on cleanup")

    def subscribe_executions(self) -> AsyncIterator[ExecutionUpdate]:
        """Subscribe to execution updates (not available).

        Returns:
            Never returns; always raises.

        Raises:
            NotImplementedError: Always.
        """
        raise NotImplementedError(_NOT_IMPLEMENTED_MSG)

    def subscribe_instruments(self, **kwargs: Any) -> AsyncIterator[dict[str, Any]]:
        """Fetch instruments from REST and yield each as a dict.

        Returns:
            AsyncIterator yielding raw instrument dicts.
        """
        return self._subscribe_instruments_impl()

    async def _subscribe_instruments_impl(self) -> AsyncIterator[dict[str, Any]]:
        """Fetch FCM instruments from internal REST API.

        Yields:
            Raw instrument dict for each tradable contract.
        """
        instruments = await self._fetch_instruments_rest()
        for inst in instruments:
            yield inst

    async def _fetch_instruments_rest(self) -> list[dict[str, Any]]:
        """Fetch all FCM futures contracts from Kraken internal API.

        Returns:
            List of raw instrument dicts (tradable + active only).
        """
        async with httpx.AsyncClient(timeout=30.0) as client:
            response = await client.get(
                _INSTRUMENTS_URL,
                params={"delayed": "true"},
                headers=_INSTRUMENTS_HEADERS,
            )
            response.raise_for_status()
            data = response.json()
        result = data.get("result", {})
        contracts: list[dict[str, Any]] = result.get("data", [])
        active = [c for c in contracts if c.get("tradable") and c.get("status") == "active"]
        logger.info(f"Fetched {len(active)} active FCM contracts (of {len(contracts)} total)")
        return active

    def get_instruments_sync(self) -> list[dict[str, Any]]:
        """Fetch all instruments synchronously via REST.

        Returns:
            List of raw instrument dicts from Kraken internal API.
        """
        with httpx.Client(timeout=30.0) as client:
            response = client.get(
                _INSTRUMENTS_URL,
                params={"delayed": "true"},
                headers=_INSTRUMENTS_HEADERS,
            )
            response.raise_for_status()
            data = response.json()
        result = data.get("result", {})
        contracts: list[dict[str, Any]] = result.get("data", [])
        return [c for c in contracts if c.get("tradable") and c.get("status") == "active"]

    def get_parsed_instrument(self, data: dict[str, Any]) -> InstrumentPairDescriptor:
        """Parse a raw instrument dict into InstrumentPairDescriptor.

        Args:
            data: Raw instrument dict from REST API.

        Returns:
            Parsed InstrumentPairDescriptor.
        """
        return parse_kraken_equities_instrument(data)
