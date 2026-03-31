"""Kraken Futures exchange client implementation.

This module provides KrakenFuturesExchangeClient, a market-data-only client
for the Kraken Futures exchange. It supports:

REST API Operations:
    - Market data: tickers, OHLCV candles (via CCXT krakenfutures)
    - Instrument metadata (via kraken.futures.Market)

WebSocket Subscriptions (via callback-to-queue bridge):
    - Public: tickers, trades

The Kraken Futures SDK uses a callback-driven WebSocket client. This
implementation bridges callbacks to asyncio.Queue objects so that the
publisher can consume data via async iterators (matching Snapper's
established pattern).

Phase 1 Limitations:
    - No order execution (create_order, cancel_order raise NotImplementedError)
    - No execution subscriptions (subscribe_executions raises NotImplementedError)
    - Candle subscriptions not available via WS (use get_ohlcv REST instead)
    - supports_websocket_executions = False
"""

import asyncio
from collections.abc import AsyncIterator
from typing import Any
from typing import cast

import ccxt
from kraken.futures import FuturesWSClient
from kraken.futures import Market
from loguru import logger

from snapper.core.types import ExchangeEnum
from snapper.data.repository import Repository
from snapper.infrastructure.exchanges.adapters.kraken_futures import parse_kraken_futures_instrument
from snapper.infrastructure.exchanges.adapters.kraken_futures import parse_kraken_futures_ticker
from snapper.infrastructure.exchanges.adapters.kraken_futures import parse_kraken_futures_trade
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
from snapper.infrastructure.symbols.functions import native_to_kraken_futures_ws

_NOT_IMPLEMENTED_MSG = "Order execution not available in Phase 1 (market data only)"
_QUEUE_DRAIN_TIMEOUT = 0.1


class KrakenFuturesExchangeClient(ExchangeClientBase):
    """Kraken Futures exchange client for market data.

    Bridges the callback-driven FuturesWSClient to asyncio.Queue-based
    async iterators matching Snapper's publisher consumption pattern.

    Attributes:
        sandbox: Whether to use the demo/sandbox environment.
    """

    supports_websocket_executions: bool = False

    def __init__(
        self,
        sandbox: bool = False,
        repository: Repository | None = None,
    ) -> None:
        """Initialize Kraken Futures exchange client.

        Args:
            sandbox: Use sandbox environment for testing (default: False).
            repository: Database repository for order/execution logging.
        """
        super().__init__(repository=repository, exchange_name=ExchangeEnum.KRAKEN_FUTURES)
        self.sandbox = sandbox
        self._ccxt_client = cast(Any, ccxt.krakenfutures)({"sandbox": sandbox, "timeout": 30000})
        self._market_client: Market | None = None
        self._ws_client: FuturesWSClient | None = None
        self._tick_queue: asyncio.Queue[TickerUpdate] = asyncio.Queue()
        self._trade_queue: asyncio.Queue[TradeUpdate] = asyncio.Queue()
        self._instrument_queue: asyncio.Queue[dict[str, Any]] = asyncio.Queue()

    async def connect(self) -> None:
        """Establish REST connection to Kraken Futures.

        Loads markets via CCXT and creates the Market REST client.

        Raises:
            RuntimeError: If connection fails.
        """
        try:
            await asyncio.to_thread(self._ccxt_client.load_markets)
            self._market_client = Market(sandbox=self.sandbox)
            logger.info("Kraken Futures REST connection established")
        except Exception as e:
            logger.error(f"Failed to connect to Kraken Futures: {e}")
            raise

    async def disconnect(self) -> None:
        """Close all Kraken Futures connections."""
        if self._ws_client:
            try:
                await self._ws_client.async_close()
            except Exception as e:
                logger.warning(f"Error closing Kraken Futures WS: {e}")
            self._ws_client = None
        logger.info("Kraken Futures connections closed")

    async def _on_ws_message(self, message: dict[str, Any]) -> None:
        """Route incoming WS messages to the appropriate queue.

        This is the callback passed to FuturesWSClient. It parses the
        feed type and dispatches to tick_queue or trade_queue.

        Args:
            message: Raw WebSocket message dictionary.
        """
        if "event" in message:
            return
        feed = message.get("feed", "")
        if feed in ("ticker", "ticker_lite"):
            try:
                if "symbol" not in message and "product_id" in message:
                    message["symbol"] = message["product_id"]
                tick = parse_kraken_futures_ticker(message)
                self._tick_queue.put_nowait(tick)
            except (ValueError, KeyError) as exc:
                logger.debug(f"Skipping unparseable ticker WS message: {exc}")
        elif feed == "trade":
            product_id = message.get("product_id", "")
            for raw_trade in message.get("trades", []):
                try:
                    raw_trade["product_id"] = product_id
                    trade = parse_kraken_futures_trade(raw_trade)
                    self._trade_queue.put_nowait(trade)
                except (ValueError, KeyError) as exc:
                    logger.debug(f"Skipping unparseable trade WS message: {exc}")

    async def _ensure_ws_connected(self) -> None:
        """Connect the FuturesWSClient if not already connected."""
        if self._ws_client is not None:
            return
        self._ws_client = FuturesWSClient(callback=self._on_ws_message, sandbox=self.sandbox)
        await self._ws_client.start()
        logger.info("Kraken Futures WebSocket connected")

    async def get_ticker(self, symbol: str) -> TickerSnapshot:
        """Fetch current ticker via CCXT.

        Args:
            symbol: CCXT-format symbol (e.g., ``BTC/USD:USD``).

        Returns:
            TickerSnapshot with current price data.
        """
        data = await asyncio.to_thread(self._ccxt_client.fetch_ticker, symbol)
        return TickerSnapshot(
            symbol=symbol,
            bid=float(data.get("bid") or 0),
            ask=float(data.get("ask") or 0),
            last=float(data.get("last") or 0),
            timestamp=float(data.get("timestamp") or 0) / 1000,
        )

    async def get_ohlcv(
        self,
        symbol: str,
        timeframe: str = "1m",
        since: int | None = None,
        limit: int | None = None,
    ) -> list[OhlcvSnapshot]:
        """Fetch OHLCV candles via CCXT.

        Args:
            symbol: CCXT-format symbol.
            timeframe: Candle interval (e.g., ``1m``, ``1h``).
            since: Start timestamp in milliseconds.
            limit: Maximum number of candles.

        Returns:
            List of OhlcvSnapshot objects.
        """
        raw = await asyncio.to_thread(
            self._ccxt_client.fetch_ohlcv, symbol, timeframe, since, limit
        )
        return [
            OhlcvSnapshot(
                timestamp=candle[0] / 1000,
                open=float(candle[1]),
                high=float(candle[2]),
                low=float(candle[3]),
                close=float(candle[4]),
                volume=float(candle[5]),
            )
            for candle in raw
        ]

    async def create_order(self, request: ExchangeOrderRequest) -> ExchangeOrderSnapshot:
        """Submit a new order (not available in Phase 1).

        Args:
            request: Order parameters.

        Returns:
            Never returns; always raises.

        Raises:
            NotImplementedError: Always.
        """
        raise NotImplementedError(_NOT_IMPLEMENTED_MSG)

    async def cancel_order(self, order_id: str, symbol: str | None = None) -> ExchangeOrderSnapshot:
        """Cancel an existing order (not available in Phase 1).

        Args:
            order_id: Exchange order ID to cancel.
            symbol: Symbol filter (unused).

        Returns:
            Never returns; always raises.

        Raises:
            NotImplementedError: Always.
        """
        raise NotImplementedError(_NOT_IMPLEMENTED_MSG)

    async def get_order(self, order_id: str, symbol: str | None = None) -> ExchangeOrderSnapshot:
        """Fetch order details (not available in Phase 1).

        Args:
            order_id: Exchange order ID.
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
        """Fetch orders (not available in Phase 1).

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
        """Fetch account balance (not available in Phase 1).

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
            symbols: List of Kraken Futures product IDs to subscribe.

        Returns:
            AsyncIterator yielding TickerUpdate for each price change.
        """
        return self._subscribe_ticks_impl(symbols)

    async def _subscribe_ticks_impl(self, symbols: list[str]) -> AsyncIterator[TickerUpdate]:
        """Implement ticker subscription via callback-to-queue bridge.

        Converts native symbols (e.g., ``BTC-USD-PERP``) to Kraken Futures
        product IDs (e.g., ``PF_XBTUSD``) before subscribing.

        Args:
            symbols: Native symbols to subscribe.

        Yields:
            TickerUpdate for each price change.

        Raises:
            ConnectionError: If the WebSocket connection is lost.
        """
        await self._ensure_ws_connected()
        assert self._ws_client is not None
        ws_symbols = [native_to_kraken_futures_ws(s) for s in symbols]
        await self._ws_client.subscribe(feed="ticker", products=ws_symbols)
        logger.info(f"Subscribed to Kraken Futures tickers: {symbols} -> {ws_symbols}")
        try:
            while True:
                if self._ws_client and getattr(self._ws_client, "exception_occur", False):
                    raise ConnectionError("Kraken Futures WS connection lost (tickers)")
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
                    await self._ws_client.unsubscribe(feed="ticker", products=ws_symbols)
                except Exception:
                    logger.debug("Failed to unsubscribe from tickers on cleanup")

    def subscribe_candles(
        self,
        symbols: list[str],
        timeframe: str = "1m",
    ) -> AsyncIterator[CandleUpdate]:
        """Subscribe to candle updates (not available for Kraken Futures WS).

        Callers should use get_ohlcv() for REST-based candle polling.

        Args:
            symbols: Product IDs (unused).
            timeframe: Candle interval (unused).

        Returns:
            Never returns; always raises.

        Raises:
            NotImplementedError: Always. No WS candle feed available.
        """
        raise NotImplementedError("Kraken Futures has no WS candle feed; use get_ohlcv() instead")

    def subscribe_trades(self, symbols: list[str]) -> AsyncIterator[TradeUpdate]:
        """Subscribe to real-time trade updates via WebSocket.

        Args:
            symbols: List of Kraken Futures product IDs to subscribe.

        Returns:
            AsyncIterator yielding TradeUpdate for each trade.
        """
        return self._subscribe_trades_impl(symbols)

    async def _subscribe_trades_impl(self, symbols: list[str]) -> AsyncIterator[TradeUpdate]:
        """Implement trade subscription via callback-to-queue bridge.

        Converts native symbols to Kraken Futures product IDs before subscribing.

        Args:
            symbols: Native symbols to subscribe.

        Yields:
            TradeUpdate for each execution.

        Raises:
            ConnectionError: If the WebSocket connection is lost.
        """
        await self._ensure_ws_connected()
        assert self._ws_client is not None
        ws_symbols = [native_to_kraken_futures_ws(s) for s in symbols]
        await self._ws_client.subscribe(feed="trade", products=ws_symbols)
        logger.info(f"Subscribed to Kraken Futures trades: {symbols} -> {ws_symbols}")
        try:
            while True:
                if self._ws_client and getattr(self._ws_client, "exception_occur", False):
                    raise ConnectionError("Kraken Futures WS connection lost (trades)")
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
                    await self._ws_client.unsubscribe(feed="trade", products=ws_symbols)
                except Exception:
                    logger.debug("Failed to unsubscribe from trades on cleanup")

    def subscribe_executions(self) -> AsyncIterator[ExecutionUpdate]:
        """Subscribe to execution updates (not available in Phase 1).

        Returns:
            Never returns; always raises.

        Raises:
            NotImplementedError: Always.
        """
        raise NotImplementedError(_NOT_IMPLEMENTED_MSG)

    def subscribe_instruments(self, **kwargs: Any) -> AsyncIterator[dict[str, Any]]:
        """Subscribe to instrument data via REST.

        Unlike Kraken Spot, Futures has no WS instrument feed.
        This fetches instruments once via REST and yields each.

        Returns:
            AsyncIterator yielding raw instrument dicts.
        """
        return self._subscribe_instruments_impl()

    async def _subscribe_instruments_impl(self) -> AsyncIterator[dict[str, Any]]:
        """Fetch instruments from REST and yield each as a dict.

        Yields:
            Raw instrument dict from Market.get_instruments().
        """
        if not self._market_client:
            self._market_client = Market(sandbox=self.sandbox)
        result = await asyncio.to_thread(self._market_client.get_instruments)
        instruments: list[dict[str, Any]] = result.get("instruments", [])
        for inst in instruments:
            yield inst

    def get_instruments_sync(self) -> list[dict[str, Any]]:
        """Fetch all instruments synchronously via REST.

        Returns:
            List of raw instrument dicts from Kraken Futures API.
        """
        if not self._market_client:
            self._market_client = Market(sandbox=self.sandbox)
        result = self._market_client.get_instruments()
        return list(result.get("instruments", []))

    def get_parsed_instrument(self, data: dict[str, Any]) -> InstrumentPairDescriptor:
        """Parse a raw instrument dict into InstrumentPairDescriptor.

        Args:
            data: Raw instrument dict from Kraken Futures API.

        Returns:
            Parsed InstrumentPairDescriptor.
        """
        return parse_kraken_futures_instrument(data)
