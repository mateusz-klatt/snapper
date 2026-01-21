"""Kraken cryptocurrency exchange client implementation.

This module provides KrakenExchangeClient, a full-featured client for
interacting with the Kraken exchange. It supports:

REST API Operations:
    - Market data: tickers, OHLCV candles
    - Order management: create, cancel, get orders
    - Account: balance inquiries

WebSocket Subscriptions:
    - Public: tickers, candles, trades, instruments
    - Private: executions (requires API credentials)

Features:
    - CCXT integration for standard operations
    - Native Kraken SDK for advanced features and fallbacks
    - Circuit breaker pattern for resilience
    - Automatic rate limit handling with exponential backoff
    - Symbol conversion between native, CCXT, and Kraken formats

The client maintains internal queues for each WebSocket channel to
decouple message reception from consumption, enabling async iteration
patterns for real-time data streaming.
"""

import asyncio
import time
from collections.abc import AsyncIterator
from collections.abc import Callable
from typing import Any
from typing import Literal
from typing import cast

import ccxt
from kraken.spot import SpotWSClient
from kraken.spot import Trade
from loguru import logger
from pydantic import ValidationError

from snapper.config.settings import get_settings
from snapper.data.repository import Repository
from snapper.infrastructure.exchanges.adapters.kraken import parse_kraken_candle_list
from snapper.infrastructure.exchanges.adapters.kraken import parse_kraken_execution_list
from snapper.infrastructure.exchanges.adapters.kraken import parse_kraken_instrument_list
from snapper.infrastructure.exchanges.adapters.kraken import parse_kraken_ticker_list
from snapper.infrastructure.exchanges.adapters.kraken import parse_kraken_trade_list
from snapper.infrastructure.exchanges.base import ExchangeClientBase
from snapper.infrastructure.exchanges.contracts import AccountBalance
from snapper.infrastructure.exchanges.contracts import CandleUpdate
from snapper.infrastructure.exchanges.contracts import ExchangeOrderRequest
from snapper.infrastructure.exchanges.contracts import ExchangeOrderSnapshot
from snapper.infrastructure.exchanges.contracts import ExecutionUpdate
from snapper.infrastructure.exchanges.contracts import InstrumentPairDescriptor
from snapper.infrastructure.exchanges.contracts import OhlcvSnapshot
from snapper.infrastructure.exchanges.contracts import OrderSideEnum
from snapper.infrastructure.exchanges.contracts import OrderStatusEnum
from snapper.infrastructure.exchanges.contracts import OrderTypeEnum
from snapper.infrastructure.exchanges.contracts import TickerSnapshot
from snapper.infrastructure.exchanges.contracts import TickerUpdate
from snapper.infrastructure.exchanges.contracts import TradeUpdate
from snapper.infrastructure.exchanges.schemas.kraken import KrakenExecutionSubscribeParamsSchema
from snapper.infrastructure.exchanges.schemas.kraken import KrakenExecutionSubscriptionAckSchema
from snapper.infrastructure.exchanges.schemas.kraken import KrakenInstrumentPairSchema
from snapper.infrastructure.exchanges.schemas.kraken import KrakenInstrumentSubscribeParamsSchema
from snapper.infrastructure.exchanges.schemas.kraken import KrakenOhlcSubscribeParamsSchema
from snapper.infrastructure.exchanges.schemas.kraken import KrakenOhlcSubscriptionAckSchema
from snapper.infrastructure.exchanges.schemas.kraken import KrakenTickerSubscribeParamsSchema
from snapper.infrastructure.exchanges.schemas.kraken import KrakenTradeSubscribeParamsSchema
from snapper.infrastructure.exchanges.schemas.kraken import KrakenTradeSubscriptionAckSchema
from snapper.infrastructure.symbols.functions import ccxt_to_native
from snapper.infrastructure.symbols.functions import native_to_ccxt
from snapper.infrastructure.symbols.functions import native_to_kraken_rest
from snapper.infrastructure.symbols.functions import native_to_kraken_websocket


class KrakenExchangeClient(ExchangeClientBase):
    """Kraken exchange client with REST and WebSocket support.

    This client provides comprehensive access to the Kraken cryptocurrency
    exchange, combining CCXT for standard operations with the native Kraken
    SDK for advanced features like WebSocket trading.

    The client implements circuit breaker pattern to handle API failures
    gracefully and automatic retry with exponential backoff for rate limits.

    Attributes:
        settings: Application settings instance.
        api_key: Kraken API key for authenticated requests.
        api_secret: Kraken API secret for request signing.
        sandbox: Whether to use sandbox/demo environment.
    """

    def __init__(
        self,
        api_key: str | None = None,
        api_secret: str | None = None,
        sandbox: bool = False,
        enable_rate_limit: bool = True,
        repository: Repository | None = None,
    ) -> None:
        """Initialize Kraken exchange client.

        Args:
            api_key: Kraken API key. Required for trading and private data.
            api_secret: Kraken API secret. Required for trading and private data.
            sandbox: Use sandbox environment for testing (default: False).
            enable_rate_limit: Enable automatic rate limiting (default: True).
            repository: Database repository for order/execution logging.
        """
        super().__init__(repository=repository, exchange_name="kraken")
        self.settings = get_settings()
        self.api_key = api_key
        self.api_secret = api_secret
        self.sandbox = sandbox
        ccxt_kwargs: dict[str, Any] = {
            "apiKey": api_key,
            "secret": api_secret,
            "sandbox": sandbox,
            "enableRateLimit": enable_rate_limit,
            "timeout": 30000,
            "rateLimit": 1000,
        }
        self._ccxt_client = cast(Any, ccxt.kraken)(ccxt_kwargs)
        self._ws_client: SpotWSClient | None = None
        self._ws_connected = False
        self._trade_client: Trade | None = None
        self._tick_queue: asyncio.Queue[TickerUpdate] = asyncio.Queue()
        self._candle_queues: dict[int, asyncio.Queue[CandleUpdate]] = {}
        self._trade_queue: asyncio.Queue[TradeUpdate] = asyncio.Queue()
        self._execution_queue: asyncio.Queue[ExecutionUpdate] = asyncio.Queue()
        self._raw_instrument_queue: asyncio.Queue[dict[str, Any]] = asyncio.Queue()
        self._instrument_queue: asyncio.Queue[InstrumentPairDescriptor] = asyncio.Queue()
        self._ws_order_requests: dict[int, asyncio.Future[dict[str, Any]]] = {}
        self._ws_req_id_counter = 1
        self._circuit_failures = 0
        self._circuit_open_until = 0.0
        self._max_failures = 5
        self._circuit_timeout = 60.0

    def get_ccxt_client(self) -> Any:
        """Get the underlying CCXT client instance.

        Returns:
            The CCXT Kraken client for direct access to CCXT methods.
        """
        return self._ccxt_client

    async def connect(self) -> None:
        """Establish REST connection to Kraken exchange.

        Loads markets and configures nonce generation for authenticated
        requests. Resets circuit breaker state on successful connection.

        Raises:
            Exception: If connection or market loading fails.
        """
        try:
            self._ccxt_client.nonce = lambda: int(time.time() * 100_000_000)
            await self._with_retry(self._ccxt_client.load_markets)
            logger.info("Kraken REST connection established")
            self._circuit_failures = 0
            self._circuit_open_until = 0.0
        except Exception as e:
            logger.error(f"Failed to connect to Kraken: {e}")
            raise

    async def _close_ws_client(self) -> None:
        if not self._ws_client:
            return
        try:
            if self._ws_connected:
                await self._ws_client.close()
                self._ws_connected = False
                logger.info("Kraken WebSocket disconnected")
            if hasattr(self._ws_client, "_SpotAsyncClient__session"):
                session = getattr(self._ws_client, "_SpotAsyncClient__session", None)
                if session and not session.closed:
                    await session.close()
                    logger.debug("Closed aiohttp session from kraken websocket client")
        finally:
            self._ws_client = None

    async def disconnect_websocket(self) -> None:
        """Close WebSocket connection without disconnecting REST client."""
        await self._close_ws_client()

    async def disconnect(self) -> None:
        """Disconnect from Kraken exchange.

        Closes both WebSocket and REST client sessions.
        """
        try:
            await self._close_ws_client()
            if self._ccxt_client and hasattr(self._ccxt_client, "session"):
                self._ccxt_client.session.close()
                logger.info("Kraken REST client session closed")
        except Exception as e:
            logger.warning(f"Error during disconnect: {e}")

    async def get_ticker(self, symbol: str) -> TickerSnapshot:
        """Fetch current ticker data for a symbol.

        Args:
            symbol: Trading pair in native format (e.g., 'BTC-USD').

        Returns:
            Current ticker with bid, ask, last price and timestamp.

        Raises:
            Exception: If ticker fetch fails.
        """
        try:
            ccxt_symbol = native_to_ccxt(symbol)
            ticker_data = await self._with_retry(self._ccxt_client.fetch_ticker, ccxt_symbol)
            native_symbol = ccxt_to_native(ticker_data["symbol"])
            return TickerSnapshot(
                symbol=native_symbol,
                bid=float(ticker_data["bid"]),
                ask=float(ticker_data["ask"]),
                last=float(ticker_data["last"]),
                timestamp=ticker_data["timestamp"] / 1000.0,
            )
        except Exception as e:
            logger.error(f"Failed to get ticker for {symbol}: {e}")
            raise

    async def get_ohlcv(
        self,
        symbol: str,
        timeframe: str = "1m",
        since: int | None = None,
        limit: int | None = None,
    ) -> list[OhlcvSnapshot]:
        """Fetch OHLCV candle data for a symbol.

        Args:
            symbol: Trading pair in native format.
            timeframe: Candle interval (e.g., '1m', '1h', '1d').
            since: Start timestamp in milliseconds.
            limit: Maximum number of candles to return.

        Returns:
            List of OHLCV snapshots.

        Raises:
            Exception: If OHLCV fetch fails.
        """
        try:
            ccxt_symbol = native_to_ccxt(symbol)
            ohlcv_data = await self._with_retry(
                self._ccxt_client.fetch_ohlcv,
                ccxt_symbol,
                timeframe,
                since,
                limit,
            )
            return [
                OhlcvSnapshot(
                    timestamp=float(candle[0]) / 1000.0,
                    open=float(candle[1]),
                    high=float(candle[2]),
                    low=float(candle[3]),
                    close=float(candle[4]),
                    volume=float(candle[5]),
                )
                for candle in ohlcv_data
            ]
        except Exception as e:
            logger.error(f"Failed to get OhlcvSnapshot for {symbol}: {e}")
            raise

    async def create_order(self, request: ExchangeOrderRequest) -> ExchangeOrderSnapshot:
        """Create a new order on the exchange.

        Uses CCXT for standard symbols, falls back to native Kraken API
        for unsupported symbols (e.g., tokenized assets).

        Args:
            request: Order parameters including symbol, side, type, amount.

        Returns:
            Created order snapshot with status.

        Raises:
            RuntimeError: If API credentials are missing.
            Exception: If order creation fails.
        """
        if not self.api_key or not self.api_secret:
            raise RuntimeError("API credentials required for trading")
        try:
            ccxt_symbol = native_to_ccxt(request.symbol)
            ccxt_params = {}
            if request.client_order_id:
                ccxt_params["clientOrderId"] = request.client_order_id
            order_data = await self._with_retry(
                self._ccxt_client.create_order,
                ccxt_symbol,
                request.type.value,
                request.side.value,
                float(request.amount),
                float(request.price) if request.price else None,
                ccxt_params,
            )
            order = self._convert_ccxt_order(order_data)
            await self._log_order_to_db(request, order)
            return order
        except ValueError as e:
            if "Unknown native symbol" not in str(e):
                raise
            logger.info(
                f"Symbol {request.symbol} not supported by CCXT, using native Kraken API fallback"
            )
            try:
                kraken_rest_symbol = native_to_kraken_rest(request.symbol)
                trade_client = self._get_trade_client()
                kraken_params: dict[str, Any] = {
                    "ordertype": request.type.value,
                    "side": request.side.value,
                    "pair": kraken_rest_symbol,
                    "volume": str(request.amount),
                }
                if request.price:
                    kraken_params["price"] = str(request.price)
                extra_params: dict[str, Any] = {}
                if request.client_order_id:
                    extra_params["cl_ord_id"] = str(request.client_order_id)
                if kraken_rest_symbol.endswith("x/USD") or kraken_rest_symbol.endswith("x/EUR"):
                    extra_params["asset_class"] = "tokenized_asset"
                result = trade_client.create_order(
                    **kraken_params,
                    extra_params=extra_params or None,
                )
                order = self._convert_kraken_native_order(result, request)
                await self._log_order_to_db(request, order)
                return order
            except Exception as fallback_error:
                logger.error(f"Failed to create order with native Kraken API: {fallback_error}")
                raise
        except Exception as e:
            logger.error(f"Failed to create order: {e}")
            raise

    async def cancel_order(self, order_id: str, symbol: str | None = None) -> ExchangeOrderSnapshot:
        """Cancel an existing order.

        Args:
            order_id: Exchange order ID to cancel.
            symbol: Trading pair (optional, improves performance).

        Returns:
            Cancelled order snapshot.

        Raises:
            RuntimeError: If API credentials are missing.
            Exception: If cancellation fails.
        """
        if not self.api_key or not self.api_secret:
            raise RuntimeError("API credentials required for trading")
        try:
            ccxt_symbol = native_to_ccxt(symbol) if symbol else None
            order_data = await self._with_retry(
                self._ccxt_client.cancel_order,
                order_id,
                ccxt_symbol,
            )
            canceled_order = self._convert_ccxt_order(order_data)
            return canceled_order
        except ValueError as e:
            if not symbol or "Unknown native symbol" not in str(e):
                raise
            logger.info(f"Symbol {symbol} not supported by CCXT, using native Kraken API fallback")
            try:
                trade_client = self._get_trade_client()
                trade_client.cancel_order(txid=order_id)
                return ExchangeOrderSnapshot(
                    id=order_id,
                    client_order_id=None,
                    symbol=symbol,
                    side=OrderSideEnum.BUY,
                    type=OrderTypeEnum.LIMIT,
                    amount=float("0"),
                    price=None,
                    status=OrderStatusEnum.CANCELED,
                    filled=float("0"),
                    remaining=float("0"),
                    timestamp=time.time(),
                    fee=None,
                )
            except Exception as fallback_error:
                logger.error(f"Failed to cancel order with native Kraken API: {fallback_error}")
                raise
        except Exception as e:
            logger.error(f"Failed to cancel order {order_id}: {e}")
            raise

    async def create_order_ws(
        self,
        request: ExchangeOrderRequest,
        timeout: float = 10.0,
    ) -> ExchangeOrderSnapshot:
        """Create order via WebSocket for lower latency.

        Args:
            request: Order parameters.
            timeout: Maximum wait time for response in seconds.

        Returns:
            Created order snapshot.

        Raises:
            RuntimeError: If credentials missing or WebSocket not connected.
            TimeoutError: If order creation times out.
        """
        if not self.api_key or not self.api_secret:
            raise RuntimeError("API credentials required for trading")
        ws = await self._get_or_create_ws_client()
        if not ws or not self._ws_connected:
            raise RuntimeError("WebSocket not connected - call connect() first")
        req_id = self._ws_req_id_counter
        self._ws_req_id_counter += 1
        ws_symbol = native_to_kraken_websocket(request.symbol)
        params: dict[str, Any] = {
            "order_type": request.type.value,
            "side": request.side.value,
            "symbol": ws_symbol,
            "order_qty": float(request.amount),
        }
        if request.price:
            params["limit_price"] = float(request.price)
        if request.client_order_id:
            params["cl_ord_id"] = str(request.client_order_id)
        future: asyncio.Future[dict[str, Any]] = asyncio.Future()
        self._ws_order_requests[req_id] = future
        try:
            await ws.send_message(
                message={
                    "method": "add_order",
                    "params": params,
                    "req_id": req_id,
                },
            )
            result = await asyncio.wait_for(future, timeout=timeout)
            return self._convert_ws_order_response(result, request)
        except TimeoutError as e:
            logger.error(f"WebSocket order timeout after {timeout}s (req_id={req_id})")
            raise TimeoutError(f"ExchangeOrderSnapshot creation timeout after {timeout}s") from e
        finally:
            self._ws_order_requests.pop(req_id, None)

    async def cancel_order_ws(
        self,
        order_id: str,
        timeout: float = 10.0,
    ) -> ExchangeOrderSnapshot:
        """Cancel order via WebSocket for lower latency.

        Args:
            order_id: Exchange order ID to cancel.
            timeout: Maximum wait time for response in seconds.

        Returns:
            Cancelled order snapshot.

        Raises:
            RuntimeError: If credentials missing or WebSocket not connected.
            TimeoutError: If cancellation times out.
        """
        if not self.api_key or not self.api_secret:
            raise RuntimeError("API credentials required for trading")
        ws = await self._get_or_create_ws_client()
        if not ws or not self._ws_connected:
            raise RuntimeError("WebSocket not connected - call connect() first")
        req_id = self._ws_req_id_counter
        self._ws_req_id_counter += 1
        params: dict[str, Any] = {
            "order_id": [order_id],
        }
        future: asyncio.Future[dict[str, Any]] = asyncio.Future()
        self._ws_order_requests[req_id] = future
        try:
            await ws.send_message(
                message={
                    "method": "cancel_order",
                    "params": params,
                    "req_id": req_id,
                },
            )
            await asyncio.wait_for(future, timeout=timeout)
            return ExchangeOrderSnapshot(
                id=order_id,
                client_order_id=None,
                symbol="",
                side=OrderSideEnum.BUY,
                type=OrderTypeEnum.LIMIT,
                amount=float("0"),
                price=None,
                status=OrderStatusEnum.CANCELED,
                filled=float("0"),
                remaining=float("0"),
                timestamp=time.time(),
                fee=None,
            )
        except TimeoutError as e:
            logger.error(f"WebSocket cancel timeout after {timeout}s (req_id={req_id})")
            raise TimeoutError(
                f"ExchangeOrderSnapshot cancellation timeout after {timeout}s"
            ) from e
        finally:
            self._ws_order_requests.pop(req_id, None)

    async def get_order(self, order_id: str, symbol: str | None = None) -> ExchangeOrderSnapshot:
        """Fetch details of a specific order.

        Args:
            order_id: Exchange order ID.
            symbol: Trading pair (optional).

        Returns:
            Order snapshot with current status.

        Raises:
            RuntimeError: If API credentials are missing.
            Exception: If order fetch fails.
        """
        if not self.api_key or not self.api_secret:
            raise RuntimeError("API credentials required for trading")
        try:
            ccxt_symbol = native_to_ccxt(symbol) if symbol else None
            order_data = await self._with_retry(
                self._ccxt_client.fetch_order,
                order_id,
                ccxt_symbol,
            )
            return self._convert_ccxt_order(order_data)
        except Exception as e:
            logger.error(f"Failed to get order {order_id}: {e}")
            raise

    async def get_orders(
        self,
        symbol: str | None = None,
        status: OrderStatusEnum | None = None,
        limit: int | None = None,
    ) -> list[ExchangeOrderSnapshot]:
        """Fetch multiple orders with optional filtering.

        Args:
            symbol: Filter by trading pair.
            status: Filter by order status (OPEN, CLOSED, etc.).
            limit: Maximum number of orders to return.

        Returns:
            List of order snapshots.

        Raises:
            RuntimeError: If API credentials are missing.
            Exception: If orders fetch fails.
        """
        if not self.api_key or not self.api_secret:
            raise RuntimeError("API credentials required for trading")
        try:
            if status == OrderStatusEnum.OPEN:
                ccxt_symbol = native_to_ccxt(symbol) if symbol else None
                orders_data = await self._with_retry(
                    self._ccxt_client.fetch_open_orders,
                    ccxt_symbol,
                    None,
                    limit,
                )
            else:
                ccxt_symbol = native_to_ccxt(symbol) if symbol else None
                orders_data = await self._with_retry(
                    self._ccxt_client.fetch_orders,
                    ccxt_symbol,
                    None,
                    limit,
                )
            orders = [self._convert_ccxt_order(order) for order in orders_data]
            if status:
                orders = [order for order in orders if order.status == status]
            return orders
        except Exception as e:
            logger.error(f"Failed to get orders: {e}")
            raise

    async def get_balance(self, currency: str | None = None) -> dict[str, AccountBalance]:
        """Fetch account balances.

        Args:
            currency: Filter by specific currency (e.g., 'BTC', 'USD').

        Returns:
            Dictionary of currency to balance info.

        Raises:
            RuntimeError: If API credentials are missing.
            Exception: If balance fetch fails.
        """
        if not self.api_key or not self.api_secret:
            raise RuntimeError("API credentials required for balance")
        try:
            balance_data = await self._with_retry(self._ccxt_client.fetch_balance)
            balances = {}
            for curr, data in balance_data.items():
                if curr in ["free", "used", "total", "info", "timestamp", "datetime"]:
                    continue
                if not isinstance(data, dict):
                    continue
                balances[curr] = AccountBalance(
                    currency=curr,
                    free=float(data.get("free", 0)),
                    used=float(data.get("used", 0)),
                    total=float(data.get("total", 0)),
                )
            if currency:
                return {
                    currency: balances.get(
                        currency, AccountBalance(currency, float("0"), float("0"), float("0"))
                    )
                }
            return balances
        except Exception as e:
            logger.error(f"Failed to get balance: {e}")
            raise

    async def subscribe_ticks(
        self, symbols: list[str], *, req_id: int | None = None
    ) -> AsyncIterator[TickerUpdate]:
        """Subscribe to real-time ticker updates via WebSocket.

        Args:
            symbols: List of symbols or ['*'] for all.
            req_id: Optional request ID for tracking.

        Yields:
            TickerUpdate for each price change.

        Raises:
            Exception: If subscription fails.
        """
        try:
            await self._ensure_ws_connected()
            assert self._ws_client is not None, "WebSocket client should be connected"
            if symbols == ["*"]:
                ws_symbols = ["*"]
            else:
                ws_symbols = [native_to_kraken_websocket(symbol) for symbol in symbols]
            logger.info(f"Subscribing to ticks: {symbols} -> {ws_symbols}")
            async with self._ws_client as ws:
                params = KrakenTickerSubscribeParamsSchema(symbol=ws_symbols).as_params()
                await ws.subscribe(params=params, req_id=req_id)
                while not hasattr(ws, "exception_occur") or not ws.exception_occur:
                    try:
                        message = await asyncio.wait_for(self._tick_queue.get(), timeout=0.1)
                        yield message
                    except TimeoutError:
                        await asyncio.sleep(0.01)
                    except Exception as e:
                        logger.error(f"Error receiving WebSocket message: {e}")
                        break
        except Exception as e:
            logger.error(f"WebSocket tick subscription error: {e}")
            raise

    async def subscribe_candles(
        self,
        symbols: list[str],
        timeframe: str = "1m",
        *,
        req_id: int | None = None,
    ) -> AsyncIterator[CandleUpdate]:
        """Subscribe to real-time OHLC candle updates.

        Args:
            symbols: List of symbols or ['*'] for all.
            timeframe: Candle interval (e.g., '1m', '1h').
            req_id: Optional request ID.

        Yields:
            CandleUpdate for each candle change.

        Raises:
            Exception: If subscription fails.
        """
        try:
            await self._ensure_ws_connected()
            assert self._ws_client is not None, "WebSocket client should be connected"
            if symbols == ["*"]:
                ws_symbols = ["*"]
            else:
                ws_symbols = [native_to_kraken_websocket(symbol) for symbol in symbols]
            logger.info(f"Subscribing to {timeframe} candles: {symbols} -> {ws_symbols}")
            interval_map = {
                "1m": 1,
                "5m": 5,
                "15m": 15,
                "30m": 30,
                "1h": 60,
                "4h": 240,
                "1d": 1440,
            }
            interval = interval_map.get(timeframe, 1)
            if interval not in self._candle_queues:
                self._candle_queues[interval] = asyncio.Queue()
            async with self._ws_client as ws:
                subscribe_params = KrakenOhlcSubscribeParamsSchema(
                    symbol=ws_symbols, interval=interval
                )
                await ws.subscribe(params=subscribe_params.as_params(), req_id=req_id)
                while not hasattr(ws, "exception_occur") or not ws.exception_occur:
                    try:
                        message = await asyncio.wait_for(
                            self._candle_queues[interval].get(), timeout=0.1
                        )
                        yield message
                    except TimeoutError:
                        await asyncio.sleep(0.01)
                    except Exception as e:
                        logger.error(f"Error receiving WebSocket message: {e}")
                        break
        except Exception as e:
            logger.error(f"WebSocket candle subscription error: {e}")
            raise

    async def subscribe_trades(
        self, symbols: list[str], *, req_id: int | None = None
    ) -> AsyncIterator[TradeUpdate]:
        """Subscribe to real-time trade updates.

        Args:
            symbols: List of symbols or ['*'] for all.
            req_id: Optional request ID.

        Yields:
            TradeUpdate for each trade execution.

        Raises:
            Exception: If subscription fails.
        """
        try:
            await self._ensure_ws_connected()
            assert self._ws_client is not None, "WebSocket client should be connected"
            if symbols == ["*"]:
                ws_symbols = ["*"]
            else:
                ws_symbols = [native_to_kraken_websocket(symbol) for symbol in symbols]
            logger.info(f"Subscribing to trades: {symbols} -> {ws_symbols}")
            async with self._ws_client as ws:
                trade_params = KrakenTradeSubscribeParamsSchema(symbol=ws_symbols).as_params()
                await ws.subscribe(params=trade_params, req_id=req_id)
                while not hasattr(ws, "exception_occur") or not ws.exception_occur:
                    try:
                        message = await asyncio.wait_for(self._trade_queue.get(), timeout=0.1)
                        yield message
                    except TimeoutError:
                        await asyncio.sleep(0.01)
                    except Exception as e:
                        logger.error(f"Error receiving WebSocket trade message: {e}")
                        break
        except Exception as e:
            logger.error(f"WebSocket trade subscription error: {e}")
            raise

    async def subscribe_executions(
        self,
        *,
        snap_orders: bool | None = True,
        snap_trades: bool | None = True,
        order_status: bool | None = True,
        ratecounter: bool | None = None,
        users: Literal["all"] | None = None,
        req_id: int | None = None,
    ) -> AsyncIterator[ExecutionUpdate]:
        """Subscribe to user execution reports (private channel).

        Args:
            snap_orders: Include order snapshot on subscribe.
            snap_trades: Include trade snapshot on subscribe.
            order_status: Include order status updates.
            ratecounter: Enable rate counter.
            users: Subscribe to all users (for sub-accounts).
            req_id: Optional request ID.

        Yields:
            ExecutionUpdate for each order fill or status change.

        Raises:
            RuntimeError: If API credentials are missing.
            Exception: If subscription fails.
        """
        if not self.api_key or not self.api_secret:
            raise RuntimeError("API credentials required for executions subscription")
        try:
            await self._ensure_ws_connected()
            assert self._ws_client is not None, "WebSocket client should be started"
            logger.info("Subscribing to executions (private channel)")
            subscribe_params = KrakenExecutionSubscribeParamsSchema(
                snap_orders=snap_orders,
                snap_trades=snap_trades,
                order_status=order_status,
                ratecounter=ratecounter,
                users=users,
                reqid=req_id,
            ).as_params()
            await self._ws_client.subscribe(params=subscribe_params, req_id=req_id)
            while not self._ws_client.exception_occur:
                try:
                    message = await asyncio.wait_for(self._execution_queue.get(), timeout=0.1)
                    yield message
                except TimeoutError:
                    await asyncio.sleep(0.01)
                except Exception as e:
                    logger.error(f"Error receiving WebSocket execution message: {e}")
                    break
        except Exception as e:
            logger.error(f"WebSocket executions subscription error: {e}")
            raise

    def subscribe_instruments(self, **kwargs: Any) -> AsyncIterator[dict[str, Any]]:
        """Subscribe to instrument/pair updates.

        Args:
            **kwargs: Options including req_id and raw (bool).

        Returns:
            AsyncIterator yielding instrument dictionaries.
        """
        req_id = kwargs.get("req_id")
        raw = kwargs.get("raw", False)
        return self._subscribe_instruments_impl(req_id=req_id, raw=raw)

    async def _subscribe_instruments_impl(
        self, *, req_id: int | None = None, raw: bool = False
    ) -> AsyncIterator[dict[str, Any]]:
        try:
            await self._ensure_ws_connected()
            assert self._ws_client is not None, "WebSocket client should be connected"
            logger.info("Subscribing to instruments (public channel, raw={})", raw)
            async with self._ws_client as ws:
                params = KrakenInstrumentSubscribeParamsSchema(snapshot=True).as_params()
                await ws.subscribe(params=params, req_id=req_id)
                if raw:
                    queue: asyncio.Queue[Any] = self._raw_instrument_queue
                else:
                    queue = self._instrument_queue
                while not hasattr(ws, "exception_occur") or not ws.exception_occur:
                    try:
                        message = await asyncio.wait_for(queue.get(), timeout=0.1)
                        if raw:
                            yield message
                        else:
                            from dataclasses import asdict

                            yield asdict(message)
                    except TimeoutError:
                        await asyncio.sleep(0.01)
                    except Exception as e:
                        logger.error(f"Error receiving WebSocket instrument message: {e}")
                        break
        except Exception as e:
            logger.error(f"WebSocket instrument subscription error: {e}")
            raise

    async def _on_message(self, message: dict[str, Any] | list[Any]) -> None:
        try:
            if isinstance(message, dict) and "req_id" in message:
                req_id = message["req_id"]
                if req_id in self._ws_order_requests:
                    future = self._ws_order_requests[req_id]
                    if not future.done():
                        if message.get("success"):
                            future.set_result(message)
                        else:
                            error_msg = message.get("error", "Unknown error")
                            future.set_exception(
                                Exception(f"ExchangeOrderSnapshot request failed: {error_msg}")
                            )
                    return
            if (
                isinstance(message, dict)
                and message.get("method") == "subscribe"
                and isinstance(message.get("result"), dict)
            ):
                result_dict = cast(dict[str, Any], message["result"])
                channel_name = result_dict.get("channel")
                if channel_name == "trade":
                    try:
                        trade_ack = KrakenTradeSubscriptionAckSchema.model_validate(message)
                        if not trade_ack.success:
                            logger.warning(
                                "Trade subscription failed symbol={} error={}",
                                result_dict.get("symbol"),
                                trade_ack.error,
                            )
                    except ValidationError:
                        logger.debug(
                            "Received non-standard trade control message: {}",
                            message,
                        )
                    return
                if channel_name == "executions":
                    try:
                        execution_ack = KrakenExecutionSubscriptionAckSchema.model_validate(message)
                        if not execution_ack.success:
                            logger.warning(
                                "Executions subscription failed (orders={} trades={}) error={}",
                                execution_ack.result.snap_orders,
                                execution_ack.result.snap_trades,
                                execution_ack.error,
                            )
                        if execution_ack.warnings:
                            for warning in execution_ack.warnings:
                                logger.warning("Execution subscription warning: {}", warning)
                    except ValidationError:
                        logger.debug(
                            "Received non-standard executions control message: {}",
                            message,
                        )
                    return
                if channel_name == "ohlc":
                    try:
                        ohlc_ack = KrakenOhlcSubscriptionAckSchema.model_validate(message)
                        if not ohlc_ack.success:
                            logger.warning(
                                "OHLC subscription failed symbol={} interval={} error={}",
                                ohlc_ack.result.symbol,
                                ohlc_ack.result.interval,
                                ohlc_ack.error,
                            )
                        if ohlc_ack.result.warnings:
                            for warning in ohlc_ack.result.warnings:
                                logger.warning(f"OHLC subscription warning: {warning}")
                    except ValidationError:
                        logger.debug(
                            "Received non-standard OHLC control message: {}",
                            message,
                        )
                    return
            if isinstance(message, dict) and "channel" in message and "data" in message:
                message_dict: dict[str, Any] = message
                channel = message_dict["channel"]
                data = message_dict["data"]
                if channel == "ohlc":
                    try:
                        if isinstance(data, list):
                            candle_list = parse_kraken_candle_list(data)
                        else:
                            candle_list = []
                        for candle_data in candle_list:
                            if candle_data.interval in self._candle_queues:
                                await self._candle_queues[candle_data.interval].put(candle_data)
                            else:
                                for queue in self._candle_queues.values():
                                    await queue.put(candle_data)
                    except (ValidationError, ValueError) as e:
                        logger.warning(f"Failed to parse candle data: {e}")
                elif channel == "ticker":
                    try:
                        ticker_list = parse_kraken_ticker_list(data)
                        for ticker_data in ticker_list:
                            await self._tick_queue.put(ticker_data)
                    except (ValidationError, ValueError) as e:
                        logger.warning(f"Failed to parse ticker data: {e}")
                elif channel == "trade":
                    try:
                        trade_list = parse_kraken_trade_list(data)
                        for trade_data in trade_list:
                            await self._trade_queue.put(trade_data)
                    except (ValidationError, ValueError) as e:
                        logger.warning(f"Failed to parse trade data: {e}")
                elif channel == "executions":
                    try:
                        execution_list = parse_kraken_execution_list(data)
                        for execution_data in execution_list:
                            await self._execution_queue.put(execution_data)
                    except (ValidationError, ValueError) as e:
                        logger.warning(f"Failed to parse execution data: {e}")
                elif channel == "instrument":
                    try:
                        data = message_dict.get("data")
                        if data is None:
                            return
                        if not isinstance(data, dict):
                            logger.warning(f"Unexpected instrument data format: {type(data)}")
                            return
                        pairs_data: list[dict[str, Any]] = data.get("pairs", [])
                        for pair_dict in pairs_data:
                            try:
                                validated = KrakenInstrumentPairSchema.model_validate(pair_dict)
                                await self._raw_instrument_queue.put(
                                    validated.model_dump(by_alias=True)
                                )
                            except ValidationError as e:
                                logger.warning(f"Invalid instrument data, skipping: {e}")
                                continue
                            try:
                                instrument_pair = parse_kraken_instrument_list([pair_dict])[0]
                                await self._instrument_queue.put(instrument_pair)
                            except (ValidationError, ValueError, IndexError) as e:
                                logger.debug(f"Skipping instrument parse (raw available): {e}")
                    except Exception as e:
                        logger.warning(f"Failed to process instrument data: {e}")
        except Exception as e:
            logger.error(f"Error processing WebSocket message: {e}")

    async def _get_or_create_ws_client(self) -> Any:
        if not self.api_key or not self.api_secret:
            raise RuntimeError("API credentials required for WebSocket trading")
        await self._ensure_ws_connected()
        return self._ws_client

    def _convert_ws_order_response(
        self, result: dict[str, Any], request: ExchangeOrderRequest
    ) -> ExchangeOrderSnapshot:
        order_data = result.get("result", {})
        order_id = order_data.get("order_id", "")
        client_order_id = order_data.get("cl_ord_id") or order_data.get("order_userref")
        return ExchangeOrderSnapshot(
            id=order_id,
            client_order_id=str(client_order_id) if client_order_id else None,
            symbol=request.symbol,
            side=request.side,
            type=request.type,
            amount=float(request.amount),
            price=float(request.price) if request.price else None,
            status=OrderStatusEnum.PENDING,
            filled=float("0"),
            remaining=float(request.amount),
            timestamp=time.time(),
            fee=None,
        )

    async def _ensure_ws_connected(self) -> None:
        if not self._ws_client:
            self._ws_client = SpotWSClient(
                key=self.api_key or "",
                secret=self.api_secret or "",
                callback=self._on_message,
            )
            logger.info("Kraken WebSocket client initialized")
            await self._ws_client.start()
            logger.info("Kraken WebSocket client started")
        self._ws_connected = True

    def _get_trade_client(self) -> Trade:
        if not self.api_key or not self.api_secret:
            raise RuntimeError("API credentials required for native Kraken Trade API")
        if self._trade_client is None:
            self._trade_client = Trade(key=self.api_key, secret=self.api_secret)
            logger.debug("Initialized native Kraken Trade REST API client")
        return self._trade_client

    async def _with_retry(self, func: Callable[..., Any], *args: Any, **kwargs: Any) -> Any:
        if time.time() < self._circuit_open_until:
            raise RuntimeError("Circuit breaker open")
        max_retries = 3
        base_delay = 1.0
        attempt = 0
        while True:
            try:
                if asyncio.iscoroutinefunction(func):
                    result = await func(*args, **kwargs)
                else:
                    result = func(*args, **kwargs)
                self._circuit_failures = 0
                return result
            except ccxt.RateLimitExceeded:
                logger.warning(f"Rate limit exceeded, retrying in {base_delay * (2 ** attempt)}s")
                if attempt < max_retries - 1:
                    attempt += 1
                    await asyncio.sleep(base_delay * (2 ** (attempt - 1)))
                    continue
                raise
            except (ccxt.NetworkError, ccxt.ExchangeNotAvailable) as e:
                logger.warning(f"Network error: {e}, retrying in {base_delay * (2 ** attempt)}s")
                self._circuit_failures += 1
                if attempt < max_retries - 1:
                    attempt += 1
                    await asyncio.sleep(base_delay * (2 ** (attempt - 1)))
                    continue
                if self._circuit_failures >= self._max_failures:
                    self._circuit_open_until = time.time() + self._circuit_timeout
                    logger.error("Circuit breaker opened due to repeated failures")
                raise
            except Exception as e:
                logger.error(f"Unexpected error: {e}")
                raise

    def _convert_ccxt_order(self, ccxt_order: dict[str, Any]) -> ExchangeOrderSnapshot:
        status_map = {
            "open": OrderStatusEnum.OPEN,
            "closed": OrderStatusEnum.CLOSED,
            "canceled": OrderStatusEnum.CANCELED,
            "cancelled": OrderStatusEnum.CANCELED,
            "expired": OrderStatusEnum.EXPIRED,
            "pending": OrderStatusEnum.PENDING,
        }
        side_map = {
            "buy": OrderSideEnum.BUY,
            "sell": OrderSideEnum.SELL,
        }
        type_map = {
            "market": OrderTypeEnum.MARKET,
            "limit": OrderTypeEnum.LIMIT,
            "stop": OrderTypeEnum.STOP_LOSS,
            "stop-loss": OrderTypeEnum.STOP_LOSS,
            "take-profit": OrderTypeEnum.TAKE_PROFIT,
        }
        native_symbol = ccxt_to_native(ccxt_order["symbol"])
        return ExchangeOrderSnapshot(
            id=ccxt_order["id"],
            client_order_id=ccxt_order.get("clientOrderId"),
            symbol=native_symbol,
            side=side_map[ccxt_order["side"]],
            type=type_map[ccxt_order["type"]],
            amount=float(ccxt_order["amount"]),
            price=float(ccxt_order["price"]) if ccxt_order["price"] else None,
            status=status_map[ccxt_order["status"]],
            filled=float(ccxt_order.get("filled", 0)),
            remaining=float(ccxt_order.get("remaining", 0)),
            timestamp=ccxt_order["timestamp"] / 1000.0 if ccxt_order["timestamp"] else time.time(),
            fee=float(ccxt_order["fee"]["cost"]) if ccxt_order.get("fee") else None,
        )

    def _convert_kraken_native_order(
        self, kraken_response: dict[str, Any], request: ExchangeOrderRequest
    ) -> ExchangeOrderSnapshot:
        txid = (
            kraken_response.get("txid", [""])[0]
            if isinstance(kraken_response.get("txid"), list)
            else kraken_response.get("txid", "")
        )
        return ExchangeOrderSnapshot(
            id=str(txid),
            client_order_id=request.client_order_id,
            symbol=request.symbol,
            side=request.side,
            type=request.type,
            amount=float(request.amount),
            price=float(request.price) if request.price else None,
            status=OrderStatusEnum.PENDING,
            filled=float("0"),
            remaining=float(request.amount),
            timestamp=time.time(),
            fee=None,
        )
