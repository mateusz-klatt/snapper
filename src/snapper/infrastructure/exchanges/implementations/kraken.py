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
import inspect
import time
from collections.abc import AsyncIterator
from collections.abc import Callable
from time import monotonic
from typing import Any
from typing import Final
from typing import Literal
from typing import cast

import ccxt
from kraken.spot import SpotWSClient
from kraken.spot import Trade
from loguru import logger
from pydantic import ValidationError

from snapper.config.settings import get_settings
from snapper.core.types import ExchangeEnum
from snapper.core.types import TradeSideEnum
from snapper.data.repository import Repository
from snapper.infrastructure.exchanges.adapters.kraken import parse_kraken_candle_list
from snapper.infrastructure.exchanges.adapters.kraken import parse_kraken_execution_list
from snapper.infrastructure.exchanges.adapters.kraken import parse_kraken_instrument
from snapper.infrastructure.exchanges.adapters.kraken import parse_kraken_ticker_list
from snapper.infrastructure.exchanges.adapters.kraken import parse_kraken_trade_list
from snapper.infrastructure.exchanges.base import ExchangeClientBase
from snapper.infrastructure.exchanges.contracts import AccountBalance
from snapper.infrastructure.exchanges.contracts import CandleUpdate
from snapper.infrastructure.exchanges.contracts import ExchangeOrderRequest
from snapper.infrastructure.exchanges.contracts import ExchangeOrderSnapshot
from snapper.infrastructure.exchanges.contracts import ExchangeOrderStatusEnum
from snapper.infrastructure.exchanges.contracts import ExchangeOrderTypeEnum
from snapper.infrastructure.exchanges.contracts import ExecutionUpdate
from snapper.infrastructure.exchanges.contracts import InstrumentPairDescriptor
from snapper.infrastructure.exchanges.contracts import OhlcvSnapshot
from snapper.infrastructure.exchanges.contracts import OrderSideEnum
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

_CREDENTIALS_REQUIRED_MSG = "API credentials required for trading"
_WS_CLIENT_CONNECTED_MSG = "WebSocket client should be connected"
_QUEUE_MAX_SIZE = 10_000

_DROP_LOG_INTERVAL_S = 1.0
_drop_counters: dict[str, list[float]] = {}


def _enqueue_or_drop_oldest(queue: asyncio.Queue[Any], item: Any, label: str) -> None:
    """Put item on queue, dropping the oldest if full.

    Matches the bounded-queue pattern already used by
    :py:mod:`~snapper.infrastructure.exchanges.implementations.kraken_futures`
    and
    :py:mod:`~snapper.infrastructure.exchanges.implementations.kraken_equities`.
    Drop-oldest is the safer default for market-data feeds: a slow
    consumer must never grow RSS without bound.

    Logs are rate-limited to one summary line per ``_DROP_LOG_INTERVAL_S``
    seconds per ``label`` — the previous per-drop ``logger.warning`` cost
    ~29 us each, which at sustained drop rates of hundreds per second
    became its own non-trivial fraction of the publisher hot path
    (Codex 2026-05-11 post-HV2-H5 review).

    Args:
        queue: Bounded asyncio queue.
        item: Item to enqueue.
        label: Human-readable label for the warning log.
    """
    try:
        queue.put_nowait(item)
    except asyncio.QueueFull:
        counters = _drop_counters.setdefault(label, [0.0, 0.0])
        counters[0] += 1
        now = monotonic()
        if now - counters[1] >= _DROP_LOG_INTERVAL_S:
            logger.warning(
                f"{label} queue full, dropped {int(counters[0])} messages "
                f"in last {now - counters[1]:.1f}s (drop-oldest backpressure)"
            )
            counters[0] = 0.0
            counters[1] = now
        queue.get_nowait()
        queue.put_nowait(item)


_CCXT_STATUS_MAP: Final[dict[str, ExchangeOrderStatusEnum]] = {
    "open": ExchangeOrderStatusEnum.OPEN,
    "closed": ExchangeOrderStatusEnum.CLOSED,
    "canceled": ExchangeOrderStatusEnum.CANCELED,
    "cancelled": ExchangeOrderStatusEnum.CANCELED,
    "expired": ExchangeOrderStatusEnum.EXPIRED,
    "pending": ExchangeOrderStatusEnum.PENDING,
}

_CCXT_SIDE_MAP: Final[dict[str, OrderSideEnum]] = {
    TradeSideEnum.BUY: OrderSideEnum.BUY,
    TradeSideEnum.SELL: OrderSideEnum.SELL,
}

_CCXT_TYPE_MAP: Final[dict[str, ExchangeOrderTypeEnum]] = {
    "market": ExchangeOrderTypeEnum.MARKET,
    "limit": ExchangeOrderTypeEnum.LIMIT,
    "stop": ExchangeOrderTypeEnum.STOP_LOSS,
    "stop-loss": ExchangeOrderTypeEnum.STOP_LOSS,
    "take-profit": ExchangeOrderTypeEnum.TAKE_PROFIT,
}


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
        super().__init__(repository=repository, exchange_name=ExchangeEnum.KRAKEN)
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
        self._tick_queue: asyncio.Queue[TickerUpdate] = asyncio.Queue(maxsize=_QUEUE_MAX_SIZE)
        self._candle_queues: dict[int, asyncio.Queue[CandleUpdate]] = {}
        self._trade_queue: asyncio.Queue[TradeUpdate] = asyncio.Queue(maxsize=_QUEUE_MAX_SIZE)
        self._execution_queue: asyncio.Queue[ExecutionUpdate] = asyncio.Queue(
            maxsize=_QUEUE_MAX_SIZE
        )
        self._raw_instrument_queue: asyncio.Queue[dict[str, Any]] = asyncio.Queue(
            maxsize=_QUEUE_MAX_SIZE
        )
        self._instrument_queue: asyncio.Queue[InstrumentPairDescriptor] = asyncio.Queue(
            maxsize=_QUEUE_MAX_SIZE
        )
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

    _WS_CLOSE_TIMEOUT_SECONDS = 10.0

    async def _close_ws_client(self) -> None:
        """Close WebSocket client with timeout protection.

        Ensures the process does not hang indefinitely when the Kraken
        SDK fails to close the connection or its underlying aiohttp
        session in a timely manner.
        """
        if not self._ws_client:
            return
        try:
            async with asyncio.timeout(self._WS_CLOSE_TIMEOUT_SECONDS):
                if self._ws_connected:
                    await self._ws_client.close()
                    self._ws_connected = False
                    logger.info("Kraken WebSocket disconnected")
                if hasattr(self._ws_client, "_SpotAsyncClient__session"):
                    session = getattr(self._ws_client, "_SpotAsyncClient__session", None)
                    if session and not session.closed:
                        await session.close()
                        logger.debug("Closed aiohttp session from kraken websocket client")
        except TimeoutError:
            logger.warning("WebSocket close timed out - forcing cleanup")
            self._ws_connected = False
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
            raise RuntimeError(_CREDENTIALS_REQUIRED_MSG)
        try:
            return await self._create_order_via_ccxt(request)
        except ValueError as e:
            if "Unknown native symbol" not in str(e):
                raise
            logger.info(
                f"Symbol {request.symbol} not supported by CCXT, using native Kraken API fallback"
            )
            return await self._create_order_via_native(request)
        except Exception as e:
            logger.error(f"Failed to create order: {e}")
            raise

    async def _create_order_via_ccxt(self, request: ExchangeOrderRequest) -> ExchangeOrderSnapshot:
        """Create order using CCXT client.

        Args:
            request: Order parameters.

        Returns:
            Created order snapshot.
        """
        ccxt_symbol = native_to_ccxt(request.symbol)
        ccxt_params: dict[str, Any] = {}
        if request.client_order_id:
            ccxt_params["clientOrderId"] = request.client_order_id
        if request.leverage is not None:
            ccxt_params["leverage"] = request.leverage
        if request.post_only:
            ccxt_params["postOnly"] = True
        order_data = await self._with_retry(
            self._ccxt_client.create_order,
            ccxt_symbol,
            request.type.value,
            request.side.value,
            float(request.amount),
            float(request.price) if request.price else None,
            ccxt_params,
        )
        exchange_id = str(order_data.get("id") or "")
        order = ExchangeOrderSnapshot(
            id=exchange_id,
            client_order_id=request.client_order_id,
            symbol=request.symbol,
            side=request.side,
            type=request.type,
            amount=float(request.amount),
            price=float(request.price) if request.price else None,
            status=ExchangeOrderStatusEnum.PENDING,
            filled=0.0,
            remaining=float(request.amount),
            timestamp=time.time(),
        )
        if order.id:
            db_result = await self._log_order_to_db(request, order)
            if db_result is not None:
                order.db_order_id = db_result[0]
                order.db_order_public_id = db_result[1]
        return order

    async def _create_order_via_native(
        self, request: ExchangeOrderRequest
    ) -> ExchangeOrderSnapshot:
        """Create order using native Kraken Trade API (fallback for unsupported CCXT symbols).

        Args:
            request: Order parameters.

        Returns:
            Created order snapshot.

        Raises:
            Exception: If native API order creation fails.
        """
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
            if request.leverage is not None:
                kraken_params["leverage"] = str(request.leverage)
            if request.post_only:
                kraken_params["oflags"] = "post"
            extra_params: dict[str, Any] = {}
            if request.client_order_id:
                extra_params["cl_ord_id"] = str(request.client_order_id)
            if kraken_rest_symbol.endswith("x/USD") or kraken_rest_symbol.endswith("x/EUR"):
                extra_params["asset_class"] = "tokenized_asset"
            result = await asyncio.to_thread(
                trade_client.create_order,
                **kraken_params,
                extra_params=extra_params or None,
            )
            order = self._convert_kraken_native_order(result, request)
            db_result = await self._log_order_to_db(request, order)
            if db_result is not None:
                order.db_order_id = db_result[0]
                order.db_order_public_id = db_result[1]
            return order
        except Exception as fallback_error:
            logger.error(f"Failed to create order with native Kraken API: {fallback_error}")
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
            raise RuntimeError(_CREDENTIALS_REQUIRED_MSG)
        try:
            ccxt_symbol = native_to_ccxt(symbol) if symbol else None
            await self._with_retry(
                self._ccxt_client.cancel_order,
                order_id,
                ccxt_symbol,
            )
            order_data = await self._with_retry(
                self._ccxt_client.fetch_order, order_id, ccxt_symbol
            )
            canceled_order = self._convert_ccxt_order(order_data)
            return canceled_order
        except ValueError as e:
            if not symbol or "Unknown native symbol" not in str(e):
                raise
            logger.info(f"Symbol {symbol} not supported by CCXT, using native Kraken API fallback")
            try:
                trade_client = self._get_trade_client()
                await asyncio.to_thread(trade_client.cancel_order, txid=order_id)
                return ExchangeOrderSnapshot(
                    id=order_id,
                    client_order_id=None,
                    symbol=symbol,
                    side=OrderSideEnum.BUY,
                    type=ExchangeOrderTypeEnum.LIMIT,
                    amount=float("0"),
                    price=None,
                    status=ExchangeOrderStatusEnum.CANCELED,
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
            raise RuntimeError(_CREDENTIALS_REQUIRED_MSG)
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

    async def _fetch_orders_from_exchange(
        self,
        symbol: str | None,
        status: ExchangeOrderStatusEnum | None,
        limit: int | None,
    ) -> list[dict[str, Any]]:
        """Fetch raw order data from exchange via CCXT.

        Args:
            symbol: Filter by trading pair (native format).
            status: If OPEN, fetches only open orders.
            limit: Maximum number of orders to return.

        Returns:
            List of raw CCXT order dictionaries.
        """
        ccxt_symbol = native_to_ccxt(symbol) if symbol else None
        fetch_func = (
            self._ccxt_client.fetch_open_orders
            if status == ExchangeOrderStatusEnum.OPEN
            else self._ccxt_client.fetch_orders
        )
        result: list[dict[str, Any]] = await self._with_retry(fetch_func, ccxt_symbol, None, limit)
        return result

    async def get_orders(
        self,
        symbol: str | None = None,
        status: ExchangeOrderStatusEnum | None = None,
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
            raise RuntimeError(_CREDENTIALS_REQUIRED_MSG)
        try:
            orders_data = await self._fetch_orders_from_exchange(symbol, status, limit)
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

    def subscribe_ticks(
        self, symbols: list[str], *, req_id: int | None = None
    ) -> AsyncIterator[TickerUpdate]:
        """Subscribe to real-time ticker updates via WebSocket.

        Args:
            symbols: List of symbols or ['*'] for all.
            req_id: Optional request ID for tracking.

        Returns:
            AsyncIterator yielding TickerUpdate for each price change.

        Raises:
            Exception: If subscription fails.
        """
        return self._subscribe_ticks_impl(symbols, req_id=req_id)

    async def _subscribe_ticks_impl(
        self, symbols: list[str], *, req_id: int | None
    ) -> AsyncIterator[TickerUpdate]:
        """Implement WebSocket ticker subscription.

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
            assert self._ws_client is not None, _WS_CLIENT_CONNECTED_MSG
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

    def subscribe_candles(
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

        Returns:
            AsyncIterator yielding CandleUpdate for each candle change.

        Raises:
            Exception: If subscription fails.
        """
        return self._subscribe_candles_impl(symbols, timeframe=timeframe, req_id=req_id)

    async def _subscribe_candles_impl(
        self,
        symbols: list[str],
        *,
        timeframe: str,
        req_id: int | None,
    ) -> AsyncIterator[CandleUpdate]:
        """Implement WebSocket candle subscription.

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
            assert self._ws_client is not None, _WS_CLIENT_CONNECTED_MSG
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
                self._candle_queues[interval] = asyncio.Queue(maxsize=_QUEUE_MAX_SIZE)
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

    def subscribe_trades(
        self, symbols: list[str], *, req_id: int | None = None
    ) -> AsyncIterator[TradeUpdate]:
        """Subscribe to real-time trade updates.

        Args:
            symbols: List of symbols or ['*'] for all.
            req_id: Optional request ID.

        Returns:
            AsyncIterator yielding TradeUpdate for each trade execution.

        Raises:
            Exception: If subscription fails.
        """
        return self._subscribe_trades_impl(symbols, req_id=req_id)

    async def _subscribe_trades_impl(
        self, symbols: list[str], *, req_id: int | None
    ) -> AsyncIterator[TradeUpdate]:
        """Implement WebSocket trades subscription.

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
            assert self._ws_client is not None, _WS_CLIENT_CONNECTED_MSG
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

    def subscribe_executions(
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

        Returns:
            AsyncIterator yielding ExecutionUpdate for each order fill or status change.

        Raises:
            RuntimeError: If API credentials are missing.
            Exception: If subscription fails.
        """
        return self._subscribe_executions_impl(
            snap_orders=snap_orders,
            snap_trades=snap_trades,
            order_status=order_status,
            ratecounter=ratecounter,
            users=users,
            req_id=req_id,
        )

    async def _subscribe_executions_impl(
        self,
        *,
        snap_orders: bool | None,
        snap_trades: bool | None,
        order_status: bool | None,
        ratecounter: bool | None,
        users: Literal["all"] | None,
        req_id: int | None,
    ) -> AsyncIterator[ExecutionUpdate]:
        """Implement private execution reports subscription.

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
            assert self._ws_client is not None, _WS_CLIENT_CONNECTED_MSG
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
        """Route incoming WebSocket messages to appropriate handlers.

        Dispatches based on message structure:
        - Order request responses (req_id present)
        - Subscription acknowledgements (method=subscribe)
        - Channel data messages (channel + data present)

        Args:
            message: Raw WebSocket message (dict or list).
        """
        try:
            if not isinstance(message, dict):
                return
            if message.get("method") == "subscribe" and isinstance(message.get("result"), dict):
                self._handle_subscription_ack(message)
                return
            if "channel" in message and "data" in message:
                await self._handle_channel_data(message)
        except Exception as e:
            logger.error(f"Error processing WebSocket message: {e}")

    def _handle_subscription_ack(self, message: dict[str, Any]) -> None:
        """Process subscription acknowledgement messages by channel type.

        Args:
            message: WebSocket subscription ack message.
        """
        result_dict = cast(dict[str, Any], message["result"])
        channel_name = result_dict.get("channel")
        ack_handlers: dict[str, Callable[..., None]] = {
            "trade": lambda: self._handle_trade_subscription_ack(message, result_dict),
            "executions": lambda: self._handle_execution_subscription_ack(message),
            "ohlc": lambda: self._handle_ohlc_subscription_ack(message),
        }
        handler = ack_handlers.get(channel_name or "")
        if handler:
            handler()

    def _handle_trade_subscription_ack(
        self, message: dict[str, Any], result_dict: dict[str, Any]
    ) -> None:
        """Log trade subscription acknowledgement result.

        Args:
            message: Full subscription ack message.
            result_dict: The result sub-dict from the ack.
        """
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

    def _handle_execution_subscription_ack(self, message: dict[str, Any]) -> None:
        """Log execution subscription acknowledgement result.

        Args:
            message: Full subscription ack message.
        """
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

    def _handle_ohlc_subscription_ack(self, message: dict[str, Any]) -> None:
        """Log OHLC subscription acknowledgement result.

        Args:
            message: Full subscription ack message.
        """
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

    async def _handle_channel_data(self, message_dict: dict[str, Any]) -> None:
        """Dispatch channel data messages to channel-specific handlers.

        Args:
            message_dict: WebSocket message with channel and data fields.
        """
        channel = message_dict["channel"]
        data = message_dict["data"]
        data_handlers: dict[str, Callable[[Any], None]] = {
            "ohlc": self._handle_ohlc_data,
            "ticker": self._handle_ticker_data,
            "trade": self._handle_trade_data,
            "executions": self._handle_executions_data,
        }
        handler = data_handlers.get(channel)
        if handler:
            handler(data)
        elif channel == "instrument":
            await self._handle_instrument_data(message_dict)

    def _handle_ohlc_data(self, data: Any) -> None:
        """Parse and enqueue OHLC candle data.

        Args:
            data: Raw OHLC data from WebSocket.
        """
        try:
            candle_list = parse_kraken_candle_list(data) if isinstance(data, list) else []
            for candle_data in candle_list:
                if candle_data.interval in self._candle_queues:
                    _enqueue_or_drop_oldest(
                        self._candle_queues[candle_data.interval], candle_data, "candle"
                    )
                else:
                    for queue in self._candle_queues.values():
                        _enqueue_or_drop_oldest(queue, candle_data, "candle")
        except ValueError as e:
            logger.warning(f"Failed to parse candle data: {e}")

    def _handle_ticker_data(self, data: Any) -> None:
        """Parse and enqueue ticker data.

        Args:
            data: Raw ticker data from WebSocket.
        """
        if not isinstance(data, list):
            return
        ticker_list = parse_kraken_ticker_list(data)
        for ticker_data in ticker_list:
            _enqueue_or_drop_oldest(self._tick_queue, ticker_data, "tick")

    def _handle_trade_data(self, data: Any) -> None:
        """Parse and enqueue trade data.

        Args:
            data: Raw trade data from WebSocket.
        """
        try:
            trade_list = parse_kraken_trade_list(data)
            for trade_data in trade_list:
                _enqueue_or_drop_oldest(self._trade_queue, trade_data, "trade")
        except ValueError as e:
            logger.warning(f"Failed to parse trade data: {e}")

    def _handle_executions_data(self, data: Any) -> None:
        """Parse and enqueue execution data.

        Args:
            data: Raw execution data from WebSocket.
        """
        try:
            execution_list = parse_kraken_execution_list(data)
            for execution_data in execution_list:
                _enqueue_or_drop_oldest(self._execution_queue, execution_data, "execution")
        except ValueError as e:
            logger.warning(f"Failed to parse execution data: {e}")

    async def _handle_instrument_data(self, message_dict: dict[str, Any]) -> None:
        """Parse and enqueue instrument pair data.

        Args:
            message_dict: Full WebSocket message with instrument data.
        """
        try:
            data = message_dict.get("data")
            if data is None:
                return
            if not isinstance(data, dict):
                logger.warning(f"Unexpected instrument data format: {type(data)}")
                return
            pairs_data: list[dict[str, Any]] = data.get("pairs", [])
            for pair_dict in pairs_data:
                await self._process_instrument_pair(pair_dict)
        except Exception as e:
            logger.warning(f"Failed to process instrument data: {e}")

    async def _process_instrument_pair(self, pair_dict: dict[str, Any]) -> None:
        """Validate and enqueue a single instrument pair.

        Args:
            pair_dict: Raw instrument pair dictionary from WebSocket.
        """
        try:
            validated = await asyncio.to_thread(
                KrakenInstrumentPairSchema.model_validate, pair_dict
            )
            _enqueue_or_drop_oldest(
                self._raw_instrument_queue,
                validated.model_dump(by_alias=True),
                "raw_instrument",
            )
        except ValidationError as e:
            logger.warning(f"Invalid instrument data, skipping: {e}")
            return
        try:
            instrument_pair = await asyncio.to_thread(parse_kraken_instrument, pair_dict)
            _enqueue_or_drop_oldest(self._instrument_queue, instrument_pair, "instrument")
        except ValueError as e:
            logger.debug(f"Skipping instrument parse (raw available): {e}")

    async def _get_or_create_ws_client(self) -> Any:
        if not self.api_key or not self.api_secret:
            raise RuntimeError("API credentials required for WebSocket trading")
        await self._ensure_ws_connected()
        return self._ws_client

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
        """Execute function with retry and circuit breaker logic.

        Args:
            func: Function to call (sync or async).
            *args: Positional arguments for func.
            **kwargs: Keyword arguments for func.

        Returns:
            Result of the function call.

        Raises:
            RuntimeError: If circuit breaker is open.
            Exception: If all retries exhausted or unexpected error.
        """
        if time.time() < self._circuit_open_until:
            raise RuntimeError("Circuit breaker open")
        max_retries = 3
        base_delay = 1.0
        attempt = 0
        while True:
            try:
                await self._acquire_rest_slot()
                result = await self._invoke_func(func, *args, **kwargs)
                self._circuit_failures = 0
                return result
            except ccxt.RateLimitExceeded:
                attempt = await self._handle_rate_limit(attempt, max_retries, base_delay)
            except (ccxt.NetworkError, ccxt.ExchangeNotAvailable) as e:
                attempt = await self._handle_network_error(e, attempt, max_retries, base_delay)
            except Exception as e:
                logger.error(f"Unexpected error: {e}")
                raise

    @staticmethod
    async def _invoke_func(func: Callable[..., Any], *args: Any, **kwargs: Any) -> Any:
        """Invoke a function, awaiting if it is a coroutine.

        Synchronous callables (CCXT REST helpers, native Kraken Trade
        REST SDK) are dispatched via ``asyncio.to_thread`` so a slow
        Kraken REST round-trip (2–5s under network jitter has been
        observed) no longer blocks the executor event loop. Async
        callables are awaited directly.

        Args:
            func: Function to call.
            *args: Positional arguments.
            **kwargs: Keyword arguments.

        Returns:
            Result of the function call.
        """
        if inspect.iscoroutinefunction(func):
            return await func(*args, **kwargs)
        return await asyncio.to_thread(func, *args, **kwargs)

    @staticmethod
    async def _handle_rate_limit(attempt: int, max_retries: int, base_delay: float) -> int:
        """Handle rate limit exceeded by sleeping with exponential backoff.

        Args:
            attempt: Current attempt number.
            max_retries: Maximum number of retries.
            base_delay: Base delay in seconds.

        Returns:
            Incremented attempt number.

        Raises:
            ccxt.RateLimitExceeded: If max retries exhausted.
        """
        logger.warning(f"Rate limit exceeded, retrying in {base_delay * (2 ** attempt)}s")
        if attempt < max_retries - 1:
            attempt += 1
            await asyncio.sleep(base_delay * (2 ** (attempt - 1)))
            return attempt
        raise ccxt.RateLimitExceeded("Rate limit exceeded after max retries")

    async def _handle_network_error(
        self, error: Exception, attempt: int, max_retries: int, base_delay: float
    ) -> int:
        """Handle network errors with retry and circuit breaker.

        Args:
            error: The network error that occurred.
            attempt: Current attempt number.
            max_retries: Maximum number of retries.
            base_delay: Base delay in seconds.

        Returns:
            Incremented attempt number.

        Raises:
            Exception: If max retries exhausted (re-raises original).
        """
        logger.warning(f"Network error: {error}, retrying in {base_delay * (2 ** attempt)}s")
        self._circuit_failures += 1
        if attempt < max_retries - 1:
            attempt += 1
            await asyncio.sleep(base_delay * (2 ** (attempt - 1)))
            return attempt
        if self._circuit_failures >= self._max_failures:
            self._circuit_open_until = time.time() + self._circuit_timeout
            logger.error("Circuit breaker opened due to repeated failures")
        raise

    def _convert_ccxt_order(self, ccxt_order: dict[str, Any]) -> ExchangeOrderSnapshot:
        """Convert a CCXT order dict to an ExchangeOrderSnapshot.

        Uses module-level _CCXT_STATUS_MAP, _CCXT_SIDE_MAP, and _CCXT_TYPE_MAP
        to translate CCXT string values to internal enum types.
        """
        ccxt_symbol = ccxt_order.get("symbol")
        native_symbol = ccxt_to_native(ccxt_symbol) if ccxt_symbol else ""
        return ExchangeOrderSnapshot(
            id=ccxt_order["id"],
            client_order_id=ccxt_order.get("clientOrderId"),
            symbol=native_symbol,
            side=_CCXT_SIDE_MAP.get(ccxt_order.get("side", ""), OrderSideEnum.BUY),
            type=_CCXT_TYPE_MAP.get(ccxt_order.get("type", ""), ExchangeOrderTypeEnum.LIMIT),
            amount=float(ccxt_order.get("amount") or 0),
            price=float(ccxt_order["price"]) if ccxt_order.get("price") else None,
            status=_CCXT_STATUS_MAP[ccxt_order["status"]],
            filled=float(ccxt_order.get("filled") or 0),
            remaining=float(ccxt_order.get("remaining") or 0),
            timestamp=(
                ccxt_order["timestamp"] / 1000.0 if ccxt_order.get("timestamp") else time.time()
            ),
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
            status=ExchangeOrderStatusEnum.PENDING,
            filled=float("0"),
            remaining=float(request.amount),
            timestamp=time.time(),
            fee=None,
        )
