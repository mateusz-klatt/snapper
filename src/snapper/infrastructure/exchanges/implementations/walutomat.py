"""Walutomat FX exchange client implementation.

This module provides WalutomatExchangeClient for interacting with Walutomat,
a Polish FX exchange specializing in currency pairs like EUR/PLN, USD/PLN.
It supports:

REST API Operations:
    - Market data: tickers via polling
    - Order management: create, cancel, get orders
    - Account: balance inquiries

Features:
    - HTTP polling for real-time ticker updates
    - RSA signature authentication for private API
    - Candle building from tick data
    - Support for both public and authenticated endpoints

Note: Walutomat does not provide WebSocket API, so real-time data is
obtained through periodic HTTP polling at configurable intervals.

Authentication uses RSA-SHA256 signatures with the private key provided
either as PEM format or base64-encoded PEM.
"""

import asyncio
import base64
import contextlib
import time
import uuid
from collections.abc import AsyncIterator
from datetime import UTC
from datetime import datetime
from typing import Any
from urllib.parse import urlencode

import httpx
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import padding
from loguru import logger

from snapper.infrastructure.exchanges.base import ExchangeClientBase
from snapper.infrastructure.exchanges.contracts import AccountBalance
from snapper.infrastructure.exchanges.contracts import CandleUpdate
from snapper.infrastructure.exchanges.contracts import ExchangeOrderRequest
from snapper.infrastructure.exchanges.contracts import ExchangeOrderSnapshot
from snapper.infrastructure.exchanges.contracts import ExecutionUpdate
from snapper.infrastructure.exchanges.contracts import OhlcvSnapshot
from snapper.infrastructure.exchanges.contracts import OrderSideEnum
from snapper.infrastructure.exchanges.contracts import OrderStatusEnum
from snapper.infrastructure.exchanges.contracts import OrderTypeEnum
from snapper.infrastructure.exchanges.contracts import TickerSnapshot
from snapper.infrastructure.exchanges.contracts import TickerUpdate
from snapper.infrastructure.exchanges.contracts import TradeUpdate
from snapper.infrastructure.exchanges.schemas.walutomat import WalutomatMarketPair
from snapper.infrastructure.exchanges.schemas.walutomat import WalutomatMarketResponse
from snapper.infrastructure.symbols.functions import native_to_walutomat
from snapper.infrastructure.symbols.functions import native_to_walutomat_rest
from snapper.infrastructure.symbols.functions import walutomat_rest_to_native
from snapper.infrastructure.symbols.functions import walutomat_to_native


class _RaisingAsyncIterator[T](AsyncIterator[T]):
    """Async iterator that raises the provided exception on iteration."""

    def __init__(self, exc: Exception) -> None:
        self._exc = exc

    def __aiter__(self) -> "_RaisingAsyncIterator[T]":
        return self

    async def __anext__(self) -> T:
        raise self._exc


_NOT_CONNECTED_MSG = "Not connected - call connect() first"
_AUTH_REQUIRED_MSG = "Trading requires authentication - provide api_key and private_key"


class WalutomatExchangeClient(ExchangeClientBase):
    """Walutomat FX exchange client with REST API support.

    This client provides access to the Walutomat Polish FX exchange
    through HTTP REST API with polling for real-time data.

    The client implements RSA-SHA256 authentication for private API
    endpoints and builds candles from tick data since Walutomat
    doesn't provide OHLCV data directly.

    Attributes:
        market_data_url: Public market data endpoint URL.
        api_base_url: Base URL for authenticated API.
        polling_interval: Interval between market data polls in seconds.
        timeout: HTTP request timeout in seconds.
    """

    def __init__(
        self,
        polling_interval: float = 10.0,
        timeout: float = 5.0,
        api_key: str | None = None,
        private_key_data: str | None = None,
        repository: Any | None = None,
    ) -> None:
        """Initialize Walutomat exchange client.

        Args:
            polling_interval: Interval between market data polls (default: 10s).
            timeout: HTTP request timeout in seconds (default: 5s).
            api_key: Walutomat API key for authenticated requests.
            private_key_data: RSA private key (PEM or base64-encoded PEM).
            repository: Database repository for order/execution logging.

        Raises:
            ValueError: If private key format is invalid.
        """
        super().__init__(repository=repository, exchange_name="walutomat")
        self.market_data_url = "https://user.walutomat.pl/api/public/marketBrief"
        self.api_base_url = "https://api.walutomat.pl/api/v2.0.0"
        self.polling_interval = polling_interval
        self.timeout = timeout
        self._api_key = api_key
        self._private_key: Any | None = None
        if private_key_data:
            if private_key_data.strip().startswith("-----BEGIN"):
                pem_bytes = private_key_data.encode()
            else:
                try:
                    pem_bytes = base64.b64decode(private_key_data)
                except Exception as e:
                    raise ValueError(
                        f"Invalid private key format - expected PEM or base64-encoded PEM: {e}"
                    ) from e
            try:
                self._private_key = serialization.load_pem_private_key(pem_bytes, password=None)
            except Exception as e:
                raise ValueError(f"Failed to load RSA private key: {e}") from e
        self._http_client: httpx.AsyncClient | None = None
        self._running = False
        self._tick_queue: asyncio.Queue[TickerUpdate] = asyncio.Queue()
        self._candle_queue: asyncio.Queue[CandleUpdate] = asyncio.Queue()
        self._polling_task: asyncio.Task[None] | None = None
        self._candle_builder_task: asyncio.Task[None] | None = None
        self._last_data: dict[str, WalutomatMarketPair] | None = None
        self._error_count = 0
        self._max_consecutive_errors = 5
        self._tick_buffers: dict[str, list[tuple[float, float]]] = {}

    def _require_connected(self) -> httpx.AsyncClient:
        """Verify HTTP client is connected and return it.

        Returns:
            The connected HTTP client.

        Raises:
            RuntimeError: If not connected.
        """
        if not self._http_client:
            raise RuntimeError(_NOT_CONNECTED_MSG)
        return self._http_client

    def _require_authenticated(self) -> httpx.AsyncClient:
        """Verify client is connected and has authentication credentials.

        Returns:
            The connected HTTP client.

        Raises:
            RuntimeError: If not connected or missing credentials.
        """
        client = self._require_connected()
        if not self._api_key or not self._private_key:
            raise RuntimeError(_AUTH_REQUIRED_MSG)
        return client

    async def connect(self) -> None:
        """Connect to Walutomat API and verify connectivity.

        Raises:
            ConnectionError: If initial API request fails.
        """
        if self._running:
            logger.warning("WalutomatExchangeClient already connected")
            return
        logger.info("Connecting to Walutomat API...")
        self._http_client = httpx.AsyncClient(
            timeout=httpx.Timeout(self.timeout),
            follow_redirects=True,
        )
        try:
            data = await self._fetch_market_data()
            self._last_data = data
            pair_count = len(data)
            logger.info(f"Walutomat API connected - {pair_count} pairs available")
        except Exception as e:
            await self._http_client.aclose()
            self._http_client = None
            raise ConnectionError(f"Failed to connect to Walutomat API: {e}") from e
        self._running = True

    async def disconnect(self) -> None:
        """Disconnect from Walutomat and stop polling tasks."""
        if not self._running:
            return
        logger.info("Disconnecting from Walutomat API...")
        self._running = False
        if self._polling_task:
            self._polling_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._polling_task
            self._polling_task = None
        if self._candle_builder_task:
            self._candle_builder_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._candle_builder_task
            self._candle_builder_task = None
        if self._http_client:
            await self._http_client.aclose()
            self._http_client = None
        logger.info("Disconnected from Walutomat API")

    async def _fetch_market_data(self) -> dict[str, WalutomatMarketPair]:
        """Fetch current market data from Walutomat public API.

        Returns:
            Dictionary mapping Walutomat symbol to market pair data.

        Raises:
            RuntimeError: If not connected.
        """
        if not self._http_client:
            raise RuntimeError("Not connected")
        response = await self._http_client.get(self.market_data_url)
        response.raise_for_status()
        data_list = response.json()
        market_response = WalutomatMarketResponse.from_api_response(data_list)
        return market_response.to_dict()

    def _sign_request(self, timestamp: str, endpoint: str, body: str = "") -> str:
        """Sign a request using RSA-SHA256.

        Args:
            timestamp: ISO 8601 timestamp for the request.
            endpoint: API endpoint path.
            body: Request body content.

        Returns:
            Base64-encoded signature string.

        Raises:
            RuntimeError: If private key not configured.
        """
        if not self._private_key:
            raise RuntimeError(
                "Trading API requires authentication - provide api_key and private_key"
            )
        data_to_sign = f"{timestamp}{endpoint}{body}"
        signature = self._private_key.sign(
            data_to_sign.encode(),
            padding.PKCS1v15(),
            hashes.SHA256(),
        )
        return base64.b64encode(signature).decode()

    def _get_auth_headers(self, endpoint: str, body: str = "") -> dict[str, str]:
        """Build authentication headers for a request.

        Args:
            endpoint: API endpoint path.
            body: Request body content.

        Returns:
            Dictionary of authentication headers.

        Raises:
            RuntimeError: If API key not configured.
        """
        if not self._api_key:
            raise RuntimeError("Trading API requires authentication - provide api_key")
        headers = {"X-API-Key": self._api_key}
        if self._private_key:
            timestamp = datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")
            signature = self._sign_request(timestamp, endpoint, body)
            headers["X-API-Signature"] = signature
            headers["X-API-Timestamp"] = timestamp
        return headers

    def _build_ticker_from_pair(
        self, native_symbol: str, pair_data: WalutomatMarketPair
    ) -> TickerUpdate:
        """Build a TickerUpdate from Walutomat pair data.

        Args:
            native_symbol: Native symbol string.
            pair_data: Walutomat market pair data.

        Returns:
            TickerUpdate with current market data.
        """
        offer = pair_data.best_offers
        bid = offer.bid_now if offer.bid_now is not None else 0.0
        ask = offer.ask_now if offer.ask_now is not None else 0.0
        return TickerUpdate(
            symbol=native_symbol,
            bid=bid,
            bid_qty=0.0,
            ask=ask,
            ask_qty=0.0,
            last=offer.forex_now,
            volume=0.0,
            vwap=offer.forex_now,
            low=offer.forex_now,
            high=offer.forex_now,
            change=0.0,
            change_pct=0.0,
        )

    async def _process_polling_data(
        self, data: dict[str, WalutomatMarketPair], symbol_map: dict[str, str]
    ) -> None:
        """Process fetched market data and enqueue tickers.

        Args:
            data: Fetched market data keyed by Walutomat symbol.
            symbol_map: Mapping of Walutomat symbol to native symbol.
        """
        for wal_symbol, native_symbol in symbol_map.items():
            if wal_symbol not in data:
                logger.debug(f"Symbol {wal_symbol} not in Walutomat data")
                continue
            pair_data = data[wal_symbol]
            ticker = self._build_ticker_from_pair(native_symbol, pair_data)
            await self._tick_queue.put(ticker)
            ts = time.time()
            mid_price = pair_data.best_offers.forex_now
            if native_symbol not in self._tick_buffers:
                self._tick_buffers[native_symbol] = []
            self._tick_buffers[native_symbol].append((ts, mid_price))
            logger.debug(f"{native_symbol}: bid={ticker.bid:.4f} ask={ticker.ask:.4f}")

    def _handle_http_error(self, error: httpx.HTTPError) -> bool:
        """Handle an HTTP error during polling.

        Increments error count and checks threshold.

        Args:
            error: The HTTP error that occurred.

        Returns:
            True if polling should stop (max errors exceeded), False otherwise.
        """
        self._error_count += 1
        logger.error(
            f"Walutomat API error ({self._error_count}/{self._max_consecutive_errors}): {error}"
        )
        if self._error_count >= self._max_consecutive_errors:
            logger.error("Max consecutive errors reached - stopping polling")
            self._running = False
            return True
        return False

    async def _polling_loop(self, symbols: list[str]) -> None:
        """Run the market data polling loop.

        Args:
            symbols: List of native symbols to poll.
        """
        logger.info(f"Starting Walutomat polling (interval: {self.polling_interval}s)")
        symbol_map = {native_to_walutomat(s): s for s in symbols}
        while self._running:
            try:
                data = await self._fetch_market_data()
                self._last_data = data
                self._error_count = 0
                await self._process_polling_data(data, symbol_map)
            except httpx.HTTPError as e:
                if self._handle_http_error(e):
                    break
            except Exception as e:
                logger.exception(f"Unexpected error in polling loop: {e}")
            await asyncio.sleep(self.polling_interval)
        logger.info("Walutomat polling stopped")

    async def _build_candle_for_symbol(
        self,
        symbol: str,
        ticks: list[tuple[float, float]],
        prev_minute_start: int,
        current_minute: int,
    ) -> None:
        """Build and emit a 1-minute candle for a single symbol.

        Args:
            symbol: Native symbol string.
            ticks: List of (timestamp, price) tick tuples.
            prev_minute_start: Start timestamp of the previous minute.
            current_minute: Start timestamp of the current minute.
        """
        minute_ticks = [
            (ts, price) for ts, price in ticks if prev_minute_start <= ts < current_minute
        ]
        if minute_ticks:
            prices = [price for _, price in minute_ticks]
            candle = CandleUpdate(
                symbol=symbol,
                open=prices[0],
                high=max(prices),
                low=min(prices),
                close=prices[-1],
                volume=0.0,
                vwap=sum(prices) / len(prices),
                trades=len(prices),
                interval_begin=datetime.fromtimestamp(prev_minute_start, UTC),
                interval=1,
            )
            await self._candle_queue.put(candle)
            logger.debug(
                f"{symbol} 1m candle: O={candle.open:.4f} H={candle.high:.4f} "
                f"L={candle.low:.4f} C={candle.close:.4f} ({len(prices)} ticks)"
            )
        self._tick_buffers[symbol] = [(ts, price) for ts, price in ticks if ts >= current_minute]

    async def _candle_builder_loop(self) -> None:
        """Build 1-minute candles from accumulated tick data."""
        logger.info("Starting Walutomat candle builder (1m interval)")
        while self._running:
            now = time.time()
            seconds_until_next_minute = 60 - (now % 60)
            await asyncio.sleep(seconds_until_next_minute)
            if not self._running:
                break
            current_minute = int(time.time() // 60) * 60
            prev_minute_start = current_minute - 60
            for symbol in tuple(self._tick_buffers):
                ticks = self._tick_buffers.get(symbol, [])
                if not ticks:
                    continue
                await self._build_candle_for_symbol(
                    symbol, ticks, prev_minute_start, current_minute
                )
        logger.info("Walutomat candle builder stopped")

    async def get_ticker(self, symbol: str) -> TickerSnapshot:
        """Fetch current ticker for a symbol.

        Args:
            symbol: Trading pair in native format (e.g., 'EUR-PLN').

        Returns:
            Current ticker snapshot.

        Raises:
            RuntimeError: If not connected.
            ValueError: If symbol not found.
        """
        self._require_connected()
        data = await self._fetch_market_data()
        wal_symbol = native_to_walutomat(symbol)
        if wal_symbol not in data:
            available = ", ".join(data.keys())
            raise ValueError(f"Symbol {symbol} not found. Available: {available}")
        pair_data = data[wal_symbol]
        offer = pair_data.best_offers
        bid = offer.bid_now if offer.bid_now is not None else 0.0
        ask = offer.ask_now if offer.ask_now is not None else 0.0
        return TickerSnapshot(
            symbol=symbol,
            bid=bid,
            ask=ask,
            last=offer.forex_now,
            timestamp=time.time(),
        )

    async def get_ohlcv(
        self,
        symbol: str,
        timeframe: str = "1m",
        since: int | None = None,
        limit: int | None = None,
    ) -> list[OhlcvSnapshot]:
        """Get OHLCV data (not supported by Walutomat).

        Args:
            symbol: Trading pair (ignored).
            timeframe: Candle interval (ignored).
            since: Start timestamp (ignored).
            limit: Maximum candles (ignored).

        Returns:
            Empty list - Walutomat does not provide OHLCV data.
        """
        logger.warning("Walutomat does not provide OhlcvSnapshot data - returning empty list")
        return []

    def subscribe_ticks(self, symbols: list[str]) -> AsyncIterator[TickerUpdate]:
        """Subscribe to ticker updates via HTTP polling.

        Args:
            symbols: List of symbols or ['*'] for all.

        Returns:
            AsyncIterator yielding TickerUpdate for each price change.

        Raises:
            RuntimeError: If not connected.
        """
        return self._subscribe_ticks_impl(symbols)

    async def _subscribe_ticks_impl(self, symbols: list[str]) -> AsyncIterator[TickerUpdate]:
        """Implement ticker polling subscription.

        Args:
            symbols: List of symbols or ['*'] for all.

        Yields:
            TickerUpdate for each price change.

        Raises:
            RuntimeError: If not connected.
        """
        if not self._running:
            raise RuntimeError(_NOT_CONNECTED_MSG)
        if symbols == ["*"]:
            symbols = self.get_supported_pairs()
            logger.info(f"Wildcard subscription - monitoring {len(symbols)} pairs")
        if not self._polling_task or self._polling_task.done():
            self._polling_task = asyncio.create_task(self._polling_loop(symbols))
        while self._running:
            try:
                ticker = await asyncio.wait_for(self._tick_queue.get(), timeout=1.0)
                yield ticker
            except TimeoutError:
                continue

    def subscribe_candles(
        self,
        symbols: list[str],
        timeframe: str = "1m",
    ) -> AsyncIterator[CandleUpdate]:
        """Subscribe to 1-minute candles built from tick data.

        Args:
            symbols: List of symbols or ['*'] for all.
            timeframe: Must be '1m' (only supported interval).

        Returns:
            AsyncIterator yielding CandleUpdate for each completed candle.

        Raises:
            RuntimeError: If not connected.
            NotImplementedError: If timeframe is not '1m'.
        """
        return self._subscribe_candles_impl(symbols, timeframe=timeframe)

    async def _subscribe_candles_impl(
        self,
        symbols: list[str],
        *,
        timeframe: str,
    ) -> AsyncIterator[CandleUpdate]:
        """Implement candle subscription based on tick polling.

        Args:
            symbols: List of symbols or ['*'] for all.
            timeframe: Candle interval (only '1m' supported).

        Yields:
            CandleUpdate for each completed candle.

        Raises:
            RuntimeError: If not connected.
            NotImplementedError: If timeframe is not '1m'.
        """
        if not self._running:
            raise RuntimeError(_NOT_CONNECTED_MSG)
        if timeframe != "1m":
            raise NotImplementedError(f"Walutomat only supports 1m candles, not {timeframe}")
        if symbols == ["*"]:
            symbols = self.get_supported_pairs()
            logger.info(f"Wildcard candle subscription - monitoring {len(symbols)} pairs")
        if not self._polling_task or self._polling_task.done():
            self._polling_task = asyncio.create_task(self._polling_loop(symbols))
        if not self._candle_builder_task or self._candle_builder_task.done():
            self._candle_builder_task = asyncio.create_task(self._candle_builder_loop())
        while self._running:
            try:
                candle = await asyncio.wait_for(self._candle_queue.get(), timeout=1.0)
                if candle.symbol in symbols:
                    yield candle
            except TimeoutError:
                continue

    def subscribe_trades(self, symbols: list[str]) -> AsyncIterator[TradeUpdate]:
        """Not implemented - Walutomat does not provide public trade feed.

        Args:
            symbols: List of symbols (unused).

        Yields:
            Never yields; raises before producing any value.

        Raises:
            NotImplementedError: Always raised.
        """
        _ = symbols
        return _RaisingAsyncIterator[TradeUpdate](
            NotImplementedError("Walutomat does not provide public trade feed")
        )

    def subscribe_executions(self) -> AsyncIterator[ExecutionUpdate]:
        """Not implemented - Walutomat is market data only.

        Yields:
            Never yields; raises before producing any value.

        Raises:
            NotImplementedError: Always raised.
        """
        return _RaisingAsyncIterator[ExecutionUpdate](
            NotImplementedError("Walutomat is market data only - no trading API")
        )

    def subscribe_instruments(self, **kwargs: Any) -> AsyncIterator[dict[str, Any]]:
        """Subscribe to instrument/pair information.

        Args:
            **kwargs: Ignored parameters.

        Yields:
            Dictionary with instrument details for each pair.

        Raises:
            RuntimeError: If not connected.
        """
        _ = kwargs
        return self._subscribe_instruments_impl()

    async def _subscribe_instruments_impl(self) -> AsyncIterator[dict[str, Any]]:
        if not self._running:
            raise RuntimeError(_NOT_CONNECTED_MSG)
        try:
            data = await self._fetch_market_data()
            for wal_symbol in data:
                parts = wal_symbol.split("_")
                if len(parts) != 2:
                    logger.warning(f"Unexpected symbol format: {wal_symbol}")
                    continue
                base, quote = parts
                yield {
                    "symbol": wal_symbol,
                    "walutomat_rest_symbol": f"{base}{quote}",
                    "native_symbol": f"{base}-{quote}",
                    "base": base,
                    "quote": quote,
                }
            logger.info(f"Yielded {len(data)} instrument pairs")
        except Exception as e:
            logger.error(f"Error fetching instruments: {e}")
            raise

    async def create_order(self, request: ExchangeOrderRequest) -> ExchangeOrderSnapshot:
        """Create a new FX order on Walutomat.

        Args:
            request: Order parameters.

        Returns:
            Created order snapshot.

        Raises:
            RuntimeError: If not connected or not authenticated.
        """
        client = self._require_authenticated()
        walutomat_rest_symbol = native_to_walutomat_rest(request.symbol)
        base_currency = request.symbol.split("-")[0]
        submit_id = str(uuid.uuid4())
        body_params = {
            "currencyPair": walutomat_rest_symbol,
            "buySell": request.side.value.upper(),
            "volume": f"{request.amount:.2f}",
            "volumeCurrency": base_currency,
            "dryRun": "false",
            "submitId": submit_id,
        }
        if request.price:
            body_params["limitPrice"] = f"{request.price:.4f}"
        body = urlencode(body_params)
        endpoint = "/api/v2.0.0/market_fx/orders"
        headers = self._get_auth_headers(endpoint, body)
        headers["Content-Type"] = "application/x-www-form-urlencoded"
        url = f"{self.api_base_url}/market_fx/orders"
        response = await client.post(url, content=body, headers=headers)
        response.raise_for_status()
        result = response.json()
        if not result.get("success"):
            raise RuntimeError(f"ExchangeOrderSnapshot creation failed: {result}")
        order_id = result["result"]["orderId"]
        logger.info(
            f"Created order {order_id}: {request.side.value} {request.amount} {request.symbol}"
        )
        order = ExchangeOrderSnapshot(
            id=order_id,
            client_order_id=submit_id,
            symbol=request.symbol,
            side=request.side,
            type=request.type,
            amount=request.amount,
            price=request.price,
            status=OrderStatusEnum.PENDING,
            filled=0.0,
            remaining=request.amount,
            timestamp=time.time(),
        )
        await self._log_order_to_db(request, order)
        return order

    async def cancel_order(self, order_id: str, symbol: str | None = None) -> ExchangeOrderSnapshot:
        """Cancel an existing order.

        Args:
            order_id: Order ID to cancel.
            symbol: Trading pair (optional).

        Returns:
            Cancelled order snapshot.

        Raises:
            RuntimeError: If not connected or not authenticated.
        """
        client = self._require_authenticated()
        endpoint = f"/api/v2.0.0/market_fx/orders/{order_id}/cancel"
        headers = self._get_auth_headers(endpoint, "")
        url = f"{self.api_base_url}/market_fx/orders/{order_id}/cancel"
        response = await client.post(url, headers=headers)
        response.raise_for_status()
        result = response.json()
        if not result.get("success"):
            raise RuntimeError(f"ExchangeOrderSnapshot cancellation failed: {result}")
        logger.info(f"Cancelled order {order_id}")
        return await self.get_order(order_id, symbol)

    async def get_order(self, order_id: str, symbol: str | None = None) -> ExchangeOrderSnapshot:
        """Get order details by ID.

        Args:
            order_id: Order ID to fetch.
            symbol: Trading pair (optional).

        Returns:
            Order snapshot.

        Raises:
            RuntimeError: If not connected or not authenticated.
            ValueError: If order not found.
        """
        self._require_authenticated()
        orders = await self.get_orders()
        for order in orders:
            if order.id == order_id:
                return order
        raise ValueError(f"ExchangeOrderSnapshot {order_id} not found")

    @staticmethod
    def _parse_walutomat_order(order_data: dict[str, Any]) -> ExchangeOrderSnapshot:
        """Parse a single Walutomat order response into an ExchangeOrderSnapshot.

        Args:
            order_data: Raw order data dictionary from Walutomat API.

        Returns:
            Parsed ExchangeOrderSnapshot.
        """
        return ExchangeOrderSnapshot(
            id=order_data["orderId"],
            client_order_id=order_data.get("submitId"),
            symbol=walutomat_rest_to_native(order_data["currencyPair"]),
            side=OrderSideEnum.BUY if order_data["buySell"] == "BUY" else OrderSideEnum.SELL,
            type=OrderTypeEnum.LIMIT,
            amount=float(order_data["volume"]),
            price=float(order_data["limitPrice"]),
            status=(
                OrderStatusEnum.OPEN if order_data["status"] == "ACTIVE" else OrderStatusEnum.CLOSED
            ),
            filled=float(order_data.get("boughtAmount", 0)),
            remaining=float(order_data["volume"]) - float(order_data.get("boughtAmount", 0)),
            timestamp=time.time(),
            fee=None,
        )

    async def get_orders(
        self,
        symbol: str | None = None,
        status: OrderStatusEnum | None = None,
        limit: int | None = None,
    ) -> list[ExchangeOrderSnapshot]:
        """Get list of active orders.

        Args:
            symbol: Filter by trading pair.
            status: Filter by status.
            limit: Maximum orders to return.

        Returns:
            List of order snapshots.

        Raises:
            RuntimeError: If not connected or not authenticated.
        """
        client = self._require_authenticated()
        endpoint = "/api/v2.0.0/market_fx/orders/active"
        headers = self._get_auth_headers(endpoint, "")
        url = f"{self.api_base_url}/market_fx/orders/active"
        response = await client.get(url, headers=headers)
        response.raise_for_status()
        result = response.json()
        if not result.get("success"):
            raise RuntimeError(f"Failed to fetch orders: {result}")
        orders = []
        for order_data in result["result"]:
            order = self._parse_walutomat_order(order_data)
            if symbol and order.symbol != symbol:
                continue
            if status and order.status != status:
                continue
            orders.append(order)
            if limit and len(orders) >= limit:
                break
        return orders

    async def get_balance(self, currency: str | None = None) -> dict[str, AccountBalance]:
        """Get account balances.

        Args:
            currency: Filter by specific currency.

        Returns:
            Dictionary of currency to balance info.

        Raises:
            RuntimeError: If not connected or not authenticated.
        """
        client = self._require_connected()
        if not self._api_key:
            raise RuntimeError("Trading requires authentication - provide api_key")
        endpoint = "/api/v2.0.0/account/balances"
        headers = self._get_auth_headers(endpoint, "")
        url = f"{self.api_base_url}/account/balances"
        response = await client.get(url, headers=headers)
        response.raise_for_status()
        result = response.json()
        if not result.get("success"):
            raise RuntimeError(f"Failed to fetch balances: {result}")
        balances = {}
        for balance_data in result["result"]:
            curr = balance_data["currency"]
            if currency and curr != currency:
                continue
            balance = AccountBalance(
                currency=curr,
                free=float(balance_data.get("balanceAvailable", 0)),
                used=float(balance_data.get("balanceReserved", 0)),
                total=float(balance_data.get("balanceTotal", 0)),
            )
            balances[curr] = balance
        return balances

    def get_supported_pairs(self) -> list[str]:
        """Get list of available trading pairs.

        Returns:
            List of supported FX pairs.

        Raises:
            RuntimeError: If no data available.
        """
        if not self._last_data:
            raise RuntimeError("No data available - call connect() or get_ticker() first")
        return [walutomat_to_native(symbol) for symbol in self._last_data]
