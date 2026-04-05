"""Zonda (BitBay) cryptocurrency exchange client implementation.

This module provides ZondaExchangeClient for interacting with the Zonda
exchange (formerly BitBay), a Polish cryptocurrency exchange. It supports:

REST API Operations:
    - Market data: tickers, OHLCV candles
    - Order management: create, cancel, get orders
    - Account: balance inquiries

WebSocket Subscriptions:
    - Public: tickers (merged with 24h stats), trades
    - Private: executions (requires API credentials and HMAC signing)

Features:
    - CCXT integration for standard REST operations
    - Native WebSocket implementation with reconnection logic
    - Ticker/stats cache merging for complete market data
    - HMAC-SHA512 authentication for private channels
    - Candle aggregation from trade stream

The client uses internal caches to merge ticker and 24h statistics
streams since Zonda provides them on separate WebSocket channels.
"""

import asyncio
import contextlib
import hashlib
import hmac
import json
import time
import uuid
from collections.abc import AsyncIterator
from collections.abc import Awaitable
from collections.abc import Callable
from datetime import UTC
from datetime import datetime
from typing import Any
from typing import cast

import ccxt
from loguru import logger
from websockets.asyncio.client import ClientConnection
from websockets.asyncio.client import connect
from websockets.exceptions import ConnectionClosed

from snapper.core.types import ExchangeEnum
from snapper.data.repository import Repository
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
from snapper.infrastructure.exchanges.schemas.zonda import ZondaExecutionData
from snapper.infrastructure.exchanges.schemas.zonda import ZondaStatsMessage
from snapper.infrastructure.exchanges.schemas.zonda import ZondaTickerMessage
from snapper.infrastructure.exchanges.schemas.zonda import ZondaTransactionsMessage
from snapper.infrastructure.exchanges.schemas.zonda import create_ticker_update_from_caches
from snapper.infrastructure.symbols.functions import ccxt_to_native
from snapper.infrastructure.symbols.functions import native_to_ccxt

_CREDENTIALS_REQUIRED_MSG = "API credentials required for trading"


class ZondaExchangeClient(ExchangeClientBase):
    """Zonda (BitBay) exchange client with REST and WebSocket support.

    This client provides access to the Zonda cryptocurrency exchange,
    using CCXT for REST operations and native WebSocket for real-time
    data streaming.

    The client implements automatic reconnection with exponential backoff
    and maintains caches for merging ticker and statistics streams.

    Attributes:
        ws_url: WebSocket endpoint URL.
        api_key: Zonda API key for authenticated requests.
        api_secret: Zonda API secret for HMAC signing.
    """

    def __init__(
        self,
        api_key: str | None = None,
        api_secret: str | None = None,
        max_reconnect_attempts: int = 10,
        reconnect_delay: float = 2.0,
        enable_rate_limit: bool = True,
        repository: Repository | None = None,
    ) -> None:
        """Initialize Zonda exchange client.

        Args:
            api_key: Zonda API key. Required for trading and private data.
            api_secret: Zonda API secret. Required for trading and private data.
            max_reconnect_attempts: Maximum WebSocket reconnection attempts.
            reconnect_delay: Delay between reconnection attempts in seconds.
            enable_rate_limit: Enable automatic rate limiting (default: True).
            repository: Database repository for order/execution logging.
        """
        super().__init__(repository=repository, exchange_name=ExchangeEnum.ZONDA)
        self.ws_url = "wss://api.zondacrypto.exchange/websocket/"
        self.api_key = api_key
        self.api_secret = api_secret
        ccxt_kwargs: dict[str, Any] = {
            "apiKey": api_key,
            "secret": api_secret,
            "enableRateLimit": enable_rate_limit,
            "timeout": 30000,
            "rateLimit": 1000,
        }
        self._ccxt_client = cast(Any, ccxt.zonda)(ccxt_kwargs)
        self._ws: ClientConnection | None = None
        self._tick_queue: asyncio.Queue[TickerUpdate] = asyncio.Queue()
        self._trade_queue: asyncio.Queue[TradeUpdate] = asyncio.Queue()
        self._candle_queue: asyncio.Queue[CandleUpdate] = asyncio.Queue()
        self._execution_queue: asyncio.Queue[ExecutionUpdate] = asyncio.Queue()
        self._subscribed_symbols: list[str] = []
        self._subscribed_trade_symbols: list[str] = []
        self._subscribed_candle_symbols: list[str] = []
        self._seq_no: dict[str, int] = {}
        self._max_reconnect_attempts = max_reconnect_attempts
        self._reconnect_delay = reconnect_delay
        self._running = False
        self._message_handler_task: asyncio.Task[None] | None = None
        self._ticker_cache: dict[str, dict[str, Any]] = {}
        self._stats_cache: dict[str, dict[str, Any]] = {}
        self._candle_builder: dict[str, dict[str, Any]] = {}
        self._candle_aggregator_task: asyncio.Task[None] | None = None
        self._pending_snapshots: dict[str, str] = {}

    def get_ccxt_client(self) -> Any:
        """Get the underlying CCXT client instance.

        Returns:
            The CCXT Zonda client for direct access to CCXT methods.
        """
        return self._ccxt_client

    async def connect(self) -> None:
        """Connect to Zonda WebSocket with automatic retry.

        Raises:
            Exception: If all connection attempts fail.
        """
        for attempt in range(self._max_reconnect_attempts):
            try:
                if self._ws is None:
                    logger.info(f"Connecting to Zonda WebSocket (attempt {attempt + 1})...")
                    self._ws = await connect(self.ws_url)
                    logger.info("Connected to Zonda WebSocket")
                    return
            except Exception as e:
                logger.warning(
                    f"Connection attempt {attempt + 1} failed: {e}. "
                    f"Retrying in {self._reconnect_delay}s..."
                )
                if attempt < self._max_reconnect_attempts - 1:
                    await asyncio.sleep(self._reconnect_delay)
                else:
                    logger.error("Max reconnection attempts reached")
                    raise

    async def disconnect(self) -> None:
        """Disconnect from WebSocket and clean up resources."""
        self._running = False
        if self._message_handler_task:
            self._message_handler_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._message_handler_task
        if self._ws:
            await self._ws.close()
            self._ws = None
            logger.info("Disconnected from Zonda WebSocket")

    async def _send_subscribe_pair(self, ticker_path: str, stats_path: str) -> None:
        """Send a matched pair of public subscribe messages (ticker + stats).

        Args:
            ticker_path: Path for the ticker subscription.
            stats_path: Path for the stats subscription.
        """
        assert self._ws is not None
        for path in (ticker_path, stats_path):
            msg = {
                "action": "subscribe-public",
                "module": "trading",
                "path": path,
            }
            await self._ws.send(json.dumps(msg))

    async def _resubscribe(self) -> None:
        """Resubscribe to all channels after reconnection."""
        assert self._ws is not None
        if self._subscribed_symbols == ["*"]:
            logger.info("Resubscribing to ALL symbols (wildcard)...")
            await self._send_subscribe_pair("ticker", "stats")
            logger.info("Resubscribed to ALL Zonda symbols (ticker + stats)")
            return
        logger.info(f"Resubscribing to {len(self._subscribed_symbols)} symbols...")
        for symbol in self._subscribed_symbols:
            symbol_lower = symbol.lower()
            await self._send_subscribe_pair(f"ticker/{symbol_lower}", f"stats/{symbol_lower}")
            logger.debug(f"Resubscribed to Zonda: {symbol} (ticker + stats)")
        logger.info(f"Resubscribed to {len(self._subscribed_symbols)} Zonda symbols")

    async def _handle_connection_closed(self, context: str) -> None:
        """Clean up after WebSocket connection closed and prepare for reconnect.

        Args:
            context: Description of which subscription lost the connection.
        """
        if self._message_handler_task and not self._message_handler_task.done():
            self._message_handler_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._message_handler_task
        self._ws = None
        self._seq_no.clear()
        await asyncio.sleep(self._reconnect_delay)

    def _ensure_message_handler_running(self) -> None:
        """Start the message handler task if not already running."""
        if not self._message_handler_task or self._message_handler_task.done():
            self._message_handler_task = asyncio.create_task(self._message_handler())

    async def _drain_queue(self, queue: asyncio.Queue[Any]) -> AsyncIterator[Any]:
        """Yield items from an async queue with timeout polling.

        Args:
            queue: Async queue to read from.

        Yields:
            Items from the queue while running.
        """
        while self._running:
            try:
                data = await asyncio.wait_for(queue.get(), timeout=1.0)
                yield data
            except TimeoutError:
                await asyncio.sleep(0.01)

    def subscribe_ticks(
        self, symbols: list[str], snapshot: bool = False
    ) -> AsyncIterator[TickerUpdate]:
        """Subscribe to real-time ticker updates.

        Args:
            symbols: List of symbols or ['*'] for all.
            snapshot: Request initial snapshot before streaming.

        Returns:
            AsyncIterator yielding TickerUpdate for each price change.

        Raises:
            ValueError: If wildcard used with snapshot=True.
        """
        return self._subscribe_ticks_impl(symbols, snapshot=snapshot)

    async def _subscribe_ticks_impl(
        self, symbols: list[str], *, snapshot: bool
    ) -> AsyncIterator[TickerUpdate]:
        """Implement WebSocket ticker subscription.

        Args:
            symbols: List of symbols or ['*'] for all.
            snapshot: Request initial snapshot before streaming.

        Yields:
            TickerUpdate for each price change.

        Raises:
            ValueError: If wildcard used with snapshot=True.
        """
        if snapshot and symbols == ["*"]:
            raise ValueError(
                "Zonda proxy API does not support wildcard snapshot. "
                "Use snapshot=True only with specific symbols like ['BTC-PLN', 'ETH-PLN']. "
                "For wildcard, use snapshot=False and subscribe with ['*']."
            )
        self._running = True
        self._subscribed_symbols = symbols
        while self._running:
            try:
                await self.connect()
                assert self._ws is not None
                await self._resubscribe()
                if snapshot:
                    await self._request_snapshots(symbols)
                self._ensure_message_handler_running()
                async for ticker_data in self._drain_queue(self._tick_queue):
                    yield ticker_data
            except ConnectionClosed as e:
                logger.warning(f"Connection closed by CloudFlare proxy: {e}. Reconnecting...")
                await self._handle_connection_closed("ticks")
            except Exception as e:
                logger.error(f"Zonda WebSocket subscription error: {e}")
                if self._running:
                    logger.info(f"Attempting reconnect in {self._reconnect_delay}s...")
                    await asyncio.sleep(self._reconnect_delay)
                else:
                    raise

    async def _message_handler(self) -> None:
        """Read WebSocket messages and dispatch to action-specific handlers."""
        assert self._ws is not None
        try:
            async for message in self._ws:
                try:
                    data = json.loads(message)
                    await self._dispatch_ws_action(data)
                except json.JSONDecodeError as e:
                    logger.warning(f"Failed to parse Zonda message: {e}")
                except Exception as e:
                    logger.error(f"Error handling Zonda message: {e}")
        except ConnectionClosed as e:
            logger.warning(f"Connection closed in message handler: {e}")
            return
        except Exception as e:
            logger.error(f"Zonda message handler error: {e}")
            return

    @staticmethod
    def _log_subscription_confirm(data: dict[str, Any]) -> None:
        """Log a subscription confirmation message.

        Args:
            data: WebSocket message with action, module, and path fields.
        """
        action = data.get("action", "")
        module = data.get("module", "unknown")
        path = data.get("path", "unknown")
        prefix = "Private s" if "private" in action else "S"
        logger.debug(f"{prefix}ubscription confirmed: {module}/{path}")

    async def _dispatch_ws_action(self, data: dict[str, Any]) -> None:
        """Dispatch a parsed WebSocket message based on its action field.

        Args:
            data: Parsed JSON message from WebSocket.
        """
        action = data.get("action", "")
        async_handlers: dict[str, Callable[[dict[str, Any]], Awaitable[None]]] = {
            "push": self._handle_push_message,
            "proxy-response": self._parse_proxy_response,
        }
        async_handler = async_handlers.get(action)
        if async_handler:
            await async_handler(data)
            return
        sync_actions: dict[str, Callable[[dict[str, Any]], None]] = {
            "subscribe-public-confirm": self._log_subscription_confirm,
            "subscribe-private-confirm": self._log_subscription_confirm,
            "subscribe-public-error": lambda d: logger.error(f"Subscription error: {d}"),
            "subscribe-private-error": lambda d: logger.error(f"Private subscription error: {d}"),
            "pong": lambda d: logger.debug("Pong received from server"),
            "json-error": lambda d: logger.error(f"JSON error from server: {d}"),
        }
        sync_handler = sync_actions.get(action)
        if sync_handler:
            sync_handler(data)

    def _validate_sequence_number(self, topic: str, seq_no: int | None) -> bool:
        """Validate and update sequence number for a topic.

        Args:
            topic: The topic to validate against.
            seq_no: Sequence number from the message, or None.

        Returns:
            True if the message should be processed, False if out-of-order.
        """
        if seq_no is None or not topic:
            return True
        last_seq = self._seq_no.get(topic, 0)
        if seq_no <= last_seq:
            logger.warning(
                f"Out-of-order seqNo for {topic}: received {seq_no}, expected > {last_seq}"
            )
            return False
        self._seq_no[topic] = seq_no
        return True

    async def _handle_push_message(self, data: dict[str, Any]) -> None:
        """Handle a push-type WebSocket message with sequence number validation.

        Args:
            data: Push message with topic, seqNo, and message payload.
        """
        topic = data.get("topic", "")
        if not self._validate_sequence_number(topic, data.get("seqNo")):
            return
        topic_routes: list[tuple[str, Callable[[dict[str, Any]], Awaitable[None]]]] = [
            ("trading/ticker", self._parse_ticker_message),
            ("trading/stats", self._parse_stats_message),
            ("trading/transactions/", self._parse_transactions_message),
            ("trading/history/transactions", self._parse_executions_message),
        ]
        for prefix, handler in topic_routes:
            if topic.startswith(prefix):
                await handler(data)
                return

    async def _parse_ticker_message(self, data: dict[str, Any]) -> None:
        try:
            msg = ZondaTickerMessage.model_validate(data)
            symbol = msg.extract_symbol()
            if not symbol:
                logger.warning(f"Could not extract symbol from ticker message: {data}")
                return
            self._ticker_cache[symbol] = {
                "bid": msg.message.bid,
                "ask": msg.message.ask,
                "last": msg.message.last,
            }
            logger.debug(
                f"Zonda ticker: {symbol} bid={msg.message.bid:.2f} ask={msg.message.ask:.2f}"
            )
            await self._try_merge_and_emit(symbol)
        except (ValueError, KeyError) as e:
            logger.warning(f"Failed to parse Zonda ticker data: {e}, data={data}")

    async def _parse_stats_message(self, data: dict[str, Any]) -> None:
        try:
            msg = ZondaStatsMessage.model_validate(data)
            if not msg.message:
                return
            for stats_data in msg.message:
                symbol = stats_data.symbol
                if not symbol:
                    continue
                self._stats_cache[symbol] = {
                    "high": stats_data.high,
                    "low": stats_data.low,
                    "volume": stats_data.volume,
                    "rate_24h": stats_data.rate_24h,
                }
                logger.debug(
                    f"Zonda stats: {symbol} h={stats_data.high:.2f} "
                    f"l={stats_data.low:.2f} "
                    f"v={stats_data.volume:.2f}"
                )
                await self._try_merge_and_emit(symbol)
        except (ValueError, KeyError) as e:
            logger.warning(f"Failed to parse Zonda stats data: {e}, data={data}")

    async def _send_proxy_pair(self, ticker_path: str, stats_path: str) -> None:
        """Send a matched pair of proxy requests (ticker + stats) over WebSocket.

        Args:
            ticker_path: REST path for the ticker proxy request.
            stats_path: REST path for the stats proxy request.
        """
        assert self._ws is not None
        for snapshot_type, path in (("ticker", ticker_path), ("stats", stats_path)):
            request_id = str(uuid.uuid4())
            self._pending_snapshots[request_id] = snapshot_type
            msg = {
                "requestId": request_id,
                "action": "proxy",
                "module": "trading",
                "path": path,
            }
            await self._ws.send(json.dumps(msg))

    async def _request_snapshots(self, symbols: list[str]) -> None:
        """Request ticker and stats snapshots via proxy API.

        Args:
            symbols: List of symbols or ``['*']`` for wildcard.
        """
        assert self._ws is not None
        if symbols == ["*"]:
            await self._send_proxy_pair("ticker", "stats")
            logger.info("Requested snapshots for ALL markets (wildcard)")
            return
        logger.info(f"Requesting snapshots for {len(symbols)} symbols...")
        for symbol in symbols:
            symbol_lower = symbol.lower()
            await self._send_proxy_pair(f"ticker/{symbol_lower}", f"stats/{symbol_lower}")
            await asyncio.sleep(0.01)
            logger.debug(f"Requested snapshots for {symbol}")

    async def _handle_ticker_snapshot(self, body: dict[str, Any]) -> None:
        """Process a ticker snapshot from a proxy response body.

        Args:
            body: Proxy response body dict.
        """
        ticker_data = body.get("ticker")
        if not ticker_data:
            return
        market_code = ticker_data.get("market", {}).get("code", "")
        push_data = {
            "action": "push",
            "topic": (f"trading/ticker/{market_code.lower()}" if market_code else "trading/ticker"),
            "message": ticker_data,
        }
        await self._parse_ticker_message(push_data)
        logger.debug(f"Parsed ticker snapshot: {market_code}")

    async def _handle_stats_snapshot(self, body: dict[str, Any]) -> None:
        """Process a stats snapshot from a proxy response body.

        Args:
            body: Proxy response body dict.
        """
        stats_data = body.get("stats")
        if not stats_data:
            return
        stats_array = [stats_data] if isinstance(stats_data, dict) else stats_data
        push_data = {
            "action": "push",
            "topic": "trading/stats",
            "message": stats_array,
        }
        await self._parse_stats_message(push_data)
        logger.debug(f"Parsed stats snapshot: {len(stats_array)} markets")

    async def _parse_proxy_response(self, data: dict[str, Any]) -> None:
        """Parse and dispatch a proxy API response.

        Args:
            data: Raw proxy response dict from WebSocket.
        """
        request_id = data.get("requestId")
        status_code = data.get("statusCode")
        body = data.get("body", {})
        if not request_id or request_id not in self._pending_snapshots:
            logger.warning(f"Received unexpected proxy response: {request_id}")
            return
        snapshot_type = self._pending_snapshots.pop(request_id)
        if status_code != 200:
            logger.error(f"Proxy request failed: {status_code}, body={body}")
            return
        if body.get("status") != "Ok":
            logger.warning(f"Proxy response not OK: {body}")
            return
        snapshot_handlers = {
            "ticker": self._handle_ticker_snapshot,
            "stats": self._handle_stats_snapshot,
        }
        handler = snapshot_handlers.get(snapshot_type)
        if handler:
            await handler(body)

    async def _try_merge_and_emit(self, symbol: str) -> None:
        """Merge ticker and stats caches and emit combined update."""
        if symbol not in self._ticker_cache:
            return
        ticker = self._ticker_cache[symbol]
        stats = self._stats_cache.get(symbol, {})
        ticker_data = create_ticker_update_from_caches(symbol, ticker, stats)
        await self._tick_queue.put(ticker_data)
        if stats:
            logger.debug(
                f"Zonda merged: {symbol} bid={ticker_data.bid:.2f} "
                f"ask={ticker_data.ask:.2f} (with stats)"
            )
        else:
            logger.debug(
                f"Zonda ticker-only: {symbol} bid={ticker_data.bid:.2f} "
                f"ask={ticker_data.ask:.2f} (no stats yet)"
            )

    async def _parse_transactions_message(self, data: dict[str, Any]) -> None:
        try:
            msg = ZondaTransactionsMessage.model_validate(data)
            symbol = msg.extract_symbol()
            if not symbol:
                logger.warning(f"Could not extract symbol from transactions message: {data}")
                return
            for tx in msg.message.transactions:
                trade_data = tx.to_trade_update(symbol)
                await self._trade_queue.put(trade_data)
                logger.debug(
                    f"Zonda trade: {symbol} {tx.side} {tx.amount:.6f} @ {tx.price:.2f} "
                    f"(id: {tx.id[:8]}...)"
                )
        except (ValueError, KeyError) as e:
            logger.warning(f"Failed to parse Zonda transactions data: {e}, data={data}")

    def _sign_private_message(self, payload: dict[str, Any]) -> dict[str, Any]:
        if not self.api_key or not self.api_secret:
            raise RuntimeError("API credentials required for private WebSocket subscriptions")
        timestamp = int(time.time())
        message = self.api_key + str(timestamp)
        signature = hmac.new(
            self.api_secret.encode("utf-8"), message.encode("utf-8"), hashlib.sha512
        ).hexdigest()
        signed_payload = payload.copy()
        signed_payload["publicKey"] = self.api_key
        signed_payload["hashSignature"] = signature
        signed_payload["requestTimestamp"] = timestamp
        return signed_payload

    async def _parse_executions_message(self, data: dict[str, Any]) -> None:
        """Parse private execution push from trading/history/transactions.

        WS push delivers a single flat execution object in 'message' field,
        not a list wrapped in 'history'.
        """
        try:
            raw_msg = data.get("message", {})
            exec_data = ZondaExecutionData.model_validate(raw_msg)
            execution = exec_data.to_execution_update()
            await self._execution_queue.put(execution)
            logger.debug(
                f"Zonda execution: {exec_data.market} {exec_data.user_action.lower()} "
                f"{exec_data.quantity:.6f} @ {exec_data.price:.2f} "
                f"(order: {exec_data.offer_id}, taker: {exec_data.was_taker})"
            )
        except (ValueError, KeyError) as e:
            logger.warning(f"Failed to parse Zonda executions data: {e}, data={data}")

    async def get_ticker(self, symbol: str) -> TickerSnapshot:
        """Fetch current ticker for a symbol from Zonda.

        Args:
            symbol: Trading pair symbol in native format.

        Returns:
            Current ticker snapshot with bid/ask/last.

        Raises:
            Exception: If API request fails.
        """
        try:
            ccxt_symbol = native_to_ccxt(symbol)
            ticker_data = await asyncio.to_thread(self._ccxt_client.fetch_ticker, ccxt_symbol)
            return TickerSnapshot(
                symbol=symbol,
                bid=float(ticker_data["bid"]) if ticker_data["bid"] else 0.0,
                ask=float(ticker_data["ask"]) if ticker_data["ask"] else 0.0,
                last=float(ticker_data["last"]) if ticker_data["last"] else 0.0,
                timestamp=(
                    float(ticker_data["timestamp"]) / 1000.0 if ticker_data["timestamp"] else 0.0
                ),
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
        """Fetch OHLCV candles via REST API.

        Args:
            symbol: Trading pair.
            timeframe: Candle interval.
            since: Start timestamp in milliseconds.
            limit: Maximum candles to return.

        Returns:
            List of OHLCV snapshots.

        Raises:
            Exception: If OHLCV fetch fails.
        """
        try:
            ccxt_symbol = native_to_ccxt(symbol)
            ohlcv_data = await asyncio.to_thread(
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

    @staticmethod
    def _convert_ccxt_order(
        ccxt_order: dict[str, Any], native_symbol: str | None = None
    ) -> ExchangeOrderSnapshot:
        """Convert a CCXT order dict to ExchangeOrderSnapshot.

        Args:
            ccxt_order: Raw order dict from CCXT.
            native_symbol: Override native symbol (when known from request context).

        Returns:
            Populated ExchangeOrderSnapshot.
        """
        ccxt_symbol = ccxt_order.get("symbol")
        symbol = native_symbol or (ccxt_to_native(ccxt_symbol) if ccxt_symbol else "")
        return ExchangeOrderSnapshot(
            id=ccxt_order["id"],
            client_order_id=ccxt_order.get("clientOrderId"),
            symbol=symbol,
            side=OrderSideEnum(ccxt_order.get("side", "buy")),
            type=OrderTypeEnum(ccxt_order.get("type", "limit")),
            amount=float(ccxt_order.get("amount") or 0),
            price=float(ccxt_order["price"]) if ccxt_order.get("price") else None,
            status=OrderStatusEnum(ccxt_order.get("status", "open")),
            filled=float(ccxt_order.get("filled") or 0),
            remaining=float(ccxt_order.get("remaining") or 0),
            timestamp=float(ccxt_order.get("timestamp") or time.time() * 1000) / 1000.0,
            fee=float(ccxt_order["fee"]["cost"]) if ccxt_order.get("fee") else None,
        )

    async def create_order(self, request: ExchangeOrderRequest) -> ExchangeOrderSnapshot:
        """Create a new order on Zonda.

        Args:
            request: Order parameters.

        Returns:
            Created order snapshot.

        Raises:
            RuntimeError: If API credentials are missing.
            Exception: If order creation fails.
        """
        if not self.api_key or not self.api_secret:
            raise RuntimeError(_CREDENTIALS_REQUIRED_MSG)
        try:
            ccxt_symbol = native_to_ccxt(request.symbol)
            order_params: dict[str, Any] = {}
            if request.client_order_id:
                order_params["clientOrderId"] = request.client_order_id
            ccxt_order = await asyncio.to_thread(
                self._ccxt_client.create_order,
                ccxt_symbol,
                request.type.value,
                request.side.value,
                request.amount,
                request.price,
                order_params,
            )
            exchange_id = str(ccxt_order.get("id") or "")
            order = ExchangeOrderSnapshot(
                id=exchange_id,
                client_order_id=request.client_order_id,
                symbol=request.symbol,
                side=request.side,
                type=request.type,
                amount=float(request.amount),
                price=float(request.price) if request.price else None,
                status=OrderStatusEnum.PENDING,
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
        except Exception as e:
            raw = locals().get("ccxt_order")
            logger.error(f"Failed to create order: {e}" + (f" | raw_response={raw}" if raw else ""))
            raise

    async def cancel_order(self, order_id: str, symbol: str | None = None) -> ExchangeOrderSnapshot:
        """Cancel an existing order.

        Args:
            order_id: Exchange order ID.
            symbol: Trading pair (optional).

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
            open_orders = await asyncio.to_thread(self._ccxt_client.fetch_open_orders, ccxt_symbol)
            matched = [o for o in open_orders if o.get("id") == order_id]
            if not matched:
                return ExchangeOrderSnapshot(
                    id=order_id,
                    client_order_id=None,
                    symbol=symbol or "",
                    side=OrderSideEnum.BUY,
                    type=OrderTypeEnum.LIMIT,
                    amount=0.0,
                    price=None,
                    status=OrderStatusEnum.CANCELED,
                    filled=0.0,
                    remaining=0.0,
                    timestamp=time.time(),
                )
            order_data = matched[0]
            cancel_params: dict[str, Any] = {"side": order_data.get("side", "buy")}
            if order_data.get("price") is not None:
                cancel_params["price"] = order_data["price"]
            await asyncio.to_thread(
                self._ccxt_client.cancel_order, order_id, ccxt_symbol, cancel_params
            )
            snapshot = self._convert_ccxt_order(order_data, symbol)
            snapshot.status = OrderStatusEnum.CANCELED
            return snapshot
        except Exception as e:
            logger.error(f"Failed to cancel order {order_id}: {e}")
            raise

    async def get_order(self, order_id: str, symbol: str | None = None) -> ExchangeOrderSnapshot:
        """Fetch order details by ID.

        Args:
            order_id: Exchange order ID.
            symbol: Trading pair (optional).

        Returns:
            Order snapshot.

        Raises:
            RuntimeError: If API credentials are missing.
            Exception: If order fetch fails.
        """
        if not self.api_key or not self.api_secret:
            raise RuntimeError(_CREDENTIALS_REQUIRED_MSG)
        try:
            ccxt_symbol = native_to_ccxt(symbol) if symbol else None
            ccxt_order = await asyncio.to_thread(
                self._ccxt_client.fetch_order, order_id, ccxt_symbol
            )
            return self._convert_ccxt_order(ccxt_order, symbol)
        except Exception as e:
            logger.error(f"Failed to get order {order_id}: {e}")
            raise

    async def get_orders(
        self,
        symbol: str | None = None,
        status: OrderStatusEnum | None = None,
        limit: int | None = None,
    ) -> list[ExchangeOrderSnapshot]:
        """Fetch orders with optional filtering.

        Args:
            symbol: Filter by trading pair.
            status: Filter by order status.
            limit: Maximum orders to return.

        Returns:
            List of order snapshots.

        Raises:
            RuntimeError: If API credentials are missing.
            Exception: If orders fetch fails.
        """
        if not self.api_key or not self.api_secret:
            raise RuntimeError(_CREDENTIALS_REQUIRED_MSG)
        try:
            ccxt_symbol = native_to_ccxt(symbol) if symbol else None
            if status == OrderStatusEnum.OPEN:
                ccxt_orders = await asyncio.to_thread(
                    self._ccxt_client.fetch_open_orders, ccxt_symbol, None, limit
                )
            elif status == OrderStatusEnum.CLOSED:
                ccxt_orders = await asyncio.to_thread(
                    self._ccxt_client.fetch_closed_orders, ccxt_symbol, None, limit
                )
            else:
                ccxt_orders = await asyncio.to_thread(
                    self._ccxt_client.fetch_orders, ccxt_symbol, None, limit
                )
            return [self._convert_ccxt_order(o, symbol) for o in ccxt_orders]
        except Exception as e:
            logger.error(f"Failed to get orders: {e}")
            raise

    async def get_balance(self, currency: str | None = None) -> dict[str, AccountBalance]:
        """Fetch account balances.

        Args:
            currency: Filter by specific currency.

        Returns:
            Dictionary of currency to balance info.

        Raises:
            RuntimeError: If API credentials are missing.
            Exception: If balance fetch fails.
        """
        if not self.api_key or not self.api_secret:
            raise RuntimeError(_CREDENTIALS_REQUIRED_MSG)
        try:
            ccxt_balance = await asyncio.to_thread(self._ccxt_client.fetch_balance)
            balances: dict[str, AccountBalance] = {}
            for curr, bal in ccxt_balance.items():
                if curr in ("info", "free", "used", "total", "timestamp", "datetime"):
                    continue
                if currency and curr != currency:
                    continue
                balances[curr] = AccountBalance(
                    currency=curr,
                    free=float(bal.get("free", 0)),
                    used=float(bal.get("used", 0)),
                    total=float(bal.get("total", 0)),
                )
            return balances
        except Exception as e:
            logger.error(f"Failed to get balance: {e}")
            raise

    async def _subscribe_to_transactions(self, symbols: list[str]) -> None:
        """Send subscription messages for trade transactions.

        Args:
            symbols: List of symbols to subscribe to.
        """
        assert self._ws is not None
        for symbol in symbols:
            symbol_lower = symbol.lower()
            msg = {
                "action": "subscribe-public",
                "module": "trading",
                "path": f"transactions/{symbol_lower}",
            }
            await self._ws.send(json.dumps(msg))
            logger.debug(f"Subscribed to Zonda transactions: {symbol}")

    def subscribe_trades(self, symbols: list[str]) -> AsyncIterator[TradeUpdate]:
        """Subscribe to real-time trade updates.

        Args:
            symbols: List of specific symbols (wildcard not supported).

        Returns:
            AsyncIterator yielding TradeUpdate for each trade.

        Raises:
            ValueError: If wildcard subscription attempted.
        """
        return self._subscribe_trades_impl(symbols)

    async def _subscribe_trades_impl(self, symbols: list[str]) -> AsyncIterator[TradeUpdate]:
        """Implement WebSocket trades subscription.

        Args:
            symbols: List of specific symbols (wildcard not supported).

        Yields:
            TradeUpdate for each trade.

        Raises:
            ValueError: If wildcard subscription attempted.
        """
        if symbols == ["*"]:
            raise ValueError(
                "Zonda wildcard subscription for trades is not functional. "
                "Server accepts subscription but never pushes trade data. "
                "Please provide specific symbols like ['BTC-PLN', 'ETH-EUR']"
            )
        self._running = True
        self._subscribed_trade_symbols = symbols
        while self._running:
            try:
                await self.connect()
                await self._subscribe_to_transactions(symbols)
                self._ensure_message_handler_running()
                async for trade_data in self._drain_queue(self._trade_queue):
                    yield trade_data
            except ConnectionClosed as e:
                logger.warning(f"Trades connection closed: {e}. Reconnecting...")
                await self._handle_connection_closed("trades")
            except Exception as e:
                logger.error(f"Zonda trades subscription error: {e}")
                if self._running:
                    await asyncio.sleep(self._reconnect_delay)
                else:
                    raise

    def subscribe_candles(
        self,
        symbols: list[str],
        timeframe: str = "1m",
    ) -> AsyncIterator[CandleUpdate]:
        """Subscribe to 1-minute candles built from trades.

        Args:
            symbols: List of symbols (wildcard not supported).
            timeframe: Must be '1m' (only supported interval).

        Returns:
            AsyncIterator yielding CandleUpdate for each completed candle.

        Raises:
            ValueError: If timeframe is not '1m' or wildcard used.
        """
        return self._subscribe_candles_impl(symbols, timeframe=timeframe)

    async def _subscribe_candles_impl(
        self,
        symbols: list[str],
        *,
        timeframe: str,
    ) -> AsyncIterator[CandleUpdate]:
        """Implement candle streaming built from trades.

        Args:
            symbols: List of symbols (wildcard not supported).
            timeframe: Must be '1m' (only supported interval).

        Yields:
            CandleUpdate for each completed candle.

        Raises:
            ValueError: If timeframe is not '1m' or wildcard used.
        """
        if timeframe != "1m":
            raise ValueError(
                f"Zonda only supports 1m candles (built from trades). "
                f"Got timeframe: {timeframe}. Use REST API get_ohlcv() for other timeframes."
            )
        if symbols == ["*"]:
            raise ValueError(
                "Zonda does not support wildcard subscription for candles. "
                "Please provide specific symbols like ['BTC-PLN', 'ETH-EUR']"
            )
        self._running = True
        self._subscribed_candle_symbols = symbols
        if not self._candle_aggregator_task or self._candle_aggregator_task.done():
            self._candle_aggregator_task = asyncio.create_task(self._candle_aggregator(symbols))
        try:
            while self._running:
                try:
                    candle_data = await asyncio.wait_for(self._candle_queue.get(), timeout=1.0)
                    yield candle_data
                except TimeoutError:
                    await asyncio.sleep(0.01)
        finally:
            await self._stop_candle_aggregator()

    async def _stop_candle_aggregator(self) -> None:
        """Cancel the candle aggregator task if it is still running."""
        task = self._candle_aggregator_task
        if task and not task.done():
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await task

    def _update_candle_builder(self, trade: TradeUpdate) -> None:
        """Update or create candle builder entry for a trade.

        Args:
            trade: Trade update to incorporate into candle data.
        """
        minute_ts = int(trade.timestamp.replace(second=0, microsecond=0).timestamp())
        candle_key = f"{trade.symbol}_{minute_ts}"
        if candle_key not in self._candle_builder:
            self._candle_builder[candle_key] = {
                "symbol": trade.symbol,
                "open": trade.price,
                "high": trade.price,
                "low": trade.price,
                "close": trade.price,
                "volume": trade.quantity,
                "trades": 1,
                "vwap_sum": trade.price * trade.quantity,
                "interval_begin": trade.timestamp.replace(second=0, microsecond=0),
            }
            return
        candle = self._candle_builder[candle_key]
        candle["high"] = max(candle["high"], trade.price)
        candle["low"] = min(candle["low"], trade.price)
        candle["close"] = trade.price
        candle["volume"] += trade.quantity
        candle["trades"] += 1
        candle["vwap_sum"] += trade.price * trade.quantity

    async def _emit_completed_candles(self) -> None:
        """Emit completed candles from the builder and remove them."""
        current_minute = int(datetime.now(tz=UTC).replace(second=0, microsecond=0).timestamp())
        for key, candle in self._candle_builder.copy().items():
            candle_minute = int(candle["interval_begin"].timestamp())
            if candle_minute >= current_minute:
                continue
            vwap = candle["vwap_sum"] / candle["volume"] if candle["volume"] > 0 else 0.0
            candle_data = CandleUpdate(
                symbol=candle["symbol"],
                open=candle["open"],
                high=candle["high"],
                low=candle["low"],
                close=candle["close"],
                vwap=vwap,
                trades=candle["trades"],
                volume=candle["volume"],
                interval_begin=candle["interval_begin"],
                interval=1,
            )
            await self._candle_queue.put(candle_data)
            del self._candle_builder[key]
            logger.debug(f"Emitted 1m candle for {candle['symbol']} at {candle['interval_begin']}")

    async def _candle_aggregator(self, symbols: list[str]) -> None:
        """Aggregate trades into 1-minute candles.

        Args:
            symbols: List of symbols to aggregate candles for.
        """
        async for trade in self.subscribe_trades(symbols):
            self._update_candle_builder(trade)
            await self._emit_completed_candles()

    async def _subscribe_to_private_executions(self) -> None:
        """Send private subscription for execution reports (history/transactions)."""
        assert self._ws is not None
        payload = {
            "action": "subscribe-private",
            "module": "trading",
            "path": "history/transactions",
        }
        signed_msg = self._sign_private_message(payload)
        await self._ws.send(json.dumps(signed_msg))
        logger.debug("Subscribed to Zonda private executions (trading/history/transactions)")

    def subscribe_executions(self) -> AsyncIterator[ExecutionUpdate]:
        """Subscribe to user execution reports (private channel).

        Returns:
            AsyncIterator yielding ExecutionUpdate for each order fill.

        Raises:
            RuntimeError: If API credentials are missing.
        """
        return self._subscribe_executions_impl()

    async def _subscribe_executions_impl(self) -> AsyncIterator[ExecutionUpdate]:
        """Implement private execution reports streaming.

        Yields:
            ExecutionUpdate for each order fill.

        Raises:
            RuntimeError: If API credentials are missing.
        """
        if not self.api_key or not self.api_secret:
            raise RuntimeError(
                "API credentials required for subscribe_executions(). "
                "Provide api_key and api_secret in ZondaExchangeClient constructor."
            )
        self._running = True
        while self._running:
            try:
                await self.connect()
                await self._subscribe_to_private_executions()
                self._ensure_message_handler_running()
                async for execution_data in self._drain_queue(self._execution_queue):
                    yield execution_data
            except ConnectionClosed as e:
                logger.warning(f"Executions connection closed: {e}. Reconnecting...")
                await self._handle_connection_closed("executions")
            except Exception as e:
                logger.error(f"Zonda executions subscription error: {e}")
                if self._running:
                    await asyncio.sleep(self._reconnect_delay)
                else:
                    raise

    def subscribe_instruments(self, **kwargs: Any) -> AsyncIterator[dict[str, Any]]:
        """Fetch instrument information via CCXT.

        Args:
            **kwargs: Ignored parameters.

        Yields:
            Dictionary with market info for each instrument.

        Raises:
            RuntimeError: If market loading fails.
        """
        _ = kwargs
        return self._subscribe_instruments_impl()

    async def _subscribe_instruments_impl(self) -> AsyncIterator[dict[str, Any]]:
        try:
            markets = await asyncio.to_thread(self._ccxt_client.load_markets)
            if not isinstance(markets, dict):
                raise RuntimeError(f"Unexpected markets type: {type(markets)}")
            for market_data in markets.values():
                if isinstance(market_data, dict):
                    yield market_data
            logger.info(f"Yielded {len(markets)} instruments from CCXT load_markets()")
        except Exception as e:
            logger.error(f"Zonda instruments subscription error: {e}")
            raise
